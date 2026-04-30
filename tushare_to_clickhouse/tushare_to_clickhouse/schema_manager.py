"""Schema inference and type mapping for ClickHouse."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from loguru import logger


PANDAS_TO_CLICKHOUSE = {
    "object": "String",
    "string": "String",
    "int64": "Int64",
    "int32": "Int32",
    "float64": "Float64",
    "float32": "Float32",
    "bool": "Int8",
    "datetime64[ns]": "DateTime",
}

# Columns that look like dates and have YYYYMMDD values
DATE_PATTERN = re.compile(r"^\d{8}$")


def sanitize_all_nan_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all-NaN float columns to object (String) type.

    Tushare sometimes returns text columns where every value is NaN.
    pandas infers float64, but these should become Nullable(String) in ClickHouse.
    """
    if df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "float64" and df[col].isna().all():
            df[col] = None
    return df


def infer_clickhouse_schema(
    df: pd.DataFrame, date_hints: Optional[List[str]] = None
) -> Tuple[List[Tuple[str, str]], Set[str]]:
    """Infer ClickHouse column types from a pandas DataFrame.
    
    Returns:
        (schema, date_cols) where schema is list of (col_name, ch_type) 
        and date_cols is the set of columns inferred as Date type.
    """
    schema: List[Tuple[str, str]] = []
    date_cols: Set[str] = set()
    hints = set(date_hints or [])
    for col in df.columns:
        dtype = str(df[col].dtype)
        ch_type = PANDAS_TO_CLICKHOUSE.get(dtype, "String")

        # Heuristic: if column name contains 'date' and values look like YYYYMMDD
        if ch_type == "String" and (
            "date" in col.lower() or col in hints
        ):
            sample = df[col].dropna().astype(str).head(100)
            if len(sample) > 0 and sample.str.match(DATE_PATTERN).all():
                has_nulls = df[col].isna().any()
                ch_type = "Nullable(Date)" if has_nulls else "Date"
                date_cols.add(col)

        schema.append((col, ch_type))
    return schema, date_cols


def build_create_table_sql(
    table: str,
    schema: List[Tuple[str, str]],
    order_by: List[str],
    engine: str = "MergeTree",
    partition_by: Optional[str] = None,
) -> str:
    """Generate a ClickHouse CREATE TABLE statement."""
    cols_def = ",\n    ".join(f'"{col}" {ctype}' for col, ctype in schema)
    order_by_str = ", ".join(f'"{c}"' for c in order_by)
    partition_clause = f"\nPARTITION BY ({partition_by})" if partition_by else ""
    sql = (
        f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
        f"    {cols_def}\n"
        f") ENGINE = {engine}(){partition_clause}\n"
        f"ORDER BY ({order_by_str})"
    )
    return sql


def coerce_date_columns(df: pd.DataFrame, date_cols: Set[str]) -> pd.DataFrame:
    """Convert YYYYMMDD string columns to Python date objects for ClickHouse Date columns.

    Non-null values become datetime.date; null/NaT values become None
    so that Nullable(Date) columns serialize correctly.
    """
    if not date_cols or df.empty:
        return df
    df = df.copy()
    for col in date_cols:
        if col not in df.columns:
            continue
        series = df[col]
        if series.dropna().empty:
            continue
        converted = pd.to_datetime(series, format="%Y%m%d", errors="coerce")
        if converted.notna().sum() == 0:
            converted = pd.to_datetime(series, errors="coerce")
        # Turn datetime64 → Python date; NaT → None
        df[col] = converted.apply(lambda x: x.date() if pd.notna(x) else None)
    return df


def align_dataframe_to_schema(
    df: pd.DataFrame, existing_cols: List[str], date_cols: Set[str]
) -> pd.DataFrame:
    """Align DataFrame to target schema: drop extra cols, reorder, coerce dates, normalize NaN."""
    if df.empty:
        return df

    df = df.where(pd.notna(df), None)

    extra = [c for c in df.columns if c not in existing_cols]
    if extra:
        logger.warning(f"Dropping extra columns not in target: {extra}")

    aligned = df[[c for c in existing_cols if c in df.columns]].copy()

    # Fill missing columns with None
    for col in existing_cols:
        if col not in aligned.columns:
            aligned[col] = None

    aligned = aligned[existing_cols]

    if date_cols:
        aligned = coerce_date_columns(aligned, date_cols)

    return aligned


def dedupe_by_pk(df: pd.DataFrame, pk_cols: List[str], target_table: str) -> pd.DataFrame:
    """Dedupe DataFrame by PK columns, keeping the last row."""
    if not pk_cols or df.empty:
        return df
    if any(col not in df.columns for col in pk_cols):
        return df

    sort_cols = [c for c in ["ann_date", "f_ann_date", "end_date", "update_flag"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols, kind="stable")

    before = len(df)
    deduped = df.drop_duplicates(subset=pk_cols, keep="last")
    dropped = before - len(deduped)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} duplicate rows for {target_table} (pk={pk_cols})")
    return deduped

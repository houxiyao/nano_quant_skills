"""Schema inference and type mapping for ClickHouse."""

from __future__ import annotations

from typing import List, Optional, Tuple

import pandas as pd
from loguru import logger


PANDAS_TO_CLICKHOUSE = {
    "str": "String",
    "object": "String",
    "string": "String",
    "int64": "Int64",
    "int32": "Int32",
    "float64": "Float64",
    "float32": "Float32",
    "bool": "Int8",
    "datetime64[us]": "DateTime64",
    "datetime64[s]": "DateTime64",
}


def infer_clickhouse_schema(
    df: pd.DataFrame, order_by: List[str],
) -> List[Tuple[str, str]]:
    """Infer ClickHouse column types from a pandas DataFrame.

    Returns:
        schema is list of (col_name, ch_type).
    """
    schema: List[Tuple[str, str]] = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        ch_type = PANDAS_TO_CLICKHOUSE.get(dtype)
        if ch_type is None:
            raise TypeError(f"Unknown dtype: {dtype} for column {col}")

        schema.append((col, ch_type if col in order_by else f"Nullable({ch_type})"))
    return schema


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


def align_dataframe_to_schema(
    df: pd.DataFrame, existing_cols: List[str]
) -> pd.DataFrame:
    """Align DataFrame to target schema: drop extra cols, reorder, normalize NaN."""
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

    return aligned

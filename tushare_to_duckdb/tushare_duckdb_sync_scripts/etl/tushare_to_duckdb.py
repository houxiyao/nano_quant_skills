"""从 Tushare 直接读取数据（Pandas）并写入本地 DuckDB。

Tushare 官方文档入口：
- https://tushare.pro/document/2
- https://tushare.pro/document/2?doc_id=209

最小示例：
python etl/tushare_to_duckdb.py \
  --endpoint stock_basic \
  --duckdb-path ./data/ashare.duckdb \
  --target-table raw_stock_basic \
  --mode overwrite
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import time
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from loguru import logger
import pandas as pd

try:
    from tushare_to_duckdb.tushare_duckdb_sync_scripts.utils.database_handler_util import get_tushare_client
except ModuleNotFoundError:  # pragma: no cover - direct script fallback
    from tushare_to_duckdb.tushare_duckdb_sync_scripts.utils.database_handler_util import get_tushare_client

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise ImportError(
        "duckdb is required. Please install it with `pip install duckdb`."
    ) from exc


VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class StructuredETLError(Exception):
    """结构化 ETL 异常，便于日志检索。"""

    def __init__(self, message: str, context: Optional[Dict[str, object]] = None):
        super().__init__(message)
        self.context = context or {}

    def to_dict(self) -> Dict[str, object]:
        return {
            "error": str(self),
            "context": self.context,
        }


def _log_event(event: str, payload: Dict[str, object]) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=True, default=str))


def _parse_table_name(table_name: str) -> Tuple[Optional[str], str]:
    parts = table_name.split(".")
    if len(parts) == 1:
        schema, table = None, parts[0]
    elif len(parts) == 2:
        schema, table = parts
    else:
        raise StructuredETLError(
            "Invalid DuckDB table name. Use table or schema.table",
            {"target_table": table_name},
        )

    for piece in (schema, table):
        if piece is None:
            continue
        if not VALID_IDENTIFIER.match(piece):
            raise StructuredETLError(
                "DuckDB identifier contains unsupported characters",
                {"target_table": table_name},
            )
    return schema, table


def _qualified_name(schema: Optional[str], table: str) -> str:
    return f'"{schema}"."{table}"' if schema else f'"{table}"'


def _resolve_status_backend(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    def _table_exists(table_name: str) -> bool:
        schema_name, pure_table_name = _parse_table_name(table_name)
        if schema_name:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema_name, pure_table_name],
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = ?
                """,
                [pure_table_name],
            ).fetchone()
        return bool(row and int(row[0]) > 0)

    # 优先复用已迁移的统一状态表（6 列新结构）。
    for candidate in ["table_sync_state", "meta.table_sync_status"]:
        if _table_exists(candidate):
            schema_name, table_name = _parse_table_name(candidate)
            fq_name = _qualified_name(schema_name, table_name)
            return {"mode": "meta", "fq_name": fq_name}

    # 兼容旧 3 列状态表。
    for candidate in ["raw_table_sync_status", "table_sync_status", "raw.table_sync_status"]:
        if _table_exists(candidate):
            schema_name, table_name = _parse_table_name(candidate)
            fq_name = _qualified_name(schema_name, table_name)
            return {"mode": "legacy", "fq_name": fq_name}

    con.execute('CREATE SCHEMA IF NOT EXISTS "meta"')
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS "meta"."table_sync_status" (
            source_table VARCHAR,
            dimension_type VARCHAR,
            dimension_value VARCHAR,
            is_sync INTEGER,
            error_message VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    return {"mode": "meta", "fq_name": '"meta"."table_sync_status"'}


def _write_sync_status(
    con: duckdb.DuckDBPyConnection,
    status_backend: Dict[str, Any],
    source_table: str,
    dimension_type: str,
    dimension_value: str,
    is_sync: int,
    error_message: str = "",
) -> None:
    fq_name = str(status_backend["fq_name"])
    if status_backend["mode"] == "legacy":
        con.execute(
            f"DELETE FROM {fq_name} WHERE tushare_table_name = ? AND trade_date = ?",
            [source_table, dimension_value],
        )
        con.execute(
            f"""
            INSERT INTO {fq_name}
            (tushare_table_name, trade_date, is_sync)
            VALUES (?, ?, ?)
            """,
            [source_table, dimension_value, int(is_sync)],
        )
        return

    con.execute(
        f"DELETE FROM {fq_name} WHERE source_table = ? AND dimension_type = ? AND dimension_value = ?",
        [source_table, dimension_type, dimension_value],
    )
    con.execute(
        f"""
        INSERT INTO {fq_name}
        (source_table, dimension_type, dimension_value, is_sync, error_message, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            source_table,
            dimension_type,
            dimension_value,
            int(is_sync),
            error_message,
            datetime.now(),
        ],
    )


def _list_synced_dimensions(
    con: duckdb.DuckDBPyConnection,
    status_backend: Dict[str, Any],
    source_table: str,
    dimension_type: str,
) -> set[str]:
    fq_name = str(status_backend["fq_name"])
    if status_backend["mode"] == "legacy":
        rows = con.execute(
            f"""
            SELECT trade_date
            FROM {fq_name}
            WHERE tushare_table_name = ? AND is_sync = 1
            """,
            [source_table],
        ).fetchall()
        return {str(r[0]) for r in rows}

    rows = con.execute(
        f"""
        SELECT dimension_value
        FROM {fq_name}
        WHERE source_table = ? AND dimension_type = ? AND is_sync = 1
        """,
        [source_table, dimension_type],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _normalize_date_text(date_text: str) -> str:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_text, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    raise StructuredETLError(
        "Invalid date format. Use YYYYMMDD or YYYY-MM-DD",
        {"date_text": date_text},
    )


def _get_trade_dates(tushare_client, start_date: str, end_date: str) -> List[str]:
    trade_cal = tushare_client.query("trade_cal", start_date=start_date, end_date=end_date)
    if trade_cal is None or trade_cal.empty:
        return []
    open_days = trade_cal[trade_cal["is_open"] == 1][["cal_date"]].copy()
    return sorted(open_days["cal_date"].astype(str).tolist())


def _get_report_periods(start_period: str, end_period: str) -> List[str]:
    start_ts = pd.to_datetime(start_period, format="%Y%m%d", errors="raise")
    end_ts = pd.to_datetime(end_period, format="%Y%m%d", errors="raise")
    quarter_end_dates = pd.date_range(start=start_ts, end=end_ts, freq="QE-DEC")
    return sorted(quarter_end_dates.strftime("%Y%m%d").tolist())


def _resolve_dimension_values(
    con: duckdb.DuckDBPyConnection,
    status_backend: Dict[str, Any],
    tushare_client,
    source_table: str,
    args: argparse.Namespace,
) -> List[str]:
    if args.dimension_type == "none":
        return [""]

    today_text = datetime.now().strftime("%Y%m%d")
    if args.dimension_type == "trade_date":
        start_date = _normalize_date_text(args.start_date or "20100101")
        end_date = _normalize_date_text(args.end_date or today_text)
        all_values = _get_trade_dates(tushare_client, start_date=start_date, end_date=end_date)
    elif args.dimension_type == "period":
        start_period = _normalize_date_text(args.start_date or "20100331")
        end_period = _normalize_date_text(args.end_date or today_text)
        all_values = _get_report_periods(start_period=start_period, end_period=end_period)
    else:
        raise StructuredETLError(
            "Unsupported dimension type",
            {"dimension_type": args.dimension_type},
        )

    if not all_values:
        return []

    if args.sync_all:
        synced = _list_synced_dimensions(con, status_backend, source_table, args.dimension_type)
        return [v for v in all_values if v not in synced]

    return all_values


def _parse_params_object(params: Any) -> Dict[str, object]:
    if params is None or params == "":
        return {}
    if isinstance(params, dict):
        return dict(params)
    if isinstance(params, str):
        parsed = json.loads(params)
        if not isinstance(parsed, dict):
            raise StructuredETLError("--params must be a JSON object", {"params": params})
        return parsed
    raise StructuredETLError(
        "--params must be a JSON object",
        {"params_type": type(params).__name__},
    )


def _build_query_kwargs(args: argparse.Namespace, dimension_value: str) -> Dict[str, object]:
    kwargs: Dict[str, object] = _parse_params_object(args.params)

    if args.dimension_type != "none":
        field_name = args.dimension_field or args.dimension_type
        kwargs[field_name] = dimension_value
    return kwargs


def _allow_empty_result(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "allow_empty_result", False))


def _ensure_expected_rows(
    df: pd.DataFrame,
    source_table: str,
    target_table: str,
    dimension_value: str,
    args: argparse.Namespace,
) -> None:
    if args.dimension_type == "none" or _allow_empty_result(args) or not df.empty:
        return

    raise StructuredETLError(
        (
            "Empty payload returned from Tushare for incremental sync; "
            "not marking as successful. Retry later or use --allow-empty-result if this is expected."
        ),
        {
            "source_table": source_table,
            "target_table": target_table,
            "dimension_type": args.dimension_type,
            "dimension_value": dimension_value,
        },
    )


def _fetch_with_retry(
    tushare_client,
    source_table: str,
    dimension_value: str,
    args: argparse.Namespace,
) -> pd.DataFrame:
    last_exception: Optional[Exception] = None
    for attempt in range(1, int(args.max_retries) + 1):
        try:
            kwargs = _build_query_kwargs(args, dimension_value)
            method_name = args.method or "query"
            if method_name == "query":
                df = tushare_client.query(args.endpoint, **kwargs)
            else:
                if not hasattr(tushare_client, method_name):
                    raise StructuredETLError(
                        "Unsupported tushare method",
                        {"method": method_name},
                    )
                method = getattr(tushare_client, method_name)
                df = method(**kwargs)
            if not isinstance(df, pd.DataFrame):
                raise StructuredETLError(
                    "Tushare result is not a pandas.DataFrame",
                    {"endpoint": args.endpoint, "method": method_name},
                )
            if args.limit is not None and args.limit > 0:
                return df.head(int(args.limit)).copy()
            return df.copy()
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            last_exception = exc
            if attempt == int(args.max_retries):
                break
            sleep_seconds = float(args.base_sleep_seconds) * attempt
            logger.warning(
                f"Fetch failed for source_table={source_table}, dimension={dimension_value}, "
                f"attempt={attempt}/{args.max_retries}, retry in {sleep_seconds:.2f}s: {exc}"
            )
            time.sleep(sleep_seconds)

    raise StructuredETLError(
        "Fetch failed after retries",
        {
            "source_table": source_table,
            "dimension_type": args.dimension_type,
            "dimension_value": dimension_value,
            "error": str(last_exception),
        },
    )


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # 将 pandas 缺失值统一为 None，避免写入 DuckDB 时 dtype 漂移。
    return df.where(pd.notna(df), None)


def _target_exists(con: duckdb.DuckDBPyConnection, schema: Optional[str], table: str) -> bool:
    if schema:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table],
        ).fetchone()
    else:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = current_schema() AND table_name = ?
            """,
            [table],
        ).fetchone()
    return bool(row and int(row[0]) > 0)


def _get_existing_columns(
    con: duckdb.DuckDBPyConnection,
    schema: Optional[str],
    table: str,
) -> List[str]:
    if schema:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table],
        ).fetchall()
    return [str(r[0]) for r in rows]


def _get_pk_columns(
    con: duckdb.DuckDBPyConnection,
    schema: Optional[str],
    table: str,
) -> List[str]:
    identifier = f"{schema}.{table}" if schema else table
    rows = con.execute(f"PRAGMA table_info('{identifier}')").fetchall()
    return [str(row[1]) for row in rows if int(row[5]) > 0]


def _fill_derived_columns(df: pd.DataFrame, existing_cols: List[str]) -> pd.DataFrame:
    if "ann_date_key" in existing_cols and "ann_date" in df.columns:
        df["ann_date_key"] = df.get("ann_date_key")
        df["ann_date_key"] = df["ann_date_key"].where(pd.notna(df["ann_date_key"]), df["ann_date"])
        if "end_date" in df.columns:
            df["ann_date_key"] = df["ann_date_key"].where(pd.notna(df["ann_date_key"]), df["end_date"])
    return df


def _dedupe_by_pk(
    df: pd.DataFrame,
    pk_cols: List[str],
    target_table: str,
) -> pd.DataFrame:
    if not pk_cols or any(col not in df.columns for col in pk_cols):
        return df

    sort_cols = [col for col in ["ann_date", "f_ann_date", "end_date", "update_flag"] if col in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols, kind="stable")

    before = len(df)
    deduped = df.drop_duplicates(subset=pk_cols, keep="last")
    dropped = before - len(deduped)
    if dropped > 0:
        logger.warning(
            f"Drop duplicate rows before insert: target_table={target_table}, pk={pk_cols}, rows={dropped}"
        )
    return deduped


def _get_date_columns(
    con: duckdb.DuckDBPyConnection,
    schema: Optional[str],
    table: str,
) -> set[str]:
    """返回目标表中 data_type 为 DATE 的列名集合。"""
    if schema:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ? AND data_type = 'DATE'
            """,
            [schema, table],
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = ? AND data_type = 'DATE'
            """,
            [table],
        ).fetchall()
    return {str(r[0]) for r in rows}


def _coerce_date_columns(df: pd.DataFrame, date_cols: set[str]) -> pd.DataFrame:
    """将 DataFrame 中匹配 date_cols 的 VARCHAR YYYYMMDD 列转为 datetime64，便于 DuckDB 写入 DATE 列。"""
    if not date_cols:
        return df
    for col in date_cols:
        if col not in df.columns:
            continue
        series = df[col]
        if series.dropna().empty:
            continue
        # 尝试 YYYYMMDD 格式
        converted = pd.to_datetime(series, format="%Y%m%d", errors="coerce")
        # 如果 coerce 全为 NaT，可能是 YYYY-MM-DD 格式
        if converted.notna().sum() == 0:
            converted = pd.to_datetime(series, errors="coerce")
        df[col] = converted
    return df


def _write_df_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    target_table: str,
    mode: str,
    first_batch: bool,
) -> Tuple[int, int]:
    if df.empty:
        schema_name, table_name = _parse_table_name(target_table)
        fq_name = _qualified_name(schema_name, table_name)
        if not _target_exists(con, schema_name, table_name):
            return 0, 0
        row = con.execute(f"SELECT COUNT(*) FROM {fq_name}").fetchone()
        return (int(row[0]) if row else 0), 0

    normalized_df = _normalize_dataframe(df)
    loaded_rows = int(len(normalized_df))
    schema_name, table_name = _parse_table_name(target_table)
    fq_name = _qualified_name(schema_name, table_name)

    if schema_name:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')

    if mode == "overwrite" and first_batch:
        con.execute(f"DROP TABLE IF EXISTS {fq_name}")

    table_exists = _target_exists(con, schema_name, table_name)

    if not table_exists:
        con.register("batch_df", normalized_df)
        con.execute(f"CREATE TABLE {fq_name} AS SELECT * FROM batch_df")
        con.unregister("batch_df")
    else:
        existing_cols = _get_existing_columns(con, schema_name, table_name)
        pk_cols = _get_pk_columns(con, schema_name, table_name)
        extra_cols = [c for c in normalized_df.columns if c not in existing_cols]
        if extra_cols:
            logger.warning(
                f"Drop unexpected columns before insert: target_table={target_table}, cols={extra_cols}"
            )
        aligned_df = normalized_df.reindex(columns=existing_cols)
        aligned_df = _fill_derived_columns(aligned_df, existing_cols)
        aligned_df = _dedupe_by_pk(aligned_df, pk_cols, target_table)
        # 将 YYYYMMDD 字符串转为 datetime，匹配目标表 DATE 列
        date_cols = _get_date_columns(con, schema_name, table_name)
        if date_cols:
            aligned_df = _coerce_date_columns(aligned_df.copy(), date_cols)
        con.register("batch_df", aligned_df)
        con.execute(f"INSERT INTO {fq_name} SELECT * FROM batch_df")
        con.unregister("batch_df")

    row = con.execute(f"SELECT COUNT(*) FROM {fq_name}").fetchone()
    total_rows = int(row[0]) if row else loaded_rows
    return total_rows, loaded_rows


def _iter_dimensions(dimension_values: List[str]) -> Iterable[str]:
    for value in dimension_values:
        yield value


def run_etl(args: argparse.Namespace) -> Dict[str, object]:
    source_table = args.source_table or args.endpoint
    target_table = args.target_table or source_table
    duckdb_path = Path(args.duckdb_path).expanduser().resolve()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    tushare_client = get_tushare_client()
    with duckdb.connect(str(duckdb_path)) as con:
        status_backend = _resolve_status_backend(con)
        dimension_values = _resolve_dimension_values(
            con=con,
            status_backend=status_backend,
            tushare_client=tushare_client,
            source_table=source_table,
            args=args,
        )

        if not dimension_values:
            result = {
                "duckdb_path": str(duckdb_path),
                "source_table": source_table,
                "target_table": target_table,
                "dimension_type": args.dimension_type,
                "processed": 0,
                "loaded_rows": 0,
                "target_total_rows": 0,
            }
            _log_event("etl_completed", result)
            return result

        _log_event(
            "task_started",
            {
                "source_table": source_table,
                "target_table": target_table,
                "dimension_type": args.dimension_type,
                "dimension_count": len(dimension_values),
                "mode": args.mode,
            },
        )

        processed = 0
        total_loaded = 0
        total_rows_after_write = 0
        first_batch = True

        for dimension_value in _iter_dimensions(dimension_values):
            try:
                status_dimension_value = dimension_value
                if args.dimension_type == "none":
                    status_dimension_value = datetime.now().strftime("%Y%m%d")

                df = _fetch_with_retry(
                    tushare_client=tushare_client,
                    source_table=source_table,
                    dimension_value=dimension_value,
                    args=args,
                )
                _ensure_expected_rows(
                    df=df,
                    source_table=source_table,
                    target_table=target_table,
                    dimension_value=dimension_value,
                    args=args,
                )
                total_rows_after_write, loaded_rows = _write_df_to_duckdb(
                    con=con,
                    df=df,
                    target_table=target_table,
                    mode=args.mode,
                    first_batch=first_batch,
                )
                first_batch = False
                processed += 1
                total_loaded += loaded_rows

                _write_sync_status(
                    con=con,
                    status_backend=status_backend,
                    source_table=source_table,
                    dimension_type=args.dimension_type,
                    dimension_value=status_dimension_value,
                    is_sync=1,
                )

                _log_event(
                    "dimension_loaded",
                    {
                        "source_table": source_table,
                        "dimension_type": args.dimension_type,
                        "dimension_value": dimension_value,
                        "loaded_rows": loaded_rows,
                        "target_total_rows": total_rows_after_write,
                    },
                )

                if args.sleep_seconds and args.sleep_seconds > 0:
                    time.sleep(float(args.sleep_seconds))

            except Exception as exc:
                status_dimension_value = dimension_value
                if args.dimension_type == "none":
                    status_dimension_value = datetime.now().strftime("%Y%m%d")

                _write_sync_status(
                    con=con,
                    status_backend=status_backend,
                    source_table=source_table,
                    dimension_type=args.dimension_type,
                    dimension_value=status_dimension_value,
                    is_sync=0,
                    error_message=str(exc),
                )

                logger.error(
                    f"Failed for source_table={source_table}, "
                    f"dimension_type={args.dimension_type}, dimension_value={dimension_value}: {exc}"
                )
                if args.sync_all:
                    continue
                raise

    result = {
        "duckdb_path": str(duckdb_path),
        "source_table": source_table,
        "target_table": target_table,
        "dimension_type": args.dimension_type,
        "processed": processed,
        "loaded_rows": total_loaded,
        "target_total_rows": total_rows_after_write,
        "mode": args.mode,
    }
    _log_event("etl_completed", result)
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy Tushare data to local DuckDB by Pandas")
    parser.add_argument("--endpoint", required=True, help="Tushare endpoint name, e.g. stock_basic")
    parser.add_argument(
        "--method",
        default="query",
        help="Tushare client method name. Use query for tushare_client.query(endpoint, **kwargs)",
    )
    parser.add_argument("--source-table", default=None, help="Logical source table name for sync status")
    parser.add_argument("--duckdb-path", required=True, help="Local DuckDB file path")
    parser.add_argument(
        "--target-table",
        default=None,
        help="DuckDB table name (table or schema.table). Defaults to --source-table/--endpoint",
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Write mode for target table",
    )
    parser.add_argument(
        "--dimension-type",
        default="none",
        choices=["none", "trade_date", "period"],
        help="Sync dimension. none means full pull once",
    )
    parser.add_argument(
        "--dimension-field",
        default=None,
        help="Override dimension parameter field name in Tushare API call",
    )
    parser.add_argument("--start-date", default=None, help="Start date/period, format YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date/period, format YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--sync-all", action="store_true", help="Skip already-synced dimensions and continue on failure")
    parser.add_argument("--params", default=None, help="Extra Tushare kwargs in JSON object string")
    parser.add_argument("--limit", type=int, default=None, help="Optional row cap per API call")
    parser.add_argument("--max-retries", type=int, default=3, help="Fetch retries per dimension")
    parser.add_argument("--base-sleep-seconds", type=float, default=2.0, help="Retry backoff base seconds")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Sleep seconds between successful calls")
    parser.add_argument(
        "--allow-empty-result",
        action="store_true",
        help=(
            "Treat empty payload as successful. By default, empty payloads for incremental dimensions "
            "are marked failed so they can be retried later."
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        run_etl(args)
    except StructuredETLError as exc:
        _log_event("etl_failed", exc.to_dict())
        raise
    except Exception as exc:
        _log_event("etl_failed", {"error": str(exc)})
        raise


if __name__ == "__main__":
    main()

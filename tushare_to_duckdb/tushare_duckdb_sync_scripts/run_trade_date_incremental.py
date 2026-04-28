#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from tushare_to_duckdb.tushare_duckdb_sync_scripts.common import (  # noqa: E402
    bootstrap_project_path,
    configure_logging,
    ensure_tushare_token,
    file_lock,
    filter_tasks,
    load_registry,
    log_event,
    parse_table_filters,
    resolve_duckdb_path,
    resolve_log_dir,
    resolve_trade_window,
)
from tushare_to_duckdb.tushare_duckdb_sync_scripts.runner import BatchSummary, execute_etl_task, namespace_to_payload  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run trade_date incremental sync tasks for DuckDB tables")
    parser.add_argument("--date", default=None, help="Single trading date in YYYYMMDD")
    parser.add_argument("--start-date", default=None, help="Window start date in YYYYMMDD")
    parser.add_argument("--end-date", default=None, help="Window end date in YYYYMMDD")
    parser.add_argument("--lookback-days", type=int, default=7, help="Default window size when start-date is absent")
    parser.add_argument("--tables", default=None, help="Comma-separated target_table/source_table filters")
    parser.add_argument("--duckdb-path", default=None, help="Override DuckDB file path")
    parser.add_argument("--log-dir", default=None, help="Override log directory")
    parser.add_argument("--dry-run", action="store_true", help="Print resolved tasks without writing data")
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Continue after individual task failures",
    )
    return parser


def run_with_args(args: argparse.Namespace) -> BatchSummary:
    bootstrap_project_path()
    duckdb_path = resolve_duckdb_path(args.duckdb_path)
    log_dir = resolve_log_dir(args.log_dir)
    configure_logging("trade_date_incremental", log_dir)

    window_start, window_end = resolve_trade_window(
        date_text=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=args.lookback_days,
        now=datetime.now(),
    )
    tasks = filter_tasks(
        load_registry(),
        dimension_types=["trade_date"],
        targets=parse_table_filters(args.tables),
    )
    if not tasks:
        raise RuntimeError("No trade_date tasks matched the requested filters")

    summary = BatchSummary(batch_name="trade_date_incremental")
    log_event(
        "trade_date_batch_started",
        {
            "task_count": len(tasks),
            "start_date": window_start,
            "end_date": window_end,
            "duckdb_path": str(duckdb_path),
            "dry_run": bool(args.dry_run),
        },
    )

    with file_lock("trade_date_incremental"):
        if not args.dry_run:
            ensure_tushare_token()

        for task in tasks:
            etl_args = task.to_etl_args(
                duckdb_path,
                start_date=window_start,
                end_date=window_end,
                sync_all=True,
                mode="append",
            )
            if args.dry_run:
                summary.planned.append(namespace_to_payload(etl_args))
                log_event("trade_date_task_planned", summary.planned[-1])
                continue
            try:
                result = execute_etl_task(
                    task,
                    etl_args,
                    batch_name=summary.batch_name,
                    task_label=f"{task.target_table}:{window_start}-{window_end}",
                )
                summary.successes.append(result)
            except Exception as exc:
                failure = {
                    "source_table": task.source_table,
                    "target_table": task.target_table,
                    "start_date": window_start,
                    "end_date": window_end,
                    "error": str(exc),
                }
                summary.failures.append(failure)
                log_event("trade_date_task_failed", failure)
                if not args.continue_on_error:
                    break

    log_event("trade_date_batch_completed", summary.to_payload())
    return summary


def main() -> int:
    summary = run_with_args(build_parser().parse_args())
    return summary.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

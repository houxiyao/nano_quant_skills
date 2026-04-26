#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tushare_sync_scripts.common import (  # noqa: E402
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
)
from tushare_sync_scripts.runner import BatchSummary, execute_etl_task, namespace_to_payload  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh full snapshot DuckDB tables from Tushare")
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
    configure_logging("snapshot_refresh", log_dir)

    tasks = filter_tasks(
        load_registry(),
        dimension_types=["none"],
        targets=parse_table_filters(args.tables),
    )
    if not tasks:
        raise RuntimeError("No snapshot tasks matched the requested filters")

    summary = BatchSummary(batch_name="snapshot_refresh")
    log_event(
        "snapshot_batch_started",
        {
            "task_count": len(tasks),
            "duckdb_path": str(duckdb_path),
            "dry_run": bool(args.dry_run),
        },
    )

    with file_lock("snapshot_refresh"):
        if not args.dry_run:
            ensure_tushare_token()

        for task in tasks:
            etl_args = task.to_etl_args(duckdb_path, sync_all=False, mode=task.mode)
            if args.dry_run:
                summary.planned.append(namespace_to_payload(etl_args))
                log_event("snapshot_task_planned", summary.planned[-1])
                continue
            try:
                result = execute_etl_task(
                    task,
                    etl_args,
                    batch_name=summary.batch_name,
                    task_label=task.target_table,
                )
                summary.successes.append(result)
            except Exception as exc:
                failure = {
                    "source_table": task.source_table,
                    "target_table": task.target_table,
                    "error": str(exc),
                }
                summary.failures.append(failure)
                log_event("snapshot_task_failed", failure)
                if not args.continue_on_error:
                    break

    log_event("snapshot_batch_completed", summary.to_payload())
    return summary


def main() -> int:
    summary = run_with_args(build_parser().parse_args())
    return summary.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

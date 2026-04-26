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
    resolve_recent_report_periods,
)
from tushare_sync_scripts.runner import BatchSummary, execute_etl_task, namespace_to_payload, reset_period_state  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Always overwrite the latest financial report periods")
    parser.add_argument("--as-of-date", default=None, help="Reference date in YYYYMMDD when resolving latest report periods")
    parser.add_argument("--period-count", type=int, default=2, help="How many recent report periods to overwrite")
    parser.add_argument("--periods", default=None, help="Comma-separated explicit periods in YYYYMMDD")
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


def _resolve_periods(args: argparse.Namespace) -> list[str]:
    if args.periods:
        values = [item.strip() for item in args.periods.split(",") if item.strip()]
        if not values:
            raise RuntimeError("--periods was provided but no valid period was parsed")
        return values
    return resolve_recent_report_periods(args.as_of_date, args.period_count)


def run_with_args(args: argparse.Namespace) -> BatchSummary:
    bootstrap_project_path()
    duckdb_path = resolve_duckdb_path(args.duckdb_path)
    log_dir = resolve_log_dir(args.log_dir)
    configure_logging("financial_period_overwrite", log_dir)

    periods = _resolve_periods(args)
    tasks = filter_tasks(
        load_registry(),
        dimension_types=["period"],
        targets=parse_table_filters(args.tables),
    )
    if not tasks:
        raise RuntimeError("No financial period tasks matched the requested filters")

    summary = BatchSummary(batch_name="financial_period_overwrite")
    log_event(
        "financial_batch_started",
        {
            "task_count": len(tasks),
            "periods": periods,
            "duckdb_path": str(duckdb_path),
            "dry_run": bool(args.dry_run),
        },
    )

    with file_lock("financial_period_overwrite"):
        if not args.dry_run:
            ensure_tushare_token()

        for task in tasks:
            if args.dry_run:
                summary.planned.append(
                    {
                        "source_table": task.source_table,
                        "target_table": task.target_table,
                        "periods": periods,
                    }
                )
                for period in periods:
                    preview_args = task.to_etl_args(
                        duckdb_path,
                        start_date=period,
                        end_date=period,
                        sync_all=False,
                        mode="append",
                    )
                    log_event("financial_task_planned", namespace_to_payload(preview_args))
                continue

            reset_period_state(duckdb_path, task, periods)
            for period in periods:
                etl_args = task.to_etl_args(
                    duckdb_path,
                    start_date=period,
                    end_date=period,
                    sync_all=False,
                    mode="append",
                )
                try:
                    result = execute_etl_task(
                        task,
                        etl_args,
                        batch_name=summary.batch_name,
                        task_label=f"{task.target_table}:{period}",
                    )
                    summary.successes.append(result)
                except Exception as exc:
                    failure = {
                        "source_table": task.source_table,
                        "target_table": task.target_table,
                        "period": period,
                        "error": str(exc),
                    }
                    summary.failures.append(failure)
                    log_event("financial_task_failed", failure)
                    if not args.continue_on_error:
                        break
            if summary.failures and not args.continue_on_error:
                break

    log_event("financial_batch_completed", summary.to_payload())
    return summary


def main() -> int:
    summary = run_with_args(build_parser().parse_args())
    return summary.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
from typing import List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tushare_sync_scripts.common import configure_logging, log_event, resolve_log_dir  # noqa: E402


GROUP_TO_SCRIPT = {
    "trade-date": PROJECT_ROOT / "tushare_sync_scripts" / "run_trade_date_incremental.py",
    "financial": PROJECT_ROOT / "tushare_sync_scripts" / "run_financial_period_overwrite.py",
    "snapshot": PROJECT_ROOT / "tushare_sync_scripts" / "run_snapshot_refresh.py",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full Tushare -> DuckDB cron suite")
    parser.add_argument(
        "--groups",
        default="trade-date,financial,snapshot",
        help="Comma-separated groups to run: trade-date,financial,snapshot",
    )
    parser.add_argument("--duckdb-path", default=None, help="Override DuckDB file path")
    parser.add_argument("--log-dir", default=None, help="Override log directory")
    parser.add_argument("--tables", default=None, help="Optional table filter forwarded to group scripts")
    parser.add_argument("--dry-run", action="store_true", help="Preview resolved tasks without writing data")
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Continue with remaining groups after one group fails",
    )
    parser.add_argument("--date", default=None, help="Forwarded only to trade-date group")
    parser.add_argument("--start-date", default=None, help="Forwarded only to trade-date group")
    parser.add_argument("--end-date", default=None, help="Forwarded only to trade-date group")
    parser.add_argument("--lookback-days", type=int, default=7, help="Forwarded only to trade-date group")
    parser.add_argument("--as-of-date", default=None, help="Forwarded only to financial group")
    parser.add_argument("--period-count", type=int, default=2, help="Forwarded only to financial group")
    parser.add_argument("--periods", default=None, help="Forwarded only to financial group")
    return parser


def _append_optional(command: List[str], flag: str, value: object) -> None:
    if value is None:
        return
    if isinstance(value, bool):
        if value:
            command.append(flag)
        return
    text = str(value)
    if text:
        command.extend([flag, text])


def run_with_args(args: argparse.Namespace) -> int:
    log_dir = resolve_log_dir(args.log_dir)
    configure_logging("cron_suite", log_dir)

    selected_groups = [item.strip() for item in args.groups.split(",") if item.strip()]
    if not selected_groups:
        raise RuntimeError("No valid groups were selected")

    exit_code = 0
    for group in selected_groups:
        script_path = GROUP_TO_SCRIPT.get(group)
        if script_path is None:
            raise RuntimeError(f"Unsupported group: {group}")
        command = [sys.executable, str(script_path)]
        _append_optional(command, "--duckdb-path", args.duckdb_path)
        _append_optional(command, "--log-dir", args.log_dir)
        _append_optional(command, "--tables", args.tables)
        _append_optional(command, "--dry-run", args.dry_run)
        command.append("--continue-on-error" if args.continue_on_error else "--no-continue-on-error")

        if group == "trade-date":
            _append_optional(command, "--date", args.date)
            _append_optional(command, "--start-date", args.start_date)
            _append_optional(command, "--end-date", args.end_date)
            command.extend(["--lookback-days", str(args.lookback_days)])
        elif group == "financial":
            _append_optional(command, "--as-of-date", args.as_of_date)
            command.extend(["--period-count", str(args.period_count)])
            _append_optional(command, "--periods", args.periods)

        log_event("cron_group_started", {"group": group, "command": command})
        completed = subprocess.run(command, check=False)
        log_event(
            "cron_group_completed",
            {"group": group, "returncode": completed.returncode},
        )
        if completed.returncode != 0:
            exit_code = completed.returncode
            if not args.continue_on_error:
                break

    log_event("cron_suite_completed", {"groups": selected_groups, "returncode": exit_code})
    return exit_code


def main() -> int:
    return run_with_args(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())

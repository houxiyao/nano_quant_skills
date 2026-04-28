#!/usr/bin/env python3
"""运行 index_weight（指数成分和权重）的增量同步。

类似 run_trade_date_incremental.py，但针对 index_code 维度。

使用方式：
    python run_index_weight_sync.py \\
        --start-date 20240101 \\
        --end-date 20240131
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import sys

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from tushare_to_duckdb.tushare_duckdb_sync_scripts.common import (
    bootstrap_project_path,
    configure_logging,
    ensure_tushare_token,
    file_lock,
    resolve_duckdb_path,
    resolve_log_dir,
)
from tushare_to_duckdb.tushare_duckdb_sync_scripts.etl.sync_by_index_code import main as run_sync_by_index_code
from loguru import logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run index_weight incremental sync for DuckDB"
    )
    parser.add_argument("--start-date", default=None, help="Start date YYYYMMDD")
    parser.add_argument("--end-date", default=None, help="End date YYYYMMDD")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Default window when start-date is absent"
    )
    parser.add_argument("--duckdb-path", default=None, help="Override DuckDB path")
    parser.add_argument("--log-dir", default=None, help="Override log directory")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between API calls")
    parser.add_argument("--sync-all", action="store_true", help="Re-sync all index codes")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    return parser


def resolve_date_range(
    start_date: str | None,
    end_date: str | None,
    lookback_days: int,
    now: datetime,
) -> tuple[str, str]:
    """Resolve start and end dates"""
    if start_date and end_date:
        return start_date, end_date
    
    if start_date:
        return start_date, end_date or now.strftime("%Y%m%d")
    
    # Default: lookback_days from today
    end = end_date or now.strftime("%Y%m%d")
    start = (now - timedelta(days=lookback_days)).strftime("%Y%m%d")
    return start, end


def main():
    args = build_parser().parse_args()
    
    bootstrap_project_path()
    duckdb_path = resolve_duckdb_path(args.duckdb_path)
    log_dir = resolve_log_dir(args.log_dir)
    configure_logging("index_weight_sync", log_dir)
    
    # Verify token
    token = ensure_tushare_token()
    logger.info(f"✅ Tushare token verified")
    
    # Resolve dates
    start_date, end_date = resolve_date_range(
        args.start_date,
        args.end_date,
        args.lookback_days,
        datetime.now(),
    )
    
    logger.info(f"📅 Sync window: {start_date} to {end_date}")
    logger.info(f"📦 DuckDB: {duckdb_path}")
    
    if args.dry_run:
        logger.info("🏃 [DRY-RUN] Would execute sync_by_index_code with:")
        logger.info(f"  --start-date {start_date}")
        logger.info(f"  --end-date {end_date}")
        logger.info(f"  --batch-size {args.batch_size}")
        logger.info(f"  --sleep {args.sleep}")
        if args.sync_all:
            logger.info(f"  --sync-all")
        return 0
    
    # Execute sync
    with file_lock("index_weight_sync", lock_dir=log_dir):
        logger.info("🚀 Starting index_weight sync...")
        
        sys.argv = [
            "sync_by_index_code.py",
            "--endpoint", "index_weight",
            "--target-table", "idx_weight",
            "--duckdb-path", str(duckdb_path),
            "--start-date", start_date,
            "--end-date", end_date,
            "--batch-size", str(args.batch_size),
            "--sleep", str(args.sleep),
        ]
        
        if args.sync_all:
            sys.argv.append("--sync-all")
        
        try:
            exit_code = run_sync_by_index_code()
            if exit_code == 0:
                logger.info("✅ index_weight sync completed successfully")
            else:
                logger.error(f"❌ index_weight sync failed with code {exit_code}")
            return exit_code
        except Exception as e:
            logger.error(f"❌ Error during sync: {e}", exc_info=True)
            return 1


if __name__ == "__main__":
    sys.exit(main())

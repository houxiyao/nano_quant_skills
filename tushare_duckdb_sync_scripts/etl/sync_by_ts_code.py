"""Batch 3/4 - 按 ts_code 循环的 Tushare 同步（独立脚本）。

适用接口（必须传 ts_code 的）:
  - fina_mainbz    (每家 × 3 type: P/D/I)
  - stk_managers
  - stk_rewards
  - pledge_stat
  - pledge_detail

职责：
  - 从 stk_info 读全市场 ts_code
  - 对每个 ts_code 调 Tushare API，累积 DataFrame，按批次（每 N 家）flush 到 DuckDB
  - 在 table_sync_state 中以 dimension_type='ts_code' + dimension_value=ts_code 记录进度
  - 支持断点续传：重跑时跳过已成功的 ts_code
  - 空 payload 记成功（很多公司确实没有质押/解禁/主营拆分）
  - 失败的 ts_code 记失败，下次重跑

用法::

    TUSHARE_TOKEN=xxx python sync_by_ts_code.py \\
        --endpoint stk_managers \\
        --target-table stk_managers \\
        --duckdb-path data/ashare.duckdb \\
        [--extra-param-sets '[{"type":"P"},{"type":"D"},{"type":"I"}]'] \\
        [--batch-size 100] [--sleep 0.1]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger


def _get_tushare():
    import tushare as ts
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        sys.exit("TUSHARE_TOKEN not set")
    return ts.pro_api(token=token)


def _ensure_sync_state(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS table_sync_state (
            source_table VARCHAR,
            dimension_type VARCHAR,
            dimension_value VARCHAR,
            is_sync INTEGER,
            error_message VARCHAR,
            updated_at TIMESTAMP
        )
    """)


def _load_ts_codes(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        "SELECT ts_code FROM stk_info ORDER BY ts_code"
    ).fetchall()
    return [r[0] for r in rows]


def _load_synced(con: duckdb.DuckDBPyConnection, source_table: str) -> set[str]:
    rows = con.execute(
        """SELECT dimension_value FROM table_sync_state
           WHERE source_table = ? AND dimension_type = 'ts_code' AND is_sync = 1""",
        [source_table],
    ).fetchall()
    return {r[0] for r in rows}


def _write_state(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    ts_code: str,
    success: bool,
    err: str = "",
) -> None:
    con.execute(
        """DELETE FROM table_sync_state
           WHERE source_table = ? AND dimension_type = 'ts_code'
             AND dimension_value = ?""",
        [source_table, ts_code],
    )
    con.execute(
        """INSERT INTO table_sync_state
           (source_table, dimension_type, dimension_value, is_sync, error_message, updated_at)
           VALUES (?, 'ts_code', ?, ?, ?, ?)""",
        [source_table, ts_code, 1 if success else 0, err, datetime.now()],
    )


def _flush(
    con: duckdb.DuckDBPyConnection,
    target_table: str,
    buffer: list[pd.DataFrame],
    first_flush: dict,
) -> int:
    if not buffer:
        return 0
    df = pd.concat(buffer, ignore_index=True)
    if df.empty:
        return 0
    if first_flush["value"]:
        # 建表 as select
        con.register("df_tmp", df)
        con.execute(f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM df_tmp LIMIT 0")
        con.execute(f"INSERT INTO {target_table} SELECT * FROM df_tmp")
        con.unregister("df_tmp")
        first_flush["value"] = False
    else:
        con.register("df_tmp", df)
        # 插入前对齐列顺序
        target_cols = [r[1] for r in con.execute(f"PRAGMA table_info('{target_table}')").fetchall()]
        existing_cols = [c for c in target_cols if c in df.columns]
        cols_list = ", ".join(existing_cols)
        con.execute(f"INSERT INTO {target_table} ({cols_list}) SELECT {cols_list} FROM df_tmp")
        con.unregister("df_tmp")
    return len(df)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--target-table", required=True)
    ap.add_argument("--duckdb-path", required=True)
    ap.add_argument(
        "--extra-param-sets",
        default="[]",
        help="JSON list of param dicts to merge with ts_code, e.g. [{\"type\":\"P\"},{\"type\":\"D\"}]",
    )
    ap.add_argument("--source-table", default=None, help="defaults to endpoint")
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--max-codes", type=int, default=0, help="0=all")
    ap.add_argument("--truncate", action="store_true", help="drop target before start")
    args = ap.parse_args()

    source_table = args.source_table or args.endpoint
    extra_sets = json.loads(args.extra_param_sets)
    if not isinstance(extra_sets, list):
        sys.exit("--extra-param-sets must be a JSON list")
    if not extra_sets:
        extra_sets = [{}]

    pro = _get_tushare()
    con = duckdb.connect(args.duckdb_path)
    _ensure_sync_state(con)

    if args.truncate:
        con.execute(f"DROP TABLE IF EXISTS {args.target_table}")
        con.execute(
            "DELETE FROM table_sync_state WHERE source_table = ? AND dimension_type = 'ts_code'",
            [source_table],
        )
        logger.warning(f"Truncated {args.target_table} and its sync_state")

    all_codes = _load_ts_codes(con)
    synced = _load_synced(con, source_table)
    todo = [c for c in all_codes if c not in synced]
    if args.max_codes:
        todo = todo[: args.max_codes]

    logger.info(
        f"[{args.endpoint}] total={len(all_codes)} synced={len(synced)} todo={len(todo)} param_sets={len(extra_sets)}"
    )

    exists_target = bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
            [args.target_table],
        ).fetchone()[0]
    )
    first_flush = {"value": not exists_target}

    buffer: list[pd.DataFrame] = []
    t0 = time.time()
    total_rows = 0
    total_success = 0
    total_failed = 0

    for i, ts_code in enumerate(todo, 1):
        ok = True
        err = ""
        try:
            for ps in extra_sets:
                params = {"ts_code": ts_code, **ps}
                df = pro.query(args.endpoint, **params)
                if df is not None and len(df) > 0:
                    # 把 extra param (e.g. type=P/D/I) 注入为列，便于合并表后区分来源
                    for k, v in ps.items():
                        col = f"_param_{k}"
                        if col not in df.columns:
                            df[col] = v
                    buffer.append(df)
            total_success += 1
        except Exception as exc:  # noqa: BLE001
            ok = False
            err = str(exc)[:500]
            total_failed += 1
            logger.warning(f"[{args.endpoint}] {ts_code} failed: {err}")

        _write_state(con, source_table, ts_code, ok, err)

        if i % args.batch_size == 0:
            n = _flush(con, args.target_table, buffer, first_flush)
            total_rows += n
            buffer.clear()
            dt = time.time() - t0
            logger.info(
                f"[{args.endpoint}] progress={i}/{len(todo)} rows+={n} total_rows={total_rows} "
                f"success={total_success} failed={total_failed} elapsed={dt:.0f}s rate={i/dt:.1f}/s"
            )

        time.sleep(args.sleep)

    # final flush
    n = _flush(con, args.target_table, buffer, first_flush)
    total_rows += n
    dt = time.time() - t0
    logger.info(
        f"[{args.endpoint}] DONE rows={total_rows} success={total_success} failed={total_failed} "
        f"elapsed={dt:.0f}s"
    )
    con.close()
    return 0 if total_failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

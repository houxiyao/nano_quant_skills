"""structured_evidence_probes.py — DuckDB 结构化证据探针。

每个 probe 返回 `(rows, Evidence)`：
- rows: 原始查询结果（dict 列表，供上游做评级逻辑）
- Evidence: 以 `duckdb://table?q=...` 为 source_url，excerpt 是关键字段摘要

所有连接必须 read_only；失败时返回 `([], None)`。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

try:
    from .eight_questions_domain import Evidence, SourceType, now_iso
except ImportError:
    from eight_questions_domain import Evidence, SourceType, now_iso


logger = logging.getLogger(__name__)


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    raw_map = os.environ.get("SEVEN_LOOK_DB_TABLE_MAP", "")
    table_map: dict[str, str] = {}
    if raw_map:
        try:
            loaded = json.loads(raw_map)
            if isinstance(loaded, dict):
                table_map = {str(k): str(v) for k, v in loaded.items() if k != v}
        except Exception:  # noqa: BLE001
            pass

    if not table_map:
        return duckdb.connect(str(db_path), read_only=True)

    def _quote_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    con = duckdb.connect(":memory:")
    safe_path = str(db_path).replace("'", "''")
    con.execute(f"ATTACH '{safe_path}' AS physdb (READ_ONLY)")
    phys_tables = {
        str(r[0])
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_catalog = 'physdb' AND table_schema = 'main'"
        ).fetchall()
    }

    for phys_name in sorted(phys_tables):
        ident = _quote_ident(phys_name)
        con.execute(f"CREATE VIEW {ident} AS SELECT * FROM physdb.{ident}")

    for logical_name, physical_name in table_map.items():
        if physical_name not in phys_tables:
            continue
        logical_ident = _quote_ident(logical_name)
        physical_ident = _quote_ident(physical_name)
        con.execute(
            f"CREATE OR REPLACE VIEW {logical_ident} AS SELECT * FROM physdb.{physical_ident}"
        )
    return con


def _rows_to_dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]


def _mk_evidence(
    table: str, query_tag: str, excerpt: str
) -> Evidence | None:
    if not excerpt.strip():
        return None
    return Evidence(
        source_type=SourceType.DB,
        source_url=f"duckdb://{table}?{query_tag}",
        retrieved_at=now_iso(),
        excerpt=excerpt[:600],
        title=f"DuckDB.{table}",
    )


# ---------------------------------------------------------------------------
# Q3 管理团队
# ---------------------------------------------------------------------------


def probe_company_overview(
    con: duckdb.DuckDBPyConnection, ts_code: str
) -> tuple[dict[str, Any] | None, Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT ts_code, com_name, chairman, manager, secretary, reg_capital,
                   setup_date, province, employees, main_business, exchange
            FROM stk_company WHERE ts_code = ?
            """,
            [ts_code],
        )
    )
    if not rows:
        return None, None
    r = rows[0]
    excerpt = (
        f"{r['com_name']} | 董事长:{r['chairman']} | 总经理:{r['manager']} | "
        f"董秘:{r['secretary']} | 注册资本:{r['reg_capital']} | 员工:{r['employees']} | "
        f"主营:{(r.get('main_business') or '')[:200]}"
    )
    ev = _mk_evidence("stk_company", f"ts_code={ts_code}", excerpt)
    return r, ev


def probe_managers(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 20
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT ann_date, name, title, lev, edu, begin_date, end_date
            FROM stk_managers
            WHERE ts_code = ?
            ORDER BY COALESCE(end_date, '99999999') DESC, ann_date DESC
            LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], None
    active = [r for r in rows if not r["end_date"]]
    excerpt = f"当前在任 {len(active)} 人；代表：" + "; ".join(
        f"{r['name']}({r['title']})" for r in active[:5]
    )
    return rows, _mk_evidence("stk_managers", f"ts_code={ts_code}", excerpt)


def probe_rewards(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 10
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT ann_date, end_date, name, title, reward, hold_vol
            FROM stk_rewards WHERE ts_code = ?
            ORDER BY end_date DESC LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], None
    latest = rows[0]
    excerpt = (
        f"最新披露:{latest['end_date']} {latest['name']}({latest['title']}) "
        f"薪酬={latest['reward']} 持股={latest['hold_vol']}；共 {len(rows)} 条记录"
    )
    return rows, _mk_evidence("stk_rewards", f"ts_code={ts_code}", excerpt)


def probe_top_holders(
    con: duckdb.DuckDBPyConnection, ts_code: str
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            WITH latest AS (
                SELECT MAX(end_date) AS d FROM fin_top10_holders WHERE ts_code = ?
            )
            SELECT holder_name, hold_amount, hold_ratio, holder_type, end_date
            FROM fin_top10_holders
            WHERE ts_code = ? AND end_date = (SELECT d FROM latest)
            ORDER BY hold_ratio DESC NULLS LAST
            """,
            [ts_code, ts_code],
        )
    )
    if not rows:
        return [], None
    top_ratio = sum((r["hold_ratio"] or 0) for r in rows[:10])
    excerpt = (
        f"截止 {rows[0]['end_date']}，前十大股东合计持股 {top_ratio:.2f}%；"
        + "; ".join(
            f"{r['holder_name']}({r['hold_ratio']:.2f}%)" for r in rows[:3] if r["hold_ratio"]
        )
    )
    return rows, _mk_evidence("fin_top10_holders", f"ts_code={ts_code}", excerpt)


# ---------------------------------------------------------------------------
# Q4 财务真实性
# ---------------------------------------------------------------------------


def probe_name_history(
    con: duckdb.DuckDBPyConnection, ts_code: str
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT name, start_date, end_date, ann_date, change_reason
            FROM stk_name_history WHERE ts_code = ?
            ORDER BY start_date DESC
            """,
            [ts_code],
        )
    )
    if not rows:
        return [], None
    excerpt = f"共 {len(rows)} 次更名；最近一次：{rows[0]['start_date']} → {rows[0]['name']} ({rows[0].get('change_reason') or ''})"
    return rows, _mk_evidence("stk_name_history", f"ts_code={ts_code}", excerpt)


def probe_cash_ratio(
    con: duckdb.DuckDBPyConnection, ts_code: str, years: int = 5
) -> tuple[list[dict[str, Any]], Evidence | None]:
    """净现比 = 经营性现金流净额 / 净利润（用 ocf_to_profit 近似）。"""
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT end_date, ocf_to_profit, salescash_to_or, ocf_to_or
            FROM fin_indicator
            WHERE ts_code = ?
              AND EXTRACT(MONTH FROM end_date) = 12
              AND EXTRACT(DAY FROM end_date) = 31
            ORDER BY end_date DESC LIMIT ?
            """,
            [ts_code, years],
        )
    )
    if not rows:
        return [], None
    values = [
        f"{r['end_date']}: 净现比(ocf/profit)={r['ocf_to_profit']}, 销售现金比(salescash/or)={r['salescash_to_or']}"
        for r in rows
    ]
    excerpt = "最近 {n} 年现金流质量 | ".format(n=len(rows)) + " | ".join(values)
    return rows, _mk_evidence("fin_indicator", f"ts_code={ts_code}&metric=cash", excerpt)


# ---------------------------------------------------------------------------
# Q5/Q6 主营业务 & 同行
# ---------------------------------------------------------------------------


def probe_mainbz(
    con: duckdb.DuckDBPyConnection, ts_code: str, years: int = 3
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            WITH latest_years AS (
                SELECT DISTINCT end_date FROM fin_mainbz WHERE ts_code = ?
                ORDER BY end_date DESC LIMIT ?
            )
            SELECT m.end_date, m._param_type, m.bz_item, m.bz_sales, m.bz_profit, m.bz_cost
            FROM fin_mainbz m
            INNER JOIN latest_years y ON m.end_date = y.end_date
            WHERE m.ts_code = ?
            ORDER BY m.end_date DESC, m.bz_sales DESC NULLS LAST
            """,
            [ts_code, years, ts_code],
        )
    )
    if not rows:
        return [], None
    latest_end = rows[0]["end_date"]
    latest_rows = [r for r in rows if r["end_date"] == latest_end]
    total = sum((r["bz_sales"] or 0) for r in latest_rows) or 1
    top = latest_rows[:3]
    excerpt = f"{latest_end} 主营构成（前3）：" + "; ".join(
        f"{r['bz_item']}({(r['bz_sales'] or 0)/total*100:.1f}%)" for r in top
    )
    return rows, _mk_evidence("fin_mainbz", f"ts_code={ts_code}", excerpt)


def probe_sw_peers(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 10
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT anchor_ts_code, anchor_name, l1_name, l2_name, l3_name,
                   peer_group_size, peer_ts_code, peer_name, peer_is_self
            FROM idx_sw_l3_peers
            WHERE anchor_ts_code = ?
              AND peer_is_self = FALSE
            ORDER BY peer_ts_code LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], None
    r0 = rows[0]
    excerpt = (
        f"申万三级分类：{r0['l1_name']}/{r0['l2_name']}/{r0['l3_name']}，"
        f"同行池 {r0['peer_group_size']} 只；样例：" + ", ".join(
            f"{r['peer_ts_code']}({r['peer_name']})" for r in rows[:5]
        )
    )
    return rows, _mk_evidence("idx_sw_l3_peers", f"anchor={ts_code}", excerpt)


# ---------------------------------------------------------------------------
# Q7 风险
# ---------------------------------------------------------------------------


def probe_pledge(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 6
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT end_date, pledge_count, pledge_ratio, total_share, rest_pledge, unrest_pledge
            FROM stk_pledge_stat WHERE ts_code = ?
            ORDER BY end_date DESC LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], None
    latest = rows[0]
    excerpt = (
        f"最新质押统计 {latest['end_date']}：质押比例={latest['pledge_ratio']}%，"
        f"笔数={latest['pledge_count']}，共 {len(rows)} 期历史"
    )
    return rows, _mk_evidence("stk_pledge_stat", f"ts_code={ts_code}", excerpt)


def probe_st(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 10
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT trade_date, name, type, type_name
            FROM stk_st_daily WHERE ts_code = ?
            ORDER BY trade_date DESC LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], _mk_evidence(
            "stk_st_daily", f"ts_code={ts_code}", "未查到 ST/退市警示记录"
        )
    latest = rows[0]
    excerpt = (
        f"最近 ST 警示：{latest['trade_date']} {latest['name']} [{latest['type_name']}]；"
        f"历史记录 {len(rows)} 条"
    )
    return rows, _mk_evidence("stk_st_daily", f"ts_code={ts_code}", excerpt)


# ---------------------------------------------------------------------------
# Q8 未来规划
# ---------------------------------------------------------------------------


def probe_forecast(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 4
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT ann_date, end_date, type, p_change_min, p_change_max,
                   net_profit_min, net_profit_max, summary, change_reason
            FROM fin_forecast WHERE ts_code = ?
            ORDER BY ann_date DESC LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], None
    r = rows[0]
    excerpt = (
        f"最新业绩预告 {r['ann_date']} → {r['end_date']} 类型={r['type']}，"
        f"净利变动 {r['p_change_min']}%~{r['p_change_max']}%；原因：{(r.get('change_reason') or '')[:100]}"
    )
    return rows, _mk_evidence("fin_forecast", f"ts_code={ts_code}", excerpt)


def probe_express(
    con: duckdb.DuckDBPyConnection, ts_code: str, limit: int = 4
) -> tuple[list[dict[str, Any]], Evidence | None]:
    rows = _rows_to_dicts(
        con.execute(
            """
            SELECT ann_date, end_date, revenue, n_income, yoy_net_profit, yoy_sales, perf_summary
            FROM fin_express WHERE ts_code = ?
            ORDER BY ann_date DESC LIMIT ?
            """,
            [ts_code, limit],
        )
    )
    if not rows:
        return [], None
    r = rows[0]
    excerpt = (
        f"最新业绩快报 {r['ann_date']} → {r['end_date']}：营收YoY={r['yoy_sales']}%, "
        f"归母YoY={r['yoy_net_profit']}%；摘要：{(r.get('perf_summary') or '')[:120]}"
    )
    return rows, _mk_evidence("fin_express", f"ts_code={ts_code}", excerpt)


# ---------------------------------------------------------------------------
# 公开入口
# ---------------------------------------------------------------------------


def open_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    return _connect(db_path)

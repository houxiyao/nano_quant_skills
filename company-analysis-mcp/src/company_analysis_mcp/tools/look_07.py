"""look-07: ROE 与资本回报（杜邦分解）MCP 工具。"""

from __future__ import annotations

import json
import math
from datetime import date
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import (
    CompanyProfile,
    detect_company_profile,
    get_connection,
    parse_date,
)

REPORT_TYPE = "1"
ROE_DRIVER_THRESHOLDS = {
    "leverage_driven": {"em_gt": 5.0, "npm_lt": 0.08},
    "profitability_driven": {"npm_gt": 0.10, "em_lt": 4.0},
    "turnover_driven": {"at_gt": 1.0, "npm_lt": 0.08},
}


# ── 辅助函数 ────────────────────────────────────────────────


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    return float(value)


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _object_exists(con: Connection, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            k: (
                None if _is_missing(v)
                else v.isoformat() if isinstance(v, date)
                else round(v, 6) if isinstance(v, float)
                else v
            )
            for k, v in row.items()
        }
        for row in rows
    ]


# ── 数据查询 ────────────────────────────────────────────────


def _fetch_dupont_inputs(
    con: Connection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    query = f"""
    WITH params AS (
        SELECT CAST(? AS VARCHAR) AS ts_code, CAST(? AS DATE) AS as_of_date, CAST(? AS INTEGER) AS lookback_years
    ),
    balance_yearly AS (
        SELECT b.ts_code, b.end_date,
            COALESCE(b.f_ann_date, b.ann_date, b.end_date) AS visible_date,
            b.total_assets, b.total_hldr_eqy_exc_min_int
        FROM fin_balance b CROSS JOIN params p
        WHERE b.ts_code = p.ts_code AND b.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM b.end_date) = 12 AND EXTRACT(DAY FROM b.end_date) = 31
          AND COALESCE(b.f_ann_date, b.ann_date, b.end_date) <= p.as_of_date
    ),
    income_yearly AS (
        SELECT i.ts_code, i.end_date,
            COALESCE(i.f_ann_date, i.ann_date, i.end_date) AS visible_date,
            i.revenue, i.n_income_attr_p
        FROM fin_income i CROSS JOIN params p
        WHERE i.ts_code = p.ts_code AND i.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM i.end_date) = 12 AND EXTRACT(DAY FROM i.end_date) = 31
          AND COALESCE(i.f_ann_date, i.ann_date, i.end_date) <= p.as_of_date
    ),
    bal_dedup AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY visible_date DESC) AS rn FROM balance_yearly),
    inc_dedup AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY visible_date DESC) AS rn FROM income_yearly),
    combined AS (
        SELECT b.ts_code, b.end_date, b.total_assets, b.total_hldr_eqy_exc_min_int, i.revenue, i.n_income_attr_p
        FROM bal_dedup b
        LEFT JOIN inc_dedup i ON b.ts_code = i.ts_code AND b.end_date = i.end_date AND i.rn = 1
        WHERE b.rn = 1
    ),
    ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) AS rn FROM combined)
    SELECT ts_code, end_date, total_assets, total_hldr_eqy_exc_min_int, revenue, n_income_attr_p
    FROM ranked WHERE rn <= (SELECT lookback_years + 2 FROM params) ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


def _fetch_indicator_rows(
    con: Connection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    query = f"""
    WITH params AS (
        SELECT CAST(? AS VARCHAR) AS ts_code, CAST(? AS DATE) AS as_of_date, CAST(? AS INTEGER) AS lookback_years
    ),
    indicator_yearly AS (
        SELECT fi.ts_code, fi.end_date,
            COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) AS sort_key,
            fi.roe, fi.roe_dt, fi.roa, fi.netprofit_margin, fi.assets_turn, fi.debt_to_assets
        FROM fin_indicator fi CROSS JOIN params p
        WHERE fi.ts_code = p.ts_code
          AND EXTRACT(MONTH FROM fi.end_date) = 12 AND EXTRACT(DAY FROM fi.end_date) = 31
          AND COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) <= p.as_of_date
    ),
    deduped AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY sort_key DESC) AS rn FROM indicator_yearly),
    ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) AS rn2 FROM deduped WHERE rn = 1)
    SELECT ts_code, end_date, roe, roe_dt, roa, netprofit_margin, assets_turn, debt_to_assets
    FROM ranked WHERE rn2 <= (SELECT lookback_years FROM params) ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


def _find_benchmark(
    con: Connection,
    stock: str,
    as_of_date: date,
) -> dict[str, Any] | None:
    if not _object_exists(con, "idx_sw_l3_peers") or not _object_exists(con, "stk_factor_pro"):
        return None
    peers = con.execute(
        "SELECT DISTINCT peer_ts_code, peer_name FROM idx_sw_l3_peers WHERE anchor_ts_code = ? AND peer_is_self = false",
        [stock],
    ).fetchall()
    if not peers:
        return None
    peer_codes = [p[0] for p in peers]
    peer_name_map = {p[0]: p[1] for p in peers}
    latest_date_row = con.execute(
        "SELECT MAX(trade_date) FROM stk_factor_pro WHERE trade_date <= CAST(? AS DATE) AND total_mv IS NOT NULL",
        [as_of_date],
    ).fetchone()
    if not latest_date_row or latest_date_row[0] is None:
        return None
    latest_trade_date = latest_date_row[0]
    placeholders = ", ".join(["?"] * len(peer_codes))
    top_row = con.execute(
        f"SELECT ts_code, total_mv FROM stk_factor_pro WHERE trade_date = ? AND ts_code IN ({placeholders}) AND total_mv IS NOT NULL ORDER BY total_mv DESC LIMIT 1",
        [latest_trade_date] + peer_codes,
    ).fetchone()
    if not top_row:
        return None
    return {
        "ts_code": top_row[0],
        "name": peer_name_map.get(top_row[0], ""),
        "total_mv": float(top_row[1]),
        "mv_trade_date": latest_trade_date.isoformat() if isinstance(latest_trade_date, date) else str(latest_trade_date),
    }


def _fetch_peer_industry_info(con: Connection, stock: str) -> dict[str, Any]:
    if not _object_exists(con, "idx_sw_l3_peers"):
        return {}
    row = con.execute(
        "SELECT DISTINCT l1_name, l2_name, l3_name, l3_code, peer_group_size FROM idx_sw_l3_peers WHERE anchor_ts_code = ? LIMIT 1",
        [stock],
    ).fetchone()
    if not row:
        return {}
    return {"l1_name": row[0], "l2_name": row[1], "l3_name": row[2], "l3_code": row[3], "peer_group_size": row[4]}


# ── 杜邦分解 ────────────────────────────────────────────────


def _compute_dupont(raw_rows: list[dict[str, Any]], lookback_years: int) -> list[dict[str, Any]]:
    if len(raw_rows) < 2:
        if len(raw_rows) == 1:
            row = raw_rows[0]
            return [{
                "end_date": row["end_date"],
                "revenue": _float_or_none(row.get("revenue")),
                "n_income_attr_p": _float_or_none(row.get("n_income_attr_p")),
                "total_assets": _float_or_none(row.get("total_assets")),
                "parent_equity": _float_or_none(row.get("total_hldr_eqy_exc_min_int")),
                "avg_total_assets": None,
                "avg_parent_equity": None,
                "npm": _safe_div(_float_or_none(row.get("n_income_attr_p")), _float_or_none(row.get("revenue"))),
                "asset_turnover": None,
                "equity_multiplier": None,
                "roe_dupont": None,
                "negative_equity": False,
            }]
        return []

    by_date = {row["end_date"]: row for row in raw_rows}
    sorted_dates = sorted(by_date.keys(), reverse=True)
    output_count = min(lookback_years, len(sorted_dates) - 1)
    results = []

    for i in range(output_count):
        curr = by_date[sorted_dates[i]]
        prior = by_date.get(sorted_dates[i + 1]) if (i + 1) < len(sorted_dates) else None

        rev = _float_or_none(curr.get("revenue"))
        ni = _float_or_none(curr.get("n_income_attr_p"))
        ta_end = _float_or_none(curr.get("total_assets"))
        eq_end = _float_or_none(curr.get("total_hldr_eqy_exc_min_int"))
        ta_beg = _float_or_none(prior.get("total_assets")) if prior else None
        eq_beg = _float_or_none(prior.get("total_hldr_eqy_exc_min_int")) if prior else None

        avg_ta = (ta_beg + ta_end) / 2 if (ta_end is not None and ta_beg is not None) else None
        avg_eq = (eq_beg + eq_end) / 2 if (eq_end is not None and eq_beg is not None) else None

        npm = _safe_div(ni, rev)
        at = _safe_div(rev, avg_ta)
        negative_equity = avg_eq is not None and avg_eq <= 0
        em = _safe_div(avg_ta, avg_eq) if not negative_equity else None
        roe_dupont = (npm * at * em) if (npm is not None and at is not None and em is not None) else None

        results.append({
            "end_date": sorted_dates[i],
            "revenue": rev,
            "n_income_attr_p": ni,
            "total_assets": ta_end,
            "parent_equity": eq_end,
            "avg_total_assets": avg_ta,
            "avg_parent_equity": avg_eq,
            "negative_equity": negative_equity,
            "npm": npm,
            "asset_turnover": at,
            "equity_multiplier": em,
            "roe_dupont": roe_dupont,
        })

    return results


def _classify_roe_driver(dupont_rows: list[dict[str, Any]]) -> str:
    if not dupont_rows:
        return "insufficient-data"
    latest = dupont_rows[0]
    if latest.get("negative_equity"):
        return "negative-equity"
    roe = latest.get("roe_dupont")
    if roe is None:
        return "insufficient-data"
    if roe < 0:
        return "negative-roe"
    npm = latest.get("npm")
    at = latest.get("asset_turnover")
    em = latest.get("equity_multiplier")
    if npm is None or at is None or em is None:
        return "insufficient-data"
    t = ROE_DRIVER_THRESHOLDS
    if em > t["leverage_driven"]["em_gt"] and npm < t["leverage_driven"]["npm_lt"]:
        return "leverage-driven"
    if npm > t["profitability_driven"]["npm_gt"] and em < t["profitability_driven"]["em_lt"]:
        return "profitability-driven"
    if at > t["turnover_driven"]["at_gt"] and npm < t["turnover_driven"]["npm_lt"]:
        return "turnover-driven"
    return "mixed"


def _assess_roe_trend(dupont_rows: list[dict[str, Any]]) -> str:
    roe_values = [r["roe_dupont"] for r in dupont_rows if r.get("roe_dupont") is not None]
    if len(roe_values) < 2:
        return "insufficient-data"
    newest = roe_values[0]
    oldest = roe_values[-1]
    if oldest == 0:
        return "volatile" if newest != 0 else "stable"
    change_ratio = (newest - oldest) / abs(oldest)
    if change_ratio > 0.15:
        return "improving"
    if change_ratio < -0.15:
        return "deteriorating"
    return "stable"


def _build_summary(dupont_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not dupont_rows:
        return {
            "years_returned": 0,
            "roe_latest": None,
            "roe_driver": "insufficient-data",
            "roe_trend": "insufficient-data",
            "npm_latest": None,
            "at_latest": None,
            "em_latest": None,
        }
    latest = dupont_rows[0]
    return {
        "years_returned": len(dupont_rows),
        "latest_end_date": latest["end_date"].isoformat() if isinstance(latest["end_date"], date) else str(latest["end_date"]),
        "roe_latest": latest.get("roe_dupont"),
        "roe_driver": _classify_roe_driver(dupont_rows),
        "roe_trend": _assess_roe_trend(dupont_rows),
        "npm_latest": latest.get("npm"),
        "at_latest": latest.get("asset_turnover"),
        "em_latest": latest.get("equity_multiplier"),
        "roe_driver_thresholds": ROE_DRIVER_THRESHOLDS,
    }


def _build_comparison(target: list[dict[str, Any]], bench: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bench_by_date = {}
    for r in bench:
        key = r["end_date"].isoformat() if isinstance(r["end_date"], date) else str(r["end_date"])
        bench_by_date[key] = r
    comparison = []
    for t in target:
        key = t["end_date"].isoformat() if isinstance(t["end_date"], date) else str(t["end_date"])
        b = bench_by_date.get(key, {})
        entry: dict[str, Any] = {"end_date": key}
        for metric in ("roe_dupont", "npm", "asset_turnover", "equity_multiplier"):
            entry[f"target_{metric}"] = t.get(metric)
            entry[f"bench_{metric}"] = b.get(metric)
        comparison.append(entry)
    return comparison


# ── 内部执行接口 ─────────────────────────────────────────────


def execute_look_07(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int = 5,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 look-07 ROE 与资本回报（杜邦分解）分析，返回结构化结果字典。"""
    parsed_date = parse_date(as_of_date)
    should_close = con is None
    if con is None:
        con = get_connection()

    try:
        profile: CompanyProfile = detect_company_profile(con, ts_code, parsed_date)
        if profile.is_financial:
            return {
                "rule_id": "look-07",
                "status": "not-applicable",
                "stock": ts_code,
                "as_of_date": parsed_date.isoformat(),
                "lookback_years": lookback_years,
                "company_profile": profile.to_payload(),
                "reason": "金融类公司杠杆结构与一般工商业差异过大，杜邦分解结果不可直接类比。",
            }
        raw_rows = _fetch_dupont_inputs(con, ts_code, parsed_date, lookback_years)
        indicator_rows = _fetch_indicator_rows(con, ts_code, parsed_date, lookback_years)
        industry_info = _fetch_peer_industry_info(con, ts_code)
        benchmark = _find_benchmark(con, ts_code, parsed_date)
        bench_raw = _fetch_dupont_inputs(con, benchmark["ts_code"], parsed_date, lookback_years) if benchmark else []
    finally:
        if should_close:
            con.close()

    dupont_rows = _compute_dupont(raw_rows, lookback_years)
    bench_dupont = _compute_dupont(bench_raw, lookback_years) if benchmark else []
    summary = _build_summary(dupont_rows)
    comparison = _build_comparison(dupont_rows, bench_dupont) if benchmark else []

    status = "ready" if summary["years_returned"] > 0 else "no-data"

    return {
        "rule_id": "look-07",
        "status": status,
        "stock": ts_code,
        "as_of_date": parsed_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "industry_info": industry_info,
        "summary": summary,
        "dupont_rows": _serialize_rows(dupont_rows),
        "indicator_rows": _serialize_rows(indicator_rows),
        "benchmark": benchmark,
        "benchmark_dupont_rows": _serialize_rows(bench_dupont),
        "comparison": _serialize_rows(comparison),
    }


# ── MCP Tool 注册 ────────────────────────────────────────────


def register_look_07_tools(mcp: FastMCP) -> None:
    """注册 look-07 ROE 与资本回报分析工具。"""

    @mcp.tool()
    def look_07_roe_capital_return(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int = 5,
    ) -> str:
        """七看 ROE 与资本回报（杜邦分解）。

        通过三因子杜邦分解（NPM × 资产周转率 × 权益乘数）拆解 ROE 来源，
        判断是盈利能力驱动、周转驱动还是杠杆驱动，并与行业龙头对比。
        金融类公司返回 status=not-applicable。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"
            as_of_date: 分析截止日期 YYYY-MM-DD，默认今天
            lookback_years: 回看年数，默认 5

        Returns:
            JSON 字符串，含 rule_id, status, summary, dupont_rows, indicator_rows, benchmark, comparison
        """
        result = execute_look_07(ts_code, as_of_date, lookback_years)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

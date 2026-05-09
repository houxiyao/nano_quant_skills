"""look-06: 投入产出效率分析 MCP 工具。"""

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


def _fetch_efficiency_inputs(
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
            b.accounts_receiv, b.inventories, b.prepayment,
            b.acct_payable, b.adv_receipts, b.contract_liab,
            b.fix_assets, b.total_assets
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
    cashflow_yearly AS (
        SELECT c.ts_code, c.end_date,
            COALESCE(c.f_ann_date, c.ann_date, c.end_date) AS visible_date,
            c.c_paid_to_for_empl
        FROM fin_cashflow c CROSS JOIN params p
        WHERE c.ts_code = p.ts_code AND c.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM c.end_date) = 12 AND EXTRACT(DAY FROM c.end_date) = 31
          AND COALESCE(c.f_ann_date, c.ann_date, c.end_date) <= p.as_of_date
    ),
    bal_dedup AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY visible_date DESC) AS rn FROM balance_yearly),
    inc_dedup AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY visible_date DESC) AS rn FROM income_yearly),
    cf_dedup AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY visible_date DESC) AS rn FROM cashflow_yearly),
    combined AS (
        SELECT b.ts_code, b.end_date,
            b.accounts_receiv, b.inventories, b.prepayment,
            b.acct_payable, b.adv_receipts, b.contract_liab,
            b.fix_assets, b.total_assets,
            i.revenue, i.n_income_attr_p, c.c_paid_to_for_empl
        FROM bal_dedup b
        LEFT JOIN inc_dedup i ON b.ts_code = i.ts_code AND b.end_date = i.end_date AND i.rn = 1
        LEFT JOIN cf_dedup c ON b.ts_code = c.ts_code AND b.end_date = c.end_date AND c.rn = 1
        WHERE b.rn = 1
    ),
    ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) AS rn FROM combined)
    SELECT ts_code, end_date, accounts_receiv, inventories, prepayment,
        acct_payable, adv_receipts, contract_liab, fix_assets, total_assets,
        revenue, n_income_attr_p, c_paid_to_for_empl
    FROM ranked WHERE rn <= (SELECT lookback_years FROM params) ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


def _fetch_turnover_indicators(
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
            fi.ar_turn, fi.fa_turn, fi.assets_turn, fi.ca_turn
        FROM fin_indicator fi CROSS JOIN params p
        WHERE fi.ts_code = p.ts_code
          AND EXTRACT(MONTH FROM fi.end_date) = 12 AND EXTRACT(DAY FROM fi.end_date) = 31
          AND COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) <= p.as_of_date
    ),
    deduped AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY sort_key DESC) AS rn FROM indicator_yearly),
    ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) AS rn2 FROM deduped WHERE rn = 1)
    SELECT ts_code, end_date, ar_turn, fa_turn, assets_turn, ca_turn
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


# ── 效率计算 ────────────────────────────────────────────────


def _parse_employee_bundle(employee_bundle_json: str) -> dict[tuple[str, int], int]:
    if not employee_bundle_json:
        return {}
    payload = json.loads(employee_bundle_json)
    entries = payload.get("employee_counts") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        raise ValueError("Employee-count bundle must be a list or an object with an 'employee_counts' field")
    mapping: dict[tuple[str, int], int] = {}
    for item in entries:
        if not isinstance(item, dict):
            raise ValueError("Each employee-count entry must be an object")
        ts_code = str(item.get("ts_code") or "").strip().upper()
        if not ts_code:
            raise ValueError("Each employee-count entry must contain ts_code")
        year = int(str(item.get("year")).strip())
        count = int(item.get("employee_count"))
        if count <= 0:
            raise ValueError(f"employee_count must be positive, got: {count}")
        mapping[(ts_code, year)] = count
    return mapping


def _compute_efficiency(
    rows: list[dict[str, Any]],
    employee_bundle: dict[tuple[str, int], int] | None = None,
    ts_code: str | None = None,
) -> list[dict[str, Any]]:
    employee_bundle = employee_bundle or {}
    ts_code_key = (ts_code or "").strip().upper()
    results = []
    for row in rows:
        ar = _float_or_none(row.get("accounts_receiv"))
        inv = _float_or_none(row.get("inventories"))
        prep = _float_or_none(row.get("prepayment"))
        ap = _float_or_none(row.get("acct_payable"))
        adv = _float_or_none(row.get("adv_receipts"))
        cl = _float_or_none(row.get("contract_liab"))
        fix = _float_or_none(row.get("fix_assets"))
        rev = _float_or_none(row.get("revenue"))
        profit = _float_or_none(row.get("n_income_attr_p"))
        labor = _float_or_none(row.get("c_paid_to_for_empl"))

        wc_parts = [ar, inv, prep]
        if all(p is not None for p in wc_parts) and ap is not None:
            wc = sum(wc_parts) - sum([ap, adv or 0, cl or 0])
        else:
            wc = None

        year_value = row["end_date"].year if isinstance(row["end_date"], date) else None
        employee_count = employee_bundle.get((ts_code_key, year_value)) if ts_code_key and year_value else None

        results.append({
            "end_date": row["end_date"],
            "revenue": rev,
            "n_income_attr_p": profit,
            "working_capital": wc,
            "wc_per_revenue": _safe_div(wc, rev),
            "fix_assets": fix,
            "fix_assets_per_revenue": _safe_div(fix, rev),
            "c_paid_to_for_empl": labor,
            "revenue_per_labor_cost": _safe_div(rev, labor),
            "profit_per_labor_cost": _safe_div(profit, labor),
            "employee_count": employee_count,
            "revenue_per_employee": _safe_div(rev, employee_count) if employee_count else None,
            "profit_per_employee": _safe_div(profit, employee_count) if employee_count else None,
        })
    return results


def _build_summary(eff_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not eff_rows:
        return {"years_returned": 0, "per_capita_status": "human-in-loop-required", "wc_trend": "insufficient-data"}

    latest = eff_rows[0]
    wc_values = [r["wc_per_revenue"] for r in eff_rows if r["wc_per_revenue"] is not None]
    if len(wc_values) >= 2:
        if wc_values[0] < wc_values[-1] * 0.95:
            wc_trend = "improving"
        elif wc_values[0] > wc_values[-1] * 1.05:
            wc_trend = "deteriorating"
        else:
            wc_trend = "stable"
    else:
        wc_trend = "insufficient-data"

    years_with_emp = sum(1 for r in eff_rows if r.get("employee_count") is not None)
    if years_with_emp == len(eff_rows):
        per_capita_status = "ready"
    elif years_with_emp > 0:
        per_capita_status = "partial"
    else:
        per_capita_status = "human-in-loop-required"

    return {
        "years_returned": len(eff_rows),
        "latest_end_date": latest["end_date"].isoformat() if isinstance(latest["end_date"], date) else str(latest["end_date"]),
        "wc_per_revenue_latest": latest.get("wc_per_revenue"),
        "fix_assets_per_revenue_latest": latest.get("fix_assets_per_revenue"),
        "revenue_per_labor_cost_latest": latest.get("revenue_per_labor_cost"),
        "revenue_per_employee_latest": latest.get("revenue_per_employee"),
        "per_capita_status": per_capita_status,
        "wc_trend": wc_trend,
    }


def _build_comparison(target_eff: list[dict[str, Any]], bench_eff: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bench_by_date = {}
    for r in bench_eff:
        key = r["end_date"].isoformat() if isinstance(r["end_date"], date) else str(r["end_date"])
        bench_by_date[key] = r
    comparison = []
    for t in target_eff:
        key = t["end_date"].isoformat() if isinstance(t["end_date"], date) else str(t["end_date"])
        b = bench_by_date.get(key, {})
        entry: dict[str, Any] = {"end_date": key}
        for metric in ("wc_per_revenue", "fix_assets_per_revenue", "revenue_per_labor_cost"):
            entry[f"target_{metric}"] = t.get(metric)
            entry[f"bench_{metric}"] = b.get(metric)
        comparison.append(entry)
    return comparison


# ── 内部执行接口 ─────────────────────────────────────────────


def execute_look_06(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int = 3,
    employee_bundle_json: str = "",
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 look-06 投入产出效率分析，返回结构化结果字典。"""
    parsed_date = parse_date(as_of_date)
    should_close = con is None
    if con is None:
        con = get_connection()

    try:
        profile: CompanyProfile = detect_company_profile(con, ts_code, parsed_date)
        if profile.is_financial:
            return {
                "rule_id": "look-06",
                "status": "not-applicable",
                "stock": ts_code,
                "as_of_date": parsed_date.isoformat(),
                "lookback_years": lookback_years,
                "company_profile": profile.to_payload(),
                "reason": "当前规则针对一般工商业公司设计，金融类公司的投入产出指标口径不可直接类比。",
            }
        raw_rows = _fetch_efficiency_inputs(con, ts_code, parsed_date, lookback_years)
        turnover_rows = _fetch_turnover_indicators(con, ts_code, parsed_date, lookback_years)
        industry_info = _fetch_peer_industry_info(con, ts_code)
        benchmark = _find_benchmark(con, ts_code, parsed_date)
        bench_raw = _fetch_efficiency_inputs(con, benchmark["ts_code"], parsed_date, lookback_years) if benchmark else []
    finally:
        if should_close:
            con.close()

    employee_bundle = _parse_employee_bundle(employee_bundle_json)
    eff_rows = _compute_efficiency(raw_rows, employee_bundle, ts_code)
    bench_eff = _compute_efficiency(bench_raw, employee_bundle, benchmark["ts_code"] if benchmark else None)
    summary = _build_summary(eff_rows)
    comparison = _build_comparison(eff_rows, bench_eff) if benchmark else []

    human_in_loop_requests: list[str] = []
    if summary.get("per_capita_status") in ("human-in-loop-required", "partial"):
        missing_years = [
            (r["end_date"].year if isinstance(r["end_date"], date) else str(r["end_date"])[:4])
            for r in eff_rows if r.get("employee_count") is None
        ]
        years_str = ", ".join(str(y) for y in missing_years)
        human_in_loop_requests.append(
            f"请提供 {ts_code} 以下年度的真实员工总数（来源：年报员工情况章节），缺口年份：[{years_str}]。"
        )

    per_capita_status = summary.get("per_capita_status", "human-in-loop-required")
    if summary["years_returned"] == 0:
        status = "no-data"
    elif per_capita_status == "human-in-loop-required":
        status = "partial"
    else:
        status = "ready"

    return {
        "rule_id": "look-06",
        "status": status,
        "stock": ts_code,
        "as_of_date": parsed_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "industry_info": industry_info,
        "summary": summary,
        "efficiency_rows": _serialize_rows(eff_rows),
        "turnover_indicators": _serialize_rows(turnover_rows),
        "benchmark": benchmark,
        "benchmark_efficiency_rows": _serialize_rows(bench_eff),
        "comparison": _serialize_rows(comparison),
        "human_in_loop_requests": human_in_loop_requests,
    }


# ── MCP Tool 注册 ────────────────────────────────────────────


def register_look_06_tools(mcp: FastMCP) -> None:
    """注册 look-06 投入产出效率分析工具。"""

    @mcp.tool()
    def look_06_input_output_efficiency(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int = 3,
        employee_bundle_json: str = "",
    ) -> str:
        """六看投入产出效率。

        分析营运资本/营收、固定资产/营收、人均产出等效率指标，
        并与申万 L3 行业龙头做横向对比。真实人均指标需要提供员工人数 JSON。
        金融类公司返回 status=not-applicable。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"
            as_of_date: 分析截止日期 YYYY-MM-DD，默认今天
            lookback_years: 回看年数，默认 3
            employee_bundle_json: 员工人数 JSON ([{ts_code, year, employee_count}...])，可选

        Returns:
            JSON 字符串，含 rule_id, status, summary, efficiency_rows, turnover_indicators, benchmark, comparison
        """
        result = execute_look_06(ts_code, as_of_date, lookback_years, employee_bundle_json)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

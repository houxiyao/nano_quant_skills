"""look-05: 资产负债表健康度分析 MCP 工具。"""

from __future__ import annotations

import json
import math
import re
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
INTERESTDEBT_COMPONENT_FIELDS = ("st_borr", "lt_borr", "bond_payable", "lease_liab")

HIDDEN_LIABILITY_KEYWORDS = {
    "guarantee": ["对外担保", "担保余额", "担保总额", "被担保方", "担保金额", "担保事项", "互保"],
    "contingent_liability": ["或有事项", "或有负债", "潜在义务", "未决诉讼", "未决仲裁"],
    "off_balance_sheet": ["表外安排", "表外融资", "表外业务"],
    "sale_leaseback": ["售后回租", "融资租赁"],
    "receivable_transfer": ["应收账款转让", "保理", "出表", "应收票据贴现", "应收账款融资"],
    "shadow_equity": ["明股实债", "有限合伙", "SPV", "结构化主体", "特殊目的实体"],
}
HIDDEN_LIABILITY_LABELS = {
    "guarantee": "对外担保",
    "contingent_liability": "或有事项/或有负债",
    "off_balance_sheet": "表外安排",
    "sale_leaseback": "售后回租/融资租赁",
    "receivable_transfer": "应收账款转让/保理出表",
    "shadow_equity": "明股实债/结构化主体",
}


# ── 辅助函数 ────────────────────────────────────────────────


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    return float(value)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            key: (
                None if _is_missing(value)
                else value.isoformat() if isinstance(value, date)
                else value
            )
            for key, value in row.items()
        }
        for row in rows
    ]


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\u3000", " ")).strip()


def _collect_windows(text: str, keywords: list[str], limit: int = 5, window: int = 120) -> list[dict[str, Any]]:
    normalized = _normalize_text(text)
    if not normalized:
        return []
    found: dict[str, dict[str, Any]] = {}
    order = 0
    for keyword in keywords:
        for match in re.finditer(re.escape(keyword), normalized):
            start = max(0, match.start() - window)
            end = min(len(normalized), match.end() + window)
            snippet = normalized[start:end].strip(" ，,。；;：:|/")
            if not snippet:
                continue
            matched = [kw for kw in keywords if kw in snippet]
            payload = {
                "snippet": snippet,
                "matched_keywords": list(dict.fromkeys(matched)),
                "numeric_candidates": {
                    "percentages": list(dict.fromkeys(re.findall(r"\d+(?:\.\d+)?\s*%", snippet)))[:5],
                    "amounts": list(dict.fromkeys(
                        re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?\s*(?:亿元|万元|元|亿美元|亿港元|万美元)", snippet)
                    ))[:5],
                },
                "score": len(set(matched)),
                "order": order,
            }
            current = found.get(snippet)
            if current is None or payload["score"] > current["score"]:
                found[snippet] = payload
            order += 1
            if order >= 200:
                break
        if order >= 200:
            break
    rows = sorted(found.values(), key=lambda item: (-item["score"], item["order"]))[:limit]
    for row in rows:
        row.pop("order", None)
    return rows


# ── 数据查询 ────────────────────────────────────────────────


def _fetch_balance_cashflow(
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
        SELECT b.ts_code, b.end_date, b.comp_type,
            COALESCE(b.f_ann_date, b.ann_date, b.end_date) AS visible_date,
            b.money_cap, b.total_cur_assets, b.total_assets,
            b.st_borr, b.lt_borr, b.bond_payable, b.lease_liab,
            b.total_cur_liab, b.total_liab, b.total_hldr_eqy_exc_min_int, b.estimated_liab
        FROM fin_balance b CROSS JOIN params p
        WHERE b.ts_code = p.ts_code AND b.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM b.end_date) = 12 AND EXTRACT(DAY FROM b.end_date) = 31
          AND COALESCE(b.f_ann_date, b.ann_date, b.end_date) <= p.as_of_date
    ),
    cashflow_yearly AS (
        SELECT c.ts_code, c.end_date,
            c.n_cashflow_act, c.n_cashflow_inv_act, c.n_cash_flows_fnc_act,
            c.free_cashflow, c.c_cash_equ_end_period, c.c_pay_acq_const_fiolta
        FROM fin_cashflow c CROSS JOIN params p
        WHERE c.ts_code = p.ts_code AND c.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM c.end_date) = 12 AND EXTRACT(DAY FROM c.end_date) = 31
          AND COALESCE(c.f_ann_date, c.ann_date, c.end_date) <= p.as_of_date
    ),
    combined AS (
        SELECT b.*, c.n_cashflow_act, c.n_cashflow_inv_act, c.n_cash_flows_fnc_act,
            c.free_cashflow, c.c_cash_equ_end_period, c.c_pay_acq_const_fiolta,
            ROW_NUMBER() OVER (PARTITION BY b.ts_code, b.end_date ORDER BY b.visible_date DESC) AS rn_dup
        FROM balance_yearly b
        LEFT JOIN cashflow_yearly c ON b.ts_code = c.ts_code AND b.end_date = c.end_date
    ),
    deduped AS (SELECT * FROM combined WHERE rn_dup = 1),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) AS rn FROM deduped
    )
    SELECT ts_code, end_date, comp_type, money_cap, total_cur_assets, total_assets,
        st_borr, lt_borr, bond_payable, lease_liab, total_cur_liab, total_liab,
        total_hldr_eqy_exc_min_int, estimated_liab,
        n_cashflow_act, n_cashflow_inv_act, n_cash_flows_fnc_act,
        free_cashflow, c_cash_equ_end_period, c_pay_acq_const_fiolta
    FROM ranked WHERE rn <= (SELECT lookback_years FROM params) ORDER BY end_date DESC
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
            COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) AS sort_key, fi.ann_date,
            fi.current_ratio, fi.quick_ratio, fi.cash_ratio,
            fi.debt_to_assets, fi.debt_to_eqt, fi.assets_to_eqt,
            fi.ebit_to_interest, fi.ocf_to_debt, fi.ocf_to_shortdebt,
            fi.ocf_to_interestdebt, fi.interestdebt, fi.netdebt
        FROM fin_indicator fi CROSS JOIN params p
        WHERE fi.ts_code = p.ts_code
          AND EXTRACT(MONTH FROM fi.end_date) = 12 AND EXTRACT(DAY FROM fi.end_date) = 31
          AND COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) <= p.as_of_date
    ),
    deduped AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY sort_key DESC, ann_date DESC) AS rn_dup
        FROM indicator_yearly
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) AS rn
        FROM deduped WHERE rn_dup = 1
    )
    SELECT ts_code, end_date, current_ratio, quick_ratio, cash_ratio,
        debt_to_assets, debt_to_eqt, assets_to_eqt, ebit_to_interest,
        ocf_to_debt, ocf_to_shortdebt, ocf_to_interestdebt, interestdebt, netdebt
    FROM ranked WHERE rn <= (SELECT lookback_years FROM params) ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


# ── 数据合并 ────────────────────────────────────────────────


def _merge_rows(balance_rows: list[dict[str, Any]], indicator_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indicator_by_date = {row["end_date"]: row for row in indicator_rows}
    merged = []
    for brow in balance_rows:
        irow = indicator_by_date.get(brow["end_date"], {})
        combined = {**brow}
        for key in ("current_ratio", "quick_ratio", "cash_ratio", "debt_to_assets",
                    "debt_to_eqt", "assets_to_eqt", "ebit_to_interest",
                    "ocf_to_debt", "ocf_to_shortdebt", "ocf_to_interestdebt",
                    "interestdebt", "netdebt"):
            combined[key] = irow.get(key)
        if _is_missing(combined.get("interestdebt")):
            component_values = {f: _float_or_none(brow.get(f)) for f in INTERESTDEBT_COMPONENT_FIELDS}
            missing_components = [f for f, v in component_values.items() if v is None]
            if not missing_components:
                combined["interestdebt"] = sum(component_values.values())
            else:
                combined["interestdebt"] = None
            combined["interestdebt_derived"] = True
            combined["interestdebt_missing_components"] = missing_components
        else:
            combined["interestdebt_derived"] = False
            combined["interestdebt_missing_components"] = []
        merged.append(combined)
    return merged


def _compute_cashflow_coverage(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    coverage = []
    for row in rows:
        ocf = _float_or_none(row.get("n_cashflow_act"))
        icf = _float_or_none(row.get("n_cashflow_inv_act"))
        fcf_fnc = _float_or_none(row.get("n_cash_flows_fnc_act"))
        capex = _float_or_none(row.get("c_pay_acq_const_fiolta"))

        ocf_covers_inv = (ocf + icf) if (ocf is not None and icf is not None) else None
        ocf_covers_all = (ocf + icf + fcf_fnc) if (ocf is not None and icf is not None and fcf_fnc is not None) else None
        ocf_minus_capex = (ocf - capex) if (ocf is not None and capex is not None) else None

        coverage.append({
            "end_date": row["end_date"],
            "n_cashflow_act": ocf,
            "n_cashflow_inv_act": icf,
            "n_cash_flows_fnc_act": fcf_fnc,
            "free_cashflow": _float_or_none(row.get("free_cashflow")),
            "c_pay_acq_const_fiolta": capex,
            "ocf_minus_capex": ocf_minus_capex,
            "ocf_covers_capex": ocf_minus_capex >= 0 if ocf_minus_capex is not None else None,
            "ocf_plus_icf": ocf_covers_inv,
            "ocf_covers_investing": ocf_covers_inv is not None and ocf_covers_inv >= 0,
            "net_cash_change": ocf_covers_all,
        })
    return coverage


# ── 隐性负债分析 ─────────────────────────────────────────────


def _parse_report_bundle(report_bundle_json: str) -> list[dict[str, Any]]:
    if not report_bundle_json:
        return []
    payload = json.loads(report_bundle_json)
    reports = payload.get("reports") if isinstance(payload, dict) else payload
    if not isinstance(reports, list):
        raise ValueError("Report bundle must be a list or an object with a 'reports' field")
    normalized = []
    for item in reports:
        if not isinstance(item, dict):
            raise ValueError("Each report entry must be an object")
        ts_code = str(item.get("ts_code") or "").strip().upper()
        if not ts_code:
            raise ValueError("Each report entry must contain ts_code")
        normalized.append({
            "ts_code": ts_code,
            "year": int(str(item.get("year")).strip()),
            "text": str(item.get("text") or item.get("content") or ""),
        })
    return normalized


def _analyze_hidden_liabilities(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        return {
            "status": "human-in-loop-required",
            "reason": "未提供年报附注文本，无法自动提取隐性负债证据。",
            "report_count": 0,
            "total_evidence_count": 0,
            "reports": [],
        }
    report_results = []
    for report in sorted(reports, key=lambda r: int(r.get("year") or 0), reverse=True):
        text = str(report.get("text") or "")
        entry: dict[str, Any] = {"ts_code": report["ts_code"], "year": report.get("year"), "text_available": bool(_normalize_text(text))}
        missing_dims = []
        for dimension, keywords in HIDDEN_LIABILITY_KEYWORDS.items():
            field = f"{dimension}_evidence"
            entry[field] = _collect_windows(text, keywords)
            if not entry[field]:
                missing_dims.append(dimension)
        entry["missing_dimensions"] = missing_dims
        report_results.append(entry)

    total_evidence = sum(
        len(r.get(f"{dim}_evidence", []))
        for r in report_results
        for dim in HIDDEN_LIABILITY_KEYWORDS
    )
    if total_evidence == 0:
        status = "human-in-loop-required"
        reason = "提供了年报文本但未匹配到任何隐性负债关键词。"
    else:
        status = "ready"
        reason = "已匹配到隐性负债证据。"

    return {
        "status": status,
        "reason": reason,
        "report_count": len(report_results),
        "total_evidence_count": total_evidence,
        "reports": report_results,
    }


# ── 汇总 ────────────────────────────────────────────────────


def _build_summary(
    merged_rows: list[dict[str, Any]],
    cashflow_coverage: list[dict[str, Any]],
    hidden_result: dict[str, Any],
) -> dict[str, Any]:
    if not merged_rows:
        return {"years_returned": 0, "leverage_trend": "unknown", "hidden_liability_status": hidden_result["status"]}

    latest = merged_rows[0]
    oldest = merged_rows[-1]
    ocf_pos = sum(1 for c in cashflow_coverage if (c["n_cashflow_act"] or 0) > 0)
    fcf_pos = sum(1 for c in cashflow_coverage if (c["free_cashflow"] or 0) > 0)
    ocf_covers = sum(1 for c in cashflow_coverage if c["ocf_covers_investing"])
    ocf_covers_capex_years = sum(1 for c in cashflow_coverage if c.get("ocf_covers_capex") is True)

    a2e_values = [_float_or_none(r.get("assets_to_eqt")) for r in merged_rows if not _is_missing(r.get("assets_to_eqt"))]
    if len(a2e_values) >= 2:
        if a2e_values[0] > a2e_values[-1] * 1.05:
            leverage_trend = "rising"
        elif a2e_values[0] < a2e_values[-1] * 0.95:
            leverage_trend = "declining"
        else:
            leverage_trend = "stable"
    else:
        leverage_trend = "insufficient-data"

    return {
        "years_returned": len(merged_rows),
        "latest_end_date": latest["end_date"].isoformat() if isinstance(latest["end_date"], date) else str(latest["end_date"]),
        "ocf_positive_years": ocf_pos,
        "fcf_positive_years": fcf_pos,
        "ocf_covers_investing_years": ocf_covers,
        "ocf_covers_capex_years": ocf_covers_capex_years,
        "leverage_trend": leverage_trend,
        "hidden_liability_status": hidden_result["status"],
    }


# ── 内部执行接口 ─────────────────────────────────────────────


def execute_look_05(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int = 3,
    report_bundle_json: str = "",
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 look-05 资产负债表健康度分析，返回结构化结果字典。"""
    parsed_date = parse_date(as_of_date)
    should_close = con is None
    if con is None:
        con = get_connection()

    try:
        profile: CompanyProfile = detect_company_profile(con, ts_code, parsed_date)
        if profile.is_financial:
            return {
                "rule_id": "look-05",
                "status": "not-applicable",
                "stock": ts_code,
                "as_of_date": parsed_date.isoformat(),
                "lookback_years": lookback_years,
                "company_profile": profile.to_payload(),
                "reason": "当前规则针对一般工商业公司设计，金融类公司的负债结构口径不可直接类比。",
            }
        balance_rows = _fetch_balance_cashflow(con, ts_code, parsed_date, lookback_years)
        indicator_rows = _fetch_indicator_rows(con, ts_code, parsed_date, lookback_years)
    finally:
        if should_close:
            con.close()

    merged_rows = _merge_rows(balance_rows, indicator_rows)
    cashflow_coverage = _compute_cashflow_coverage(merged_rows)

    reports = _parse_report_bundle(report_bundle_json)
    target_reports = [r for r in reports if r["ts_code"] == ts_code.upper()]
    hidden_result = _analyze_hidden_liabilities(target_reports)

    human_requests: list[str] = []
    if hidden_result["report_count"] == 0:
        human_requests.append(
            f"请提供 {ts_code} 最近{lookback_years}年的年报附注全文，"
            "以便自动提取对外担保、或有事项、表外融资等隐性负债证据。"
        )

    summary = _build_summary(merged_rows, cashflow_coverage, hidden_result)

    if human_requests:
        status = "partial" if summary["years_returned"] > 0 else "human-in-loop-required"
    else:
        status = "ready" if summary["years_returned"] > 0 else "no-data"

    return {
        "rule_id": "look-05",
        "status": status,
        "stock": ts_code,
        "as_of_date": parsed_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "summary": summary,
        "cashflow_coverage": _serialize_rows(cashflow_coverage),
        "debt_solvency_rows": _serialize_rows(merged_rows),
        "hidden_liability_analysis": hidden_result,
        "human_in_loop_requests": human_requests,
    }


# ── MCP Tool 注册 ────────────────────────────────────────────


def register_look_05_tools(mcp: FastMCP) -> None:
    """注册 look-05 资产负债表健康度分析工具。"""

    @mcp.tool()
    def look_05_balance_sheet_health(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int = 3,
        report_bundle_json: str = "",
    ) -> str:
        """五看资产负债表健康度。

        分析流动性（流动/速动/现金比率）、偿债能力（资产负债率/利息覆盖倍数）、
        现金流覆盖（OCF覆盖资本开支/投资活动），以及通过年报附注提取隐性负债证据。
        金融类公司返回 status=not-applicable。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"
            as_of_date: 分析截止日期 YYYY-MM-DD，默认今天
            lookback_years: 回看年数，默认 3
            report_bundle_json: 年报附注文本 JSON（用于隐性负债分析），可选

        Returns:
            JSON 字符串，含 rule_id, status, summary, cashflow_coverage, debt_solvency_rows, hidden_liability_analysis
        """
        result = execute_look_05(ts_code, as_of_date, lookback_years, report_bundle_json)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

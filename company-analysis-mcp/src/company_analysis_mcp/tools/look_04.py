"""look-04: 业务与市场分布分析 MCP 工具。"""

from __future__ import annotations

import json
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

KEYWORDS = {
    "business_composition": [
        "主营业务", "业务构成", "收入构成", "营业收入构成",
        "分产品", "分地区", "分行业", "主营业务收入",
        "按产品", "按地区",
    ],
    "overseas_sales": [
        "境外", "海外", "国外", "外销", "出口",
        "国际市场", "海外收入", "境外收入", "境内外",
    ],
    "customer_concentration": [
        "前五大客户", "前五名客户", "单一客户", "客户集中度",
        "第一大客户", "前五大客户销售额", "前五名客户销售额",
        "销售总额比例", "客户销售额占年度销售总额比例", "客户依赖",
    ],
}
DIMENSION_LABELS = {
    "business_composition": "主营业务构成",
    "overseas_sales": "海外/境外销售",
    "customer_concentration": "单一客户/前五大客户",
}
OVERSEAS_SALES_CONTEXT_KEYWORDS = (
    "销售", "收入", "营收", "主营", "业务", "地区", "分部", "客户", "占比", "毛利",
)
OVERSEAS_SALES_REJECT_PATTERNS = (
    re.compile(r"境内外会计准则"),
    re.compile(r"境内外.*会计.*差异"),
)


# ── 辅助函数 ────────────────────────────────────────────────


def _object_exists(con: Connection, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\u3000", " ")).strip()


def _normalize_report_year(value: Any) -> int:
    if value is None or str(value).strip() == "":
        raise ValueError("Each report entry must contain year")
    return int(str(value).strip())


def _is_valid_overseas_sales_evidence(evidence: dict[str, Any]) -> bool:
    snippet = str(evidence.get("snippet") or "")
    matched_keywords = evidence.get("matched_keywords") or []
    if any(pattern.search(snippet) for pattern in OVERSEAS_SALES_REJECT_PATTERNS):
        return False
    if "境内外" in matched_keywords and not any(
        keyword in snippet for keyword in OVERSEAS_SALES_CONTEXT_KEYWORDS
    ):
        return False
    return True


def _filter_dimension_evidence(dimension: str, evidences: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if dimension != "overseas_sales":
        return evidences
    return [row for row in evidences if _is_valid_overseas_sales_evidence(row)]


def _collect_windows(text: str, keywords: list[str], limit: int = 5, window: int = 90) -> list[dict[str, Any]]:
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
            matched = [item for item in keywords if item in snippet]
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


def _fetch_stock_info(con: Connection, stock: str) -> dict[str, Any] | None:
    if not _object_exists(con, "stk_info"):
        return None
    result = con.execute(
        "SELECT ts_code, symbol, name, area, industry, market, list_date, act_name, act_ent_type FROM stk_info WHERE ts_code = ?",
        [stock],
    )
    row = result.fetchone()
    if row is None:
        return None
    columns = [item[0] for item in result.description]
    return {col: (v.isoformat() if isinstance(v, date) else v) for col, v in zip(columns, row)}


def _fetch_peer_groups(con: Connection, stock: str) -> list[dict[str, Any]]:
    if not _object_exists(con, "idx_sw_l3_peers"):
        return []
    result = con.execute(
        "SELECT DISTINCT anchor_l3_count, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, peer_group_size FROM idx_sw_l3_peers WHERE anchor_ts_code = ? ORDER BY l3_code",
        [stock],
    )
    columns = [item[0] for item in result.description]
    return [{col: v for col, v in zip(columns, record)} for record in result.fetchall()]


def _fetch_peers(con: Connection, stock: str) -> list[dict[str, Any]]:
    if not _object_exists(con, "idx_sw_l3_peers"):
        return []
    result = con.execute(
        "SELECT l3_code, l3_name, peer_group_size, peer_ts_code, peer_name FROM idx_sw_l3_peers WHERE anchor_ts_code = ? AND peer_is_self = false ORDER BY l3_code, peer_ts_code",
        [stock],
    )
    rows = result.fetchall()
    peers: dict[str, dict[str, Any]] = {}
    for l3_code, l3_name, peer_group_size, peer_ts_code, peer_name in rows:
        peer = peers.setdefault(
            str(peer_ts_code),
            {"peer_ts_code": str(peer_ts_code), "peer_name": peer_name, "l3_codes": [], "l3_names": [], "peer_group_size_max": 0},
        )
        if l3_code and l3_code not in peer["l3_codes"]:
            peer["l3_codes"].append(l3_code)
        if l3_name and l3_name not in peer["l3_names"]:
            peer["l3_names"].append(l3_name)
        peer["peer_group_size_max"] = max(peer["peer_group_size_max"], int(peer_group_size or 0))
    return sorted(peers.values(), key=lambda item: item["peer_ts_code"])


# ── 报告分析 ────────────────────────────────────────────────


def _parse_report_bundle(report_bundle_json: str) -> list[dict[str, Any]]:
    """解析 JSON 字符串格式的年报文本包。"""
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
            "name": item.get("name"),
            "year": _normalize_report_year(item.get("year")),
            "url": item.get("url"),
            "text": str(item.get("text") or item.get("content") or ""),
        })
    return normalized


def _analyze_report(report: dict[str, Any]) -> dict[str, Any]:
    text = str(report.get("text") or "")
    payload: dict[str, Any] = {
        "ts_code": report["ts_code"],
        "company_name": report.get("name"),
        "year": report.get("year"),
        "url": report.get("url"),
        "text_available": bool(_normalize_text(text)),
    }
    missing = []
    for dimension, keywords in KEYWORDS.items():
        field = f"{dimension}_evidence"
        payload[field] = _filter_dimension_evidence(dimension, _collect_windows(text, keywords))
        if not payload[field]:
            missing.append(dimension)
    payload["missing_dimensions"] = missing
    return payload


def _summarize_company(ts_code: str, company_name: str | None, reports: list[dict[str, Any]], lookback_years: int, role: str) -> dict[str, Any]:
    ordered = sorted(reports, key=lambda item: int(item.get("year") or 0), reverse=True)[:lookback_years]
    report_rows = [_analyze_report(report) for report in ordered]
    evidence_counts = {}
    missing_dimensions = []
    for dimension in KEYWORDS:
        field = f"{dimension}_evidence"
        count = sum(len(row[field]) for row in report_rows)
        evidence_counts[dimension] = count
        if count == 0:
            missing_dimensions.append(dimension)
    if not report_rows:
        status = "human-in-loop-required"
    elif len(report_rows) < lookback_years or missing_dimensions:
        status = "partial"
    else:
        status = "ready"

    def _sample(field: str) -> str | None:
        for row in report_rows:
            evidences = row.get(field) or []
            if evidences:
                return evidences[0]["snippet"]
        return None

    return {
        "role": role,
        "ts_code": ts_code,
        "company_name": company_name,
        "provided_report_count": len(report_rows),
        "report_years": [row.get("year") for row in report_rows],
        "business_composition_evidence_count": evidence_counts["business_composition"],
        "overseas_sales_evidence_count": evidence_counts["overseas_sales"],
        "customer_concentration_evidence_count": evidence_counts["customer_concentration"],
        "missing_dimensions": missing_dimensions,
        "status": status,
        "sample_business_composition_snippet": _sample("business_composition_evidence"),
        "sample_overseas_sales_snippet": _sample("overseas_sales_evidence"),
        "sample_customer_concentration_snippet": _sample("customer_concentration_evidence"),
        "report_rows": report_rows,
    }


# ── 内部执行接口 ─────────────────────────────────────────────


def execute_look_04(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int = 3,
    peer_limit: int = 5,
    report_bundle_json: str = "",
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 look-04 业务与市场分布分析，返回结构化结果字典。"""
    parsed_date = parse_date(as_of_date)
    should_close = con is None
    if con is None:
        con = get_connection()

    try:
        profile: CompanyProfile = detect_company_profile(con, ts_code, parsed_date)
        if profile.is_financial:
            return {
                "rule_id": "look-04",
                "status": "not-applicable",
                "stock": ts_code,
                "as_of_date": parsed_date.isoformat(),
                "lookback_years": lookback_years,
                "company_profile": profile.to_payload(),
                "reason": "当前规则针对一般工商业公司设计，金融类公司的业务构成与市场分布口径不可直接类比。",
                "rows": [],
            }
        stock_info = _fetch_stock_info(con, ts_code)
        sw_peer_view_available = _object_exists(con, "idx_sw_l3_peers")
        peer_groups = _fetch_peer_groups(con, ts_code)
        all_peers = _fetch_peers(con, ts_code)
    finally:
        if should_close:
            con.close()

    reports = _parse_report_bundle(report_bundle_json)
    reports_by_company: dict[str, list[dict[str, Any]]] = {}
    for report in reports:
        reports_by_company.setdefault(report["ts_code"], []).append(report)

    selected_peers = all_peers[:peer_limit]
    target_name = stock_info.get("name") if stock_info else None
    structured_context = {
        "stock_info": stock_info,
        "sw_peer_view_available": sw_peer_view_available,
        "peer_groups": peer_groups,
        "peer_candidate_count": len(all_peers),
        "selected_peers": selected_peers,
    }
    target_analysis = _summarize_company(ts_code, target_name, reports_by_company.get(ts_code, []), lookback_years, "target")

    peer_analyses: dict[str, dict[str, Any]] = {}
    peer_rows = []
    for peer in selected_peers:
        analysis = _summarize_company(peer["peer_ts_code"], peer["peer_name"], reports_by_company.get(peer["peer_ts_code"], []), lookback_years, "peer")
        peer_analyses[peer["peer_ts_code"]] = analysis
        peer_rows.append({
            "peer_ts_code": peer["peer_ts_code"],
            "peer_name": peer["peer_name"],
            "provided_report_count": analysis["provided_report_count"],
            "missing_dimensions": analysis["missing_dimensions"],
            "status": analysis["status"],
        })

    # Human-in-loop requests
    human_requests: list[str] = []
    if target_analysis["provided_report_count"] < lookback_years:
        human_requests.append(f"请提供 {ts_code} {target_name or ''} 最近{lookback_years}年的年报全文或全文地址。".strip())
    for dimension in target_analysis["missing_dimensions"]:
        human_requests.append(f"请补充 {ts_code} {target_name or ''} 年报中与{DIMENSION_LABELS[dimension]}相关的原文段落。".strip())

    # Status
    if target_analysis["provided_report_count"] == 0:
        status = "human-in-loop-required"
    elif human_requests:
        status = "partial"
    else:
        status = "ready"

    return {
        "rule_id": "look-04",
        "status": status,
        "stock": ts_code,
        "as_of_date": parsed_date.isoformat(),
        "lookback_years": lookback_years,
        "peer_limit": peer_limit,
        "company_profile": profile.to_payload(),
        "structured_context": structured_context,
        "target_analysis": target_analysis,
        "peer_comparison_rows": peer_rows,
        "human_in_loop_requests": human_requests,
    }


# ── MCP Tool 注册 ────────────────────────────────────────────


def register_look_04_tools(mcp: FastMCP) -> None:
    """注册 look-04 业务与市场分布分析工具。"""

    @mcp.tool()
    def look_04_business_market_distribution(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int = 3,
        peer_limit: int = 5,
        report_bundle_json: str = "",
    ) -> str:
        """四看业务与市场分布。

        从年报全文中提取主营业务构成、海外销售、客户集中度等维度的文本证据，
        并与申万 L3 同行公司做横向对比。需要提供年报全文 JSON 才能产出证据。
        金融类公司返回 status=not-applicable。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"
            as_of_date: 分析截止日期 YYYY-MM-DD，默认今天
            lookback_years: 回看年数，默认 3
            peer_limit: 同行比较最大数量，默认 5
            report_bundle_json: 年报文本 JSON（含 ts_code/year/text 字段的数组或 {reports:[...]}）

        Returns:
            JSON 字符串，含 rule_id, status, structured_context, target_analysis, peer_comparison_rows, human_in_loop_requests
        """
        result = execute_look_04(ts_code, as_of_date, lookback_years, peer_limit, report_bundle_json)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

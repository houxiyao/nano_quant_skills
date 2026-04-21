from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

try:
    from .common import CompanyProfile, detect_company_profile
except ImportError:
    from common import CompanyProfile, detect_company_profile


KEYWORDS = {
    "business_composition": [
        "主营业务",
        "业务构成",
        "收入构成",
        "营业收入构成",
        "分产品",
        "分地区",
        "分行业",
        "主营业务收入",
        "按产品",
        "按地区",
    ],
    "overseas_sales": [
        "境外",
        "海外",
        "国外",
        "外销",
        "出口",
        "国际市场",
        "海外收入",
        "境外收入",
        "境内外",
    ],
    "customer_concentration": [
        "前五大客户",
        "前五名客户",
        "单一客户",
        "客户集中度",
        "第一大客户",
        "前五大客户销售额",
        "前五名客户销售额",
        "销售总额比例",
        "客户销售额占年度销售总额比例",
        "客户依赖",
    ],
}
DIMENSION_LABELS = {
    "business_composition": "主营业务构成",
    "overseas_sales": "海外/境外销售",
    "customer_concentration": "单一客户/前五大客户",
}
STRUCTURED_MISSING_ITEMS = [
    "annual_report_full_text",
    "segment_revenue_mix",
    "regional_sales_split",
    "single_customer_sales",
]


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[4] / "data" / "ashare.duckdb"


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def _object_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _fetch_stock_info(con: duckdb.DuckDBPyConnection, stock: str) -> dict[str, Any] | None:
    if not _object_exists(con, "stk_info"):
        return None
    result = con.execute(
        """
        SELECT ts_code, symbol, name, area, industry, market, list_date, act_name, act_ent_type
        FROM stk_info
        WHERE ts_code = ?
        """,
        [stock],
    )
    row = result.fetchone()
    if row is None:
        return None
    columns = [item[0] for item in result.description]
    payload = {}
    for column, value in zip(columns, row):
        payload[column] = value.isoformat() if isinstance(value, date) else value
    return payload


def _fetch_peer_groups(con: duckdb.DuckDBPyConnection, stock: str) -> list[dict[str, Any]]:
    if not _object_exists(con, "idx_sw_l3_peers"):
        return []
    result = con.execute(
        """
        SELECT DISTINCT anchor_l3_count, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, peer_group_size
        FROM idx_sw_l3_peers
        WHERE anchor_ts_code = ?
        ORDER BY l3_code
        """,
        [stock],
    )
    columns = [item[0] for item in result.description]
    return [{column: value for column, value in zip(columns, record)} for record in result.fetchall()]


def _fetch_peers(con: duckdb.DuckDBPyConnection, stock: str) -> list[dict[str, Any]]:
    if not _object_exists(con, "idx_sw_l3_peers"):
        return []
    result = con.execute(
        """
        SELECT l3_code, l3_name, peer_group_size, peer_ts_code, peer_name
        FROM idx_sw_l3_peers
        WHERE anchor_ts_code = ?
          AND peer_is_self = false
        ORDER BY l3_code, peer_ts_code
        """,
        [stock],
    )
    rows = result.fetchall()
    peers: dict[str, dict[str, Any]] = {}
    for l3_code, l3_name, peer_group_size, peer_ts_code, peer_name in rows:
        peer = peers.setdefault(
            str(peer_ts_code),
            {
                "peer_ts_code": str(peer_ts_code),
                "peer_name": peer_name,
                "l3_codes": [],
                "l3_names": [],
                "peer_group_size_max": 0,
            },
        )
        if l3_code and l3_code not in peer["l3_codes"]:
            peer["l3_codes"].append(l3_code)
        if l3_name and l3_name not in peer["l3_names"]:
            peer["l3_names"].append(l3_name)
        peer["peer_group_size_max"] = max(peer["peer_group_size_max"], int(peer_group_size or 0))
    return sorted(peers.values(), key=lambda item: item["peer_ts_code"])


def _load_report_bundle(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
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
        normalized.append(
            {
                "ts_code": ts_code,
                "name": item.get("name"),
                "year": item.get("year"),
                "url": item.get("url"),
                "text": str(item.get("text") or item.get("content") or ""),
            }
        )
    return normalized


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\u3000", " ")).strip()


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
                    "amounts": list(
                        dict.fromkeys(
                            re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?\s*(?:亿元|万元|元|亿美元|亿港元|万美元)", snippet)
                        )
                    )[:5],
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


def _analyze_report(report: dict[str, Any]) -> dict[str, Any]:
    text = str(report.get("text") or "")
    payload = {
        "ts_code": report["ts_code"],
        "company_name": report.get("name"),
        "year": report.get("year"),
        "url": report.get("url"),
        "text_available": bool(_normalize_text(text)),
    }
    missing = []
    for dimension, keywords in KEYWORDS.items():
        field = f"{dimension}_evidence"
        payload[field] = _collect_windows(text, keywords)
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


def _build_requests(stock: str, target_name: str | None, lookback_years: int, structured_context: dict[str, Any], target_analysis: dict[str, Any], peer_rows: list[dict[str, Any]]) -> list[str]:
    requests: list[str] = []
    if target_analysis["provided_report_count"] < lookback_years:
        requests.append(f"请提供 {stock} {target_name or ''} 最近{lookback_years}年的年报全文或全文地址。".strip())
    for dimension in target_analysis["missing_dimensions"]:
        requests.append(f"请补充 {stock} {target_name or ''} 年报中与{DIMENSION_LABELS[dimension]}相关的原文段落、表格页或全文地址。".strip())
    if not structured_context["sw_peer_view_available"]:
        requests.append("请确认数据库中已经创建 idx_sw_l3_peers 视图，或提供可比公司清单。")
    elif not structured_context["selected_peers"]:
        requests.append(f"当前未从 idx_sw_l3_peers 中找到 {stock} 的同类公司，请确认申万行业成分表和视图是否已同步。")
    for peer_row in peer_rows:
        peer_code = peer_row["peer_ts_code"]
        peer_name = peer_row["peer_name"]
        if peer_row["provided_report_count"] == 0:
            requests.append(f"请提供同行 {peer_code} {peer_name} 最近{lookback_years}年的年报全文或全文地址，用于同类对比。")
            continue
        for dimension in peer_row["missing_dimensions"]:
            requests.append(f"请补充同行 {peer_code} {peer_name} 年报中与{DIMENSION_LABELS[dimension]}相关的原文段落、表格页或全文地址。")
    return list(dict.fromkeys(requests))


def _flatten_rows(target_analysis: dict[str, Any], peer_analyses: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for analysis in [target_analysis, *peer_analyses.values()]:
        for row in analysis["report_rows"]:
            rows.append(
                {
                    "role": analysis["role"],
                    "ts_code": analysis["ts_code"],
                    "company_name": analysis["company_name"],
                    "year": row.get("year"),
                    "url": row.get("url"),
                    "text_available": row.get("text_available"),
                    "business_composition_evidence_count": len(row.get("business_composition_evidence") or []),
                    "overseas_sales_evidence_count": len(row.get("overseas_sales_evidence") or []),
                    "customer_concentration_evidence_count": len(row.get("customer_concentration_evidence") or []),
                    "missing_dimensions": row.get("missing_dimensions") or [],
                }
            )
    return sorted(rows, key=lambda item: (item["role"], item["ts_code"], str(item.get("year") or "")))


def _build_payload(stock: str, as_of_date: date, lookback_years: int, peer_limit: int, profile: CompanyProfile, structured_context: dict[str, Any], target_analysis: dict[str, Any], peer_rows: list[dict[str, Any]], peer_analyses: dict[str, dict[str, Any]], human_requests: list[str]) -> dict[str, Any]:
    if target_analysis["provided_report_count"] == 0:
        status = "human-in-loop-required"
        reason = "当前数据库只有同行池与基础画像，没有年报全文证据。"
    elif human_requests:
        status = "partial"
        reason = "已拿到部分文本证据，但仍存在缺失输入或缺失披露。"
    else:
        status = "ready"
        reason = "已拿到目标公司与同行公司的核心文本证据，可做横向对比。"
    summary = {
        "status": status,
        "status_reason": reason,
        "sw_peer_group_count": len(structured_context["peer_groups"]),
        "sw_peer_candidate_count": structured_context["peer_candidate_count"],
        "selected_peer_count": len(structured_context["selected_peers"]),
        "structured_missing_items": structured_context["structured_missing_items"],
        "target_report_count": target_analysis["provided_report_count"],
        "target_missing_dimensions": target_analysis["missing_dimensions"],
        "business_composition_evidence_count": target_analysis["business_composition_evidence_count"],
        "overseas_sales_evidence_count": target_analysis["overseas_sales_evidence_count"],
        "customer_concentration_evidence_count": target_analysis["customer_concentration_evidence_count"],
        "peer_ready_count": sum(1 for row in peer_rows if row["status"] == "ready"),
        "peer_partial_or_missing_count": sum(1 for row in peer_rows if row["status"] != "ready"),
        "human_request_count": len(human_requests),
    }
    return {
        "rule_id": "look-04",
        "status": status,
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "peer_limit": peer_limit,
        "company_profile": profile.to_payload(),
        "structured_context": structured_context,
        "summary": summary,
        "target_analysis": target_analysis,
        "peer_comparison_rows": peer_rows,
        "human_in_loop_requests": human_requests,
        "rows": _flatten_rows(target_analysis, peer_analyses),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    structured = payload["structured_context"]
    target = payload["target_analysis"]
    lines = [
        "# look-04 Business Market Distribution",
        "",
        f"- stock: {payload['stock']}",
        f"- as_of_date: {payload['as_of_date']}",
        f"- status: {payload['status']}",
        f"- status_reason: {summary['status_reason']}",
        f"- sw_peer_group_count: {summary['sw_peer_group_count']}",
        f"- selected_peer_count: {summary['selected_peer_count']}",
        f"- target_report_count: {summary['target_report_count']}",
        f"- human_request_count: {summary['human_request_count']}",
        f"- stock_name: {(structured.get('stock_info') or {}).get('name', '')}",
        "",
        "## Human In Loop Requests",
    ]
    for request in payload["human_in_loop_requests"] or ["none"]:
        lines.append(f"- {request}")
    lines.extend(
        [
            "",
            "## Target Analysis",
            f"- target_status: {target['status']}",
            f"- business_composition_evidence_count: {target['business_composition_evidence_count']}",
            f"- overseas_sales_evidence_count: {target['overseas_sales_evidence_count']}",
            f"- customer_concentration_evidence_count: {target['customer_concentration_evidence_count']}",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run look-04 business and market distribution analysis")
    parser.add_argument("--stock", required=True)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--lookback-years", type=int, default=3)
    parser.add_argument("--peer-limit", type=int, default=5)
    parser.add_argument("--report-bundle", default=None)
    parser.add_argument("--db-path", default=str(_default_db_path()))
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args()

    if args.lookback_years <= 0:
        raise SystemExit("--lookback-years must be a positive integer")
    if args.peer_limit <= 0:
        raise SystemExit("--peer-limit must be a positive integer")

    as_of_date = _parse_date(args.as_of_date)
    db_path = Path(args.db_path).expanduser().resolve()
    report_bundle = Path(args.report_bundle).expanduser().resolve() if args.report_bundle else None

    with _connect(db_path) as con:
        profile = detect_company_profile(con, args.stock, as_of_date)
        if profile.is_financial:
            payload = {
                "rule_id": "look-04",
                "status": "not-applicable",
                "stock": args.stock,
                "as_of_date": as_of_date.isoformat(),
                "lookback_years": args.lookback_years,
                "peer_limit": args.peer_limit,
                "company_profile": profile.to_payload(),
                "warning": profile.warning,
                "reason": "当前规则针对一般工商业公司设计，金融类公司的业务构成与市场分布口径不可直接类比。",
                "rows": [],
            }
            if args.format == "json":
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                print("\n".join(["# look-04 Business Market Distribution", "", "## Not Applicable", f"- stock: {args.stock}", f"- warning: {profile.warning or ''}", f"- reason: {payload['reason']}"]))
            return
        stock_info = _fetch_stock_info(con, args.stock)
        sw_peer_view_available = _object_exists(con, "idx_sw_l3_peers")
        peer_groups = _fetch_peer_groups(con, args.stock)
        all_peers = _fetch_peers(con, args.stock)

    reports = _load_report_bundle(report_bundle)
    reports_by_company: dict[str, list[dict[str, Any]]] = {}
    for report in reports:
        reports_by_company.setdefault(report["ts_code"], []).append(report)

    selected_peers = all_peers[: args.peer_limit]
    target_name = stock_info.get("name") if stock_info else None
    structured_context = {
        "stock_info": stock_info,
        "sw_peer_view_available": sw_peer_view_available,
        "peer_groups": peer_groups,
        "peer_candidate_count": len(all_peers),
        "selected_peers": selected_peers,
        "structured_missing_items": STRUCTURED_MISSING_ITEMS,
    }
    target_analysis = _summarize_company(args.stock, target_name, reports_by_company.get(args.stock, []), args.lookback_years, "target")
    peer_analyses: dict[str, dict[str, Any]] = {}
    peer_rows = []
    for peer in selected_peers:
        analysis = _summarize_company(peer["peer_ts_code"], peer["peer_name"], reports_by_company.get(peer["peer_ts_code"], []), args.lookback_years, "peer")
        peer_analyses[peer["peer_ts_code"]] = analysis
        peer_rows.append(
            {
                "peer_ts_code": peer["peer_ts_code"],
                "peer_name": peer["peer_name"],
                "l3_codes": peer["l3_codes"],
                "l3_names": peer["l3_names"],
                "peer_group_size_max": peer["peer_group_size_max"],
                "provided_report_count": analysis["provided_report_count"],
                "report_years": analysis["report_years"],
                "business_composition_evidence_count": analysis["business_composition_evidence_count"],
                "overseas_sales_evidence_count": analysis["overseas_sales_evidence_count"],
                "customer_concentration_evidence_count": analysis["customer_concentration_evidence_count"],
                "missing_dimensions": analysis["missing_dimensions"],
                "status": analysis["status"],
                "sample_business_composition_snippet": analysis["sample_business_composition_snippet"],
                "sample_overseas_sales_snippet": analysis["sample_overseas_sales_snippet"],
                "sample_customer_concentration_snippet": analysis["sample_customer_concentration_snippet"],
            }
        )
    human_requests = _build_requests(args.stock, target_name, args.lookback_years, structured_context, target_analysis, peer_rows)
    payload = _build_payload(args.stock, as_of_date, args.lookback_years, args.peer_limit, profile, structured_context, target_analysis, peer_rows, peer_analyses, human_requests)
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(_render_markdown(payload))


if __name__ == "__main__":
    main()

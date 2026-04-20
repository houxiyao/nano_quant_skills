from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

try:
    from .common import CompanyProfile, detect_company_profile
except ImportError:
    from common import CompanyProfile, detect_company_profile


REPORT_TYPE = "1"

HIDDEN_LIABILITY_KEYWORDS = {
    "guarantee": [
        "对外担保",
        "担保余额",
        "担保总额",
        "被担保方",
        "担保金额",
        "担保事项",
        "互保",
    ],
    "contingent_liability": [
        "或有事项",
        "或有负债",
        "潜在义务",
        "未决诉讼",
        "未决仲裁",
    ],
    "off_balance_sheet": [
        "表外安排",
        "表外融资",
        "表外业务",
    ],
    "sale_leaseback": [
        "售后回租",
        "融资租赁",
    ],
    "receivable_transfer": [
        "应收账款转让",
        "保理",
        "出表",
        "应收票据贴现",
        "应收账款融资",
    ],
    "shadow_equity": [
        "明股实债",
        "有限合伙",
        "SPV",
        "结构化主体",
        "特殊目的实体",
    ],
}
HIDDEN_LIABILITY_LABELS = {
    "guarantee": "对外担保",
    "contingent_liability": "或有事项/或有负债",
    "off_balance_sheet": "表外安排",
    "sale_leaseback": "售后回租/融资租赁",
    "receivable_transfer": "应收账款转让/保理出表",
    "shadow_equity": "明股实债/结构化主体",
}


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


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    return float(value)


# ---------------------------------------------------------------------------
# Structured data queries
# ---------------------------------------------------------------------------

def _fetch_balance_cashflow(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    query = f"""
    WITH params AS (
        SELECT
            CAST(? AS VARCHAR)  AS ts_code,
            CAST(? AS DATE)     AS as_of_date,
            CAST(? AS INTEGER)  AS lookback_years
    ),
    balance_yearly AS (
        SELECT
            b.ts_code,
            b.end_date,
            b.comp_type,
            COALESCE(b.f_ann_date, b.ann_date, b.end_date) AS visible_date,
            b.money_cap,
            b.total_cur_assets,
            b.total_assets,
            b.st_borr,
            b.lt_borr,
            b.bond_payable,
            b.lease_liab,
            b.total_cur_liab,
            b.total_liab,
            b.total_hldr_eqy_exc_min_int,
            b.estimated_liab
        FROM fin_balance b
        CROSS JOIN params p
        WHERE b.ts_code = p.ts_code
          AND b.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM b.end_date) = 12
          AND EXTRACT(DAY FROM b.end_date) = 31
          AND COALESCE(b.f_ann_date, b.ann_date, b.end_date) <= p.as_of_date
    ),
    cashflow_yearly AS (
        SELECT
            c.ts_code,
            c.end_date,
            c.n_cashflow_act,
            c.n_cashflow_inv_act,
            c.n_cash_flows_fnc_act,
            c.free_cashflow,
            c.c_cash_equ_end_period
        FROM fin_cashflow c
        CROSS JOIN params p
        WHERE c.ts_code = p.ts_code
          AND c.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM c.end_date) = 12
          AND EXTRACT(DAY FROM c.end_date) = 31
          AND COALESCE(c.f_ann_date, c.ann_date, c.end_date) <= p.as_of_date
    ),
    combined AS (
        SELECT
            b.*,
            c.n_cashflow_act,
            c.n_cashflow_inv_act,
            c.n_cash_flows_fnc_act,
            c.free_cashflow,
            c.c_cash_equ_end_period,
            ROW_NUMBER() OVER (
                PARTITION BY b.ts_code, b.end_date
                ORDER BY b.visible_date DESC
            ) AS rn_dup
        FROM balance_yearly b
        LEFT JOIN cashflow_yearly c
          ON b.ts_code = c.ts_code AND b.end_date = c.end_date
    ),
    deduped AS (
        SELECT * FROM combined WHERE rn_dup = 1
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ts_code ORDER BY end_date DESC
            ) AS rn
        FROM deduped
    )
    SELECT
        ts_code,
        end_date,
        comp_type,
        money_cap,
        total_cur_assets,
        total_assets,
        st_borr,
        lt_borr,
        bond_payable,
        lease_liab,
        total_cur_liab,
        total_liab,
        total_hldr_eqy_exc_min_int,
        estimated_liab,
        n_cashflow_act,
        n_cashflow_inv_act,
        n_cash_flows_fnc_act,
        free_cashflow,
        c_cash_equ_end_period
    FROM ranked
    WHERE rn <= (SELECT lookback_years + 1 FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


def _fetch_indicator_rows(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    query = f"""
    WITH params AS (
        SELECT
            CAST(? AS VARCHAR)  AS ts_code,
            CAST(? AS DATE)     AS as_of_date,
            CAST(? AS INTEGER)  AS lookback_years
    ),
    indicator_yearly AS (
        SELECT
            fi.ts_code,
            fi.end_date,
            COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) AS sort_key,
            fi.ann_date,
            fi.current_ratio,
            fi.quick_ratio,
            fi.cash_ratio,
            fi.debt_to_assets,
            fi.debt_to_eqt,
            fi.assets_to_eqt,
            fi.ebit_to_interest,
            fi.ocf_to_debt,
            fi.ocf_to_shortdebt,
            fi.ocf_to_interestdebt,
            fi.interestdebt,
            fi.netdebt
        FROM fin_indicator fi
        CROSS JOIN params p
        WHERE fi.ts_code = p.ts_code
          AND EXTRACT(MONTH FROM fi.end_date) = 12
          AND EXTRACT(DAY FROM fi.end_date) = 31
          AND COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) <= p.as_of_date
    ),
    deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ts_code, end_date
                ORDER BY sort_key DESC, ann_date DESC
            ) AS rn_dup
        FROM indicator_yearly
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ts_code ORDER BY end_date DESC
            ) AS rn
        FROM deduped
        WHERE rn_dup = 1
    )
    SELECT
        ts_code,
        end_date,
        current_ratio,
        quick_ratio,
        cash_ratio,
        debt_to_assets,
        debt_to_eqt,
        assets_to_eqt,
        ebit_to_interest,
        ocf_to_debt,
        ocf_to_shortdebt,
        ocf_to_interestdebt,
        interestdebt,
        netdebt
    FROM ranked
    WHERE rn <= (SELECT lookback_years + 1 FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


def _merge_rows(
    balance_rows: list[dict[str, Any]],
    indicator_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    indicator_by_date = {row["end_date"]: row for row in indicator_rows}
    merged = []
    for brow in balance_rows:
        irow = indicator_by_date.get(brow["end_date"], {})
        combined = {**brow}
        for key in (
            "current_ratio", "quick_ratio", "cash_ratio",
            "debt_to_assets", "debt_to_eqt", "assets_to_eqt",
            "ebit_to_interest",
            "ocf_to_debt", "ocf_to_shortdebt", "ocf_to_interestdebt",
            "interestdebt", "netdebt",
        ):
            combined[key] = irow.get(key)
        # derive interest-bearing debt from balance sheet if indicator missing
        if _is_missing(combined.get("interestdebt")):
            parts = [
                _float_or_none(brow.get("st_borr")),
                _float_or_none(brow.get("lt_borr")),
                _float_or_none(brow.get("bond_payable")),
                _float_or_none(brow.get("lease_liab")),
            ]
            valid = [p for p in parts if p is not None]
            combined["interestdebt"] = sum(valid) if valid else None
            combined["interestdebt_derived"] = True
        else:
            combined["interestdebt_derived"] = False
        merged.append(combined)
    return merged


# ---------------------------------------------------------------------------
# Cashflow coverage analysis
# ---------------------------------------------------------------------------

def _compute_cashflow_coverage(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    coverage = []
    for row in rows:
        ocf = _float_or_none(row.get("n_cashflow_act"))
        icf = _float_or_none(row.get("n_cashflow_inv_act"))
        fcf_fnc = _float_or_none(row.get("n_cash_flows_fnc_act"))

        ocf_covers_inv = None
        if ocf is not None and icf is not None and icf != 0:
            ocf_covers_inv = ocf + icf  # positive means OCF covers investing outflow

        ocf_covers_all = None
        if ocf is not None and icf is not None and fcf_fnc is not None:
            ocf_covers_all = ocf + icf + fcf_fnc

        coverage.append({
            "end_date": row["end_date"],
            "n_cashflow_act": ocf,
            "n_cashflow_inv_act": icf,
            "n_cash_flows_fnc_act": fcf_fnc,
            "free_cashflow": _float_or_none(row.get("free_cashflow")),
            "ocf_plus_icf": ocf_covers_inv,
            "ocf_covers_investing": ocf_covers_inv is not None and ocf_covers_inv >= 0,
            "net_cash_change": ocf_covers_all,
        })
    return coverage


# ---------------------------------------------------------------------------
# Hidden liability text extraction
# ---------------------------------------------------------------------------

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
        normalized.append({
            "ts_code": ts_code,
            "name": item.get("name"),
            "year": item.get("year"),
            "url": item.get("url"),
            "text": str(item.get("text") or item.get("content") or ""),
        })
    return normalized


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


def _analyze_hidden_liabilities(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        return {
            "status": "human-in-loop-required",
            "reason": "未提供年报附注文本，无法自动提取隐性负债证据。",
            "report_count": 0,
            "reports": [],
        }

    report_results = []
    all_missing = []
    for report in sorted(reports, key=lambda r: int(r.get("year") or 0), reverse=True):
        text = str(report.get("text") or "")
        entry = {
            "ts_code": report["ts_code"],
            "year": report.get("year"),
            "text_available": bool(_normalize_text(text)),
        }
        missing_dims = []
        for dimension, keywords in HIDDEN_LIABILITY_KEYWORDS.items():
            field = f"{dimension}_evidence"
            entry[field] = _collect_windows(text, keywords)
            if not entry[field]:
                missing_dims.append(dimension)
        entry["missing_dimensions"] = missing_dims
        report_results.append(entry)
        all_missing.extend(missing_dims)

    total_evidence = sum(
        len(r.get(f"{dim}_evidence", []))
        for r in report_results
        for dim in HIDDEN_LIABILITY_KEYWORDS
    )
    if total_evidence == 0:
        status = "human-in-loop-required"
        reason = "提供了年报文本但未匹配到任何隐性负债关键词。"
    elif all_missing:
        status = "partial"
        reason = "部分隐性负债维度未找到证据。"
    else:
        status = "ready"
        reason = "所有隐性负债维度均找到至少一条证据。"

    return {
        "status": status,
        "reason": reason,
        "report_count": len(report_results),
        "total_evidence_count": total_evidence,
        "reports": report_results,
    }


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _build_summary(
    merged_rows: list[dict[str, Any]],
    cashflow_coverage: list[dict[str, Any]],
    hidden_result: dict[str, Any],
) -> dict[str, Any]:
    if not merged_rows:
        return {
            "years_returned": 0,
            "latest_end_date": None,
            "oldest_end_date": None,
            "ocf_positive_years": 0,
            "fcf_positive_years": 0,
            "ocf_covers_investing_years": 0,
            "leverage_trend": "unknown",
            "hidden_liability_status": hidden_result["status"],
        }

    latest = merged_rows[0]
    oldest = merged_rows[-1]

    ocf_pos = sum(1 for c in cashflow_coverage if (c["n_cashflow_act"] or 0) > 0)
    fcf_pos = sum(1 for c in cashflow_coverage if (c["free_cashflow"] or 0) > 0)
    ocf_covers = sum(1 for c in cashflow_coverage if c["ocf_covers_investing"])

    # Leverage trend based on assets_to_eqt
    a2e_values = [
        (_float_or_none(r.get("assets_to_eqt")), r["end_date"])
        for r in merged_rows
        if not _is_missing(r.get("assets_to_eqt"))
    ]
    if len(a2e_values) >= 2:
        newest_val = a2e_values[0][0]
        oldest_val = a2e_values[-1][0]
        if newest_val > oldest_val * 1.05:
            leverage_trend = "rising"
        elif newest_val < oldest_val * 0.95:
            leverage_trend = "declining"
        else:
            leverage_trend = "stable"
    else:
        leverage_trend = "insufficient-data"

    def _missing_count(field: str) -> int:
        return sum(1 for r in merged_rows if _is_missing(r.get(field)))

    return {
        "years_returned": len(merged_rows),
        "latest_end_date": latest["end_date"].isoformat() if isinstance(latest["end_date"], date) else str(latest["end_date"]),
        "oldest_end_date": oldest["end_date"].isoformat() if isinstance(oldest["end_date"], date) else str(oldest["end_date"]),
        "ocf_positive_years": ocf_pos,
        "fcf_positive_years": fcf_pos,
        "ocf_covers_investing_years": ocf_covers,
        "leverage_trend": leverage_trend,
        "hidden_liability_status": hidden_result["status"],
        "missing_counts": {
            "n_cashflow_act": _missing_count("n_cashflow_act"),
            "interestdebt": _missing_count("interestdebt"),
            "current_ratio": _missing_count("current_ratio"),
            "debt_to_assets": _missing_count("debt_to_assets"),
            "ebit_to_interest": _missing_count("ebit_to_interest"),
        },
    }


# ---------------------------------------------------------------------------
# Human-in-loop requests
# ---------------------------------------------------------------------------

def _build_requests(
    stock: str,
    lookback_years: int,
    hidden_result: dict[str, Any],
) -> list[str]:
    requests: list[str] = []
    if hidden_result["report_count"] == 0:
        requests.append(
            f"请提供 {stock} 最近{lookback_years}年的年报附注全文（或全文地址），"
            "以便自动提取对外担保、或有事项、表外融资等隐性负债证据。"
        )
    else:
        for report in hidden_result.get("reports", []):
            for dim in report.get("missing_dimensions", []):
                label = HIDDEN_LIABILITY_LABELS.get(dim, dim)
                year = report.get("year", "?")
                requests.append(
                    f"请补充 {stock} {year}年年报附注中与'{label}'相关的段落或表格页。"
                )
    return list(dict.fromkeys(requests))


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _format_number(value: Any) -> str:
    if _is_missing(value):
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, bool):
        return str(value)
    return str(value)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = []
    for row in rows:
        payload.append({
            key: (
                None if _is_missing(value)
                else value.isoformat() if isinstance(value, date)
                else value
            )
            for key, value in row.items()
        })
    return payload


def _render_markdown(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    merged_rows: list[dict[str, Any]],
    cashflow_coverage: list[dict[str, Any]],
    summary: dict[str, Any],
    hidden_result: dict[str, Any],
    human_requests: list[str],
) -> str:
    lines = [
        "# look-05 Balance Sheet Health",
        "",
        f"- stock: {stock}",
        f"- as_of_date: {as_of_date.isoformat()}",
        f"- lookback_years: {lookback_years}",
        f"- company_type: {profile.comp_type_label} ({profile.comp_type or 'unknown'})",
        f"- report_type: {REPORT_TYPE}",
        "",
        "## Summary",
        "",
        f"- years_returned: {summary['years_returned']}",
        f"- ocf_positive_years: {summary['ocf_positive_years']}",
        f"- fcf_positive_years: {summary['fcf_positive_years']}",
        f"- ocf_covers_investing_years: {summary['ocf_covers_investing_years']}",
        f"- leverage_trend: {summary['leverage_trend']}",
        f"- hidden_liability_status: {summary['hidden_liability_status']}",
    ]
    missing = summary.get("missing_counts", {})
    if missing:
        parts = ", ".join(f"{k}={v}" for k, v in missing.items())
        lines.append(f"- missing_counts: {parts}")

    # Cashflow coverage table
    lines.extend(["", "## Cashflow Coverage", ""])
    cf_header = [
        "end_date", "n_cashflow_act", "n_cashflow_inv_act",
        "n_cash_flows_fnc_act", "free_cashflow",
        "ocf_plus_icf", "ocf_covers_investing",
    ]
    lines.append("| " + " | ".join(cf_header) + " |")
    lines.append("|" + "|".join("---" for _ in cf_header) + "|")
    for row in cashflow_coverage:
        values = []
        for col in cf_header:
            v = row.get(col)
            if isinstance(v, date):
                values.append(v.isoformat())
            else:
                values.append(_format_number(v))
        lines.append("| " + " | ".join(values) + " |")
    if not cashflow_coverage:
        lines.append("| no data |" + " |" * (len(cf_header) - 1))

    # Debt & solvency table
    lines.extend(["", "## Debt & Solvency", ""])
    debt_header = [
        "end_date", "interestdebt", "netdebt", "money_cap",
        "debt_to_assets", "debt_to_eqt", "assets_to_eqt",
        "current_ratio", "quick_ratio", "cash_ratio",
        "ebit_to_interest",
        "ocf_to_debt", "ocf_to_shortdebt",
    ]
    lines.append("| " + " | ".join(debt_header) + " |")
    lines.append("|" + "|".join("---" for _ in debt_header) + "|")
    for row in merged_rows:
        values = []
        for col in debt_header:
            v = row.get(col)
            if isinstance(v, date):
                values.append(v.isoformat())
            else:
                values.append(_format_number(v))
        lines.append("| " + " | ".join(values) + " |")
    if not merged_rows:
        lines.append("| no data |" + " |" * (len(debt_header) - 1))

    # Hidden liability section
    lines.extend(["", "## Hidden Liability Evidence", ""])
    lines.append(f"- status: {hidden_result['status']}")
    lines.append(f"- reason: {hidden_result['reason']}")
    if hidden_result.get("total_evidence_count", 0) > 0:
        for report in hidden_result.get("reports", []):
            year = report.get("year", "?")
            lines.append(f"### {year}")
            for dim in HIDDEN_LIABILITY_KEYWORDS:
                field = f"{dim}_evidence"
                evidences = report.get(field, [])
                label = HIDDEN_LIABILITY_LABELS.get(dim, dim)
                if evidences:
                    lines.append(f"#### {label} ({len(evidences)} hits)")
                    for ev in evidences:
                        lines.append(f"- {ev['snippet'][:200]}")

    # Human requests
    lines.extend(["", "## Human In Loop Requests", ""])
    for req in human_requests or ["none"]:
        lines.append(f"- {req}")

    return "\n".join(lines)


def _render_json(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    merged_rows: list[dict[str, Any]],
    cashflow_coverage: list[dict[str, Any]],
    summary: dict[str, Any],
    hidden_result: dict[str, Any],
    human_requests: list[str],
) -> str:
    if human_requests:
        status = "partial" if summary["years_returned"] > 0 else "human-in-loop-required"
    else:
        status = "ready" if summary["years_returned"] > 0 else "no-data"

    payload = {
        "rule_id": "look-05",
        "status": status,
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "summary": summary,
        "cashflow_coverage": _serialize_rows(cashflow_coverage),
        "debt_solvency_rows": _serialize_rows(merged_rows),
        "hidden_liability_analysis": hidden_result,
        "human_in_loop_requests": human_requests,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run look-05 balance sheet health analysis"
    )
    parser.add_argument("--stock", required=True)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--lookback-years", type=int, default=3)
    parser.add_argument("--report-bundle", default=None)
    parser.add_argument("--db-path", default=str(_default_db_path()))
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args()

    if args.lookback_years <= 0:
        raise SystemExit("--lookback-years must be a positive integer")

    as_of_date = _parse_date(args.as_of_date)
    db_path = Path(args.db_path).expanduser().resolve()
    report_bundle_path = (
        Path(args.report_bundle).expanduser().resolve()
        if args.report_bundle
        else None
    )

    with _connect(db_path) as con:
        profile = detect_company_profile(con, args.stock, as_of_date)

        if profile.is_financial:
            payload = {
                "rule_id": "look-05",
                "status": "not-applicable",
                "stock": args.stock,
                "as_of_date": as_of_date.isoformat(),
                "lookback_years": args.lookback_years,
                "company_profile": profile.to_payload(),
                "warning": profile.warning,
                "reason": "当前规则针对一般工商业公司设计，金融类公司的负债结构口径不可直接类比。",
            }
            if args.format == "json":
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                print("\n".join([
                    "# look-05 Balance Sheet Health",
                    "",
                    "## Not Applicable",
                    f"- stock: {args.stock}",
                    f"- warning: {profile.warning or ''}",
                    f"- reason: {payload['reason']}",
                ]))
            return

        balance_rows = _fetch_balance_cashflow(con, args.stock, as_of_date, args.lookback_years)
        indicator_rows = _fetch_indicator_rows(con, args.stock, as_of_date, args.lookback_years)

    merged_rows = _merge_rows(balance_rows, indicator_rows)
    cashflow_coverage = _compute_cashflow_coverage(merged_rows)

    reports = _load_report_bundle(report_bundle_path)
    target_reports = [r for r in reports if r["ts_code"] == args.stock.upper()]
    hidden_result = _analyze_hidden_liabilities(target_reports)
    human_requests = _build_requests(args.stock, args.lookback_years, hidden_result)

    summary = _build_summary(merged_rows, cashflow_coverage, hidden_result)

    if args.format == "json":
        print(_render_json(
            args.stock, as_of_date, args.lookback_years, profile,
            merged_rows, cashflow_coverage, summary,
            hidden_result, human_requests,
        ))
    else:
        print(_render_markdown(
            args.stock, as_of_date, args.lookback_years, profile,
            merged_rows, cashflow_coverage, summary,
            hidden_result, human_requests,
        ))


if __name__ == "__main__":
    main()

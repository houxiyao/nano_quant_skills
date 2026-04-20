from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

try:
    from .common import CompanyProfile, detect_company_profile
except ImportError:
    from common import CompanyProfile, detect_company_profile


REPORT_TYPE = "1"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_db_path() -> Path:
    return _project_root() / "data" / "ashare.duckdb"


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def _fetch_rows(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    query = f"""
    WITH params AS (
        SELECT
            CAST(? AS VARCHAR) AS ts_code,
            CAST(? AS DATE) AS as_of_date,
            CAST(? AS INTEGER) AS lookback_years
    ),
    income_yearly AS (
        SELECT
            i.ts_code,
            i.end_date,
            COALESCE(i.f_ann_date, i.ann_date, i.end_date) AS visible_date,
            i.comp_type,
            i.revenue,
            i.total_revenue,
            i.n_income_attr_p
        FROM fin_income i
        CROSS JOIN params p
        WHERE i.ts_code = p.ts_code
          AND i.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM i.end_date) = 12
          AND EXTRACT(DAY FROM i.end_date) = 31
          AND COALESCE(i.f_ann_date, i.ann_date, i.end_date) <= p.as_of_date
    ),
    balance_yearly AS (
        SELECT
            b.ts_code,
            b.end_date,
            b.goodwill,
            b.total_assets
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
            c.n_disp_subs_oth_biz
        FROM fin_cashflow c
        CROSS JOIN params p
        WHERE c.ts_code = p.ts_code
          AND c.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM c.end_date) = 12
          AND EXTRACT(DAY FROM c.end_date) = 31
          AND COALESCE(c.f_ann_date, c.ann_date, c.end_date) <= p.as_of_date
    ),
    history AS (
        SELECT
            i.ts_code,
            i.end_date,
            i.comp_type,
            i.revenue,
            i.total_revenue,
            i.n_income_attr_p,
            b.goodwill,
            b.total_assets,
            c.n_disp_subs_oth_biz
        FROM income_yearly i
        LEFT JOIN balance_yearly b
          ON i.ts_code = b.ts_code
         AND i.end_date = b.end_date
        LEFT JOIN cashflow_yearly c
          ON i.ts_code = c.ts_code
         AND i.end_date = c.end_date
    ),
    prepared AS (
        SELECT
            h.*,
            CASE
                WHEN h.goodwill IS NOT NULL AND h.total_assets IS NOT NULL AND h.total_assets <> 0
                    THEN h.goodwill / h.total_assets * 100
                ELSE NULL
            END AS goodwill_to_assets,
            CASE
                WHEN h.n_disp_subs_oth_biz IS NOT NULL AND h.total_revenue IS NOT NULL AND h.total_revenue <> 0
                    THEN h.n_disp_subs_oth_biz / h.total_revenue * 100
                ELSE NULL
            END AS acquisition_cash_to_revenue,
            LAG(h.revenue) OVER w AS prev_revenue,
            LAG(h.n_income_attr_p) OVER w AS prev_n_income_attr_p,
            LAG(h.goodwill) OVER w AS prev_goodwill
        FROM history h
        WINDOW w AS (PARTITION BY h.ts_code ORDER BY h.end_date)
    ),
    scored AS (
        SELECT
            p.ts_code,
            p.end_date,
            p.comp_type,
            p.revenue,
            p.total_revenue,
            p.n_income_attr_p,
            p.goodwill,
            p.total_assets,
            p.goodwill_to_assets,
            p.n_disp_subs_oth_biz,
            p.acquisition_cash_to_revenue,
            CASE
                WHEN p.prev_revenue IS NULL OR p.revenue IS NULL OR p.prev_revenue = 0 THEN NULL
                ELSE (p.revenue - p.prev_revenue) / ABS(p.prev_revenue) * 100
            END AS revenue_yoy_calc,
            CASE
                WHEN p.prev_n_income_attr_p IS NULL OR p.n_income_attr_p IS NULL OR p.prev_n_income_attr_p = 0 THEN NULL
                ELSE (p.n_income_attr_p - p.prev_n_income_attr_p) / ABS(p.prev_n_income_attr_p) * 100
            END AS net_profit_yoy_calc,
            CASE
                WHEN p.goodwill IS NULL OR p.prev_goodwill IS NULL THEN NULL
                ELSE p.goodwill - p.prev_goodwill
            END AS goodwill_change,
            CASE
                WHEN (p.goodwill IS NOT NULL AND p.prev_goodwill IS NOT NULL AND p.goodwill - p.prev_goodwill > 0)
                  OR (p.n_disp_subs_oth_biz IS NOT NULL AND p.n_disp_subs_oth_biz > 0)
                THEN TRUE
                ELSE FALSE
            END AS acquisition_signal,
            ROW_NUMBER() OVER (
                PARTITION BY p.ts_code
                ORDER BY p.end_date DESC
            ) AS rn
        FROM prepared p
    )
    SELECT
        ts_code,
        end_date,
        comp_type,
        revenue,
        total_revenue,
        n_income_attr_p,
        goodwill,
        total_assets,
        goodwill_to_assets,
        n_disp_subs_oth_biz,
        acquisition_cash_to_revenue,
        revenue_yoy_calc,
        net_profit_yoy_calc,
        goodwill_change,
        acquisition_signal
    FROM scored
    WHERE rn <= (SELECT lookback_years FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    rows: list[dict[str, Any]] = []
    for record in result.fetchall():
        rows.append({column: value for column, value in zip(columns, record)})
    return rows


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    return float(value)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                key: (None if _is_missing(value) else value.isoformat() if isinstance(value, date) else value)
                for key, value in row.items()
            }
        )
    return payload


def _compute_cagr(start_value: float | None, end_value: float | None, span_years: int) -> float | None:
    if start_value is None or end_value is None or span_years <= 0:
        return None
    if start_value <= 0 or end_value <= 0:
        return None
    return (end_value / start_value) ** (1 / span_years) - 1


def _cagr_reason(metric_name: str, start_value: float | None, end_value: float | None, span_years: int) -> str | None:
    if start_value is None or end_value is None:
        return f"{metric_name} CAGR 无法计算：起点或终点缺失。"
    if span_years <= 0:
        return f"{metric_name} CAGR 无法计算：有效跨度不足。"
    if start_value <= 0 or end_value <= 0:
        return f"{metric_name} CAGR 无法计算：起点或终点非正数。"
    return None


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "years_returned": 0,
            "latest_end_date": None,
            "oldest_end_date": None,
            "span_years": 0,
            "revenue_cagr": None,
            "net_profit_cagr": None,
            "cagr_notes": {
                "revenue": "营业收入 CAGR 无法计算：没有可用年报。",
                "net_profit": "归母净利润 CAGR 无法计算：没有可用年报。",
            },
            "acquisition_signal_years": 0,
            "growth_mode_signal": "unclear",
            "growth_mode_rationale": "没有可用年报。",
            "missing_counts": {},
        }

    latest = rows[0]
    oldest = rows[-1]
    span_years = latest["end_date"].year - oldest["end_date"].year
    revenue_start = _float_or_none(oldest.get("revenue"))
    revenue_end = _float_or_none(latest.get("revenue"))
    net_profit_start = _float_or_none(oldest.get("n_income_attr_p"))
    net_profit_end = _float_or_none(latest.get("n_income_attr_p"))
    revenue_cagr = _compute_cagr(revenue_start, revenue_end, span_years)
    net_profit_cagr = _compute_cagr(net_profit_start, net_profit_end, span_years)

    acquisition_signal_years = sum(1 for row in rows if row.get("acquisition_signal") is True)
    if acquisition_signal_years == 0:
        growth_mode_signal = "likely-endogenous"
        growth_mode_rationale = "窗口内未检测到商誉增长或取得子公司现金支出等并购代理信号。"
    else:
        growth_mode_signal = "acquisition-assisted-or-mixed"
        growth_mode_rationale = "窗口内检测到商誉增长或取得子公司现金支出，增长可能包含并购驱动成分。"

    def _missing_count(field: str) -> int:
        return sum(1 for row in rows if _is_missing(row.get(field)))

    return {
        "years_returned": len(rows),
        "latest_end_date": latest["end_date"].isoformat(),
        "oldest_end_date": oldest["end_date"].isoformat(),
        "span_years": span_years,
        "revenue_cagr": revenue_cagr,
        "net_profit_cagr": net_profit_cagr,
        "cagr_notes": {
            "revenue": _cagr_reason("营业收入", revenue_start, revenue_end, span_years),
            "net_profit": _cagr_reason("归母净利润", net_profit_start, net_profit_end, span_years),
        },
        "acquisition_signal_years": acquisition_signal_years,
        "growth_mode_signal": growth_mode_signal,
        "growth_mode_rationale": growth_mode_rationale,
        "missing_counts": {
            "revenue": _missing_count("revenue"),
            "n_income_attr_p": _missing_count("n_income_attr_p"),
            "goodwill": _missing_count("goodwill"),
            "n_disp_subs_oth_biz": _missing_count("n_disp_subs_oth_biz"),
        },
    }


def _format_number(value: Any) -> str:
    if _is_missing(value):
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _render_markdown(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    rows: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append("# look-03 Growth Trend")
    lines.append("")
    lines.append(f"- stock: {stock}")
    lines.append(f"- as_of_date: {as_of_date.isoformat()}")
    lines.append(f"- lookback_years: {lookback_years}")
    lines.append(f"- company_type: {profile.comp_type_label} ({profile.comp_type or 'unknown'})")
    lines.append(f"- report_type: {REPORT_TYPE}")
    lines.append("- annual_rule: end_date month=12 and day=31")
    lines.append("- growth_quality_rule: distinguish by goodwill growth and acquisition cash proxies")
    lines.append("")

    summary = _build_summary(rows)
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- years_returned: {summary['years_returned']}")
    lines.append(f"- oldest_end_date: {summary['oldest_end_date']}")
    lines.append(f"- latest_end_date: {summary['latest_end_date']}")
    lines.append(f"- span_years: {summary['span_years']}")
    lines.append(
        f"- revenue_cagr: {'' if summary['revenue_cagr'] is None else f'{summary['revenue_cagr'] * 100:.2f}%'}"
    )
    lines.append(
        f"- net_profit_cagr: {'' if summary['net_profit_cagr'] is None else f'{summary['net_profit_cagr'] * 100:.2f}%'}"
    )
    lines.append(f"- acquisition_signal_years: {summary['acquisition_signal_years']}")
    lines.append(f"- growth_mode_signal: {summary['growth_mode_signal']}")
    lines.append(f"- growth_mode_rationale: {summary['growth_mode_rationale']}")
    cagr_notes = summary["cagr_notes"]
    if cagr_notes["revenue"]:
        lines.append(f"- revenue_cagr_note: {cagr_notes['revenue']}")
    if cagr_notes["net_profit"]:
        lines.append(f"- net_profit_cagr_note: {cagr_notes['net_profit']}")
    missing_counts = summary["missing_counts"]
    missing_parts = ", ".join(f"{field}={count}" for field, count in missing_counts.items())
    lines.append(f"- missing_counts: {missing_parts}")
    lines.append("")
    lines.append("## Yearly Evidence")
    lines.append("")
    header = [
        "end_date",
        "revenue",
        "total_revenue",
        "n_income_attr_p",
        "revenue_yoy_calc",
        "net_profit_yoy_calc",
        "goodwill",
        "goodwill_change",
        "goodwill_to_assets",
        "n_disp_subs_oth_biz",
        "acquisition_cash_to_revenue",
        "acquisition_signal",
    ]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows:
        values = []
        for column in header:
            value = row.get(column)
            if isinstance(value, date):
                values.append(value.isoformat())
            else:
                values.append(_format_number(value))
        lines.append("| " + " | ".join(values) + " |")
    if not rows:
        lines.append("| no data |  |  |  |  |  |  |  |  |  |  |  |")
    return "\n".join(lines)


def _render_json(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    rows: list[dict[str, Any]],
) -> str:
    payload = {
        "rule_id": "look-03",
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "report_type": REPORT_TYPE,
        "annual_rule": {"month": 12, "day": 31},
        "growth_quality_rule": "distinguish by goodwill growth and acquisition cash proxies",
        "summary": _build_summary(rows),
        "rows": _serialize_rows(rows),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _render_not_applicable_markdown(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
) -> str:
    lines = [
        "# look-03 Growth Trend",
        "",
        "## Not Applicable",
        "",
        f"- stock: {stock}",
        f"- as_of_date: {as_of_date.isoformat()}",
        f"- lookback_years: {lookback_years}",
        f"- company_type: {profile.comp_type_label} ({profile.comp_type or 'unknown'})",
        f"- source_table: {profile.source_table or ''}",
        f"- latest_end_date: {profile.latest_end_date.isoformat() if profile.latest_end_date else ''}",
        f"- warning: {profile.warning or ''}",
        "- reason: 当前规则针对一般工商业公司设计，金融类公司的增长口径不可直接类比。",
    ]
    return "\n".join(lines)


def _render_not_applicable_json(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
) -> str:
    payload = {
        "rule_id": "look-03",
        "status": "not-applicable",
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "warning": profile.warning,
        "reason": "当前规则针对一般工商业公司设计，金融类公司的增长口径不可直接类比。",
        "rows": [],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run look-03 growth trend analysis")
    parser.add_argument("--stock", required=True, help="Target stock code, e.g. 000001.SZ")
    parser.add_argument("--as-of-date", default=None, help="Analysis date in YYYY-MM-DD format")
    parser.add_argument("--lookback-years", type=int, default=3, help="Number of annual reports to return")
    parser.add_argument("--db-path", default=str(_default_db_path()), help="DuckDB file path")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown", help="Output format")
    args = parser.parse_args()

    if args.lookback_years <= 0:
        raise SystemExit("--lookback-years must be a positive integer")

    as_of_date = _parse_date(args.as_of_date)
    db_path = Path(args.db_path).expanduser().resolve()

    with _connect(db_path) as con:
        profile = detect_company_profile(con, args.stock, as_of_date)
        if profile.is_financial:
            if args.format == "json":
                print(_render_not_applicable_json(args.stock, as_of_date, args.lookback_years, profile))
                return
            print(_render_not_applicable_markdown(args.stock, as_of_date, args.lookback_years, profile))
            return
        rows = _fetch_rows(con, args.stock, as_of_date, args.lookback_years)

    if args.format == "json":
        print(_render_json(args.stock, as_of_date, args.lookback_years, profile, rows))
        return

    print(_render_markdown(args.stock, as_of_date, args.lookback_years, profile, rows))


if __name__ == "__main__":
    main()
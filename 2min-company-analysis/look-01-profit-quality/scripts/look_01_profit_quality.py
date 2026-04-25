from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

try:
    from .common import CompanyProfile, detect_company_profile, connect_read_only
except ImportError:
    from common import CompanyProfile, detect_company_profile, connect_read_only


REPORT_TYPE = "1"
PRIMARY_MARGIN_COLUMN = "netprofit_margin"
SECONDARY_MARGIN_COLUMN = "profit_to_gr"
ALLOWED_MARGIN_COLUMNS = {PRIMARY_MARGIN_COLUMN, SECONDARY_MARGIN_COLUMN}


@dataclass(frozen=True)
class MarginSelection:
    primary_column: str
    fallback_column: str
    total_rows: int
    primary_non_null: int
    fallback_non_null: int

    @property
    def primary_null_rows(self) -> int:
        return self.total_rows - self.primary_non_null

    @property
    def fallback_null_rows(self) -> int:
        return self.total_rows - self.fallback_non_null

    @property
    def primary_null_rate(self) -> float:
        return 0.0 if self.total_rows == 0 else self.primary_null_rows / self.total_rows

    @property
    def fallback_null_rate(self) -> float:
        return 0.0 if self.total_rows == 0 else self.fallback_null_rows / self.total_rows

    @property
    def rationale(self) -> str:
        if self.primary_non_null > self.fallback_non_null:
            return f"{self.primary_column} has fewer nulls than {self.fallback_column} on deduplicated annual reports."
        if self.primary_non_null < self.fallback_non_null:
            return f"{self.fallback_column} has fewer nulls than {self.primary_column} on deduplicated annual reports."
        return (
            f"{self.primary_column} and {self.fallback_column} have the same null coverage on deduplicated annual reports; "
            f"defaulting to {self.primary_column} because it is the standard net margin field."
        )


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_db_path() -> Path:
    return _project_root() / "data" / "ashare.duckdb"


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()



def _choose_margin_column(con: duckdb.DuckDBPyConnection, as_of_date: date) -> MarginSelection:
    query = """
    WITH indicator_yearly AS (
        SELECT
            ind.ts_code,
            ind.end_date,
            ind.netprofit_margin,
            ind.profit_to_gr,
            ROW_NUMBER() OVER (
                PARTITION BY ind.ts_code, ind.end_date
                ORDER BY COALESCE(ind.ann_date_key, ind.ann_date, ind.end_date) DESC,
                         ind.ann_date DESC
            ) AS rn
        FROM fin_indicator ind
        WHERE EXTRACT(MONTH FROM ind.end_date) = 12
          AND EXTRACT(DAY FROM ind.end_date) = 31
          AND COALESCE(ind.ann_date_key, ind.ann_date, ind.end_date) <= CAST(? AS DATE)
    )
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN netprofit_margin IS NOT NULL THEN 1 ELSE 0 END) AS netprofit_margin_non_null,
        SUM(CASE WHEN profit_to_gr IS NOT NULL THEN 1 ELSE 0 END) AS profit_to_gr_non_null
    FROM indicator_yearly
    WHERE rn = 1
    """
    total_rows, netprofit_non_null, profit_to_gr_non_null = con.execute(query, [as_of_date]).fetchone()

    if profit_to_gr_non_null > netprofit_non_null:
        return MarginSelection(
            primary_column=SECONDARY_MARGIN_COLUMN,
            fallback_column=PRIMARY_MARGIN_COLUMN,
            total_rows=total_rows,
            primary_non_null=profit_to_gr_non_null,
            fallback_non_null=netprofit_non_null,
        )

    return MarginSelection(
        primary_column=PRIMARY_MARGIN_COLUMN,
        fallback_column=SECONDARY_MARGIN_COLUMN,
        total_rows=total_rows,
        primary_non_null=netprofit_non_null,
        fallback_non_null=profit_to_gr_non_null,
    )


def _fetch_rows(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
    margin_selection: MarginSelection,
) -> list[dict[str, Any]]:
    primary_column = margin_selection.primary_column
    fallback_column = margin_selection.fallback_column
    if primary_column not in ALLOWED_MARGIN_COLUMNS:
        raise ValueError(f"Invalid primary margin column: {primary_column}")
    if fallback_column not in ALLOWED_MARGIN_COLUMNS:
        raise ValueError(f"Invalid fallback margin column: {fallback_column}")
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
    indicator_yearly_dedup AS (
        SELECT
            t.ts_code,
            t.end_date,
            t.ann_date,
            t.ann_date_key,
            t.profit_dedt,
            t.grossprofit_margin,
            t.netprofit_margin,
            t.profit_to_gr,
            t.tr_yoy,
            t.or_yoy,
            t.dt_netprofit_yoy,
            t.ocf_yoy
        FROM (
            SELECT
                ind.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ind.ts_code, ind.end_date
                    ORDER BY COALESCE(ind.ann_date_key, ind.ann_date, ind.end_date) DESC,
                             ind.ann_date DESC
                ) AS rn
            FROM fin_indicator ind
            CROSS JOIN params p
            WHERE ind.ts_code = p.ts_code
              AND EXTRACT(MONTH FROM ind.end_date) = 12
              AND EXTRACT(DAY FROM ind.end_date) = 31
              AND COALESCE(ind.ann_date_key, ind.ann_date, ind.end_date) <= p.as_of_date
        ) t
        WHERE t.rn = 1
    ),
    cashflow_yearly AS (
        SELECT
            c.ts_code,
            c.end_date,
            COALESCE(c.f_ann_date, c.ann_date, c.end_date) AS visible_date,
            c.n_cashflow_act,
            c.c_pay_acq_const_fiolta
        FROM fin_cashflow c
        CROSS JOIN params p
        WHERE c.ts_code = p.ts_code
          AND c.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM c.end_date) = 12
          AND EXTRACT(DAY FROM c.end_date) = 31
          AND COALESCE(c.f_ann_date, c.ann_date, c.end_date) <= p.as_of_date
    ),
    joined AS (
        SELECT
            i.ts_code,
            i.end_date,
            i.comp_type,
            i.revenue,
            i.total_revenue,
            i.n_income_attr_p,
            ind.profit_dedt,
            ind.grossprofit_margin,
            ind.netprofit_margin,
            ind.profit_to_gr,
            COALESCE(ind.{primary_column}, ind.{fallback_column}) AS selected_netprofit_margin,
            CASE
                WHEN ind.{primary_column} IS NOT NULL THEN '{primary_column}'
                WHEN ind.{fallback_column} IS NOT NULL THEN '{fallback_column}'
                ELSE NULL
            END AS selected_netprofit_margin_source,
            c.n_cashflow_act,
            c.c_pay_acq_const_fiolta,
            CASE
                WHEN c.n_cashflow_act IS NOT NULL AND i.n_income_attr_p IS NOT NULL
                     AND i.n_income_attr_p <> 0
                THEN c.n_cashflow_act / i.n_income_attr_p
                ELSE NULL
            END AS net_profit_cash_ratio,
            CASE
                WHEN c.n_cashflow_act IS NOT NULL AND c.c_pay_acq_const_fiolta IS NOT NULL
                THEN c.n_cashflow_act - c.c_pay_acq_const_fiolta
                ELSE NULL
            END AS fcf,
            ind.tr_yoy,
            ind.or_yoy,
            ind.dt_netprofit_yoy,
            ind.ocf_yoy,
            ROW_NUMBER() OVER (
                PARTITION BY i.ts_code
                ORDER BY i.end_date DESC
            ) AS rn
        FROM income_yearly i
        LEFT JOIN indicator_yearly_dedup ind
          ON i.ts_code = ind.ts_code
         AND i.end_date = ind.end_date
        LEFT JOIN cashflow_yearly c
          ON i.ts_code = c.ts_code
         AND i.end_date = c.end_date
    )
    SELECT
        ts_code,
        end_date,
        comp_type,
        revenue,
        total_revenue,
        n_income_attr_p,
        profit_dedt,
        grossprofit_margin,
        netprofit_margin,
        profit_to_gr,
        selected_netprofit_margin,
        selected_netprofit_margin_source,
        n_cashflow_act,
        c_pay_acq_const_fiolta,
        net_profit_cash_ratio,
        fcf,
        tr_yoy,
        or_yoy,
        dt_netprofit_yoy,
        ocf_yoy
    FROM joined
    WHERE rn <= (SELECT lookback_years FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    rows = []
    for record in result.fetchall():
        row = {column: value for column, value in zip(columns, record)}
        rows.append(row)
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


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def _positive_count(field: str) -> int:
        count = 0
        for row in rows:
            value = _float_or_none(row.get(field))
            if value is not None and value > 0:
                count += 1
        return count

    def _missing_count(field: str) -> int:
        return sum(1 for row in rows if _is_missing(row.get(field)))

    # 净现比（OCF / 归母净利润）：理想值 ≥ 1。低于 1 表明账面利润未落地为现金。
    cash_ratios: list[float] = []
    cash_ratio_below_one_years = 0
    for row in rows:
        ni = _float_or_none(row.get("n_income_attr_p"))
        ratio = _float_or_none(row.get("net_profit_cash_ratio"))
        # 只在归母净利润 > 0 的年份衡量净现比质量；亏损年份净现比本身无经济含义。
        if ratio is not None and ni is not None and ni > 0:
            cash_ratios.append(ratio)
            if ratio < 1.0:
                cash_ratio_below_one_years += 1
    net_profit_cash_ratio_avg = (
        sum(cash_ratios) / len(cash_ratios) if cash_ratios else None
    )

    fcf_positive_years = _positive_count("fcf")

    # 毛利率趋势：按 end_date DESC 排列。若严格单调下降（最老 > 中间 > 最新），
    # 且至少 3 个样本均为非空正值，则标记 declining。
    gm_declining = False
    gm_values: list[float] = []
    for row in rows:
        gm = _float_or_none(row.get("grossprofit_margin"))
        if gm is not None:
            gm_values.append(gm)
    if len(gm_values) >= 3:
        # rows 为 end_date DESC；反转后为时间正序
        chronological = list(reversed(gm_values))
        if all(b < a for a, b in zip(chronological, chronological[1:])):
            gm_declining = True

    latest_end_date = rows[0]["end_date"].isoformat() if rows else None
    return {
        "years_returned": len(rows),
        "latest_end_date": latest_end_date,
        "profit_dedt_positive_years": _positive_count("profit_dedt"),
        "operating_cashflow_positive_years": _positive_count("n_cashflow_act"),
        "fcf_positive_years": fcf_positive_years,
        "net_profit_cash_ratio_avg": net_profit_cash_ratio_avg,
        "net_profit_cash_ratio_samples": len(cash_ratios),
        "net_profit_cash_ratio_below_one_years": cash_ratio_below_one_years,
        "grossprofit_margin_declining_3y": gm_declining,
        "missing_counts": {
            "revenue": _missing_count("revenue"),
            "total_revenue": _missing_count("total_revenue"),
            "n_income_attr_p": _missing_count("n_income_attr_p"),
            "grossprofit_margin": _missing_count("grossprofit_margin"),
            "selected_netprofit_margin": _missing_count("selected_netprofit_margin"),
            "profit_dedt": _missing_count("profit_dedt"),
            "n_cashflow_act": _missing_count("n_cashflow_act"),
            "c_pay_acq_const_fiolta": _missing_count("c_pay_acq_const_fiolta"),
            "net_profit_cash_ratio": _missing_count("net_profit_cash_ratio"),
            "fcf": _missing_count("fcf"),
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
    margin_selection: MarginSelection,
) -> str:
    lines: list[str] = []
    lines.append("# look-01 Profit Quality")
    lines.append("")
    lines.append(f"- stock: {stock}")
    lines.append(f"- as_of_date: {as_of_date.isoformat()}")
    lines.append(f"- lookback_years: {lookback_years}")
    lines.append(f"- company_type: {profile.comp_type_label} ({profile.comp_type or 'unknown'})")
    lines.append(f"- report_type: {REPORT_TYPE}")
    lines.append("- annual_rule: end_date month=12 and day=31")
    lines.append("- revenue_fields: revenue, total_revenue")
    lines.append(f"- selected_netprofit_margin_field: {margin_selection.primary_column}")
    lines.append(f"- fallback_netprofit_margin_field: {margin_selection.fallback_column}")
    lines.append("")
    lines.append("## Data Quality")
    lines.append("")
    lines.append(f"- total_deduplicated_annual_rows: {margin_selection.total_rows}")
    lines.append(f"- {margin_selection.primary_column}_null_rate: {margin_selection.primary_null_rate:.4%}")
    lines.append(f"- {margin_selection.fallback_column}_null_rate: {margin_selection.fallback_null_rate:.4%}")
    lines.append(f"- rationale: {margin_selection.rationale}")
    lines.append("")

    summary = _build_summary(rows)
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- years_returned: {summary['years_returned']}")
    lines.append(f"- latest_end_date: {summary['latest_end_date']}")
    lines.append(f"- profit_dedt_positive_years: {summary['profit_dedt_positive_years']}")
    lines.append(f"- operating_cashflow_positive_years: {summary['operating_cashflow_positive_years']}")
    lines.append(f"- fcf_positive_years: {summary['fcf_positive_years']}")
    npcr_avg = summary.get("net_profit_cash_ratio_avg")
    lines.append(
        f"- net_profit_cash_ratio_avg: {npcr_avg:.3f} (samples={summary['net_profit_cash_ratio_samples']})"
        if npcr_avg is not None
        else f"- net_profit_cash_ratio_avg: n/a (samples={summary['net_profit_cash_ratio_samples']})"
    )
    lines.append(f"- net_profit_cash_ratio_below_one_years: {summary['net_profit_cash_ratio_below_one_years']}")
    lines.append(f"- grossprofit_margin_declining_3y: {summary['grossprofit_margin_declining_3y']}")
    missing_counts = summary["missing_counts"]
    missing_parts = ", ".join(f"{field}={count}" for field, count in missing_counts.items())
    lines.append(f"- missing_counts: {missing_parts}")
    lines.append("")
    lines.append("## Yearly Evidence")
    lines.append("")
    header = [
        "end_date",
        "comp_type",
        "revenue",
        "total_revenue",
        "n_income_attr_p",
        "profit_dedt",
        "grossprofit_margin",
        "netprofit_margin",
        "profit_to_gr",
        "selected_netprofit_margin",
        "selected_netprofit_margin_source",
        "n_cashflow_act",
        "c_pay_acq_const_fiolta",
        "net_profit_cash_ratio",
        "fcf",
        "tr_yoy",
        "or_yoy",
        "dt_netprofit_yoy",
        "ocf_yoy",
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
        lines.append("| no data |" + "|".join("  " for _ in header[1:]) + "|")
    return "\n".join(lines)


def _render_json(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    rows: list[dict[str, Any]],
    margin_selection: MarginSelection,
) -> str:
    payload = {
        "rule_id": "look-01",
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "report_type": REPORT_TYPE,
        "annual_rule": {"month": 12, "day": 31},
        "revenue_fields": ["revenue", "total_revenue"],
        "selected_netprofit_margin_field": margin_selection.primary_column,
        "fallback_netprofit_margin_field": margin_selection.fallback_column,
        "data_quality": {
            "total_deduplicated_annual_rows": margin_selection.total_rows,
            f"{margin_selection.primary_column}_non_null": margin_selection.primary_non_null,
            f"{margin_selection.primary_column}_null_rate": margin_selection.primary_null_rate,
            f"{margin_selection.fallback_column}_non_null": margin_selection.fallback_non_null,
            f"{margin_selection.fallback_column}_null_rate": margin_selection.fallback_null_rate,
            "rationale": margin_selection.rationale,
        },
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
        "# look-01 Profit Quality",
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
        "- reason: 当前规则针对一般工商业公司设计，金融类公司的营收、毛利率、现金流口径不可直接类比。",
    ]
    return "\n".join(lines)


def _render_not_applicable_json(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
) -> str:
    payload = {
        "rule_id": "look-01",
        "status": "not-applicable",
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "warning": profile.warning,
        "reason": "当前规则针对一般工商业公司设计，金融类公司的营收、毛利率、现金流口径不可直接类比。",
        "rows": [],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run look-01 profit quality analysis")
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

    with connect_read_only(db_path) as con:
        profile = detect_company_profile(con, args.stock, as_of_date)
        if profile.is_financial:
            if args.format == "json":
                print(_render_not_applicable_json(args.stock, as_of_date, args.lookback_years, profile))
                return
            print(_render_not_applicable_markdown(args.stock, as_of_date, args.lookback_years, profile))
            return
        margin_selection = _choose_margin_column(con, as_of_date)
        rows = _fetch_rows(con, args.stock, as_of_date, args.lookback_years, margin_selection)

    if args.format == "json":
        print(_render_json(args.stock, as_of_date, args.lookback_years, profile, rows, margin_selection))
        return

    print(_render_markdown(args.stock, as_of_date, args.lookback_years, profile, rows, margin_selection))


if __name__ == "__main__":
    main()
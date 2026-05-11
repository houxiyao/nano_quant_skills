"""look-01: 盈收与利润质量 MCP 工具。"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import (
    CompanyProfile,
    connect_read_only,
    detect_company_profile,
    get_connection,
    parse_date,
)

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
    def primary_null_rate(self) -> float:
        return 0.0 if self.total_rows == 0 else (self.total_rows - self.primary_non_null) / self.total_rows

    @property
    def fallback_null_rate(self) -> float:
        return 0.0 if self.total_rows == 0 else (self.total_rows - self.fallback_non_null) / self.total_rows

    @property
    def rationale(self) -> str:
        if self.primary_non_null > self.fallback_non_null:
            return f"{self.primary_column} 非空率更高，作为净利率主字段。"
        if self.primary_non_null < self.fallback_non_null:
            return f"{self.fallback_column} 非空率更高，已切换为净利率主字段。"
        return f"{self.primary_column} 和 {self.fallback_column} 非空率相同，默认使用 {self.primary_column}。"


def _choose_margin_column(con: Connection, as_of_date: date) -> MarginSelection:
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
    con: Connection,
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
    return [dict(zip(columns, record)) for record in result.fetchall()]


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

    cash_ratios: list[float] = []
    cash_ratio_below_one_years = 0
    for row in rows:
        ni = _float_or_none(row.get("n_income_attr_p"))
        ratio = _float_or_none(row.get("net_profit_cash_ratio"))
        if ratio is not None and ni is not None and ni > 0:
            cash_ratios.append(ratio)
            if ratio < 1.0:
                cash_ratio_below_one_years += 1
    net_profit_cash_ratio_avg = (
        sum(cash_ratios) / len(cash_ratios) if cash_ratios else None
    )

    fcf_positive_years = _positive_count("fcf")

    gm_declining = False
    gm_values: list[float] = []
    for row in rows:
        gm = _float_or_none(row.get("grossprofit_margin"))
        if gm is not None:
            gm_values.append(gm)
    if len(gm_values) >= 3:
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


# ── 内部执行函数（供编排器进程内调用） ─────────────────────────


def execute_look_01(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int = 3,
    *,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 look-01 分析并返回结构化结果。

    双接口之一：供编排器进程内直接调用。
    """
    parsed_date = parse_date(as_of_date or None)

    should_close = False
    if con is None:
        con = get_connection()
        should_close = True

    try:
        profile = detect_company_profile(con, ts_code, parsed_date)

        if profile.is_financial:
            return {
                "rule_id": "look-01",
                "status": "not-applicable",
                "stock": ts_code,
                "as_of_date": parsed_date.isoformat(),
                "lookback_years": lookback_years,
                "company_profile": profile.to_payload(),
                "reason": "当前规则针对一般工商业公司设计，金融类公司的营收、毛利率、现金流口径不可直接类比。",
                "summary": None,
                "rows": [],
            }

        margin_selection = _choose_margin_column(con, parsed_date)
        rows = _fetch_rows(con, ts_code, parsed_date, lookback_years, margin_selection)
    finally:
        if should_close:
            con.close()

    return {
        "rule_id": "look-01",
        "status": "ready",
        "stock": ts_code,
        "as_of_date": parsed_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "data_quality": {
            "total_deduplicated_annual_rows": margin_selection.total_rows,
            "selected_netprofit_margin_field": margin_selection.primary_column,
            "fallback_netprofit_margin_field": margin_selection.fallback_column,
            f"{margin_selection.primary_column}_null_rate": margin_selection.primary_null_rate,
            f"{margin_selection.fallback_column}_null_rate": margin_selection.fallback_null_rate,
            "rationale": margin_selection.rationale,
        },
        "summary": _build_summary(rows),
        "rows": _serialize_rows(rows),
    }


# ── MCP Tool 注册 ────────────────────────────────────────────


def register_look_01_tools(mcp: FastMCP) -> None:
    """注册 look-01 盈收与利润质量工具。"""

    @mcp.tool()
    def look_01_profit_quality(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int = 3,
    ) -> str:
        """一看盈收与利润质量。

        评估公司利润的真实性和现金含量：扣非利润、净现比、自由现金流、毛利率趋势。
        金融类公司(银行/保险/证券)返回 status=not-applicable。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"
            as_of_date: 分析截止日期 YYYY-MM-DD，默认今天
            lookback_years: 回看年数，默认 3

        Returns:
            JSON 字符串，含 rule_id, status, company_profile, data_quality, summary, rows
        """
        result = execute_look_01(ts_code, as_of_date, lookback_years)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

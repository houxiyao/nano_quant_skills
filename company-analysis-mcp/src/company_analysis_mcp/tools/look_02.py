"""look-02: 费用结构分析 MCP 工具。"""

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


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                key: (
                    None if _is_missing(value)
                    else value.isoformat() if isinstance(value, date)
                    else value
                )
                for key, value in row.items()
            }
        )
    return payload


# ── 数据查询 ────────────────────────────────────────────────


def _fetch_rows(
    con: Connection,
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
            i.total_revenue,
            i.sell_exp,
            i.admin_exp,
            i.fin_exp,
            i.rd_exp
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
            t.saleexp_to_gr,
            t.adminexp_of_gr,
            t.finaexp_of_gr,
            t.tr_yoy,
            t.or_yoy
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
    history AS (
        SELECT
            i.ts_code,
            i.end_date,
            i.comp_type,
            i.total_revenue,
            i.sell_exp,
            i.admin_exp,
            i.fin_exp,
            i.rd_exp,
            ind.saleexp_to_gr,
            ind.adminexp_of_gr,
            ind.finaexp_of_gr,
            CASE
                WHEN i.rd_exp IS NOT NULL AND i.total_revenue IS NOT NULL AND i.total_revenue <> 0
                    THEN i.rd_exp / i.total_revenue * 100
                ELSE NULL
            END AS rdexp_to_gr,
            COALESCE(ind.tr_yoy, ind.or_yoy) AS revenue_growth_yoy,
            CASE
                WHEN ind.tr_yoy IS NOT NULL THEN 'tr_yoy'
                WHEN ind.or_yoy IS NOT NULL THEN 'or_yoy'
                ELSE NULL
            END AS revenue_growth_source
        FROM income_yearly i
        LEFT JOIN indicator_yearly_dedup ind
          ON i.ts_code = ind.ts_code
         AND i.end_date = ind.end_date
    ),
    prepared AS (
        SELECT
            h.*,
            LAG(h.sell_exp) OVER w AS prev_sell_exp,
            LAG(h.admin_exp) OVER w AS prev_admin_exp,
            LAG(h.fin_exp) OVER w AS prev_fin_exp,
            LAG(h.rd_exp) OVER w AS prev_rd_exp,
            LAG(h.saleexp_to_gr) OVER w AS prev_saleexp_to_gr,
            LAG(h.adminexp_of_gr) OVER w AS prev_adminexp_of_gr,
            LAG(h.finaexp_of_gr) OVER w AS prev_finaexp_of_gr,
            LAG(h.rdexp_to_gr) OVER w AS prev_rdexp_to_gr
        FROM history h
        WINDOW w AS (PARTITION BY h.ts_code ORDER BY h.end_date)
    ),
    scored AS (
        SELECT
            p.ts_code,
            p.end_date,
            p.comp_type,
            p.total_revenue,
            p.sell_exp,
            p.admin_exp,
            p.fin_exp,
            p.rd_exp,
            p.saleexp_to_gr,
            p.adminexp_of_gr,
            p.finaexp_of_gr,
            p.rdexp_to_gr,
            p.revenue_growth_yoy,
            p.revenue_growth_source,
            CASE
                WHEN p.prev_sell_exp IS NULL OR p.sell_exp IS NULL OR p.prev_sell_exp = 0 THEN NULL
                ELSE (p.sell_exp - p.prev_sell_exp) / ABS(p.prev_sell_exp) * 100
            END AS sell_exp_yoy,
            CASE
                WHEN p.prev_admin_exp IS NULL OR p.admin_exp IS NULL OR p.prev_admin_exp = 0 THEN NULL
                ELSE (p.admin_exp - p.prev_admin_exp) / ABS(p.prev_admin_exp) * 100
            END AS admin_exp_yoy,
            CASE
                WHEN p.prev_fin_exp IS NULL OR p.fin_exp IS NULL OR p.prev_fin_exp = 0 THEN NULL
                ELSE (p.fin_exp - p.prev_fin_exp) / ABS(p.prev_fin_exp) * 100
            END AS fin_exp_yoy,
            CASE
                WHEN p.prev_rd_exp IS NULL OR p.rd_exp IS NULL OR p.prev_rd_exp = 0 THEN NULL
                ELSE (p.rd_exp - p.prev_rd_exp) / ABS(p.prev_rd_exp) * 100
            END AS rd_exp_yoy,
            p.saleexp_to_gr - p.prev_saleexp_to_gr AS saleexp_to_gr_change,
            p.adminexp_of_gr - p.prev_adminexp_of_gr AS adminexp_of_gr_change,
            p.finaexp_of_gr - p.prev_finaexp_of_gr AS finaexp_of_gr_change,
            p.rdexp_to_gr - p.prev_rdexp_to_gr AS rdexp_to_gr_change
        FROM prepared p
    ),
    final_rows AS (
        SELECT
            s.*,
            CASE
                WHEN s.sell_exp_yoy IS NULL OR s.revenue_growth_yoy IS NULL THEN NULL
                WHEN s.sell_exp_yoy > 0 AND s.revenue_growth_yoy <= 0 THEN TRUE
                ELSE FALSE
            END AS sales_growth_mismatch,
            CASE
                WHEN s.rd_exp_yoy IS NULL OR s.revenue_growth_yoy IS NULL THEN NULL
                WHEN s.rd_exp_yoy > 0 AND s.revenue_growth_yoy <= 0 THEN TRUE
                ELSE FALSE
            END AS rd_growth_mismatch,
            ROW_NUMBER() OVER (
                PARTITION BY s.ts_code
                ORDER BY s.end_date DESC
            ) AS rn
        FROM scored s
    )
    SELECT
        ts_code,
        end_date,
        comp_type,
        total_revenue,
        sell_exp,
        admin_exp,
        fin_exp,
        rd_exp,
        saleexp_to_gr,
        adminexp_of_gr,
        finaexp_of_gr,
        rdexp_to_gr,
        revenue_growth_yoy,
        revenue_growth_source,
        sell_exp_yoy,
        admin_exp_yoy,
        fin_exp_yoy,
        rd_exp_yoy,
        saleexp_to_gr_change,
        adminexp_of_gr_change,
        finaexp_of_gr_change,
        rdexp_to_gr_change,
        sales_growth_mismatch,
        rd_growth_mismatch
    FROM final_rows
    WHERE rn <= (SELECT lookback_years FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    rows: list[dict[str, Any]] = []
    for record in result.fetchall():
        rows.append({column: value for column, value in zip(columns, record)})
    return rows


# ── 汇总分析 ────────────────────────────────────────────────


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def _positive_count(field: str) -> int:
        return sum(1 for row in rows if (_float_or_none(row.get(field)) or 0) > 0)

    def _non_null_count(field: str) -> int:
        return sum(1 for row in rows if not _is_missing(row.get(field)))

    def _missing_count(field: str) -> int:
        return sum(1 for row in rows if _is_missing(row.get(field)))

    def _true_count(field: str) -> int:
        return sum(1 for row in rows if row.get(field) is True)

    def _max_abs_change(field: str) -> float | None:
        values = [abs(v) for row in rows if (v := _float_or_none(row.get(field))) is not None]
        return max(values) if values else None

    latest_end_date = rows[0]["end_date"].isoformat() if rows else None
    return {
        "years_returned": len(rows),
        "latest_end_date": latest_end_date,
        "positive_revenue_growth_years": _positive_count("revenue_growth_yoy"),
        "rd_exp_available_years": _non_null_count("rd_exp"),
        "mismatch_counts": {
            "sales_exp_vs_revenue": _true_count("sales_growth_mismatch"),
            "rd_exp_vs_revenue": _true_count("rd_growth_mismatch"),
        },
        "max_abs_ratio_change": {
            "saleexp_to_gr": _max_abs_change("saleexp_to_gr_change"),
            "adminexp_of_gr": _max_abs_change("adminexp_of_gr_change"),
            "finaexp_of_gr": _max_abs_change("finaexp_of_gr_change"),
            "rdexp_to_gr": _max_abs_change("rdexp_to_gr_change"),
        },
        "missing_counts": {
            "sell_exp": _missing_count("sell_exp"),
            "admin_exp": _missing_count("admin_exp"),
            "fin_exp": _missing_count("fin_exp"),
            "rd_exp": _missing_count("rd_exp"),
            "saleexp_to_gr": _missing_count("saleexp_to_gr"),
            "adminexp_of_gr": _missing_count("adminexp_of_gr"),
            "finaexp_of_gr": _missing_count("finaexp_of_gr"),
            "rdexp_to_gr": _missing_count("rdexp_to_gr"),
            "revenue_growth_yoy": _missing_count("revenue_growth_yoy"),
        },
    }


# ── 内部执行接口 ─────────────────────────────────────────────


def execute_look_02(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int = 3,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 look-02 费用结构分析，返回结构化结果字典。"""
    parsed_date = parse_date(as_of_date)
    should_close = con is None
    if con is None:
        con = get_connection()

    try:
        profile: CompanyProfile = detect_company_profile(con, ts_code, parsed_date)
        if profile.is_financial:
            return {
                "rule_id": "look-02",
                "status": "not-applicable",
                "stock": ts_code,
                "as_of_date": parsed_date.isoformat(),
                "lookback_years": lookback_years,
                "company_profile": profile.to_payload(),
                "reason": "当前规则针对一般工商业公司设计，金融类公司的费用率口径不可直接类比。",
                "rows": [],
            }
        rows = _fetch_rows(con, ts_code, parsed_date, lookback_years)
    finally:
        if should_close:
            con.close()

    return {
        "rule_id": "look-02",
        "status": "ready",
        "stock": ts_code,
        "as_of_date": parsed_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "report_type": REPORT_TYPE,
        "annual_rule": {"month": 12, "day": 31},
        "expense_ratio_fields": ["saleexp_to_gr", "adminexp_of_gr", "finaexp_of_gr", "rdexp_to_gr"],
        "revenue_growth_field": "COALESCE(tr_yoy, or_yoy)",
        "summary": _build_summary(rows),
        "rows": _serialize_rows(rows),
    }


# ── MCP Tool 注册 ────────────────────────────────────────────


def register_look_02_tools(mcp: FastMCP) -> None:
    """注册 look-02 费用结构分析工具。"""

    @mcp.tool()
    def look_02_cost_structure(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int = 3,
    ) -> str:
        """二看费用结构。

        分析销售、管理、财务、研发费用占比及同比变化，识别费用增长与营收增长的错配信号。
        金融类公司(银行/保险/证券)返回 status=not-applicable。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"
            as_of_date: 分析截止日期 YYYY-MM-DD，默认今天
            lookback_years: 回看年数，默认 3

        Returns:
            JSON 字符串，含 rule_id, status, company_profile, summary, rows
        """
        result = execute_look_02(ts_code, as_of_date, lookback_years)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

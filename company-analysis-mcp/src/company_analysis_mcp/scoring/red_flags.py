"""red_flags.py — 从七看各维度结果中提取红旗预警。

每个 extractor 函数返回 list[(flag_text, severity)]，severity: "critical" | "warning"。
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _summary_dict(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    summary = data.get("summary")
    return summary if isinstance(summary, dict) else {}


def _is_leverage_trend_deteriorating(trend: str) -> bool:
    return trend in ("deteriorating", "rising")


# ---------------------------------------------------------------------------
# Per-look extractors
# ---------------------------------------------------------------------------


def extract_flags_01(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-01: 盈收与利润质量。"""
    flags: list[tuple[str, str]] = []
    summary = _summary_dict(data)

    profit_positive_years = summary.get("profit_dedt_positive_years")
    total_years = summary.get("years_returned", 0)
    if profit_positive_years is not None and total_years > 0:
        if profit_positive_years == 0:
            flags.append(("扣非利润连续亏损", "critical"))
        elif profit_positive_years < total_years:
            flags.append((f"扣非利润仅{profit_positive_years}/{total_years}年为正", "warning"))

    ocf_positive = summary.get("operating_cashflow_positive_years")
    if ocf_positive is not None and total_years > 0:
        if ocf_positive == 0:
            flags.append(("经营现金流连续为负", "critical"))
        elif ocf_positive < total_years:
            flags.append((f"经营现金流仅{ocf_positive}/{total_years}年为正", "warning"))

    npcr_avg = summary.get("net_profit_cash_ratio_avg")
    npcr_below = summary.get("net_profit_cash_ratio_below_one_years") or 0
    npcr_samples = summary.get("net_profit_cash_ratio_samples") or 0
    if npcr_avg is not None and npcr_samples > 0:
        if npcr_avg < 0.5:
            flags.append(
                (f"净现比均值仅{npcr_avg:.2f}（<0.5），利润未落地为现金", "critical")
            )
        elif npcr_below > 0:
            flags.append(
                (f"净现比有{npcr_below}/{npcr_samples}年<1，利润含金量不足", "warning")
            )

    fcf_positive = summary.get("fcf_positive_years")
    if fcf_positive is not None and total_years > 0:
        if fcf_positive == 0:
            flags.append(("自由现金流连续为负，公司持续失血", "critical"))
        elif fcf_positive < total_years:
            flags.append(
                (f"自由现金流仅{fcf_positive}/{total_years}年为正", "warning")
            )

    if summary.get("grossprofit_margin_declining_3y"):
        flags.append(("毛利率连续>=3年下滑", "warning"))

    return flags


def extract_flags_02(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-02: 费用成本结构。"""
    flags: list[tuple[str, str]] = []
    summary = _summary_dict(data)

    mismatch_counts = summary.get("mismatch_counts", {})
    sales_mismatch = mismatch_counts.get("sales_exp_vs_revenue", 0)
    if sales_mismatch and sales_mismatch > 0:
        flags.append((f"销售费用增长但营收不增长（{sales_mismatch}次）", "warning"))

    return flags


def extract_flags_03(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-03: 增长率趋势。"""
    flags: list[tuple[str, str]] = []
    summary = _summary_dict(data)

    rev_cagr = summary.get("revenue_cagr")
    if rev_cagr is not None and rev_cagr < -0.05:
        flags.append(("营收CAGR为负，收入持续萎缩", "critical"))

    ni_cagr = summary.get("net_profit_cagr")
    if ni_cagr is not None and ni_cagr < -0.10:
        flags.append(("归母净利润CAGR大幅为负", "critical"))

    mode = summary.get("growth_mode_signal", "")
    if mode == "acquisition-assisted-or-mixed":
        flags.append(("增长可能含并购驱动成分", "warning"))

    return flags


def extract_flags_04(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-04: 业务构成与市场分布。"""
    flags: list[tuple[str, str]] = []
    status = data.get("status", "")
    if status in ("human-in-loop-required", "partial"):
        flags.append(("业务构成与市场分布数据不完整，需人工补充年报", "warning"))
    return flags


def extract_flags_05(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-05: 资产负债健康度。"""
    flags: list[tuple[str, str]] = []
    summary = _summary_dict(data)
    rows = data.get("debt_solvency_rows", [])

    leverage_trend = summary.get("leverage_trend", "")
    if _is_leverage_trend_deteriorating(leverage_trend):
        flags.append(("杠杆水平持续恶化", "warning"))

    if rows:
        latest = rows[0] if isinstance(rows, list) else {}
        dta = latest.get("debt_to_assets")
        if dta is not None and dta > 80:
            flags.append((f"资产负债率极高（{dta:.1f}%）", "critical"))

    capex_covers = summary.get("ocf_covers_capex_years")
    capex_samples = summary.get("ocf_covers_capex_samples") or 0
    if capex_covers is not None and capex_samples > 0:
        if capex_covers == 0:
            flags.append(
                (f"最近{capex_samples}年经营现金流均无法覆盖资本开支，靠筹资续命", "critical")
            )
        elif capex_covers < capex_samples / 2:
            flags.append(
                (f"经营现金流仅{capex_covers}/{capex_samples}年能覆盖资本开支", "warning")
            )

    hidden_status = summary.get("hidden_liability_status", "")
    if hidden_status == "human-in-loop-required":
        flags.append(("隐性负债未检测，需人工补充年报附注", "warning"))

    return flags


def extract_flags_06(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-06: 投入产出效率。"""
    flags: list[tuple[str, str]] = []
    summary = _summary_dict(data)

    wc_trend = summary.get("wc_trend", "")
    if wc_trend == "deteriorating":
        flags.append(("营运资金效率持续恶化", "warning"))

    wc_per_rev = summary.get("wc_per_revenue_latest")
    if wc_per_rev is not None and wc_per_rev > 1.0:
        flags.append(("一元收入需要超过一元营运资金", "warning"))

    return flags


def extract_flags_07(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-07: 收益率与资本回报。"""
    flags: list[tuple[str, str]] = []
    summary = _summary_dict(data)

    driver = summary.get("roe_driver", "")
    if driver == "negative-equity":
        flags.append(("资不抵债，杜邦分解完全失效", "critical"))
    elif driver == "negative-roe":
        flags.append(("ROE为负，处于亏损状态", "critical"))
    elif driver == "leverage-driven":
        flags.append(("高ROE主要靠杠杆驱动，非真实盈利能力", "warning"))

    trend = summary.get("roe_trend", "")
    if trend == "deteriorating":
        flags.append(("ROE持续恶化", "warning"))

    return flags


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

FLAG_EXTRACTORS: dict[str, Any] = {
    "look-01": extract_flags_01,
    "look-02": extract_flags_02,
    "look-03": extract_flags_03,
    "look-04": extract_flags_04,
    "look-05": extract_flags_05,
    "look-06": extract_flags_06,
    "look-07": extract_flags_07,
}


def collect_all_flags(
    results: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    """从七看各维度结果中收集所有红旗预警。"""
    all_flags: list[dict[str, str]] = []
    for rule_id, data in sorted(results.items()):
        status = data.get("status", "")
        if status in ("not-applicable", "error"):
            continue
        extractor = FLAG_EXTRACTORS.get(rule_id)
        if extractor:
            for flag_text, severity in extractor(data):
                all_flags.append({
                    "rule_id": rule_id,
                    "flag": flag_text,
                    "severity": severity,
                })
    return all_flags

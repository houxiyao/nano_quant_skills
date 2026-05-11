"""orchestrate_seven_looks.py — 七看财务质量综合评估编排工具。

执行 look-01 ~ look-07 并汇总为综合报告，含红旗提取、质量评分、行动建议。
支持并行执行各维度分析。

双接口：
  - execute_orchestrate_seven_looks(...) → dict  (供内部调用)
  - register_orchestrate_seven_looks_tools(mcp)  (MCP tool 注册)
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any

from mcp.server.fastmcp import FastMCP

from ..scoring.quality_score import compute_quality_score, generate_commentary
from ..scoring.recommendations import (
    collect_human_requests,
    generate_recommendations,
)
from ..scoring.red_flags import collect_all_flags

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 七看维度配置
# ---------------------------------------------------------------------------

LOOK_SPECS: list[dict[str, Any]] = [
    {
        "rule_id": "look-01",
        "title": "盈收与利润质量",
        "default_lookback": 3,
    },
    {
        "rule_id": "look-02",
        "title": "费用成本结构",
        "default_lookback": 3,
    },
    {
        "rule_id": "look-03",
        "title": "增长率趋势",
        "default_lookback": 3,
    },
    {
        "rule_id": "look-04",
        "title": "业务构成与市场分布",
        "default_lookback": 3,
    },
    {
        "rule_id": "look-05",
        "title": "资产负债健康度",
        "default_lookback": 3,
    },
    {
        "rule_id": "look-06",
        "title": "投入产出效率",
        "default_lookback": 3,
    },
    {
        "rule_id": "look-07",
        "title": "收益率与资本回报",
        "default_lookback": 5,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str | None) -> str:
    if not value or not value.strip():
        return date.today().isoformat()
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return date.today().isoformat()


def _summary_dict(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    s = data.get("summary")
    return s if isinstance(s, dict) else {}


def _summarize_one_look(rule_id: str, data: dict[str, Any]) -> str:
    """为每个维度生成一行摘要。"""
    status = data.get("status", "")
    if status == "not-applicable":
        return "金融类公司，不适用"
    if status == "error":
        return f"执行出错: {data.get('error', '')[:60]}"
    if status in ("human-in-loop-required",):
        return "需人工补充年报文本"

    summary = _summary_dict(data)

    if rule_id == "look-01":
        pos = summary.get("profit_dedt_positive_years", "?")
        total = summary.get("years_returned", "?")
        ocf = summary.get("operating_cashflow_positive_years", "?")
        return f"扣非利润为正: {pos}/{total}年, 经营现金流为正: {ocf}/{total}年"

    if rule_id == "look-02":
        mismatch_counts = summary.get("mismatch_counts", {})
        mis = mismatch_counts.get("sales_exp_vs_revenue", 0)
        return f"销售费用/营收不匹配: {mis}次" if mis else "费用与营收匹配度正常"

    if rule_id == "look-03":
        rc = summary.get("revenue_cagr")
        nc = summary.get("net_profit_cagr")
        rc_str = f"{rc*100:.1f}%" if rc is not None else "N/A"
        nc_str = f"{nc*100:.1f}%" if nc is not None else "N/A"
        mode = summary.get("growth_mode_signal", "")
        return f"营收CAGR: {rc_str}, 净利润CAGR: {nc_str}, 模式: {mode}"

    if rule_id == "look-04":
        target = data.get("target_analysis", {})
        biz = target.get("business_composition_evidence_count", 0)
        overseas = target.get("overseas_sales_evidence_count", 0)
        return f"业务构成证据: {biz}条, 海外销售证据: {overseas}条"

    if rule_id == "look-05":
        trend = summary.get("leverage_trend", "")
        hidden = summary.get("hidden_liability_status", "")
        return f"杠杆趋势: {trend}, 隐性负债: {hidden}"

    if rule_id == "look-06":
        wc = summary.get("wc_per_revenue_latest")
        trend = summary.get("wc_trend", "")
        wc_str = f"{wc:.2f}" if wc is not None else "N/A"
        return f"WC/收入: {wc_str}, 趋势: {trend}"

    if rule_id == "look-07":
        driver = summary.get("roe_driver", "")
        trend = summary.get("roe_trend", "")
        roe = summary.get("roe_latest")
        roe_str = f"{roe*100:.2f}%" if roe is not None else "N/A"
        return f"ROE(DuPont): {roe_str}, 驱动: {driver}, 趋势: {trend}"

    return str(summary)[:80] if summary else "已完成"


# ---------------------------------------------------------------------------
# 核心执行
# ---------------------------------------------------------------------------


def execute_orchestrate_seven_looks(
    ts_code: str,
    as_of_date: str = "",
    lookback_years: int | None = None,
    report_bundle_json_04: str = "",
    report_bundle_json_05: str = "",
    employee_bundle_json_06: str = "",
    max_workers: int = 4,
    con: Any = None,
) -> dict[str, Any]:
    """执行七看全量分析并汇总。

    Args:
        ts_code: 股票代码
        as_of_date: 分析日期 YYYY-MM-DD
        lookback_years: 统一回看年数；None 则各维度使用默认值
        report_bundle_json_04: look-04 年报文本包 JSON
        report_bundle_json_05: look-05 年报附注包 JSON
        employee_bundle_json_06: look-06 员工数据包 JSON
        max_workers: 并行线程数
        con: 可选 DuckDB 连接

    Returns:
        综合评估结果字典
    """
    from .look_01 import execute_look_01
    from .look_02 import execute_look_02
    from .look_03 import execute_look_03
    from .look_04 import execute_look_04
    from .look_05 import execute_look_05
    from .look_06 import execute_look_06
    from .look_07 import execute_look_07

    as_of = _parse_date(as_of_date)

    # 构建各维度的执行参数
    def _run_look_01() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 3
        return "look-01", execute_look_01(ts_code, as_of, lb, con=con)

    def _run_look_02() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 3
        return "look-02", execute_look_02(ts_code, as_of, lb, con=con)

    def _run_look_03() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 3
        return "look-03", execute_look_03(ts_code, as_of, lb, con=con)

    def _run_look_04() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 3
        return "look-04", execute_look_04(
            ts_code, as_of, lb,
            report_bundle_json=report_bundle_json_04,
            con=con,
        )

    def _run_look_05() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 3
        return "look-05", execute_look_05(
            ts_code, as_of, lb,
            report_bundle_json=report_bundle_json_05,
            con=con,
        )

    def _run_look_06() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 3
        return "look-06", execute_look_06(
            ts_code, as_of, lb,
            employee_bundle_json=employee_bundle_json_06,
            con=con,
        )

    def _run_look_07() -> tuple[str, dict[str, Any]]:
        lb = lookback_years if lookback_years is not None else 5
        return "look-07", execute_look_07(ts_code, as_of, lb, con=con)

    runners = [
        _run_look_01,
        _run_look_02,
        _run_look_03,
        _run_look_04,
        _run_look_05,
        _run_look_06,
        _run_look_07,
    ]

    # 执行所有维度
    results: dict[str, dict[str, Any]] = {}
    effective_workers = max(1, min(max_workers, 7))

    if effective_workers == 1:
        for runner in runners:
            rid, data = runner()
            results[rid] = data
            logger.info("[七看] %s 完成, status=%s", rid, data.get("status", "unknown"))
    else:
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futures = {pool.submit(runner): runner for runner in runners}
            for fut in as_completed(futures):
                try:
                    rid, data = fut.result()
                    results[rid] = data
                    logger.info("[七看] %s 完成, status=%s", rid, data.get("status", "unknown"))
                except Exception as exc:
                    logger.exception("[七看] 维度执行异常: %s", exc)

    # 汇总
    flags = collect_all_flags(results)
    quality = compute_quality_score(flags)
    human_requests = collect_human_requests(results)
    recommendations = generate_recommendations(results, flags, quality)
    commentary = generate_commentary(ts_code, results, flags, quality)

    # 构建规范化结果
    normalized_results = {
        rid: {
            "rule_id": rid,
            "title": next((s["title"] for s in LOOK_SPECS if s["rule_id"] == rid), rid),
            "status": data.get("status", "unknown"),
            "summary": _summary_dict(data),
            "one_line": _summarize_one_look(rid, data),
        }
        for rid, data in sorted(results.items())
    }

    payload: dict[str, Any] = {
        "framework": "七看财务质量综合评估",
        "stock": ts_code,
        "as_of_date": as_of,
        "lookback_years": lookback_years,
        "quality_score": quality,
        "red_flags": flags,
        "commentary": commentary,
        "human_in_loop_requests": human_requests,
        "recommendations": recommendations,
        "results": normalized_results,
        "raw_results": {rid: data for rid, data in sorted(results.items())},
    }

    return payload


# ---------------------------------------------------------------------------
# MCP Tool 注册
# ---------------------------------------------------------------------------


def register_orchestrate_seven_looks_tools(mcp: FastMCP) -> None:
    """注册七看编排 MCP 工具。"""

    @mcp.tool()
    def orchestrate_seven_looks(
        ts_code: str,
        as_of_date: str = "",
        lookback_years: int | None = None,
        report_bundle_json_04: str = "",
        report_bundle_json_05: str = "",
        employee_bundle_json_06: str = "",
        max_workers: int = 4,
    ) -> str:
        """七看财务质量综合评估 — 执行全部七个维度分析并汇总报告。

        依次/并行执行 look-01~look-07，收集各维度结果，
        提取红旗预警、计算质量评分、生成行动建议。

        Args:
            ts_code: 股票代码（如 000002.SZ）
            as_of_date: 分析日期 YYYY-MM-DD（空串使用今天）
            lookback_years: 统一回看年数（None则各维度用默认值）
            report_bundle_json_04: look-04 年报文本包（JSON字符串）
            report_bundle_json_05: look-05 年报附注包（JSON字符串）
            employee_bundle_json_06: look-06 员工数据包（JSON字符串）
            max_workers: 并行线程数（1=串行，默认4）

        Returns:
            综合评估 JSON（含 quality_score, red_flags, recommendations 等）
        """
        result = execute_orchestrate_seven_looks(
            ts_code=ts_code,
            as_of_date=as_of_date,
            lookback_years=lookback_years,
            report_bundle_json_04=report_bundle_json_04,
            report_bundle_json_05=report_bundle_json_05,
            employee_bundle_json_06=employee_bundle_json_06,
            max_workers=max_workers,
        )
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

"""quality_score.py — 基于红旗预警计算财务质量综合评分。

评分规则：从 100 分起，每个 critical 红旗扣 15 分，每个 warning 扣 5 分，下限为 0。
"""

from __future__ import annotations

from typing import Any


def compute_quality_score(flags: list[dict[str, str]]) -> dict[str, Any]:
    """基于红旗列表计算质量评分。

    Returns:
        {
            "score": int,       # 0-100
            "grade": str,       # A/B/C/D
            "label": str,       # 中文等级说明
            "critical_count": int,
            "warning_count": int,
        }
    """
    score = 100
    criticals = [f for f in flags if f.get("severity") == "critical"]
    warnings = [f for f in flags if f.get("severity") == "warning"]
    score -= len(criticals) * 15
    score -= len(warnings) * 5
    score = max(score, 0)

    if score >= 80:
        grade = "A"
        label = "财务质量良好"
    elif score >= 60:
        grade = "B"
        label = "财务质量一般，存在部分隐患"
    elif score >= 40:
        grade = "C"
        label = "财务质量较差，多项红旗预警"
    else:
        grade = "D"
        label = "财务质量极差，建议高度警惕"

    return {
        "score": score,
        "grade": grade,
        "label": label,
        "critical_count": len(criticals),
        "warning_count": len(warnings),
    }


def generate_commentary(
    stock: str,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
) -> str:
    """基于七看各维度结果生成简短的量化评语。"""
    parts: list[str] = []

    def _summary_dict(data: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(data, dict):
            return {}
        s = data.get("summary")
        return s if isinstance(s, dict) else {}

    grade = quality["grade"]
    score = quality["score"]

    if grade == "A":
        parts.append(f"综合评分 {score}/100，七看维度整体表现良好，未发现明显财务质量隐患。")
    elif grade == "B":
        parts.append(f"综合评分 {score}/100，财务质量总体尚可，但存在个别需要关注的预警信号。")
    elif grade == "C":
        parts.append(f"综合评分 {score}/100，多个维度触发预警红旗，财务质量堪忧，建议审慎对待。")
    else:
        parts.append(f"综合评分 {score}/100，严重红旗密集，财务质量极差，强烈建议回避或做深度尽调。")

    # 利润质量
    r01 = _summary_dict(results.get("look-01", {}))
    ocf_pos = r01.get("operating_cashflow_positive_years")
    total_y = r01.get("years_returned")
    if ocf_pos is not None and total_y and ocf_pos < total_y:
        parts.append(f"利润质量方面，经营现金流仅{ocf_pos}年为正（共{total_y}年），利润含金量不足。")

    # 增长
    r03 = _summary_dict(results.get("look-03", {}))
    rev_cagr = r03.get("revenue_cagr")
    if rev_cagr is not None:
        if rev_cagr < -0.05:
            parts.append(f"增长方面，营收CAGR为{rev_cagr*100:.1f}%，处于明显收缩通道。")
        elif rev_cagr > 0.15:
            parts.append(f"增长方面，营收CAGR为{rev_cagr*100:.1f}%，保持较高增速。")

    # ROE
    r07 = _summary_dict(results.get("look-07", {}))
    driver = r07.get("roe_driver", "")
    if driver == "profitability-driven":
        parts.append("资本回报方面，ROE由盈利能力驱动，属于健康模式。")
    elif driver == "leverage-driven":
        parts.append("资本回报方面，ROE主要依赖杠杆，真实盈利能力有限，需警惕债务风险。")
    elif driver in ("negative-roe", "negative-equity"):
        parts.append("资本回报方面，公司处于亏损或资不抵债状态，需要高度关注。")

    # 资产负债
    r05 = _summary_dict(results.get("look-05", {}))
    lev_trend = r05.get("leverage_trend", "")
    if lev_trend in ("deteriorating", "rising"):
        parts.append("负债健康度方面，杠杆水平逐年攀升，偿债压力持续增大。")

    if not parts:
        parts.append("数据不足或公司类型不适用，无法生成有效评语。")

    return " ".join(parts)

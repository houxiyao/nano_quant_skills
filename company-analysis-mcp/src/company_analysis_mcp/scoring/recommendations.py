"""recommendations.py — 基于七看结果生成行动建议（最多 3 条）。"""

from __future__ import annotations

from typing import Any


# 七看维度标题映射
_LOOK_TITLES: dict[str, str] = {
    "look-01": "盈收与利润质量",
    "look-02": "费用成本结构",
    "look-03": "增长率趋势",
    "look-04": "业务构成与市场分布",
    "look-05": "资产负债健康度",
    "look-06": "投入产出效率",
    "look-07": "收益率与资本回报",
}


def _summary_dict(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    s = data.get("summary")
    return s if isinstance(s, dict) else {}


def generate_recommendations(
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
) -> list[dict[str, str]]:
    """基于七看结果生成最多 3 条下一步行动建议。"""
    recs: list[dict[str, str]] = []

    # 1. 需要年报文本补充
    pending_human = []
    for rid in ("look-04", "look-05"):
        status = results.get(rid, {}).get("status", "")
        if status in ("human-in-loop-required", "partial"):
            pending_human.append(rid)
    if pending_human:
        recs.append({
            "action": "补充年报文本",
            "detail": (
                f"{'、'.join(pending_human)} 需要年报全文/附注文本才能完成分析。"
                "建议下载最近3年年报PDF，提取正文后以 report_bundle_json 参数传入，"
                "以获得业务构成、市场分布和隐性负债的完整证据。"
            ),
            "priority": "high",
        })

    # 2. 严重红旗需深入排查
    critical_rules = list({f["rule_id"] for f in flags if f.get("severity") == "critical"})
    if critical_rules:
        names = "、".join(
            f"{r}（{_LOOK_TITLES.get(r, r)}）" for r in sorted(critical_rules)
        )
        recs.append({
            "action": "深入排查关键风险",
            "detail": (
                f"以下维度触发了严重红旗：{names}。"
                "建议逐项展开该维度的详细报告，"
                "结合管理层讨论与分析（MD&A）、审计报告意见、"
                "以及同行业对比数据进行交叉验证。"
            ),
            "priority": "high",
        })

    # 3. ROE 由杠杆或亏损驱动
    roe_driver = _summary_dict(results.get("look-07", {})).get("roe_driver", "")
    if roe_driver in ("leverage-driven", "negative-roe", "negative-equity") and len(recs) < 3:
        driver_label = {
            "leverage-driven": "杠杆驱动",
            "negative-roe": "亏损",
            "negative-equity": "资不抵债",
        }.get(roe_driver, roe_driver)
        recs.append({
            "action": "分析资本结构可持续性",
            "detail": (
                f"当前ROE质量存在问题（{driver_label}）。"
                "建议进一步分析：(1) 有息负债到期时间表，"
                "(2) 再融资能力评估，(3) 经营现金流能否覆盖利息支出。"
            ),
            "priority": "medium",
        })

    # 4. 营收持续萎缩
    rev_cagr = _summary_dict(results.get("look-03", {})).get("revenue_cagr")
    if rev_cagr is not None and rev_cagr < 0 and len(recs) < 3:
        recs.append({
            "action": "评估收入恢复可能性",
            "detail": (
                f"营收CAGR为{rev_cagr*100:.1f}%，处于收缩趋势。"
                "建议结合行业景气度数据、公司新业务布局、"
                "在手订单/合同负债变化来判断收入是否有望触底回升。"
            ),
            "priority": "medium",
        })

    # 5. 财务质量整体良好
    if quality.get("grade") in ("A", "B") and len(recs) < 3:
        recs.append({
            "action": "进入估值与股东结构分析",
            "detail": (
                "七看财务质量检查未发现严重问题。"
                "建议继续执行八问中的估值合理性（PE/PB/PS）"
                "和股东结构分析，判断当前价格是否具有安全边际。"
            ),
            "priority": "medium",
        })

    return recs[:3]


def collect_human_requests(results: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    """收集七看各维度中需要人工补充的请求。"""
    requests: list[dict[str, str]] = []

    # Look-04
    r04 = results.get("look-04", {})
    if r04.get("status") in ("human-in-loop-required", "partial"):
        human_reqs = r04.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-04",
                    "request": req if isinstance(req, str) else str(req),
                })
        else:
            requests.append({
                "rule_id": "look-04",
                "request": "请提供目标公司最近3年年报全文文本（JSON格式），用于提取业务构成、海外销售和客户集中度证据。",
            })

    # Look-05
    r05 = results.get("look-05", {})
    hidden_status = _summary_dict(r05).get("hidden_liability_status", "")
    if r05.get("status") in ("human-in-loop-required", "partial") or hidden_status in (
        "human-in-loop-required",
        "partial",
    ):
        human_reqs = r05.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-05",
                    "request": req if isinstance(req, str) else str(req),
                })
        else:
            requests.append({
                "rule_id": "look-05",
                "request": "请提供目标公司最近3年年报附注全文文本（JSON格式），用于提取隐性负债（对外担保、表外融资等）证据。",
            })

    # Look-06
    r06 = results.get("look-06", {})
    per_capita_status = _summary_dict(r06).get("per_capita_status", "")
    if per_capita_status in ("human-in-loop-required", "partial") or r06.get("status") == "partial":
        human_reqs = r06.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-06",
                    "request": req if isinstance(req, str) else str(req),
                })

    return requests

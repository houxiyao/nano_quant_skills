"""ask-q1: 行业前景 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_sw_peers
from ..core.collectors import collect_industry_policies, collect_industry_reports


QUESTION_ID = 1
QUESTION_TITLE = "行业前景"

_POSITIVE_KW = ("支持", "鼓励", "扶持", "利好", "高景气", "持续增长", "龙头", "升级", "加快发展")
_NEGATIVE_KW = ("限制", "去产能", "替代", "萎缩", "衰退", "过剩", "下行", "淘汰", "严控")


def _score_sentiment(evidence_list: list) -> tuple[int, int]:
    pos = neg = 0
    for e in evidence_list:
        if e.source_type not in (SourceType.REGULATORY, SourceType.INDUSTRY_REPORT):
            continue
        text = f"{e.title or ''} {e.excerpt or ''}"
        pos += sum(1 for kw in _POSITIVE_KW if kw in text)
        neg += sum(1 for kw in _NEGATIVE_KW if kw in text)
    return pos, neg


def execute_ask_q1(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q1 行业前景分析，返回 EightQuestionAnswer payload。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    industry_sw_l2 = ""
    try:
        peers, ev_peers = probe_sw_peers(con, ts_code, limit=10)
        if ev_peers:
            ans.evidence.append(ev_peers)
        if peers:
            industry_sw_l2 = peers[0].get("l2_name", "") or ""
    finally:
        if should_close:
            con.close()

    if not industry_sw_l2:
        ans.status = "insufficient-evidence"
        ans.missing_inputs.append(f"{ts_code} 无申万 L2 分类映射")
        ans.finalize_status()
        return ans.to_payload()

    rep_res = collect_industry_reports(ts_code, industry_sw_l2=industry_sw_l2, limit=8)
    ans.evidence.extend(rep_res.evidence)
    if rep_res.requires_human:
        ans.missing_inputs.extend(rep_res.missing_inputs)
        ans.human_in_loop_requests.append(f"行业研报采集失败（{rep_res.error_type}）：{rep_res.error}")
    if rep_res.error:
        ans.notes.append(f"industry_reports: {rep_res.error}")

    pol_res = collect_industry_policies(industry_sw_l2)
    ans.evidence.extend(pol_res.evidence)
    if pol_res.requires_human:
        ans.missing_inputs.extend(pol_res.missing_inputs)
        ans.human_in_loop_requests.append(f"行业政策采集失败（{pol_res.error_type}）：{pol_res.error}")
    if pol_res.error:
        ans.notes.append(f"industry_policies: {pol_res.error}")

    has_factual = any(e.source_type in (SourceType.DB, SourceType.REGULATORY) for e in ans.evidence)
    has_view = any(e.source_type == SourceType.INDUSTRY_REPORT for e in ans.evidence)
    report_cnt = sum(1 for e in ans.evidence if e.source_type == SourceType.INDUSTRY_REPORT)
    policy_cnt = sum(1 for e in ans.evidence if e.source_type == SourceType.REGULATORY)

    if has_factual and has_view:
        ans.status = "ready"
        pos, neg = _score_sentiment(ans.evidence)
        net = pos - neg
        rating = 3
        if net >= 3:
            rating = 4
        elif net <= -3:
            rating = 2
        if net >= 6 and policy_cnt >= 2:
            rating = 5
        elif net <= -6 and policy_cnt >= 2:
            rating = 1
        ans.rating = rating
        ans.rating_signals.append(
            f"sentiment_hits pos={pos} neg={neg} net={net}; reports={report_cnt} policies={policy_cnt} → rating={rating}"
        )
        ans.answer = (
            f"公司归属申万 L2 行业【{industry_sw_l2}】；已采集 "
            f"{report_cnt} 条研报、{policy_cnt} 条政策。"
            f"关键词情绪净值 {net}（正 {pos} / 负 {neg}）→ 评级 {rating}。"
        )
        if policy_cnt == 0:
            ans.critical_gaps.append("无产业政策证据，景气度判断置信度降低")
    elif has_factual:
        ans.status = "partial"
        ans.missing_inputs.append(f"补充 {industry_sw_l2} 近 1 年研报/政策证据")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q1_tools(mcp: FastMCP) -> None:
    """注册 Q1 行业前景工具。"""

    @mcp.tool()
    def ask_q1_industry_prospect(ts_code: str) -> str:
        """八问之一：行业前景。

        通过申万分类定位行业，采集行业研报和产业政策，基于关键词情绪判断行业景气度。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q1(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

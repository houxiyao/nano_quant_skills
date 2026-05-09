"""ask-q7: 风险因素 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_pledge, probe_st
from ..core.collectors import collect_announcements, collect_penalties


QUESTION_ID = 7
QUESTION_TITLE = "风险因素"

PLEDGE_WARN_THRESHOLD = 10
PLEDGE_HIGH_RISK_THRESHOLD = 30

_RISK_KEYWORDS = [
    "诉讼", "仲裁", "处罚", "违规", "警示", "退市",
    "终止上市", "实际控制人变更", "关联交易",
]


def execute_ask_q7(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q7 风险因素分析。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    pledge_ratio = None
    st_count = 0
    try:
        pledge_rows, ev_pledge = probe_pledge(con, ts_code, limit=6)
        if ev_pledge:
            ans.evidence.append(ev_pledge)
        if pledge_rows:
            pledge_ratio = pledge_rows[0].get("pledge_ratio")
        st_rows, ev_st = probe_st(con, ts_code, limit=10)
        if ev_st:
            ans.evidence.append(ev_st)
        st_count = len(st_rows)
    finally:
        if should_close:
            con.close()

    pen_res = collect_penalties(ts_code)
    ans.evidence.extend(pen_res.evidence)
    if pen_res.requires_human:
        ans.missing_inputs.extend(pen_res.missing_inputs)
        ans.human_in_loop_requests.append(f"监管处罚采集失败（{pen_res.error_type}）：{pen_res.error}")
    ans.notes.extend(pen_res.notes)
    if pen_res.error:
        ans.notes.append(f"penalties: {pen_res.error}")
        ans.critical_gaps.append("处罚记录未能检索，风险可能被低估")

    risk_ann = collect_announcements(ts_code, keywords=_RISK_KEYWORDS, limit=20)
    ans.evidence.extend(risk_ann.evidence)
    if risk_ann.requires_human:
        ans.missing_inputs.extend(risk_ann.missing_inputs)
        ans.human_in_loop_requests.append(f"风险公告采集失败（{risk_ann.error_type}）：{risk_ann.error}")
    if risk_ann.error:
        ans.notes.append(f"risk announcements: {risk_ann.error}")

    red = 0
    pledge_signal = ""
    if pledge_ratio is not None and pledge_ratio > PLEDGE_HIGH_RISK_THRESHOLD:
        red += 2
        pledge_signal = f"质押比例 {pledge_ratio}% > {PLEDGE_HIGH_RISK_THRESHOLD}% → +2"
    elif pledge_ratio is not None and pledge_ratio > PLEDGE_WARN_THRESHOLD:
        red += 1
        pledge_signal = f"质押比例 {pledge_ratio}% > {PLEDGE_WARN_THRESHOLD}% → +1"
    if st_count > 0:
        red += 2
    penalty_count = sum(1 for e in ans.evidence if e.source_type == SourceType.REGULATORY and "未发现" not in e.excerpt)
    red += min(penalty_count, 3)

    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)
    if has_db or ans.evidence:
        ans.status = "ready"
        if red >= 5:
            ans.rating = 1
        elif red >= 3:
            ans.rating = 2
        elif red >= 1:
            ans.rating = 3
        else:
            ans.rating = 4
        ans.answer = f"风险打分：质押比例={pledge_ratio}%, ST记录={st_count}, 处罚证据={penalty_count}；综合 red_flags={red}。"
        if pledge_signal:
            ans.rating_signals.append(pledge_signal)
        ans.rating_signals.append(f"红旗累计：ST(+{2 if st_count > 0 else 0}) + 处罚(+{min(penalty_count, 3)}) + 质押贡献")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q7_tools(mcp: FastMCP) -> None:
    """注册 Q7 风险因素工具。"""

    @mcp.tool()
    def ask_q7_risk_factors(ts_code: str) -> str:
        """八问之七：风险因素。

        通过股权质押比例、ST 记录、监管处罚和风险公告综合评估风险水平。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q7(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

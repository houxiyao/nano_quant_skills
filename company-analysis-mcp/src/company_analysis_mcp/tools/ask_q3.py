"""ask-q3: 管理团队 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_company_overview, probe_managers, probe_rewards, probe_top_holders
from ..core.collectors import collect_announcements


QUESTION_ID = 3
QUESTION_TITLE = "管理团队"


def execute_ask_q3(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q3 管理团队分析。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    active_count = 0
    top_concentration = 0.0
    try:
        _, ev_company = probe_company_overview(con, ts_code)
        if ev_company:
            ans.evidence.append(ev_company)
        mgrs, ev_mgrs = probe_managers(con, ts_code, limit=30)
        if ev_mgrs:
            ans.evidence.append(ev_mgrs)
        active_count = sum(1 for r in mgrs if not r.get("end_date"))
        _, ev_rew = probe_rewards(con, ts_code, limit=10)
        if ev_rew:
            ans.evidence.append(ev_rew)
        th_rows, ev_top = probe_top_holders(con, ts_code)
        if ev_top:
            ans.evidence.append(ev_top)
        if th_rows:
            top_concentration = sum((r.get("hold_ratio") or 0) for r in th_rows[:10])
    finally:
        if should_close:
            con.close()

    chg_res = collect_announcements(ts_code, keywords=["辞职", "聘任", "换届", "高管", "董事", "监事"], limit=15)
    ans.evidence.extend(chg_res.evidence)
    ans.missing_inputs.extend(chg_res.missing_inputs)
    if chg_res.error:
        ans.notes.append(f"announcements: {chg_res.error}")
    if chg_res.requires_human:
        ans.human_in_loop_requests.append(f"高管变动公告采集失败（{chg_res.error_type}）：{chg_res.error}")

    change_ann_count = sum(
        1 for e in ans.evidence
        if e.source_type == SourceType.PRIMARY
        and any(k in (e.title or "") for k in ("辞职", "聘任", "换届"))
    )

    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)
    if has_db:
        ans.status = "ready"
        rating = 3
        if top_concentration >= 50:
            rating += 1
        if change_ann_count >= 3:
            rating -= 1
        if 0 < active_count < 5:
            rating -= 1
        rating = max(1, min(5, rating))
        ans.rating = rating
        ans.rating_signals.append(f"active_mgrs={active_count} top10_ratio={top_concentration:.1f}% change_ann={change_ann_count} → rating={rating}")
        ans.answer = f"团队档案：现任 {active_count} 人，前十大股东合计 {top_concentration:.1f}%，近期高管变动公告 {change_ann_count} 条。"
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q3_tools(mcp: FastMCP) -> None:
    """注册 Q3 管理团队工具。"""

    @mcp.tool()
    def ask_q3_management(ts_code: str) -> str:
        """八问之三：管理团队。

        分析高管团队稳定性、前十大股东集中度、最近高管变动公告频率。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q3(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

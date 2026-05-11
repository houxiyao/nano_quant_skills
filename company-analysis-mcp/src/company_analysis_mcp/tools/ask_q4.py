"""ask-q4: 财务真实性 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_cash_ratio, probe_name_history
from ..core.collectors import collect_announcements


QUESTION_ID = 4
QUESTION_TITLE = "财务真实性"

_AUDIT_KEYWORDS = [
    "审计", "非标", "保留意见", "无法表示", "否定意见",
    "问询", "关注函", "立案", "会计差错更正", "更正", "更名",
]


def execute_ask_q4(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q4 财务真实性分析。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    cash_rows: list[dict[str, Any]] = []
    name_changes = 0
    try:
        cash_rows, ev_cash = probe_cash_ratio(con, ts_code, years=5)
        if ev_cash:
            ans.evidence.append(ev_cash)
        nh_rows, ev_nh = probe_name_history(con, ts_code)
        if ev_nh:
            ans.evidence.append(ev_nh)
            name_changes = len(nh_rows)
    finally:
        if should_close:
            con.close()

    audit_res = collect_announcements(ts_code, keywords=_AUDIT_KEYWORDS, limit=20)
    ans.evidence.extend(audit_res.evidence)
    if audit_res.requires_human:
        ans.missing_inputs.extend(audit_res.missing_inputs)
        ans.human_in_loop_requests.append(f"审计/问询公告采集失败（{audit_res.error_type}）：{audit_res.error}")
    if audit_res.error:
        ans.notes.append(f"announcements: {audit_res.error}")
        ans.critical_gaps.append("审计/问询/立案类公告未能检索，财务真实性置信度降低")

    red_flags = 0
    for r in cash_rows:
        val = r.get("ocf_to_profit")
        if val is not None and val < 0.3:
            red_flags += 1
    if name_changes >= 3:
        red_flags += 1

    audit_buckets = ["问询", "立案", "更正", "保留意见", "非标"]
    hit_buckets: set[str] = set()
    for e in ans.evidence:
        if not e.title:
            continue
        for kw in audit_buckets:
            if kw in e.title:
                hit_buckets.add(kw)
    red_flags += min(len(hit_buckets), 3)

    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)
    if has_db:
        ans.status = "ready"
        if red_flags >= 3:
            ans.rating = 2
        elif red_flags >= 1:
            ans.rating = 3
        else:
            ans.rating = 4
        ans.rating_signals.append(
            f"cash_low_years={sum(1 for r in cash_rows if (r.get('ocf_to_profit') or 1) < 0.3)} "
            f"name_changes={name_changes} audit_buckets={sorted(hit_buckets)} → red_flags={red_flags} rating={ans.rating}"
        )
        ans.answer = f"红旗信号合计 {red_flags} 条。命中的审计/问询关键词桶：{sorted(hit_buckets) or '无'}。"
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q4_tools(mcp: FastMCP) -> None:
    """注册 Q4 财务真实性工具。"""

    @mcp.tool()
    def ask_q4_financial_integrity(ts_code: str) -> str:
        """八问之四：财务真实性。

        通过净现比、更名历史、审计/问询/立案公告等多维度检测财务造假红旗。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q4(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

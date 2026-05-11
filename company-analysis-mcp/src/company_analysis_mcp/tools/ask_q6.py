"""ask-q6: 业务模式 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_mainbz
from ..core.collectors import collect_annual_reports


QUESTION_ID = 6
QUESTION_TITLE = "业务模式"


def _analyze_mainbz(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"top1_share": 0.0, "item_change_ratio": 0.0, "new_items_latest": 0, "years": 0}
    by_year: dict[Any, list[dict[str, Any]]] = {}
    for r in rows:
        by_year.setdefault(r["end_date"], []).append(r)
    years_sorted = sorted(by_year.keys(), reverse=True)
    latest = by_year[years_sorted[0]]
    latest_total = sum((r.get("bz_sales") or 0) for r in latest) or 1
    top1_share = (latest[0].get("bz_sales") or 0) / latest_total if latest else 0.0

    latest_items = {r.get("bz_item") for r in latest}
    previous_items: set = set()
    for y in years_sorted[1:]:
        previous_items.update({r.get("bz_item") for r in by_year[y]})
    new_items = latest_items - previous_items if previous_items else set()

    earliest_items = {r.get("bz_item") for r in by_year[years_sorted[-1]]}
    diff = latest_items.symmetric_difference(earliest_items)
    change_ratio = len(diff) / max(len(latest_items | earliest_items), 1)
    return {
        "top1_share": top1_share,
        "item_change_ratio": change_ratio,
        "new_items_latest": len(new_items),
        "years": len(by_year),
    }


def execute_ask_q6(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q6 业务模式分析。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    mainbz_rows: list[dict[str, Any]] = []
    try:
        mainbz_rows, ev = probe_mainbz(con, ts_code, years=3)
        if ev:
            ans.evidence.append(ev)
    finally:
        if should_close:
            con.close()

    rep_res = collect_annual_reports(ts_code, limit=3, fetch_content=True)
    ans.evidence.extend(rep_res.evidence)
    if rep_res.requires_human:
        ans.missing_inputs.extend(rep_res.missing_inputs)
        ans.human_in_loop_requests.append(f"年报采集失败（{rep_res.error_type}）：{rep_res.error}")
    if rep_res.error:
        ans.notes.append(f"annual_reports: {rep_res.error}")

    stats = _analyze_mainbz(mainbz_rows)
    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)

    if has_primary and stats["years"] >= 2:
        ans.status = "ready"
        rating = 3
        if stats["top1_share"] > 0.8:
            rating -= 1
        if stats["item_change_ratio"] > 0.5:
            rating -= 1
        if stats["new_items_latest"] >= 1:
            rating += 1
        rating = max(1, min(5, rating))
        ans.rating = rating
        ans.rating_signals.append(
            f"years={stats['years']} top1={stats['top1_share']:.2f} "
            f"change_ratio={stats['item_change_ratio']:.2f} new_items={stats['new_items_latest']} → rating={rating}"
        )
        ans.answer = (
            f"覆盖 {stats['years']} 个会计年度；单一产品占比 {stats['top1_share']*100:.1f}%，"
            f"业务条目变动率 {stats['item_change_ratio']*100:.1f}%，"
            f"最新年新增条目 {stats['new_items_latest']} 个。"
        )
    elif stats["years"] >= 2:
        ans.status = "partial"
        ans.missing_inputs.append(f"需要 {ts_code} 年报正文以确认业务模式描述")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q6_tools(mcp: FastMCP) -> None:
    """注册 Q6 业务模式工具。"""

    @mcp.tool()
    def ask_q6_business_model(ts_code: str) -> str:
        """八问之六：业务模式。

        通过主营产品集中度、条目变动率和年报正文分析业务模式稳定性。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q6(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

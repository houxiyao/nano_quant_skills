"""ask-q2: 竞争优势 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_mainbz, probe_sw_peers
from ..core.collectors import collect_annual_reports


QUESTION_ID = 2
QUESTION_TITLE = "竞争优势"

_MOAT_POSITIVE = ("自主研发", "专利", "品牌", "龙头", "市占率第一", "行业领先", "核心技术", "独家")
_MOAT_NEGATIVE = ("毛利率下降", "竞争加剧", "价格战", "替代", "受冲击", "依赖许可", "依赖行政")


def _mainbz_concentration(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    latest_end = rows[0]["end_date"]
    latest_rows = [r for r in rows if r["end_date"] == latest_end]
    total = sum((r.get("bz_sales") or 0) for r in latest_rows) or 1
    top3 = sum((r.get("bz_sales") or 0) for r in latest_rows[:3])
    return top3 / total


def execute_ask_q2(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q2 竞争优势分析。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    rep_res = collect_annual_reports(ts_code, limit=2, fetch_content=True)
    ans.evidence.extend(rep_res.evidence)
    ans.missing_inputs.extend(rep_res.missing_inputs)
    if rep_res.error:
        ans.notes.append(f"annual_reports: {rep_res.error}")
    if rep_res.requires_human:
        ans.human_in_loop_requests.append(f"年报采集失败（{rep_res.error_type}）：{rep_res.error}")

    mainbz_rows: list[dict[str, Any]] = []
    peers_count = 0
    try:
        mainbz_rows, ev_mainbz = probe_mainbz(con, ts_code, years=3)
        if ev_mainbz:
            ans.evidence.append(ev_mainbz)
        peers, ev_peers = probe_sw_peers(con, ts_code, limit=5)
        if ev_peers:
            ans.evidence.append(ev_peers)
        if peers:
            peers_count = peers[0].get("peer_group_size") or 0
    finally:
        if should_close:
            con.close()

    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)
    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)

    if has_primary and has_db:
        ans.status = "ready"
        concentration = _mainbz_concentration(mainbz_rows)
        primary_text = " ".join(e.excerpt or "" for e in ans.evidence if e.source_type == SourceType.PRIMARY)
        pos_kw = sum(primary_text.count(kw) for kw in _MOAT_POSITIVE)
        neg_kw = sum(primary_text.count(kw) for kw in _MOAT_NEGATIVE)
        rating = 3
        if concentration >= 0.6 and peers_count >= 10:
            rating += 1
        if pos_kw >= 3:
            rating += 1
        if neg_kw >= 2:
            rating -= 1
        rating = max(1, min(5, rating))
        ans.rating = rating
        ans.rating_signals.append(f"concentration={concentration:.2f} peers={peers_count} pos_kw={pos_kw} neg_kw={neg_kw} → rating={rating}")
        ans.answer = (
            f"年报正文 + 3 年主营 + 同行池 {peers_count} 家；"
            f"前三集中度 {concentration*100:.1f}%，护城河正信号 {pos_kw} / 负 {neg_kw}。"
        )
    elif has_db and not has_primary:
        ans.status = "partial"
        ans.missing_inputs.append(f"需要 {ts_code} 最新年报全文以提取竞争优势描述")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q2_tools(mcp: FastMCP) -> None:
    """注册 Q2 竞争优势工具。"""

    @mcp.tool()
    def ask_q2_moat(ts_code: str) -> str:
        """八问之二：竞争优势（护城河）。

        通过主营业务集中度、同行规模和年报关键词分析竞争壁垒。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q2(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

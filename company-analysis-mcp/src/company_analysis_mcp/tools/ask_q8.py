"""ask-q8: 未来规划 MCP 工具。"""

from __future__ import annotations

import json
from typing import Any

from ..core.connection import Connection
from mcp.server.fastmcp import FastMCP

from ..core.common import get_connection
from ..core.domain import EightQuestionAnswer, SourceType
from ..core.probes import open_connection, probe_express, probe_forecast
from ..core.collectors import collect_annual_reports, collect_ir_meetings


QUESTION_ID = 8
QUESTION_TITLE = "未来规划"


def execute_ask_q8(
    ts_code: str,
    con: Connection | None = None,
) -> dict[str, Any]:
    """执行 Q8 未来规划分析。"""
    should_close = con is None
    if con is None:
        con = get_connection()

    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    forecast_row: dict[str, Any] | None = None
    try:
        fc_rows, ev_fc = probe_forecast(con, ts_code, limit=4)
        if ev_fc:
            ans.evidence.append(ev_fc)
        if fc_rows:
            forecast_row = fc_rows[0]
        _, ev_ex = probe_express(con, ts_code, limit=4)
        if ev_ex:
            ans.evidence.append(ev_ex)
    finally:
        if should_close:
            con.close()

    ir_res = collect_ir_meetings(ts_code, limit=15)
    ans.evidence.extend(ir_res.evidence)
    if ir_res.requires_human:
        ans.missing_inputs.extend(ir_res.missing_inputs)
        ans.human_in_loop_requests.append(f"IR 纪要采集失败（{ir_res.error_type}）：{ir_res.error}")
    if ir_res.error:
        ans.notes.append(f"ir_meetings: {ir_res.error}")

    rep_res = collect_annual_reports(ts_code, limit=1, fetch_content=True)
    ans.evidence.extend(rep_res.evidence)
    if rep_res.requires_human:
        ans.missing_inputs.extend(rep_res.missing_inputs)
        ans.human_in_loop_requests.append(f"年报采集失败（{rep_res.error_type}）：{rep_res.error}")
    if rep_res.error:
        ans.notes.append(f"annual_reports: {rep_res.error}")

    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)
    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)

    if has_db or has_primary:
        ans.status = "ready"
        ans.rating = 3
        forecast_mid: float | None = None
        if forecast_row:
            pmin = forecast_row.get("p_change_min")
            pmax = forecast_row.get("p_change_max")
            bounds = [v for v in (pmin, pmax) if v is not None]
            if bounds:
                forecast_mid = sum(bounds) / len(bounds)
                if forecast_mid > 30:
                    ans.rating = 4
                elif forecast_mid < -30:
                    ans.rating = 2

        ir_count = sum(1 for e in ans.evidence if e.source_type == SourceType.IR_MEETING)
        primary_count = sum(1 for e in ans.evidence if e.source_type == SourceType.PRIMARY)
        db_count = sum(1 for e in ans.evidence if e.source_type == SourceType.DB)
        if forecast_mid is not None:
            ans.rating_signals.append(f"业绩预告净利变动中位数 {forecast_mid:.1f}% → rating={ans.rating}")
        else:
            ans.rating_signals.append("无业绩预告区间，基线 rating=3")
        ans.rating_signals.append(f"证据构成：IR 纪要 {ir_count} / 年报 {primary_count} / DB 指标 {db_count}")
        ans.answer = "已采集业绩预告/快报 + IR 调研纪要 + 最新年报。IR 纪要为公司口径。"
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans.to_payload()


def register_ask_q8_tools(mcp: FastMCP) -> None:
    """注册 Q8 未来规划工具。"""

    @mcp.tool()
    def ask_q8_future_plan(ts_code: str) -> str:
        """八问之八：未来规划。

        通过业绩预告/快报、IR 调研纪要和年报战略段落评估公司未来方向。

        Args:
            ts_code: A 股 Tushare 代码，如 "000002.SZ"

        Returns:
            JSON 字符串，含 question_id, status, rating, evidence, answer 等
        """
        result = execute_ask_q8(ts_code)
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

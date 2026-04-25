"""Q8: 未来规划。

- [公司口径] IR 调研纪要 fetch_ir_meeting_list
- [事实·DB] fin_forecast 业绩预告
- [事实·DB] fin_express 业绩快报
- [事实] 年报 future_strategy 段落
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

def _find_analysis_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if parent.name != "2min-company-analysis":
            continue
        marker = parent / "seven-look-eight-question" / "scripts" / "eight_questions_domain.py"
        if marker.exists():
            return parent
    raise RuntimeError(f"Cannot locate 2min-company-analysis root from: {current}")


_SHARED_SCRIPTS_DIR = _find_analysis_root() / "seven-look-eight-question" / "scripts"
if str(_SHARED_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_SCRIPTS_DIR))

from eight_questions_domain import EightQuestionAnswer, SourceType
from structured_evidence_probes import open_connection, probe_express, probe_forecast
from external_evidence_collectors import collect_annual_reports, collect_ir_meetings
from single_question_cli import run_single_question_cli


QUESTION_ID = 8
QUESTION_TITLE = "未来规划"


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    forecast_row: dict[str, Any] | None = None
    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.finalize_status()
        return ans
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
        con.close()

    ir_res = collect_ir_meetings(ts_code, limit=15)
    ans.evidence.extend(ir_res.evidence)
    # H2: missing_inputs 仅在 requires_human 时写入
    if ir_res.requires_human:
        ans.missing_inputs.extend(ir_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"IR 纪要采集失败（{ir_res.error_type}）：{ir_res.error}"
        )
    if ir_res.error:
        ans.notes.append(f"ir_meetings: {ir_res.error}")

    rep_res = collect_annual_reports(ts_code, limit=1, fetch_content=True)
    ans.evidence.extend(rep_res.evidence)
    if rep_res.requires_human:
        ans.missing_inputs.extend(rep_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"年报采集失败（{rep_res.error_type}）：{rep_res.error}"
        )
    if rep_res.error:
        ans.notes.append(f"annual_reports: {rep_res.error}")

    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)
    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)

    if has_db or has_primary:
        ans.status = "ready"
        # 粗评：以最新预告的净利变动中位数为 proxy
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

        # S3: ready 分支补 rating_signals，保证渲染时"评级依据"非空
        ir_count = sum(1 for e in ans.evidence if e.source_type == SourceType.IR_MEETING)
        primary_count = sum(1 for e in ans.evidence if e.source_type == SourceType.PRIMARY)
        db_count = sum(1 for e in ans.evidence if e.source_type == SourceType.DB)
        if forecast_mid is not None:
            ans.rating_signals.append(
                f"业绩预告净利变动中位数 {forecast_mid:.1f}% → rating={ans.rating}"
            )
        else:
            ans.rating_signals.append("无业绩预告区间，基线 rating=3")
        ans.rating_signals.append(
            f"证据构成：IR 纪要 {ir_count} / 年报 {primary_count} / DB 指标 {db_count}"
        )

        ans.answer = (
            "已采集业绩预告/快报 + IR 调研纪要 + 最新年报；IR 纪要为公司口径，"
            "渲染时会打 [公司口径·IR 调研] 标记。"
        )
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q08_future", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

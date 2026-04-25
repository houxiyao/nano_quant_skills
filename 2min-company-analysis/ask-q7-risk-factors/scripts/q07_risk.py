"""Q7: 风险因素。

- [事实] 监管处罚 fetch_penalty_list
- [事实] 诉讼/处罚类公告
- [事实·DB] stk_pledge_stat 质押比例
- [事实·DB] stk_st_daily ST 警示
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
from structured_evidence_probes import open_connection, probe_pledge, probe_st
from external_evidence_collectors import collect_announcements, collect_penalties
from single_question_cli import run_single_question_cli


QUESTION_ID = 7
QUESTION_TITLE = "风险因素"

PLEDGE_WARN_THRESHOLD = 10
PLEDGE_HIGH_RISK_THRESHOLD = 30


_RISK_KEYWORDS = [
    "诉讼", "仲裁", "处罚", "违规", "警示", "退市",
    "终止上市", "实际控制人变更", "关联交易",
]


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    pledge_ratio = None
    st_count = 0
    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.finalize_status()
        return ans
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
        con.close()

    pen_res = collect_penalties(ts_code)
    ans.evidence.extend(pen_res.evidence)
    # H2: missing_inputs 仅在 requires_human 时写入
    if pen_res.requires_human:
        ans.missing_inputs.extend(pen_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"监管处罚采集失败（{pen_res.error_type}）：{pen_res.error}"
        )
    ans.notes.extend(pen_res.notes)
    if pen_res.error:
        ans.notes.append(f"penalties: {pen_res.error}")
    if pen_res.error:
        ans.critical_gaps.append("处罚记录未能检索，风险可能被低估")

    risk_ann = collect_announcements(ts_code, keywords=_RISK_KEYWORDS, limit=20)
    ans.evidence.extend(risk_ann.evidence)
    if risk_ann.requires_human:
        ans.missing_inputs.extend(risk_ann.missing_inputs)
        ans.human_in_loop_requests.append(
            f"风险公告采集失败（{risk_ann.error_type}）：{risk_ann.error}"
        )
    if risk_ann.error:
        ans.notes.append(f"risk announcements: {risk_ann.error}")

    red = 0
    pledge_signal = ""
    if pledge_ratio is not None and pledge_ratio > PLEDGE_HIGH_RISK_THRESHOLD:
        red += 2
        pledge_signal = (
            f"质押比例 {pledge_ratio}% > {PLEDGE_HIGH_RISK_THRESHOLD}%（内部前瞻风控阈值，低于监管高比例常见 50% 口径）→ +2"
        )
    elif pledge_ratio is not None and pledge_ratio > PLEDGE_WARN_THRESHOLD:
        red += 1
        pledge_signal = f"质押比例 {pledge_ratio}% > {PLEDGE_WARN_THRESHOLD}%（预警阈值）→ +1"
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
        ans.answer = (
            f"风险打分：质押比例={pledge_ratio}%, ST 记录={st_count}, "
            f"处罚证据={penalty_count}；综合 red_flags={red}。"
        )
        if pledge_signal:
            ans.rating_signals.append(pledge_signal)
        ans.rating_signals.append(
            f"红旗累计：ST(+{2 if st_count > 0 else 0}) + 处罚(+{min(penalty_count, 3)}) + 质押规则贡献"
        )
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q07_risk", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

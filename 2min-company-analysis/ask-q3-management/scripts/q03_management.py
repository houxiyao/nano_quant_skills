"""Q3: 管理团队。

评级逻辑
--------
- 必须有 DB 结构化证据才给 ready。
- 动态评级基线 3：
  +1: 前十大股东集中度 ≥50%（控制权清晰）
  -1: 最近高管变动公告数 ≥3（稳定性差）
  -1: 在任高管数 < 5（团队薄弱）
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
from structured_evidence_probes import (
    open_connection,
    probe_company_overview,
    probe_managers,
    probe_rewards,
    probe_top_holders,
)
from external_evidence_collectors import collect_announcements
from single_question_cli import run_single_question_cli


QUESTION_ID = 3
QUESTION_TITLE = "管理团队"


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.critical_gaps.append(f"DuckDB 不可访问: {exc}")
        ans.finalize_status()
        return ans

    active_count = 0
    top_concentration = 0.0
    try:
        company, ev_company = probe_company_overview(con, ts_code)
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
        con.close()

    chg_res = collect_announcements(
        ts_code,
        keywords=["辞职", "聘任", "换届", "高管", "董事", "监事"],
        limit=15,
    )
    ans.evidence.extend(chg_res.evidence)
    ans.missing_inputs.extend(chg_res.missing_inputs)
    if chg_res.error:
        ans.notes.append(f"announcements: {chg_res.error}")
    if chg_res.requires_human:
        ans.human_in_loop_requests.append(
            f"高管变动公告采集失败（{chg_res.error_type}）：{chg_res.error}"
        )

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
        if active_count > 0 and active_count < 5:
            rating -= 1
        rating = max(1, min(5, rating))
        ans.rating = rating
        ans.rating_signals.append(
            f"active_mgrs={active_count} top10_ratio={top_concentration:.1f}% "
            f"change_ann={change_ann_count} → rating={rating}"
        )
        ans.answer = (
            f"团队档案：现任 {active_count} 人，前十大股东合计 {top_concentration:.1f}%，"
            f"近期高管变动公告 {change_ann_count} 条。"
        )
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q03_management", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

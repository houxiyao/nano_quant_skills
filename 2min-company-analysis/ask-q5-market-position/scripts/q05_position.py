"""Q5: 市场地位。

评级逻辑
--------
- 必须有 年报正文 + DB 主营 + 同行池 才给 ready。
- 动态评级基线 3：
  +1: 同行池规模 ≥20 且 主营前3集中度 ≥60%（规模地位）
  +1: 年报正文命中"市占率第一/龙头/行业领先"
  -1: 年报命中"市场份额下降/客户流失/竞争激烈"
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
from structured_evidence_probes import open_connection, probe_mainbz, probe_sw_peers
from external_evidence_collectors import collect_annual_reports
from single_question_cli import run_single_question_cli


QUESTION_ID = 5
QUESTION_TITLE = "市场地位"


_LEADER_KW = ("市占率第一", "龙头", "行业领先", "市场第一", "份额领先")
_LAGGING_KW = ("份额下降", "客户流失", "竞争激烈", "份额萎缩")


def _top3_concentration(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    latest_end = rows[0]["end_date"]
    latest_rows = [r for r in rows if r["end_date"] == latest_end]
    total = sum((r.get("bz_sales") or 0) for r in latest_rows) or 1
    top3 = sum((r.get("bz_sales") or 0) for r in latest_rows[:3])
    return top3 / total


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

    mainbz_rows: list[dict[str, Any]] = []
    peers_count = 0
    try:
        mainbz_rows, ev_mainbz = probe_mainbz(con, ts_code, years=3)
        if ev_mainbz:
            ans.evidence.append(ev_mainbz)
        peers, ev_peers = probe_sw_peers(con, ts_code, limit=10)
        if ev_peers:
            ans.evidence.append(ev_peers)
        if peers:
            peers_count = peers[0].get("peer_group_size") or 0
    finally:
        con.close()

    rep_res = collect_annual_reports(ts_code, limit=2, fetch_content=True)
    ans.evidence.extend(rep_res.evidence)
    ans.missing_inputs.extend(rep_res.missing_inputs)
    if rep_res.error:
        ans.notes.append(f"annual_reports: {rep_res.error}")
    if rep_res.requires_human:
        ans.human_in_loop_requests.append(
            f"年报采集失败（{rep_res.error_type}）：{rep_res.error}"
        )

    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)
    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)

    if has_primary and has_db:
        ans.status = "ready"
        concentration = _top3_concentration(mainbz_rows)
        primary_text = " ".join(
            e.excerpt or "" for e in ans.evidence if e.source_type == SourceType.PRIMARY
        )
        leader = sum(primary_text.count(kw) for kw in _LEADER_KW)
        lagging = sum(primary_text.count(kw) for kw in _LAGGING_KW)
        rating = 3
        if peers_count >= 20 and concentration >= 0.6:
            rating += 1
        if leader >= 1:
            rating += 1
        if lagging >= 2:
            rating -= 1
        rating = max(1, min(5, rating))
        ans.rating = rating
        ans.rating_signals.append(
            f"peers={peers_count} concentration={concentration:.2f} "
            f"leader_kw={leader} lagging_kw={lagging} → rating={rating}"
        )
        ans.answer = (
            f"主营构成 + 同行池 {peers_count} 家 + 最新年报；前三集中度 {concentration*100:.1f}%，"
            f"龙头信号 {leader} / 落后信号 {lagging}。"
            "请从 evidence 中提取前五大客户 + 市占率线索做最终评级。"
        )
    elif has_db:
        ans.status = "partial"
        ans.missing_inputs.append(f"需要 {ts_code} 最新年报全文以提取客户/市占率")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q05_position", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

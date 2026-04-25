"""Q2: 竞争优势。

评级逻辑
--------
- 必须同时具备 年报正文证据 + DuckDB 主营数据 才给 ready。
- 动态评级基线 3：
  +1: 主营前3项集中度 ≥60% 且同行池规模 ≥10
  +1: 年报 excerpt 命中"自主研发/专利/品牌/龙头"等护城河词 ≥3 次
  -1: 年报 excerpt 命中"毛利率下降/竞争加剧/价格战"等负信号 ≥2 次
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


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
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
        ans.human_in_loop_requests.append(
            f"年报采集失败（{rep_res.error_type}）：{rep_res.error}"
        )

    mainbz_rows: list[dict[str, Any]] = []
    peers_count = 0
    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.critical_gaps.append(f"DuckDB 不可访问: {exc}")
        ans.finalize_status()
        return ans
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
        con.close()

    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)
    has_db = any(e.source_type == SourceType.DB for e in ans.evidence)

    if has_primary and has_db:
        ans.status = "ready"
        concentration = _mainbz_concentration(mainbz_rows)
        primary_text = " ".join(
            e.excerpt or "" for e in ans.evidence if e.source_type == SourceType.PRIMARY
        )
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
        ans.rating_signals.append(
            f"concentration={concentration:.2f} peers={peers_count} "
            f"pos_kw={pos_kw} neg_kw={neg_kw} → rating={rating}"
        )
        ans.answer = (
            f"年报正文 {sum(1 for e in ans.evidence if e.source_type == SourceType.PRIMARY)} 份 + "
            f"3 年主营 + 同行池 {peers_count} 家；"
            f"前三集中度 {concentration*100:.1f}%，护城河正信号 {pos_kw} / 负 {neg_kw}。"
            " 请核阅 evidence 中 MD&A 片段判断护城河类型（品牌/技术/规模/渠道/许可）。"
        )
    elif has_db and not has_primary:
        ans.status = "partial"
        ans.missing_inputs.append(f"需要 {ts_code} 最新年报全文以提取竞争优势描述")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q02_moat", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

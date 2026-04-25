"""Q4: 财务真实性。

高危信号：
- 审计非标意见 / 问询函 / 立案调查
- 频繁更名（stk_name_history）
- 净现比（ocf/profit）长期 < 0.5
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
from structured_evidence_probes import open_connection, probe_cash_ratio, probe_name_history
from external_evidence_collectors import collect_announcements
from single_question_cli import run_single_question_cli


QUESTION_ID = 4
QUESTION_TITLE = "财务真实性"


_AUDIT_KEYWORDS = [
    "审计", "非标", "保留意见", "无法表示", "否定意见",
    "问询", "关注函", "立案", "会计差错更正", "更正", "更名",
]


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    cash_rows: list[dict[str, Any]] = []
    name_changes = 0
    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.finalize_status()
        return ans

    try:
        cash_rows, ev_cash = probe_cash_ratio(con, ts_code, years=5)
        if ev_cash:
            ans.evidence.append(ev_cash)

        nh_rows, ev_nh = probe_name_history(con, ts_code)
        if ev_nh:
            ans.evidence.append(ev_nh)
            name_changes = len(nh_rows)
    finally:
        con.close()

    # 审计/问询/立案公告
    audit_res = collect_announcements(ts_code, keywords=_AUDIT_KEYWORDS, limit=20)
    ans.evidence.extend(audit_res.evidence)
    # H2: 仅当采集真正失败（requires_human）时才登记 missing_inputs，避免 "成功但空结果" 污染
    if audit_res.requires_human:
        ans.missing_inputs.extend(audit_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"审计/问询公告采集失败（{audit_res.error_type}）：{audit_res.error}"
        )
    if audit_res.error:
        ans.notes.append(f"announcements: {audit_res.error}")
        ans.critical_gaps.append("审计/问询/立案类公告未能检索，财务真实性置信度降低")

    # 粗评：低净现比 + 负面公告 ⇒ 降级
    # H5(方案 C): 阈值 0.5 → 0.3 避免对 tushare `ocf_to_profit` 通用口径误伤，
    # 与 look-01 归母口径差异在 SKILL.md 中文档化。
    red_flags = 0
    for r in cash_rows:
        val = r.get("ocf_to_profit")
        if val is not None and val < 0.3:
            red_flags += 1
    if name_changes >= 3:
        red_flags += 1
    # L1: 按关键词 bucket 去重（同一关键词无论命中多少条公告仅计 1），
    # 避免单一事件多条公告（关注函+回复+更正）重复加分。
    audit_buckets = ["问询", "立案", "更正", "保留意见", "非标"]
    hit_buckets: set[str] = set()
    for e in ans.evidence:
        if not e.title:
            continue
        for kw in audit_buckets:
            if kw in e.title:
                hit_buckets.add(kw)
    audit_flags = min(len(hit_buckets), 3)
    red_flags += audit_flags

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
            f"cash_low_years(ocf_to_profit<0.3)={sum(1 for r in cash_rows if (r.get('ocf_to_profit') or 1) < 0.3)} "
            f"name_changes={name_changes} audit_buckets={sorted(hit_buckets)} "
            f"→ red_flags={red_flags} rating={ans.rating}"
        )
        ans.answer = (
            f"红旗信号合计 {red_flags} 条（低净现比年数 + 更名次数 + 问询/立案/更正公告 bucket 数）。"
            f" 命中的审计/问询关键词桶：{sorted(hit_buckets) or '无'}。"
            " 注意：本处净现比使用 tushare `fin_indicator.ocf_to_profit`（通用口径，分母含少数股东），"
            "与 look-01 的归母口径 (`n_cashflow_act / n_income_attr_p`) 存在差异，仅作方向性红旗；"
            "rating 为规则初评，需人工审阅 evidence 中具体公告标题。"
        )
    else:
        ans.status = "insufficient-evidence"
    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q04_integrity", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

"""Q6: 业务模式。

评级逻辑
--------
- 必须有 年报正文 + ≥2 年主营数据 才给 ready。
- 动态评级基线 3：
  -1: 主营前1项占比 > 80%（单一产品依赖）
  -1: 主营项目 3 年内变动比例 > 50%（业务频繁切换）
  +1: 最新年末出现新业务条目（可能是第二曲线）
"""

from __future__ import annotations

import json
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
from structured_evidence_probes import open_connection, probe_mainbz
from external_evidence_collectors import collect_annual_reports
from single_question_cli import run_single_question_cli


QUESTION_ID = 6
QUESTION_TITLE = "业务模式"
RULE_REGISTRY_PATH = _SHARED_SCRIPTS_DIR.parent / "assets" / "rule_registry.json"


def _load_q06_thresholds() -> dict[str, float]:
    defaults = {
        "top1_share_warning": 0.8,
        "item_change_ratio_warning": 0.5,
        "new_items_latest_bonus": 1,
    }
    try:
        payload = json.loads(RULE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return defaults

    for rule in payload.get("rules", []):
        if not isinstance(rule, dict):
            continue
        if rule.get("rule_id") != "question-06":
            continue
        params = rule.get("derived_metric_thresholds")
        if not isinstance(params, dict):
            return defaults
        loaded = dict(defaults)
        for key in loaded:
            value = params.get(key)
            if isinstance(value, (int, float)):
                loaded[key] = float(value)
        return loaded
    return defaults


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

    # 稳定性：计算最新 vs 最早年主营条目差异
    earliest_items = {r.get("bz_item") for r in by_year[years_sorted[-1]]}
    diff = latest_items.symmetric_difference(earliest_items)
    change_ratio = len(diff) / max(len(latest_items | earliest_items), 1)
    return {
        "top1_share": top1_share,
        "item_change_ratio": change_ratio,
        "new_items_latest": len(new_items),
        "years": len(by_year),
    }


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    mainbz_rows: list[dict[str, Any]] = []
    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.critical_gaps.append(f"DuckDB 不可访问: {exc}")
        ans.finalize_status()
        return ans
    try:
        mainbz_rows, ev = probe_mainbz(con, ts_code, years=3)
        if ev:
            ans.evidence.append(ev)
    finally:
        con.close()

    rep_res = collect_annual_reports(ts_code, limit=3, fetch_content=True)
    ans.evidence.extend(rep_res.evidence)
    # H2: 仅在 requires_human 时登记 missing_inputs，避免 "成功但空结果" 污染
    if rep_res.requires_human:
        ans.missing_inputs.extend(rep_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"年报采集失败（{rep_res.error_type}）：{rep_res.error}"
        )
    if rep_res.error:
        ans.notes.append(f"annual_reports: {rep_res.error}")

    stats = _analyze_mainbz(mainbz_rows)
    thresholds = _load_q06_thresholds()
    has_primary = any(e.source_type == SourceType.PRIMARY for e in ans.evidence)

    if has_primary and stats["years"] >= 2:
        ans.status = "ready"
        rating = 3
        if stats["top1_share"] > thresholds["top1_share_warning"]:
            rating -= 1
        if stats["item_change_ratio"] > thresholds["item_change_ratio_warning"]:
            rating -= 1
        if stats["new_items_latest"] >= int(thresholds["new_items_latest_bonus"]):
            rating += 1
        rating = max(1, min(5, rating))
        ans.rating = rating
        ans.rating_signals.append(
            f"years={stats['years']} top1={stats['top1_share']:.2f} "
            f"change_ratio={stats['item_change_ratio']:.2f} "
            f"new_items={stats['new_items_latest']} → rating={rating}"
        )
        ans.answer = (
            f"覆盖 {stats['years']} 个会计年度；单一产品占比 {stats['top1_share']*100:.1f}%，"
            f"业务条目变动率 {stats['item_change_ratio']*100:.1f}%，"
            f"最新年新增条目 {stats['new_items_latest']} 个。"
            " 请基于 evidence 判断商业模式类型（B2B/B2C/订阅/项目制）与稳定性。"
        )
    elif stats["years"] >= 2:
        ans.status = "partial"
        ans.missing_inputs.append(f"需要 {ts_code} 年报正文以确认业务模式描述")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q06_business_model", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())

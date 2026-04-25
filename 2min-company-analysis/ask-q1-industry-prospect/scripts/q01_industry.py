"""Q1: 行业前景。

证据策略
--------
- [事实] DuckDB idx_sw_l3_peers → 申万 L1/L2/L3 分类
- [观点] nano_search_mcp industry_reports → 行业研报（预测·券商观点）
- [事实] nano_search_mcp industry_policies → gov.cn 产业政策（依赖 DASHSCOPE_API_KEY）

评级规则（仅在至少 1 条 primary/regulatory/db + 1 条 industry_report 时给 rating）
- 基线 3；按下列信号做 ±1：
  +1: excerpt 命中"支持/鼓励/扶持/政策利好/高景气/持续增长"
  -1: excerpt 命中"限制/去产能/替代/萎缩/衰退/过剩/下行"
- 研报 ≥5 且 政策 ≥1 → 数据充分可升至 4；缺政策则降 1 级
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
from structured_evidence_probes import open_connection, probe_sw_peers
from external_evidence_collectors import collect_industry_policies, collect_industry_reports
from single_question_cli import run_single_question_cli


QUESTION_ID = 1
QUESTION_TITLE = "行业前景"


_POSITIVE_KW = ("支持", "鼓励", "扶持", "利好", "高景气", "持续增长", "龙头", "升级", "加快发展")
_NEGATIVE_KW = ("限制", "去产能", "替代", "萎缩", "衰退", "过剩", "下行", "淘汰", "严控")


def _score_sentiment(evidence_list) -> tuple[int, int]:
    """返回 (positive_hits, negative_hits)。只看 REGULATORY + INDUSTRY_REPORT。"""
    pos = neg = 0
    for e in evidence_list:
        if e.source_type not in (SourceType.REGULATORY, SourceType.INDUSTRY_REPORT):
            continue
        text = f"{e.title or ''} {e.excerpt or ''}"
        pos += sum(1 for kw in _POSITIVE_KW if kw in text)
        neg += sum(1 for kw in _NEGATIVE_KW if kw in text)
    return pos, neg


def answer(ts_code: str, db_path: Path, **_: Any) -> EightQuestionAnswer:
    ans = EightQuestionAnswer(
        question_id=QUESTION_ID, question_title=QUESTION_TITLE,
        rating=None, answer="",
    )

    # 1. SW 分类（事实）
    industry_sw_l2 = ""
    try:
        con = open_connection(db_path)
    except FileNotFoundError as exc:
        ans.notes.append(str(exc))
        ans.critical_gaps.append(f"DuckDB 不可访问: {exc}")
        ans.finalize_status()
        return ans
    try:
        peers, ev_peers = probe_sw_peers(con, ts_code, limit=10)
        if ev_peers:
            ans.evidence.append(ev_peers)
        if peers:
            industry_sw_l2 = peers[0].get("l2_name", "") or ""
    finally:
        con.close()

    if not industry_sw_l2:
        ans.status = "insufficient-evidence"
        ans.missing_inputs.append(f"{ts_code} 无申万 L2 分类映射")
        ans.finalize_status()
        return ans

    # 2. 研报（观点）
    rep_res = collect_industry_reports(
        ts_code, industry_sw_l2=industry_sw_l2, limit=8
    )
    ans.evidence.extend(rep_res.evidence)
    # H2: missing_inputs 仅在 requires_human 时写入
    if rep_res.requires_human:
        ans.missing_inputs.extend(rep_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"行业研报采集失败（{rep_res.error_type}）：{rep_res.error}"
        )
    if rep_res.error:
        ans.notes.append(f"industry_reports: {rep_res.error}")

    # 3. 产业政策（事实）
    pol_res = collect_industry_policies(industry_sw_l2)
    ans.evidence.extend(pol_res.evidence)
    if pol_res.requires_human:
        ans.missing_inputs.extend(pol_res.missing_inputs)
        ans.human_in_loop_requests.append(
            f"行业政策采集失败（{pol_res.error_type}）：{pol_res.error}"
        )
    if pol_res.error:
        ans.notes.append(f"industry_policies: {pol_res.error}")

    # 评级门槛：必须有 DB 事实 + 至少 1 条研报或政策
    has_factual = any(e.source_type in (SourceType.DB, SourceType.REGULATORY) for e in ans.evidence)
    has_view = any(e.source_type == SourceType.INDUSTRY_REPORT for e in ans.evidence)
    report_cnt = sum(1 for e in ans.evidence if e.source_type == SourceType.INDUSTRY_REPORT)
    policy_cnt = sum(1 for e in ans.evidence if e.source_type == SourceType.REGULATORY)

    if has_factual and has_view:
        ans.status = "ready"
        # 动态评级：基线 3 ± 情绪信号
        pos, neg = _score_sentiment(ans.evidence)
        net = pos - neg
        rating = 3
        if net >= 3:
            rating = 4
        elif net <= -3:
            rating = 2
        if net >= 6 and policy_cnt >= 2:
            rating = 5
        elif net <= -6 and policy_cnt >= 2:
            rating = 1
        ans.rating = rating
        ans.rating_signals.append(
            f"sentiment_hits pos={pos} neg={neg} net={net}; "
            f"reports={report_cnt} policies={policy_cnt} → rating={rating}"
        )
        ans.answer = (
            f"公司归属申万 L2 行业【{industry_sw_l2}】；已采集 "
            f"{report_cnt} 条研报、{policy_cnt} 条政策。"
            f"关键词情绪净值 {net}（正 {pos} / 负 {neg}）→ 评级 {rating}。"
            " 研报属预测性观点，仅作景气度参考；建议人工复核 evidence 中政策原文。"
        )
        if policy_cnt == 0:
            ans.critical_gaps.append("无产业政策证据，景气度判断置信度降低")
    elif has_factual:
        ans.status = "partial"
        ans.missing_inputs.append(f"补充 {industry_sw_l2} 近 1 年研报/政策证据")
    else:
        ans.status = "insufficient-evidence"

    ans.finalize_status()
    return ans


def main(argv: list[str] | None = None) -> int:
    return run_single_question_cli(question_key="q01_industry", answer_fn=answer, argv=argv)


if __name__ == "__main__":
    raise SystemExit(main())


"""eight_questions_orchestrator.py — 八问总入口。

用法
----
python eight_questions_orchestrator.py --ts-code 000002.SZ
python eight_questions_orchestrator.py --ts-code 000002.SZ --questions 1,4,7 --format json
python eight_questions_orchestrator.py --ts-code 000002.SZ --output-dir /tmp/vanke --format both
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, cast

SCRIPTS_DIR = Path(__file__).resolve().parent


def _find_analysis_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if parent.name != "2min-company-analysis":
            continue
        marker = parent / "seven-look-eight-question" / "assets" / "rule_registry.json"
        if marker.exists():
            return parent
    raise RuntimeError(f"Cannot locate 2min-company-analysis root from: {current}")


SKILLS_ROOT = _find_analysis_root()

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from eight_questions_domain import (  # type: ignore[no-redef]
    EIGHT_QUESTIONS,
    EightQuestionAnswer,
    SOURCE_LABEL,
    default_db_path,
    now_iso,
    sanitize_excerpt,
)


RULE_REGISTRY = SKILLS_ROOT / "seven-look-eight-question" / "assets" / "rule_registry.json"


def _load_question_module(module_name: str, skill_dir: str, script_name: str):
    script_path = SKILLS_ROOT / skill_dir / "scripts" / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Question script not found: {script_path}")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load question module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_rule_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Rule registry not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _build_question_specs(registry: dict[str, Any]) -> dict[int, dict[str, str]]:
    specs: dict[int, dict[str, str]] = {}
    for rule in registry.get("rules", []):
        if not isinstance(rule, dict):
            continue
        if rule.get("category") != "question":
            continue
        rid = str(rule.get("rule_id", ""))
        if not rid.startswith("question-"):
            continue
        script = rule.get("script")
        if not isinstance(script, str) or "/scripts/" not in script:
            continue
        try:
            qid = int(rid.split("-", 1)[1])
        except (IndexError, ValueError):
            continue
        skill_dir, script_name = script.split("/scripts/", 1)
        specs[qid] = {"skill_dir": skill_dir, "script": script_name}
    return specs


def _build_question_modules() -> dict[int, Callable[..., EightQuestionAnswer]]:
    registry = _load_rule_registry(RULE_REGISTRY)
    specs = _build_question_specs(registry)
    modules: dict[int, Callable[..., EightQuestionAnswer]] = {}
    for meta in EIGHT_QUESTIONS:
        qid = int(meta["id"])
        spec = specs.get(qid)
        if not spec:
            raise RuntimeError(f"Question spec missing in rule_registry for question-{qid:02d}")
        module = _load_question_module(
            module_name=f"look08_q{qid:02d}",
            skill_dir=spec["skill_dir"],
            script_name=spec["script"],
        )
        raw_answer_fn = getattr(module, "answer", None)
        if not callable(raw_answer_fn):
            raise RuntimeError(f"Question module missing callable answer(): {spec['skill_dir']}/{spec['script']}")
        answer_fn = cast(Callable[..., EightQuestionAnswer], raw_answer_fn)
        modules[qid] = answer_fn
    return modules


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("look_08")


# (question_id, module) —— module 必须暴露 `answer(ts_code, db_path, **kw) -> EightQuestionAnswer`
_QUESTION_MODULES: dict[int, Callable[..., EightQuestionAnswer]] = _build_question_modules()


# ---------------------------------------------------------------------------
# 执行
# ---------------------------------------------------------------------------


def run_questions(
    ts_code: str,
    db_path: Path,
    question_ids: list[int],
    *,
    max_workers: int = 4,
) -> dict[int, EightQuestionAnswer]:
    """并发跑指定问题集合。单个失败不会中断其它问题。"""
    results: dict[int, EightQuestionAnswer] = {}

    def _run_one(qid: int) -> tuple[int, EightQuestionAnswer]:
        try:
            ans = _QUESTION_MODULES[qid](ts_code=ts_code, db_path=db_path)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Q%s 执行异常: %s", qid, exc)
            qmeta = next(q for q in EIGHT_QUESTIONS if q["id"] == qid)
            ans = EightQuestionAnswer(
                question_id=qid,
                question_title=qmeta["title"],
                rating=None,
                answer="",
                status="insufficient-evidence",
                notes=[f"执行异常: {type(exc).__name__}: {exc}"],
            )
        ans.finalize_status()
        try:
            ans.validate()
        except ValueError as exc:
            logger.warning("Q%s validate 失败，强制降级: %s", qid, exc)
            ans.status = "insufficient-evidence"
            ans.rating = None
            ans.notes.append(f"validate 失败: {exc}")
        return qid, ans

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_run_one, qid) for qid in question_ids]
        for fut in as_completed(futures):
            qid, ans = fut.result()
            results[qid] = ans
            logger.info(
                "Q%s [%s] 完成 status=%s rating=%s evidence=%d",
                qid, ans.question_title, ans.status, ans.rating, len(ans.evidence),
            )

    return results


# ---------------------------------------------------------------------------
# 汇总
# ---------------------------------------------------------------------------


def _summarize(results: dict[int, EightQuestionAnswer]) -> dict[str, Any]:
    ratings = [a.rating for a in results.values() if a.rating is not None]
    weighted: list[float] = []
    for answer in results.values():
        wr = answer.weighted_rating()
        if wr is not None:
            weighted.append(wr)
    status_counts: dict[str, int] = {}
    for a in results.values():
        status_counts[a.status] = status_counts.get(a.status, 0) + 1

    human_requests: list[dict[str, Any]] = []
    critical_gaps: list[dict[str, Any]] = []
    for qid, a in results.items():
        for req in a.human_in_loop_requests:
            human_requests.append({"question_id": qid, "question_title": a.question_title, "request": req})
        for gap in a.critical_gaps:
            critical_gaps.append({"question_id": qid, "question_title": a.question_title, "gap": gap})

    return {
        "question_count": len(results),
        "status_counts": status_counts,
        "avg_rating": round(sum(ratings) / len(ratings), 3) if ratings else None,
        "avg_weighted_rating": round(sum(weighted) / len(weighted), 3) if weighted else None,
        "ready_questions": [qid for qid, a in results.items() if a.status == "ready"],
        "insufficient_questions": [
            qid for qid, a in results.items() if a.status == "insufficient-evidence"
        ],
        "human_in_loop_required": [
            qid for qid, a in results.items() if a.status == "human-in-loop-required"
        ],
        "human_in_loop_requests": human_requests,
        "critical_gaps": critical_gaps,
    }


# ---------------------------------------------------------------------------
# Markdown 渲染
# ---------------------------------------------------------------------------


_STATUS_ICON = {
    "ready": "✅",
    "partial": "⚠️",
    "insufficient-evidence": "❌",
    "human-in-loop-required": "👤",
}


def render_markdown(
    ts_code: str,
    as_of_date: str,
    results: dict[int, EightQuestionAnswer],
    summary: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append(f"# 八问调研 · {ts_code}")
    lines.append("")
    lines.append(f"- 分析日期: {as_of_date}")
    lines.append(f"- 生成时间: {now_iso()}")
    lines.append(
        f"- 平均评级: `{summary['avg_rating']}` · 加权平均: `{summary['avg_weighted_rating']}`"
    )
    lines.append(f"- 状态分布: `{summary['status_counts']}`")
    lines.append("")
    lines.append(
        "> ⚠️ **证据铁律**：任何结论必须有 `evidence[]` 支撑。研报/IR 属预测性或公司口径，已自动加标记。"
    )
    lines.append("")

    # 最高优先级：人工介入请求
    if summary.get("human_in_loop_requests"):
        lines.append("## 🔴 需要人工介入（阻塞）")
        lines.append("")
        lines.append("以下采集失败必须先处理，否则对应问题评级不可信：")
        lines.append("")
        for req in summary["human_in_loop_requests"]:
            lines.append(f"- **Q{req['question_id']} {req['question_title']}**: {req['request']}")
        lines.append("")
    if summary.get("critical_gaps"):
        lines.append("## ⚠️ 关键证据缺口（降低置信度）")
        lines.append("")
        for gap in summary["critical_gaps"]:
            lines.append(f"- **Q{gap['question_id']} {gap['question_title']}**: {gap['gap']}")
        lines.append("")

    for qid in sorted(results.keys()):
        ans = results[qid]
        icon = _STATUS_ICON.get(ans.status, "•")
        lines.append(f"## {icon} Q{qid}. {ans.question_title}")
        lines.append("")
        lines.append(
            f"- 状态: `{ans.status}` · 评级: `{ans.rating}` · 加权: `{ans.weighted_rating()}`"
        )
        if ans.answer:
            lines.append(f"- 回答: {ans.answer}")
        if ans.human_in_loop_requests:
            lines.append("- 🔴 **需要人工介入**:")
            for r in ans.human_in_loop_requests:
                lines.append(f"  - {r}")
        if ans.critical_gaps:
            lines.append("- ⚠️ 关键证据缺口:")
            for g in ans.critical_gaps:
                lines.append(f"  - {g}")
        if ans.rating_signals:
            lines.append("- 评级依据:")
            for s in ans.rating_signals:
                lines.append(f"  - {s}")
        if ans.missing_inputs:
            lines.append("- 待补输入:")
            for m in ans.missing_inputs:
                lines.append(f"  - {m}")
        if ans.notes:
            lines.append("- 备注:")
            for n in ans.notes:
                lines.append(f"  - {n}")
        lines.append("")
        if ans.evidence:
            lines.append("| # | 来源类型 | 标题 / 摘录 | URL |")
            lines.append("| - | --- | --- | --- |")
            for i, e in enumerate(ans.evidence, 1):
                label = SOURCE_LABEL[e.source_type]
                excerpt = sanitize_excerpt(e.excerpt)
                title = sanitize_excerpt(e.title or "", limit=200)
                lines.append(f"| {i} | {label} | {title} — {excerpt} | <{e.source_url}> |")
        else:
            lines.append("_无证据。_")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 交叉校验（供上游 seven_looks_orchestrator.py 使用）
# ---------------------------------------------------------------------------


def cross_validate(
    results: dict[int, EightQuestionAnswer],
    look01_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    flags: dict[str, Any] = {}
    q4 = results.get(4)
    if q4 and q4.rating is not None and look01_payload:
        cash_ratio = (
            look01_payload.get("metrics", {}).get("ocf_to_profit_latest")
            or look01_payload.get("ocf_to_profit_latest")
            or look01_payload.get("summary", {}).get("net_profit_cash_ratio_avg")
        )
        if cash_ratio is not None and cash_ratio < 0.5 and q4.rating <= 2:
            flags["financial_integrity"] = "reinforced"
    return flags


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_question_ids(raw: str) -> list[int]:
    if not raw or raw.strip().lower() == "all":
        return list(range(1, 9))
    ids = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        ids.append(int(part))
    for qid in ids:
        if qid not in _QUESTION_MODULES:
            raise ValueError(f"未知 question_id: {qid}")
    return ids


def _default_output_dir(ts_code: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d-%H%M")
    return Path("logs") / "look-08" / ts_code / ts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="八问调研 (look-08)")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--duckdb-path", type=Path, default=default_db_path())
    parser.add_argument("--as-of-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--questions", default="all")
    parser.add_argument("--format", choices=["json", "markdown", "both"], default="both")
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args(argv)

    qids = _parse_question_ids(args.questions)
    output_dir: Path = args.output_dir or _default_output_dir(args.ts_code)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "开跑八问: ts_code=%s db=%s questions=%s output=%s",
        args.ts_code, args.duckdb_path, qids, output_dir,
    )

    results = run_questions(
        args.ts_code, args.duckdb_path, qids, max_workers=args.max_workers
    )
    summary = _summarize(results)

    payload = {
        "ts_code": args.ts_code,
        "as_of_date": args.as_of_date,
        "generated_at": now_iso(),
        "summary": summary,
        "answers": [results[qid].to_payload() for qid in sorted(results.keys())],
    }

    if args.format in ("json", "both"):
        out = output_dir / "eight_questions.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("JSON 写出: %s", out)

    if args.format in ("markdown", "both"):
        md = render_markdown(args.ts_code, args.as_of_date, results, summary)
        out = output_dir / "eight_questions.md"
        out.write_text(md, encoding="utf-8")
        logger.info("Markdown 写出: %s", out)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

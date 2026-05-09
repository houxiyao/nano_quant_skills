"""orchestrate_eight_questions.py — 八问调研编排工具。

并发执行 Q1~Q8 所有问题模块，合并结果，生成综合调研报告。

双接口：
  - execute_orchestrate_eight_questions(...) → dict  (供内部调用)
  - register_orchestrate_eight_questions_tools(mcp)  (MCP tool 注册)
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import FastMCP

from ..core.domain import EightQuestionAnswer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 八问元信息
# ---------------------------------------------------------------------------

EIGHT_QUESTIONS: list[dict[str, Any]] = [
    {"id": 1, "title": "行业前景"},
    {"id": 2, "title": "护城河"},
    {"id": 3, "title": "管理层"},
    {"id": 4, "title": "财务诚信"},
    {"id": 5, "title": "市场地位"},
    {"id": 6, "title": "商业模式"},
    {"id": 7, "title": "风险因素"},
    {"id": 8, "title": "未来规划"},
]


# ---------------------------------------------------------------------------
# 核心执行
# ---------------------------------------------------------------------------


def execute_orchestrate_eight_questions(
    ts_code: str,
    max_workers: int = 4,
    question_ids: list[int] | None = None,
    con: Any = None,
) -> dict[str, Any]:
    """并发执行指定的八问并汇总结果。

    Args:
        ts_code: 股票代码
        max_workers: 并行线程数
        question_ids: 执行哪些问题（None=全部 1-8）
        con: 可选 DuckDB 连接

    Returns:
        综合结果字典
    """
    from .ask_q1 import execute_ask_q1
    from .ask_q2 import execute_ask_q2
    from .ask_q3 import execute_ask_q3
    from .ask_q4 import execute_ask_q4
    from .ask_q5 import execute_ask_q5
    from .ask_q6 import execute_ask_q6
    from .ask_q7 import execute_ask_q7
    from .ask_q8 import execute_ask_q8

    executors: dict[int, Any] = {
        1: execute_ask_q1,
        2: execute_ask_q2,
        3: execute_ask_q3,
        4: execute_ask_q4,
        5: execute_ask_q5,
        6: execute_ask_q6,
        7: execute_ask_q7,
        8: execute_ask_q8,
    }

    qids = question_ids if question_ids else list(range(1, 9))
    qids = [qid for qid in qids if qid in executors]

    results: dict[int, dict[str, Any]] = {}

    def _run_one(qid: int) -> tuple[int, dict[str, Any]]:
        try:
            payload = executors[qid](ts_code=ts_code, con=con)
            return qid, payload
        except Exception as exc:
            logger.exception("Q%s 执行异常: %s", qid, exc)
            qmeta = next((q for q in EIGHT_QUESTIONS if q["id"] == qid), {"title": f"Q{qid}"})
            error_payload = EightQuestionAnswer(
                question_id=qid,
                question_title=qmeta["title"],
                rating=None,
                answer="",
                status="insufficient-evidence",
                notes=[f"执行异常: {type(exc).__name__}: {exc}"],
            )
            error_payload.finalize_status()
            return qid, error_payload.to_payload()

    effective_workers = max(1, min(max_workers, len(qids)))

    if effective_workers == 1:
        for qid in qids:
            q, payload = _run_one(qid)
            results[q] = payload
            logger.info("[八问] Q%s 完成, status=%s", q, payload.get("status", "unknown"))
    else:
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futures = {pool.submit(_run_one, qid): qid for qid in qids}
            for fut in as_completed(futures):
                try:
                    q, payload = fut.result()
                    results[q] = payload
                    logger.info("[八问] Q%s 完成, status=%s", q, payload.get("status", "unknown"))
                except Exception as exc:
                    logger.exception("[八问] 执行异常: %s", exc)

    # 汇总
    summary = _summarize(results)

    output: dict[str, Any] = {
        "ts_code": ts_code,
        "generated_at": datetime.now().isoformat(),
        "summary": summary,
        "answers": [results[qid] for qid in sorted(results.keys())],
    }

    return output


def _summarize(results: dict[int, dict[str, Any]]) -> dict[str, Any]:
    """汇总八问结果。"""
    ratings: list[float] = []
    weighted_ratings: list[float] = []
    status_counts: dict[str, int] = {}
    human_requests: list[dict[str, Any]] = []
    critical_gaps: list[dict[str, Any]] = []

    for qid, payload in sorted(results.items()):
        # rating
        rating = payload.get("rating")
        if rating is not None:
            ratings.append(float(rating))

        # weighted_rating
        wr = payload.get("weighted_rating")
        if wr is not None:
            weighted_ratings.append(float(wr))

        # status
        status = payload.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

        # human_in_loop_requests
        for req in payload.get("human_in_loop_requests", []):
            human_requests.append({
                "question_id": qid,
                "question_title": payload.get("question_title", ""),
                "request": req,
            })

        # critical_gaps
        for gap in payload.get("critical_gaps", []):
            critical_gaps.append({
                "question_id": qid,
                "question_title": payload.get("question_title", ""),
                "gap": gap,
            })

    return {
        "question_count": len(results),
        "status_counts": status_counts,
        "avg_rating": round(sum(ratings) / len(ratings), 3) if ratings else None,
        "avg_weighted_rating": (
            round(sum(weighted_ratings) / len(weighted_ratings), 3) if weighted_ratings else None
        ),
        "ready_questions": [
            qid for qid, p in results.items() if p.get("status") == "ready"
        ],
        "insufficient_questions": [
            qid for qid, p in results.items() if p.get("status") == "insufficient-evidence"
        ],
        "human_in_loop_required": [
            qid for qid, p in results.items() if p.get("status") == "human-in-loop-required"
        ],
        "human_in_loop_requests": human_requests,
        "critical_gaps": critical_gaps,
    }


# ---------------------------------------------------------------------------
# MCP Tool 注册
# ---------------------------------------------------------------------------


def register_orchestrate_eight_questions_tools(mcp: FastMCP) -> None:
    """注册八问编排 MCP 工具。"""

    @mcp.tool()
    def orchestrate_eight_questions(
        ts_code: str,
        question_ids: str = "",
        max_workers: int = 4,
    ) -> str:
        """八问调研综合评估 — 并发执行所有（或指定）业务问题并汇总。

        执行 Q1~Q8 各问题模块（行业前景、护城河、管理层、财务诚信、
        市场地位、商业模式、风险因素、未来规划），收集证据并给出评级。

        Args:
            ts_code: 股票代码（如 000002.SZ）
            question_ids: 逗号分隔的问题ID（如 "1,2,4"），空串执行全部
            max_workers: 并行线程数（默认4）

        Returns:
            八问综合结果 JSON（含 summary, answers 等）
        """
        # 解析 question_ids
        qids: list[int] | None = None
        if question_ids and question_ids.strip():
            try:
                qids = [int(x.strip()) for x in question_ids.split(",") if x.strip()]
                qids = [q for q in qids if 1 <= q <= 8]
            except ValueError:
                qids = None

        result = execute_orchestrate_eight_questions(
            ts_code=ts_code,
            max_workers=max_workers,
            question_ids=qids,
        )
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)

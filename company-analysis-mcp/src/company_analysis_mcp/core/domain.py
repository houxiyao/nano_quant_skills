"""八问共享领域模型与证据规范。

所有 8 个问题模块共享：
- SourceType 枚举（含权重表）
- Evidence 证据单元（强校验）
- EightQuestionAnswer 每问回答（必须带 evidence 才能 ready）
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


# ── 来源类型与权重 ────────────────────────────────────────────


class SourceType(str, Enum):
    PRIMARY = "primary"
    REGULATORY = "regulatory"
    DB = "db"
    INDUSTRY_REPORT = "industry_report"
    NEWS = "news"
    IR_MEETING = "ir_meeting"


SOURCE_WEIGHTS: dict[SourceType, float] = {
    SourceType.PRIMARY: 1.0,
    SourceType.REGULATORY: 1.0,
    SourceType.DB: 1.0,
    SourceType.INDUSTRY_REPORT: 0.6,
    SourceType.NEWS: 0.4,
    SourceType.IR_MEETING: 0.5,
}

PREDICTIVE_SOURCES: frozenset[SourceType] = frozenset(
    {SourceType.INDUSTRY_REPORT, SourceType.IR_MEETING}
)

SOURCE_LABEL = {
    SourceType.PRIMARY: "事实·定期报告",
    SourceType.REGULATORY: "事实·监管披露",
    SourceType.DB: "事实·结构化数据库",
    SourceType.INDUSTRY_REPORT: "[预测·券商观点]",
    SourceType.NEWS: "聚合·新闻舆情",
    SourceType.IR_MEETING: "[公司口径·IR 调研]",
}


def sanitize_excerpt(text: str, limit: int = 200) -> str:
    """Normalize excerpt text for markdown table rendering."""
    return text.replace("\n", " ").replace("|", "/")[:limit]


# ── Evidence & Answer ─────────────────────────────────────────


_ISO_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?)?")


@dataclass(frozen=True)
class Evidence:
    """单条证据。严禁空引证。"""

    source_type: SourceType
    source_url: str
    retrieved_at: str
    excerpt: str
    title: str | None = None

    def __post_init__(self) -> None:
        if not self.source_url:
            raise ValueError("Evidence.source_url must not be empty")
        if not self.excerpt or not self.excerpt.strip():
            raise ValueError("Evidence.excerpt must not be empty (禁止无原文证据)")
        if not _ISO_DT_RE.match(self.retrieved_at):
            raise ValueError(
                f"Evidence.retrieved_at must be ISO8601, got: {self.retrieved_at!r}"
            )

    @property
    def weight(self) -> float:
        return SOURCE_WEIGHTS[self.source_type]

    @property
    def is_predictive(self) -> bool:
        return self.source_type in PREDICTIVE_SOURCES

    def to_payload(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type.value,
            "source_label": SOURCE_LABEL[self.source_type],
            "source_url": self.source_url,
            "retrieved_at": self.retrieved_at,
            "excerpt": self.excerpt,
            "title": self.title,
            "weight": self.weight,
            "is_predictive": self.is_predictive,
        }


AnswerStatus = str  # "ready" | "partial" | "insufficient-evidence" | "human-in-loop-required"

VALID_STATUSES = {
    "ready",
    "partial",
    "insufficient-evidence",
    "human-in-loop-required",
}


@dataclass
class EightQuestionAnswer:
    question_id: int
    question_title: str
    rating: int | None
    answer: str
    evidence: list[Evidence] = field(default_factory=list)
    status: AnswerStatus = "insufficient-evidence"
    missing_inputs: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    human_in_loop_requests: list[str] = field(default_factory=list)
    critical_gaps: list[str] = field(default_factory=list)
    rating_signals: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"Q{self.question_id}: invalid status {self.status!r}")
        if not 1 <= self.question_id <= 8:
            raise ValueError(f"question_id out of range: {self.question_id}")
        if self.status == "ready":
            if not self.evidence:
                raise ValueError(
                    f"Q{self.question_id}: status=ready 但 evidence 为空（禁止编造）"
                )
            if self.rating is None or not 1 <= self.rating <= 5:
                raise ValueError(
                    f"Q{self.question_id}: ready 状态下 rating 必须在 1..5"
                )
            if self.missing_inputs:
                raise ValueError(
                    f"Q{self.question_id}: status=ready 但 missing_inputs 非空 → "
                    f"应先 finalize_status() 降级为 partial"
                )
            if self.human_in_loop_requests:
                raise ValueError(
                    f"Q{self.question_id}: status=ready 但 human_in_loop_requests 非空 → "
                    f"应 finalize_status() 降级为 human-in-loop-required"
                )
        if self.rating is not None and not 1 <= self.rating <= 5:
            raise ValueError(f"Q{self.question_id}: rating out of range {self.rating}")

    def finalize_status(self) -> None:
        """按 evidence harness 铁律，根据 missing_inputs / human_in_loop_requests 自动降级。"""
        if self.human_in_loop_requests:
            self.status = "human-in-loop-required"
            return
        if self.status == "ready" and self.missing_inputs:
            self.status = "partial"
            self.notes.append(
                f"status 由 ready 降级为 partial：存在 {len(self.missing_inputs)} 条待补证据"
            )

    def weighted_rating(self) -> float | None:
        """按证据权重加权的评级。"""
        if self.rating is None or not self.evidence:
            return None
        avg_weight = sum(e.weight for e in self.evidence) / len(self.evidence)
        return round(self.rating * avg_weight, 3)

    def to_payload(self) -> dict[str, Any]:
        return {
            "question_id": self.question_id,
            "question_title": self.question_title,
            "rating": self.rating,
            "weighted_rating": self.weighted_rating(),
            "answer": self.answer,
            "status": self.status,
            "evidence": [e.to_payload() for e in self.evidence],
            "evidence_count": len(self.evidence),
            "has_predictive_sources": any(e.is_predictive for e in self.evidence),
            "missing_inputs": list(self.missing_inputs),
            "notes": list(self.notes),
            "human_in_loop_requests": list(self.human_in_loop_requests),
            "critical_gaps": list(self.critical_gaps),
            "rating_signals": list(self.rating_signals),
        }


# ── 八问清单 ─────────────────────────────────────────────────


EIGHT_QUESTIONS: list[dict[str, Any]] = [
    {"id": 1, "title": "行业前景", "description": "所处行业未来 3-5 年的空间、景气度、政策与周期位置。"},
    {"id": 2, "title": "竞争优势", "description": "公司的护城河：品牌、技术、规模、成本、渠道、许可证。"},
    {"id": 3, "title": "管理团队", "description": "董事长/总经理/核心团队背景、股权激励、持股与变动。"},
    {"id": 4, "title": "财务真实性", "description": "审计意见、问询/立案/更名史、净现比、关联交易等异常信号。"},
    {"id": 5, "title": "市场地位", "description": "市占率、前五大客户/供应商集中度、同行横向对比。"},
    {"id": 6, "title": "业务模式", "description": "主营业务构成、收入确认方式、商业模式稳定性。"},
    {"id": 7, "title": "风险因素", "description": "诉讼、处罚、质押、ST 风险、退市风险、关联风险。"},
    {"id": 8, "title": "未来规划", "description": "管理层指引、战略方向、业绩预告/快报、在建产能。"},
]


# ── 工具函数 ─────────────────────────────────────────────────


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def stockid_from_ts_code(ts_code: str) -> str:
    """`000002.SZ` → `000002`；部分 MCP 工具只接受 6 位代码。"""
    return ts_code.split(".")[0]

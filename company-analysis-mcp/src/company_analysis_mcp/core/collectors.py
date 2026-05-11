"""对 nano_search_mcp 工具的统一封装。

设计目标：
1. 把 MCP tool 当普通 Python 模块 import，零进程开销。
2. 任何 MCP 异常 → 返回 CollectResult(error=...)，绝不让上游抛异常。
3. 统一的返回形状：CollectResult。
4. error_type 语义化（env_missing / network_fail / not_found / module_missing / upstream_contract_break / source_disabled）。

重要：本模块只做"取证"，不做"评分"。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from .domain import Evidence, SourceType, now_iso, stockid_from_ts_code

logger = logging.getLogger(__name__)

_EXCERPT_MAX_CHARS = 600


@dataclass
class CollectResult:
    evidence: list[Evidence] = field(default_factory=list)
    status: str = "insufficient-evidence"
    missing_inputs: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    error: str | None = None
    error_type: str | None = None

    def extend(self, other: "CollectResult") -> None:
        self.evidence.extend(other.evidence)
        self.missing_inputs.extend(other.missing_inputs)
        self.notes.extend(other.notes)
        if other.error and not self.error:
            self.error = other.error
        if other.error_type and not self.error_type:
            self.error_type = other.error_type

    @property
    def requires_human(self) -> bool:
        return self.error_type in (
            "env_missing",
            "module_missing",
            "upstream_contract_break",
            "source_disabled",
        )


def _truncate(text: str, limit: int = _EXCERPT_MAX_CHARS) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


_BAILIAN_DEP_TOOLS = ("industry_policies", "deferred_search", "search (WebSearch)")

_CONTRACT_BREAK_EXC_TYPES: tuple[type[BaseException], ...] = (
    json.JSONDecodeError,
    KeyError,
    AttributeError,
    TypeError,
)


def check_bailian_env() -> tuple[bool, str | None]:
    """返回 (ok, missing_reason)。"""
    if not os.getenv("DASHSCOPE_API_KEY"):
        return False, (
            "CRITICAL: 未检测到环境变量 DASHSCOPE_API_KEY。"
            f"依赖百炼 WebSearch 的工具（{', '.join(_BAILIAN_DEP_TOOLS)}）将无法工作。"
            " 请在 shell 中 `export DASHSCOPE_API_KEY=sk-...` 后重新执行。"
        )
    return True, None


def _classify_mcp_error(exc: BaseException) -> str:
    msg = str(exc)
    if "DASHSCOPE_API_KEY" in msg or "缺少环境变量" in msg:
        return "env_missing"
    if isinstance(exc, _CONTRACT_BREAK_EXC_TYPES):
        return "upstream_contract_break"
    if any(kw in msg for kw in ("timeout", "Timeout", "Connection", "ConnectError", "服务不可用")):
        return "network_fail"
    return "network_fail"


# ── 年报 ─────────────────────────────────────────────────────


def collect_annual_reports(
    ts_code: str,
    *,
    limit: int = 3,
    fetch_content: bool = False,
) -> CollectResult:
    result = CollectResult()
    try:
        from nano_search_mcp.tools.sina_reports import fetch_reports
    except ImportError as exc:
        result.error = f"nano_search_mcp 不可用: {exc}"
        result.error_type = "module_missing"
        result.missing_inputs.append("安装 nano_search_mcp 模块")
        return result

    stockid = stockid_from_ts_code(ts_code)
    try:
        data = fetch_reports(stockid, "annual", limit=limit, fetch_content=fetch_content)
    except Exception as exc:  # noqa: BLE001
        result.error = f"fetch_reports(annual) 失败: {exc}"
        result.error_type = _classify_mcp_error(exc)
        result.missing_inputs.append(f"手动提供 {ts_code} 最近 {limit} 年年报 URL")
        return result

    retrieved_at = now_iso()
    for rep in data.get("reports", []):
        url = rep.get("url") or rep.get("source_url") or ""
        title = rep.get("title", "")
        content = rep.get("content") or title
        if not url or not content:
            continue
        try:
            result.evidence.append(
                Evidence(
                    source_type=SourceType.PRIMARY,
                    source_url=url,
                    retrieved_at=retrieved_at,
                    excerpt=_truncate(content),
                    title=title,
                )
            )
        except ValueError:
            continue

    result.status = "ready" if result.evidence else "insufficient-evidence"
    return result


# ── 公告 ─────────────────────────────────────────────────────


def collect_announcements(
    ts_code: str,
    *,
    keywords: list[str] | None = None,
    start_date: str = "",
    end_date: str = "",
    limit: int = 20,
) -> CollectResult:
    result = CollectResult()
    try:
        from nano_search_mcp.tools.announcements import fetch_announcement_list
    except ImportError as exc:
        result.error = f"nano_search_mcp 不可用: {exc}"
        result.error_type = "module_missing"
        result.missing_inputs.append("安装 nano_search_mcp 模块")
        return result

    stockid = stockid_from_ts_code(ts_code)
    try:
        entries = fetch_announcement_list(stockid, start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        result.error = f"fetch_announcement_list 失败: {exc}"
        result.error_type = _classify_mcp_error(exc)
        result.missing_inputs.append(f"手动提供 {ts_code} 公告列表")
        return result

    retrieved_at = now_iso()
    kws = [k for k in (keywords or []) if k]
    matched = 0
    for ent in entries:
        title = ent.get("title", "")
        url = ent.get("source_url") or ent.get("url") or ""
        if not title or not url:
            continue
        if kws and not any(kw in title for kw in kws):
            continue
        try:
            result.evidence.append(
                Evidence(
                    source_type=SourceType.PRIMARY,
                    source_url=url,
                    retrieved_at=retrieved_at,
                    excerpt=_truncate(f"{ent.get('ann_date', '')} {title}"),
                    title=title,
                )
            )
            matched += 1
            if matched >= limit:
                break
        except ValueError:
            continue

    result.status = "ready" if result.evidence else "partial"
    if not result.evidence and kws:
        result.notes.append(f"在 {len(entries)} 条公告中未匹配关键词 {kws}")
        result.missing_inputs.append(f"{ts_code} 相关公告缺失，关键词 {kws}")
    return result


# ── 行业研报 ─────────────────────────────────────────────────


def collect_industry_reports(
    ts_code: str,
    *,
    industry_sw_l2: str = "",
    keywords: list[str] | None = None,
    limit: int = 10,
) -> CollectResult:
    result = CollectResult()
    try:
        from nano_search_mcp.tools.industry_reports import fetch_industry_report_list
    except ImportError as exc:
        result.error = f"nano_search_mcp 不可用: {exc}"
        result.error_type = "module_missing"
        result.missing_inputs.append("安装 nano_search_mcp 模块")
        return result

    try:
        entries = fetch_industry_report_list(
            industry_sw_l2=industry_sw_l2, keywords=keywords, limit=limit, ts_code=ts_code,
        )
    except Exception as exc:  # noqa: BLE001
        result.error = f"fetch_industry_report_list 失败: {exc}"
        result.error_type = _classify_mcp_error(exc)
        result.missing_inputs.append(f"手动提供 {ts_code} 所属行业研报")
        return result

    retrieved_at = now_iso()
    for rep in entries:
        url = rep.get("source_url") or rep.get("url") or ""
        title = rep.get("title", "")
        excerpt = rep.get("summary") or rep.get("abstract") or title
        if not url or not excerpt:
            continue
        try:
            result.evidence.append(
                Evidence(
                    source_type=SourceType.INDUSTRY_REPORT,
                    source_url=url,
                    retrieved_at=retrieved_at,
                    excerpt=_truncate(excerpt),
                    title=title,
                )
            )
        except ValueError:
            continue

    result.status = "ready" if result.evidence else "insufficient-evidence"
    return result


# ── IR 会议纪要 ──────────────────────────────────────────────


def collect_ir_meetings(
    ts_code: str,
    *,
    start_date: str = "",
    end_date: str = "",
    limit: int = 20,
) -> CollectResult:
    result = CollectResult()
    try:
        from nano_search_mcp.tools.ir_meetings import fetch_ir_meeting_list
    except ImportError as exc:
        result.error = f"nano_search_mcp 不可用: {exc}"
        result.error_type = "module_missing"
        result.missing_inputs.append("安装 nano_search_mcp 模块")
        return result

    stockid = stockid_from_ts_code(ts_code)
    try:
        entries = fetch_ir_meeting_list(stockid, start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        result.error = f"fetch_ir_meeting_list 失败: {exc}"
        result.error_type = _classify_mcp_error(exc)
        result.missing_inputs.append(f"手动提供 {ts_code} IR/调研纪要")
        return result

    retrieved_at = now_iso()
    for ent in entries[:limit]:
        url = ent.get("source_url") or ent.get("url") or ""
        title = ent.get("title", "")
        if not url or not title:
            continue
        try:
            result.evidence.append(
                Evidence(
                    source_type=SourceType.IR_MEETING,
                    source_url=url,
                    retrieved_at=retrieved_at,
                    excerpt=_truncate(
                        f"{ent.get('ann_date', '')} [{ent.get('meeting_type', '')}] {title}"
                    ),
                    title=title,
                )
            )
        except ValueError:
            continue

    result.status = "ready" if result.evidence else "insufficient-evidence"
    return result


# ── 监管处罚 ─────────────────────────────────────────────────


def collect_penalties(
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> CollectResult:
    result = CollectResult()
    try:
        from nano_search_mcp.tools.regulatory_penalties import fetch_penalty_list
    except ImportError as exc:
        result.error = f"nano_search_mcp 不可用: {exc}"
        result.error_type = "module_missing"
        result.missing_inputs.append("安装 nano_search_mcp 模块")
        return result

    try:
        data = fetch_penalty_list(ts_code, start_date=start_date, end_date=end_date)
    except Exception as exc:  # noqa: BLE001
        result.error = f"fetch_penalty_list 失败: {exc}"
        result.error_type = _classify_mcp_error(exc)
        result.missing_inputs.append(f"手动确认 {ts_code} 是否存在监管处罚记录")
        return result

    if data.get("source") == "unavailable":
        result.error = data.get("error", "penalty source unavailable")
        result.error_type = "source_disabled"
        result.missing_inputs.append(f"监管处罚源不可用，请手动核查 {ts_code} 处罚记录")
        return result

    retrieved_at = now_iso()
    for pen in data.get("penalties", []):
        url = pen.get("source_url", "")
        title = pen.get("title", "")
        excerpt_parts = [
            pen.get("punish_date", ""),
            pen.get("event_type", ""),
            title,
            pen.get("reason", ""),
            pen.get("content", ""),
        ]
        excerpt = " | ".join(p for p in excerpt_parts if p)
        if not url or not excerpt:
            continue
        try:
            result.evidence.append(
                Evidence(
                    source_type=SourceType.REGULATORY,
                    source_url=url,
                    retrieved_at=retrieved_at,
                    excerpt=_truncate(excerpt),
                    title=title,
                )
            )
        except ValueError:
            continue

    if not result.evidence:
        result.status = "ready"
        result.notes = ["未发现监管处罚记录"]
    else:
        result.status = "ready"
    return result


# ── 行业政策 ─────────────────────────────────────────────────


def collect_industry_policies(
    industry_sw_l2: str,
    *,
    keywords: list[str] | None = None,
) -> CollectResult:
    result = CollectResult()
    if not industry_sw_l2:
        result.missing_inputs.append("industry_sw_l2 缺失，无法检索行业政策")
        return result

    ok, reason = check_bailian_env()
    if not ok:
        result.error = reason
        result.error_type = "env_missing"
        result.missing_inputs.append(f"[人工介入] {reason}")
        return result

    try:
        from nano_search_mcp.tools.industry_policies import fetch_industry_policy_list
    except ImportError as exc:
        result.error = f"nano_search_mcp 不可用: {exc}"
        result.error_type = "module_missing"
        result.missing_inputs.append("nano_search_mcp 未安装，无法检索行业政策")
        return result

    try:
        entries = fetch_industry_policy_list(industry_sw_l2=industry_sw_l2, keywords=keywords)
    except Exception as exc:  # noqa: BLE001
        result.error = f"fetch_industry_policy_list 失败: {exc}"
        result.error_type = _classify_mcp_error(exc)
        if result.error_type == "env_missing":
            result.missing_inputs.append(f"[人工介入] {exc}")
        else:
            result.missing_inputs.append(f"手动补充 {industry_sw_l2} 行业政策文件")
        return result

    retrieved_at = now_iso()
    for ent in entries:
        url = ent.get("source_url") or ent.get("url") or ""
        title = ent.get("title", "")
        excerpt = ent.get("snippet") or ent.get("summary") or title
        if not url or not excerpt:
            continue
        try:
            result.evidence.append(
                Evidence(
                    source_type=SourceType.REGULATORY,
                    source_url=url,
                    retrieved_at=retrieved_at,
                    excerpt=_truncate(excerpt),
                    title=title,
                )
            )
        except ValueError:
            continue

    result.status = "ready" if result.evidence else "insufficient-evidence"
    return result

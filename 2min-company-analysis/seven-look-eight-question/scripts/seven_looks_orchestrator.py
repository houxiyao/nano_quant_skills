"""seven_looks_orchestrator.py — 七看总编排脚本

依次执行 look-01 ~ look-07 七个独立分析脚本，收集中间 JSON，
汇总为一份综合财务质量报告，并附带量化评语与行动建议。

执行流程
--------
Phase 1（自动）: 运行 look-01/02/03/06/07（纯数据库，无需外部输入）
Phase 2（半自动）: 运行 look-04/05（若未提供年报文本包则标记 human-in-loop）
Phase 3（汇总）: 合并 7 份中间 JSON → 综合评估
Phase 4（评语）: 附加质量评分 + 最多 3 条行动建议

用法示例
--------
# 全自动（look-04/05 将提示需要年报文本）
python seven_looks_orchestrator.py --stock 000002.SZ --as-of-date 2025-04-30

# 提供年报文本包后全量执行
python seven_looks_orchestrator.py --stock 000002.SZ --as-of-date 2025-04-30 \
    --report-bundle-04 /tmp/vanke_reports.json \
    --report-bundle-05 /tmp/vanke_notes.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb


_STDERR_LOCK = threading.Lock()


def _log(msg: str) -> None:
    """线程安全的 stderr 日志；并行执行时防止行交错。"""
    with _STDERR_LOCK:
        print(msg, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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
PROJECT_ROOT = SKILLS_ROOT.parent
EIGHT_QUESTIONS_SCRIPT = (
    SKILLS_ROOT
    / "seven-look-eight-question"
    / "scripts"
    / "eight_questions_orchestrator.py"
)

LOOK_SPECS: list[dict[str, Any]] = [
    {
        "rule_id": "look-01",
        "title": "盈收与利润质量",
        "skill_dir": "look-01-profit-quality",
        "script": "look_01_profit_quality.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-02",
        "title": "费用成本结构",
        "skill_dir": "look-02-cost-structure",
        "script": "look_02_cost_structure.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-03",
        "title": "增长率趋势",
        "skill_dir": "look-03-growth-trend",
        "script": "look_03_growth_trend.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-04",
        "title": "业务构成与市场分布",
        "skill_dir": "look-04-business-market-distribution",
        "script": "look_04_business_market_distribution.py",
        "default_lookback": 3,
        "extra_args_key": "report_bundle_04",
    },
    {
        "rule_id": "look-05",
        "title": "资产负债健康度",
        "skill_dir": "look-05-balance-sheet-health",
        "script": "look_05_balance_sheet_health.py",
        "default_lookback": 3,
        "extra_args_key": "report_bundle_05",
    },
    {
        "rule_id": "look-06",
        "title": "投入产出效率",
        "skill_dir": "look-06-input-output-efficiency",
        "script": "look_06_input_output_efficiency.py",
        "default_lookback": 3,
        "extra_args_key": "employee_count_bundle_06",
    },
    {
        "rule_id": "look-07",
        "title": "收益率与资本回报",
        "skill_dir": "look-07-roe-capital-return",
        "script": "look_07_roe_capital_return.py",
        "default_lookback": 5,
        "extra_args_key": None,
    },
]


_SEVEN_LOOK_TABLE_EXISTENCE_REQUIRED = [
    "fin_income",
    "fin_balance",
    "fin_cashflow",
    "fin_indicator",
    "idx_sw_l3_peers",
    "stk_factor_pro",
    "stk_info",
]

_SEVEN_LOOK_DATA_REQUIRED = [
    {"table": "fin_income", "code_col": "ts_code", "annual": True, "min_rows": 1},
    {"table": "fin_balance", "code_col": "ts_code", "annual": True, "min_rows": 1},
    {"table": "fin_cashflow", "code_col": "ts_code", "annual": True, "min_rows": 1},
    {"table": "fin_indicator", "code_col": "ts_code", "annual": True, "min_rows": 1},
    {"table": "idx_sw_l3_peers", "code_col": "anchor_ts_code", "annual": False, "min_rows": 1},
    {"table": "stk_factor_pro", "code_col": "ts_code", "annual": False, "min_rows": 1},
]

_EIGHT_QUESTION_TABLE_EXISTENCE_REQUIRED = [
    "stk_company",
    "stk_managers",
    "stk_rewards",
    "fin_top10_holders",
    "stk_name_history",
    "fin_mainbz",
    "stk_pledge_stat",
    "stk_st_daily",
    "fin_forecast",
    "fin_express",
]

_EIGHT_QUESTION_DATA_REQUIRED = [
    {"table": "fin_mainbz", "code_col": "ts_code", "annual": False, "min_rows": 1},
    {"table": "stk_company", "code_col": "ts_code", "annual": False, "min_rows": 1},
]

_LOGICAL_TABLE_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "fin_income": {"ts_code", "end_date", "revenue"},
    "fin_balance": {"ts_code", "end_date", "total_assets", "total_liab"},
    "fin_cashflow": {"ts_code", "end_date", "n_cashflow_act"},
    "fin_indicator": {"ts_code", "end_date", "roe"},
    "idx_sw_l3_peers": {"anchor_ts_code", "peer_ts_code", "peer_is_self"},
    "stk_factor_pro": {"ts_code", "trade_date"},
    "stk_info": {"ts_code"},
    "stk_company": {"ts_code"},
    "stk_managers": {"ts_code", "name", "title"},
    "stk_rewards": {"ts_code", "name", "title"},
    "fin_top10_holders": {"ts_code", "end_date", "holder_name"},
    "stk_name_history": {"ts_code", "name"},
    "fin_mainbz": {"ts_code", "end_date", "bz_item"},
    "stk_pledge_stat": {"ts_code", "end_date"},
    "stk_st_daily": {"ts_code", "trade_date"},
    "fin_forecast": {"ts_code", "ann_date", "end_date"},
    "fin_express": {"ts_code", "ann_date", "end_date"},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_db_path() -> Path:
    return PROJECT_ROOT / "data" / "ashare.duckdb"


_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_valid_table_name(name: Any) -> bool:
    return isinstance(name, str) and bool(_TABLE_NAME_RE.match(name.strip()))


def _extract_source_target_mappings(payload: Any) -> dict[str, str]:
    """Extract source_table->target_table pairs from list/dict payload.

    Supports:
    - {"tables": [{"source_table": ..., "target_table": ...}, ...]}
    - [{"source_table": ..., "target_table": ...}, ...]
    """
    if isinstance(payload, dict):
        entries = payload.get("tables", [])
    elif isinstance(payload, list):
        entries = payload
    else:
        return {}

    if not isinstance(entries, list):
        return {}

    mappings: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        src = entry.get("source_table")
        tgt = entry.get("target_table")
        if not (_is_valid_table_name(src) and _is_valid_table_name(tgt)):
            continue
        src_s = str(src).strip()
        tgt_s = str(tgt).strip()
        if src_s == tgt_s:
            continue
        mappings[src_s] = tgt_s
    return mappings


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _load_mapping_registry_fallback() -> dict[str, str]:
    """Read source_table->target_table from tushare_duckdb_sync_skills/templates/mapping_registry.json.

    Silently returns {} when the file is absent, empty, or unparseable.
    Only entries where source_table != target_table are included.
    """
    registry_path = (
        PROJECT_ROOT / "tushare_duckdb_sync_skills" / "templates" / "mapping_registry.json"
    )
    if not registry_path.exists():
        return {}
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
        return _extract_source_target_mappings(payload)
    except Exception:  # noqa: BLE001
        return {}


def _load_task_config_fallback() -> dict[str, str]:
    """Read source_table->target_table from tushare_duckdb_sync_skills/templates/task_config.json.

    Silently returns {} when the file is absent, empty, or unparseable.
    """
    task_config_path = PROJECT_ROOT / "tushare_duckdb_sync_skills" / "templates" / "task_config.json"
    if not task_config_path.exists():
        return {}
    try:
        payload = json.loads(task_config_path.read_text(encoding="utf-8"))
        return _extract_source_target_mappings(payload)
    except Exception:  # noqa: BLE001
        return {}


def _load_table_map(table_map_arg: str | None) -> dict[str, str]:
    """Load logical->physical mapping from CLI arg, env, or mapping_registry fallback.

    Priority:
    1) --db-table-map passed as JSON string or JSON file path
    2) env SEVEN_LOOK_DB_TABLE_MAP as JSON string
     3) auto-scan tushare_duckdb_sync_skills/templates/mapping_registry.json
         and templates/task_config.json for source_table -> target_table
         entries where names differ

    Explicit mappings (1/2) always override registry fallback (3).
    """
    raw = table_map_arg
    if not raw:
        raw = os.environ.get("SEVEN_LOOK_DB_TABLE_MAP")

    # Priority 3 fallback (always loaded; overridden by explicit mapping below)
    base: dict[str, str] = _load_task_config_fallback()
    base.update(_load_mapping_registry_fallback())

    if not raw:
        return base

    maybe_path = Path(raw).expanduser()
    try:
        if maybe_path.exists():
            payload = json.loads(maybe_path.read_text(encoding="utf-8"))
        else:
            payload = json.loads(raw)
    except Exception:  # noqa: BLE001
        return base

    if not isinstance(payload, dict):
        return base

    explicit = {
        str(k): str(v)
        for k, v in payload.items()
        if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
    }
    return {**base, **explicit}


def _list_relations(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def _discover_relation_by_columns(
    con: duckdb.DuckDBPyConnection,
    logical_name: str,
) -> tuple[str | None, list[str]]:
    required = _LOGICAL_TABLE_REQUIRED_COLUMNS.get(logical_name)
    if not required:
        return None, []

    candidates: list[str] = []
    for relation_name in _list_relations(con):
        cols = _table_columns(con, relation_name)
        if required.issubset(cols):
            candidates.append(relation_name)

    if not candidates:
        return None, []
    if len(candidates) == 1:
        return candidates[0], candidates

    # Prefer fuzzy name match to reduce ambiguity when several relations share same columns.
    logical_tokens = [tok for tok in logical_name.split("_") if tok]
    ranked = sorted(
        candidates,
        key=lambda name: sum(1 for tok in logical_tokens if tok in name),
        reverse=True,
    )
    top_score = sum(1 for tok in logical_tokens if tok in ranked[0])
    second_score = sum(1 for tok in logical_tokens if tok in ranked[1]) if len(ranked) > 1 else -1
    if top_score > second_score:
        return ranked[0], candidates
    return None, candidates


def _resolve_relation_name(
    con: duckdb.DuckDBPyConnection,
    logical_name: str,
    table_map: dict[str, str],
) -> tuple[str | None, str | None]:
    """Resolve physical relation by mapping first, then direct name, then column-based auto-discovery.

    Returns (resolved_name, note)
    """
    mapped = table_map.get(logical_name)
    if mapped:
        if _table_exists(con, mapped):
            return mapped, f"mapped:{logical_name}->{mapped}"
        return None, f"mapped-missing:{logical_name}->{mapped}"

    if _table_exists(con, logical_name):
        return logical_name, "exact"

    discovered, candidates = _discover_relation_by_columns(con, logical_name)
    if discovered:
        return discovered, f"auto-discovered:{logical_name}->{discovered}"
    if candidates:
        return None, f"ambiguous:{logical_name}:{','.join(candidates[:5])}"
    return None, "not-found"


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _count_rows_for_stock(
    con: duckdb.DuckDBPyConnection,
    relation_name: str,
    code_col: str,
    stock: str,
    *,
    annual: bool,
) -> int:
    columns = _table_columns(con, relation_name)
    sql = f"SELECT COUNT(*) FROM {relation_name} WHERE {code_col} = ?"
    params: list[Any] = [stock]
    if annual and "end_date" in columns:
        sql += " AND EXTRACT(MONTH FROM end_date) = 12 AND EXTRACT(DAY FROM end_date) = 31"
    row = con.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def _build_sync_command_hint(table_name: str, db_path: str) -> str:
    return (
        "conda run -n legonanobot python "
        "tushare_duckdb_sync_skills/scripts/sync_table.py "
        f"--table {table_name} --db-path {db_path}"
    )


def _run_duckdb_preflight(
    stock: str,
    db_path: str,
    *,
    include_eight_questions: bool,
    table_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    preflight: dict[str, Any] = {
        "status": "ready",
        "missing_tables": [],
        "insufficient_data": [],
        "human_in_loop_requests": [],
        "resolved_relations": {},
        "alias_shim_required": [],
    }

    db_file = Path(db_path)
    if not db_file.exists():
        preflight["status"] = "human-in-loop-required"
        preflight["human_in_loop_requests"].append(
            f"DuckDB 文件不存在: {db_file}。请提供正确 --db-path，或先构建数据库。"
        )
        return preflight

    table_map = table_map or {}

    required_tables = list(_SEVEN_LOOK_TABLE_EXISTENCE_REQUIRED)
    data_requirements = list(_SEVEN_LOOK_DATA_REQUIRED)
    if include_eight_questions:
        required_tables.extend(_EIGHT_QUESTION_TABLE_EXISTENCE_REQUIRED)
        data_requirements.extend(_EIGHT_QUESTION_DATA_REQUIRED)

    try:
        con = duckdb.connect(str(db_file), read_only=True)
    except Exception as exc:  # noqa: BLE001
        preflight["status"] = "human-in-loop-required"
        preflight["human_in_loop_requests"].append(
            f"DuckDB 打开失败: {type(exc).__name__}: {exc}。请确认数据库文件可读且未损坏。"
        )
        return preflight

    try:
        for logical_name in required_tables:
            resolved_name, note = _resolve_relation_name(con, logical_name, table_map)
            if resolved_name:
                preflight["resolved_relations"][logical_name] = {
                    "relation": resolved_name,
                    "resolution": note,
                }
                if resolved_name != logical_name:
                    # Record the shim for informational purposes; the mapping layer
                    # passes SEVEN_LOOK_DB_TABLE_MAP to subprocesses so no view creation
                    # is required.
                    shim_sql = (
                        f"CREATE OR REPLACE VIEW {logical_name} AS "
                        f"SELECT * FROM {resolved_name};"
                    )
                    preflight["alias_shim_required"].append(
                        {
                            "logical_table": logical_name,
                            "resolved_relation": resolved_name,
                            "suggested_sql": shim_sql,
                        }
                    )
                continue
            preflight["missing_tables"].append(logical_name)
            if note and note.startswith("ambiguous:"):
                preflight["human_in_loop_requests"].append(
                    f"逻辑表 {logical_name} 检测到多个候选关系（{note}），"
                    "请通过 --db-table-map 指定唯一映射。"
                )
            elif note and note.startswith("mapped-missing:"):
                preflight["human_in_loop_requests"].append(
                    f"逻辑表 {logical_name} 的映射目标不存在（{note}），"
                    "请修正 --db-table-map 或同步对应表。"
                )
            else:
                preflight["human_in_loop_requests"].append(
                    "缺失逻辑数据域 "
                    f"{logical_name}。请先同步兼容表，或通过 --db-table-map 指定物理表名。"
                )

        for req in data_requirements:
            logical_name = str(req["table"])
            code_col = str(req["code_col"])
            annual = bool(req.get("annual", False))
            min_rows = int(req.get("min_rows", 1))

            if logical_name in preflight["missing_tables"]:
                continue
            resolved_name = preflight["resolved_relations"].get(logical_name, {}).get("relation")
            if not resolved_name:
                continue
            try:
                count = _count_rows_for_stock(
                    con,
                    resolved_name,
                    code_col,
                    stock,
                    annual=annual,
                )
            except Exception as exc:  # noqa: BLE001
                preflight["insufficient_data"].append(
                    {
                        "table": logical_name,
                        "relation": resolved_name,
                        "code_col": code_col,
                        "stock": stock,
                        "count": 0,
                        "required": min_rows,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                preflight["human_in_loop_requests"].append(
                    f"关系 {resolved_name}（逻辑域 {logical_name}）校验失败（{type(exc).__name__}: {exc}）。"
                    f" 请检查表结构并重建数据：{_build_sync_command_hint(logical_name, db_path)}"
                )
                continue

            if count >= min_rows:
                continue
            preflight["insufficient_data"].append(
                {
                    "table": logical_name,
                    "relation": resolved_name,
                    "code_col": code_col,
                    "stock": stock,
                    "count": count,
                    "required": min_rows,
                }
            )
            annual_hint = "（年报口径，仅统计 12-31 期末）" if annual else ""
            preflight["human_in_loop_requests"].append(
                f"关系 {resolved_name}（逻辑域 {logical_name}）在 {code_col}={stock} 下记录数为 {count}，"
                f"低于最低要求 {min_rows}{annual_hint}。"
                f" 请同步该证券数据并复核：{_build_sync_command_hint(logical_name, db_path)}"
            )
    finally:
        con.close()

    if preflight["missing_tables"] or preflight["insufficient_data"]:
        preflight["status"] = "human-in-loop-required"
    return preflight


def _render_preflight_blocked_json(
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    preflight: dict[str, Any],
) -> str:
    payload = {
        "framework": "七看财务质量综合评估",
        "stock": stock,
        "as_of_date": as_of_date,
        "lookback_years": lookback_years,
        "orchestration_status": "blocked",
        "block_reason": "duckdb-preflight-failed",
        "preflight": preflight,
        "quality_score": None,
        "red_flags": [],
        "commentary": "编排已阻断：DuckDB 关键表缺失或关键证券数据不足，需先补齐数据后再执行七看八问。",
        "human_in_loop_requests": preflight.get("human_in_loop_requests", []),
        "recommendations": [
            {
                "priority": "high",
                "action": "先补数再重跑",
                "detail": "按 human_in_loop_requests 中的具体命令补齐 DuckDB 表/证券数据，然后重新执行编排。",
            }
        ],
        "results": {},
        "look_results": {},
        "raw_results": {},
        "intermediate_files": None,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def _render_preflight_blocked_markdown(
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    preflight: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append("# 七看财务质量综合评估报告（阻断）")
    lines.append("")
    lines.append(f"- 股票代码: {stock}")
    lines.append(f"- 分析日期: {as_of_date}")
    lines.append(f"- 回看年数: {lookback_years or '各维度默认'}")
    lines.append("- 编排状态: blocked（duckdb-preflight-failed）")
    lines.append("")
    lines.append("## 阻断原因")
    lines.append("")
    lines.append("DuckDB 关键表缺失或关键证券数据不足。为避免在证据不全时强行作答，编排已提前终止。")
    lines.append("")

    missing_tables = preflight.get("missing_tables") or []
    if missing_tables:
        lines.append("## 缺失表")
        lines.append("")
        for table_name in missing_tables:
            lines.append(f"- {table_name}")
        lines.append("")

    insufficient = preflight.get("insufficient_data") or []
    if insufficient:
        lines.append("## 关键数据不足")
        lines.append("")
        for item in insufficient:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('table')} | {item.get('code_col')}={item.get('stock')} | "
                f"count={item.get('count')} / required={item.get('required')}"
            )
        lines.append("")

    requests = preflight.get("human_in_loop_requests") or []
    if requests:
        lines.append("## 待人工补充信息")
        lines.append("")
        for idx, req in enumerate(requests, 1):
            lines.append(f"{idx}. {req}")
        lines.append("")

    return "\n".join(lines)


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _summary_dict(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    summary = data.get("summary")
    return summary if isinstance(summary, dict) else {}


def _is_leverage_trend_deteriorating(trend: str) -> bool:
    return trend in ("deteriorating", "rising")


def _get_look_04_evidence_counts(data: dict[str, Any]) -> tuple[int, int]:
    summary = _summary_dict(data)
    target = data.get("target_analysis", {})
    biz = _first_non_none(
        summary.get("business_composition_evidence_count"),
        target.get("business_composition_evidence_count"),
        0,
    )
    overseas = _first_non_none(
        summary.get("overseas_sales_evidence_count"),
        target.get("overseas_sales_evidence_count"),
        0,
    )
    return int(biz), int(overseas)


def _run_look(
    spec: dict[str, Any],
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    db_path: str,
    extra_args: dict[str, str | None],
    timeout_seconds: int,
    table_map_json: str | None = None,
) -> dict[str, Any]:
    """Run a single look script as a subprocess and return its JSON output."""
    script_path = SKILLS_ROOT / spec["skill_dir"] / "scripts" / spec["script"]
    if not script_path.exists():
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": f"Script not found: {script_path}",
        }

    lb = lookback_years if lookback_years is not None else spec["default_lookback"]
    cmd = [
        sys.executable,
        str(script_path),
        "--stock", stock,
        "--as-of-date", as_of_date,
        "--lookback-years", str(lb),
        "--db-path", db_path,
        "--format", "json",
    ]

    # Add extra arguments (report-bundle for look-04/05, employee-count-bundle for look-06)
    extra_key = spec.get("extra_args_key")
    if extra_key and extra_args.get(extra_key):
        bundle_path = extra_args.get(extra_key)
        if isinstance(bundle_path, str):
            if spec["rule_id"] == "look-04":
                cmd.extend(["--report-bundle", bundle_path])
            elif spec["rule_id"] == "look-05":
                cmd.extend(["--report-bundle", bundle_path])
            elif spec["rule_id"] == "look-06":
                cmd.extend(["--employee-count-bundle", bundle_path])

    try:
        env = {**os.environ}
        if table_map_json:
            env["SEVEN_LOOK_DB_TABLE_MAP"] = table_map_json
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": f"Execution timed out ({timeout_seconds}s)",
        }

    if result.returncode != 0:
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": result.stderr.strip()[:500],
        }

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": f"Invalid JSON output: {result.stdout[:200]}",
        }

    # Normalize status: scripts that don't set a status key are considered ready
    if "status" not in data:
        data["status"] = "ready"
    return data


def _eight_questions_error(message: str) -> dict[str, Any]:
    return {
        "overall_status": "insufficient-evidence",
        "summary": {},
        "answers": [],
        "question_summaries": [],
        "cross_validation_flags": {},
        "error": message,
    }


def _derive_eight_questions_status(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    if not isinstance(summary, dict):
        return "unknown"

    question_count = int(summary.get("question_count") or 0)
    status_counts = summary.get("status_counts")
    if not isinstance(status_counts, dict) or question_count <= 0:
        return "unknown"

    ready_count = int(status_counts.get("ready") or 0)
    partial_count = int(status_counts.get("partial") or 0)
    human_count = int(status_counts.get("human-in-loop-required") or 0)
    insufficient_count = int(status_counts.get("insufficient-evidence") or 0)

    if ready_count == question_count:
        return "ready"
    if human_count == question_count:
        return "human-in-loop-required"
    if insufficient_count == question_count:
        return "insufficient-evidence"
    if ready_count or partial_count or human_count or insufficient_count:
        return "partial"
    return "unknown"


def _build_eight_question_summaries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    answers = payload.get("answers")
    if not isinstance(answers, list):
        return []

    return [
        {
            "question_id": answer.get("question_id"),
            "question_title": answer.get("question_title"),
            "status": answer.get("status", "unknown"),
            "rating": answer.get("rating"),
            "weighted_rating": answer.get("weighted_rating"),
        }
        for answer in answers
        if isinstance(answer, dict)
    ]


def _build_eight_questions_cross_validation(
    payload: dict[str, Any],
    look01_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    q4_rating = None
    answers = payload.get("answers")
    if isinstance(answers, list):
        for answer in answers:
            if isinstance(answer, dict) and answer.get("question_id") == 4:
                q4_rating = answer.get("rating")
                break

    net_profit_cash_ratio_avg = _summary_dict(look01_payload).get("net_profit_cash_ratio_avg")
    if (
        q4_rating is not None
        and net_profit_cash_ratio_avg is not None
        and q4_rating <= 2
        and net_profit_cash_ratio_avg < 0.5
    ):
        return {
            "financial_integrity": "reinforced",
            "net_profit_cash_ratio_avg": round(float(net_profit_cash_ratio_avg), 3),
            "q4_rating": q4_rating,
        }
    return {}


def _run_eight_questions(
    stock: str,
    as_of_date: str,
    db_path: str,
    base_output_dir: Path,
    look01_payload: dict[str, Any] | None,
    timeout_seconds: int,
    table_map_json: str | None = None,
) -> dict[str, Any]:
    """Run look-08 as a subprocess, then read the full payload from its output file."""
    if not EIGHT_QUESTIONS_SCRIPT.exists():
        return _eight_questions_error(f"Script not found: {EIGHT_QUESTIONS_SCRIPT}")

    output_dir = base_output_dir / "look-08"
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(EIGHT_QUESTIONS_SCRIPT),
        "--ts-code",
        stock,
        "--duckdb-path",
        db_path,
        "--as-of-date",
        as_of_date,
        "--output-dir",
        str(output_dir),
        "--format",
        "json",
    ]

    try:
        env = {**os.environ}
        if table_map_json:
            env["SEVEN_LOOK_DB_TABLE_MAP"] = table_map_json
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return _eight_questions_error(f"Execution timed out ({timeout_seconds}s)")

    if result.returncode != 0:
        return _eight_questions_error(result.stderr.strip()[:500])

    payload_path = output_dir / "eight_questions.json"
    if not payload_path.exists():
        return _eight_questions_error(f"Expected output file missing: {payload_path}")

    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _eight_questions_error(f"Invalid JSON output: {exc}")

    payload["overall_status"] = _derive_eight_questions_status(payload)
    payload["question_summaries"] = _build_eight_question_summaries(payload)
    payload["cross_validation_flags"] = _build_eight_questions_cross_validation(
        payload,
        look01_payload,
    )
    payload["output_file"] = str(payload_path)
    return payload


def _extend_with_eight_questions_human_requests(
    requests: list[dict[str, str]],
    eight_questions: dict[str, Any] | None,
) -> list[dict[str, str]]:
    merged = list(requests)
    if not isinstance(eight_questions, dict):
        return merged

    summary = eight_questions.get("summary")
    if not isinstance(summary, dict):
        return merged

    for req in summary.get("human_in_loop_requests", []):
        if not isinstance(req, dict):
            continue
        prefix_parts = []
        if req.get("question_id") is not None:
            prefix_parts.append(f"Q{req['question_id']}")
        if req.get("question_title"):
            prefix_parts.append(str(req["question_title"]))
        prefix = " ".join(prefix_parts)
        request_text = str(req.get("request", "")).strip()
        merged.append(
            {
                "rule_id": "look-08",
                "request": f"{prefix}: {request_text}".strip(": "),
            }
        )
    return merged


def _collect_eight_questions_critical_gaps(
    eight_questions: dict[str, Any] | None,
) -> list[dict[str, str]]:
    if not isinstance(eight_questions, dict):
        return []

    summary = eight_questions.get("summary")
    if not isinstance(summary, dict):
        return []

    gaps: list[dict[str, str]] = []
    for gap in summary.get("critical_gaps", []):
        if not isinstance(gap, dict):
            continue
        prefix_parts = []
        if gap.get("question_id") is not None:
            prefix_parts.append(f"Q{gap['question_id']}")
        if gap.get("question_title"):
            prefix_parts.append(str(gap["question_title"]))
        prefix = " ".join(prefix_parts)
        gap_text = str(gap.get("gap", "")).strip()
        gaps.append(
            {
                "rule_id": "look-08",
                "gap": f"{prefix}: {gap_text}".strip(": "),
            }
        )
    return gaps
# ---------------------------------------------------------------------------

# Red flag extractors: each returns a list of (flag_text, severity) tuples
# severity: "critical" | "warning"

def _extract_flags_01(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-01: profit quality red flags."""
    flags = []
    summary = _summary_dict(data)

    profit_positive_years = summary.get("profit_dedt_positive_years")
    total_years = summary.get("years_returned", 0)
    if profit_positive_years is not None and total_years > 0:
        if profit_positive_years == 0:
            flags.append(("扣非利润连续亏损", "critical"))
        elif profit_positive_years < total_years:
            flags.append((f"扣非利润仅{profit_positive_years}/{total_years}年为正", "warning"))

    ocf_positive = summary.get("operating_cashflow_positive_years")
    if ocf_positive is not None and total_years > 0:
        if ocf_positive == 0:
            flags.append(("经营现金流连续为负", "critical"))
        elif ocf_positive < total_years:
            flags.append((f"经营现金流仅{ocf_positive}/{total_years}年为正", "warning"))

    # 净现比质量：平均 < 0.5 critical；任一年 < 1 warning
    npcr_avg = summary.get("net_profit_cash_ratio_avg")
    npcr_below = summary.get("net_profit_cash_ratio_below_one_years") or 0
    npcr_samples = summary.get("net_profit_cash_ratio_samples") or 0
    if npcr_avg is not None and npcr_samples > 0:
        if npcr_avg < 0.5:
            flags.append(
                (f"净现比均值仅{npcr_avg:.2f}（<0.5），利润未落地为现金", "critical")
            )
        elif npcr_below > 0:
            flags.append(
                (f"净现比有{npcr_below}/{npcr_samples}年<1，利润含金量不足", "warning")
            )

    # 自由现金流
    fcf_positive = summary.get("fcf_positive_years")
    if fcf_positive is not None and total_years > 0:
        if fcf_positive == 0:
            flags.append(("自由现金流连续为负，公司持续失血", "critical"))
        elif fcf_positive < total_years:
            flags.append(
                (f"自由现金流仅{fcf_positive}/{total_years}年为正", "warning")
            )

    # 毛利率趋势
    if summary.get("grossprofit_margin_declining_3y"):
        flags.append(("毛利率连续≥3年下滑", "warning"))

    return flags


def _extract_flags_02(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-02: cost structure red flags."""
    flags = []
    summary = _summary_dict(data)

    mismatch_counts = summary.get("mismatch_counts", {})
    sales_mismatch = mismatch_counts.get("sales_exp_vs_revenue", 0)
    if sales_mismatch and sales_mismatch > 0:
        flags.append((f"销售费用增长但营收不增长（{sales_mismatch}次）", "warning"))

    return flags


def _extract_flags_03(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-03: growth trend red flags."""
    flags = []
    summary = _summary_dict(data)

    rev_cagr = summary.get("revenue_cagr")
    if rev_cagr is not None and rev_cagr < -0.05:
        flags.append(("营收CAGR为负，收入持续萎缩", "critical"))

    ni_cagr = summary.get("net_profit_cagr")
    if ni_cagr is not None and ni_cagr < -0.10:
        flags.append(("归母净利润CAGR大幅为负", "critical"))

    mode = summary.get("growth_mode_signal", "")
    if mode == "acquisition-assisted-or-mixed":
        flags.append(("增长可能含并购驱动成分", "warning"))

    return flags


def _extract_flags_04(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-04: business/market distribution flags."""
    flags = []
    status = data.get("status", "")
    if status in ("human-in-loop-required", "partial"):
        flags.append(("业务构成与市场分布数据不完整，需人工补充年报", "warning"))
    return flags


def _extract_flags_05(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-05: balance sheet health red flags."""
    flags = []
    summary = _summary_dict(data)
    rows = data.get("debt_solvency_rows", [])

    leverage_trend = summary.get("leverage_trend", "")
    if _is_leverage_trend_deteriorating(leverage_trend):
        flags.append(("杠杆水平持续恶化", "warning"))

    # Check debt_to_assets from the latest row
    if rows:
        latest = rows[0] if isinstance(rows, list) else {}
        dta = latest.get("debt_to_assets")
        if dta is not None and dta > 80:
            flags.append((f"资产负债率极高（{dta:.1f}%）", "critical"))

    # OCF 覆盖 CapEx：全负/半数以下均要报警
    capex_covers = summary.get("ocf_covers_capex_years")
    capex_samples = summary.get("ocf_covers_capex_samples") or 0
    if capex_covers is not None and capex_samples > 0:
        if capex_covers == 0:
            flags.append(
                (f"最近{capex_samples}年经营现金流均无法覆盖资本开支，靠筹资续命", "critical")
            )
        elif capex_covers < capex_samples / 2:
            flags.append(
                (f"经营现金流仅{capex_covers}/{capex_samples}年能覆盖资本开支", "warning")
            )

    hidden_status = summary.get("hidden_liability_status", "")
    if hidden_status == "human-in-loop-required":
        flags.append(("隐性负债未检测，需人工补充年报附注", "warning"))

    return flags


def _extract_flags_06(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-06: input-output efficiency red flags."""
    flags = []
    summary = _summary_dict(data)

    wc_trend = summary.get("wc_trend", "")
    if wc_trend == "deteriorating":
        flags.append(("营运资金效率持续恶化", "warning"))

    wc_per_rev = summary.get("wc_per_revenue_latest")
    if wc_per_rev is not None and wc_per_rev > 1.0:
        flags.append(("一元收入需要超过一元营运资金", "warning"))

    return flags


def _extract_flags_07(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-07: ROE & capital return red flags."""
    flags = []
    summary = _summary_dict(data)

    driver = summary.get("roe_driver", "")
    if driver == "negative-equity":
        flags.append(("资不抵债，杜邦分解完全失效", "critical"))
    elif driver == "negative-roe":
        flags.append(("ROE为负，处于亏损状态", "critical"))
    elif driver == "leverage-driven":
        flags.append(("高ROE主要靠杠杆驱动，非真实盈利能力", "warning"))

    trend = summary.get("roe_trend", "")
    if trend == "deteriorating":
        flags.append(("ROE持续恶化", "warning"))

    return flags


FLAG_EXTRACTORS = {
    "look-01": _extract_flags_01,
    "look-02": _extract_flags_02,
    "look-03": _extract_flags_03,
    "look-04": _extract_flags_04,
    "look-05": _extract_flags_05,
    "look-06": _extract_flags_06,
    "look-07": _extract_flags_07,
}


def _collect_all_flags(
    results: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    """Collect red flags from all 7 look results."""
    all_flags = []
    for rule_id, data in sorted(results.items()):
        status = data.get("status", "")
        if status in ("not-applicable", "error"):
            continue
        extractor = FLAG_EXTRACTORS.get(rule_id)
        if extractor:
            for flag_text, severity in extractor(data):
                all_flags.append({
                    "rule_id": rule_id,
                    "flag": flag_text,
                    "severity": severity,
                })
    return all_flags


def _compute_quality_score(flags: list[dict[str, str]]) -> dict[str, Any]:
    """Compute a simple quality score based on red flag counts.

    Scoring: start from 100, deduct 15 per critical, 5 per warning. Floor at 0.
    """
    score = 100
    criticals = [f for f in flags if f["severity"] == "critical"]
    warnings = [f for f in flags if f["severity"] == "warning"]
    score -= len(criticals) * 15
    score -= len(warnings) * 5
    score = max(score, 0)

    if score >= 80:
        grade = "A"
        label = "财务质量良好"
    elif score >= 60:
        grade = "B"
        label = "财务质量一般，存在部分隐患"
    elif score >= 40:
        grade = "C"
        label = "财务质量较差，多项红旗预警"
    else:
        grade = "D"
        label = "财务质量极差，建议高度警惕"

    return {
        "score": score,
        "grade": grade,
        "label": label,
        "critical_count": len(criticals),
        "warning_count": len(warnings),
    }


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _generate_recommendations(
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
) -> list[dict[str, str]]:
    """Generate up to 3 actionable next-step recommendations."""
    recs: list[dict[str, str]] = []

    # 1. If look-04 or look-05 need human input, recommend providing report bundles
    pending_human = []
    for rid in ("look-04", "look-05"):
        status = results.get(rid, {}).get("status", "")
        if status in ("human-in-loop-required", "partial"):
            pending_human.append(rid)
    if pending_human:
        recs.append({
            "action": "补充年报文本",
            "detail": (
                f"{'、'.join(pending_human)} 需要年报全文/附注文本才能完成分析。"
                "建议下载最近3年年报PDF，提取正文后以 --report-bundle 参数传入，"
                "以获得业务构成、市场分布和隐性负债的完整证据。"
            ),
            "priority": "high",
        })

    # 2. If critical flags exist, recommend deep-dive
    critical_rules = list({f["rule_id"] for f in flags if f["severity"] == "critical"})
    if critical_rules:
        rule_names = {s["rule_id"]: s["title"] for s in LOOK_SPECS}
        names = "、".join(f"{r}（{rule_names.get(r, r)}）" for r in sorted(critical_rules))
        recs.append({
            "action": "深入排查关键风险",
            "detail": (
                f"以下维度触发了严重红旗：{names}。"
                "建议逐项展开该维度的详细 Markdown 报告，"
                "结合管理层讨论与分析（MD&A）、审计报告意见、"
                "以及同行业对比数据进行交叉验证。"
            ),
            "priority": "high",
        })

    # 3. If look-07 shows leverage-driven or negative, suggest investigating capital structure
    roe_driver = _summary_dict(results.get("look-07", {})).get("roe_driver", "")
    if roe_driver in ("leverage-driven", "negative-roe", "negative-equity") and len(recs) < 3:
        recs.append({
            "action": "分析资本结构可持续性",
            "detail": (
                "当前ROE质量存在问题（"
                + {"leverage-driven": "杠杆驱动", "negative-roe": "亏损",
                   "negative-equity": "资不抵债"}.get(roe_driver, roe_driver)
                + "）。建议进一步分析：(1) 有息负债到期时间表，"
                "(2) 再融资能力评估，(3) 经营现金流能否覆盖利息支出。"
            ),
            "priority": "medium",
        })

    # 4. If growth is negative, suggest investigating turnaround potential
    rev_cagr = _summary_dict(results.get("look-03", {})).get("revenue_cagr")
    if rev_cagr is not None and rev_cagr < 0 and len(recs) < 3:
        recs.append({
            "action": "评估收入恢复可能性",
            "detail": (
                f"营收CAGR为{rev_cagr*100:.1f}%，处于收缩趋势。"
                "建议结合行业景气度数据、公司新业务布局、"
                "在手订单/合同负债变化来判断收入是否有望触底回升。"
            ),
            "priority": "medium",
        })

    # 5. If everything looks good, suggest valuation check
    if quality["grade"] in ("A", "B") and len(recs) < 3:
        recs.append({
            "action": "进入估值与股东结构分析",
            "detail": (
                "七看财务质量检查未发现严重问题。"
                "建议继续执行八问中的估值合理性（PE/PB/PS）"
                "和股东结构分析，判断当前价格是否具有安全边际。"
            ),
            "priority": "medium",
        })

    return recs[:3]


# ---------------------------------------------------------------------------
# Human-in-loop summary
# ---------------------------------------------------------------------------

def _collect_human_requests(results: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    """Collect all human-in-loop requests from the 7 look results."""
    requests = []

    # Look-04
    r04 = results.get("look-04", {})
    if r04.get("status") in ("human-in-loop-required", "partial"):
        human_reqs = r04.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-04",
                    "request": req if isinstance(req, str) else str(req),
                })
        else:
            requests.append({
                "rule_id": "look-04",
                "request": "请提供目标公司最近3年年报全文文本（JSON格式），用于提取业务构成、海外销售和客户集中度证据。",
            })

    # Look-05
    r05 = results.get("look-05", {})
    hidden_status = _summary_dict(r05).get("hidden_liability_status", "")
    if r05.get("status") in ("human-in-loop-required", "partial") or hidden_status in (
        "human-in-loop-required",
        "partial",
    ):
        human_reqs = r05.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-05",
                    "request": req if isinstance(req, str) else str(req),
                })
        else:
            requests.append({
                "rule_id": "look-05",
                "request": "请提供目标公司最近3年年报附注全文文本（JSON格式），用于提取隐性负债（对外担保、表外融资等）证据。",
            })

    # Look-06: per-capita headcount
    r06 = results.get("look-06", {})
    per_capita_status = _summary_dict(r06).get("per_capita_status", "")
    if per_capita_status in ("human-in-loop-required", "partial") or r06.get("status") == "partial":
        human_reqs = r06.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-06",
                    "request": req if isinstance(req, str) else str(req),
                })

    return requests


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------

def _render_json(
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
    human_requests: list[dict[str, str]],
    recommendations: list[dict[str, str]],
    intermediate_dir: Path | None,
    eight_questions: dict[str, Any] | None = None,
    critical_gaps: list[dict[str, str]] | None = None,
) -> str:
    commentary = _generate_commentary(stock, results, flags, quality)
    normalized_results = {
        rid: {
            "rule_id": rid,
            "title": next((s["title"] for s in LOOK_SPECS if s["rule_id"] == rid), rid),
            "status": data.get("status", "unknown"),
            "summary": _summary_dict(data),
        }
        for rid, data in sorted(results.items())
    }
    raw_results = {
        rid: data for rid, data in sorted(results.items())
    }
    payload = {
        "framework": "七看财务质量综合评估",
        "stock": stock,
        "as_of_date": as_of_date,
        "lookback_years": lookback_years,
        "quality_score": quality,
        "red_flags": flags,
        "commentary": commentary,
        "human_in_loop_requests": human_requests,
        "recommendations": recommendations,
        "results": normalized_results,
        "look_results": normalized_results,
        "raw_results": raw_results,
        "intermediate_files": (
            str(intermediate_dir) if intermediate_dir else None
        ),
    }
    if eight_questions is not None:
        payload["eight_questions"] = eight_questions
    if critical_gaps:
        payload["critical_gaps"] = critical_gaps
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def _render_markdown(
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
    human_requests: list[dict[str, str]],
    recommendations: list[dict[str, str]],
    intermediate_dir: Path | None,
    eight_questions: dict[str, Any] | None = None,
    critical_gaps: list[dict[str, str]] | None = None,
) -> str:
    lines: list[str] = []

    # Header
    lines.append("# 七看财务质量综合评估报告")
    lines.append("")
    lines.append(f"- 股票代码: {stock}")
    lines.append(f"- 分析日期: {as_of_date}")
    lines.append(f"- 回看年数: {lookback_years or '各维度默认'}")
    lines.append("")

    # Quality score
    lines.append("## 综合质量评分")
    lines.append("")
    grade = quality["grade"]
    score = quality["score"]
    label = quality["label"]
    lines.append(f"**{grade} ({score}/100)** — {label}")
    lines.append("")
    lines.append(f"- 严重红旗: {quality['critical_count']} 项")
    lines.append(f"- 一般预警: {quality['warning_count']} 项")
    lines.append("")

    # Red flags
    if flags:
        lines.append("## 红旗预警清单")
        lines.append("")
        lines.append("| 维度 | 预警内容 | 严重程度 |")
        lines.append("|------|---------|---------|")
        severity_icon = {"critical": "🔴 严重", "warning": "🟡 警示"}
        rule_titles = {s["rule_id"]: s["title"] for s in LOOK_SPECS}
        for f in flags:
            rid = f["rule_id"]
            title = rule_titles.get(rid, rid)
            sev = severity_icon.get(f["severity"], f["severity"])
            lines.append(f"| {rid} {title} | {f['flag']} | {sev} |")
        lines.append("")

    # 7-dimension summary table
    lines.append("## 七看维度概览")
    lines.append("")
    lines.append("| # | 维度 | 状态 | 核心发现 |")
    lines.append("|---|------|------|---------|")
    for spec in LOOK_SPECS:
        rid = spec["rule_id"]
        data = results.get(rid, {})
        status = data.get("status", "未执行")
        finding = _summarize_one_look(rid, data)
        lines.append(f"| {rid} | {spec['title']} | {status} | {finding} |")
    lines.append("")

    if eight_questions is not None:
        lines.append("## 八问摘要")
        lines.append("")
        lines.append(f"- 总体状态: `{eight_questions.get('overall_status', 'unknown')}`")
        summary = eight_questions.get("summary", {})
        if isinstance(summary, dict):
            lines.append(
                f"- 平均评级: `{summary.get('avg_rating')}` · 加权平均: `{summary.get('avg_weighted_rating')}`"
            )
        cross_validation_flags = eight_questions.get("cross_validation_flags", {})
        if cross_validation_flags:
            lines.append(f"- 交叉校验: `{cross_validation_flags}`")
        error_message = eight_questions.get("error")
        if error_message:
            lines.append(f"- 执行异常: `{error_message}`")
        lines.append("")

        question_summaries = eight_questions.get("question_summaries", [])
        if isinstance(question_summaries, list) and question_summaries:
            lines.append("| 问题 | 状态 | 评级 | 加权评级 |")
            lines.append("|------|------|------|---------|")
            for item in question_summaries:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| Q{item.get('question_id')} {item.get('question_title', '')} | "
                    f"{item.get('status', 'unknown')} | {item.get('rating')} | {item.get('weighted_rating')} |"
                )
            lines.append("")

        if critical_gaps:
            lines.append("### 八问关键证据缺口")
            lines.append("")
            for gap in critical_gaps:
                lines.append(f"- {gap['gap']}")
            lines.append("")

    # Human-in-loop
    if human_requests:
        lines.append("## 待人工补充信息")
        lines.append("")
        for i, req in enumerate(human_requests, 1):
            lines.append(f"{i}. **{req['rule_id']}**: {req['request']}")
        lines.append("")

    # Recommendations
    if recommendations:
        lines.append("## 下一步行动建议（最多3条）")
        lines.append("")
        priority_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}
        for i, rec in enumerate(recommendations, 1):
            icon = priority_icon.get(rec.get("priority", ""), "")
            lines.append(f"### {i}. {icon} {rec['action']}")
            lines.append("")
            lines.append(rec["detail"])
            lines.append("")

    # Commentary
    lines.append("## 量化评语")
    lines.append("")
    lines.append(_generate_commentary(stock, results, flags, quality))
    lines.append("")

    # Raw pass-through per look for auditability
    lines.append("## 分项原始分析透传")
    lines.append("")
    lines.append("以下内容为各分项脚本的原始 JSON 输出透传，用于复核汇总结论是否存在遗漏或失真。")
    lines.append("")
    for spec in LOOK_SPECS:
        rid = spec["rule_id"]
        data = results.get(rid, {})
        lines.append(f"### {rid} {spec['title']}")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(data, ensure_ascii=False, indent=2, default=str))
        lines.append("```")
        lines.append("")

    # Intermediate files
    if intermediate_dir:
        lines.append("## 中间文件")
        lines.append("")
        lines.append(f"7 份量化中间 JSON 已保存至: `{intermediate_dir}/`")
        lines.append("")

    return "\n".join(lines)


def _summarize_one_look(rule_id: str, data: dict[str, Any]) -> str:
    """Generate a one-line summary for each look dimension."""
    status = data.get("status", "")
    if status == "not-applicable":
        return "金融类公司，不适用"
    if status == "error":
        return f"执行出错: {data.get('error', '')[:60]}"
    if status in ("human-in-loop-required",):
        return "需人工补充年报文本"

    summary = _summary_dict(data)

    if rule_id == "look-01":
        pos = summary.get("profit_dedt_positive_years", "?")
        total = summary.get("years_returned", "?")
        ocf = summary.get("operating_cashflow_positive_years", "?")
        return f"扣非利润为正: {pos}/{total}年, 经营现金流为正: {ocf}/{total}年"

    if rule_id == "look-02":
        mismatch_counts = summary.get("mismatch_counts", {})
        mis = mismatch_counts.get("sales_exp_vs_revenue", 0)
        return f"销售费用/营收不匹配: {mis}次" if mis else "费用与营收匹配度正常"

    if rule_id == "look-03":
        rc = summary.get("revenue_cagr")
        nc = summary.get("net_profit_cagr")
        rc_str = f"{rc*100:.1f}%" if rc is not None else "N/A"
        nc_str = f"{nc*100:.1f}%" if nc is not None else "N/A"
        mode = summary.get("growth_mode_signal", "")
        return f"营收CAGR: {rc_str}, 净利润CAGR: {nc_str}, 模式: {mode}"

    if rule_id == "look-04":
        biz, overseas = _get_look_04_evidence_counts(data)
        return f"业务构成证据: {biz}条, 海外销售证据: {overseas}条"

    if rule_id == "look-05":
        trend = summary.get("leverage_trend", "")
        hidden = summary.get("hidden_liability_status", "")
        return f"杠杆趋势: {trend}, 隐性负债: {hidden}"

    if rule_id == "look-06":
        wc = summary.get("wc_per_revenue_latest")
        trend = summary.get("wc_trend", "")
        wc_str = f"{wc:.2f}" if wc is not None else "N/A"
        return f"WC/收入: {wc_str}, 趋势: {trend}"

    if rule_id == "look-07":
        driver = summary.get("roe_driver", "")
        trend = summary.get("roe_trend", "")
        roe = summary.get("roe_latest")
        roe_str = f"{roe*100:.2f}%" if roe is not None else "N/A"
        return f"ROE(DuPont): {roe_str}, 驱动: {driver}, 趋势: {trend}"

    return str(summary)[:80] if summary else "已完成"


def _generate_commentary(
    stock: str,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
) -> str:
    """Generate a brief financial commentary based on all 7 dimensions."""
    parts: list[str] = []

    grade = quality["grade"]
    score = quality["score"]

    if grade == "A":
        parts.append(f"综合评分 {score}/100，七看维度整体表现良好，未发现明显财务质量隐患。")
    elif grade == "B":
        parts.append(f"综合评分 {score}/100，财务质量总体尚可，但存在个别需要关注的预警信号。")
    elif grade == "C":
        parts.append(f"综合评分 {score}/100，多个维度触发预警红旗，财务质量堪忧，建议审慎对待。")
    else:
        parts.append(f"综合评分 {score}/100，严重红旗密集，财务质量极差，强烈建议回避或做深度尽调。")

    # Profit quality
    r01 = _summary_dict(results.get("look-01", {}))
    ocf_pos = r01.get("operating_cashflow_positive_years")
    total_y = r01.get("years_returned")
    if ocf_pos is not None and total_y and ocf_pos < total_y:
        parts.append(f"利润质量方面，经营现金流仅{ocf_pos}年为正（共{total_y}年），利润含金量不足。")

    # Growth
    r03 = _summary_dict(results.get("look-03", {}))
    rev_cagr = r03.get("revenue_cagr")
    if rev_cagr is not None:
        if rev_cagr < -0.05:
            parts.append(f"增长方面，营收CAGR为{rev_cagr*100:.1f}%，处于明显收缩通道。")
        elif rev_cagr > 0.15:
            parts.append(f"增长方面，营收CAGR为{rev_cagr*100:.1f}%，保持较高增速。")

    # ROE
    r07 = _summary_dict(results.get("look-07", {}))
    driver = r07.get("roe_driver", "")
    if driver == "profitability-driven":
        parts.append("资本回报方面，ROE由盈利能力驱动，属于健康模式。")
    elif driver == "leverage-driven":
        parts.append("资本回报方面，ROE主要依赖杠杆，真实盈利能力有限，需警惕债务风险。")
    elif driver in ("negative-roe", "negative-equity"):
        parts.append("资本回报方面，公司处于亏损或资不抵债状态，需要高度关注。")

    # Balance sheet
    r05 = _summary_dict(results.get("look-05", {}))
    lev_trend = r05.get("leverage_trend", "")
    if _is_leverage_trend_deteriorating(lev_trend):
        parts.append("负债健康度方面，杠杆水平逐年攀升，偿债压力持续增大。")

    if not parts:
        parts.append("数据不足或公司类型不适用，无法生成有效评语。")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="七看财务质量综合评估 — 依次执行 look-01 ~ look-07 并汇总报告",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            示例:
              python seven_looks_orchestrator.py --stock 000002.SZ --as-of-date 2025-04-30
              python seven_looks_orchestrator.py --stock 000002.SZ --as-of-date 2025-04-30 \\
                  --report-bundle-04 reports.json --report-bundle-05 notes.json
        """),
    )
    parser.add_argument("--stock", required=True, help="股票代码, 如 000002.SZ")
    parser.add_argument("--as-of-date", default=None, help="分析日期 YYYY-MM-DD")
    parser.add_argument("--lookback-years", type=int, default=None,
                        help="统一回看年数（不设则每个维度用自己的默认值）")
    parser.add_argument("--db-path", default=str(_default_db_path()), help="DuckDB 路径")
    parser.add_argument("--report-bundle-04", default=None,
                        help="look-04 年报全文文本包（JSON）")
    parser.add_argument("--report-bundle-05", default=None,
                        help="look-05 年报附注文本包（JSON）")
    parser.add_argument(
        "--employee-count-bundle-06",
        default=None,
        help=(
            "look-06 员工总数 JSON（人工从年报「员工情况」抄录）。"
            " 未提供时 look-06 会在 human_in_loop_requests 中要求补数据。"
        ),
    )
    parser.add_argument("--output-dir", default=None,
                        help="中间文件输出目录（不设则使用临时目录）")
    parser.add_argument("--final-output", default=None,
                        help="最终综合报告输出路径（官方权威 artifact）")
    parser.add_argument(
        "--include-eight-questions",
        action="store_true",
        help="调用 look-08 业务问题脚本，并将结果合并到最终输出的 eight_questions 字段",
    )
    parser.add_argument(
        "--eight-questions-bundle",
        default=None,
        help="已废弃；look-08 现通过 evidence harness 自动取证，请改用 --include-eight-questions",
    )
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown",
                        help="输出格式")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help=(
            "并行执行 look-0N 子进程的线程池大小；设为 1 退化为串行。"
            " DuckDB 只读模式支持多连接，默认 4 线程在 MBP 级硬件上约比串行快 2-3 倍。"
        ),
    )
    parser.add_argument(
        "--per-look-timeout",
        type=int,
        default=120,
        help="每个 look 子进程超时时间（秒），默认 120。",
    )
    parser.add_argument(
        "--eight-questions-timeout",
        type=int,
        default=180,
        help="look-08 子进程超时时间（秒），默认 180。",
    )
    parser.add_argument(
        "--db-table-map",
        default=None,
        help=(
            "逻辑表名到物理表名映射。可传 JSON 字符串或 JSON 文件路径。"
            "示例: '{\"fin_income\":\"my_fin_income\"}'"
        ),
    )
    args = parser.parse_args()

    if args.eight_questions_bundle:
        parser.error(
            "--eight-questions-bundle 已废弃；look-08 现通过 evidence harness 自动取证，请改用 --include-eight-questions。"
        )

    as_of_date = _parse_date(args.as_of_date).isoformat()

    # Determine output directory for intermediate files
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        import tempfile
        output_dir = Path(tempfile.mkdtemp(prefix=f"seven_looks_{args.stock}_"))

    output_dir.mkdir(parents=True, exist_ok=True)

    table_map = _load_table_map(args.db_table_map)

    _log("[七看] Phase 0: DuckDB 数据门禁校验 ...")
    preflight = _run_duckdb_preflight(
        stock=args.stock,
        db_path=args.db_path,
        include_eight_questions=args.include_eight_questions,
        table_map=table_map,
    )
    if preflight.get("status") != "ready":
        _log("[七看]   → 门禁失败，停止编排，转入 human-in-loop")
        if args.format == "json":
            final_output = _render_preflight_blocked_json(
                args.stock,
                as_of_date,
                args.lookback_years,
                preflight,
            )
        else:
            final_output = _render_preflight_blocked_markdown(
                args.stock,
                as_of_date,
                args.lookback_years,
                preflight,
            )

        final_output_path: Path | None = None
        if args.final_output:
            final_output_path = Path(args.final_output).expanduser().resolve()
            final_output_path.parent.mkdir(parents=True, exist_ok=True)
            final_output_path.write_text(final_output, encoding="utf-8")

        print(final_output)
        if final_output_path is not None:
            print(f"[七看] 最终报告已保存至: {final_output_path}", file=sys.stderr)
        print(f"\n[七看] 已阻断。中间目录: {output_dir}", file=sys.stderr)
        return

    extra_args = {
        "report_bundle_04": args.report_bundle_04,
        "report_bundle_05": args.report_bundle_05,
        "employee_count_bundle_06": args.employee_count_bundle_06,
    }

    # Build effective table map from preflight resolved relations and pass to subprocesses.
    # Entries where physical name differs from logical name become VIEW shims inside each
    # look subprocess via connect_read_only() in common.py.
    _effective_map = {
        logical: info["relation"]
        for logical, info in preflight.get("resolved_relations", {}).items()
        if info.get("relation") and info["relation"] != logical
    }
    effective_map_json: str | None = (
        json.dumps(_effective_map, ensure_ascii=False) if _effective_map else None
    )
    if _effective_map:
        _log(f"[七看] 映射层已激活 ({len(_effective_map)} 条逻辑→物理表名映射)")

    # Phase 1 & 2: Run all 7 looks
    results: dict[str, dict[str, Any]] = {}
    auto_rules = ["look-01", "look-02", "look-03", "look-06", "look-07"]
    human_rules = ["look-04", "look-05"]

    _log(f"[七看] 开始分析 {args.stock}，分析日期 {as_of_date}（并发 {args.max_workers}）")

    def _execute(spec: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        rid = spec["rule_id"]
        phase = "Phase 1 (自动)" if rid in auto_rules else "Phase 2 (半自动)"
        _log(f"[七看] {phase} 启动 {rid}: {spec['title']} ...")
        data = _run_look(
            spec=spec,
            stock=args.stock,
            as_of_date=as_of_date,
            lookback_years=args.lookback_years,
            db_path=args.db_path,
            extra_args=extra_args,
            timeout_seconds=args.per_look_timeout,
            table_map_json=effective_map_json,
        )
        # Write intermediate file（线程内独占该 rule_id 的文件名，无竞争）
        intermediate_path = output_dir / f"{rid}.json"
        intermediate_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        status = data.get("status", "unknown")
        _log(f"[七看]   → {rid} 完成, status={status}")
        return rid, data

    max_workers = max(1, int(args.max_workers))
    if max_workers == 1:
        # 串行通道（调试/排查用）
        for spec in LOOK_SPECS:
            rid, data = _execute(spec)
            results[rid] = data
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_execute, spec): spec["rule_id"] for spec in LOOK_SPECS}
            for fut in as_completed(futures):
                rid, data = fut.result()
                results[rid] = data

    eight_questions: dict[str, Any] | None = None
    if args.include_eight_questions:
        print("[七看] Phase 2.5: 执行 look-08 八问 ...", file=sys.stderr)
        eight_questions = _run_eight_questions(
            stock=args.stock,
            as_of_date=as_of_date,
            db_path=args.db_path,
            base_output_dir=output_dir,
            look01_payload=results.get("look-01"),
            timeout_seconds=args.eight_questions_timeout,
            table_map_json=effective_map_json,
        )
        print(
            "[七看]   → look-08 完成, overall_status="
            f"{eight_questions.get('overall_status', 'unknown')}",
            file=sys.stderr,
        )

    # Phase 3: Aggregate
    print("[七看] Phase 3: 汇总红旗与评分 ...", file=sys.stderr)
    flags = _collect_all_flags(results)
    quality = _compute_quality_score(flags)
    human_requests = _collect_human_requests(results)
    if eight_questions is not None:
        human_requests = _extend_with_eight_questions_human_requests(
            human_requests,
            eight_questions,
        )
    critical_gaps = _collect_eight_questions_critical_gaps(eight_questions)

    # Phase 4: Commentary + recommendations
    print("[七看] Phase 4: 生成评语与建议 ...", file=sys.stderr)
    recommendations = _generate_recommendations(results, flags, quality)

    render_args = (
        args.stock, as_of_date, args.lookback_years,
        results, flags, quality, human_requests, recommendations, output_dir,
        eight_questions, critical_gaps,
    )

    final_output = _render_json(*render_args) if args.format == "json" else _render_markdown(*render_args)

    final_output_path: Path | None = None
    if args.final_output:
        final_output_path = Path(args.final_output).expanduser().resolve()
        final_output_path.parent.mkdir(parents=True, exist_ok=True)
        final_output_path.write_text(final_output, encoding="utf-8")

    if args.format == "json":
        print(final_output)
    else:
        print(final_output)

    if final_output_path is not None:
        print(f"[七看] 最终报告已保存至: {final_output_path}", file=sys.stderr)

    print(f"\n[七看] 完成。中间文件已保存至: {output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()

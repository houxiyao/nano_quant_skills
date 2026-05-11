"""ClickHouse 数据门禁——表存在性 + 数据充足性检查。"""

from __future__ import annotations

import logging
from typing import Any

from .connection import Connection

logger = logging.getLogger(__name__)

# 七看所需的核心表
REQUIRED_TABLES_SEVEN_LOOKS = [
    "fin_income",
    "fin_indicator",
    "fin_cashflow",
    "fin_balance",
]

# 八问额外需要的表
REQUIRED_TABLES_EIGHT_QUESTIONS = [
    "idx_sw_l3_peers",
    "stk_company",
    "stk_managers",
    "stk_rewards",
    "fin_top10_holders",
    "stk_name_history",
    "stk_pledge_stat",
    "fin_forecast",
    "fin_mainbz",
]


def _list_tables(con: Connection) -> set[str]:
    """列出当前数据库中所有可见的表。"""
    try:
        rows = con.execute(
            "SELECT name FROM system.tables WHERE database = currentDatabase()"
        ).fetchall()
        return {str(r[0]) for r in rows}
    except Exception:  # noqa: BLE001
        return set()


def run_preflight(
    con: Connection,
    ts_code: str,
    *,
    include_eight_questions: bool = False,
) -> dict[str, Any]:
    """执行数据门禁检查。

    Returns:
        {
            "passed": bool,
            "missing_tables": [...],
            "data_checks": {...},
            "warnings": [...]
        }
    """
    available = _list_tables(con)
    required = list(REQUIRED_TABLES_SEVEN_LOOKS)
    if include_eight_questions:
        required.extend(REQUIRED_TABLES_EIGHT_QUESTIONS)

    missing = [t for t in required if t not in available]
    warnings: list[str] = []

    # 基础数据充足性：fin_income 中是否有该股票数据
    data_checks: dict[str, Any] = {}
    if "fin_income" in available:
        try:
            count = con.execute(
                "SELECT COUNT(*) FROM fin_income WHERE ts_code = ?", [ts_code]
            ).fetchone()[0]
            data_checks["fin_income_rows"] = count
            if count == 0:
                warnings.append(f"fin_income 中无 {ts_code} 数据")
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"fin_income 查询失败: {exc}")

    passed = len(missing) == 0 and not any(
        "无" in w and "数据" in w for w in warnings
    )

    return {
        "passed": passed,
        "missing_tables": missing,
        "data_checks": data_checks,
        "warnings": warnings,
    }

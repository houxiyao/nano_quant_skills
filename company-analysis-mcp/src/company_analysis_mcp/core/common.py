"""统一的 ClickHouse 连接管理 + 公司类型检测。

消除原 7 份 look-*/scripts/common.py 的重复。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime

from .connection import ClickHouseAdapter, Connection, get_connection as _get_conn

REPORT_TYPE = "1"
FINANCIAL_COMP_TYPES = {"2", "3", "4"}
COMPANY_TYPE_LABELS = {
    "1": "一般工商业",
    "2": "银行",
    "3": "保险",
    "4": "证券",
}


@dataclass(frozen=True)
class CompanyProfile:
    stock: str
    comp_type: str | None
    comp_type_label: str
    source_table: str | None
    latest_end_date: date | None
    visible_date: date | None

    @property
    def is_financial(self) -> bool:
        return self.comp_type in FINANCIAL_COMP_TYPES

    @property
    def warning(self) -> str | None:
        if not self.is_financial:
            return None
        return (
            f"目标公司属于金融类公司（comp_type={self.comp_type}，{self.comp_type_label}），"
            "当前规则不适用。"
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "stock": self.stock,
            "comp_type": self.comp_type,
            "comp_type_label": self.comp_type_label,
            "source_table": self.source_table,
            "latest_end_date": self.latest_end_date.isoformat() if self.latest_end_date else None,
            "visible_date": self.visible_date.isoformat() if self.visible_date else None,
            "is_financial": self.is_financial,
        }


def parse_date(value: str | None) -> date:
    """解析日期字符串，空值返回今天。"""
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _load_table_map(table_map_cfg: str = "") -> dict[str, str]:
    """从配置或环境变量加载表映射。"""
    raw = table_map_cfg or os.environ.get("SEVEN_LOOK_DB_TABLE_MAP", "")
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
        if isinstance(loaded, dict):
            return {str(k): str(v) for k, v in loaded.items() if k != v}
    except Exception:  # noqa: BLE001
        pass
    return {}


def get_connection() -> Connection:
    """从全局配置获取 ClickHouse 连接适配器。"""
    from ..config import get_settings

    cfg = get_settings()
    table_map = _load_table_map(cfg.database.table_map)
    return _get_conn(table_map)


def detect_company_profile(
    con: Connection,
    stock: str,
    as_of_date: date,
) -> CompanyProfile:
    """通过三表 UNION 检测公司类型（一般工商业/银行/保险/证券）。"""
    base_query = """
    WITH candidates AS (
        SELECT
            'fin_income' AS source_table,
            comp_type,
            end_date,
            COALESCE(f_ann_date, ann_date, end_date) AS visible_date
        FROM fin_income
        WHERE ts_code = ? AND report_type = ?

        UNION ALL

        SELECT
            'fin_balance' AS source_table,
            comp_type,
            end_date,
            COALESCE(f_ann_date, ann_date, end_date) AS visible_date
        FROM fin_balance
        WHERE ts_code = ? AND report_type = ?

        UNION ALL

        SELECT
            'fin_cashflow' AS source_table,
            comp_type,
            end_date,
            COALESCE(f_ann_date, ann_date, end_date) AS visible_date
        FROM fin_cashflow
        WHERE ts_code = ? AND report_type = ?
    )
    SELECT
        source_table,
        comp_type,
        end_date,
        visible_date
    FROM candidates
    {where_clause}
    ORDER BY visible_date DESC NULLS LAST, end_date DESC NULLS LAST, source_table
    LIMIT 1
    """
    params = [stock, REPORT_TYPE, stock, REPORT_TYPE, stock, REPORT_TYPE]
    row = con.execute(
        base_query.format(where_clause="WHERE visible_date <= CAST(? AS DATE)"),
        params + [as_of_date],
    ).fetchone()
    if row is None:
        row = con.execute(base_query.format(where_clause=""), params).fetchone()

    if row is None:
        return CompanyProfile(
            stock=stock,
            comp_type=None,
            comp_type_label="未知",
            source_table=None,
            latest_end_date=None,
            visible_date=None,
        )

    source_table, comp_type, latest_end_date, visible_date = row
    return CompanyProfile(
        stock=stock,
        comp_type=comp_type,
        comp_type_label=COMPANY_TYPE_LABELS.get(comp_type, "未知"),
        source_table=source_table,
        latest_end_date=latest_end_date,
        visible_date=visible_date,
    )

"""ClickHouse 连接适配器。

提供与 duckdb.DuckDBPyConnection 兼容的接口，内部使用 clickhouse-connect。
工具层代码无需修改 SQL 或执行逻辑，只需替换类型注解即可。
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 类型推断：Python 值 → ClickHouse 参数类型后缀
# ---------------------------------------------------------------------------

_PY_TO_CH_TYPE: dict[type, str] = {
    str: "String",
    int: "Int64",
    float: "Float64",
    date: "Date",
    datetime: "DateTime",
    bool: "UInt8",
}


def _infer_ch_type(value: Any) -> str:
    """根据 Python 值类型推断 ClickHouse 参数类型标签。"""
    if value is None:
        return "Nullable(String)"
    for py_type, ch_type in _PY_TO_CH_TYPE.items():
        if isinstance(value, py_type):
            return ch_type
    return "String"


# ---------------------------------------------------------------------------
# SQL 翻译
# ---------------------------------------------------------------------------

# DuckDB 类型 → ClickHouse 类型（用于 CAST 语句）
_TYPE_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bVARCHAR\b", re.IGNORECASE), "String"),
    (re.compile(r"\bINTEGER\b", re.IGNORECASE), "Int32"),
    (re.compile(r"\bBIGINT\b", re.IGNORECASE), "Int64"),
    (re.compile(r"\bBOOLEAN\b", re.IGNORECASE), "UInt8"),
]

# information_schema → system.tables 重写
_INFO_SCHEMA_RE = re.compile(
    r"\bFROM\s+information_schema\.tables\b",
    re.IGNORECASE,
)
_TABLE_NAME_FILTER_RE = re.compile(
    r"\btable_name\b",
    re.IGNORECASE,
)
_TABLE_SCHEMA_FILTER_RE = re.compile(
    r"\btable_schema\s*=\s*'main'",
    re.IGNORECASE,
)

# 布尔值比较修正
_FALSE_RE = re.compile(r"\b=\s*FALSE\b", re.IGNORECASE)
_TRUE_RE = re.compile(r"\b=\s*TRUE\b", re.IGNORECASE)


def _translate(
    query: str,
    params: list[Any] | None,
    table_map: dict[str, str],
) -> tuple[str, dict[str, Any] | None]:
    """将 DuckDB 风格的 SQL 翻译为 ClickHouse 兼容格式。

    1. ? 占位符 → {p0:Type}, {p1:Type}, ...
    2. CAST 类型关键字替换
    3. information_schema 重写
    4. 布尔值修正
    5. 表名映射
    """
    sql = query

    # 1) 替换 ? 占位符为命名参数
    ch_params: dict[str, Any] | None = None
    if params:
        ch_params = {}
        idx = 0

        def _replace_placeholder(match: re.Match[str]) -> str:
            nonlocal idx
            if idx < len(params):
                val = params[idx]
                param_name = f"p{idx}"
                ch_type = _infer_ch_type(val)
                ch_params[param_name] = val  # type: ignore[index]
                idx += 1
                return "{" + f"{param_name}:{ch_type}" + "}"
            return match.group(0)

        # 匹配不在字符串常量内的 ? 号
        sql = re.sub(r"\?", _replace_placeholder, sql)

    # 2) CAST 类型替换
    for pattern, replacement in _TYPE_REPLACEMENTS:
        sql = pattern.sub(replacement, sql)

    # 3) information_schema → system.tables 重写
    if _INFO_SCHEMA_RE.search(sql):
        sql = _INFO_SCHEMA_RE.sub("FROM system.tables", sql)
        sql = _TABLE_NAME_FILTER_RE.sub("name", sql)
        sql = _TABLE_SCHEMA_FILTER_RE.sub(
            "database = currentDatabase()", sql
        )

    # 4) 布尔值修正（ClickHouse 使用 UInt8）
    sql = _FALSE_RE.sub("= 0", sql)
    sql = _TRUE_RE.sub("= 1", sql)

    # 5) 表名映射
    if table_map:
        for logical_name, physical_name in table_map.items():
            # 词边界匹配，避免部分替换
            pattern = re.compile(r"\b" + re.escape(logical_name) + r"\b")
            sql = pattern.sub(physical_name, sql)

    return sql, ch_params


# ---------------------------------------------------------------------------
# CursorResult — 模拟 DB-API 2.0 cursor
# ---------------------------------------------------------------------------


class CursorResult:
    """模拟 DB-API 2.0 cursor 结果对象。

    兼容 DuckDB 的 .description / .fetchone() / .fetchall() 接口。
    """

    def __init__(self, column_names: list[str], rows: list[tuple]):
        self.description: list[tuple[str, ...]] = [
            (name, None, None, None, None, None, None) for name in column_names
        ]
        self._rows = rows
        self._pos = 0

    def fetchone(self) -> tuple | None:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchall(self) -> list[tuple]:
        remaining = self._rows[self._pos:]
        self._pos = len(self._rows)
        return remaining


# ---------------------------------------------------------------------------
# ClickHouseAdapter — 对外兼容 duckdb.DuckDBPyConnection
# ---------------------------------------------------------------------------


class ClickHouseAdapter:
    """适配 clickhouse-connect Client 为 duckdb.DuckDBPyConnection 兼容接口。

    用法与 DuckDB 连接完全相同：
        result = con.execute(query, [param1, param2])
        rows = result.fetchall()
        one = result.fetchone()
        cols = [d[0] for d in result.description]
    """

    def __init__(
        self,
        client: Client,
        table_map: dict[str, str] | None = None,
    ):
        self._client = client
        self._table_map = table_map or {}

    def execute(self, query: str, params: list[Any] | None = None) -> CursorResult:
        """执行 SQL 查询，自动翻译占位符和方言差异。"""
        translated_sql, ch_params = _translate(query, params, self._table_map)
        try:
            result = self._client.query(translated_sql, parameters=ch_params)
            rows = [tuple(r) for r in result.result_rows]
            return CursorResult(list(result.column_names), rows)
        except Exception as exc:
            logger.error(
                "ClickHouse 查询失败:\nSQL: %s\nParams: %s\nError: %s",
                translated_sql[:500],
                ch_params,
                exc,
            )
            raise

    def close(self) -> None:
        """No-op — HTTP 连接池由 clickhouse-connect 自动管理。"""
        pass


# 类型别名，供全包引用
Connection = ClickHouseAdapter


# ---------------------------------------------------------------------------
# Client 单例工厂
# ---------------------------------------------------------------------------

_client_instance: Client | None = None


def get_client() -> Client:
    """获取全局 ClickHouse Client 单例。"""
    global _client_instance
    if _client_instance is None:
        from ..config import get_settings

        cfg = get_settings().database
        _client_instance = clickhouse_connect.get_client(
            host=cfg.ch_host,
            port=cfg.ch_port,
            username=cfg.ch_user,
            password=cfg.ch_password,
            database=cfg.ch_database,
        )
        logger.info(
            "ClickHouse 连接已建立: %s:%d/%s",
            cfg.ch_host,
            cfg.ch_port,
            cfg.ch_database,
        )
    return _client_instance


def get_connection(table_map: dict[str, str] | None = None) -> ClickHouseAdapter:
    """获取 ClickHouseAdapter 实例（带可选表名映射）。"""
    return ClickHouseAdapter(get_client(), table_map)

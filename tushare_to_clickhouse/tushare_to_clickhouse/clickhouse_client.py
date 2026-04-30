"""ClickHouse client manager with lazy connection, table operations, and sync state."""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional, Set

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from loguru import logger

from tushare_to_clickhouse.config import SyncConfig
from tushare_to_clickhouse.schema_manager import (
    build_create_table_sql,
    coerce_date_columns,
    infer_clickhouse_schema,
)


class ClickHouseManager:
    """Manages ClickHouse connection, table operations, and sync state."""

    def __init__(self, config: SyncConfig):
        self._config = config
        self._client: Optional[Client] = None

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self._config.ch_host,
                port=self._config.ch_port,
                username=self._config.ch_user,
                password=self._config.ch_password,
                database=self._config.ch_database,
            )
            logger.info(
                f"Connected to ClickHouse at "
                f"{self._config.ch_host}:{self._config.ch_port}/{self._config.ch_database}"
            )
        return self._client

    # ------------------------------------------------------------------
    # Database / table metadata
    # ------------------------------------------------------------------

    def ensure_database(self) -> None:
        self.client.command(f'CREATE DATABASE IF NOT EXISTS "{self._config.ch_database}"')

    def table_exists(self, table: str) -> bool:
        db = self.client.database or "default"
        result = self.client.query(
            "SELECT COUNT() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": db, "tbl": table},
        )
        return bool(result.result_rows and result.result_rows[0][0] > 0)

    def get_columns(self, table: str) -> List[str]:
        db = self.client.database or "default"
        result = self.client.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tbl:String} ORDER BY position",
            parameters={"db": db, "tbl": table},
        )
        return [row[0] for row in result.result_rows]

    def get_date_columns(self, table: str) -> Set[str]:
        db = self.client.database or "default"
        result = self.client.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tbl:String} "
            "AND type IN ('Date', 'DateTime', 'Nullable(Date)', 'Nullable(DateTime)')",
            parameters={"db": db, "tbl": table},
        )
        return {row[0] for row in result.result_rows}

    # ------------------------------------------------------------------
    # DDL / DML
    # ------------------------------------------------------------------

    def drop_table(self, table: str) -> None:
        self.client.command(f'DROP TABLE IF EXISTS "{table}"')
        logger.info(f"Dropped table {table}")

    def create_table_from_df(
        self,
        table: str,
        df: pd.DataFrame,
        order_by: List[str],
        partition_by: Optional[str] = None,
        engine: str = "MergeTree",
    ) -> Set[str]:
        """Create a ClickHouse table from a DataFrame's schema.

        Returns the set of columns inferred as Date type, so callers can
        coerce the DataFrame before inserting.
        """
        schema, date_cols = infer_clickhouse_schema(df)
        sql = build_create_table_sql(table, schema, order_by, engine, partition_by)
        self.client.command(sql)
        order_by_str = ", ".join(f'"{c}"' for c in order_by)
        logger.info(f"Created table {table} with ORDER BY ({order_by_str})")
        return date_cols

    def insert_dataframe(
        self, table: str, df: pd.DataFrame, batch_size: Optional[int] = None
    ) -> int:
        """Insert DataFrame into ClickHouse, optionally in batches."""
        if df.empty:
            return 0
        rows = len(df)
        if batch_size and batch_size < rows:
            for start in range(0, rows, batch_size):
                chunk = df.iloc[start : start + batch_size]
                self.client.insert_df(table, chunk, database=self.client.database)
            logger.debug(f"Inserted {rows} rows into {table} in {(rows + batch_size - 1) // batch_size} batches")
        else:
            self.client.insert_df(table, df, database=self.client.database)
            logger.debug(f"Inserted {rows} rows into {table}")
        return rows

    # ------------------------------------------------------------------
    # Sync state table (ReplacingMergeTree)
    # ------------------------------------------------------------------

    STATE_TABLE = "table_sync_state"

    def ensure_sync_state_table(self) -> None:
        sql = (
            f'CREATE TABLE IF NOT EXISTS "{self.STATE_TABLE}" (\n'
            '    "source_table" String,\n'
            '    "dimension_type" String,\n'
            '    "dimension_value" String,\n'
            '    "is_sync" Int8,\n'
            '    "error_message" String,\n'
            '    "updated_at" DateTime DEFAULT now()\n'
            ") ENGINE = ReplacingMergeTree(updated_at)\n"
            'ORDER BY (source_table, dimension_type, dimension_value)'
        )
        self.client.command(sql)

    def write_sync_status(
        self,
        source_table: str,
        dimension_type: str,
        dimension_value: str,
        is_sync: int,
        error_message: str = "",
    ) -> None:
        self.client.insert(
            self.STATE_TABLE,
            [[source_table, dimension_type, dimension_value, is_sync, error_message, datetime.now()]],
            database=self.client.database,
        )

    def list_synced_dimensions(self, source_table: str, dimension_type: str) -> Set[str]:
        result = self.client.query(
            (
                f'SELECT dimension_value FROM "{self.STATE_TABLE}" FINAL '
                "WHERE source_table = {src:String} AND dimension_type = {dim:String} AND is_sync = 1"
            ),
            parameters={"src": source_table, "dim": dimension_type},
        )
        return {row[0] for row in result.result_rows}

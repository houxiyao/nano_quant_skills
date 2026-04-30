"""Core sync engine: Tushare -> ClickHouse."""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from tushare_to_clickhouse.clickhouse_client import ClickHouseManager
from tushare_to_clickhouse.config import SyncConfig
from tushare_to_clickhouse.registry import RegistryEntry, SyncRegistry
from tushare_to_clickhouse.schema_manager import (
    align_dataframe_to_schema,
    coerce_date_columns,
    dedupe_by_pk,
    sanitize_all_nan_columns,
)
from tushare_to_clickhouse.tushare_client import TushareFetcher


class SyncError(Exception):
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.context = context or {}


class SyncEngine:
    """Orchestrates Tushare data sync to ClickHouse."""

    def __init__(self, config: SyncConfig):
        self.config = config
        self.ch = ClickHouseManager(config)
        self.fetcher = TushareFetcher(config.tushare_token)
        # Initialize database and state table
        self.ch.ensure_database()
        self.ch.ensure_sync_state_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sync_table(
        self,
        endpoint: str,
        target_table: str,
        *,
        dimension_type: str = "none",
        dimension_field: Optional[str] = None,
        method: str = "query",
        mode: str = "overwrite",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sync_all: bool = False,
        params: Optional[Dict[str, Any]] = None,
        sleep: Optional[float] = None,
        max_retries: Optional[int] = None,
        allow_empty_result: bool = False,
        publish_cutoff_hour: Optional[int] = None,
        disable_safe_trade_date: bool = False,
        order_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        pk: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Sync a single Tushare endpoint to ClickHouse."""
        # Resolve defaults from config
        _sleep = sleep if sleep is not None else self.config.default_sleep
        _retries = max_retries if max_retries is not None else self.config.max_retries
        _cutoff = publish_cutoff_hour if publish_cutoff_hour is not None else self.config.publish_cutoff_hour

        source = endpoint
        target = target_table or source
        if order_by is None:
            if dimension_type == "none":
                order_by = ["ts_code"]
            else:
                dim_col = dimension_field or dimension_type
                order_by = ["ts_code", dim_col]
                if dimension_type == "period" and dimension_field is None:
                    logger.warning(
                        f"order_by defaulted to {order_by}. For period tables, "
                        "consider setting --dimension-field and --order-by explicitly."
                    )

        dims = self._resolve_dimensions(
            source_table=source,
            dimension_type=dimension_type,
            start_date=start_date or self.config.default_start_date,
            end_date=end_date,
            sync_all=sync_all,
            publish_cutoff_hour=_cutoff,
            disable_safe_trade_date=disable_safe_trade_date,
        )

        if not dims:
            result = {
                "source_table": source,
                "target_table": target,
                "dimension_type": dimension_type,
                "processed": 0,
                "loaded_rows": 0,
                "total_rows": 0,
                "mode": mode,
            }
            _log_event("sync_skipped", result)
            return result

        _log_event(
            "sync_started",
            {
                "source_table": source,
                "target_table": target,
                "dimension_type": dimension_type,
                "dimensions": len(dims),
                "mode": mode,
            },
        )

        processed = 0
        total_loaded = 0
        total_rows = 0
        first_batch = True

        for dim in dims:
            status_dim = dim or datetime.now().strftime("%Y%m%d")
            try:
                kwargs = dict(params or {})
                if dimension_type != "none":
                    field = dimension_field or dimension_type
                    kwargs[field] = dim

                df = self.fetcher.fetch(endpoint, method, kwargs, _retries)
                self._ensure_expected_rows(
                    df, source, target, dim, dimension_type, allow_empty_result
                )
                total_rows, loaded = self._write_batch(
                    df, target, mode, first_batch, order_by, partition_by, pk
                )
                first_batch = False
                processed += 1
                total_loaded += loaded

                if dimension_type != "none":
                    self.ch.write_sync_status(source, dimension_type, status_dim, 1)

                _log_event(
                    "dimension_done",
                    {
                        "source_table": source,
                        "dimension": dim,
                        "loaded": loaded,
                        "total": total_rows,
                    },
                )

                if _sleep > 0:
                    time.sleep(_sleep)

            except Exception as exc:
                if dimension_type != "none":
                    self.ch.write_sync_status(
                        source, dimension_type, status_dim, 0, str(exc)
                    )
                logger.error(f"Failed: source={source} dim={dim}: {exc}")
                if sync_all:
                    continue
                raise

        result = {
            "source_table": source,
            "target_table": target,
            "dimension_type": dimension_type,
            "mode": mode,
            "processed": processed,
            "loaded_rows": total_loaded,
            "total_rows": total_rows,
        }
        _log_event("sync_completed", result)
        return result

    def sync_registry(self, registry: SyncRegistry) -> List[Dict[str, Any]]:
        """Batch sync from a SyncRegistry object."""
        results: List[Dict[str, Any]] = []
        for entry in registry.entries:
            try:
                result = self.sync_table(
                    endpoint=entry.endpoint,
                    target_table=entry.target_table,
                    dimension_type=entry.dimension_type,
                    dimension_field=entry.dimension_field,
                    method=entry.method,
                    mode=entry.mode,
                    start_date=entry.start_date,
                    end_date=entry.end_date,
                    sync_all=True,
                    params=entry.params,
                    sleep=entry.sleep,
                    max_retries=entry.max_retries,
                    allow_empty_result=entry.allow_empty_result,
                    order_by=entry.order_by or None,
                    partition_by=entry.partition_by,
                    pk=entry.pk or None,
                )
                results.append(result)
                logger.info(
                    f"✅ {entry.target_table}: loaded {result['loaded_rows']} rows, "
                    f"total {result['total_rows']}"
                )
            except Exception as exc:
                logger.error(f"❌ {entry.target_table}: {exc}")
                results.append(
                    {
                        "source_table": entry.source_table,
                        "target_table": entry.target_table,
                        "error": str(exc),
                    }
                )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_dimensions(
        self,
        source_table: str,
        dimension_type: str,
        start_date: str,
        end_date: Optional[str],
        sync_all: bool,
        publish_cutoff_hour: int,
        disable_safe_trade_date: bool,
    ) -> List[str]:
        if dimension_type == "none":
            return [""]

        if dimension_type == "trade_date":
            safe_end = self.fetcher.resolve_trade_date_end(
                end_date=end_date,
                publish_cutoff_hour=publish_cutoff_hour,
                disable_safe=disable_safe_trade_date,
            )
            start = self.fetcher.normalize_date(start_date)
            if start > safe_end:
                raise SyncError(
                    "No safe trade_date window available yet.",
                    {"start": start, "safe_end": safe_end},
                )
            values = self.fetcher.get_trade_dates(start, safe_end)
        elif dimension_type == "period":
            start = self.fetcher.normalize_date(start_date)
            end = self.fetcher.normalize_date(
                end_date or datetime.now().strftime("%Y%m%d")
            )
            values = self.fetcher.get_report_periods(start, end)
        else:
            raise SyncError(f"Unsupported dimension_type: {dimension_type}")

        if not values:
            return []

        if sync_all:
            synced = self.ch.list_synced_dimensions(source_table, dimension_type)
            values = [v for v in values if v not in synced]

        return values

    def _ensure_expected_rows(
        self,
        df: pd.DataFrame,
        source_table: str,
        target_table: str,
        dim_value: str,
        dimension_type: str,
        allow_empty_result: bool,
    ) -> None:
        if dimension_type == "none" or allow_empty_result or not df.empty:
            return
        raise SyncError(
            "Empty payload for incremental sync; not marking as successful. "
            "Use allow_empty_result if zero rows are expected.",
            {
                "source_table": source_table,
                "target_table": target_table,
                "dimension_type": dimension_type,
                "dimension_value": dim_value,
            },
        )

    def _write_batch(
        self,
        df: pd.DataFrame,
        target_table: str,
        mode: str,
        first_batch: bool,
        order_by: List[str],
        partition_by: Optional[str],
        pk: Optional[List[str]] = None,
    ) -> Tuple[int, int]:
        """Write DataFrame to ClickHouse. Returns (total_rows, loaded_rows)."""
        if df.empty:
            if not self.ch.table_exists(target_table):
                return 0, 0
            result = self.ch.client.query(f'SELECT COUNT() FROM "{target_table}"')
            total = int(result.result_rows[0][0]) if result.result_rows else 0
            return total, 0

        # Sanitize all-NaN float columns before any schema operations
        df = sanitize_all_nan_columns(df)

        loaded = len(df)

        if mode == "overwrite" and first_batch:
            self.ch.drop_table(target_table)

        if not self.ch.table_exists(target_table):
            date_cols = self.ch.create_table_from_df(
                target_table, df, order_by=order_by, partition_by=partition_by
            )
            if date_cols:
                df = coerce_date_columns(df, date_cols)
            # Normalize NaN → None for new tables
            df = df.where(pd.notna(df), None)
        else:
            existing_cols = self.ch.get_columns(target_table)
            date_cols = self.ch.get_date_columns(target_table)
            df = align_dataframe_to_schema(df, existing_cols, date_cols)

        # Dedupe by PK if provided
        if pk:
            df = dedupe_by_pk(df, pk, target_table)

        self.ch.insert_dataframe(target_table, df, batch_size=self.config.batch_size)

        result = self.ch.client.query(f'SELECT COUNT() FROM "{target_table}"')
        total = int(result.result_rows[0][0]) if result.result_rows else loaded
        return total, loaded


def _log_event(event: str, payload: Dict[str, Any]) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))

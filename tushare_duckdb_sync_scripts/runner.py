from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import duckdb

from tushare_duckdb_sync_scripts.common import SyncTask, bootstrap_project_path, log_event, period_to_duckdb_date, sql_placeholders

bootstrap_project_path()

from tushare_duckdb_sync_scripts.etl.tushare_to_duckdb import (  # noqa: E402
    _parse_table_name,
    _qualified_name,
    _resolve_status_backend,
    _target_exists,
    run_etl,
)


@dataclass
class BatchSummary:
    batch_name: str
    successes: List[Dict[str, Any]] = field(default_factory=list)
    failures: List[Dict[str, Any]] = field(default_factory=list)
    planned: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        return 0 if not self.failures else 2

    def to_payload(self) -> Dict[str, Any]:
        return {
            "batch_name": self.batch_name,
            "success_count": len(self.successes),
            "failed_count": len(self.failures),
            "planned_count": len(self.planned),
            "failures": self.failures,
        }


def namespace_to_payload(args: argparse.Namespace) -> Dict[str, Any]:
    payload = vars(args).copy()
    if payload.get("params"):
        payload["params"] = str(payload["params"])
    return payload


def execute_etl_task(
    task: SyncTask,
    args: argparse.Namespace,
    *,
    batch_name: str,
    task_label: Optional[str] = None,
) -> Dict[str, Any]:
    label = task_label or task.target_table
    log_event(
        "sync_task_started",
        {
            "batch_name": batch_name,
            "task_label": label,
            "source_table": task.source_table,
            "target_table": task.target_table,
            "dimension_type": task.dimension_type,
            "start_date": getattr(args, "start_date", None) or "",
            "end_date": getattr(args, "end_date", None) or "",
            "mode": getattr(args, "mode", task.mode),
            "sync_all": bool(getattr(args, "sync_all", False)),
        },
    )
    result = run_etl(args)
    result["task_label"] = label
    result["source_table"] = task.source_table
    result["target_table"] = task.target_table
    result["batch_name"] = batch_name
    log_event("sync_task_completed", result)
    return result


def reset_period_state(duckdb_path: Path, task: SyncTask, periods: Sequence[str]) -> Dict[str, int]:
    normalized_periods = [str(period) for period in periods]
    if not normalized_periods:
        return {"deleted_rows": 0, "deleted_status": 0}

    target_periods = [period_to_duckdb_date(period) for period in normalized_periods]
    with duckdb.connect(str(duckdb_path)) as con:
        schema_name, table_name = _parse_table_name(task.target_table)
        fq_name = _qualified_name(schema_name, table_name)

        deleted_rows = 0
        if _target_exists(con, schema_name, table_name):
            row = con.execute(
                f"SELECT COUNT(*) FROM {fq_name} WHERE end_date IN ({sql_placeholders(len(target_periods))})",
                target_periods,
            ).fetchone()
            deleted_rows = int(row[0]) if row else 0
            if deleted_rows > 0:
                con.execute(
                    f"DELETE FROM {fq_name} WHERE end_date IN ({sql_placeholders(len(target_periods))})",
                    target_periods,
                )

        deleted_status = 0
        status_backend = _resolve_status_backend(con)
        status_name = str(status_backend["fq_name"])
        if status_backend["mode"] == "legacy":
            row = con.execute(
                f"SELECT COUNT(*) FROM {status_name} WHERE tushare_table_name = ? AND trade_date IN ({sql_placeholders(len(normalized_periods))})",
                [task.source_table, *normalized_periods],
            ).fetchone()
            deleted_status = int(row[0]) if row else 0
            if deleted_status > 0:
                con.execute(
                    f"DELETE FROM {status_name} WHERE tushare_table_name = ? AND trade_date IN ({sql_placeholders(len(normalized_periods))})",
                    [task.source_table, *normalized_periods],
                )
        else:
            row = con.execute(
                f"SELECT COUNT(*) FROM {status_name} WHERE source_table = ? AND dimension_type = 'period' AND dimension_value IN ({sql_placeholders(len(normalized_periods))})",
                [task.source_table, *normalized_periods],
            ).fetchone()
            deleted_status = int(row[0]) if row else 0
            if deleted_status > 0:
                con.execute(
                    f"DELETE FROM {status_name} WHERE source_table = ? AND dimension_type = 'period' AND dimension_value IN ({sql_placeholders(len(normalized_periods))})",
                    [task.source_table, *normalized_periods],
                )

    payload = {
        "source_table": task.source_table,
        "target_table": task.target_table,
        "deleted_rows": deleted_rows,
        "deleted_status": deleted_status,
    }
    log_event("financial_period_reset", payload)
    return payload

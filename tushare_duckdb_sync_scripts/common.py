from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
import fcntl
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from loguru import logger


PACKAGE_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = PACKAGE_ROOT.parent
# 数据库默认放在仓库根的 data/，与下游分析 skill (2min-company-analysis) 保持一致；
# 可通过环境变量 TUSHARE_SYNC_DUCKDB_PATH 覆盖。
DEFAULT_DUCKDB_PATH = (
    Path(os.environ["TUSHARE_SYNC_DUCKDB_PATH"]).expanduser().resolve()
    if os.environ.get("TUSHARE_SYNC_DUCKDB_PATH")
    else WORKSPACE_ROOT / "data" / "ashare.duckdb"
)
DEFAULT_LOG_DIR = PACKAGE_ROOT / "logs" / "tushare_sync"
DEFAULT_LOCK_DIR = PACKAGE_ROOT / "temporary" / "locks"
REGISTRY_PATH = PACKAGE_ROOT / "mapping_registry.json"
LEGACY_REGISTRY_PATH = WORKSPACE_ROOT / "docs" / "mapping_registry.json"
CONDA_SH_PATH = Path(os.environ["CONDA_SH_PATH"]).expanduser().resolve() if os.environ.get("CONDA_SH_PATH") else None
CONDA_ENV_NAME = os.environ.get("CONDA_ENV_NAME", "legonanobot")
TRADE_DATE_PUBLISH_HOUR = 18


@dataclass(frozen=True)
class SyncTask:
    source_table: str
    target_table: str
    endpoint: str
    dimension_type: str
    method: str = "query"
    mode: str = "append"
    dimension_field: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    allow_empty_result: bool = False
    sleep_seconds: float = 0.3
    max_retries: int = 3
    base_sleep_seconds: float = 2.0

    def to_etl_args(
        self,
        duckdb_path: Path,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sync_all: bool = False,
        mode: Optional[str] = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            endpoint=self.endpoint,
            method=self.method,
            source_table=self.source_table,
            duckdb_path=str(duckdb_path),
            target_table=self.target_table,
            mode=mode or self.mode,
            dimension_type=self.dimension_type,
            dimension_field=self.dimension_field,
            start_date=start_date,
            end_date=end_date,
            sync_all=sync_all,
            params=json.dumps(self.params, ensure_ascii=False) if self.params else None,
            limit=None,
            max_retries=self.max_retries,
            base_sleep_seconds=self.base_sleep_seconds,
            sleep_seconds=self.sleep_seconds,
            allow_empty_result=self.allow_empty_result,
        )


SPECIAL_TASK_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "trade_cal": {
        "mode": "overwrite",
        "params": {"exchange": "SSE", "start_date": "20000101", "end_date": "20301231"},
        "sleep_seconds": 0.0,
    },
    "dividend": {
        "dimension_field": "ann_date",
        "allow_empty_result": True,
        "sleep_seconds": 0.15,
    },
    "share_float": {
        "dimension_field": "ann_date",
        "allow_empty_result": True,
        "sleep_seconds": 0.15,
    },
    "stock_basic": {"mode": "overwrite", "sleep_seconds": 0.0},
    "index_basic": {"mode": "overwrite", "sleep_seconds": 0.0},
    "namechange": {"mode": "overwrite", "sleep_seconds": 0.0},
    "bse_mapping": {"mode": "overwrite", "sleep_seconds": 0.0},
    "index_classify": {"mode": "overwrite", "sleep_seconds": 0.0},
    "index_member_all": {"mode": "overwrite", "sleep_seconds": 0.0},
    "stock_company": {"mode": "overwrite", "sleep_seconds": 0.0},
}


def bootstrap_project_path() -> None:
    if str(WORKSPACE_ROOT) not in sys.path:
        sys.path.insert(0, str(WORKSPACE_ROOT))


def resolve_registry_path(registry_path: Optional[Path] = None) -> Path:
    explicit_path = registry_path or (
        Path(os.environ["TUSHARE_SYNC_REGISTRY_PATH"]) if os.environ.get("TUSHARE_SYNC_REGISTRY_PATH") else None
    )
    if explicit_path is not None:
        resolved_path = Path(explicit_path).expanduser().resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Mapping registry not found: {resolved_path}")
        return resolved_path

    for candidate in (REGISTRY_PATH, LEGACY_REGISTRY_PATH):
        resolved_path = candidate.expanduser().resolve()
        if resolved_path.exists():
            return resolved_path

    checked_paths = ", ".join(
        str(candidate.expanduser().resolve()) for candidate in (REGISTRY_PATH, LEGACY_REGISTRY_PATH)
    )
    raise FileNotFoundError(
        "Unable to locate mapping registry. "
        f"Checked: {checked_paths}. "
        "Set TUSHARE_SYNC_REGISTRY_PATH to override the default lookup."
    )


def ensure_tushare_token() -> None:
    if os.environ.get("TUSHARE_TOKEN"):
        return
    raise RuntimeError("TUSHARE_TOKEN is required")


def configure_logging(script_name: str, log_dir: Optional[Path] = None) -> Path:
    output_dir = (log_dir or DEFAULT_LOG_DIR).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / f"{script_name}_{datetime.now().strftime('%Y%m%d')}.log"

    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )
    logger.add(
        str(log_file),
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
    )
    logger.info(f"日志文件: {log_file}")
    return log_file


def log_event(event: str, payload: Dict[str, Any]) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))


@contextmanager
def file_lock(lock_name: str, lock_dir: Optional[Path] = None) -> Iterator[Path]:
    target_dir = (lock_dir or DEFAULT_LOCK_DIR).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    lock_path = target_dir / f"{lock_name}.lock"
    with open(lock_path, "w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Another sync process is already running: {lock_path}") from exc
        handle.write(str(os.getpid()))
        handle.flush()
        try:
            yield lock_path
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_registry(registry_path: Optional[Path] = None) -> List[SyncTask]:
    path = resolve_registry_path(registry_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    tables = raw.get("tables", [])
    if not isinstance(tables, list):
        raise RuntimeError(f"Invalid mapping registry format: {path}")

    tasks: List[SyncTask] = []
    for item in tables:
        if not isinstance(item, dict):
            continue
        task = SyncTask(
            source_table=str(item["source_table"]),
            target_table=str(item["target_table"]),
            endpoint=str(item["endpoint"]),
            dimension_type=str(item["dimension_type"]),
            method=str(item.get("method") or "query"),
            mode="overwrite" if str(item["dimension_type"]) == "none" else "append",
        )
        overrides = SPECIAL_TASK_OVERRIDES.get(task.source_table, {})
        tasks.append(replace(task, **overrides))
    return tasks


def filter_tasks(
    tasks: Sequence[SyncTask],
    *,
    dimension_types: Optional[Iterable[str]] = None,
    targets: Optional[Iterable[str]] = None,
) -> List[SyncTask]:
    wanted_dimensions = {item for item in (dimension_types or [])}
    wanted_targets = {item for item in (targets or [])}
    filtered: List[SyncTask] = []
    for task in tasks:
        if wanted_dimensions and task.dimension_type not in wanted_dimensions:
            continue
        if wanted_targets and task.target_table not in wanted_targets and task.source_table not in wanted_targets:
            continue
        filtered.append(task)
    return filtered


def parse_table_filters(raw_value: Optional[str]) -> Optional[List[str]]:
    if not raw_value:
        return None
    items = [item.strip() for item in raw_value.split(",") if item.strip()]
    return items or None


def resolve_duckdb_path(raw_value: Optional[str]) -> Path:
    return Path(raw_value).expanduser().resolve() if raw_value else DEFAULT_DUCKDB_PATH.resolve()


def resolve_log_dir(raw_value: Optional[str]) -> Path:
    return Path(raw_value).expanduser().resolve() if raw_value else DEFAULT_LOG_DIR.resolve()


def resolve_safe_trade_end_date(now: Optional[datetime] = None) -> str:
    current = now or datetime.now()
    if current.hour >= TRADE_DATE_PUBLISH_HOUR:
        return current.strftime("%Y%m%d")
    return (current - timedelta(days=1)).strftime("%Y%m%d")


def resolve_trade_window(
    *,
    date_text: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = 7,
    now: Optional[datetime] = None,
) -> tuple[str, str]:
    if date_text:
        return date_text, date_text
    resolved_end = end_date or resolve_safe_trade_end_date(now)
    if start_date:
        return start_date, resolved_end
    current = datetime.now() if now is None else now
    resolved_start = (current - timedelta(days=lookback_days)).strftime("%Y%m%d")
    return resolved_start, resolved_end


def _quarter_end_month_day(month: int) -> tuple[int, int]:
    mapping = {
        1: (12, 31),
        2: (12, 31),
        3: (3, 31),
        4: (3, 31),
        5: (3, 31),
        6: (6, 30),
        7: (6, 30),
        8: (6, 30),
        9: (9, 30),
        10: (9, 30),
        11: (9, 30),
        12: (12, 31),
    }
    return mapping[month]


def resolve_recent_report_periods(as_of_date: Optional[str] = None, period_count: int = 2) -> List[str]:
    if period_count <= 0:
        raise ValueError("period_count must be positive")

    current = (
        datetime.strptime(as_of_date, "%Y%m%d").date()
        if as_of_date
        else date.today()
    )
    year = current.year
    month, day = _quarter_end_month_day(current.month)
    candidate = date(year, month, day)
    if candidate > current:
        year -= 1
        candidate = date(year, 12, 31)

    periods: List[str] = []
    while len(periods) < period_count:
        periods.append(candidate.strftime("%Y%m%d"))
        if candidate.month == 12:
            candidate = date(candidate.year, 9, 30)
        elif candidate.month == 9:
            candidate = date(candidate.year, 6, 30)
        elif candidate.month == 6:
            candidate = date(candidate.year, 3, 31)
        else:
            candidate = date(candidate.year - 1, 12, 31)
    return sorted(periods)


def period_to_duckdb_date(period: str) -> str:
    return datetime.strptime(period, "%Y%m%d").strftime("%Y-%m-%d")


def sql_placeholders(count: int) -> str:
    return ", ".join(["?"] * count)

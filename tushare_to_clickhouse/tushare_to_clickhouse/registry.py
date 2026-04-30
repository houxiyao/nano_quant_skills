"""Registry loading, validation, and filtering."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger


VALID_DIMENSION_TYPES = {"none", "trade_date", "period"}
VALID_MODES = {"overwrite", "append"}
REQUIRED_FIELDS = {"endpoint", "target_table"}


@dataclass
class RegistryEntry:
    """Single table entry from the sync registry."""

    source_table: str
    target_table: str
    endpoint: str
    dimension_type: str = "none"
    dimension_field: Optional[str] = None
    method: str = "query"
    pk: List[str] = field(default_factory=list)
    order_by: List[str] = field(default_factory=list)
    partition_by: Optional[str] = None
    mode: str = "overwrite"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    sleep: float = 0.3
    max_retries: int = 3
    allow_empty_result: bool = False
    points: Optional[int] = None
    params: Optional[Dict[str, Any]] = None
    description: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RegistryEntry:
        return cls(
            source_table=data.get("source_table", data.get("endpoint", "")),
            target_table=data["target_table"],
            endpoint=data["endpoint"],
            dimension_type=data.get("dimension_type", "none"),
            dimension_field=data.get("dimension_field"),
            method=data.get("method", "query"),
            pk=data.get("pk", []),
            order_by=data.get("order_by", []),
            partition_by=data.get("partition_by"),
            mode=data.get("mode", "overwrite"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            sleep=float(data.get("sleep", 0.3)),
            max_retries=int(data.get("max_retries", 3)),
            allow_empty_result=bool(data.get("allow_empty_result", False)),
            points=data.get("points"),
            params=data.get("params"),
            description=data.get("description", ""),
        )


class SyncRegistry:
    """Loads, validates, and filters table registry."""

    def __init__(self, entries: List[RegistryEntry]):
        self.entries = entries

    @classmethod
    def from_yaml(cls, path: str) -> SyncRegistry:
        """Load and validate registry from YAML file."""
        p = Path(path).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"Registry not found: {p}")
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        tables = data.get("tables", [])
        if not isinstance(tables, list):
            raise ValueError("Registry must contain a 'tables' list")

        entries: List[RegistryEntry] = []
        for i, item in enumerate(tables):
            _validate_entry(item, i)
            entries.append(RegistryEntry.from_dict(item))

        logger.info(f"Loaded {len(entries)} tables from registry {p}")
        return cls(entries)

    def filter_by_points(self, max_points: int) -> SyncRegistry:
        """Return a new registry with only entries within max_points."""
        filtered = []
        for e in self.entries:
            if e.points is None or e.points <= max_points:
                filtered.append(e)
            else:
                logger.debug(f"Skipping {e.target_table} (points={e.points} > {max_points})")
        logger.info(f"Filtered registry: {len(filtered)} / {len(self.entries)} tables within {max_points} points")
        return SyncRegistry(filtered)

    def filter_by_tables(self, tables: List[str]) -> SyncRegistry:
        """Return a new registry with only entries matching the given target/source table names."""
        wanted = set(tables)
        filtered = [
            e for e in self.entries
            if e.target_table in wanted or e.source_table in wanted
        ]
        return SyncRegistry(filtered)


def _validate_entry(item: Dict[str, Any], index: int) -> None:
    """Validate a single registry entry."""
    missing = REQUIRED_FIELDS - set(item.keys())
    if missing:
        raise ValueError(
            f"Registry entry #{index} ({item.get('target_table', '?')}): "
            f"missing required fields: {missing}"
        )
    dt = item.get("dimension_type", "none")
    if dt not in VALID_DIMENSION_TYPES:
        raise ValueError(
            f"Registry entry #{index} ({item['target_table']}): "
            f"invalid dimension_type '{dt}'"
        )
    mode = item.get("mode", "overwrite")
    if mode not in VALID_MODES:
        raise ValueError(
            f"Registry entry #{index} ({item['target_table']}): "
            f"invalid mode '{mode}'"
        )

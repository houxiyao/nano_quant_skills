"""YAML configuration loader with typed SyncConfig dataclass."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from loguru import logger


DEFAULT_CONFIG_PATH = Path.cwd() / "config.yaml"


@dataclass
class SyncConfig:
    """All configuration for tushare-to-clickhouse."""

    # ClickHouse
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""
    ch_database: str = "default"
    # Tushare
    tushare_token: str = ""
    # Sync defaults
    default_start_date: str = "20100101"
    default_start_period: str = "20100331"
    publish_cutoff_hour: int = 18
    default_sleep: float = 0.3
    max_retries: int = 3
    batch_size: int = 10000
    # Logging
    log_level: str = "INFO"
    log_file: str = ""
    log_retention_days: int = 30

    @classmethod
    def from_yaml(cls, path: Optional[str | Path] = None) -> SyncConfig:
        """Load config from YAML file, falling back to defaults for missing fields."""
        if path:
            config_path = Path(path).expanduser().resolve()
        else:
            config_path = DEFAULT_CONFIG_PATH.expanduser().resolve()

        if not config_path.exists():
            if path:
                raise FileNotFoundError(f"Config file not found: {config_path}")
            logger.warning(f"Config file not found at {config_path}, using defaults")
            return cls()

        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        ch = data.get("clickhouse", {})
        ts = data.get("tushare", {})
        sync = data.get("sync", {})
        log = data.get("logging", {})

        cfg = cls(
            ch_host=ch.get("host", "localhost"),
            ch_port=int(ch.get("port", 8123)),
            ch_user=ch.get("user", "default"),
            ch_password=ch.get("password", ""),
            ch_database=ch.get("database", "default"),
            tushare_token=ts.get("token", ""),
            default_start_date=sync.get("default_start_date", "20100101"),
            default_start_period=sync.get("default_start_period", "20100331"),
            publish_cutoff_hour=int(sync.get("publish_cutoff_hour", 18)),
            default_sleep=float(sync.get("default_sleep", 0.3)),
            max_retries=int(sync.get("max_retries", 3)),
            batch_size=int(sync.get("batch_size", 10000)),
            log_level=log.get("level", "INFO"),
            log_file=log.get("file", ""),
            log_retention_days=int(log.get("retention_days", 30)),
        )
        logger.info(f"Loaded config from {config_path}")
        return cfg

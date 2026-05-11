"""集中式配置管理模块。

支持四层配置源，优先级从高到低：
1. 命令行参数 (CLI)
2. YAML 配置文件
3. 环境变量
4. 内置默认值

配置文件查找顺序：
1. --config 指定路径
2. ./config.yaml
3. ./company-analysis-mcp.yaml
4. ~/.config/company-analysis-mcp/config.yaml
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


# ── 数据类定义 ───────────────────────────────────────────────


@dataclass
class DatabaseSettings:
    """ClickHouse 数据库连接配置（对齐 ts2ck 命名风格）。"""

    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""
    ch_database: str = "tushare_data"
    table_map: str = ""  # JSON 字符串，逻辑表名→物理表名映射


@dataclass
class ServerSettings:
    """MCP Server 运行参数。"""

    transport: str = "streamable-http"
    host: str = "0.0.0.0"
    port: int = 8001


@dataclass
class AnalysisSettings:
    """分析执行参数。"""

    default_lookback_years: int = 3
    max_workers: int = 4
    per_tool_timeout: int = 120


@dataclass
class Settings:
    """顶层配置，聚合所有子配置。"""

    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    server: ServerSettings = field(default_factory=ServerSettings)
    analysis: AnalysisSettings = field(default_factory=AnalysisSettings)


# ── 旧环境变量 → 新配置路径映射（向后兼容） ─────────────────────
_LEGACY_ENV_MAP: dict[str, tuple[str, str]] = {
    "SEVEN_LOOK_DB_TABLE_MAP": ("database", "table_map"),
}

# ── 新环境变量前缀 ────────────────────────────────────────────
_ENV_PREFIX = "COMPANY_ANALYSIS_MCP"

# ── 子配置类注册表 ────────────────────────────────────────────
_SECTION_CLASSES: dict[str, type] = {
    "database": DatabaseSettings,
    "server": ServerSettings,
    "analysis": AnalysisSettings,
}


# ── 工具函数 ─────────────────────────────────────────────────


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """递归合并两个字典，override 覆盖 base。"""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _cast_value(value: str, target_type: type) -> Any:
    """将环境变量字符串值转换为目标类型。"""
    if target_type is bool:
        return value.lower() in ("1", "true", "yes", "on")
    if target_type is int:
        return int(value)
    if target_type is float:
        return float(value)
    return value


def _find_config_file(cli_path: str | None = None) -> Path | None:
    """按优先级查找配置文件。"""
    if cli_path:
        p = Path(cli_path).expanduser()
        if p.is_file():
            return p
        logger.warning("--config 指定的配置文件不存在: %s", cli_path)
        return None

    candidates: list[Path] = []
    cwd = Path.cwd()
    candidates.append(cwd / "config.yaml")
    candidates.append(cwd / "company-analysis-mcp.yaml")
    candidates.append(Path("~/.config/company-analysis-mcp/config.yaml").expanduser())

    for path in candidates:
        if path.is_file():
            logger.info("使用配置文件: %s", path)
            return path

    return None


def _load_yaml_config(path: Path) -> dict[str, Any]:
    """加载 YAML 配置文件。"""
    try:
        text = path.read_text(encoding="utf-8")
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            logger.warning("配置文件内容不是字典格式，已忽略: %s", path)
            return {}
        return data
    except Exception as exc:  # noqa: BLE001
        logger.warning("加载配置文件失败: %s — %s", path, exc)
        return {}


def _load_env_overrides() -> dict[str, Any]:
    """从环境变量读取配置覆盖。"""
    overrides: dict[str, Any] = {}

    # 1) 旧环境变量兼容
    for env_name, (section, field_name) in _LEGACY_ENV_MAP.items():
        value = os.environ.get(env_name)
        if value is not None:
            overrides.setdefault(section, {})[field_name] = value

    # 2) 新环境变量（COMPANY_ANALYSIS_MCP_{SECTION}_{FIELD}）
    prefix = f"{_ENV_PREFIX}_"
    for env_name, env_value in os.environ.items():
        if not env_name.startswith(prefix):
            continue

        rest = env_name[len(prefix):].lower()
        for section_name, cls in _SECTION_CLASSES.items():
            section_prefix = section_name + "_"
            if rest.startswith(section_prefix):
                field_name = rest[len(section_prefix):]
                field_names = {f.name for f in fields(cls)}
                if field_name in field_names:
                    target_field = next(f for f in fields(cls) if f.name == field_name)
                    try:
                        typed_value = _cast_value(env_value, target_field.type)
                    except (ValueError, TypeError):
                        typed_value = env_value
                    overrides.setdefault(section_name, {})[field_name] = typed_value
                    break

    return overrides


def _apply_cli_overrides(
    data: dict[str, Any],
    cli_args: dict[str, Any] | None,
) -> dict[str, Any]:
    """将 CLI 参数覆盖到配置字典。"""
    if not cli_args:
        return data

    cli_mapping: dict[str, tuple[str, str]] = {
        "transport": ("server", "transport"),
        "host": ("server", "host"),
        "port": ("server", "port"),
        "ch_host": ("database", "ch_host"),
        "ch_port": ("database", "ch_port"),
        "ch_database": ("database", "ch_database"),
        "ch_user": ("database", "ch_user"),
        "ch_password": ("database", "ch_password"),
    }

    for cli_key, (section, field_name) in cli_mapping.items():
        value = cli_args.get(cli_key)
        if value is not None:
            data.setdefault(section, {})[field_name] = value

    return data


def _dict_to_settings(data: dict[str, Any]) -> Settings:
    """将合并后的字典转换为 Settings dataclass 实例。"""
    kwargs: dict[str, Any] = {}
    for section_name, cls in _SECTION_CLASSES.items():
        section_data = data.get(section_name, {})
        if isinstance(section_data, dict):
            valid_fields = {f.name for f in fields(cls)}
            filtered = {k: v for k, v in section_data.items() if k in valid_fields}
            kwargs[section_name] = cls(**filtered)
        elif isinstance(section_data, cls):
            kwargs[section_name] = section_data
    return Settings(**kwargs)


def _settings_to_defaults() -> dict[str, Any]:
    """从 dataclass 默认值构造字典。"""
    defaults: dict[str, Any] = {}
    default_settings = Settings()
    for section_name, cls in _SECTION_CLASSES.items():
        section_obj = getattr(default_settings, section_name)
        section_dict: dict[str, Any] = {}
        for f in fields(cls):
            section_dict[f.name] = getattr(section_obj, f.name)
        defaults[section_name] = section_dict
    return defaults


# ── 全局单例 ─────────────────────────────────────────────────

_settings: Settings | None = None
_initialized: bool = False


def init_settings(
    cli_args: dict[str, Any] | None = None,
    config_path: str | None = None,
) -> Settings:
    """显式初始化全局配置单例。

    执行四层合并：defaults → env → YAML → CLI。
    应在 server.main() 中 argparse 之后调用。
    """
    global _settings, _initialized

    data = _settings_to_defaults()

    env_overrides = _load_env_overrides()
    if env_overrides:
        data = _deep_merge(data, env_overrides)

    yaml_path = _find_config_file(config_path)
    if yaml_path is not None:
        yaml_data = _load_yaml_config(yaml_path)
        data = _deep_merge(data, yaml_data)

    data = _apply_cli_overrides(data, cli_args)

    _settings = _dict_to_settings(data)
    _initialized = True
    return _settings


def get_settings() -> Settings:
    """获取全局配置单例。

    若未通过 init_settings() 初始化，则 fallback 到默认值 + 环境变量 + YAML。
    """
    global _settings
    if _settings is not None:
        return _settings

    data = _settings_to_defaults()
    env_overrides = _load_env_overrides()
    if env_overrides:
        data = _deep_merge(data, env_overrides)
    yaml_path = _find_config_file()
    if yaml_path is not None:
        yaml_data = _load_yaml_config(yaml_path)
        data = _deep_merge(data, yaml_data)
    _settings = _dict_to_settings(data)
    return _settings


def _reset_settings() -> None:
    """重置全局单例（仅供测试使用）。"""
    global _settings, _initialized
    _settings = None
    _initialized = False


# ── 示例配置生成 ─────────────────────────────────────────────

_SAMPLE_CONFIG = """\
# CompanyAnalysis MCP Server 配置文件
# 配置优先级: CLI 参数 > 本文件 > 环境变量 > 内置默认值

database:
  # ClickHouse 连接（对齐 ts2ck 命名风格）
  ch_host: "localhost"
  ch_port: 8123
  ch_database: "tushare_data"
  ch_user: "default"
  # ch_password: ""  # 推荐通过环境变量 COMPANY_ANALYSIS_MCP_DATABASE_CH_PASSWORD 设置
  # 逻辑表名→物理表名映射（JSON 字符串）
  # table_map: '{"fin_income": "fin_income_v2"}'

server:
  # MCP transport 类型: streamable-http 或 stdio
  transport: "streamable-http"
  # HTTP 监听地址
  host: "0.0.0.0"
  # HTTP 监听端口
  port: 8001

analysis:
  # 默认回看年数
  default_lookback_years: 3
  # 并发执行最大线程数
  max_workers: 4
  # 单个 tool 超时（秒）
  per_tool_timeout: 120
"""


def generate_sample_config() -> str:
    """返回带注释的示例 YAML 配置文件内容。"""
    return _SAMPLE_CONFIG

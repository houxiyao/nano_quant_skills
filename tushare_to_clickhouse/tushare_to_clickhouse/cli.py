"""CLI entry point for tushare-to-clickhouse using Click."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from tushare_to_clickhouse.clickhouse_client import ClickHouseManager
from tushare_to_clickhouse.config import DEFAULT_CONFIG_PATH, SyncConfig
from tushare_to_clickhouse.quality_checker import QualityChecker
from tushare_to_clickhouse.registry import SyncRegistry
from tushare_to_clickhouse.sync_engine import SyncEngine


def _setup_logging(config: SyncConfig) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=config.log_level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )
    if config.log_file:
        log_path = Path(config.log_file).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(log_path),
            level="DEBUG",
            rotation="00:00",
            retention=f"{config.log_retention_days} days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
        )


def _load_config(config_path: Optional[str]) -> SyncConfig:
    cfg = SyncConfig.from_yaml(config_path)
    _setup_logging(cfg)
    return cfg


@click.group()
@click.option("--config", default=None, help="配置文件路径 (默认: ./config.yaml)")
@click.pass_context
def cli(ctx, config):
    """Tushare 数据同步到 ClickHouse"""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


@cli.command()
@click.option("--force", is_flag=True, help="覆盖已有配置")
@click.pass_context
def init(ctx, force):
    """创建示例配置文件"""
    config_path = Path(ctx.obj.get("config_path") or DEFAULT_CONFIG_PATH).expanduser().resolve()
    if config_path.exists() and not force:
        click.echo(f"Config already exists at {config_path}. Use --force to overwrite.")
        raise SystemExit(1)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    sample = """# Tushare to ClickHouse configuration
clickhouse:
  host: localhost
  port: 8123
  user: default
  password: ""
  database: tushare_data

tushare:
  token: "your_tushare_token_here"

sync:
  default_start_date: "20100101"
  default_start_period: "20100331"
  publish_cutoff_hour: 18
  default_sleep: 0.3
  max_retries: 3
  batch_size: 10000

logging:
  level: INFO
  file: "~/.config/tushare_to_clickhouse/sync.log"
  retention_days: 30
"""
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(sample)
    click.echo(f"Created sample config at {config_path}")
    click.echo("Please edit it and set your tushare.token.")


@cli.command("sync")
@click.option("--endpoint", required=True, help="Tushare 接口名")
@click.option("--target-table", default=None, help="ClickHouse 目标表名")
@click.option("--dimension-type", type=click.Choice(["none", "trade_date", "period"]), default="none")
@click.option("--dimension-field", default=None, help="覆盖维度参数名")
@click.option("--method", default="query", help="Tushare 方法 (query 或方法名)")
@click.option("--mode", type=click.Choice(["overwrite", "append"]), default="overwrite")
@click.option("--start-date", default=None, help="起始日期 YYYYMMDD")
@click.option("--end-date", default=None, help="截止日期 YYYYMMDD")
@click.option("--sync-all", is_flag=True, help="跳过已同步维度")
@click.option("--params", default=None, help="额外 Tushare 参数 (JSON 字符串)")
@click.option("--sleep", type=float, default=None, help="API 调用间隔 (秒)")
@click.option("--max-retries", type=int, default=None)
@click.option("--allow-empty-result", is_flag=True)
@click.option("--publish-cutoff-hour", type=int, default=None, help="安全截止时间 (小时)")
@click.option("--disable-safe-trade-date", is_flag=True)
@click.option("--order-by", default=None, help="ORDER BY 列 (逗号分隔)")
@click.option("--partition-by", default=None, help="PARTITION BY 表达式")
@click.pass_context
def cmd_sync(ctx, endpoint, target_table, dimension_type, dimension_field, method, mode,
             start_date, end_date, sync_all, params, sleep, max_retries,
             allow_empty_result, publish_cutoff_hour, disable_safe_trade_date,
             order_by, partition_by):
    """同步单张表"""
    config = _load_config(ctx.obj.get("config_path"))
    engine = SyncEngine(config)
    try:
        result = engine.sync_table(
            endpoint=endpoint,
            target_table=target_table or endpoint,
            dimension_type=dimension_type,
            dimension_field=dimension_field,
            method=method,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            sync_all=sync_all,
            params=json.loads(params) if params else None,
            sleep=sleep,
            max_retries=max_retries,
            allow_empty_result=allow_empty_result,
            publish_cutoff_hour=publish_cutoff_hour,
            disable_safe_trade_date=disable_safe_trade_date,
            order_by=order_by.split(",") if order_by else None,
            partition_by=partition_by,
        )
        click.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    except Exception as exc:
        logger.error(f"Sync failed: {exc}")
        raise SystemExit(1)


@cli.command("sync-all")
@click.option("--registry", default=None, help="注册表 YAML 路径")
@click.option("--tables", default=None, help="逗号分隔的目标表名")
@click.option("--max-points", type=int, default=5000, help="积分过滤上限")
@click.pass_context
def cmd_sync_all(ctx, registry, tables, max_points):
    """按注册表批量同步"""
    config = _load_config(ctx.obj.get("config_path"))

    registry_path = registry
    if not registry_path:
        default = Path(__file__).resolve().parent.parent / "registry" / "full_sync_registry.yaml"
        if default.exists():
            registry_path = str(default)
        else:
            click.echo("No registry file specified and default not found.")
            raise SystemExit(1)

    engine = SyncEngine(config)
    reg = SyncRegistry.from_yaml(registry_path)
    reg = reg.filter_by_points(max_points)
    if tables:
        reg = reg.filter_by_tables(tables.split(","))

    results = engine.sync_registry(reg)
    ok = sum(1 for r in results if "error" not in r)
    fail = len(results) - ok
    click.echo(f"\nBatch done: {ok} succeeded, {fail} failed out of {len(results)}")
    if fail > 0:
        raise SystemExit(1)


@cli.command("check")
@click.option("--table", required=True, help="表名")
@click.option("--pk", required=True, help="主键列 (逗号分隔)")
@click.option("--date-col", default=None, help="日期列")
@click.option("--format", "fmt", type=click.Choice(["text", "json", "markdown"]), default="text")
@click.pass_context
def cmd_check(ctx, table, pk, date_col, fmt):
    """数据质检"""
    config = _load_config(ctx.obj.get("config_path"))
    ch = ClickHouseManager(config)
    checker = QualityChecker(ch)
    report = checker.check_table(table, pk.split(","), date_col)

    if fmt == "json":
        click.echo(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    elif fmt == "markdown":
        click.echo(checker.format_markdown(report))
    else:
        status = "PASSED" if report["passed"] else "FAILED"
        click.echo(f"\nQuality Report: {report['table']} — {status}")
        click.echo(f"Check Time: {report['check_time']}")
        for name, check in report["checks"].items():
            flag = "✓" if check.get("pass", True) else "✗"
            click.echo(f"  {flag} {name}: {check['value']}")


@cli.command("status")
@click.option("--source-table", default=None, help="按源表名过滤")
@click.pass_context
def cmd_status(ctx, source_table):
    """查看同步状态"""
    config = _load_config(ctx.obj.get("config_path"))
    ch = ClickHouseManager(config)
    ch.ensure_sync_state_table()
    client = ch.client

    if source_table:
        result = client.query(
            f'SELECT dimension_type, dimension_value, is_sync, error_message, updated_at '
            f'FROM "table_sync_state" FINAL '
            f'WHERE source_table = {{src:String}} '
            f'ORDER BY updated_at DESC LIMIT 20',
            parameters={"src": source_table},
        )
    else:
        result = client.query(
            f'SELECT source_table, dimension_type, dimension_value, is_sync, updated_at '
            f'FROM "table_sync_state" FINAL '
            f'ORDER BY updated_at DESC LIMIT 50',
        )

    headers = ["source_table", "dimension_type", "dimension_value", "is_sync", "updated_at"]
    if source_table:
        headers = ["dimension_type", "dimension_value", "is_sync", "error_message", "updated_at"]

    click.echo(" | ".join(headers))
    click.echo("-" * 80)
    for row in result.result_rows:
        click.echo(" | ".join(str(c) for c in row))


def main():
    cli()

"""MCP Server 创建与生命周期管理。"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP

from .config import generate_sample_config, get_settings, init_settings

_MCP_INSTRUCTIONS = """\
# CompanyAnalysis MCP 工具集

本服务提供 A 股上市公司的"七看八问"分析工具。

## 七看工具（定量财务分析，ClickHouse）
- look_01_profit_quality: 盈收与利润质量
- look_02_cost_structure: 费用成本结构
- look_03_growth_trend: 增长率趋势
- look_04_business_distribution: 业务构成与市场分布
- look_05_balance_sheet_health: 资产负债健康度
- look_06_efficiency: 投入产出效率
- look_07_roe_capital_return: 收益率与资本回报

## 八问工具（定性证据分析，ClickHouse + nano-search-mcp）
- ask_q1_industry_prospect ~ ask_q8_future_plan

## 编排工具
- run_seven_looks: 并发执行七看 + 红旗提取 + 质量评分
- run_eight_questions: 并发执行八问 + 汇总评级

## 通用约定
- 所有 tool 不抛异常，通过 status 字段表达失败
- 金融类公司（银行/保险/证券）look 工具返回 status=not-applicable
- 数据库连接由服务端配置，无需 tool 参数传入
- 返回值均为 JSON 结构，不含 Markdown 渲染
"""

# ── 全局单例 ─────────────────────────────────────────────────

_mcp_instance: FastMCP | None = None


def _create_mcp() -> FastMCP:
    """创建 FastMCP 实例并注册所有工具。"""
    instance = FastMCP(
        name="CompanyAnalysis",
        streamable_http_path="/mcp",
        instructions=_MCP_INSTRUCTIONS,
    )

    # Phase 3: 七看工具注册
    from .tools import register_all_tools

    register_all_tools(instance)

    return instance


def get_mcp() -> FastMCP:
    """获取 MCP 实例（延迟创建）。"""
    global _mcp_instance
    if _mcp_instance is None:
        _mcp_instance = _create_mcp()
    return _mcp_instance


def __getattr__(name: str) -> Any:
    if name == "mcp":
        return get_mcp()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ── CLI 入口 ─────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CompanyAnalysis MCP Server — A 股七看八问分析服务"
    )
    parser.add_argument(
        "--transport",
        choices=("streamable-http", "stdio"),
        default=None,
        help="MCP transport 类型（默认 streamable-http）",
    )
    parser.add_argument("--host", default=None, help="HTTP 监听地址")
    parser.add_argument("--port", type=int, default=None, help="HTTP 监听端口")
    parser.add_argument("--ch-host", default=None, help="ClickHouse 主机地址")
    parser.add_argument("--ch-port", type=int, default=None, help="ClickHouse HTTP 端口")
    parser.add_argument("--config", default=None, help="YAML 配置文件路径")
    parser.add_argument(
        "--generate-config",
        action="store_true",
        help="输出示例配置文件到 stdout",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """启动 MCP Server。"""
    args = _build_parser().parse_args(list(argv) if argv is not None else None)

    if args.generate_config:
        sys.stdout.write(generate_sample_config())
        return

    cli_args = {
        "transport": args.transport,
        "host": args.host,
        "port": args.port,
        "ch_host": args.ch_host,
        "ch_port": args.ch_port,
    }
    cfg = init_settings(cli_args=cli_args, config_path=args.config)

    mcp_instance = get_mcp()
    mcp_instance.run(transport=cfg.server.transport)

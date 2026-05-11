"""tools 包 — MCP 工具注册。"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP


def register_all_tools(mcp: FastMCP) -> None:
    """注册所有 MCP 工具到 FastMCP 实例。"""
    from .look_01 import register_look_01_tools
    from .look_02 import register_look_02_tools
    from .look_03 import register_look_03_tools
    from .look_04 import register_look_04_tools
    from .look_05 import register_look_05_tools
    from .look_06 import register_look_06_tools
    from .look_07 import register_look_07_tools

    register_look_01_tools(mcp)
    register_look_02_tools(mcp)
    register_look_03_tools(mcp)
    register_look_04_tools(mcp)
    register_look_05_tools(mcp)
    register_look_06_tools(mcp)
    register_look_07_tools(mcp)

    from .ask_q1 import register_ask_q1_tools
    from .ask_q2 import register_ask_q2_tools
    from .ask_q3 import register_ask_q3_tools
    from .ask_q4 import register_ask_q4_tools
    from .ask_q5 import register_ask_q5_tools
    from .ask_q6 import register_ask_q6_tools
    from .ask_q7 import register_ask_q7_tools
    from .ask_q8 import register_ask_q8_tools

    register_ask_q1_tools(mcp)
    register_ask_q2_tools(mcp)
    register_ask_q3_tools(mcp)
    register_ask_q4_tools(mcp)
    register_ask_q5_tools(mcp)
    register_ask_q6_tools(mcp)
    register_ask_q7_tools(mcp)
    register_ask_q8_tools(mcp)

    from .orchestrate_seven_looks import register_orchestrate_seven_looks_tools
    from .orchestrate_eight_questions import register_orchestrate_eight_questions_tools

    register_orchestrate_seven_looks_tools(mcp)
    register_orchestrate_eight_questions_tools(mcp)

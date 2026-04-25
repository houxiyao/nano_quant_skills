"""tests/test_search.py — 通用搜索工具单元测试。"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from mcp.server.fastmcp import FastMCP

from nano_search_mcp.tools.search import register_search_tools


def _get_tool_fn(tool_name: str) -> Any:
    mcp = FastMCP("test-search")
    register_search_tools(mcp)
    for t in mcp._tool_manager.list_tools():  # type: ignore[attr-defined]
        if t.name == tool_name:
            return t.fn
    raise AssertionError(f"tool not found: {tool_name}")


def test_general_search_success_with_query_composition() -> None:
    tool_fn = _get_tool_fn("general_search")

    with patch("nano_search_mcp.tools.search._search_via_bailian") as mock_search:
        mock_search.return_value = [
            {
                "title": "行业政策",
                "url": "https://www.gov.cn/policy",
                "snippet": "政策摘要",
            }
        ]

        result = tool_fn(
            query="新能源 政策",
            max_results=8,
            region="zh-cn",
            timelimit="m",
            site="gov.cn",
            include_terms=["发改委", "产业"],
            exclude_terms=["广告"],
        )

        assert result["source"] == "bailian_web_search"
        assert len(result["results"]) == 1
        assert "fetch_time" in result
        assert "site:gov.cn" in result["query"]
        assert '"发改委"' in result["query"]
        assert '"产业"' in result["query"]
        assert "-广告" in result["query"]
        assert "过去1个月" in result["query"]

        called = mock_search.call_args
        assert called is not None
        assert called.kwargs["query"] == result["query"]
        assert called.kwargs["max_results"] == 8
        assert called.kwargs["region"] == ""
        assert called.kwargs["timelimit"] is None


def test_general_search_empty_query_returns_unavailable() -> None:
    tool_fn = _get_tool_fn("general_search")

    result = tool_fn(query="   ")

    assert result["source"] == "unavailable"
    assert result["results"] == []
    assert "不能为空" in result["error"]


def test_general_search_failure_returns_unavailable() -> None:
    tool_fn = _get_tool_fn("general_search")

    with patch("nano_search_mcp.tools.search._search_via_bailian") as mock_search:
        mock_search.side_effect = RuntimeError("upstream timeout")

        result = tool_fn(query="宏观经济")

        assert result["source"] == "unavailable"
        assert result["results"] == []
        assert "upstream timeout" in result["error"]


    def test_general_search_keeps_arbitrary_non_finance_queries_unchanged() -> None:
        tool_fn = _get_tool_fn("general_search")

        with patch("nano_search_mcp.tools.search._search_via_bailian") as mock_search:
            mock_search.return_value = []

            result = tool_fn(query="海豚的颜色")

            assert result["source"] == "bailian_web_search"
            assert result["query"] == "海豚的颜色"

            called = mock_search.call_args
            assert called is not None
            assert called.kwargs["query"] == "海豚的颜色"

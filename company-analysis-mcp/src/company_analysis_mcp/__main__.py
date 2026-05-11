"""company-analysis-mcp CLI 入口。"""

from __future__ import annotations

import sys


def main() -> None:
    """CLI 入口，转发到 server.main()。"""
    from .server import main as _server_main

    _server_main(sys.argv[1:])


if __name__ == "__main__":
    main()

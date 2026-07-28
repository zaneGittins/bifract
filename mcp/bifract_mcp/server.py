"""Entry point for the Bifract MCP server."""

import sys

from . import http, tools  # noqa: F401  (importing tools registers them)
from .app import mcp


def main() -> None:
    config = http.config()
    if config.error:
        print(f"Error: {config.error}", file=sys.stderr)
        sys.exit(1)
    mcp.run()


if __name__ == "__main__":
    main()

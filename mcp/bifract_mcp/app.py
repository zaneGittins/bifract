"""The MCP server instance and the decorator every tool is registered with."""

import functools
import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from .http import BifractError

INSTRUCTIONS = """\
You are connected to a Bifract log management and detection platform. The API key
determines which fractal (tenant) you can see; every tool is scoped to it.

Orientation, in order:
  1. get_context     - which fractal, what role, server version.
  2. get_fields      - the field names available in queries.
  3. get_bql_reference - BQL functions and operators.

Then query with query_logs. Field values live under `fields`, but BQL refers to them
by bare name (host=web-01, not fields.host). Use validate_bql on a complex pipeline
before running it; it costs nothing and catches syntax errors in one round trip.

For endpoint/EDR data, find_processes locates a process_guid and provenance_graph
expands it into a scored process tree. Record findings with add_comment (tag related
comments IR-<Name>) or collect them in a notebook.

Time ranges are RFC3339 and default to the last 24 hours server-side. Log volume can
reach billions of rows, so filter and bound the range before widening a search."""

mcp = FastMCP("bifract", instructions=INSTRUCTIONS)


def as_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def tool(fn):
    """Register an async function as an MCP tool.

    Turns a BifractError into the string the model sees, so a misconfigured server or
    a rejected request reads as an explanation rather than a traceback.
    """

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs) -> str:
        try:
            return await fn(*args, **kwargs)
        except BifractError as exc:
            return f"Error: {exc}"

    return mcp.tool()(wrapper)

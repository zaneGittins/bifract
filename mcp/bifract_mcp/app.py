"""The MCP server instance and the decorator every tool is registered with."""

import functools
import json
from typing import Any

from mcp.server.mcpserver import MCPServer

from .http import BifractError

INSTRUCTIONS = """\
You are connected to Bifract, a log management and detection platform. The API key
fixes which fractal (an isolated container of logs, alerts, comments and watchlists)
you can see, and the role that governs what you may change.

Start here:
  get_context        which fractal, what role, which server.
  get_fields         the field names this fractal's logs actually carry.
  get_bql_reference  BQL's commands and operators.

Querying. query_logs runs BQL against hot storage. Field values live under `fields`
but BQL names them bare: host=web-01, not fields.host. Run validate_bql on anything
non-trivial first; it costs one round trip and catches syntax errors before a scan.
Volume reaches billions of rows, so filter and bound the time range before widening.
Times are RFC3339 and default to the last 24 hours.

Investigating. find_processes locates a process_guid, and get_provenance_graph
expands it into a scored process tree for endpoint data. search_dictionary checks an
indicator against the watchlists already in place. When a hunt reaches past hot
retention, search_archive runs the same BQL over the archive; it is minutes-slow and
returns a job to poll, so reach for it only once query_logs cannot answer.

Detections. list_alerts shows what is already watched and is the best guide to this
fractal's real query patterns. get_attack_coverage and get_attack_gaps say which
ATT&CK techniques are covered and which are worth covering next. Validate a query
before creating an alert on it.

Recording. Findings belong in the product, not only in your reply: add_comment
against the logs that evidence them (tag related comments IR-<Name>), or collect the
narrative in a notebook. This is a collaborative platform and other analysts read
what you leave behind.

Writes are real. Creating an alert, or adding rows to a dictionary, changes what a
live system detects. Confirm scope with get_context before changing anything, and
prefer reading the current state before overwriting it."""

mcp = MCPServer("bifract", instructions=INSTRUCTIONS)


def as_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def summarize(rows: Any, fields: tuple[str, ...]) -> list[dict]:
    """Reduce API rows to the fields worth spending the model's context on.

    A dashboard or model row carries timestamps, internal ids and full definitions
    that a list view never needs. Empty values are dropped; a zero is kept, since a
    row count of 0 is an answer.
    """
    if not isinstance(rows, list):
        return []
    return [
        {k: r.get(k) for k in fields if r.get(k) not in (None, "")}
        for r in rows
        if isinstance(r, dict)
    ]


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

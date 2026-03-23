"""Bifract MCP Server - Connect Claude Code to your Bifract instance."""

import json
import os
import sys

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "bifract",
    instructions=(
        "You are connected to a Bifract log management instance. "
        "Use the provided tools to query logs with BQL, manage alerts, "
        "manage notebooks, and investigate security events. Start by calling "
        "get_bql_reference and get_fields to understand the available query "
        "syntax and log fields."
    ),
)

BIFRACT_URL = os.environ.get("BIFRACT_URL", "").rstrip("/")
BIFRACT_API_KEY = os.environ.get("BIFRACT_API_KEY", "")


def _check_config() -> str | None:
    """Return an error string if configuration is missing, else None."""
    if not BIFRACT_URL:
        return "BIFRACT_URL environment variable is not set."
    if not BIFRACT_API_KEY:
        return "BIFRACT_API_KEY environment variable is not set."
    return None


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {BIFRACT_API_KEY}",
        "Content-Type": "application/json",
    }


async def _get(path: str, params: dict | None = None) -> dict:
    """Make an authenticated GET request to the Bifract API."""
    async with httpx.AsyncClient(verify=True, timeout=30) as client:
        resp = await client.get(
            f"{BIFRACT_URL}/api/v1{path}",
            headers=_headers(),
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


async def _post(path: str, body: dict) -> dict:
    """Make an authenticated POST request to the Bifract API."""
    async with httpx.AsyncClient(verify=True, timeout=60) as client:
        resp = await client.post(
            f"{BIFRACT_URL}/api/v1{path}",
            headers=_headers(),
            json=body,
        )
        resp.raise_for_status()
        return resp.json()


async def _put(path: str, body: dict) -> dict:
    """Make an authenticated PUT request to the Bifract API."""
    async with httpx.AsyncClient(verify=True, timeout=30) as client:
        resp = await client.put(
            f"{BIFRACT_URL}/api/v1{path}",
            headers=_headers(),
            json=body,
        )
        resp.raise_for_status()
        return resp.json()


async def _delete(path: str) -> dict:
    """Make an authenticated DELETE request to the Bifract API."""
    async with httpx.AsyncClient(verify=True, timeout=30) as client:
        resp = await client.delete(
            f"{BIFRACT_URL}/api/v1{path}",
            headers=_headers(),
        )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Tools: Log Querying
# ---------------------------------------------------------------------------


@mcp.tool()
async def query_logs(
    query: str,
    start: str = "",
    end: str = "",
) -> str:
    """
    Execute a BQL query against the fractal's logs.

    The fractal is determined by the API key. Time range is optional
    (defaults to last 24 hours on the server).

    Args:
        query: BQL query string. Must start with a filter expression.
               Examples: 'level=error | head(10)', '/failed/i | groupby(host)',
               'level=* | count()'.
        start: Optional start time in RFC3339 format (e.g. '2025-01-01T00:00:00Z').
        end: Optional end time in RFC3339 format.

    Returns:
        Query results as JSON including matching log entries or aggregation results.
    """
    if (err := _check_config()):
        return err

    body: dict = {"query": query}
    if start:
        body["start"] = start
    if end:
        body["end"] = end

    try:
        result = await _post("/query", body)
    except httpx.HTTPStatusError as e:
        return f"Query failed ({e.response.status_code}): {e.response.text}"

    if not result.get("success"):
        return f"Query error: {result.get('error', 'Unknown error')}"

    count = result.get("count", 0)
    execution_ms = result.get("execution_ms", 0)
    results = result.get("results", [])
    field_order = result.get("field_order", [])
    is_aggregated = result.get("is_aggregated", False)
    limit_hit = result.get("limit_hit", "")

    summary = f"Found {count} results in {execution_ms}ms"
    if is_aggregated:
        summary += " (aggregated)"
    if limit_hit:
        summary += f" [limit: {limit_hit}]"

    output = {
        "summary": summary,
        "field_order": field_order,
        "results": results,
    }
    return json.dumps(output, indent=2, default=str)


@mcp.tool()
async def get_recent_logs(count: int = 10) -> str:
    """
    Fetch the most recent logs from the fractal. Useful for discovering
    available fields and understanding log structure.

    Args:
        count: Number of recent logs to return (1-100, default 10).

    Returns:
        Recent log entries with all their fields.
    """
    if (err := _check_config()):
        return err

    count = max(1, min(count, 100))
    try:
        result = await _get("/logs/recent", {"count": str(count)})
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def get_bql_reference() -> str:
    """
    Get the BQL (Bifract Query Language) syntax reference.

    Returns all supported functions, operators, and their usage examples.
    Call this to understand how to write BQL queries.

    Returns:
        Complete BQL syntax reference with functions and operators.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get("/query/reference")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools: Alerts
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_alerts(enabled_only: bool = False) -> str:
    """
    List all detection alerts configured in the fractal.

    Args:
        enabled_only: If true, only return enabled alerts.

    Returns:
        List of alerts with their names, BQL queries, type, labels, and status.
    """
    if (err := _check_config()):
        return err

    params = {}
    if enabled_only:
        params["enabled"] = "true"

    try:
        result = await _get("/alerts", params)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def get_alert(alert_id: str) -> str:
    """
    Get details of a specific alert by ID.

    Args:
        alert_id: The alert UUID.

    Returns:
        Full alert details including query, schedule, actions, and execution history.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get(f"/alerts/{alert_id}")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def create_alert(
    name: str,
    query_string: str,
    description: str = "",
    alert_type: str = "match",
    enabled: bool = True,
    labels: list[str] | None = None,
    references: list[str] | None = None,
    throttle_time_seconds: int = 0,
    throttle_field: str = "",
) -> str:
    """
    Create a new detection alert.

    Args:
        name: Alert name (e.g. 'Brute Force Detection').
        query_string: BQL query that triggers the alert (e.g. 'level=error AND source=auth').
        description: Human-readable description of what this alert detects.
        alert_type: Alert type - 'match' (fires on each matching log) or
                    'threshold' (fires when count exceeds threshold).
        enabled: Whether the alert is active immediately.
        labels: Optional list of labels/tags (e.g. ['T1110', 'brute-force']).
        references: Optional list of reference URLs.
        throttle_time_seconds: Minimum seconds between repeated firings (0 = no throttle).
        throttle_field: Field to throttle on (e.g. 'source_ip') - only re-fires
                        when this field's value changes.

    Returns:
        The created alert details.
    """
    if (err := _check_config()):
        return err

    body = {
        "name": name,
        "query_string": query_string,
        "description": description,
        "alert_type": alert_type,
        "enabled": enabled,
        "labels": labels or [],
        "references": references or [],
        "throttle_time_seconds": throttle_time_seconds,
        "throttle_field": throttle_field,
    }

    try:
        result = await _post("/alerts", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def update_alert(
    alert_id: str,
    name: str = "",
    query_string: str = "",
    description: str = "",
    alert_type: str = "",
    enabled: bool | None = None,
    labels: list[str] | None = None,
    throttle_time_seconds: int | None = None,
    throttle_field: str | None = None,
) -> str:
    """
    Update an existing alert. Fetches the current alert first and applies
    only the fields you provide.

    Args:
        alert_id: The alert UUID to update.
        name: New alert name (leave empty to keep current).
        query_string: New BQL query (leave empty to keep current).
        description: New description (leave empty to keep current).
        alert_type: New alert type (leave empty to keep current).
        enabled: Set enabled state (omit to keep current).
        labels: New labels list (omit to keep current).
        throttle_time_seconds: New throttle seconds (omit to keep current).
        throttle_field: New throttle field (omit to keep current).

    Returns:
        The updated alert details.
    """
    if (err := _check_config()):
        return err

    # Fetch current alert to merge with updates
    try:
        current = await _get(f"/alerts/{alert_id}")
    except httpx.HTTPStatusError as e:
        return f"Failed to fetch alert ({e.response.status_code}): {e.response.text}"

    alert = current.get("alert", current)

    body = {
        "name": name or alert.get("name", ""),
        "query_string": query_string or alert.get("query_string", ""),
        "description": description or alert.get("description", ""),
        "alert_type": alert_type or alert.get("alert_type", "match"),
        "enabled": enabled if enabled is not None else alert.get("enabled", True),
        "labels": labels if labels is not None else alert.get("labels", []),
        "references": alert.get("references", []),
        "throttle_time_seconds": (
            throttle_time_seconds
            if throttle_time_seconds is not None
            else alert.get("throttle_time_seconds", 0)
        ),
        "throttle_field": (
            throttle_field
            if throttle_field is not None
            else alert.get("throttle_field", "")
        ),
    }

    try:
        result = await _put(f"/alerts/{alert_id}", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def delete_alert(alert_id: str) -> str:
    """
    Delete an alert by ID.

    Args:
        alert_id: The alert UUID to delete.

    Returns:
        Confirmation of deletion.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _delete(f"/alerts/{alert_id}")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def get_alert_executions(alert_id: str) -> str:
    """
    Get recent execution history for an alert, showing when it fired
    and what it matched.

    Args:
        alert_id: The alert UUID.

    Returns:
        List of recent alert executions with timestamps and match counts.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get(f"/alerts/{alert_id}/executions")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools: Comments
# ---------------------------------------------------------------------------


@mcp.tool()
async def add_comment(
    log_id: str,
    text: str,
    tags: list[str] | None = None,
) -> str:
    """
    Add a comment to a specific log entry. Comments enable collaboration
    by letting analysts annotate logs with findings, context, or notes.

    Args:
        log_id: The log_id of the log entry to comment on.
        text: The comment text (supports markdown).
        tags: Optional list of tags to attach to the comment.

    Returns:
        The created comment.
    """
    if (err := _check_config()):
        return err

    body: dict = {
        "log_id": log_id,
        "text": text,
    }
    if tags:
        body["tags"] = tags

    try:
        result = await _post("/comments", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def list_comments() -> str:
    """
    List all comments in the fractal, ordered by most recent.

    Returns:
        List of comments with their content, tags, and associated log IDs.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get("/comments/flat")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools: Saved Queries
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_saved_queries() -> str:
    """
    List saved BQL queries. These are queries that users have bookmarked
    for reuse. Useful for understanding common query patterns in this fractal.

    Returns:
        List of saved queries with names and BQL strings.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get("/saved-queries")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools: Notebooks
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_notebooks(limit: int = 20, offset: int = 0) -> str:
    """
    List notebooks in the fractal with pagination.

    Args:
        limit: Max notebooks to return (1-100, default 20).
        offset: Pagination offset (default 0).

    Returns:
        Paginated list of notebooks with their metadata.
    """
    if (err := _check_config()):
        return err

    params = {"limit": str(max(1, min(limit, 100))), "offset": str(max(0, offset))}

    try:
        result = await _get("/notebooks", params)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def get_notebook(notebook_id: str) -> str:
    """
    Get a notebook by ID, including all its sections.

    Args:
        notebook_id: The notebook UUID.

    Returns:
        Full notebook details with all sections.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get(f"/notebooks/{notebook_id}")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def create_notebook(
    name: str,
    time_range_type: str = "24h",
    description: str = "",
    max_results_per_section: int = 1000,
) -> str:
    """
    Create a new notebook in the fractal.

    Args:
        name: Notebook name (e.g. 'Incident Investigation 2025-03-22').
        time_range_type: Time range for queries in the notebook.
                         One of: '1h', '24h', '7d', '30d', 'all', 'custom'.
                         Default '24h'.
        description: Optional description of the notebook's purpose.
        max_results_per_section: Max query results per section (default 1000).

    Returns:
        The created notebook details including its ID.
    """
    if (err := _check_config()):
        return err

    body: dict = {
        "name": name,
        "description": description,
        "time_range_type": time_range_type,
        "max_results_per_section": max_results_per_section,
    }

    try:
        result = await _post("/notebooks", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def add_notebook_section(
    notebook_id: str,
    section_type: str,
    content: str,
    title: str = "",
    order_index: int = -1,
) -> str:
    """
    Add a new section to an existing notebook.

    Args:
        notebook_id: The notebook UUID to add the section to.
        section_type: Type of section - 'markdown' for text/notes or 'query' for
                      a BQL query that can be executed within the notebook.
        content: Section content. For 'markdown' sections this is the markdown text.
                 For 'query' sections this is the BQL query string.
        title: Optional section title.
        order_index: Position in the notebook (0-based). Use -1 to append at the end.

    Returns:
        The created section details including its ID.
    """
    if (err := _check_config()):
        return err

    # If appending, fetch existing sections to determine next index
    if order_index < 0:
        try:
            nb = await _get(f"/notebooks/{notebook_id}")
            notebook = nb.get("notebook", nb)
            sections = notebook.get("sections", [])
            order_index = len(sections)
        except httpx.HTTPStatusError:
            order_index = 0

    body: dict = {
        "section_type": section_type,
        "content": content,
        "order_index": order_index,
    }
    if title:
        body["title"] = title

    try:
        result = await _post(f"/notebooks/{notebook_id}/sections", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools: AI Instructions
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_instructions() -> str:
    """
    List all AI instructions configured for this fractal.

    AI instructions are reusable system prompts that guide the Bifract AI chat.
    They can be collaboratively created and refined. One instruction can be
    marked as the default for the fractal.

    Returns:
        List of instructions with their names, content, and default status.
    """
    if (err := _check_config()):
        return err

    try:
        result = await _get("/chat/instructions")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def get_instruction(instruction_id: str) -> str:
    """
    Get a specific AI instruction by ID to view its full content.

    Args:
        instruction_id: The instruction UUID.

    Returns:
        Full instruction details including name, content, and default status.
    """
    if (err := _check_config()):
        return err

    # The API doesn't have a dedicated GET by ID endpoint,
    # so list all and filter client-side.
    try:
        result = await _get("/chat/instructions")
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    instructions = result.get("instructions", [])
    for inst in instructions:
        if inst.get("id") == instruction_id:
            return json.dumps(inst, indent=2, default=str)

    return f"Instruction {instruction_id} not found."


@mcp.tool()
async def create_instruction(
    name: str,
    content: str,
    is_default: bool = False,
) -> str:
    """
    Create a new AI instruction for the fractal.

    AI instructions are system prompts that guide the Bifract AI chat assistant.
    Use them to customize how the AI responds for this fractal's domain
    (e.g. security-focused analysis, compliance checks, incident response).

    Args:
        name: Instruction name (e.g. 'Security Analyst', 'Compliance Reviewer').
        content: The instruction content / system prompt text. This will be
                 injected into the AI chat's context to guide its responses.
        is_default: If true, this becomes the default instruction for the fractal.
                    Only one instruction can be default at a time.

    Returns:
        The created instruction details.
    """
    if (err := _check_config()):
        return err

    body = {
        "name": name,
        "content": content,
        "is_default": is_default,
    }

    try:
        result = await _post("/chat/instructions", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def update_instruction(
    instruction_id: str,
    name: str,
    content: str,
    is_default: bool = False,
) -> str:
    """
    Update an existing AI instruction.

    Use this to refine and improve instructions over time based on how well
    they guide the AI chat assistant.

    Args:
        instruction_id: The instruction UUID to update.
        name: Updated instruction name.
        content: Updated instruction content / system prompt text.
        is_default: Whether this should be the default instruction for the fractal.

    Returns:
        The updated instruction details.
    """
    if (err := _check_config()):
        return err

    body = {
        "name": name,
        "content": content,
        "is_default": is_default,
    }

    try:
        result = await _put(f"/chat/instructions/{instruction_id}", body)
    except httpx.HTTPStatusError as e:
        return f"Failed ({e.response.status_code}): {e.response.text}"

    return json.dumps(result, indent=2, default=str)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    if (err := _check_config()):
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
    mcp.run()


if __name__ == "__main__":
    main()

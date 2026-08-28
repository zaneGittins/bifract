"""Tools for running BQL and discovering what is queryable."""

from .. import http
from ..app import as_json, tool
from ..bql import time_window


@tool
async def get_context() -> str:
    """
    Report which Bifract instance and fractal this session is bound to.

    Call this first in a new conversation. The API key fixes the fractal, so nothing
    here is selectable; it tells you the scope your queries will run in and the role
    that governs what you may write.

    Returns:
        Instance URL, server version, fractal ID, and effective role.
    """
    identity = await http.get("/auth/user")
    user = identity.get("user", identity)
    version = await http.get("/version")

    return as_json(
        {
            "url": http.config().url,
            "server_version": version.get("version", "unknown"),
            "identity": user.get("display_name") or user.get("username", ""),
            "fractal_id": user.get("selected_fractal", ""),
            "prism_id": user.get("selected_prism", ""),
            "role": user.get("fractal_role") or user.get("prism_role") or "none",
            "note": (
                "Queries default to the last 24 hours unless start/end are given. "
                "'viewer' can read only; 'analyst' can also write comments, alerts, "
                "notebooks, and dashboards."
            ),
        }
    )


@tool
async def query_logs(query: str, start: str = "", end: str = "") -> str:
    """
    Execute a BQL query against the fractal's logs.

    The fractal comes from the API key. Prefer a bounded time range: the backing store
    holds up to billions of rows and an unbounded scan is expensive.

    Args:
        query: BQL query string, starting with a filter expression.
               Examples: 'level=error | head(10)', 'image=~powershell | groupby(computer_name)',
               'bifract_category="process_creation" | count()'.
        start: Optional start time, RFC3339 (e.g. '2025-01-01T00:00:00Z').
        end: Optional end time, RFC3339.

    Returns:
        Matching rows or aggregation results, with the field order and row count.
    """
    body = {"query": query, **time_window(start, end)}
    result = await http.post("/query", body)

    summary = f"Found {result.get('count', 0)} results in {result.get('execution_ms', 0)}ms"
    if result.get("is_aggregated"):
        summary += " (aggregated)"
    if limit_hit := result.get("limit_hit"):
        summary += f" [limit: {limit_hit}]"

    return as_json(
        {
            "summary": summary,
            "field_order": result.get("field_order", []),
            "results": result.get("results", []),
        }
    )


@tool
async def validate_bql(query: str) -> str:
    """
    Check a BQL query for syntax and translation errors without running it.

    Costs nothing (no database work). Use it on a long pipeline before query_logs so a
    typo does not cost a full query round trip.

    Args:
        query: The BQL query string to check.

    Returns:
        Whether the query is valid, and the parse or translation error if not.
    """
    return as_json(await http.post("/query/validate", {"query": query}))


@tool
async def get_fields(filter: str = "") -> str:
    """
    List the field names available to queries in this fractal.

    These are the normalized fields BQL refers to by bare name (host=web-01, not
    fields.host). For what the values actually look like, follow with get_field_stats.

    Args:
        filter: Optional case-insensitive substring to narrow the list (e.g. 'ip', 'process').

    Returns:
        The field names, filtered if a filter was given.
    """
    response = await http.get("/query/fields")
    fields = response.get("fields", []) if isinstance(response, dict) else []
    if needle := filter.strip().lower():
        fields = [f for f in fields if needle in f.lower()]
    return as_json({"count": len(fields), "fields": fields})


@tool
async def get_field_stats(query: str, start: str = "", end: str = "") -> str:
    """
    Profile the fields present in the events a query matches.

    For each field: how many sampled rows carry it, how many distinct values it has,
    and its most frequent values. Use this to learn a field's real value shape before
    writing an equality filter, and to spot which fields are worth grouping on.

    Runs over a bounded sample at low database priority, so it does not slow searches.
    Not supported for source-command queries such as pgr().

    Args:
        query: The BQL filter to profile. Only the filter portion is used.
        start: Optional start time, RFC3339.
        end: Optional end time, RFC3339.

    Returns:
        Per-field coverage, cardinality, and top values over the sample.
    """
    body = {"query": query, **time_window(start, end)}
    result = await http.post("/query/fieldstats", body)

    if not result.get("supported", True):
        return "Field stats are not available for this query shape (source commands like pgr())."

    return as_json(
        {
            "sample_size": result.get("sample_size", 0),
            "approximate": result.get("approximate", False),
            "fields": result.get("fields", []),
        }
    )


@tool
async def get_recent_logs(count: int = 10) -> str:
    """
    Fetch the most recent logs in the fractal.

    Useful for seeing the real shape of ingested events, and for checking how fresh
    the data is before choosing a time range.

    Args:
        count: Number of logs to return (1-100, default 10).

    Returns:
        Recent log entries with all their fields.
    """
    count = max(1, min(count, 100))
    return as_json(await http.get("/logs/recent", {"count": str(count)}))


@tool
async def get_bql_reference() -> str:
    """
    Get the BQL syntax reference: every supported function and operator with examples.

    Returns:
        The full BQL function and operator reference.
    """
    return as_json(await http.get("/query/reference"))

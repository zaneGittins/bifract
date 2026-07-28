"""Tools for dashboards. API keys need the dashboard permission."""

from .. import http
from ..app import as_json, tool

SUMMARY_FIELDS = (
    "id",
    "name",
    "description",
    "time_range_type",
    "refresh_interval",
    "created_by",
    "updated_at",
)


@tool
async def list_dashboards() -> str:
    """
    List the dashboards in this fractal.

    Returns a summary per dashboard; use get_dashboard for a specific one's widgets
    and their queries.

    Returns:
        Dashboard IDs, names, descriptions, and time ranges.
    """
    response = await http.get("/dashboards")
    dashboards = response.get("data", response)
    if not isinstance(dashboards, list):
        return as_json(response)

    summaries = [{k: d.get(k) for k in SUMMARY_FIELDS if k in d} for d in dashboards]
    return as_json({"count": len(summaries), "dashboards": summaries})


@tool
async def get_dashboard(dashboard_id: str) -> str:
    """
    Read a dashboard with all of its widgets.

    Each widget carries the BQL query behind it, which makes a dashboard a useful
    source of vetted queries for this fractal.

    Args:
        dashboard_id: The dashboard UUID.

    Returns:
        The dashboard, its variables, and every widget with its query and chart config.
    """
    return as_json(await http.get(f"/dashboards/{dashboard_id}"))

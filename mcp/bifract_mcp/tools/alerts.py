"""Tools for detection alerts. API keys need the alert_manage permission."""

from .. import http
from ..app import as_json, tool


@tool
async def list_alerts(enabled_only: bool = False) -> str:
    """
    List the detection alerts configured in this fractal.

    Also a good way to learn the fractal's real query patterns and which fields
    existing detections rely on.

    Args:
        enabled_only: Return only alerts that are currently active.

    Returns:
        Alerts with their names, BQL queries, type, labels, and status.
    """
    params = {"enabled": "true"} if enabled_only else None
    return as_json(await http.get("/alerts", params))


@tool
async def get_alert(alert_id: str) -> str:
    """
    Get one alert in full.

    Args:
        alert_id: The alert UUID.

    Returns:
        The alert's query, schedule, actions, and execution history.
    """
    return as_json(await http.get(f"/alerts/{alert_id}"))


@tool
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
    Create a detection alert.

    Alerts run on a background interval against newly ingested logs, tracking a cursor
    so nothing is missed across restarts. Validate the query with validate_bql first.

    Args:
        name: Alert name (e.g. 'Brute Force Detection').
        query_string: The BQL query that triggers the alert.
        description: What this alert detects and why it matters.
        alert_type: 'match' fires on each matching log; 'threshold' fires when the
                    count exceeds a threshold.
        enabled: Whether the alert starts active.
        labels: Tags such as ['T1110', 'brute-force'].
        references: Reference URLs.
        throttle_time_seconds: Minimum seconds between repeat firings (0 = none).
        throttle_field: Re-fire only when this field's value changes (e.g. 'src_ip').

    Returns:
        The created alert.
    """
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
    return as_json(await http.post("/alerts", body))


@tool
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
    Update an alert. Only the fields you supply change; the rest keep their values.

    Args:
        alert_id: The alert UUID.
        name: New name (empty keeps the current one).
        query_string: New BQL query (empty keeps the current one).
        description: New description (empty keeps the current one).
        alert_type: New type (empty keeps the current one).
        enabled: New enabled state (omit to keep).
        labels: New label list (omit to keep).
        throttle_time_seconds: New throttle window (omit to keep).
        throttle_field: New throttle field (omit to keep).

    Returns:
        The updated alert.
    """
    current = await http.get(f"/alerts/{alert_id}")
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
            throttle_field if throttle_field is not None else alert.get("throttle_field", "")
        ),
    }
    return as_json(await http.put(f"/alerts/{alert_id}", body))


@tool
async def delete_alert(alert_id: str) -> str:
    """
    Delete an alert.

    Args:
        alert_id: The alert UUID.

    Returns:
        Confirmation of the deletion.
    """
    return as_json(await http.delete(f"/alerts/{alert_id}"))


@tool
async def get_alert_executions(alert_id: str) -> str:
    """
    Show when an alert fired and what it matched.

    Use this to judge whether a detection is noisy before tuning it.

    Args:
        alert_id: The alert UUID.

    Returns:
        Recent executions with timestamps and match counts.
    """
    return as_json(await http.get(f"/alerts/{alert_id}/executions"))

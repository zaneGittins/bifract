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
    alert_type: str = "event",
    severity: str = "medium",
    enabled: bool = True,
    labels: list[str] | None = None,
    references: list[str] | None = None,
    throttle_time_seconds: int = 0,
    throttle_field: str = "",
    schedule_cron: str = "",
    query_window_seconds: int = 0,
    window_duration: int = 0,
) -> str:
    """
    Create a detection alert.

    Alerts run on a background interval against newly ingested logs, tracking a cursor
    so nothing is missed across restarts. Validate the query with validate_bql first.

    Each type has a requirement the others do not, and a request that misses it is
    rejected rather than stored:
      event      the query must not aggregate. Use a plain filter.
      scheduled  needs schedule_cron and query_window_seconds.
      compound   needs window_duration.

    Args:
        name: Alert name (e.g. 'Brute Force Detection').
        query_string: The BQL query that triggers the alert.
        description: What this alert detects and why it matters.
        alert_type: 'event' evaluates each newly ingested log as it arrives (the usual
                    choice); 'scheduled' re-runs the query on a cron; 'compound'
                    correlates several conditions over a window.
        severity: 'critical', 'high', 'medium', 'low' or 'informational'.
        enabled: Whether the alert starts active.
        labels: Tags such as ['T1110', 'brute-force'].
        references: Reference URLs.
        throttle_time_seconds: Minimum seconds between repeat firings (0 = none).
        throttle_field: Re-fire only when this field's value changes (e.g. 'src_ip').
        schedule_cron: Five-field cron for a scheduled alert (e.g. '*/15 * * * *').
        query_window_seconds: How far back a scheduled run looks, in seconds.
        window_duration: Correlation window for a compound alert, in seconds.

    Returns:
        The created alert.
    """
    body = {
        "name": name,
        "query_string": query_string,
        "description": description,
        "alert_type": alert_type,
        "severity": severity,
        "enabled": enabled,
        "labels": labels or [],
        "references": references or [],
        "throttle_time_seconds": throttle_time_seconds,
        "throttle_field": throttle_field,
    }
    # Sent only when set: the API reads their absence as "not applicable to this
    # type", and a zero would fail the positive-value check instead.
    if schedule_cron.strip():
        body["schedule_cron"] = schedule_cron.strip()
    if query_window_seconds > 0:
        body["query_window_seconds"] = query_window_seconds
    if window_duration > 0:
        body["window_duration"] = window_duration
    return as_json(await http.post("/alerts", body))


# The update endpoint replaces the alert rather than patching it, and fills an
# absent field with its default: an omitted severity silently became 'medium', and
# an omitted schedule_cron made a scheduled alert unsaveable. So every settable
# field is read back and resent.
CARRIED = (
    "name",
    "query_string",
    "description",
    "alert_type",
    "severity",
    "enabled",
    "labels",
    "references",
    "throttle_time_seconds",
    "throttle_field",
    "window_duration",
    "schedule_cron",
    "query_window_seconds",
    # Read and written under the same name, unlike the actions below.
    "dictionary_action_ids",
)

# The alert is read with its actions expanded, but written with their ids.
ACTION_FIELDS = {
    "webhook_actions": "webhook_action_ids",
    "fractal_actions": "fractal_action_ids",
    "email_actions": "email_action_ids",
}


def _carry_forward(alert: dict) -> dict:
    """The alert's current state, in the shape the update endpoint accepts."""
    body = {field: alert[field] for field in CARRIED if alert.get(field) is not None}
    for read_as, write_as in ACTION_FIELDS.items():
        actions = alert.get(read_as) or []
        body[write_as] = [a["id"] for a in actions if isinstance(a, dict) and a.get("id")]
    return body


@tool
async def update_alert(
    alert_id: str,
    name: str = "",
    query_string: str = "",
    description: str = "",
    alert_type: str = "",
    severity: str = "",
    enabled: bool | None = None,
    labels: list[str] | None = None,
    throttle_time_seconds: int | None = None,
    throttle_field: str | None = None,
) -> str:
    """
    Update an alert. Only the fields you supply change; the rest keep their values.

    The API replaces the whole alert, so this reads it first and sends the current
    values back for everything you did not name.

    Args:
        alert_id: The alert UUID.
        name: New name (empty keeps the current one).
        query_string: New BQL query (empty keeps the current one).
        description: New description (empty keeps the current one).
        alert_type: New type, 'event', 'scheduled' or 'compound' (empty keeps the current one).
        severity: New severity, 'critical', 'high', 'medium', 'low' or
                  'informational' (empty keeps the current one).
        enabled: New enabled state (omit to keep).
        labels: New label list (omit to keep).
        throttle_time_seconds: New throttle window (omit to keep).
        throttle_field: New throttle field (omit to keep).

    Returns:
        The updated alert.
    """
    current = await http.get(f"/alerts/{alert_id}")
    alert = current.get("alert", current)

    body = _carry_forward(alert)
    changes = {
        "name": name or None,
        "query_string": query_string or None,
        "description": description or None,
        "alert_type": alert_type or None,
        "severity": severity or None,
        "enabled": enabled,
        "labels": labels,
        "throttle_time_seconds": throttle_time_seconds,
        "throttle_field": throttle_field,
    }
    body.update({k: v for k, v in changes.items() if v is not None})
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

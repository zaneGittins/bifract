"""Tools for seeing which fractals and prisms a credential can reach."""

from .. import http
from ..app import as_json, summarize, tool

FRACTAL_FIELDS = ("id", "name", "description", "log_count", "created_at")
PRISM_FIELDS = ("id", "name", "description", "fractal_ids")


@tool
async def list_fractals() -> str:
    """
    List the fractals and prisms this credential can reach.

    A fractal is an isolated container of logs, alerts, comments and dictionaries;
    teams, environments and customers are usually kept in separate ones. A prism is
    a read-only view spanning several fractals, used to query across them at once.

    Most tools act on the single fractal the API key is bound to, which get_context
    reports. Use this to understand what else exists on the instance, to interpret
    a fractal id seen elsewhere, or to confirm the key's scope really is the data
    you were asked about.

    Returns:
        The fractals with their ids, names and row counts, and the prisms with the
        fractals each spans.
    """
    payload = await http.get("/fractals")
    if not isinstance(payload, dict):
        return as_json(payload)

    fractals = summarize(payload.get("fractals"), FRACTAL_FIELDS)
    prisms = summarize(payload.get("prisms"), PRISM_FIELDS)
    return as_json(
        {
            "fractals": fractals,
            "prisms": prisms,
            "count": len(fractals),
            "note": (
                "Reachability is not the same as scope: tools run against the fractal "
                "the API key is bound to, which get_context reports."
            ),
        }
    )

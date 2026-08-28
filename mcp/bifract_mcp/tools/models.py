"""Tools for behavioral models: rolling baselines maintained as logs arrive."""

from .. import http
from ..app import as_json, summarize, tool

SUMMARY_FIELDS = (
    "id",
    "name",
    "description",
    "model_type",
    "status",
    "alert_mode",
    "source_query",
    "backfill_status",
    "error_message",
)


@tool
async def list_models() -> str:
    """
    List the behavioral models defined in this fractal.

    Models are baselines the platform maintains continuously (beaconing intervals,
    long-lived connections, first-seen values, volume norms) rather than point-in-time
    queries. They answer "is this normal for this environment" in a way a single query
    cannot.

    Returns:
        Each model's ID, name, type, status, and the BQL source query that feeds it.
    """
    response = await http.get("/models")
    models = response.get("models", []) if isinstance(response, dict) else []

    summaries = summarize(models, SUMMARY_FIELDS)
    if not summaries:
        return as_json(
            {
                "count": 0,
                "hint": "No models are defined in this fractal. They are created in the UI under Models.",
            }
        )
    return as_json({"count": len(summaries), "models": summaries})


@tool
async def get_model(model_id: str) -> str:
    """
    Get one model's full definition.

    Args:
        model_id: The model UUID.

    Returns:
        The model's type, definition, status, and backfill state.
    """
    return as_json(await http.get(f"/models/{model_id}"))


@tool
async def get_model_data(
    model_id: str,
    search: str = "",
    sort: str = "",
    order: str = "",
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    Read the rows a model has accumulated.

    This is the baseline itself: what the model has observed and how often. Use it to
    check whether an artifact from an investigation is normal for this environment or
    genuinely new.

    Args:
        model_id: The model UUID.
        search: Optional substring filter over the rows.
        sort: Column to sort by (model-specific; see get_model).
        order: 'asc' or 'desc'.
        limit: Max rows (1-500, default 50).
        offset: Pagination offset.

    Returns:
        The model's rows and the total row count.
    """
    params = {"limit": str(max(1, min(limit, 500))), "offset": str(max(0, offset))}
    for key, value in (("search", search), ("sort", sort), ("order", order)):
        if value.strip():
            params[key] = value.strip()

    return as_json(await http.get(f"/models/{model_id}/data", params))

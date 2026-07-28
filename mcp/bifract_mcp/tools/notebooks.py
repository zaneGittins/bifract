"""Tools for investigation notebooks. API keys need the notebook permission."""

from .. import http
from ..app import as_json, tool


@tool
async def list_notebooks(limit: int = 20, offset: int = 0) -> str:
    """
    List the notebooks in this fractal.

    Args:
        limit: Max notebooks to return (1-100, default 20).
        offset: Pagination offset.

    Returns:
        Notebooks with their metadata.
    """
    params = {"limit": str(max(1, min(limit, 100))), "offset": str(max(0, offset))}
    return as_json(await http.get("/notebooks", params))


@tool
async def get_notebook(notebook_id: str) -> str:
    """
    Read a notebook and all of its sections.

    Args:
        notebook_id: The notebook UUID.

    Returns:
        The notebook with every section in order.
    """
    return as_json(await http.get(f"/notebooks/{notebook_id}"))


@tool
async def create_notebook(
    name: str,
    time_range_type: str = "24h",
    description: str = "",
    max_results_per_section: int = 1000,
) -> str:
    """
    Create a notebook.

    Notebooks interleave markdown and live BQL queries, so an investigation stays
    reproducible: a reader can re-run each step rather than trust a pasted result.

    Args:
        name: Notebook name (e.g. 'Incident 2025-03-22 - Suspicious PowerShell').
        time_range_type: Range applied to the notebook's queries. One of
                         '1h', '24h', '7d', '30d', 'all', 'custom'.
        description: What this notebook investigates.
        max_results_per_section: Row cap per query section (default 1000).

    Returns:
        The created notebook, including its ID.
    """
    body = {
        "name": name,
        "description": description,
        "time_range_type": time_range_type,
        "max_results_per_section": max_results_per_section,
    }
    return as_json(await http.post("/notebooks", body))


@tool
async def add_notebook_section(
    notebook_id: str,
    section_type: str,
    content: str,
    title: str = "",
    order_index: int = -1,
    tags: list[str] | None = None,
) -> str:
    """
    Append a section to a notebook.

    Args:
        notebook_id: The notebook UUID.
        section_type: 'markdown' for narrative, or 'query' for a runnable BQL step.
        content: The markdown text, or the BQL query for a query section.
        title: Optional section heading.
        order_index: Position, 0-based. Use -1 to append at the end.
        tags: Tags for the section (e.g. ['timeline', 'lateral-movement']).

    Returns:
        The created section, including its ID.
    """
    if order_index < 0:
        response = await http.get(f"/notebooks/{notebook_id}")
        notebook = response.get("notebook", response)
        order_index = len(notebook.get("sections") or [])

    body = {"section_type": section_type, "content": content, "order_index": order_index}
    if title:
        body["title"] = title
    if tags is not None:
        body["tags"] = tags

    return as_json(await http.post(f"/notebooks/{notebook_id}/sections", body))

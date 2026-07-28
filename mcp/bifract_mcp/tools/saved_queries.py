"""Tools for reading saved BQL queries."""

from .. import http
from ..app import as_json, tool


@tool
async def list_saved_queries() -> str:
    """
    List the BQL queries users have saved in this fractal.

    The best available record of how this environment is actually searched: field
    names in real use, and the pipelines analysts trust.

    Returns:
        Saved queries with their names, BQL, descriptions, and tags.
    """
    return as_json(await http.get("/saved-queries"))

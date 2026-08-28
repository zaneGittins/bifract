"""Tools for dictionaries: the watchlists and lookup tables detections join against."""

from .. import http
from ..app import as_json, summarize, tool

SUMMARY_FIELDS = ("id", "name", "description", "key_column", "row_count", "is_global")
MAX_ROWS = 200


@tool
async def list_dictionaries() -> str:
    """
    List the dictionaries available in this fractal.

    A dictionary is a keyed lookup table held alongside the logs: known-bad indicators,
    asset inventories, user-to-team mappings, allow lists. Detections consult them
    instead of hard-coding values, so a watchlist changes without touching a query.

    Read one before writing a detection that refers to it: the key column is what a
    lookup matches on, and the other columns are what a match returns.

    Returns:
        Each dictionary's id, name, key column and row count.
    """
    dictionaries = await http.get("/dictionaries")
    if not isinstance(dictionaries, list):
        return as_json(dictionaries)
    summaries = summarize(dictionaries, SUMMARY_FIELDS)
    return as_json({"count": len(summaries), "dictionaries": summaries})


@tool
async def get_dictionary(dictionary_id: str) -> str:
    """
    Get one dictionary's definition: its columns, key, and how it is populated.

    Args:
        dictionary_id: The dictionary UUID, from list_dictionaries.

    Returns:
        The dictionary's columns, key column, row count and metadata. This is the
        definition only; use search_dictionary to read the rows.
    """
    return as_json(await http.get(f"/dictionaries/{dictionary_id}"))


@tool
async def search_dictionary(dictionary_id: str, search: str = "", limit: int = 50) -> str:
    """
    Read rows from a dictionary, optionally filtered.

    Use this to answer "is this indicator already on a watchlist" before escalating,
    and to see what a detection's lookup would return for a given key.

    Args:
        dictionary_id: The dictionary UUID, from list_dictionaries.
        search: Match rows containing this text. Empty returns the first rows as-is.
        limit: How many rows to return, capped at 200. A dictionary can hold far more,
               so filter rather than paging through it.

    Returns:
        The matching rows, each with its key and field values.
    """
    params = {"limit": max(1, min(limit, MAX_ROWS))}
    if needle := search.strip():
        params["search"] = needle
    return as_json(await http.get(f"/dictionaries/{dictionary_id}/data", params))


@tool
async def add_dictionary_rows(dictionary_id: str, rows: list[dict]) -> str:
    """
    Insert or update rows in a dictionary.

    This changes what live detections match on, so treat it as a production edit:
    read the dictionary first and confirm the key column and column names, because a
    row whose key does not line up is stored but never matched.

    Args:
        dictionary_id: The dictionary UUID, from list_dictionaries.
        rows: The rows to write, each {"key": "<key value>", "fields": {"col": "value"}}.
              A key that already exists is overwritten, not duplicated.

    Returns:
        How many rows were written.
    """
    if not rows:
        return "Error: no rows given, so there is nothing to write."

    malformed = [i for i, row in enumerate(rows) if not isinstance(row, dict) or not row.get("key")]
    if malformed:
        return (
            f"Error: rows at index {malformed} have no 'key'. Each row must be "
            '{"key": "<key value>", "fields": {"column": "value"}}.'
        )
    return as_json(await http.post(f"/dictionaries/{dictionary_id}/data", {"rows": rows}))

"""Tools for Recall: searching archived logs that have aged out of hot storage."""

from .. import http, scope
from ..app import as_json, tool

MAX_ROWS = 250
TERMINAL = ("succeeded", "failed", "canceled")


@tool
async def search_archive(query: str, start: str, end: str, max_rows: int = 250) -> str:
    """
    Search archived logs, for hunts reaching past the fractal's hot retention.

    query_logs reads hot storage only; this runs the same BQL against object
    storage, which takes minutes rather than seconds. Use it only when the range
    query_logs covers cannot answer the question. Returns a job id to poll with
    get_archive_search.

    Args:
        query: BQL, the same syntax query_logs takes. Some pipeline commands are
               not available over the archive; a rejected query says which.
        start: Start of the window, RFC3339. Required, and worth keeping tight:
               the archive has no safe default and a wide window scans everything.
        end: End of the window, RFC3339. Must be after start.
        max_rows: Rows to return, capped at 250. Narrow the query rather than
                  raising this: Recall finds evidence, it is not a bulk export.

    Returns:
        The job id to poll, and whether an identical recent search was reused.
    """
    if not query.strip():
        return "Error: a query is required."
    if not start.strip() or not end.strip():
        return "Error: search_archive needs both start and end, as RFC3339 times."

    body = {
        "query": query,
        "from": start.strip(),
        "to": end.strip(),
        "max_rows": max(1, min(max_rows, MAX_ROWS)),
    }
    result = await http.post(f"/recall/{await scope.fractal_id()}", body)
    if isinstance(result, dict) and result.get("id") is not None:
        result = dict(result)
        result["note"] = "Poll get_archive_search with this id. Archive scans can take minutes."
    return as_json(result)


@tool
async def get_archive_search(job_id: str) -> str:
    """
    Read a Recall job: its status, and its rows once it has finished.

    Status is one of pending, running, succeeded, failed or canceled. While it is
    pending or running there are no rows yet; wait before polling again rather
    than calling in a tight loop, as each call costs a round trip and the scan
    itself is what takes the time.

    Args:
        job_id: The job id returned by search_archive.

    Returns:
        The job's status and, once it has succeeded, the matching rows.
    """
    job = await http.get(f"/recall/{await scope.fractal_id()}/{job_id}")
    if not isinstance(job, dict):
        return as_json(job)

    status = str(job.get("status", "")).lower()
    if status and status not in TERMINAL:
        return as_json(
            {
                "id": job.get("id", job_id),
                "status": status,
                "note": "Still scanning. Poll again in a few seconds; results appear when it succeeds.",
            }
        )
    return as_json(job)


@tool
async def cancel_archive_search(job_id: str) -> str:
    """
    Cancel a Recall job that is still pending or running.

    Worth doing when a search turns out to be too broad: it stops the scan rather
    than leaving it consuming read capacity. A job that has already finished
    cannot be canceled.

    Args:
        job_id: The job id returned by search_archive.

    Returns:
        Confirmation that the job was canceled.
    """
    return as_json(await http.post(f"/recall/{await scope.fractal_id()}/{job_id}/cancel"))

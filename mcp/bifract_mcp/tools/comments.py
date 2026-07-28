"""Tools for annotating logs. API keys need the comment permission."""

from .. import http
from ..app import as_json, tool

AI_TAG = "AI-Generated"


@tool
async def add_comment(log_id: str, text: str, tags: list[str] | None = None) -> str:
    """
    Attach a comment to a log entry.

    Comments are how findings are recorded for other analysts. The tag "AI-Generated"
    is always added.

    When several logs belong to one investigation, give them a shared tag of the form
    IR-<OneWord> (IR-BruteForce, IR-Exfiltration, IR-LateralMovement) and reuse it
    across every comment in that investigation so they can be pulled up together.

    Args:
        log_id: The log_id of the entry to comment on (query results carry it).
        text: The comment body; markdown is supported.
        tags: Additional tags to attach.

    Returns:
        The created comment.
    """
    all_tags = list(tags) if tags else []
    if AI_TAG not in all_tags:
        all_tags.insert(0, AI_TAG)

    body = {"log_id": log_id, "text": text, "tags": all_tags}
    return as_json(await http.post("/comments", body))


@tool
async def list_comments() -> str:
    """
    List every comment in the fractal, most recent first.

    Returns:
        Comments with their text, tags, author, and the log they annotate.
    """
    return as_json(await http.get("/comments/flat"))


@tool
async def list_comment_tags() -> str:
    """
    List the comment tags in use in this fractal.

    Use this to find the IR-<Name> tag for an investigation already under way, so a
    new finding joins it instead of starting a parallel thread.

    Returns:
        The tags currently applied to comments.
    """
    return as_json(await http.get("/comments/tags"))


@tool
async def get_log_comments(log_id: str) -> str:
    """
    Get the comments on one log entry.

    Worth checking before adding a comment: an analyst may have already triaged it.

    Args:
        log_id: The log_id to look up.

    Returns:
        The comments on that log.
    """
    return as_json(await http.get(f"/logs/{log_id}/comments"))

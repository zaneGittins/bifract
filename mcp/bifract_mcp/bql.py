"""Helpers for assembling BQL from tool arguments."""

BREAKOUT_CHARS = '"\'\\|'


class BQLValueError(ValueError):
    """A tool argument cannot be placed into a query safely."""


def time_window(start: str, end: str) -> dict:
    """Build the start/end fragment of a query request body, omitting blanks."""
    window = {}
    if start.strip():
        window["start"] = start.strip()
    if end.strip():
        window["end"] = end.strip()
    return window


def literal(value: str, *, field: str) -> str:
    """Quote a value for a BQL comparison.

    BQL has no escape sequence inside a quoted value, so a value carrying a quote,
    backslash, pipe, or control character cannot be represented and is rejected rather
    than silently changing the query's meaning.
    """
    if any(c in value for c in BREAKOUT_CHARS) or any(ord(c) < 0x20 for c in value):
        raise BQLValueError(
            f"{field} contains a character BQL cannot quote "
            f"(one of {BREAKOUT_CHARS} or a control character): {value!r}"
        )
    return f'"{value}"'


def contains(field: str, value: str) -> str:
    """A case-insensitive substring match (=~), which is index-accelerated."""
    return f"{field}=~{literal(value, field=field)}"


def equals(field: str, value: str) -> str:
    return f"{field}={literal(value, field=field)}"

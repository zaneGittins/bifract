"""The fractal this session acts in.

Most endpoints infer the fractal from the credential, but a few name it in the
path. The API key binds it for the life of the process, so it is resolved once
and reused rather than costing a round trip per call.
"""

from . import http

_fractal_id: str | None = None


async def fractal_id() -> str:
    """The fractal the API key is bound to.

    Raises BifractError when the credential has no fractal, which is the case
    for a tenant-wide key: those endpoints cannot pick one on the caller's behalf.
    """
    global _fractal_id
    if _fractal_id:
        return _fractal_id

    identity = await http.get("/auth/user")
    user = identity.get("user", identity) if isinstance(identity, dict) else {}
    resolved = user.get("selected_fractal") or ""
    if not resolved:
        raise http.BifractError(
            "This API key is not bound to a fractal, so there is no scope for this "
            "call. Use a fractal-scoped key, or call list_fractals and use a tool "
            "that takes an explicit id."
        )
    _fractal_id = resolved
    return resolved


def reset() -> None:
    """Drop the cached fractal. For tests."""
    global _fractal_id
    _fractal_id = None

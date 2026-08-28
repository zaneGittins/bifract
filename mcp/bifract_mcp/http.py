"""Authenticated access to the Bifract HTTP API.

One shared connection pool for the process. Every failure mode (bad status,
unreachable host, TLS rejection, non-JSON body) surfaces as BifractError with a
message meant to be read by a model, never as a raw traceback.
"""

import json
from typing import Any

import httpx

from .config import Config, load

MAX_ERROR_BODY = 400

_config: Config | None = None
_client: httpx.AsyncClient | None = None


class BifractError(Exception):
    """A request to Bifract could not be completed."""


def config() -> Config:
    global _config
    if _config is None:
        _config = load()
    return _config


def _client_for(cfg: Config) -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=cfg.api_base,
            headers={
                "Authorization": f"Bearer {cfg.api_key}",
                "Content-Type": "application/json",
            },
            verify=cfg.verify,
            cert=cfg.client_cert,
            timeout=cfg.timeout,
            follow_redirects=False,
        )
    return _client


async def close() -> None:
    """Release the shared connection pool."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _error_detail(resp: httpx.Response) -> str:
    """Pull the message out of a Bifract error body.

    Handlers answer with either {"error": ...} JSON or bare text, so try both.
    """
    text = resp.text.strip()
    try:
        payload = json.loads(text)
    except ValueError:
        return text[:MAX_ERROR_BODY]
    if isinstance(payload, dict):
        detail = _failure(payload)
        if detail:
            return detail[:MAX_ERROR_BODY]
        for key in ("message", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value[:MAX_ERROR_BODY]
    return text[:MAX_ERROR_BODY]


async def request(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    body: Any = None,
    timeout: float | None = None,
) -> Any:
    """Call the API and return the decoded JSON body."""
    cfg = config()
    if cfg.error:
        raise BifractError(cfg.error)

    try:
        resp = await _client_for(cfg).request(
            method,
            path,
            params=params,
            json=body,
            timeout=timeout if timeout is not None else cfg.timeout,
        )
    except httpx.TimeoutException:
        raise BifractError(
            f"Timed out after {timeout or cfg.timeout:.0f}s calling {method} {path}. "
            "Narrow the time range or raise BIFRACT_TIMEOUT."
        ) from None
    except httpx.ConnectError as exc:
        raise BifractError(
            f"Cannot reach Bifract at {cfg.url}: {exc}. Check BIFRACT_URL, and set "
            "BIFRACT_CA_CERT (or BIFRACT_VERIFY_SSL=false) if the instance uses a "
            "certificate this machine does not trust."
        ) from None
    except httpx.HTTPError as exc:
        raise BifractError(f"Request to {cfg.url} failed: {exc}") from None

    if resp.status_code == 401:
        raise BifractError("Unauthorized. BIFRACT_API_KEY is invalid, disabled, or expired.")
    if resp.status_code == 403:
        raise BifractError(
            f"Forbidden: {_error_detail(resp)}. The API key's permissions do not cover this."
        )
    if resp.status_code == 404:
        raise BifractError(f"{method} {path}: not found. Check the id, and that it exists in this fractal.")
    if resp.status_code >= 400:
        raise BifractError(f"{method} {path} failed ({resp.status_code}): {_error_detail(resp)}")

    if not resp.content:
        return {}
    try:
        payload = resp.json()
    except ValueError:
        raise BifractError(
            f"{method} {path} returned a non-JSON response: {resp.text[:MAX_ERROR_BODY]}"
        ) from None

    # Belt and braces: failures answer 4xx now, but an endpoint that has not been
    # migrated could still report one in the body.
    if isinstance(payload, dict) and payload.get("success") is False:
        raise BifractError(_failure(payload) or f"{method} {path} was rejected by the server")
    return unwrap(payload)


def unwrap(payload: Any) -> Any:
    """Return the payload the model cares about rather than the transport.

    Bifract answers {"success": true, "data": ...}. Repeating that wrapper in
    every tool result spends the model's context teaching it nothing. Endpoints
    that answer their own shape, such as a query result, are passed through
    untouched.

    A paged answer keeps its page counts, because a model shown 100 of 4,000
    alerts and told nothing will reason as though it saw all of them.
    """
    if not isinstance(payload, dict) or "success" not in payload:
        return payload

    if "data" not in payload:
        rest = {k: v for k, v in payload.items() if k != "success"}
        return rest or {}

    data = payload["data"]
    page = payload.get("page")
    if isinstance(page, dict) and isinstance(data, list):
        total, limit, offset = page.get("total", 0), page.get("limit", 0), page.get("offset", 0)
        if total > offset + len(data):
            return {
                "items": data,
                "showing": f"{offset + 1}-{offset + len(data)} of {total}",
                "more": True,
                "next_offset": offset + len(data),
                "note": "This is one page. Ask for the next with offset, or narrow the request.",
            }
    return data


def _failure(payload: dict) -> str:
    """The message a failure envelope carries, with its machine-readable code."""
    message = payload.get("error") or payload.get("message") or ""
    code = payload.get("code")
    return f"{message} [{code}]" if code and message else message


async def get(path: str, params: dict | None = None, **kwargs) -> Any:
    return await request("GET", path, params=params, **kwargs)


async def post(path: str, body: Any = None, **kwargs) -> Any:
    return await request("POST", path, body=body, **kwargs)


async def put(path: str, body: Any = None, **kwargs) -> Any:
    return await request("PUT", path, body=body, **kwargs)


async def delete(path: str, **kwargs) -> Any:
    return await request("DELETE", path, **kwargs)

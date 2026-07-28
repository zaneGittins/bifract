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
        for key in ("error", "message", "detail"):
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

    # A handled failure comes back 200 with {"success": false}, so status alone is
    # not enough to tell a write that landed from one that was rejected.
    if isinstance(payload, dict) and payload.get("success") is False:
        raise BifractError(payload.get("error") or f"{method} {path} was rejected by the server")
    return payload


async def get(path: str, params: dict | None = None, **kwargs) -> Any:
    return await request("GET", path, params=params, **kwargs)


async def post(path: str, body: Any = None, **kwargs) -> Any:
    return await request("POST", path, body=body, **kwargs)


async def put(path: str, body: Any = None, **kwargs) -> Any:
    return await request("PUT", path, body=body, **kwargs)


async def delete(path: str, **kwargs) -> Any:
    return await request("DELETE", path, **kwargs)

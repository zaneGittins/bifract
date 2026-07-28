"""Environment configuration for the Bifract MCP server."""

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TIMEOUT = 60.0

_FALSEY = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class Config:
    """Resolved connection settings. Build with load()."""

    url: str
    api_key: str
    verify: bool | str
    client_cert: str | tuple[str, str] | None
    timeout: float
    error: str | None = None

    @property
    def api_base(self) -> str:
        return f"{self.url}/api/v1"


def _missing(path: str, label: str) -> str | None:
    return None if Path(path).is_file() else f"{label} file not found: {path}"


def _resolve_verify(errors: list[str]) -> bool | str:
    """CA bundle path, or a bool from BIFRACT_VERIFY_SSL. A bundle implies verification."""
    ca = os.environ.get("BIFRACT_CA_CERT", "").strip()
    if ca:
        if err := _missing(ca, "BIFRACT_CA_CERT"):
            errors.append(err)
        return ca
    return os.environ.get("BIFRACT_VERIFY_SSL", "true").strip().lower() not in _FALSEY


def _resolve_client_cert(errors: list[str]) -> str | tuple[str, str] | None:
    """mTLS material: a combined PEM, or a cert/key pair."""
    cert = os.environ.get("BIFRACT_CLIENT_CERT", "").strip()
    if not cert:
        return None
    if err := _missing(cert, "BIFRACT_CLIENT_CERT"):
        errors.append(err)
    key = os.environ.get("BIFRACT_CLIENT_KEY", "").strip()
    if not key:
        return cert
    if err := _missing(key, "BIFRACT_CLIENT_KEY"):
        errors.append(err)
    return (cert, key)


def _resolve_timeout(errors: list[str]) -> float:
    raw = os.environ.get("BIFRACT_TIMEOUT", "").strip()
    if not raw:
        return DEFAULT_TIMEOUT
    try:
        value = float(raw)
    except ValueError:
        errors.append(f"BIFRACT_TIMEOUT is not a number: {raw}")
        return DEFAULT_TIMEOUT
    if value <= 0:
        errors.append(f"BIFRACT_TIMEOUT must be positive: {raw}")
        return DEFAULT_TIMEOUT
    return value


def load() -> Config:
    """Read configuration from the environment.

    Never raises: problems are collected into Config.error so tools can report
    them to the model instead of the server dying at import time.
    """
    errors: list[str] = []

    url = os.environ.get("BIFRACT_URL", "").strip().rstrip("/")
    if not url:
        errors.append("BIFRACT_URL environment variable is not set.")
    elif not url.startswith(("http://", "https://")):
        errors.append(f"BIFRACT_URL must start with http:// or https:// (got {url}).")

    api_key = os.environ.get("BIFRACT_API_KEY", "").strip()
    if not api_key:
        errors.append("BIFRACT_API_KEY environment variable is not set.")

    verify = _resolve_verify(errors)
    client_cert = _resolve_client_cert(errors)
    timeout = _resolve_timeout(errors)

    return Config(
        url=url,
        api_key=api_key,
        verify=verify,
        client_cert=client_cert,
        timeout=timeout,
        error=" ".join(errors) if errors else None,
    )

"""Tests for how API responses and failures are surfaced to the model."""

import httpx
import pytest

from bifract_mcp import config, http


@pytest.fixture(autouse=True)
def isolated_client(monkeypatch):
    """Point the module at a stub transport and reset its shared state."""
    monkeypatch.setattr(http, "_config", None)
    monkeypatch.setattr(http, "_client", None)
    monkeypatch.setenv("BIFRACT_URL", "https://bifract.example.com")
    monkeypatch.setenv("BIFRACT_API_KEY", "bifract_abc123")
    yield
    monkeypatch.setattr(http, "_client", None)


def stub(handler):
    """Install an AsyncClient backed by handler, keeping real request plumbing."""
    client = httpx.AsyncClient(
        base_url="https://bifract.example.com/api/v1",
        transport=httpx.MockTransport(handler),
    )
    http._client = client
    return client


async def test_successful_json_is_returned():
    stub(lambda request: httpx.Response(200, json={"success": True, "data": [1, 2]}))
    assert await http.get("/alerts") == {"success": True, "data": [1, 2]}


async def test_handled_failure_body_becomes_an_error():
    stub(lambda request: httpx.Response(200, json={"success": False, "error": "Notebook not found"}))
    with pytest.raises(http.BifractError, match="Notebook not found"):
        await http.get("/notebooks/x")


async def test_failure_body_without_a_message_still_errors():
    stub(lambda request: httpx.Response(200, json={"success": False}))
    with pytest.raises(http.BifractError, match="rejected by the server"):
        await http.get("/query/fieldstats")


async def test_unauthorized_names_the_api_key():
    stub(lambda request: httpx.Response(401, text="Unauthorized"))
    with pytest.raises(http.BifractError, match="BIFRACT_API_KEY"):
        await http.get("/alerts")


async def test_forbidden_mentions_permissions():
    stub(lambda request: httpx.Response(403, json={"error": "access denied"}))
    with pytest.raises(http.BifractError, match="permissions"):
        await http.get("/saved-queries")


async def test_plain_text_error_body_is_preserved():
    stub(lambda request: httpx.Response(500, text="Failed to create alert"))
    with pytest.raises(http.BifractError, match="Failed to create alert"):
        await http.post("/alerts", {})


async def test_non_json_success_body_is_reported_clearly():
    stub(lambda request: httpx.Response(200, text="<html>gateway</html>"))
    with pytest.raises(http.BifractError, match="non-JSON"):
        await http.get("/alerts")


async def test_empty_body_is_an_empty_result():
    stub(lambda request: httpx.Response(204))
    assert await http.delete("/alerts/x") == {}


async def test_connection_failure_suggests_the_tls_settings():
    def boom(request):
        raise httpx.ConnectError("certificate verify failed", request=request)

    stub(boom)
    with pytest.raises(http.BifractError, match="BIFRACT_CA_CERT"):
        await http.get("/alerts")


async def test_timeout_suggests_narrowing_the_query():
    def slow(request):
        raise httpx.ReadTimeout("timed out", request=request)

    stub(slow)
    with pytest.raises(http.BifractError, match="BIFRACT_TIMEOUT"):
        await http.post("/query", {"query": "level=*"})


async def test_configuration_errors_are_raised_before_any_request(monkeypatch):
    monkeypatch.setattr(http, "_config", config.Config("", "", True, None, 60.0, error="no URL"))
    with pytest.raises(http.BifractError, match="no URL"):
        await http.get("/alerts")


def test_client_is_built_with_auth_and_the_api_base():
    client = http._client_for(http.config())
    assert client.headers["Authorization"] == "Bearer bifract_abc123"
    assert str(client.base_url).rstrip("/") == "https://bifract.example.com/api/v1"


async def test_query_params_reach_the_request():
    seen = {}

    def capture(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    stub(capture)
    await http.get("/logs/recent", {"count": "5"})
    assert seen["url"].endswith("/logs/recent?count=5")


async def test_the_client_is_reused_across_requests():
    stub(lambda request: httpx.Response(200, json={}))
    first = http._client
    await http.get("/version")
    await http.get("/version")
    assert http._client is first

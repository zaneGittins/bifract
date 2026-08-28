"""Behaviour of the fractal, dictionary, ATT&CK and Recall tools, against a stubbed API."""

import json

import httpx
import pytest

from bifract_mcp import http, scope
from bifract_mcp.app import mcp
import bifract_mcp.tools  # noqa: F401


@pytest.fixture(autouse=True)
def isolated_client(monkeypatch):
    monkeypatch.setattr(http, "_config", None)
    monkeypatch.setattr(http, "_client", None)
    monkeypatch.setenv("BIFRACT_URL", "https://bifract.example.com")
    monkeypatch.setenv("BIFRACT_API_KEY", "bifract_abc123")
    scope.reset()
    yield
    monkeypatch.setattr(http, "_client", None)
    scope.reset()


def stub(handler):
    http._client = httpx.AsyncClient(
        base_url="https://bifract.example.com/api/v1",
        transport=httpx.MockTransport(handler),
    )


async def call(name, args=None):
    result = await mcp.call_tool(name, args or {})
    return "\n".join(getattr(c, "text", str(c)) for c in result.content)


def _identity(fractal="f-1"):
    return {"success": True, "user": {"username": "svc", "selected_fractal": fractal}}


async def test_list_fractals_summarizes_both_scopes():
    stub(
        lambda r: httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "fractals": [{"id": "f-1", "name": "prod", "log_count": 12, "secret": "x"}],
                    "prisms": [{"id": "p-1", "name": "all", "fractal_ids": ["f-1"]}],
                },
            },
        )
    )
    payload = json.loads(await call("list_fractals"))
    assert payload["count"] == 1
    assert payload["fractals"][0]["name"] == "prod"
    assert "secret" not in payload["fractals"][0], "only the declared summary fields are returned"
    assert payload["prisms"][0]["id"] == "p-1"


async def test_search_dictionary_caps_the_row_count():
    seen = {}

    def route(request):
        seen["limit"] = request.url.params.get("limit")
        seen["search"] = request.url.params.get("search")
        return httpx.Response(200, json={"success": True, "data": []})

    stub(route)
    await call("search_dictionary", {"dictionary_id": "d-1", "search": " evil ", "limit": 5000})
    assert seen["limit"] == "200", "a model asking for 5000 rows must not get an unbounded read"
    assert seen["search"] == "evil"


async def test_add_dictionary_rows_rejects_a_row_with_no_key():
    """A keyless row is stored but never matches, so it fails silently in production."""
    stub(lambda r: pytest.fail("a malformed write must not reach the API"))
    result = await call(
        "add_dictionary_rows",
        {"dictionary_id": "d-1", "rows": [{"key": "1.2.3.4", "fields": {}}, {"fields": {}}]},
    )
    assert result.startswith("Error:")
    assert "[1]" in result


async def test_add_dictionary_rows_rejects_an_empty_write():
    stub(lambda r: pytest.fail("an empty write must not reach the API"))
    assert (await call("add_dictionary_rows", {"dictionary_id": "d-1", "rows": []})).startswith("Error:")


async def test_attack_coverage_filters_by_tactic_and_ranks_by_rule_count():
    stub(
        lambda r: httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "summary": {"covered": 2},
                    "techniques": [
                        {"id": "T1110", "tactic": "credential-access", "rule_count": 1},
                        {"id": "T1003", "tactic": "credential-access", "rule_count": 9},
                        {"id": "T1059", "tactic": "execution", "rule_count": 4},
                        {"id": "T1567", "tactic": "credential-access", "rule_count": 0},
                    ],
                },
            },
        )
    )
    payload = json.loads(await call("get_attack_coverage", {"tactic": "Credential-Access"}))
    assert [t["id"] for t in payload["techniques"]] == ["T1003", "T1110"]
    assert payload["covered_techniques"] == 2, "an uncovered technique is not coverage"


async def test_attack_gaps_explains_an_unpopulated_catalog():
    stub(lambda r: httpx.Response(200, json={"success": True, "data": {"catalog_populated": False}}))
    payload = json.loads(await call("get_attack_gaps"))
    assert payload["gaps"] == []
    assert "feed" in payload["note"]


async def test_search_archive_requires_a_bounded_window():
    stub(lambda r: pytest.fail("an unbounded archive scan must not be submitted"))
    result = await call("search_archive", {"query": "level=error", "start": "", "end": ""})
    assert result.startswith("Error:")
    assert "start and end" in result


async def test_search_archive_submits_the_window_and_returns_a_job():
    sent = {}

    def route(request):
        if request.url.path.endswith("/auth/user"):
            return httpx.Response(200, json=_identity())
        sent.update(json.loads(request.content))
        sent["path"] = request.url.path
        return httpx.Response(200, json={"success": True, "id": 42, "reused": False})

    stub(route)
    payload = json.loads(
        await call(
            "search_archive",
            {
                "query": "level=error",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-02T00:00:00Z",
                "max_rows": 9999,
            },
        )
    )
    assert sent["path"].endswith("/recall/f-1"), "the search runs in the key's fractal"
    assert sent["from"] == "2026-01-01T00:00:00Z" and sent["to"] == "2026-01-02T00:00:00Z"
    assert sent["max_rows"] == 250, "the row cap is enforced before the scan is submitted"
    assert payload["id"] == 42
    assert "Poll" in payload["note"]


async def test_get_archive_search_does_not_present_a_running_job_as_empty():
    """Returning a running job's empty result set would read as 'nothing matched'."""

    def route(request):
        if request.url.path.endswith("/auth/user"):
            return httpx.Response(200, json=_identity())
        return httpx.Response(200, json={"success": True, "data": {"id": 42, "status": "running", "results": []}})

    stub(route)
    payload = json.loads(await call("get_archive_search", {"job_id": "42"}))
    assert payload["status"] == "running"
    assert "results" not in payload
    assert "Poll again" in payload["note"]


async def test_get_archive_search_returns_the_rows_once_it_succeeds():
    def route(request):
        if request.url.path.endswith("/auth/user"):
            return httpx.Response(200, json=_identity())
        return httpx.Response(
            200, json={"success": True, "data": {"id": 42, "status": "succeeded", "results": [{"a": 1}]}}
        )

    stub(route)
    payload = json.loads(await call("get_archive_search", {"job_id": "42"}))
    assert payload["results"] == [{"a": 1}]


async def test_a_key_with_no_fractal_is_told_why_not_which_traceback():
    stub(lambda r: httpx.Response(200, json=_identity(fractal="")))
    result = await call("get_archive_search", {"job_id": "42"})
    assert result.startswith("Error:")
    assert "not bound to a fractal" in result


async def test_the_fractal_is_resolved_once_per_process():
    calls = {"identity": 0}

    def route(request):
        if request.url.path.endswith("/auth/user"):
            calls["identity"] += 1
            return httpx.Response(200, json=_identity())
        return httpx.Response(200, json={"success": True, "data": {"id": 1, "status": "succeeded"}})

    stub(route)
    await call("get_archive_search", {"job_id": "1"})
    await call("get_archive_search", {"job_id": "1"})
    assert calls["identity"] == 1, "the key's fractal is fixed; re-resolving it wastes a round trip"


async def test_update_alert_does_not_reset_the_fields_it_was_not_asked_to_change():
    """The endpoint replaces the alert and defaults what is absent.

    Editing a description used to send no severity, and the API filled in 'medium',
    silently downgrading a critical detection. A scheduled alert lost its cron and
    could not be saved at all.
    """
    sent = {}

    def route(request):
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "id": "a-1",
                        "name": "keep me",
                        "query_string": "level=error | count()",
                        "description": "old",
                        "alert_type": "scheduled",
                        "severity": "critical",
                        "enabled": True,
                        "labels": ["T1110"],
                        "references": ["https://example.test"],
                        "throttle_time_seconds": 60,
                        "throttle_field": "src_ip",
                        "schedule_cron": "*/15 * * * *",
                        "query_window_seconds": 900,
                        "webhook_actions": [{"id": "w-1", "name": "slack"}],
                        "email_actions": [],
                        "dictionary_action_ids": ["d-1"],
                    },
                },
            )
        sent.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "data": {"id": "a-1"}})

    stub(route)
    await call("update_alert", {"alert_id": "a-1", "description": "new"})

    assert sent["description"] == "new", "the requested change is applied"
    assert sent["severity"] == "critical", "a critical detection must not be downgraded by an edit"
    assert sent["schedule_cron"] == "*/15 * * * *", "a scheduled alert must keep its cron"
    assert sent["query_window_seconds"] == 900
    assert sent["name"] == "keep me"
    assert sent["labels"] == ["T1110"]
    assert sent["throttle_field"] == "src_ip"
    assert sent["webhook_action_ids"] == ["w-1"], "actions are read expanded but written as ids"
    assert sent["dictionary_action_ids"] == ["d-1"], (
        "dictionary actions are read and written under the same name, and were the "
        "one action type the carry-forward originally missed"
    )


async def test_create_alert_sends_type_specific_fields_only_when_set():
    """A zero window fails the API's positive-value check; absent means not applicable."""
    sent = {}

    def route(request):
        sent.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "data": {"id": "a-1"}})

    stub(route)
    await call("create_alert", {"name": "n", "query_string": "level=error"})
    assert "schedule_cron" not in sent and "window_duration" not in sent
    assert sent["severity"] == "medium"

    sent.clear()
    await call(
        "create_alert",
        {
            "name": "n",
            "query_string": "level=error | count()",
            "alert_type": "scheduled",
            "schedule_cron": "*/15 * * * *",
            "query_window_seconds": 900,
        },
    )
    assert sent["schedule_cron"] == "*/15 * * * *"
    assert sent["query_window_seconds"] == 900
    assert "window_duration" not in sent

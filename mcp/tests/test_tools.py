"""Tests for tool registration and argument handling, against a stubbed API."""

import json

import httpx
import pytest

from bifract_mcp import http
from bifract_mcp.app import mcp
import bifract_mcp.tools  # noqa: F401


@pytest.fixture(autouse=True)
def isolated_client(monkeypatch):
    monkeypatch.setattr(http, "_config", None)
    monkeypatch.setattr(http, "_client", None)
    monkeypatch.setenv("BIFRACT_URL", "https://bifract.example.com")
    monkeypatch.setenv("BIFRACT_API_KEY", "bifract_abc123")
    yield
    monkeypatch.setattr(http, "_client", None)


def stub(handler):
    http._client = httpx.AsyncClient(
        base_url="https://bifract.example.com/api/v1",
        transport=httpx.MockTransport(handler),
    )


async def call(name, args=None):
    result = await mcp.call_tool(name, args or {})
    content = result[0] if isinstance(result, tuple) else result
    return "\n".join(getattr(c, "text", str(c)) for c in content)


async def test_every_tool_has_a_description():
    tools = await mcp.list_tools()
    assert len(tools) >= 30
    for tool in tools:
        assert tool.description, f"{tool.name} has no description"
        assert tool.inputSchema["type"] == "object"


async def test_api_errors_are_returned_as_text_not_raised():
    stub(lambda request: httpx.Response(500, text="Failed to create alert"))
    result = await call("create_alert", {"name": "x", "query_string": "level=error"})
    assert result.startswith("Error:")
    assert "Failed to create alert" in result


async def test_query_logs_summarizes_the_result():
    stub(
        lambda request: httpx.Response(
            200,
            json={
                "success": True,
                "count": 2,
                "execution_ms": 17,
                "is_aggregated": True,
                "field_order": ["_count"],
                "results": [{"_count": 42}],
            },
        )
    )
    payload = json.loads(await call("query_logs", {"query": "level=error | count()"}))
    assert payload["summary"] == "Found 2 results in 17ms (aggregated)"
    assert payload["results"] == [{"_count": 42}]


async def test_query_logs_passes_the_time_window_through():
    seen = {}

    def capture(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "count": 0, "results": []})

    stub(capture)
    await call("query_logs", {"query": "level=error", "start": "2026-01-01T00:00:00Z"})
    assert seen == {"query": "level=error", "start": "2026-01-01T00:00:00Z"}


async def test_get_fields_filters_case_insensitively():
    stub(
        lambda request: httpx.Response(
            200, json={"success": True, "data": {"fields": ["src_ip", "dst_ip", "image"]}}
        )
    )
    payload = json.loads(await call("get_fields", {"filter": "IP"}))
    assert payload["fields"] == ["src_ip", "dst_ip"]
    assert payload["count"] == 2


async def test_get_recent_logs_clamps_the_count():
    seen = {}

    def capture(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"success": True})

    stub(capture)
    await call("get_recent_logs", {"count": 5000})
    assert "count=100" in seen["url"]


async def test_find_processes_builds_a_bounded_process_query():
    seen = {}

    def capture(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "count": 0, "results": []})

    stub(capture)
    await call("find_processes", {"image": "powershell", "host": "WKSTN", "limit": 500})

    query = seen["query"]
    assert 'bifract_category="process_creation"' in query
    assert 'image=~"powershell"' in query
    assert 'computer_name=~"WKSTN"' in query
    assert "head(100)" in query


async def test_find_processes_rejects_a_value_it_cannot_quote():
    stub(lambda request: httpx.Response(200, json={"success": True}))
    result = await call("find_processes", {"image": 'evil" OR image=~"'})
    assert result.startswith("Error:")
    assert "cannot quote" in result


async def test_provenance_graph_builds_the_pgr_source_command():
    seen = {}

    def capture(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "count": 0, "results": []})

    stub(capture)
    await call(
        "provenance_graph",
        {
            "guid": "{abc-123}",
            "depth": 99,
            "direction": "forward",
            "threshold": 0.4,
            "reconnect": False,
            "include": "file_write, dns_query",
        },
    )

    query = seen["query"]
    assert query.startswith('pgr(start="{abc-123}"')
    assert "depth=50" in query  # clamped to the server's maximum
    assert 'direction="forward"' in query
    assert "threshold=0.4" in query
    assert "reconnect=false" in query
    assert 'include="file_write,dns_query"' in query


@pytest.mark.parametrize(
    "args, expected",
    [
        ({"guid": "g", "direction": "sideways"}, "direction must be"),
        ({"guid": "g", "threshold": 5}, "threshold must be"),
        ({"guid": "g", "include": "nonsense"}, "unknown edge type"),
        ({"guid": 'g" OR 1'}, "cannot quote"),
    ],
)
async def test_provenance_graph_rejects_bad_arguments(args, expected):
    stub(lambda request: httpx.Response(200, json={"success": True, "results": []}))
    result = await call("provenance_graph", args)
    assert result.startswith("Error:")
    assert expected in result


async def test_provenance_graph_explains_an_empty_result():
    stub(lambda request: httpx.Response(200, json={"success": True, "count": 0, "results": []}))
    payload = json.loads(await call("provenance_graph", {"guid": "{missing}"}))
    assert payload["processes"] == 0
    assert "hint" in payload


async def test_provenance_graph_renders_the_tree():
    rows = [
        {
            "parent": "ROOT",
            "child": "A",
            "label": "powershell.exe",
            "event_type": "spawn",
            "anomaly_score": 0.9,
            "log_id": "log-a",
            "host": "HOST-1",
            "command_line": "powershell -enc AAA",
            "proc_user": "CORP\\alice",
            "timestamp": "2026-07-07 14:00:00.000",
        },
        {
            "parent": "A",
            "child": "dns:evil.example",
            "label": "evil.example",
            "event_type": "dns_query",
            "anomaly_score": 0.99,
            "log_id": "log-dns",
            "host": "HOST-1",
            "command_line": "",
            "proc_user": "",
            "timestamp": "2026-07-07 14:01:00.000",
        },
    ]
    stub(
        lambda request: httpx.Response(
            200, json={"success": True, "count": 2, "execution_ms": 12, "results": rows}
        )
    )
    payload = json.loads(await call("provenance_graph", {"guid": "A"}))

    assert payload["processes"] == 1
    assert "powershell.exe" in payload["process_tree"]
    assert "cmd: powershell -enc AAA" in payload["process_tree"]
    assert payload["notable_activity"][0]["target"] == "evil.example"
    assert payload["edge_counts"] == {"spawn": 1, "dns_query": 1}


async def test_add_comment_always_tags_ai_generated():
    seen = {}

    def capture(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True})

    stub(capture)
    await call("add_comment", {"log_id": "abc", "text": "note", "tags": ["IR-Test"]})
    assert seen["tags"] == ["AI-Generated", "IR-Test"]


async def test_add_comment_does_not_duplicate_the_ai_tag():
    seen = {}

    def capture(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True})

    stub(capture)
    await call("add_comment", {"log_id": "abc", "text": "note", "tags": ["AI-Generated"]})
    assert seen["tags"] == ["AI-Generated"]


async def test_add_tag_posts_trimmed_ids_to_the_bulk_endpoint():
    seen = {}

    def capture(request):
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "data": {"updated": 2}})

    stub(capture)
    result = await call("add_tag", {"comment_ids": [" a ", "b", ""], "tag": " IR-Test "})
    assert seen["path"].endswith("/comments/bulk-add-tag")
    assert seen["body"] == {"comment_ids": ["a", "b"], "tag": "IR-Test"}
    assert "updated" in result


async def test_remove_tag_posts_to_the_bulk_remove_endpoint():
    seen = {}

    def capture(request):
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "data": {"updated": 1}})

    stub(capture)
    await call("remove_tag", {"comment_ids": ["a"], "tag": "IR-Test"})
    assert seen["path"].endswith("/comments/bulk-remove-tag")
    assert seen["body"] == {"comment_ids": ["a"], "tag": "IR-Test"}


async def test_tag_tools_reject_bad_input_without_calling_the_api():
    def capture(request):
        raise AssertionError("should not reach the API")

    stub(capture)
    assert (await call("add_tag", {"comment_ids": [], "tag": "IR-Test"})).startswith("Error:")
    assert (await call("add_tag", {"comment_ids": ["a"], "tag": "  "})).startswith("Error:")
    assert (await call("remove_tag", {"comment_ids": ["a"], "tag": "x" * 101})).startswith("Error:")
    assert (await call("add_tag", {"comment_ids": ["a"] * 501, "tag": "t"})).startswith("Error:")


async def test_add_notebook_section_appends_after_existing_sections():
    seen = {}

    def capture(request):
        if request.method == "GET":
            return httpx.Response(
                200, json={"success": True, "sections": [{"id": "1"}, {"id": "2"}]}
            )
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True})

    stub(capture)
    await call(
        "add_notebook_section",
        {"notebook_id": "nb", "section_type": "markdown", "content": "hello"},
    )
    assert seen["order_index"] == 2


async def test_list_models_explains_an_empty_fractal():
    stub(lambda request: httpx.Response(200, json={"success": True, "data": {"models": []}}))
    payload = json.loads(await call("list_models"))
    assert payload["count"] == 0
    assert "hint" in payload


async def test_list_dashboards_returns_a_summary_not_every_widget():
    stub(
        lambda request: httpx.Response(
            200,
            json={
                "success": True,
                "data": [
                    {"id": "d1", "name": "Ops", "widgets": [{"query": "level=*"}] * 50},
                ],
            },
        )
    )
    payload = json.loads(await call("list_dashboards"))
    assert payload["dashboards"] == [{"id": "d1", "name": "Ops"}]


async def test_get_context_reports_the_scope():
    def route(request):
        if request.url.path.endswith("/version"):
            return httpx.Response(200, json={"version": "1.2.3"})
        return httpx.Response(
            200,
            json={
                "success": True,
                "user": {
                    "display_name": "API Key: ops",
                    "selected_fractal": "frac-1",
                    "fractal_role": "analyst",
                },
            },
        )

    stub(route)
    payload = json.loads(await call("get_context"))
    assert payload["fractal_id"] == "frac-1"
    assert payload["role"] == "analyst"
    assert payload["server_version"] == "1.2.3"

"""The tools are checked against the API they claim to call.

A description can drift from the build it documents; a generated spec cannot.
These tests read openapi.json, which is regenerated from the live router, so a
route that is renamed or an enum that gains a value fails here rather than at a
user's first call.
"""

import ast
import json
import re
from pathlib import Path

import pytest

from bifract_mcp.app import mcp
import bifract_mcp.tools  # noqa: F401

SPEC_PATH = Path(__file__).resolve().parents[2] / "openapi.json"
PREFIX = "/api/v1"
CALL = re.compile(r'http\.(get|post|put|delete)\(\s*f?"([^"]+)"')


def _spec():
    if not SPEC_PATH.is_file():
        pytest.skip(f"{SPEC_PATH} not generated; run the server's openapi test first")
    return json.loads(SPEC_PATH.read_text())


def _placeholders(path: str) -> str:
    """Reduce both spellings of a path variable to one, so they compare."""
    return re.sub(r"\{[^}]*\}", "{}", path)


def _spec_routes(spec) -> set[tuple[str, str]]:
    return {
        (method.upper(), _placeholders(path[len(PREFIX):]))
        for path, ops in spec["paths"].items()
        if path.startswith(PREFIX)
        for method in ops
        if method in ("get", "post", "put", "delete")
    }


def _tool_calls() -> set[tuple[str, str, str]]:
    """Every (method, path, module) the tool package calls."""
    found = set()
    for module in Path("bifract_mcp/tools").glob("*.py"):
        for method, path in CALL.findall(module.read_text()):
            found.add((method.upper(), _placeholders(path), module.name))
    return found


def _spec_enums(spec) -> dict[str, list[str]]:
    """Field name to allowed values, for every enum the API declares."""
    schemas = spec["components"]["schemas"]

    def deref(node):
        while isinstance(node, dict) and "$ref" in node:
            node = schemas[node["$ref"].rsplit("/", 1)[-1]]
        return node

    enums: dict[str, list[str]] = {}
    for schema in schemas.values():
        for field, prop in (deref(schema).get("properties") or {}).items():
            prop = deref(prop)
            if prop.get("enum"):
                enums.setdefault(field, prop["enum"])
    return enums


def test_every_endpoint_a_tool_calls_still_exists():
    """A route renamed in the API must not leave a tool calling the old path."""
    routes = _spec_routes(_spec())
    stale = [
        f"{module}: {method} {path}"
        for method, path, module in sorted(_tool_calls())
        if (method, path) not in routes
    ]
    assert not stale, "tools call endpoints the API no longer serves:\n  " + "\n  ".join(stale)


async def test_enum_arguments_match_the_values_the_api_accepts():
    """A default outside the enum is a tool that fails on every call.

    This is not hypothetical: create_alert defaulted alert_type to 'match' long
    after the API narrowed it to event/scheduled/compound.
    """
    enums = _spec_enums(_spec())
    wrong = []
    for tool in await mcp.list_tools():
        for name, prop in (tool.input_schema.get("properties") or {}).items():
            allowed = enums.get(name)
            default = prop.get("default")
            if not allowed or not isinstance(default, str) or default == "":
                continue
            if default not in allowed:
                wrong.append(f"{tool.name}({name}={default!r}) not in {allowed}")
    assert not wrong, "tool defaults the API would reject:\n  " + "\n  ".join(wrong)


async def test_enum_arguments_tell_the_model_the_allowed_values():
    """A model cannot guess an enum. The description has to carry the values."""
    enums = _spec_enums(_spec())
    silent = []
    for tool in await mcp.list_tools():
        for name, prop in (tool.input_schema.get("properties") or {}).items():
            allowed = enums.get(name)
            if not allowed:
                continue
            text = f"{tool.description or ''} {prop.get('description') or ''}"
            if missing := [v for v in allowed if v not in text]:
                silent.append(f"{tool.name}({name}) never mentions {missing}")
    assert not silent, "enum arguments a model would have to guess:\n  " + "\n  ".join(silent)


async def test_every_tool_documents_every_argument():
    """An undocumented argument is one the model fills in by guessing."""
    undocumented = []
    for tool in await mcp.list_tools():
        doc = tool.description or ""
        for name in (tool.input_schema.get("properties") or {}):
            if not re.search(rf"^\s*{re.escape(name)}\s*:", doc, re.M):
                undocumented.append(f"{tool.name}({name})")
    assert not undocumented, "arguments with no explanation:\n  " + "\n  ".join(undocumented)


async def test_every_tool_says_what_it_returns():
    """Without this a model cannot tell an empty result from a failed one."""
    missing = [t.name for t in await mcp.list_tools() if "Returns:" not in (t.description or "")]
    assert not missing, "tools that never describe their result: " + ", ".join(missing)


async def test_tool_names_are_verb_led_and_unique():
    tools = await mcp.list_tools()
    names = [t.name for t in tools]
    assert len(names) == len(set(names)), "duplicate tool names"
    verbs = ("list_", "get_", "create_", "update_", "delete_", "add_", "remove_",
             "query_", "validate_", "find_", "read_", "search_", "run_", "save_", "cancel_")
    odd = [n for n in names if not n.startswith(verbs)]
    assert not odd, "tool names that do not lead with a verb: " + ", ".join(odd)


DOCS_PATH = Path(__file__).resolve().parents[2] / "docs" / "features" / "mcp-server.md"
DOC_ROW = re.compile(r"^\|\s*`(\w+)`\s*\|", re.M)


async def test_the_documented_tool_list_matches_the_server():
    """The docs table is the tool list users read before installing anything."""
    if not DOCS_PATH.is_file():
        pytest.skip(f"{DOCS_PATH} not present")

    text = DOCS_PATH.read_text()
    start = text.index("## Available Tools")
    end = text.index("\n## ", start)
    documented = set(DOC_ROW.findall(text[start:end]))
    registered = {t.name for t in await mcp.list_tools()}

    undocumented = sorted(registered - documented)
    phantom = sorted(documented - registered)
    assert not undocumented, "tools the server exposes but the docs never mention: " + ", ".join(undocumented)
    assert not phantom, "tools the docs promise but the server does not expose: " + ", ".join(phantom)


def _request_properties(spec):
    """(method, normalised path) -> the body keys the API declares, or None."""
    schemas = spec["components"]["schemas"]

    def deref(node):
        while isinstance(node, dict) and "$ref" in node:
            node = schemas[node["$ref"].rsplit("/", 1)[-1]]
        return node

    out = {}
    for path, ops in spec["paths"].items():
        if not path.startswith(PREFIX):
            continue
        for method, op in ops.items():
            if method not in ("get", "post", "put", "delete"):
                continue
            body = op.get("requestBody")
            schema = (
                deref(body.get("content", {}).get("application/json", {}).get("schema", {}))
                if body
                else None
            )
            out[(method, _placeholders(path[len(PREFIX):]))] = (
                set((schema.get("properties") or {}).keys()) if schema else None
            )
    return out


def _sent_body_keys():
    """Every (method, path, keys) a tool posts, recovered from the source."""
    found = []
    for module in Path("bifract_mcp/tools").glob("*.py"):
        tree = ast.parse(module.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            # Bodies are built as a dict literal, sometimes with later key assignments.
            local: dict[str, set] = {}
            for node in ast.walk(fn):
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
                    if isinstance(node.value, ast.Dict):
                        local[node.targets[0].id] = {
                            k.value for k in node.value.keys
                            if isinstance(k, ast.Constant) and isinstance(k.value, str)
                        }
                elif isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Subscript):
                    target = node.targets[0]
                    if isinstance(target.value, ast.Name) and isinstance(target.slice, ast.Constant):
                        local.setdefault(target.value.id, set()).add(target.slice.value)

            for node in ast.walk(fn):
                if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                    continue
                if node.func.attr not in ("post", "put") or not isinstance(node.func.value, ast.Name):
                    continue
                if node.func.value.id != "http" or not node.args:
                    continue
                target = node.args[0]
                if isinstance(target, ast.Constant):
                    path = target.value
                elif isinstance(target, ast.JoinedStr):
                    path = "".join(
                        v.value if isinstance(v, ast.Constant) else "{x}" for v in target.values
                    )
                else:
                    continue
                keys = set()
                if len(node.args) > 1:
                    arg = node.args[1]
                    if isinstance(arg, ast.Dict):
                        keys = {
                            k.value for k in arg.keys
                            if isinstance(k, ast.Constant) and isinstance(k.value, str)
                        }
                    elif isinstance(arg, ast.Name):
                        keys = local.get(arg.id, set())
                if keys:
                    found.append((node.func.attr, _placeholders(path), keys, f"{module.name}:{fn.name}"))
    return found


def test_no_tool_sends_a_body_field_the_api_does_not_accept():
    """An unknown key is dropped silently, so the tool reports success having done nothing.

    This is the other half of endpoint conformance: a tool must not reference a
    parameter the server does not have, not just a path.
    """
    declared = _request_properties(_spec())
    wrong = []
    for method, path, keys, where in _sent_body_keys():
        allowed = declared.get((method, path), "missing")
        if allowed == "missing":
            wrong.append(f"{where}: {method.upper()} {path} is not a route")
        elif allowed is None:
            wrong.append(f"{where}: {method.upper()} {path} takes no body, but sends {sorted(keys)}")
        elif unknown := keys - allowed:
            wrong.append(f"{where}: {method.upper()} {path} sends {sorted(unknown)}, not in the schema")
    assert not wrong, "request fields the API would ignore:\n  " + "\n  ".join(wrong)

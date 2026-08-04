# MCP Server

Connect [Claude Code](https://claude.com/claude-code) or any [Model Context Protocol](https://modelcontextprotocol.io/) client to your Bifract instance. Query logs with BQL, manage detection alerts, annotate logs with comments, and more from your local terminal.

The MCP server is a lightweight Python wrapper around the Bifract HTTP API. It runs locally and authenticates with a Bifract API key.

## Prerequisites

- Python 3.10+
- A running Bifract instance
- A Bifract [API key](../administration/ingest-tokens.md) with at least `query` permission

## Install

The MCP server lives in the `mcp/` directory at the project root.

```bash
cd mcp
pip install -e .
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
cd mcp
uv pip install -e .
```

## Configure Claude Code

Create a `.mcp.json` file in the directory where you use Claude Code:

```json
{
  "mcpServers": {
    "bifract": {
      "command": "bifract-mcp",
      "env": {
        "BIFRACT_URL": "https://your-bifract-instance.example.com",
        "BIFRACT_API_KEY": "bifract_your_api_key_here"
      }
    }
  }
}
```

If you installed in a virtualenv, use the full path to the binary:

```json
{
  "mcpServers": {
    "bifract": {
      "command": "/path/to/venv/bin/bifract-mcp",
      "env": {
        "BIFRACT_URL": "https://your-bifract-instance.example.com",
        "BIFRACT_API_KEY": "bifract_your_api_key_here"
      }
    }
  }
}
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BIFRACT_URL` | Yes | Base URL of your Bifract instance |
| `BIFRACT_API_KEY` | Yes | API key starting with `bifract_`. Determines which fractal is queried. |
| `BIFRACT_CA_CERT` | No | Path to a CA bundle, for an instance behind a private or self-signed CA |
| `BIFRACT_CLIENT_CERT` | No | Client certificate for mTLS (a combined PEM, or the cert half of a pair) |
| `BIFRACT_CLIENT_KEY` | No | Client private key, when `BIFRACT_CLIENT_CERT` holds only the certificate |
| `BIFRACT_VERIFY_SSL` | No | Set to `false` to skip certificate verification. Prefer `BIFRACT_CA_CERT`. |
| `BIFRACT_TIMEOUT` | No | Request timeout in seconds (default 60) |

The API key is scoped to a single fractal. All queries, alerts, and comments are automatically scoped to that fractal with no additional configuration.

### Connecting through mTLS

A deployment fronted by Caddy with mTLS needs the client certificate generated under **Manage > Access > Users > Client Certificate**:

```json
"env": {
  "BIFRACT_URL": "https://bifract.example.com",
  "BIFRACT_API_KEY": "bifract_...",
  "BIFRACT_CA_CERT": "/etc/bifract/ca.pem",
  "BIFRACT_CLIENT_CERT": "/etc/bifract/client.pem",
  "BIFRACT_CLIENT_KEY": "/etc/bifract/client-key.pem"
}
```

## Available Tools

### Orientation

| Tool | Description |
|------|-------------|
| `get_context` | Which instance, fractal, and role this session is bound to |
| `get_fields` | The field names available to queries, optionally filtered |
| `get_bql_reference` | The full BQL syntax reference |

### Log Querying

| Tool | Description |
|------|-------------|
| `query_logs` | Execute a BQL query with optional time range |
| `validate_bql` | Check a query for syntax errors without running it (no database work) |
| `get_field_stats` | Per-field coverage, cardinality, and top values for a query's matches |
| `get_recent_logs` | Fetch recent logs to see the real event shape and data freshness |

### Provenance

| Tool | Description |
|------|-------------|
| `find_processes` | Locate process-creation events and their `process_guid` |
| `provenance_graph` | Expand a GUID into a scored process tree with its notable activity |

`provenance_graph` runs [`pgr()`](provenance-graph.md) and returns a rendered tree, the highest-anomaly file, network, and DNS actions, and any cross-tree reconnections, rather than a raw edge list. It needs endpoint behavioral analytics enabled and endpoint/EDR data normalized to `bifract_category` `process_creation`.

A typical sequence:

```
find_processes(image="rundll32", start="2026-07-26T00:00:00Z")
provenance_graph(guid="{390eae98-...}", threshold=0.3, start="2026-07-26T00:00:00Z")
add_comment(log_id="...", text="...", tags=["IR-Rundll32"])
```

### Behavioral Models

| Tool | Description |
|------|-------------|
| `list_models` | The behavioral baselines defined in this fractal |
| `get_model` | One model's full definition and backfill state |
| `get_model_data` | The rows a model has accumulated, to check whether an artifact is normal |

### Dashboards

| Tool | Description |
|------|-------------|
| `list_dashboards` | Dashboard summaries |
| `get_dashboard` | A dashboard with every widget and the BQL behind it |

### Alerts

| Tool | Description |
|------|-------------|
| `list_alerts` | List all detection alerts in the fractal |
| `get_alert` | Get full details of a specific alert |
| `create_alert` | Create a new detection alert with a BQL query |
| `update_alert` | Modify an existing alert |
| `delete_alert` | Remove an alert |
| `get_alert_executions` | View when an alert fired and what it matched |

### Collaboration

| Tool | Description |
|------|-------------|
| `add_comment` | Annotate a log entry with findings or notes |
| `list_comments` | View all comments in the fractal |
| `list_comment_tags` | Find the `IR-<Name>` tag for an investigation already under way |
| `add_tag` | Add a tag to existing comments, to group them into one investigation |
| `remove_tag` | Remove a tag from existing comments |
| `get_log_comments` | Read the comments on one log entry |
| `list_saved_queries` | Browse saved BQL queries for common patterns |

### Notebooks

| Tool | Description |
|------|-------------|
| `list_notebooks` | List notebooks in the fractal |
| `get_notebook` | Read a notebook and all its sections |
| `create_notebook` | Create a new notebook |
| `add_notebook_section` | Append a markdown or query section to a notebook |

### Instruction Libraries

| Tool | Description |
|------|-------------|
| `list_instruction_libraries` | List available instruction libraries |
| `get_instruction_library` | Read a library and its page structure |
| `read_instruction_page` | Read a single page's content |
| `create_instruction_library` | Create a new library |
| `create_instruction_page` | Add a page to a library |
| `update_instruction_page` | Edit an existing page |

## Example Prompts

Once configured, ask Claude Code things like:

- "Query Bifract for all error logs in the last hour"
- "Show me the top 10 source IPs with failed logins"
- "What alerts are currently configured?"
- "Show me recent logs so I can understand the field structure"
- "Find every rundll32 process yesterday and build the provenance graph for the most suspicious one"
- "What did this process write to disk, and is that path normal for our fleet?"
- "Add a comment to log abc123 noting this is a confirmed true positive"

## Creating an API Key


1. Log in to your Bifract instance
2. Navigate to the fractal you want to query
3. Go to **Manage > Access > API Keys**
4. Create a new key with at least `query` permission
5. For alert management, also enable `alert_manage`
6. Copy the generated key (starts with `bifract_`)

## How It Compares to AI Chat

The built-in [AI Chat](ai-chat.md) runs inside the Bifract UI and uses a server-side LLM via LiteLLM. The MCP server is the inverse: it runs locally and lets your own Claude Code instance call Bifract's API directly. Use whichever fits your workflow, or both.

| | AI Chat | MCP Server |
|---|---------|------------|
| Runs in | Bifract UI (browser) | Local terminal (Claude Code) |
| LLM | Server-side via LiteLLM | Your local Claude Code |
| Auth | Session cookie | API key |
| Best for | Quick in-app investigations | Deep analysis alongside code, scripting, automation |

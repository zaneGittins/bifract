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

The API key is scoped to a single fractal. All queries, alerts, and comments are automatically scoped to that fractal with no additional configuration.

## Available Tools

### Log Querying

| Tool | Description |
|------|-------------|
| `query_logs` | Execute a BQL query with optional time range |
| `get_recent_logs` | Fetch recent logs to discover fields and log structure |
| `get_bql_reference` | Return the full BQL syntax reference |

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
| `list_saved_queries` | Browse saved BQL queries for common patterns |

## Example Prompts

Once configured, ask Claude Code things like:

- "Query Bifract for all error logs in the last hour"
- "Show me the top 10 source IPs with failed logins"
- "Create an alert that fires on brute-force login attempts"
- "What alerts are currently configured?"
- "Show me recent logs so I can understand the field structure"
- "Add a comment to log abc123 noting this is a confirmed true positive"

## Creating an API Key

1. Log in to your Bifract instance
2. Navigate to the fractal you want to query
3. Go to **Settings > API Keys**
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

# Bifract MCP Server

Connect [Claude Code](https://claude.com/claude-code) (or any MCP client) to your Bifract instance. Query logs with BQL, manage detection alerts, comment on logs, and more, all from your terminal.

## Prerequisites

- Python 3.10+
- A running Bifract instance
- A Bifract API key with `query` permission (and `alert_manage` if you want to create alerts)

## Install

```bash
cd mcp
pip install -e .
```

Or with `uv`:

```bash
cd mcp
uv pip install -e .
```

## Configure Claude Code

Add to your Claude Code MCP settings (`~/.claude/settings.json` or project `.claude/settings.json`):

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

If you installed with `uv` or in a virtualenv, use the full path to the binary:

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

## Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `BIFRACT_URL`       | Yes      | Base URL of your Bifract instance (e.g. `https://bifract.example.com`) |
| `BIFRACT_API_KEY`   | Yes      | Bifract API key (`bifract_...`). The key determines which fractal you query. |

The API key is scoped to a specific fractal. All queries, alerts, and comments are automatically scoped to that fractal.

## Available Tools

### Log Querying
- **query_logs** - Execute BQL queries with optional time range
- **get_recent_logs** - Fetch recent logs to discover fields and log structure
- **get_bql_reference** - Get the full BQL syntax reference

### Alerts
- **list_alerts** - List all detection alerts
- **get_alert** - Get details of a specific alert
- **create_alert** - Create a new detection alert with BQL query
- **update_alert** - Modify an existing alert
- **delete_alert** - Remove an alert
- **get_alert_executions** - View when an alert fired and what it matched

### Collaboration
- **add_comment** - Annotate a log entry with findings or notes
- **list_comments** - View all comments in the fractal
- **list_saved_queries** - Browse saved BQL queries for common patterns

## Example Usage

Once configured, you can ask Claude Code things like:

- "Query Bifract for all error logs in the last hour"
- "Show me the top 10 source IPs with failed logins"
- "Create an alert that fires when there are more than 100 failed logins in 5 minutes"
- "What alerts are currently configured?"
- "Show me recent logs so I can understand the field structure"

## Creating an API Key

1. Log in to your Bifract instance
2. Navigate to the fractal you want to query
3. Go to Settings > API Keys
4. Create a new key with at least `query` permission
5. For alert management, also enable `alert_manage`
6. Copy the key (it starts with `bifract_`)

## Security Notes

- API keys are scoped to a single fractal and cannot access other fractals
- Never commit your API key to version control
- Use environment variables or a secrets manager to provide the key
- The MCP server communicates with Bifract over HTTPS with TLS verification enabled

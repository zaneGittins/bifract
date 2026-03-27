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
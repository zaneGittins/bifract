# Bifract MCP Server

Connect [Claude Code](https://claude.com/claude-code) (or any MCP client) to your Bifract instance. Query logs with BQL, expand a process into a scored provenance graph, manage detection alerts, and annotate findings, all from your terminal.

## Prerequisites

- Python 3.10+
- A running Bifract instance
- A Bifract API key with `query` permission (plus `alert_manage`, `comment`, `notebook`, or `dashboard` for the matching tools)

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
| `BIFRACT_CA_CERT`   | No       | Path to a CA bundle, for an instance behind a private or self-signed CA |
| `BIFRACT_CLIENT_CERT` | No     | Client certificate for mTLS (a combined PEM, or the cert half of a pair) |
| `BIFRACT_CLIENT_KEY` | No      | Client private key, when `BIFRACT_CLIENT_CERT` holds only the certificate |
| `BIFRACT_VERIFY_SSL` | No      | Set to `false` to skip certificate verification. Prefer `BIFRACT_CA_CERT`. |
| `BIFRACT_TIMEOUT`   | No       | Request timeout in seconds (default 60) |

The API key is scoped to a specific fractal. All queries, alerts, and comments are automatically scoped to that fractal.

A Bifract instance fronted by Caddy with mTLS needs the client certificate generated under **Manage > Access > Users > Client Certificate**:

```json
"env": {
  "BIFRACT_URL": "https://bifract.example.com",
  "BIFRACT_API_KEY": "bifract_...",
  "BIFRACT_CA_CERT": "/etc/bifract/ca.pem",
  "BIFRACT_CLIENT_CERT": "/etc/bifract/client.pem",
  "BIFRACT_CLIENT_KEY": "/etc/bifract/client-key.pem"
}
```

## Layout

| Module | Contents |
|--------|----------|
| `config.py` | Environment settings, including TLS material |
| `http.py` | Shared authenticated client, error handling |
| `bql.py` | Building and quoting BQL fragments |
| `provenance.py` | Shaping `pgr()` edge rows into a readable tree |
| `app.py` | The server instance and the tool decorator |
| `tools/` | One module per feature area; importing the package registers the tools |

## Tests

```bash
pip install -e . pytest pytest-asyncio
pytest
```

The suite stubs the HTTP transport, so it needs no running Bifract instance.

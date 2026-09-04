# MCP Server

Connect [Claude Code](https://claude.com/claude-code) or any [Model Context Protocol](https://modelcontextprotocol.io/) client to your Bifract instance. Query logs with BQL, manage detection alerts, annotate logs with comments, and more from your local terminal.

The MCP server is a mode of the `bifract` CLI, so there is nothing extra to install. It
runs on your own machine and reaches your instance over the same HTTP API the web UI
uses, authenticating with a Bifract API key: a tool can never do more than that key is
allowed to do.

## Prerequisites

- A running Bifract instance
- The `bifract` CLI (the same binary used to install and manage a deployment)
- A Bifract [API key](../administration/ingest-tokens.md) with at least `query` permission

## Configure Claude Code

Create a `.mcp.json` file in the directory where you use Claude Code:

```json
{
  "mcpServers": {
    "bifract": {
      "command": "bifract",
      "args": ["--mcp"],
      "env": {
        "BIFRACT_URL": "https://your-bifract-instance.example.com",
        "BIFRACT_API_KEY": "bifract_your_api_key_here"
      }
    }
  }
}
```

Use the full path to the binary if `bifract` is not on the PATH the client launches with.
`bifract --mcp --help` prints the settings it reads.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BIFRACT_URL` | Yes | Base URL of your Bifract instance |
| `BIFRACT_API_KEY` | Yes | API key. Determines the scope and the role. |
| `BIFRACT_FRACTAL_ID` | No | The fractal to act in. Only an instance-wide key needs it; see below. |
| `BIFRACT_PRISM_ID` | No | A prism to act in instead, for reading across fractals |
| `BIFRACT_CA_CERT` | No | Path to a CA bundle, for an instance behind a private or self-signed CA |
| `BIFRACT_CLIENT_CERT` | No | Client certificate for mTLS (a combined PEM, or the cert half of a pair) |
| `BIFRACT_CLIENT_KEY` | No | Client private key, when `BIFRACT_CLIENT_CERT` holds only the certificate |
| `BIFRACT_VERIFY_SSL` | No | Set to `false` to skip certificate verification. Prefer `BIFRACT_CA_CERT`. |
| `BIFRACT_TIMEOUT` | No | Request timeout in seconds (default 60) |

`bifract --mcp --help` prints this list without leaving the terminal.

### Choosing the scope

A key issued for one fractal or prism (`bifract_<name>_...`) carries its scope already.
Every query, alert and comment lands there with nothing else to configure, and
`get_context` reports which one.

An instance-wide key (`bifract_admin_...`) belongs to no fractal, so it names the one it
means on each request. Give the session a fractal to act in:

```json
"env": {
  "BIFRACT_URL": "https://bifract.example.com",
  "BIFRACT_API_KEY": "bifract_admin_...",
  "BIFRACT_FRACTAL_ID": "588f9ff8-4fe9-484a-9ca8-2ee77260e0b8"
}
```

Without it, tools that name a fractal in the request say so rather than guessing. Prefer a
fractal-scoped key where one will do: it cannot reach past the fractal it was issued for,
whichever way the session is configured.

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
| `list_fractals` | The fractals and prisms this credential can reach |
| `get_fields` | The field names available to queries, optionally filtered |
| `get_bql_reference` | The full BQL syntax reference |

### Log Querying

| Tool | Description |
|------|-------------|
| `query_logs` | Execute a BQL query with optional time range |
| `validate_bql` | Check a query for syntax errors without running it (no database work) |
| `get_field_stats` | Per-field coverage, cardinality, and top values for a query's matches |
| `get_recent_logs` | Fetch recent logs to see the real event shape and data freshness |

Hot storage only. To search past the fractal's retention window, see [Recall](#recall).

### Provenance

| Tool | Description |
|------|-------------|
| `find_processes` | Locate process-creation events and their `process_guid` |
| `get_provenance_graph` | Expand a GUID into a scored process tree with its notable activity |

`get_provenance_graph` runs [`pgr()`](provenance-graph.md) and returns a rendered tree, the highest-anomaly file, network, and DNS actions, and any cross-tree reconnections, rather than a raw edge list. It needs endpoint behavioral analytics enabled and endpoint/EDR data normalized to `bifract_category` `process_creation`.

A typical sequence:

```
find_processes(image="rundll32", start="2026-07-26T00:00:00Z")
get_provenance_graph(guid="{390eae98-...}", threshold=0.3, start="2026-07-26T00:00:00Z")
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
| `get_alert_tests` | Show the test cases stored with an alert |
| `run_alert_tests` | Run test cases against a query without saving anything |
| `get_alert_policies` | List the policy rules this scope enforces on a definition |

### Alert governance

A fractal or prism can keep a history of every alert definition, enforce policy rules on
what a definition must contain, and review changes before they go live. Where review is
on, `create_alert`, `update_alert` and `delete_alert` are refused and the work is
submitted with `propose_alert_change` instead.

Read `get_alert_policies` before writing a definition: a rule with severity `block`
refuses the save outright. Where a policy requires passing tests, `run_alert_tests`
checks them before anything is submitted. An update proposal carries the alert's existing
actions and tests forward unless new ones are named.

Approving and merging a proposal are not exposed as tools. A model that could approve its
own proposal would make the gate meaningless, so those stay with a reviewer.

| Tool | Description |
|------|-------------|
| `get_alert_history` | Show how an alert's definition changed, and who changed it |
| `list_alert_changes` | List proposed alert changes awaiting review |
| `propose_alert_change` | Propose a create, update or delete for review |

### Dictionaries

Watchlists and lookup tables detections join against, rather than hard-coding values in a query.

| Tool | Description |
|------|-------------|
| `list_dictionaries` | The dictionaries in the fractal, with their key column and row count |
| `get_dictionary` | One dictionary's columns and key definition |
| `search_dictionary` | Read rows, optionally filtered, to check an indicator against a watchlist |
| `add_dictionary_rows` | Insert or update rows, changing what live detections match on |

### ATT&CK Coverage

| Tool | Description |
|------|-------------|
| `get_attack_coverage` | Which techniques the configured detections cover, optionally by tactic |
| `get_attack_gaps` | Uncovered techniques, ranked by what could be covered with today's telemetry |

Coverage is derived from the `attack.*` labels on the alerts that exist, so it reports what is configured rather than what has fired.

### Recall

[Recall](../administration/backup-restore.md) searches the object-storage archive, for hunts that reach further back than hot retention. A search is submitted as a job and polled, because an archive scan can take minutes.

| Tool | Description |
|------|-------------|
| `search_archive` | Submit a BQL search over the archive for an explicit time window |
| `get_archive_search` | Poll a Recall job and read its rows once it succeeds |
| `cancel_archive_search` | Stop a pending or running Recall job |

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
6. Copy the generated key. It is shown once.

The key is named for the scope it was issued for, so `bifract_default_...` belongs to the
fractal called `default`. An instance-wide key is `bifract_admin_...` and belongs to none;
see [Choosing the scope](#choosing-the-scope).

## How It Compares to AI Chat

The built-in [AI Chat](ai-chat.md) runs inside the Bifract UI and uses a server-side LLM via LiteLLM. The MCP server is the inverse: it runs locally and lets your own Claude Code instance call Bifract's API directly. Use whichever fits your workflow, or both.

| | AI Chat | MCP Server |
|---|---------|------------|
| Runs in | Bifract UI (browser) | Local terminal (Claude Code) |
| LLM | Server-side via LiteLLM | Your local Claude Code |
| Auth | Session cookie | API key |
| Best for | Quick in-app investigations | Deep analysis alongside code, scripting, automation |

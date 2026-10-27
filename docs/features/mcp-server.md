# MCP Server

Connect [Claude Code](https://claude.com/claude-code) or any [Model Context Protocol](https://modelcontextprotocol.io/) client to Bifract, to query logs, manage alerts and annotate findings from your terminal.

The server is a mode of the `bifract` CLI (`bifract --mcp`), so there is nothing extra to install. It runs on your machine, calls your instance's HTTP API with a Bifract API key, and can never do more than that key allows.

## Set up Claude Code

1. Create an API key for the fractal (see [Create an API key](#create-an-api-key)).
2. Create `.mcp.json` in the directory where you run Claude Code:

    ```json
    {
      "mcpServers": {
        "bifract": {
          "command": "bifract",
          "args": ["--mcp"],
          "env": {
            "BIFRACT_URL": "https://bifract.example.com",
            "BIFRACT_API_KEY": "bifract_default_..."
          }
        }
      }
    }
    ```

3. Start Claude Code and ask it to call `get_context` to confirm the instance, fractal and role.

Use the full path to `bifract` if it is not on the client's `PATH`. `bifract --mcp --help` lists the settings below.

## Create an API key

1. Open the fractal and click **Fractal settings** in the sidebar.
2. Open the **Access** tab and click **Create API key**.
3. Pick a **Role**: **Viewer** for read-only tools, **Analyst** for tools that write (alerts, comments, notebooks, dictionaries).
4. Click **Create** and copy the key. It is shown once.

The key's prefix names its scope: `bifract_default_...` belongs to the fractal `default`. See [Authentication](../api/authentication.md) for tenant admin keys.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BIFRACT_URL` | Yes | Base URL of your instance (`http://` or `https://`) |
| `BIFRACT_API_KEY` | Yes | API key; sets the scope and role |
| `BIFRACT_FRACTAL_ID` | No | Fractal to act in. Only a tenant admin key needs it |
| `BIFRACT_PRISM_ID` | No | Prism to act in instead, for reading across fractals. Not with `BIFRACT_FRACTAL_ID` |
| `BIFRACT_CA_CERT` | No | CA bundle for a private or self-signed CA |
| `BIFRACT_CLIENT_CERT` | No | mTLS client certificate (combined PEM, or the cert half) |
| `BIFRACT_CLIENT_KEY` | No | mTLS private key, when the cert file holds only the certificate |
| `BIFRACT_VERIFY_SSL` | No | `false` skips certificate checks. Prefer `BIFRACT_CA_CERT` |
| `BIFRACT_TIMEOUT` | No | Request timeout in seconds (default 60) |

### Choosing the scope

A fractal or prism key already carries its scope, and no argument can widen it.

A tenant admin key (`bifract_admin_...`) has no fractal, so name one with `BIFRACT_FRACTAL_ID`, or pass `fractal_id` to individual tools to work across fractals:

```json
"env": {
  "BIFRACT_URL": "https://bifract.example.com",
  "BIFRACT_API_KEY": "bifract_admin_...",
  "BIFRACT_FRACTAL_ID": "588f9ff8-4fe9-484a-9ca8-2ee77260e0b8"
}
```

Without either, scoped calls are refused rather than sent to a fallback fractal. `list_fractals` and `get_bql_reference` need no scope. Prefer a fractal key where one will do.

### Connecting through mTLS

If Caddy requires client certificates, download one from **Settings > Users** with **Download mTLS Cert** in the user's row menu, then add:

```json
"BIFRACT_CA_CERT": "/etc/bifract/ca.pem",
"BIFRACT_CLIENT_CERT": "/etc/bifract/client.pem",
"BIFRACT_CLIENT_KEY": "/etc/bifract/client-key.pem"
```

## Available Tools

### Orientation

| Tool | Description |
|------|-------------|
| `get_context` | The instance, fractal and role this session is bound to |
| `list_fractals` | Fractals and prisms this key can reach |
| `get_fields` | Field names available to queries, optionally filtered |
| `get_bql_reference` | The full BQL syntax reference |

### Log Querying

Hot storage only; for older data use [Recall](#recall).

| Tool | Description |
|------|-------------|
| `query_logs` | Run a BQL query with an optional time range |
| `validate_bql` | Check a query for syntax errors without running it |
| `get_field_stats` | Per-field coverage, cardinality and top values for a query's matches |
| `get_recent_logs` | Recent logs, to see the event shape and data freshness |

### Provenance

| Tool | Description |
|------|-------------|
| `find_processes` | Find process-creation events and their `process_guid` |
| `get_provenance_graph` | Expand a GUID into a scored process tree with its notable activity |

`get_provenance_graph` runs [`pgr()`](provenance-graph.md). It needs endpoint behavioral analytics enabled and EDR data normalized to `bifract_category` `process_creation`.

```
find_processes(image="rundll32", start="2026-07-26T00:00:00Z")
get_provenance_graph(guid="{390eae98-...}", threshold=0.3, start="2026-07-26T00:00:00Z")
add_comment(log_id="...", text="...", tags=["IR-Rundll32"])
```

### Behavioral Models

| Tool | Description |
|------|-------------|
| `list_models` | Behavioral baselines in this fractal |
| `get_model` | One model's definition and backfill state |
| `get_model_data` | A model's accumulated rows, to check whether an artifact is normal |

### Dashboards

| Tool | Description |
|------|-------------|
| `list_dashboards` | Dashboard summaries |
| `get_dashboard` | A dashboard with every widget and its BQL |

### Alerts

| Tool | Description |
|------|-------------|
| `list_alerts` | All detection alerts in the fractal |
| `get_alert` | Full details of one alert |
| `create_alert` | Create an alert from a BQL query |
| `update_alert` | Change an alert |
| `delete_alert` | Remove an alert |
| `get_alert_executions` | When an alert fired and what it matched |
| `get_alert_tests` | Test cases stored with an alert |
| `run_alert_tests` | Run test cases against a query without saving |
| `get_alert_policies` | Policy rules this scope enforces on a definition |

### Alert governance

Where change review is on, `create_alert`, `update_alert` and `delete_alert` are refused; submit with `propose_alert_change` instead. Check `get_alert_policies` first, since a `block` rule refuses the save. Approving and merging stay with a human reviewer.

| Tool | Description |
|------|-------------|
| `get_alert_history` | How an alert's definition changed, and who changed it |
| `list_alert_changes` | Proposed alert changes awaiting review |
| `propose_alert_change` | Propose a create, update or delete for review |

### Dictionaries

Watchlists and lookup tables that detections join against.

| Tool | Description |
|------|-------------|
| `list_dictionaries` | Dictionaries in the fractal, with key column and row count |
| `get_dictionary` | One dictionary's columns and key |
| `search_dictionary` | Read rows, optionally filtered, e.g. to check an indicator |
| `add_dictionary_rows` | Insert or update rows; changes what live detections match |
| `upload_dictionary_file` | Load a local file into a dictionary, any size |
| `get_dictionary_key_collisions` | Keys that collide when case is ignored |
| `set_dictionary_case_insensitive` | Turn case-insensitive key lookups on or off |

`upload_dictionary_file` reads a file on the machine running the MCP server and streams it in chunks. It reads CSV, TSV, JSON array, NDJSON and one-value-per-line lists, and decompresses `.gz`. Rows are keyed, so re-uploading updates rather than duplicates. Useful arguments:

- `dictionary_name`: load by name; a missing dictionary is created, keyed on the first column.
- `is_global`: make a newly created dictionary visible to every fractal.
- `key_field`: the file's key column, when named differently from the dictionary's.
- `dry_run`: report what would be written without writing.

This tool exists only in the MCP server, not in [AI Chat](ai-chat.md).

### ATT&CK Coverage

| Tool | Description |
|------|-------------|
| `get_attack_coverage` | Techniques the configured detections cover (from their `attack.*` labels), optionally by tactic, and which uncovered ones a synced feed has rules for |

### Recall

[Recall](../administration/backup-restore.md) searches the object-storage archive beyond hot retention. Searches run as jobs because a scan can take minutes.

| Tool | Description |
|------|-------------|
| `search_archive` | Submit a BQL search over the archive for a time window |
| `get_archive_search` | Poll a job and read its rows once done |
| `cancel_archive_search` | Stop a pending or running job |

### Collaboration

| Tool | Description |
|------|-------------|
| `add_comment` | Annotate a log entry |
| `list_comments` | All comments in the fractal |
| `list_comment_tags` | Find the `IR-<Name>` tag of an ongoing investigation |
| `add_tag` | Add a tag to existing comments |
| `remove_tag` | Remove a tag from existing comments |
| `get_log_comments` | Comments on one log entry |
| `list_saved_queries` | Saved BQL queries |

### Notebooks

| Tool | Description |
|------|-------------|
| `list_notebooks` | Notebooks in the fractal |
| `get_notebook` | A notebook and its sections |
| `create_notebook` | Create a notebook |
| `add_notebook_section` | Append a markdown or query section |

### Instruction Libraries

| Tool | Description |
|------|-------------|
| `list_instruction_libraries` | Available instruction libraries |
| `get_instruction_library` | A library and its page structure |
| `read_instruction_page` | One page's content |
| `create_instruction_library` | Create a library |
| `create_instruction_page` | Add a page |
| `update_instruction_page` | Edit a page |

## Example prompts

- "Show me the top 10 source IPs with failed logins in the last hour"
- "What alerts are configured, and which ATT&CK tactics have no coverage?"
- "Find every rundll32 process yesterday and build the provenance graph for the most suspicious one"
- "Add a comment to log abc123 noting this is a confirmed true positive"

## MCP server or AI Chat

| | [AI Chat](ai-chat.md) | MCP server |
|---|---------|------------|
| Runs in | Bifract UI | Your terminal |
| LLM | Server-side, via LiteLLM | Your own client |
| Auth | Your session | API key |
| Writes | Each one needs your approval | Limited by the key's role and your client's prompts |
| Best for | Quick in-app investigation | Deep analysis, scripting, automation |

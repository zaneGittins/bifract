# Exporting Logs

`bifract --export` downloads every log a query matches in one fractal or prism as JSON Lines. It streams from a single ClickHouse scan with no row limit, so a whole fractal goes straight to disk. For a continuous copy of all logs, use the [Iceberg archive](backup-restore.md#iceberg-archive) instead.

You need an API key with the `admin` [role](ingest-tokens.md#roles) on the fractal or prism. Viewer and analyst keys are refused.

## Export with the CLI

1. Set the connection, the same variables the [MCP server](../features/mcp-server.md#environment-variables) uses:

    ```bash
    export BIFRACT_URL=https://bifract.example.com
    export BIFRACT_API_KEY=bifract_...
    ```

2. Run the export:

    ```bash
    bifract --export -o logs.jsonl.gz
    ```

| Option | Default | Description |
|--------|---------|-------------|
| `--output`, `-o` | stdout | File to write. A `.gz` name keeps it compressed. Required when stdout is a terminal |
| `--query`, `-q` | `*` | BQL filter |
| `--start` | `1970-01-01T00:00:00Z` | RFC3339 start |
| `--end` | now | RFC3339 end |

Export one user's October logs with two columns:

```bash
bifract --export -q 'user_name="bob" | table(user_name, rule_name)' \
  --start 2026-10-01T00:00:00Z --end 2026-11-01T00:00:00Z -o bob.jsonl
```

Or pipe to `jq`:

```bash
bifract --export -q 'event_id=4625' | jq -r '.fields.src_ip'
```

The file is written as `<output>.partial` and renamed once the server confirms every row was sent. If the export is interrupted, the partial file is removed and the CLI exits non-zero.

## What can be exported

Only filter queries, optionally ending in `table()`. Aggregations, `sort()`, charts, source commands such as `pgr()`, `join()` and result-set bindings are refused; run those as a search. A prism export whose `tlsh()` filter would skip member fractals is refused too.

## Output format

One log per line, newest first:

```json
{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"d791db93...","fractal_id":"588f9ff8-...","fields":{"user_name":"bob","rule_name":"Rundll32 Execution"}}
```

| Key | Description |
|-----|-------------|
| `timestamp` | Event time, RFC3339 UTC |
| `log_id` | Unique log ID |
| `fractal_id` | Source fractal, useful for prism exports |
| `fields` | The normalized log. With `table()`, the chosen columns appear in its place |

## Export with the API

`POST /api/v1/logs/export/stream` takes the same body as `POST /api/v1/query`. An omitted `start` means the last 24 hours, unlike the CLI.

```bash
curl -sS --compressed -X POST https://bifract.example.com/api/v1/logs/export/stream \
  -H "Authorization: Bearer $BIFRACT_API_KEY" -H 'Content-Type: application/json' \
  -d '{"query":"*","start":"1970-01-01T00:00:00Z"}' -o logs.jsonl
```

- `Accept-Encoding: gzip` (curl `--compressed`) returns a gzip stream.
- A complete response ends with a `Bifract-Export-Rows` trailer holding the row count.
- If the scan fails partway, the server aborts the connection, so curl exits non-zero.

Exports run at low ClickHouse priority so searches stay fast. Each one is logged in the `audit` fractal with `query_type` `export`, its `outcome` (`complete`, `cancelled` or `failed`), the row count, and `prism_id` for prism exports.

# Exporting Logs

`bifract --export` downloads every log a query matches in one fractal or prism as JSON Lines, one log per line. It streams the results from a single ClickHouse scan with no row limit, so a whole fractal of a few GB goes straight to disk. To keep a continuous copy of all log data instead, use the [Iceberg archive](backup-restore.md#iceberg-archive).

Exporting needs an API key with the `admin` [role](ingest-tokens.md#roles) on the fractal or prism. Viewer and analyst keys are refused.

## CLI

The CLI reads its connection from the same environment variables as the [MCP server](../features/mcp-server.md#environment-variables).

```bash
export BIFRACT_URL=https://bifract.example.com
export BIFRACT_API_KEY=bifract_...
bifract --export -o logs.jsonl.gz
```

| Option | Default | Description |
|--------|---------|-------------|
| `--output`, `-o` | stdout | File to write. A `.gz` name keeps the download compressed. |
| `--query`, `-q` | `*` | BQL filter |
| `--start` | `1970-01-01T00:00:00Z` | RFC3339 start of the time range |
| `--end` | now | RFC3339 end of the time range |

Export one user's logs for October, with only two columns:

```bash
bifract --export -q 'user_name="bob" | table(user_name, rule_name)' \
  --start 2026-10-01T00:00:00Z --end 2026-11-01T00:00:00Z -o bob.jsonl
```

Pipe to `jq` instead of writing a file:

```bash
bifract --export -q 'event_id=4625' | jq -r '.fields.src_ip'
```

The file is written as `<output>.partial` and renamed once the server confirms it sent every row. If the export is interrupted, the CLI removes the partial file and exits non-zero.

## Output format

Each line is one log, newest first:

```json
{"timestamp":"2026-10-12T08:30:00.123Z","log_id":"d791db93...","fractal_id":"588f9ff8-...","fields":{"user_name":"bob","rule_name":"Rundll32 Execution"}}
```

| Key | Description |
|-----|-------------|
| `timestamp` | Event time, RFC3339 in UTC |
| `log_id` | Unique log id |
| `fractal_id` | Fractal the log belongs to, which matters when exporting a prism |
| `fields` | The normalized log |

A query that picks columns with `table()` writes those columns in place of `fields`.

Only filter queries can be exported. Aggregations, `sort()`, charts, source commands such as `pgr()`, `join()` and result-set bindings are refused, since their results are computed or capped rather than read in full; run those as a search. A prism export whose `tlsh()` filter would skip member fractals is refused too.

## API

`POST /api/v1/logs/export/stream` takes the same body as `POST /api/v1/query`. Unlike the CLI, an omitted `start` means the last 24 hours.

```bash
curl -sS --compressed -X POST https://bifract.example.com/api/v1/logs/export/stream \
  -H "Authorization: Bearer $BIFRACT_API_KEY" -H 'Content-Type: application/json' \
  -d '{"query":"*","start":"1970-01-01T00:00:00Z"}' -o logs.jsonl
```

- Send `Accept-Encoding: gzip` (curl's `--compressed`) to receive a gzip stream.
- A complete response ends with a `Bifract-Export-Rows` HTTP trailer holding the row count.
- If the scan fails partway, the server aborts the connection instead of ending the response normally, so curl exits non-zero.

Exports run at low ClickHouse priority so they yield to interactive searches. Each one, finished or not, is written to the `audit` fractal with `query_type` `export`, its `outcome` (`complete`, `cancelled` or `failed`), the rows sent, and the `prism_id` for a prism export.

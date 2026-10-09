# Ingest CLI

The `bifract --ingest` command provides bulk log ingestion with batching, parallelism, and retry logic. By default it auto-detects workers and batch size, and adapts concurrency at runtime based on server feedback.

## Installation

Install bifract from the [releases page](https://github.com/zaneGittins/bifract/releases) or build from source:

```bash
go build -o bifract ./cmd/bifract
```

## Usage

```bash
# Basic usage (auto mode) - note port 8443 for ingest
bifract --ingest --url https://bifract.example.com:8443 --token bifract_ingest_abc123... file.json

# Multiple files
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN *.json

# Recursive directory ingestion
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN \
  --recursive /var/log/exports/

# Override auto-detected parameters
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN \
  --workers 8 --batch-size 2000 *.json
```

## Supported File Formats

- JSON (arrays or single objects)
- NDJSON (newline-delimited JSON)
- CSV
- TSV
- Parquet (detected by `.parquet` extension or file header)

Parquet rows are sent as JSON objects, so normalizers and timestamp detection apply as usual. Nested structs, lists and maps become nested JSON. Values are converted so nothing is lost on the way in:

| Parquet type | Sent as |
|------|---------|
| `TIMESTAMP`, `INT96` | RFC 3339 string in UTC |
| `DATE`, `TIME` | `2006-01-02`, `15:04:05.999999999` |
| `DECIMAL` | Exact decimal string |
| Integers beyond ±2^53 | Decimal string (JSON numbers lose precision there) |
| `UUID` | Canonical UUID string |
| Binary that is not valid UTF-8 | Hex string |
| `NaN`, `±Inf` | `"NaN"`, `"+Inf"`, `"-Inf"` |

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--url`, `-u` | `http://localhost:8443` | Bifract server URL |
| `--token`, `-t` | (required) | Ingest token |
| `--batch-size`, `-b` | auto (5000) | Logs per batch |
| `--workers`, `-w` | auto (CPU cores) | Concurrent upload workers |
| `--limit`, `-l` | unlimited | Max logs per file |
| `--recursive`, `-r` | false | Recursively find files in subdirectories |
| `--insecure`, `-k` | false | Skip TLS certificate verification |

When `--workers` and `--batch-size` are omitted, bifract runs in **auto mode**: it detects initial parameters from system resources and adapts concurrency at runtime based on server feedback (429 responses). Providing either flag switches to manual mode with fixed values.

The CLI automatically retries on `429` (rate limit / queue full) and `5xx` errors with exponential backoff (up to 5 retries).

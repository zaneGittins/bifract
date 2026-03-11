# Ingest CLI

The `bifractctl --ingest` command provides bulk log ingestion with batching, parallelism, and retry logic. By default it auto-detects workers and batch size, and adapts concurrency at runtime based on server feedback.

## Installation

Install bifractctl from the [releases page](https://github.com/zaneGittins/bifract/releases) or build from source:

```bash
go build -o bifractctl ./cmd/bifractctl
```

## Usage

```bash
# Basic usage (auto mode) - note port 8443 for ingest
bifractctl --ingest --url https://bifract.example.com:8443 --token bifract_ingest_abc123... file.json

# Multiple files
bifractctl --ingest --url https://bifract.example.com:8443 --token $TOKEN *.json

# Recursive directory ingestion
bifractctl --ingest --url https://bifract.example.com:8443 --token $TOKEN \
  --recursive /var/log/exports/

# Override auto-detected parameters
bifractctl --ingest --url https://bifract.example.com:8443 --token $TOKEN \
  --workers 8 --batch-size 2000 *.json
```

## Supported File Formats

- JSON (arrays or single objects)
- NDJSON (newline-delimited JSON)
- CSV
- TSV

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--url`, `-u` | `http://localhost:8080` | Bifract server URL |
| `--token`, `-t` | (required) | Ingest token |
| `--batch-size`, `-b` | auto (5000) | Logs per batch |
| `--workers`, `-w` | auto (CPU cores) | Concurrent upload workers |
| `--limit`, `-l` | unlimited | Max logs per file |
| `--recursive`, `-r` | false | Recursively find files in subdirectories |
| `--insecure`, `-k` | false | Skip TLS certificate verification |

When `--workers` and `--batch-size` are omitted, bifractctl runs in **auto mode**: it detects initial parameters from system resources and adapts concurrency at runtime based on server feedback (429 responses). Providing either flag switches to manual mode with fixed values.

The CLI automatically retries on `429` (rate limit / queue full) and `5xx` errors with exponential backoff (up to 5 retries).

# Ingest CLI

`bifract --ingest` uploads log files in parallel batches with automatic retries. Use it for one-off imports and backfills; use a log shipper for continuous collection.

## Install

Download `bifract` from the [releases page](https://github.com/zaneGittins/bifract/releases), or build it:

```bash
go build -o bifract ./cmd/bifract
```

## Upload files

1. Copy an ingest token from the fractal's **Ingest** page (see [Sending Logs](overview.md#get-an-ingest-token)).
2. Run the upload against the ingest port (8443):

```bash
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN file.json
```

More examples:

```bash
# Several files
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN *.json

# A directory tree
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN -r /var/log/exports/

# Fixed concurrency instead of auto mode
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN \
  --workers 8 --batch-size 2000 *.json

# Self-signed certificate (the installer's default)
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN -k file.json
```

## Load an Exchange dataset

`dataset:<id>` downloads a dataset from the [Exchange](../exchange/using.md#datasets), checks its size, SHA-256 and log count, and ingests it into the fractal the token points at:

```bash
bifract --ingest --url https://bifract.example.com:8443 --token $TOKEN dataset:bifract-labs-001
```

The verified file is kept in your user cache directory (`bifract/datasets`), so running it again skips the download. Use a token with no normalizer: datasets are already normalized. `--exchange` reads the index from another URL, as `BIFRACT_EXCHANGE_URL` does for the server.

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--url`, `-u` | `https://localhost:8443` | Bifract server URL |
| `--token`, `-t` | (required) | Ingest token |
| `--batch-size`, `-b` | auto (5000) | Logs per batch |
| `--workers`, `-w` | auto (CPU cores, 2 to 32) | Concurrent upload workers |
| `--limit`, `-l` | unlimited | Max logs per file |
| `--recursive`, `-r` | off | Recurse into directories |
| `--exchange` | `BIFRACT_EXCHANGE_URL`, else the Bifract repository | Exchange index that `dataset:<id>` is read from |
| `--insecure`, `-k` | off | Skip TLS certificate verification |
| `--help`, `-h` | | Show help |

**Auto mode** is on when you pass neither `--workers` nor `--batch-size`: the CLI starts from your CPU count and lowers or raises concurrency based on `429` responses. Passing either flag fixes both values.

**Retries:** a `429` is retried, honoring `Retry-After`, for up to 15 minutes per batch. `5xx` and connection errors are retried 5 times with exponential backoff. A `413` fails the batch; lower `--batch-size`. Payloads over 4 KB are gzip-compressed.

## Supported files

| Format | Detected by |
|---|---|
| JSON array, single JSON object, NDJSON | File content |
| CSV, TSV | `.csv`, `.tsv` extension |
| Parquet | `.parquet` extension or the `PAR1` file header |
| zstd-compressed NDJSON | The zstd file header |

With `-r`, only files ending in `.json`, `.ndjson`, `.jsonl`, `.csv`, `.tsv`, `.parquet`, `.jsonl.zst` or `.ndjson.zst` are picked up.

Each log gets a `bifract_ingest_path` field naming the file it came from, except NDJSON lines in the shape an export of logs writes (only `timestamp`, `log_id`, `fractal_id` and `fields`): those go back in as the logs they were, with their original timestamp.

Parquet rows are sent as JSON objects, so normalizers and timestamp detection apply. Nested structs, lists and maps become nested JSON. Values convert without loss:

| Parquet type | Sent as |
|------|---------|
| `TIMESTAMP`, `INT96` | RFC 3339 string in UTC |
| `DATE`, `TIME` | `2006-01-02`, `15:04:05.999999999` |
| `DECIMAL` | Exact decimal string |
| Integers beyond 2^53 (either sign) | Decimal string |
| `UUID` | Canonical UUID string |
| Binary that is not valid UTF-8 | Hex string |
| `NaN`, `+Inf`, `-Inf` | `"NaN"`, `"+Inf"`, `"-Inf"` |

# API test suite

These tests talk to a running Bifract over HTTP. Everything else in the
repository checks that the API *description* matches the router; this checks
that the API *behaves*.

The recipes double as the worked examples on the docs site, so they are written
to be read: the client hides transport only, and every call shows its method,
path and body.

## Running

```
BIFRACT_API_KEY=bifract_... go test -tags integration ./test/integration/ -v
```

| Variable | Default | Meaning |
|----------|---------|---------|
| `BIFRACT_API_KEY` | none | Required. A tenant-admin key: the recipes create and delete fractals. |
| `BIFRACT_API_URL` | `http://localhost:8080` | The instance under test. |
| `BIFRACT_FRACTAL_ID` | first reachable | The fractal to work in, where a recipe does not create its own. |

Without `BIFRACT_API_KEY` the tests skip rather than fail: they need a real
server, and refusing to run is the honest outcome.

Mint a key under **Admin > API Keys > New key**, with **Tenant admin** ticked.

## What each recipe covers

| Test | Covers |
|------|--------|
| `TestIngestAndQuery` | Create a fractal, mint an ingest token, send logs, read them back |
| `TestDetectionLifecycle` | Validate a query, create a rule disabled, enable it, read its executions |
| `TestNotebookPortability` | Build a notebook, export it as YAML, import it back |
| `TestInvestigation` | Record a finding against a log, tag it into a case, find it again |
| `TestOperations` | Topology, ingest pressure, fractal statistics, retention |
| `TestContractErrors` | The failure envelope: status and machine-readable code |
| `TestContractPagination` | The page envelope, and that limit and offset move the window |

## Notes

Recipes create what they need and clean up after themselves, so the suite is
safe to run against a development instance. It is not safe against production:
it creates and deletes fractals.

Ingestion is asynchronous and batched, so a recipe that writes and reads back
waits for the queue rather than assuming. `TestIngestAndQuery` takes about
30 seconds for that reason; the rest of the suite finishes in under three.

`TestInvestigation` needs a fractal with logs in it and skips otherwise, so set
`BIFRACT_FRACTAL_ID` to one that has data.

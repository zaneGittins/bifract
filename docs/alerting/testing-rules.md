# Testing Detection Rules

`bifract --test` runs a detection rule against sample events and reports whether it fired as expected. Use it while writing a rule, or in CI to catch regressions.

It handles both rule formats:

* **Sigma rules**, translated to BQL exactly as [Alert Feeds](alert-feeds.md) does.
* **Native Bifract alerts**, whose `queryString` is BQL, so threshold rules such as `| groupBy(image, function=count()) | _count >= 3` are testable too.

For quick checks inside the UI, the alert editor's **Tests** tab does the same for a single alert.

## How it works

The tester runs the production pipeline, not an approximation:

1. The rule is turned into BQL.
2. Sample events go through the same normalization as HTTP ingestion.
3. They are inserted into a scratch copy of the `logs` table, one synthetic fractal per case.
4. The real generated SQL runs against it. Rows returned means the rule fired.
5. The scratch table is dropped.

This needs a ClickHouse. Nothing is written to the real `logs` table, so pointing it at a running deployment is safe.

## Quick start

```bash
# Run every test under a directory. Starts a throwaway ClickHouse container if none is given.
bifract --test ./detections/

# Check one rule against one file of events, without a test file.
bifract --test detections/certutil-download/rule.yml --logs captured.json --expect match

# Check that every rule translates and parses. No ClickHouse needed.
bifract --test --lint ./detections/
```

| Exit code | Meaning |
|---|---|
| `0` | All tests passed |
| `1` | One or more tests failed |
| `2` | Could not run (bad spec, unreachable ClickHouse, ...) |

## Lay out a detection repo

Give each detection a folder with its rule, its test file and its sample events:

```text
detections/
├── normalizers/
│   └── sysmon.yaml                 # exported from Normalizers
├── certutil-download/
│   ├── rule.yml                    # Sigma rule or Bifract alert
│   ├── rule.test.yaml              # which files must fire, which must not
│   ├── true-positives.json
│   └── benign.json
└── lsass-access-burst/
    ├── rule.yml
    ├── rule.test.yaml
    ├── three-accesses-one-process.json
    └── two-accesses-below-threshold.json
```

- `--test <dir>` walks the tree and runs every file ending in `.test.yaml` or `.test.yml`.
- Paths inside a test file are relative to that file, so a folder can be moved without edits.
- `internal/ruletest/testdata/detections/` in the Bifract repository uses this layout and is a working starting point.

Event files hold only raw events, with nothing test-specific in them. The format is detected from the contents, so `.json` works for all three:

* a JSON array of events
* a single JSON object
* NDJSON, one event per line

## Write a test file

```yaml
rule: rule.yml
normalizer: ../normalizers/sysmon.yaml

cases:
  # Every event in the file must fire.
  - name: all known download variants fire
    expect: match
    logFile: true-positives.json

  # No event in the file may fire.
  - name: no benign certutil usage fires
    expect: no_match
    logFile: benign.json
```

| Field | Meaning |
|---|---|
| `rule` | Path to the Sigma rule or Bifract alert YAML. Required. |
| `normalizer` | Normalizer applied to the events. Optional, but almost always needed. |
| `cases[].name` | Case name, shown in output and CI reports. Required. |
| `cases[].expect` | `match` or `no_match`. Required. |
| `cases[].logFile` | Events file. Required. |
| `cases[].together` | Judge all events as one batch. Needed for threshold rules. |
| `cases[].count` | Exact number of result rows expected. Requires `together: true` and `expect: match`. |

### Each event is judged on its own

A case passes only if **every** event meets the expectation: with `expect: match` all must fire, with `expect: no_match` none may. Use one file per verdict. A failure names the event that broke:

```text
  PASS every known true positive fires (4/4 logs matched)
  FAIL every known true positive fires
       log 2: expected the rule to trigger, but it returned no rows
```

### Threshold rules: `together: true`

A rule that counts several events never fires on one event at a time. `together: true` runs the rule once over the whole file:

```yaml
  - name: three accesses from one process trip the threshold
    expect: match
    together: true
    logFile: three-accesses-one-process.json

  - name: two accesses stay below the threshold
    expect: no_match
    together: true
    logFile: two-accesses-below-threshold.json
```

### Use the right normalizer

Sigma rules match raw vendor names (`Image`, `CommandLine`); Bifract stores normalized ones (`image`, `commandline`). The normalizer maps between them and sets `bifract_category`, which scopes a rule to an event type. With the wrong normalizer a good rule reports `no_match`.

Export the normalizer you ingest with from **Normalizers** (row menu, **Export**) and commit it next to your rules. For an ad hoc `--logs` run, pass it with `--normalizer`.

## Debug a rule that will not fire

```bash
bifract --test ./detections/ --explain
```

For each failing case, `--explain` prints the generated BQL, the SQL, and the field names normalization produced. A Sigma rule that does not fire is nearly always looking for a field the normalizer did not produce.

## Unsupported rules

Tests of rules using `pgr()`, `modelLookup()`, `join()` or `tlsh()` fail with an error instead of reporting `no_match`. They read tables that only a live deployment populates, so the tester cannot judge them. `--lint` still validates such rules.

## Flags

| Flag | Meaning |
|---|---|
| `-n`, `--normalizer <file>` | Normalizer for specs that do not name one, and for ad hoc runs |
| `-c`, `--clickhouse <host:port>` | ClickHouse to use. Default: start a throwaway container (needs Docker) |
| `--ch-user <user>` | ClickHouse user (default `default`) |
| `--ch-password <pass>` | ClickHouse password (default `bifract`) |
| `--logs <file>` | Ad hoc events file |
| `--expect <verdict>` | Ad hoc expectation, `match` (default) or `no_match` |
| `-f`, `--format <fmt>` | `text` (default), `junit` (rendered by GitHub and GitLab) or `json` |
| `--explain` | On failure, print BQL, SQL and normalized fields |
| `--lint` | Validate rules only; no events are run |
| `-v`, `--verbose` | Verbose output |

`BIFRACT_TEST_CLICKHOUSE`, `BIFRACT_TEST_CH_USER` and `BIFRACT_TEST_CH_PASSWORD` set the same values as `--clickhouse`, `--ch-user` and `--ch-password`.

---

## CI/CD setup

Run a ClickHouse service next to the job and point `--test` at it. Use the version your deployment runs: `clickhouse/clickhouse-server:26.8.34.13-alpine` in the shipped `docker-compose.yml`.

The tester creates its own schema and waits for ClickHouse to accept queries, so the service needs no setup and the health check below is optional.

Recommended shape:

1. **`--lint` on every push.** Seconds, no database.
2. **Full `--test` on pull requests** to gate merges.
3. **Treat exit codes differently.** `1` is a detection regression; `2` means the harness could not run.

### GitHub Actions

```yaml
name: Detections

on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Get bifract
        run: |
          curl -fsSL -o bifract \
            https://github.com/zaneGittins/bifract/releases/latest/download/bifract-linux-amd64
          chmod +x bifract
      - run: ./bifract --test --lint ./detections/

  test:
    runs-on: ubuntu-latest
    needs: lint
    services:
      clickhouse:
        image: clickhouse/clickhouse-server:26.8.34.13-alpine
        ports: ['9000:9000']
        env:
          CLICKHOUSE_USER: default
          CLICKHOUSE_PASSWORD: bifract
        options: >-
          --health-cmd "clickhouse-client --password bifract --query 'SELECT 1'"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 12
    steps:
      - uses: actions/checkout@v4
      - name: Get bifract
        run: |
          curl -fsSL -o bifract \
            https://github.com/zaneGittins/bifract/releases/latest/download/bifract-linux-amd64
          chmod +x bifract

      - name: Run detection tests
        run: ./bifract --test ./detections/ --clickhouse localhost:9000 --format junit > results.xml

      - name: Publish results
        uses: mikepenz/action-junit-report@v4
        if: always()
        with:
          report_paths: results.xml
```

### GitLab CI

GitLab reaches services by their `alias`, so use `clickhouse:9000` rather than `localhost:9000`.

```yaml
stages: [lint, test]

.get-bifract: &get-bifract
  - curl -fsSL -o bifract
      https://github.com/zaneGittins/bifract/releases/latest/download/bifract-linux-amd64
  - chmod +x bifract

lint-rules:
  stage: lint
  image: alpine:3.20
  before_script:
    - apk add --no-cache curl
    - *get-bifract
  script:
    - ./bifract --test --lint ./detections/

test-detections:
  stage: test
  image: alpine:3.20
  services:
    - name: clickhouse/clickhouse-server:26.8.34.13-alpine
      alias: clickhouse
  variables:
    CLICKHOUSE_USER: default
    CLICKHOUSE_PASSWORD: bifract
  before_script:
    - apk add --no-cache curl
    - *get-bifract
  script:
    - ./bifract --test ./detections/ --clickhouse clickhouse:9000 --format junit > results.xml
  artifacts:
    when: always
    reports:
      junit: results.xml
```

### Without a service container

Omit `--clickhouse` and the tester starts and removes its own container. This needs Docker in the job and is slower:

```yaml
- run: ./bifract --test ./detections/
```

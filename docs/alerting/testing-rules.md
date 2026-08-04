# Testing Detection Rules

`bifract --test` runs a detection rule against sample events and tells you whether it fires.

You give it a rule, some events, and the verdict you expect. It reports pass or fail. Use it
while writing a rule to check your logic against a captured event, or wire it into a pipeline
to catch regressions; see [CI/CD setup](#cicd-setup) for the latter.

It works on both rule formats Bifract understands:

* **Sigma rules**, translated to BQL exactly as [Alert Feeds](alert-feeds.md) translates them.
* **Native Bifract alerts**, whose `queryString` is BQL and can therefore aggregate. Threshold
  rules such as `| groupBy(image, function=count()) | _count >= 3` are testable too.

## How it works

The tester does not approximate matching. It runs the same pipeline as production:

1. Your rule is lowered to BQL (`sigma.Translate` for Sigma, `queryString` for native alerts).
2. Your sample events go through the same normalization code path as HTTP ingestion.
3. They are inserted into a scratch clone of the `logs` table.
4. The real generated ClickHouse SQL runs against it. Rows returned means the rule fired.
5. The scratch table is dropped.

Because step 4 executes the actual query, a passing test means the rule genuinely fires. That
is also why a ClickHouse is required: there is no second matching engine that could drift from
the real one.

Nothing is written to the real `logs` table, so it is safe to point the tester at a running
deployment. Each test case is additionally scoped to its own synthetic fractal, so cases cannot
contaminate one another's results, and a threshold rule counts only its own case's events.

## Quick start

```bash
# Run every test in a directory. Starts a throwaway ClickHouse if none is given.
bifract --test ./detections/

# Check one rule against one file of events, without a test file.
bifract --test detections/certutil-download/rule.yml --logs captured.json --expect match

# Validate that every rule translates and parses. No ClickHouse needed.
bifract --test --lint ./detections/
```

Exit codes: `0` all passed, `1` one or more failed, `2` could not run.

## Laying out a detection repo

Give each detection a folder holding its rule, its test, and its sample events:

```
detections/
├── normalizers/
│   └── sysmon.yaml                 # exported from Settings -> Normalizers
├── certutil-download/
│   ├── rule.yml                    # the Sigma rule or Bifract alert
│   ├── rule.test.yaml              # which file must fire, which must not
│   ├── true-positives.json         # raw events, nothing test-specific in them
│   └── benign.json
└── lsass-access-burst/
    ├── rule.yml
    ├── rule.test.yaml
    ├── three-accesses-one-process.json
    └── two-accesses-below-threshold.json
```

Adding a detection is adding a folder. `example-detections/` in the Bifract repo is laid out
this way and is a working starting point.

### How files are found

You never name test files on the command line. Point `--test` at a directory and it walks the
whole tree, running every file whose name ends in `.test.yaml` (or `.test.yml`):

```bash
bifract --test ./detections/     # finds both rule.test.yaml files above
```

Each spec then points at its own rule, normalizer and event files by **path relative to the
spec itself**, which is why a detection folder is self-contained and can be moved or vendored
without edits.

The nesting is up to you: discovery is a recursive suffix match, so grouping detections by
platform, team or ATT&CK tactic works just as well.

### The event files hold only events

Events always come from a file, never inline in the spec. A file is raw captured telemetry
with nothing test-specific in it, so you can drop an export straight in, and it stays
reviewable and reusable. The expected verdict lives in the `.test.yaml` beside it:

```yaml
cases:
  - {name: real attacks, expect: match,    logFile: true-positives.json}
  - {name: benign usage, expect: no_match, logFile: benign.json}
```

Three shapes are accepted, auto-detected from the contents rather than the extension, so
`.json` is fine for all of them:

* a JSON array of event objects
* a single JSON object
* NDJSON, one event object per line

## Test file format

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
| `normalizer` | Normalizer applied to this spec's events. Optional but almost always wanted. |
| `cases[].name` | Case name, shown in output and in CI reports. Required. |
| `cases[].expect` | `match` or `no_match`. Required. |
| `cases[].logFile` | Events file: JSON array, single JSON object, or NDJSON. Required. |
| `cases[].together` | Judge the events as one batch. Needed by threshold rules. |
| `cases[].count` | Assert an exact number of result rows. Requires `together`. |

## A case is all-or-nothing

Every event in a case is judged **on its own**, and the case passes only if all of them meet
the expectation. A file of ten events with `expect: match` means all ten must fire; one with
`expect: no_match` means none of them may. So the usual shape is one file per verdict and a
case per file, as above.

Output reports the ratio, and a failure names the event that broke:

```
  PASS every known true positive fires (4/4 logs matched)
  FAIL every known true positive fires
       log 2: expected the rule to trigger, but it returned no rows
```

Add a newly discovered false positive to the benign file, and you will know the moment a
rule change starts flagging it.

## Threshold rules: `together: true`

Some rules only fire when they see several events at once. The LSASS example counts three
accesses from the same process, so judging each access alone would never reach the
threshold.

`together: true` presents the file's events to the rule as a single batch instead:

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

With `together`, the rule runs once over the whole file, so `expect: match` means the set as
a whole triggered. `count` describes that single run's row count and therefore requires
`together`; using it without is rejected when the spec loads.

## Always pass the right normalizer

Sigma rules match on raw vendor field names (`Image`, `CommandLine`), while Bifract stores
normalized ones (`image`, `commandline`). The normalizer bridges the two, and it is also what
sets `bifract_category`, which scopes a rule to an event type.

Test with the wrong normalizer and a perfectly good rule reports `no_match`. Export the
normalizer you actually ingest with from **Settings -> Normalizers -> Export** and commit it
next to your rules.

## Debugging a rule that will not fire

```bash
bifract --test ./detections/ --explain
```

`--explain` prints, for each failing case, the generated BQL, the generated SQL, and the field
names normalization actually produced. A Sigma rule that unexpectedly does not fire is nearly
always looking for a field name the normalizer did not produce, and the field dump shows that
immediately.

## Unsupported rules

Rules using `pgr()`, `ptg()`, `model_lookup()` or `join()` are **rejected with an error**
rather than reported as "no match". They read tables built by materialized views on the live
`logs` table or data that only exists in a real deployment, so the tester cannot judge them
honestly. Failing loudly is deliberate: a silent false negative in a detection gate is worse
than no gate at all.

---

# CI/CD setup

The pattern is the same everywhere: run a ClickHouse alongside the job and point `--test` at
it. Startup is a one-time cost of roughly ten seconds; after that thousands of cases are one
insert and one query each.

Use the same ClickHouse version your deployment runs. That is
`clickhouse/clickhouse-server:26.6.2.81-alpine` in the shipped `docker-compose.yml`.

## GitHub Actions

```yaml
name: Detections

on: [push, pull_request]

jobs:
  lint:
    # Cheap gate: proves every rule translates and parses. No database.
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
        image: clickhouse/clickhouse-server:26.6.2.81-alpine
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

Service containers are Linux-only on GitHub Actions. The tester provisions the Bifract schema
itself, so the service needs no initialization, and it waits for ClickHouse to accept queries
before running, so the `--health-cmd` options above are belt-and-braces rather than required.

## GitLab CI

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
    - name: clickhouse/clickhouse-server:26.6.2.81-alpine
      alias: clickhouse
  variables:
    CLICKHOUSE_USER: default
    CLICKHOUSE_PASSWORD: bifract
  before_script:
    - apk add --no-cache curl
    - *get-bifract
  script:
    # GitLab reaches services by their alias hostname, not localhost.
    - ./bifract --test ./detections/ --clickhouse clickhouse:9000 --format junit > results.xml
  artifacts:
    when: always
    reports:
      junit: results.xml
```

The one difference from GitHub Actions: GitLab services are reachable by their `alias`
hostname, so use `clickhouse:9000` rather than `localhost:9000`.

## Without a service container

If `--clickhouse` is omitted the tester starts its own container and removes it afterwards.
This needs Docker in the job, and is slower because the database boots serially rather than
alongside checkout:

```yaml
- run: ./bifract --test ./detections/
```

## Configuring via environment

Useful when the endpoint differs per environment and you do not want it in the command:

| Variable | Equivalent flag |
|---|---|
| `BIFRACT_TEST_CLICKHOUSE` | `--clickhouse` |
| `BIFRACT_TEST_CH_USER` | `--ch-user` |
| `BIFRACT_TEST_CH_PASSWORD` | `--ch-password` |

## Output formats

| `--format` | Use |
|---|---|
| `text` | Default. Human-readable, coloured. |
| `junit` | JUnit XML. Both GitHub and GitLab render per-case results from it. |
| `json` | Full structured results for custom tooling. |

## Recommended pipeline shape

1. **`--lint` on every push.** Seconds, no database, catches malformed rules and bad field
   references early.
2. **Full `--test` on pull requests.** Requires the ClickHouse service; gates merges.
3. **Fail the build on a non-zero exit.** Exit `1` is a detection regression, exit `2` means the
   harness itself could not run, so the two are worth alerting on differently.

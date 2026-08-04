# Testing Detection Rules

`bifract --test` runs a detection rule against sample logs and checks whether it fires.
Commit your rules, your sample logs and the verdict you expect, and a broken detection
fails the build instead of quietly going dark in production.

It works on both rule formats Bifract understands:

* **Sigma rules**, translated to BQL exactly as [Alert Feeds](alert-feeds.md) translates them.
* **Native Bifract alerts**, whose `queryString` is BQL and can therefore aggregate. Threshold
  rules such as `| groupBy(image, function=count()) | _count >= 3` are testable too.

## How it works

The tester does not approximate matching. It runs the same pipeline as production:

1. Your rule is lowered to BQL (`sigma.Translate` for Sigma, `queryString` for native alerts).
2. Your sample logs go through the same normalization code path as HTTP ingestion.
3. The logs are inserted into a scratch clone of the `logs` table.
4. The real generated ClickHouse SQL runs against it. Rows returned means the rule fired.
5. The scratch table is dropped.

Because step 4 executes the actual query, a passing test means the rule genuinely fires. That
is also why a ClickHouse is required: there is no second matching engine that could drift from
the real one.

Nothing is written to the real `logs` table, so it is safe to point the tester at a running
deployment. Each test case is additionally scoped to its own synthetic fractal, so cases cannot
contaminate one another's results, and a threshold rule counts only its own case's logs.

## Quick start

```bash
# Run every test in a directory. Starts a throwaway ClickHouse if none is given.
bifract --test ./detections/

# Check one rule against one file of logs.
bifract --test rules/certutil.yml --logs samples/evt.json --expect match

# Validate that every rule translates and parses. No ClickHouse needed.
bifract --test --lint ./detections/
```

Exit codes: `0` all passed, `1` one or more failed, `2` could not run.

## Laying out a detection repo

```
detections/
  normalizers/
    sysmon.yaml                  # exported from Settings -> Normalizers
  rules/
    certutil-download.yml
  tests/
    certutil-download.test.yaml
    samples/
      certutil-verifyctl.json
```

Any file ending in `.test.yaml` is a test. Paths inside it resolve relative to the test file,
so the tree can be moved or vendored wholesale.

## Test file format

```yaml
rule: ../rules/certutil-download.yml
normalizer: ../normalizers/sysmon.yaml

cases:
  - name: certutil urlcache download
    expect: match
    log:
      EventID: 1
      Image: C:\Windows\System32\certutil.exe
      CommandLine: certutil.exe -urlcache -split -f http://198.51.100.20/a.exe a.exe

  - name: benign certutil encode
    expect: no_match
    log:
      EventID: 1
      Image: C:\Windows\System32\certutil.exe
      CommandLine: certutil.exe -encode payload.bin payload.b64

  - name: from a sample file
    expect: match
    logFile: samples/certutil-verifyctl.json

  - name: three accesses reach the threshold
    expect: match
    count: 1
    logs:
      - {EventID: 10, SourceImage: C:\a.exe, TargetImage: C:\Windows\System32\lsass.exe}
      - {EventID: 10, SourceImage: C:\a.exe, TargetImage: C:\Windows\System32\lsass.exe}
      - {EventID: 10, SourceImage: C:\a.exe, TargetImage: C:\Windows\System32\lsass.exe}
```

| Field | Meaning |
|---|---|
| `rule` | Path to the Sigma rule or Bifract alert YAML. Required. |
| `normalizer` | Normalizer applied to this spec's logs. Optional but almost always wanted. |
| `cases[].name` | Case name, shown in output and in CI reports. Required. |
| `cases[].expect` | `match` or `no_match`. Required. |
| `cases[].log` | One log, inline. |
| `cases[].logs` | Several logs, inline. Use for threshold rules. |
| `cases[].logFile` | Logs from a file: JSON array, single JSON object, or NDJSON. |
| `cases[].each` | Evaluate every log independently. See below. |
| `cases[].count` | Assert an exact number of result rows rather than just "any". |

Set exactly one of `log`, `logs` or `logFile` per case.

## Corpora: `each: true`

By default a case presents all its logs to the rule **together**, as one batch, and the
case matches if the rule returns any row. That is what threshold rules need: three LSASS
accesses only cross the threshold when the rule sees all three at once.

It is the wrong semantics for a corpus. Given twenty true positives in one case,
`expect: match` would pass as soon as *one* of them fired, hiding the other nineteen.

`each: true` evaluates every log on its own, in its own fractal, and the case passes only
if every log meets the expectation:

```yaml
  - name: every known true positive fires
    expect: match
    each: true
    logFile: samples/certutil-true-positives.ndjson

  - name: no benign certutil usage fires
    expect: no_match
    each: true
    logFile: samples/certutil-benign.ndjson
```

Output reports the ratio, and failures name the offending log:

```
  PASS every known true positive fires (4/4 logs matched)
  FAIL every known true positive fires
       log 2: expected the rule to trigger, but it returned no rows
```

This is the pattern to reach for when curating detections: one file of attacks that must
fire, one file of benign activity that must not, both asserted log by log. Add a new false
positive to the benign file and the build tells you the moment a rule change starts
flagging it.

Do not use `each` with threshold rules, whose logs must be seen together. It is also
mutually exclusive with `count`, which is rejected at load time.

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
itself, so the service needs no initialization.

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

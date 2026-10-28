# Bifract Exchange

Content every Bifract install can browse and add from **Exchange** in the UI. The server
reads `index.yaml` from this directory on GitHub; item files are fetched relative to it.

## Layout

- `index.yaml` - every item the UI lists.
- `detections/<name>/` - `rule.yml` (Sigma or native Bifract YAML), `rule.test.yaml` and
  sample logs. CI lints the rules; run their tests against a ClickHouse with
  `BIFRACT_TEST_CLICKHOUSE=localhost:9000 go test -tags ruletestch ./internal/ruletest/`.
- `normalizers/` - normalizers in the shape the UI exports.
- `policies/` - alert policy documents.

Installs read this index from `main`, and skip items they do not understand. Add new
fields or kinds freely, but do not change what an existing field means.

Lookups have no file here. Their entry holds the source settings, and installs fetch the
rows from upstream on a schedule.

## Adding an item

1. Add the file, then an entry in `index.yaml`:

    ```yaml
    - id: my-rule            # lowercase, digits, - and _; a lookup's id is its name, so only _
      kind: detection        # lookup, detection, normalizer or policy
      name: My Rule          # for a normalizer, must equal the name in its file
      description: One line on what it does.
      tags: [windows]
      path: detections/my-rule/rule.yml
      normalizer: sysmon     # Sigma detections: the normalizer item its fields map through
    ```

2. Run `go test ./pkg/exchange` (checks every file is listed and every entry resolves) and
   `go run ./cmd/bifract --test --lint ./exchange/detections/`.
3. Open a pull request.

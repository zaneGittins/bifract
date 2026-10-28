# Bifract Exchange

Content every Bifract install can browse and add from **Exchange** in the UI, and that
the docs list under Exchange. The server reads `index.yaml` from this directory on
GitHub; item files are fetched relative to it.

## Layout

- `index.yaml` - every item the UI lists, including bundles.
- `detections/` - alert rules, Sigma or native Bifract YAML.
- `models/` - analytics models, in the shape the model import takes.
- `searches/` - saved searches: `name`, `description`, `query`, optional `tags` and
  `time_range`.
- `dashboards/` - dashboards in the shape the UI exports, on the 24-column grid.
- `normalizers/` - normalizers in the shape the UI exports.
- `policies/` - alert policy documents.

Lookups have no file here. Their entry holds the source settings, and installs fetch the
rows from upstream on a schedule.

Installs read this index from `main` and skip items they do not understand. Add new
fields or kinds freely, but do not change what an existing field means.

## Adding an item

1. Add the file, then an entry in `index.yaml`:

    ```yaml
    - id: my-rule            # lowercase, digits, - and _; lookups and models: letters, digits and _
      kind: detection        # lookup, model, detection, search, dashboard, normalizer or policy
      name: My Rule          # the name it installs under; a model installs under its id
      description: One line on what it does.
      tags: [windows]
      path: detections/my-rule.yml
      requires: [my_lookup]  # items added first; a Sigma rule's fields map through the normalizer it requires
    ```

2. To group items for one use case, add a bundle. Its items can still be added alone:

    ```yaml
    - id: my-hunt
      kind: bundle
      name: My hunt
      description: What the bundle sets up, and what data it needs.
      items: [my_lookup, my-rule]
    ```

3. Run `go test ./pkg/exchange` and `go run ./cmd/bifract --test --lint ./exchange/detections/`.
   The tests check that every file is listed and loads as its import would, that each
   item is named as it installs, and that every `dict=` and `model=` a file uses is one it
   requires.
4. Open a pull request.

## File rules

Reviewers read every file, so the tests also hold the exchange to plain text:

- `.yaml`, `.yml` and `.md` files only, no links
- lowercase names: letters, digits, `.`, `-` and `_` (`README.md` aside)
- printable ASCII, newlines and tabs only, so no control characters, CRLF line endings,
  or zero-width and bidirectional characters
- 1 MiB per file

Rule tests need sample logs, which are JSON, so tested examples live in
`internal/ruletest/testdata/detections/`. Run them against a ClickHouse with
`BIFRACT_TEST_CLICKHOUSE=localhost:9000 go test -tags ruletestch ./internal/ruletest/`.

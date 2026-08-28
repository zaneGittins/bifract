# Guides

Worked examples for the things most people do first. Each one is a test in
`test/integration/`, run against a real server, so what follows is the code that
passed rather than a transcription of it. If an endpoint changes shape, the test
fails and this page changes with it.

They are written in Go because that is what the suite is written in, but they
are only HTTP: the same calls work from any language.

## Send logs and read them back

Create somewhere for logs to live, get a credential that can write to it, send
some, and query them. Everything else assumes this works.

```go
--8<-- "test/integration/recipe_ingest_query_test.go"
```

Two things worth noticing. An ingest token is a different credential from an API
key: it is scoped to one fractal and cannot read anything back, so a compromised
log shipper cannot query your data. And ingestion is asynchronous, so a query
issued immediately after a write can legitimately return nothing; wait for it.

## Ship a detection

Validate the query first, create the rule disabled, enable it once it looks
right, then read what it did. This is the shape a detection-as-code pipeline
takes.

```go
--8<-- "test/integration/recipe_detection_test.go"
```

`POST /query/validate` costs nothing and runs no query, so a CI gate can reject a
rule that no longer translates before it is ever scheduled.

An `event` alert matches individual logs as they are ingested, so its query
filters rather than aggregates. A rule that counts or groups is a `compound`
alert, and asking for one as an event is refused at creation.

## Move an investigation between fractals

A notebook exports as a YAML document, which is what makes an investigation
reviewable in a pull request and reusable in a second environment.

```go
--8<-- "test/integration/recipe_notebook_test.go"
```

## Record what you found

Findings live next to the logs that prompted them, tagged so a case can be
pulled back together later.

```go
--8<-- "test/integration/recipe_investigation_test.go"
```

One wrinkle worth knowing: a query answers timestamps in ClickHouse's format,
while endpoints that take a timestamp want RFC3339, so a caller converts between
them.

## Check on an instance

What it is, whether ingestion is keeping up, how much each fractal holds, and
how long it holds it. These are the calls a monitoring system or a capacity
review makes.

```go
--8<-- "test/integration/recipe_operations_test.go"
```

Retention is per fractal, so noisy data can be aged out sooner than the data
worth keeping. The `/system` endpoints answer their own shapes rather than the
standard envelope.

## Running these yourself

```
BIFRACT_API_KEY=bifract_... go test -tags integration ./test/integration/ -v
```

The key needs tenant admin, because the first guide creates and deletes a
fractal. Mint one under **Admin > API Keys > New key**.

For what a failure looks like and how a collection is paged, see
[Authentication](authentication.md) and [All Operations](reference.md); both are
asserted by the same suite, in `recipe_contract_test.go`.

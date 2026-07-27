# Archive compaction harness (MinIO + Postgres)

Backing services for `pkg/archive/compaction_manual_test.go` (build tag
`archiveminio`). MinIO holds the Iceberg data and metadata; Postgres holds the
SQL catalog. The full Bifract stack is not needed: the harness drives
`pkg/archive` directly.

```sh
docker compose -p archivetest -f test/archiveminio/docker-compose.yml up -d
go test -tags archiveminio ./pkg/archive/ -run TestCompaction -v
docker compose -p archivetest -f test/archiveminio/docker-compose.yml down -v
```

What it covers, and why it exists: compaction used to lose every commit race
against a concurrently-appending archiver, because the rewrite and the commit
were retried together (a ~9 minute rewrite cannot win against a writer that
commits every few minutes) and because the resulting `requirement failed:
branch "main" has changed` is returned unwrapped by the SQL catalog, so
iceberg-go's own refresh-and-replay retry never engages. Reproducing that needs
a real catalog and a real object store, which is what this compose file is for.

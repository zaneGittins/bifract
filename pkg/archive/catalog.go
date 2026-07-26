package archive

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"

	// Register the gocloud IO backends (s3/minio/azure/gcs) and the SQL catalog.
	_ "github.com/apache/iceberg-go/catalog/sql"
	_ "github.com/apache/iceberg-go/io/gocloud"

	"bifract/pkg/objstore"
	"bifract/pkg/parser"
)

// Namespace is the single Iceberg namespace under which every fractal's table
// lives (one table per fractal).
const Namespace = "bifract"

// Catalog wraps an iceberg-go SQL catalog (Postgres-backed) plus the fixed
// archive schema/partition spec, and manages per-fractal tables.
type Catalog struct {
	cat  catalog.Catalog
	name string
}

// NewCatalog opens the Postgres-backed Iceberg SQL catalog. pgDSN is a standard
// lib/pq DSN. The catalog tables (iceberg_tables, iceberg_namespace_properties)
// are managed by Bifract's own Postgres migrations, so table auto-creation is
// disabled here.
func NewCatalog(ctx context.Context, name, pgDSN string, obj objstore.Config) (*Catalog, error) {
	// Production pre-creates the catalog tables via a Postgres migration, so
	// auto-creation stays off by default. Tests may flip BIFRACT_ARCHIVE_INIT_CATALOG=true.
	initTables := "false"
	if v := os.Getenv("BIFRACT_ARCHIVE_INIT_CATALOG"); v == "true" {
		initTables = "true"
	}
	props := iceberg.Properties{
		"type":                "sql",
		"sql.driver":          "postgres",
		"sql.dialect":         "postgres",
		"uri":                 pgDSN,
		"warehouse":           obj.WarehouseURI(),
		"init_catalog_tables": initTables,
	}
	for k, v := range obj.IcebergProps() {
		props[k] = v
	}
	cat, err := catalog.Load(ctx, name, props)
	if err != nil {
		return nil, fmt.Errorf("archive: load catalog: %w", err)
	}
	return &Catalog{cat: cat, name: name}, nil
}

// tableName maps a fractal UUID to a valid Iceberg table identifier component.
func tableName(fractalID string) string {
	return "f_" + strings.ReplaceAll(fractalID, "-", "_")
}

// EnsureTable returns the Iceberg table for a fractal, creating the namespace
// and table (with the fixed schema + ingest_date partition spec) on first use.
func (c *Catalog) EnsureTable(ctx context.Context, fractalID string) (*icetable.Table, error) {
	ns := catalog.ToIdentifier(Namespace)
	if ok, err := c.cat.CheckNamespaceExists(ctx, ns); err != nil {
		return nil, fmt.Errorf("archive: check namespace: %w", err)
	} else if !ok {
		if err := c.cat.CreateNamespace(ctx, ns, nil); err != nil {
			return nil, fmt.Errorf("archive: create namespace: %w", err)
		}
	}

	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	if ok, err := c.cat.CheckTableExists(ctx, ident); err != nil {
		return nil, fmt.Errorf("archive: check table: %w", err)
	} else if ok {
		tbl, err := c.cat.LoadTable(ctx, ident)
		if err != nil {
			return nil, err
		}
		return c.ensureTableCurrent(ctx, ident, tbl)
	}

	sc, err := icebergSchema()
	if err != nil {
		return nil, fmt.Errorf("archive: build schema: %w", err)
	}
	spec, ok := partitionSpec(sc)
	if !ok {
		return nil, fmt.Errorf("archive: partition field %q missing from schema", partitionFieldName)
	}
	tbl, err := c.cat.CreateTable(ctx, ident, sc,
		catalog.WithPartitionSpec(&spec), catalog.WithProperties(iceTableProperties()))
	if err != nil {
		// Racing archivers (multiple ingest pods) may create concurrently; on a
		// lost race just load the winner's table.
		if ok2, e2 := c.cat.CheckTableExists(ctx, ident); e2 == nil && ok2 {
			return c.cat.LoadTable(ctx, ident)
		}
		return nil, fmt.Errorf("archive: create table %s: %w", tableName(fractalID), err)
	}
	// Publish the hint at creation so a fractal that is archived to but never
	// committed to again is still resolvable from object storage alone.
	writeVersionHint(ctx, tbl)
	return tbl, nil
}

// metadataPreviousVersionsMax bounds the metadata log. iceberg-go's default is
// 100 and, with delete-after-commit off, none of them are ever removed from
// object storage. metadata.json is rewritten in full on every commit and re-read
// by ClickHouse on every iceberg*() scan, so a long log is paid for on both the
// write and the read path.
const metadataPreviousVersionsMax = 10

// targetFileSizeBytes matches Iceberg's own default write target. Set
// explicitly so compaction rewrites toward large files rather than whatever the
// library default happens to be in a future version.
const targetFileSizeBytes = 512 << 20

// iceTableProperties returns the write properties for a fractal table: zstd
// compression, a bloom filter on each promoted column for fast equality pruning
// (Parquet min/max stats, written by default, additionally prune range and time
// predicates), a bounded metadata log, and an explicit target file size.
//
// Requires ClickHouse >= 26.6 to read the arrow-go bloom filters correctly: CH
// 26.2 mis-read them and false-negative pruned (dropped real matches). The
// bundled ClickHouse image is pinned to 26.6, and --upgrade/--upgrade-k8s carry
// that to existing installs, so bloom read is safe.
func iceTableProperties() iceberg.Properties {
	props := iceberg.Properties{
		"write.parquet.compression-codec": "zstd",
		// Without both of these every vN.metadata.json ever written stays in
		// object storage forever, and the log inside the live one grows without
		// bound. This is the difference between metadata that stays kilobytes and
		// metadata that reaches hundreds of megabytes on a busy table.
		"write.metadata.delete-after-commit.enabled": "true",
		"write.metadata.previous-versions-max":       strconv.Itoa(metadataPreviousVersionsMax),
		"write.target-file-size-bytes":               strconv.Itoa(targetFileSizeBytes),
	}
	for _, f := range parser.IcePromotedFields() {
		col, _ := parser.IcePromotedColumn(f)
		props["write.parquet.bloom-filter-enabled.column."+col] = "true"
	}
	return props
}

// stalePropertyDiff returns the desired properties whose current value differs
// (or is absent), so a table is only re-committed when something actually needs
// changing. Returns nil when the table is already current.
func stalePropertyDiff(current, desired iceberg.Properties) iceberg.Properties {
	var diff iceberg.Properties
	for k, want := range desired {
		if got, ok := current[k]; ok && got == want {
			continue
		}
		if diff == nil {
			diff = iceberg.Properties{}
		}
		diff[k] = want
	}
	return diff
}

// missingIceColumns returns the `_ice_` promoted columns absent from a schema.
func missingIceColumns(sc *iceberg.Schema) []string {
	var missing []string
	for _, f := range parser.IcePromotedFields() {
		col, _ := parser.IcePromotedColumn(f)
		if _, ok := sc.FindFieldByName(col); !ok {
			missing = append(missing, col)
		}
	}
	return missing
}

// ensureTableCurrent brings an existing table up to what this build expects:
// any missing `_ice_` promoted columns (nullable, so old data files read NULL)
// and any write property whose value has drifted. Both are checked because they
// drift independently - a table created before promotion is missing columns, and
// one created before the metadata-retention properties were added has the right
// columns but an unbounded metadata log. Neither is fixed by a later append, so
// the reconcile has to happen here.
//
// The common path (table already current) makes no commit at all. Restore is
// unaffected either way: it never references `_ice_*`.
func (c *Catalog) ensureTableCurrent(ctx context.Context, ident icetable.Identifier, tbl *icetable.Table) (*icetable.Table, error) {
	missing := missingIceColumns(tbl.Schema())
	staleProps := stalePropertyDiff(tbl.Properties(), iceTableProperties())
	if len(missing) == 0 && len(staleProps) == 0 {
		return tbl, nil
	}

	txn := tbl.NewTransaction()
	if len(missing) > 0 {
		us := txn.UpdateSchema(false, false)
		for _, col := range missing {
			us = us.AddColumn([]string{col}, iceberg.PrimitiveTypes.String,
				"bifract promoted column for query pruning", false, nil)
		}
		if err := us.Commit(); err != nil {
			return nil, fmt.Errorf("archive: add promoted columns: %w", err)
		}
	}
	if len(staleProps) > 0 {
		if err := txn.SetProperties(staleProps); err != nil {
			return nil, fmt.Errorf("archive: set write properties: %w", err)
		}
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		// Lost the optimistic-concurrency race with another archiver reconciling
		// the same table. The target state is deterministic, so reload the
		// winner's table and take it if it already satisfies both checks.
		if reloaded, e2 := c.cat.LoadTable(ctx, ident); e2 == nil {
			if len(missingIceColumns(reloaded.Schema())) == 0 &&
				len(stalePropertyDiff(reloaded.Properties(), iceTableProperties())) == 0 {
				return reloaded, nil
			}
		}
		return nil, fmt.Errorf("archive: reconcile table: %w", err)
	}
	writeVersionHint(ctx, updated)
	return updated, nil
}

// TablePromotedFields returns a fractal table's storage location together with
// the set of field names whose `_ice_` promoted column actually exists in that
// table's current Iceberg schema.
//
// The set is read from the table rather than taken from parser.IcePromotedFields()
// because the two legitimately diverge. The build's promoted set grows whenever a
// field is added to the default type-hint list, but an existing table only gains
// the new columns when an archiver next commits to it (see ensureIceColumns), so
// a fractal that has stopped ingesting keeps its older, narrower schema
// indefinitely. Referencing a column that table lacks is not a missed
// optimization: ClickHouse fails the entire search with UNKNOWN_IDENTIFIER. The
// reader therefore has to ask the table rather than assume the build's set.
func (c *Catalog) TablePromotedFields(ctx context.Context, fractalID string) (string, map[string]bool, error) {
	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return "", nil, err
	}
	sc := tbl.Schema()
	promoted := make(map[string]bool, len(parser.IcePromotedFields()))
	for _, f := range parser.IcePromotedFields() {
		col, ok := parser.IcePromotedColumn(f)
		if !ok {
			continue
		}
		if _, exists := sc.FindFieldByName(col); exists {
			promoted[f] = true
		}
	}
	return tbl.Location(), promoted, nil
}

// TableLocation returns the storage base location of a fractal's table (used to
// build the ClickHouse iceberg*() restore function). Empty if the table does
// not exist yet.
func (c *Catalog) TableLocation(ctx context.Context, fractalID string) (string, error) {
	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return "", err
	}
	return tbl.Location(), nil
}

package archive

import (
	"context"
	"fmt"
	"os"
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
		// Evolve pre-promotion tables (add `_ice_` columns + pruning properties).
		return c.ensureIceColumns(ctx, ident, tbl)
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
	return tbl, nil
}

// iceTableProperties returns the Parquet write properties for a fractal table:
// zstd compression plus a bloom filter on each promoted column for fast equality
// pruning. (Parquet min/max stats, written by default, additionally prune range
// and time predicates.)
//
// Requires ClickHouse >= 26.6 to read the arrow-go bloom filters correctly: CH
// 26.2 mis-read them and false-negative pruned (dropped real matches). The
// bundled ClickHouse image is pinned to 26.6, and --upgrade/--upgrade-k8s carry
// that to existing installs, so bloom read is safe.
func iceTableProperties() iceberg.Properties {
	props := iceberg.Properties{"write.parquet.compression-codec": "zstd"}
	for _, f := range parser.IcePromotedFields() {
		col, _ := parser.IcePromotedColumn(f)
		props["write.parquet.bloom-filter-enabled.column."+col] = "true"
	}
	return props
}

// ensureIceColumns evolves a pre-promotion table by adding any missing `_ice_`
// promoted columns (nullable) and setting the pruning write properties, so new
// data files carry the typed columns + bloom filters. It is a no-op once the
// table has been evolved. Restore is unaffected: it never references `_ice_*`.
func (c *Catalog) ensureIceColumns(ctx context.Context, ident icetable.Identifier, tbl *icetable.Table) (*icetable.Table, error) {
	sc := tbl.Schema()
	var missing []string
	for _, f := range parser.IcePromotedFields() {
		col, _ := parser.IcePromotedColumn(f)
		if _, ok := sc.FindFieldByName(col); !ok {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return tbl, nil
	}

	txn := tbl.NewTransaction()
	us := txn.UpdateSchema(false, false)
	for _, col := range missing {
		us = us.AddColumn([]string{col}, iceberg.PrimitiveTypes.String,
			"bifract promoted column for query pruning", false, nil)
	}
	if err := us.Commit(); err != nil {
		return nil, fmt.Errorf("archive: add promoted columns: %w", err)
	}
	if err := txn.SetProperties(iceTableProperties()); err != nil {
		return nil, fmt.Errorf("archive: set pruning properties: %w", err)
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		// Lost the optimistic-concurrency race with another archiver evolving the
		// same table. The set of columns is deterministic, so reload the winner's
		// table and use it if it already carries the promoted columns.
		if reloaded, e2 := c.cat.LoadTable(ctx, ident); e2 == nil {
			if _, ok := reloaded.Schema().FindFieldByName(missing[0]); ok {
				return reloaded, nil
			}
		}
		return nil, fmt.Errorf("archive: evolve table schema: %w", err)
	}
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

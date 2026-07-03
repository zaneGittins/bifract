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
		return c.cat.LoadTable(ctx, ident)
	}

	sc, err := icebergSchema()
	if err != nil {
		return nil, fmt.Errorf("archive: build schema: %w", err)
	}
	spec, ok := partitionSpec(sc)
	if !ok {
		return nil, fmt.Errorf("archive: partition field %q missing from schema", partitionFieldName)
	}
	tbl, err := c.cat.CreateTable(ctx, ident, sc, catalog.WithPartitionSpec(&spec))
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

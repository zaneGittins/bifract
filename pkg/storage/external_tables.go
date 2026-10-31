package storage

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
)

// ExternalTable is a set of String columns sent alongside one query as ClickHouse external
// data, so the query can read it as a table named Name. It replaces giant literal IN-lists:
// the rows travel as a binary block instead of SQL text the server has to parse, and they are
// forwarded to remote shards with the query.
type ExternalTable struct {
	Name    string
	Columns []string
	Rows    [][]string
}

type externalTablesKey struct{}

// ExternalTablesOf returns the tables attached to ctx by a storage query helper. Test drivers
// read them, since clickhouse-go keeps its own copy private.
func ExternalTablesOf(ctx context.Context) []ExternalTable {
	t, _ := ctx.Value(externalTablesKey{}).([]ExternalTable)
	return t
}

// withExternalTables attaches tables to ctx for the next query.
func withExternalTables(ctx context.Context, tables []ExternalTable) (context.Context, error) {
	if len(tables) == 0 {
		return ctx, nil
	}
	built := make([]*ext.Table, 0, len(tables))
	for _, t := range tables {
		cols := make([]func(*ext.Table) error, len(t.Columns))
		for i, c := range t.Columns {
			cols[i] = ext.Column(c, "String")
		}
		et, err := ext.NewTable(t.Name, cols...)
		if err != nil {
			return nil, fmt.Errorf("external table %s: %w", t.Name, err)
		}
		vals := make([]any, len(t.Columns))
		for _, r := range t.Rows {
			if len(r) != len(vals) {
				return nil, fmt.Errorf("external table %s: row has %d values for %d columns", t.Name, len(r), len(vals))
			}
			for i := range vals {
				vals[i] = r[i]
			}
			if err := et.Append(vals...); err != nil {
				return nil, fmt.Errorf("external table %s: %w", t.Name, err)
			}
		}
		built = append(built, et)
	}
	ctx = context.WithValue(ctx, externalTablesKey{}, tables)
	return clickhouse.Context(ctx, clickhouse.WithExternalTable(built...)), nil
}

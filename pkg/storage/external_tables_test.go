package storage

import (
	"context"
	"reflect"
	"testing"
)

// QueryProvenance attaches its external tables to the query's context, where the driver sends
// them as data blocks, and a query without tables carries none.
func TestQueryProvenanceAttachesExternalTables(t *testing.T) {
	c, f := newFakeClient()
	var seen [][]ExternalTable
	f.CtxArgs = func(ctx context.Context) []any {
		seen = append(seen, ExternalTablesOf(ctx))
		return nil
	}
	tree := ExternalTable{Name: "_pgr_tree", Columns: []string{"guid", "node"}, Rows: [][]string{{"a", "a"}, {"b", "a"}}}
	if _, err := c.QueryProvenance(context.Background(), "SELECT 1", tree); err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryProvenance(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || !reflect.DeepEqual(seen[0], []ExternalTable{tree}) || seen[1] != nil {
		t.Errorf("tables seen by the driver: %v", seen)
	}
}

// A row that does not match its columns fails before anything is sent.
func TestQueryProvenanceRejectsMalformedTable(t *testing.T) {
	c, _ := newFakeClient()
	bad := ExternalTable{Name: "_t", Columns: []string{"guid", "node"}, Rows: [][]string{{"only-one"}}}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("malformed table panicked: %v", r)
		}
	}()
	if _, err := c.QueryProvenance(context.Background(), "SELECT 1", bad); err == nil {
		t.Error("a short row must be an error")
	}
}

package parser

import (
	"os"
	"strings"
	"testing"
)

// The key columns must be computed by the same expression the lookups apply to their GUID sets,
// or a lookup silently matches nothing.
func TestProcKeyExprMatchesDDL(t *testing.T) {
	want := map[string][]string{
		"../../db/init-clickhouse.sql": {
			"proc_key         String MATERIALIZED " + procKeyExpr("process_guid"),
			"parent_key       String MATERIALIZED " + procKeyExpr("parent_guid"),
			"proc_key         String MATERIALIZED " + procKeyExpr("process_guid") + ",\n    PROJECTION by_target",
		},
		"../../db/migrations/clickhouse/022_proc_key.sql": {
			"ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS proc_key String MATERIALIZED " + procKeyExpr("process_guid") + ";",
			"ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS parent_key String MATERIALIZED " + procKeyExpr("parent_guid") + ";",
			"ALTER TABLE process_edges ADD COLUMN IF NOT EXISTS proc_key String MATERIALIZED " + procKeyExpr("process_guid") + ";",
		},
	}
	for file, uses := range want {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range uses {
			if !strings.Contains(string(b), w) {
				t.Errorf("%s: missing %q", file, w)
			}
		}
	}
}

// Lookups by GUID name the sort key, or they read every granule of the table.
func TestProvenanceLookupsUseProcKey(t *testing.T) {
	o := reconOpts()
	hop := BuildProcessTreeHopSQL("{63047898-81ee-6860-5202-000000002502}", o)
	if !strings.Contains(hop, "proc_key = "+procKeyExpr("'{63047898-81ee-6860-5202-000000002502}'")) {
		t.Errorf("hop does not look up by key:\n%s", hop)
	}
	if rows := BuildProcessTreeRowsSQL(o); !strings.Contains(rows, "proc_key GLOBAL IN (SELECT "+procKeyExpr("guid")+" FROM _pgr_tree WHERE guid = node)") {
		t.Errorf("ptg rows do not look up by key:\n%s", rows)
	}
	union := buildProvenanceEdgeUnion(nil, o)
	if strings.Contains(union, "process_guid GLOBAL IN (SELECT guid FROM _pgr_tree)") {
		t.Errorf("an edge read looks up by GUID instead of key:\n%s", union)
	}
	recon := BuildReconnectionSQL(ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{}}, 100, 50, o)
	if strings.Contains(recon, "FROM process_edges WHERE fractal_id = 'f1' AND event_type = 'file_write' AND process_guid GLOBAL IN") {
		t.Errorf("written paths look up by GUID instead of key:\n%s", recon)
	}
}

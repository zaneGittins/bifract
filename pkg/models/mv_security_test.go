package models

import (
	"strings"
	"testing"
)

// TestModelMVsAreDefiner guards the least-privilege ingest invariant: every materialized
// view created on the logs table (including these runtime, per-model MVs) MUST be
// SQL SECURITY DEFINER. A legacy/INVOKER MV runs with the inserting user's privileges,
// so pushing an insert through it requires SELECT on logs -- which would break the
// least-privilege ingest user (INSERT-only, no log reads) and stall ingestion / the
// distribution queue. If this test fails, a model MV template dropped the DEFINER clause;
// re-add `DEFINER = default SQL SECURITY DEFINER` after the `TO <target>` clause.
func TestModelMVsAreDefiner(t *testing.T) {
	const want = "SQL SECURITY DEFINER"

	// Aggregating model MVs (rarity / volume baseline).
	for _, tc := range []struct {
		name string
		def  ModelDefinition
		mt   ModelType
	}{
		{"rarity", ModelDefinition{PartitionKey: "src_ip", ValueKey: "dst_ip"}, ModelTypeRarity},
		{"volume_baseline", ModelDefinition{PartitionKey: "src_ip", TimeBucket: "day"}, ModelTypeVolumeBaseline},
	} {
		ddl, err := generateMVDDL(tc.def, tc.mt, "state_tbl", "mv_name")
		if err != nil {
			t.Fatalf("%s: generateMVDDL: %v", tc.name, err)
		}
		if !strings.Contains(ddl, want) {
			t.Errorf("%s model MV is missing %q (would break least-privilege ingest):\n%s", tc.name, want, ddl)
		}
	}

	// Network model MVs (beacon / long_connection).
	for _, mt := range []ModelType{ModelTypeBeacon, ModelTypeLongConnection} {
		parsed := ParseSourceQuery(`channel="conn.log" src_ip="1.2.3.4" dst_ip="5.6.7.8"`, mt)
		if len(parsed.Errors) != 0 {
			t.Fatalf("%s: parse: %v", mt, parsed.Errors)
		}
		ddl, err := BuildNetStateMV(ModelDefinition{Filter: parsed.Filter}, mt, "state_tbl", "mv_name")
		if err != nil {
			t.Fatalf("%s: BuildNetStateMV: %v", mt, err)
		}
		if !strings.Contains(ddl, want) {
			t.Errorf("%s model MV is missing %q (would break least-privilege ingest):\n%s", mt, want, ddl)
		}
	}
}

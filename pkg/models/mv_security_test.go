package models

import (
	"os"
	"strings"
	"testing"
)

// Model state is maintained by StateMaintainer, never by a view on the logs table.
// A model view would have to be SQL SECURITY DEFINER or the least-privilege ingest
// user (INSERT-only, no log reads) could not push inserts through it, stalling
// ingestion and the distribution queue. Building none at all is the stronger
// guarantee, and it is also what keeps a dictionary lookup in a model's source out
// of the ingest path.
//
// pkg/storage's ReconcileMaterializedViewSecurity still owns the invariant for the
// views the product does create on logs.
func TestModelsCreateNoMaterializedViews(t *testing.T) {
	for _, name := range []string{"ddl.go", "manager.go", "statemaint.go"} {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(src), "CREATE MATERIALIZED VIEW") {
			t.Errorf("%s builds a materialized view: model state is maintained by "+
				"StateMaintainer, and a view would put the source scan back in the ingest path", name)
		}
	}
}

// Every path that writes model state goes through a builder that scopes the scan
// to the owning fractal, so none of them can be given a view's implicit "all
// inserts" source by accident.
func TestModelStateWritersAreExplicitAboutTheirSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		def  ModelDefinition
		mt   ModelType
	}{
		{"rarity", ModelDefinition{PartitionKey: "src_ip", ValueKey: "dst_ip"}, ModelTypeRarity},
		{"volume_baseline", ModelDefinition{PartitionKey: "src_ip", TimeBucket: "day"}, ModelTypeVolumeBaseline},
	} {
		sql, err := BuildBackfillInsert(tc.def, tc.mt, "state_tbl", "logs", "", "f1")
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !strings.Contains(sql, "FROM logs") {
			t.Errorf("%s: source table must be the one the caller named:\n%s", tc.name, sql)
		}
	}
	for _, mt := range []ModelType{ModelTypeBeacon, ModelTypeLongConnection} {
		parsed := ParseSourceQuery(`channel="conn.log" src_ip="1.2.3.4" dst_ip="5.6.7.8"`)
		if len(parsed.Errors) != 0 {
			t.Fatalf("%s: parse: %v", mt, parsed.Errors)
		}
		sql, err := BuildNetStateInsert(ModelDefinition{Filter: parsed.Filter}, "state_tbl", "logs", "", "f1")
		if err != nil {
			t.Fatalf("%s: %v", mt, err)
		}
		if !strings.Contains(sql, "FROM logs") {
			t.Errorf("%s: source table must be the one the caller named:\n%s", mt, sql)
		}
	}
}

package models

import (
	"strings"
	"testing"
)

// A model MV must aggregate only its owning fractal's logs. Before this was
// enforced, a model's MV carried only a non-empty-fractal check, so a model created
// in one fractal ran its aggregation over every fractal's inserts and stored rows
// that only the read-side predicate kept out of view.
func TestModelMVsAreFractalScoped(t *testing.T) {
	cases := []struct {
		name string
		mt   ModelType
		def  ModelDefinition
	}{
		{"rarity", ModelTypeRarity, ModelDefinition{PartitionKey: "computer_name", ValueKey: "sin_port"}},
		{"first_seen", ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"image"}}},
		{"volume_baseline", ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"image"}}},
		{"with_extraction", ModelTypeFirstSeen, ModelDefinition{
			KeyFields:   []string{"proc"},
			Extractions: []ExtractionStep{{FromField: "commandline", Pattern: `(\w+)`, OutputField: "proc"}},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, mvSQL, err := GenerateDDL(tc.def, tc.mt, "`t`", "`mv`", "fractal-a")
			if err != nil {
				t.Fatal(err)
			}
			mustContain(t, mvSQL, "fractal_id = 'fractal-a'", "mv scoped to owner")
			mustNotContain(t, mvSQL, "fractal_id != ''", "mv must not scan every fractal")
		})
	}
}

func TestNetStateMVIsFractalScoped(t *testing.T) {
	def := ModelDefinition{Network: &NetworkFieldMap{SrcField: "src_ip", DstField: "dst_ip", PortField: "dst_port"}}
	mvSQL, err := BuildNetStateMV(def, ModelTypeBeacon, "`state`", "`mv`", "fractal-a")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, mvSQL, "fractal_id = 'fractal-a'", "net state mv scoped to owner")
	mustNotContain(t, mvSQL, "fractal_id != ''", "net state mv must not scan every fractal")
}

// The backfill reuses the MV's SELECT, so it inherits the same scope. An unscoped
// backfill would read every fractal's history for one fractal's model.
func TestBackfillIsFractalScoped(t *testing.T) {
	def := ModelDefinition{KeyFields: []string{"image"}}
	sql, err := BuildBackfillInsert(def, ModelTypeFirstSeen, "`t`", "logs_distributed",
		"timestamp >= 'a' AND timestamp < 'b'", "fractal-a")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "fractal_id = 'fractal-a'", "backfill scoped to owner")
	mustNotContain(t, sql, "fractal_id != ''", "backfill must not scan every fractal")
}

// An empty owner must fail loudly. Rendering it would compare fractal_id against
// the empty string, which silently matches only pre-fractal legacy rows.
func TestModelDDLRejectsEmptyFractal(t *testing.T) {
	def := ModelDefinition{KeyFields: []string{"image"}}
	if _, _, err := GenerateDDL(def, ModelTypeFirstSeen, "`t`", "`mv`", ""); err == nil {
		t.Fatal("expected GenerateDDL to reject an empty fractal_id")
	}
	if _, err := BuildNetStateMV(ModelDefinition{}, ModelTypeBeacon, "`s`", "`mv`", ""); err == nil {
		t.Fatal("expected BuildNetStateMV to reject an empty fractal_id")
	}
}

// The fractal literal is escaped, so an id carrying a quote cannot break out of
// the predicate.
func TestFractalScopeClauseEscapes(t *testing.T) {
	got := fractalScopeClause("a'b")
	if strings.Contains(got, "'a'b'") {
		t.Fatalf("fractal id not escaped: %s", got)
	}
}

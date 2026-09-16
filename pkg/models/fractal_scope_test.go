package models

import (
	"strings"
	"testing"
)

// A model's state aggregation must read only its owning fractal's logs. Before
// this was enforced the scan carried only a non-empty-fractal check, so a model
// created in one fractal aggregated every fractal's rows and stored data that only
// the read-side predicate kept out of view.
func TestModelStateSelectIsFractalScoped(t *testing.T) {
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
			sql, err := BuildBackfillInsert(tc.def, tc.mt, "`t`", "logs", "", "fractal-a")
			if err != nil {
				t.Fatal(err)
			}
			mustContain(t, sql, "fractal_id = 'fractal-a'", "state scan scoped to owner")
			mustNotContain(t, sql, "fractal_id != ''", "state scan must not read every fractal")
		})
	}
}

func TestNetStateSelectIsFractalScoped(t *testing.T) {
	def := ModelDefinition{Network: &NetworkFieldMap{SrcField: "src_ip", DstField: "dst_ip", PortField: "dst_port"}}
	sql, err := BuildNetStateInsert(def, "`state`", "logs", "", "fractal-a")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "fractal_id = 'fractal-a'", "net state scan scoped to owner")
	mustNotContain(t, sql, "fractal_id != ''", "net state scan must not read every fractal")
}

// The backfill and the scheduled maintainer share this SELECT, so both inherit
// the same scope. An unscoped read would take every fractal's history into one
// fractal's model.
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
func TestModelStateSelectRejectsEmptyFractal(t *testing.T) {
	def := ModelDefinition{KeyFields: []string{"image"}}
	if _, err := BuildBackfillInsert(def, ModelTypeFirstSeen, "`t`", "logs", "", ""); err == nil {
		t.Fatal("expected the state insert to reject an empty fractal_id")
	}
	if _, err := BuildNetStateInsert(ModelDefinition{}, "`s`", "logs", "", ""); err == nil {
		t.Fatal("expected the net state insert to reject an empty fractal_id")
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

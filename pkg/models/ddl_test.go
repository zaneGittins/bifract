package models

import (
	"strings"
	"testing"
)

// TestFilterConditionWildcard locks in that a `field="*"` model filter compiles to
// a non-empty check (matching the main BQL translator in pkg/parser), not a literal
// `= '*'` match that silently empties the model's state.
func TestFilterConditionWildcard(t *testing.T) {
	cases := []struct {
		name string
		fc   FilterCondition
		want string
	}{
		{"wildcard equals -> non-empty", FilterCondition{Field: "src_ip", Op: "=", Value: "*"}, "fields.`src_ip`::String != ''"},
		{"wildcard not-equals -> empty", FilterCondition{Field: "dst_ip", Op: "!=", Value: "*"}, "fields.`dst_ip`::String = ''"},
		{"literal equals unchanged", FilterCondition{Field: "channel", Op: "=", Value: "conn-json.log"}, "fields.`channel`::String = 'conn-json.log'"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := filterConditionToSQL(c.fc); got != c.want {
				t.Fatalf("filterConditionToSQL = %q, want %q", got, c.want)
			}
		})
	}
}

// TestBuildNetStateMVWildcard verifies the end-to-end MV for a beacon model whose
// filter uses src/dst wildcards does not emit a literal `= '*'` predicate.
func TestBuildNetStateMVWildcard(t *testing.T) {
	q := `channel="conn-json.log" src_ip="*" dst_ip="*" | !cidr(dst_ip,"10.0.0.0/8")`
	parsed := ParseSourceQuery(q, ModelTypeBeacon)
	if len(parsed.Errors) != 0 {
		t.Fatalf("unexpected parse errors: %v", parsed.Errors)
	}
	def := ModelDefinition{Filter: parsed.Filter}
	mv, err := BuildNetStateMV(def, ModelTypeBeacon, "state_tbl", "mv_name")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(mv, "= '*'") {
		t.Fatalf("MV contains a literal wildcard match that would never match:\n%s", mv)
	}
}

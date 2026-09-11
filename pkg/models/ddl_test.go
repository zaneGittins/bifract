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
	mv, err := BuildNetStateMV(def, ModelTypeBeacon, "state_tbl", "mv_name", "f1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(mv, "= '*'") {
		t.Fatalf("MV contains a literal wildcard match that would never match:\n%s", mv)
	}
}

// A model definition is stored configuration, not query text, so nothing else
// constrains it. Extraction fields in particular are written into the statement
// unquoted (an extraction output becomes a plain CTE column), so a name carrying
// SQL became a second select expression in the model's materialized view, which
// then ran on every insert into logs.
func TestModelDefinitionRejectsFieldNamesThatAreNotFieldNames(t *testing.T) {
	hostile := []string{
		"out, (SELECT 1) AS pwned",
		"image` , 1 AS x, fields.`image",
		"a' OR '1'='1",
		"a) UNION ALL SELECT 1 --",
		"a b",
		`a\`,
	}
	for _, bad := range hostile {
		defs := []ModelDefinition{
			{KeyFields: []string{bad}},
			{KeyFields: []string{"ok"}, Extractions: []ExtractionStep{{FromField: "image", Pattern: "(.*)", OutputField: bad}}},
			{KeyFields: []string{"ok"}, Extractions: []ExtractionStep{{FromField: bad, Pattern: "(.*)", OutputField: "out"}}},
			{PartitionKey: bad, ValueKey: "v"},
			{PartitionKey: "p", ValueKey: bad},
		}
		for i, def := range defs {
			if err := validateDefinitionFieldNames(def); err == nil {
				t.Errorf("definition %d accepted hostile field name %q", i, bad)
			}
		}
	}

	// Real definitions must still pass, including the dots a log field carries.
	for _, def := range []ModelDefinition{
		{KeyFields: []string{"winlog.user", "computer_name"}},
		{PartitionKey: "src_ip", ValueKey: "dst_port"},
		{KeyFields: []string{"out"}, Extractions: []ExtractionStep{{FromField: "commandline", Pattern: "(.*)", OutputField: "out"}}},
		{KeyFields: []string{"host-name_1"}},
		{}, // empty: a shape question, not a name question
	} {
		if err := validateDefinitionFieldNames(def); err != nil {
			t.Errorf("legitimate definition rejected: %v", err)
		}
	}
}

// Update rebuilds the model's ClickHouse objects from the definition it is given, so
// it has to validate as create does. Without this, a model created with a benign
// definition and then edited put an unvalidated field name straight into the
// materialized view, which runs on every insert into logs.
func TestUpdateValidatesDefinitionFieldNames(t *testing.T) {
	hostile := ModelDefinition{
		KeyFields: []string{"out"},
		Extractions: []ExtractionStep{{
			FromField:   "image",
			Pattern:     "(.*)",
			OutputField: "out, (SELECT 1) AS pwned",
		}},
	}
	if err := validateDefinitionShape(ModelTypeFirstSeen, hostile); err == nil {
		t.Fatal("a definition carrying SQL in a field name must be rejected on the shape check every writer runs")
	}
}

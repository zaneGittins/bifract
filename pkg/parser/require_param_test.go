package parser

import (
	"strings"
	"testing"
	"time"
)

func requireOpts() QueryOptions {
	return QueryOptions{
		StartTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            1000,
		FractalID:          "f1",
		DictionaryDatabase: "bifract",
		Dictionaries:       map[string]map[string]string{"d": {"ip": "dict_d_ip"}},
	}
}

func requireSQL(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, requireOpts())
	if err != nil {
		t.Fatalf("translate %s: %v", query, err)
	}
	return result.SQL
}

// require= says what happens to a row that does not match, which strict= never
// did. Both spell the same thing, so a saved query keeps working.
func TestMatchRequireAndStrictAgree(t *testing.T) {
	filters := []string{
		`* | match(dict="d", field=src_ip, column=ip, include=[s], require=true)`,
		`* | match(dict="d", field=src_ip, column=ip, include=[s], strict=true)`,
	}
	for _, q := range filters {
		if sql := requireSQL(t, q); !strings.Contains(sql, "dictHas(") {
			t.Errorf("%s: want the non-matching rows dropped, got: %s", q, sql)
		}
	}
	keeps := []string{
		`* | match(dict="d", field=src_ip, column=ip, include=[s])`,
		`* | match(dict="d", field=src_ip, column=ip, include=[s], require=false)`,
		`* | match(dict="d", field=src_ip, column=ip, include=[s], strict=false)`,
	}
	for _, q := range keeps {
		if sql := requireSQL(t, q); strings.Contains(sql, "dictHas(") {
			t.Errorf("%s: want every row kept, got: %s", q, sql)
		}
	}
	// The two spellings compile identically, not merely similarly.
	if a, b := requireSQL(t, filters[0]), requireSQL(t, filters[1]); a != b {
		t.Errorf("require= and strict= must compile the same\n require=: %s\n  strict=: %s", a, b)
	}
}

// The default is unchanged: a lookup keeps every row unless asked otherwise. An
// alert that silently matched less than its author believed is worse than one
// that matches too much.
func TestMatchStillKeepsEveryRowByDefault(t *testing.T) {
	plain := requireSQL(t, `* | match(dict="d", field=src_ip, column=ip, include=[s])`)
	explicit := requireSQL(t, `* | match(dict="d", field=src_ip, column=ip, include=[s], require=false)`)
	if plain != explicit {
		t.Errorf("the default must be require=false\n  plain: %s\n   false: %s", plain, explicit)
	}
}

// A typo cannot silently flip the switch, which is the rule for every boolean
// parameter in BQL.
func TestRequireIgnoresAValueItDoesNotUnderstand(t *testing.T) {
	sql := requireSQL(t, `* | match(dict="d", field=src_ip, column=ip, include=[s], require=maybe)`)
	if strings.Contains(sql, "dictHas(") {
		t.Errorf("an unrecognised value must leave the default alone, got: %s", sql)
	}
}

// Both commands name the setting the same, even though their defaults differ:
// one word, one meaning.
func TestBothLookupsAcceptRequire(t *testing.T) {
	for _, spec := range []string{"match", "modellookup"} {
		s, ok := commandSpecs[spec]
		if !ok {
			t.Fatalf("no spec for %s", spec)
		}
		var names []string
		for _, param := range s.Params {
			names = append(names, param.Name)
		}
		joined := strings.Join(names, ",")
		for _, want := range []string{"require", "strict"} {
			if !strings.Contains(joined, want) {
				t.Errorf("%s must accept %s, has [%s]", spec, want, joined)
			}
		}
	}
}

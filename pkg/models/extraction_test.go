package models

import (
	"strings"
	"testing"
)

// The extraction probe is open to viewers and its SQL runs as the schema
// identity, which can read every fractal's logs. A field name that reached the
// SQL unquoted let a viewer write their own SELECT.
func TestExtractionTestSQLRejectsInjectedFieldNames(t *testing.T) {
	ok := ExtractionStep{FromField: "norm_log", Pattern: "(\\w+)", OutputField: "word"}
	cases := map[string]struct {
		filter []FilterCondition
		exts   []ExtractionStep
	}{
		"output field": {exts: []ExtractionStep{{FromField: "norm_log", Pattern: "(x)", OutputField: "x FROM (SELECT version() AS x) --"}}},
		"from field":   {exts: []ExtractionStep{{FromField: "a) UNION ALL SELECT 1 --", Pattern: "(x)", OutputField: "x"}}},
		"backtick":     {exts: []ExtractionStep{{FromField: "norm_log", Pattern: "(x)", OutputField: "x` FROM system.one --"}}},
		"filter field": {filter: []FilterCondition{{Field: "a' OR 1=1 --", Op: "=", Value: "v"}}, exts: []ExtractionStep{ok}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if sql, err := buildExtractionTestSQL("logs", "f1", c.filter, c.exts); err == nil {
				t.Fatalf("accepted an injected field name; built:\n%s", sql)
			}
		})
	}
}

func TestExtractionTestSQLQuotesIdentifiers(t *testing.T) {
	sql, err := buildExtractionTestSQL("logs", "f1",
		[]FilterCondition{{Field: "event_id", Op: "=", Value: "1"}},
		[]ExtractionStep{
			{FromField: "image", Pattern: "([^\\\\]+)$", OutputField: "exe"},
			{FromField: "exe", Pattern: "(\\w+)", OutputField: "stem"},
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"AS `image`",
		"extract(`image`,",
		"AS `exe`",
		"extract(`exe`,",
		"SELECT `stem`, count() AS cnt",
		"GROUP BY `stem`",
		"WHERE fractal_id = 'f1'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
}

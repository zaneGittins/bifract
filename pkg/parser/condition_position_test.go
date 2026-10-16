package parser

import (
	"strings"
	"testing"
	"time"
)

func positionSQL(t *testing.T, q string) string {
	t.Helper()
	pl, err := ParseQuery(q)
	if err != nil {
		t.Fatalf("parse %s: %v", q, err)
	}
	res, err := TranslateToSQLWithOrder(pl, QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   100,
		FractalID: "f1",
	})
	if err != nil {
		t.Fatalf("translate %s: %v", q, err)
	}
	return res.SQL
}

// A condition filters the rows as they are where it is written. Before any
// command those are the raw logs, so nothing a later command produces (a
// transform, a grouping alias, an aggregate) may reach back into it.
func TestConditionSeesFieldsAtItsPosition(t *testing.T) {
	cases := []struct {
		name, query, want, notWant string
	}{
		{"later transform does not apply", `* | level="a" | lowercase(level)`,
			"fields.`level`::String = 'a'", "lower(fields.`level`::String) = 'a'"},
		{"count before any aggregation is a field", `* | count > 5`,
			"toFloat64OrZero(fields.`count`::String) > 5", "_count"},
		{"later grouping key is still the raw field", `* | event_id=* | groupby(event_id)`,
			"fields.`event_id`::String != ''", ""},
		{"later grouping key past a scoped stage", `* | event_id=* | groupby(event_id) | count()`,
			"fields.`event_id`::String != ''", "event_id != ''"},
		{"earlier transform applies", `* | lowercase(level) | level="a"`,
			"lower(fields.`level`::String) = 'a'", ""},
		{"count after an aggregation is the aggregate", `* | groupby(user) | count > 5`,
			"HAVING _count > 5", ""},
		{"earlier assignment applies", `* | x := lower(a) | x = "b"`,
			"lower(fields.`a`::String) = 'b'", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := positionSQL(t, tc.query)
			if !strings.Contains(sql, tc.want) {
				t.Errorf("missing %q:\n%s", tc.want, sql)
			}
			if tc.notWant != "" && strings.Contains(sql, tc.notWant) {
				t.Errorf("must not contain %q:\n%s", tc.notWant, sql)
			}
		})
	}
}

// Conditions before the first command filter the scan first, ahead of anything a
// command adds, whether or not a pipe precedes them.
func TestFirstStageConditionsLeadTheScan(t *testing.T) {
	sql := positionSQL(t, `* | a="1" | cidr(dst_ip,"10.0.0.0/8")`)
	a, cidr := strings.Index(sql, "fields.`a`::String = '1'"), strings.Index(sql, "isIPAddressInRange")
	if a < 0 || cidr < 0 || a > cidr {
		t.Errorf("first-stage condition at %d, command predicate at %d; want the condition first:\n%s", a, cidr, sql)
	}
}

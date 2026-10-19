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
		{"later assignment does not apply", `y=5 | y := lower(x)`,
			"fields.`y`::String = '5'", "lower(fields.`x`::String) = '5'"},
		{"later assignment in the same stage", `y=5 y := lower(x)`,
			"fields.`y`::String = '5'", "lower(fields.`x`::String) = '5'"},
		{"later assignment after a pipe", `* | y=5 | y := lower(x)`,
			"fields.`y`::String = '5'", "lower(fields.`x`::String) = '5'"},
		{"later assignment past a command", `y=5 | head(10) | y := lower(x)`,
			"fields.`y`::String = '5'", "lower(fields.`x`::String) = '5'"},
		{"each side of an assignment", `y=5 y := lower(x) y="a"`,
			"fields.`y`::String = '5'", ""},
		{"each side of an assignment, later side", `y=5 y := lower(x) y="a"`,
			"lower(fields.`x`::String) = 'a'", ""},
		{"assignment after an OR chain", `a="1" OR b="2" y := lower(x) y="q"`,
			"(fields.`a`::String = '1' OR fields.`b`::String = '2') AND lower(fields.`x`::String) = 'q'", ""},
		{"earlier assignment applies to a function operand", `* | ip2 := lower(ip) | cidr(ip2, "10.0.0.0/8") OR a="1"`,
			"isIPv4String(lower(fields.`ip`::String))", "fields.`ip2`"},
		{"later assignment does not apply to a function operand", `cidr(ip2, "10.0.0.0/8") OR a="1" | ip2 := lower(ip)`,
			"isIPv4String(fields.`ip2`::String)", "isIPv4String(lower("},
		{"reassignment keeps the earlier value", `y := upper(x) | y=5 | y := lower(x)`,
			"upper(fields.`x`::String) = '5'", "lower(fields.`x`::String) = '5'"},
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

// Past the first command a filter still sees fields as they are where it is
// written: a transform or assignment after it must not reach back into it.
func TestFilterSeesFieldsPastTheFirstCommand(t *testing.T) {
	cases := []struct {
		name, query, want, notWant string
	}{
		{"condition before a later transform", `* | lowercase(a) | a="X" | uppercase(a)`,
			"lower(fields.`a`::String) = 'X'", "upper(lower(fields.`a`::String)) = 'X'"},
		{"condition before a later assignment", `* | lowercase(a) | a="X" | a := "z"`,
			"lower(fields.`a`::String) = 'X'", "lower('z')"},
		{"transform before a later assignment", `* | lowercase(a) | a := "z"`,
			"lower(fields.`a`::String) AS a", "lower('z')"},
		{"condition after an assignment past a command", `* | head(5) | a := lower(b) | a="x"`,
			"lower(fields.`b`::String) = 'x'", ""},
		{"in() before a later assignment", `* | in(a, values=["X"]) | a := "z"`,
			"fields.`a`::String IN ('X')", " a IN ('X')"},
		{"in() before a later transform", `* | in(a, values=["X"]) | lowercase(a)`,
			"fields.`a`::String IN ('X')", " a IN ('X')"},
		{"in() after a transform", `* | lowercase(a) | in(a, values=["x"])`,
			"lower(fields.`a`::String) IN ('x')", " a IN ('x')"},
		{"in() after a transform, before an assignment", `* | lowercase(a) | in(a, values=["x"]) | a := "z"`,
			"lower(fields.`a`::String) IN ('x')", "lower('z')"},
		{"computed assignment before a later transform", `* | c := concat(a, "x") | lowercase(a)`,
			"concat(fields.`a`::String, 'x') AS c", "concat(a, 'x')"},
		{"cidr() before a later assignment", `* | cidr(a, "10.0.0.0/8") | a := "z"`,
			"isIPv4String(fields.`a`::String)", "isIPv4String('z')"},
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

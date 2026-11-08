package parser

import (
	"slices"
	"testing"
)

func TestColumnRefs(t *testing.T) {
	for sql, want := range map[string][]string{
		"fields.`query`::String = 'a b' AND is_new = '1'": {"fields", "is_new"},
		"dateDiff('second', first_seen, _now) > 3600":     {"first_seen", "_now"},
		"toFloat64OrDefault(percent, nan) < 0.1e3":        {"percent"},
		// Unquoted nan and inf are Float64 literals; backticked, a column.
		"x < NaN OR x > -inf OR `nan` > 0":                     {"x", "x", "nan"},
		"timestamp - INTERVAL 1 HOUR > hour":                   {"timestamp", "hour"},
		"`odd name` IS NOT NULL AND x IN ('it''s', 'a\\'b')":   {"odd name", "x"},
		"fields.`a`.`b` != '' OR now() - INTERVAL 1 HOUR > ts": {"fields", "ts"},
	} {
		if got := columnRefs(sql); !slices.Equal(got, want) {
			t.Errorf("columnRefs(%q) = %q, want %q", sql, got, want)
		}
	}
}

// An alias named like a keyword (hour, first) is a column, not a keyword: reading it
// as one let an ordering on it into a scan where it does not exist.
func TestOrderingReadsOnlyScan(t *testing.T) {
	for _, c := range []struct {
		order   []OrderTerm
		limitBy string
		want    bool
	}{
		{[]OrderTerm{{Expr: "timestamp", Desc: true}, {Expr: "log_id", Desc: true}}, "", true},
		{[]OrderTerm{{Expr: "fields.`image`::String"}}, "", true},
		{[]OrderTerm{{Expr: "lower(fields.`a`::String)"}}, "", true},
		{[]OrderTerm{{Expr: "first_seen"}}, "", false},
		{[]OrderTerm{{Expr: "_len", Desc: true}}, "", false},
		{[]OrderTerm{{Expr: "hour"}}, "", false},
		{[]OrderTerm{{Expr: "first", Desc: true}}, "", false},
		{[]OrderTerm{{Expr: "timestamp", Desc: true}}, "LIMIT 1 BY day", false},
		{[]OrderTerm{{Expr: "timestamp", Desc: true}}, "LIMIT 1 BY fields.`user`::String", true},
		{[]OrderTerm{{Expr: "timestamp - INTERVAL 1 HOUR", Desc: true}}, "", true},
	} {
		if got := orderingReadsOnlyScan(c.order, c.limitBy); got != c.want {
			t.Errorf("orderingReadsOnlyScan(%q, %q) = %v, want %v", c.order, c.limitBy, got, c.want)
		}
	}
}

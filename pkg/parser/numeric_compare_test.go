package parser

import (
	"strings"
	"testing"
)

// An ordering comparison reads text through toFloat64OrDefault(x, nan): every
// comparison with NaN is false, so "", "N/A" and absent values never match.
// toFloat64OrZero read them as 0, so bytes < 100 matched them. toFloat64OrNull
// made the condition Nullable, and sequenceMatch drops a row whose condition is
// NULL from every step, so chain() lost the other steps' events.
func TestNumericComparisonSkipsNonNumbers(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"log field", `bytes < 100`, "toFloat64OrDefault(fields.`bytes`::String, nan) < 100"},
		// NOT is the complement: a non-number fails bytes < 100, so it matches NOT.
		// Flipping to >= would drop it, unlike NOT (bytes < 100 OR ...).
		{"negated log field", `NOT bytes < 100`, "NOT (toFloat64OrDefault(fields.`bytes`::String, nan) < 100)"},
		{"negated in a group", `NOT (bytes < 100 OR a = "1")`, "NOT (toFloat64OrDefault(fields.`bytes`::String, nan) < 100 OR "},
		{"negated in an OR", `a = "1" OR NOT bytes >= 5`, "OR NOT (toFloat64OrDefault(fields.`bytes`::String, nan) >= 5)"},
		{"negated field operand", `NOT src_bytes > field(dst_bytes)`, "NOT (toFloat64OrDefault(fields.`src_bytes`::String, nan) > toFloat64OrDefault(fields.`dst_bytes`::String, nan))"},
		{"negated aggregate", `* | groupBy(user) | count() | NOT _count > 5`, "HAVING NOT (_count > 5)"},
		{"per-row field", `* | x := lower(bytes) | x > 5`, "toFloat64OrDefault(lower(fields.`bytes`::String), nan) > 5"},
		{"case branch", `* | case { status >= 500 | sev := "crit" ; * | sev := "ok" }`, "toFloat64OrDefault(fields.`status`::String, nan) >= 500"},
		{"chain step", `* | chain(user) { bytes > 100; ev = "y" }`, "sequenceMatch('(?1)(?2)')(toUInt64(toUnixTimestamp64Milli(timestamp)), toFloat64OrDefault(fields.`bytes`::String, nan) > 100, "},
		{"field operand", `src_bytes > field(dst_bytes)`, "toFloat64OrDefault(fields.`src_bytes`::String, nan) > toFloat64OrDefault(fields.`dst_bytes`::String, nan)"},
		{"computed field operand", `* | groupBy(user) | count() | count > field(user)`, "toFloat64OrDefault(toString(_count), nan) > toFloat64OrDefault(user, nan)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			sql, err := TranslateToSQL(pipeline, fieldOperandOpts())
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			if !strings.Contains(sql, tt.want) {
				t.Errorf("SQL should contain %q\nGot: %s", tt.want, sql)
			}
			for _, bad := range []string{"toFloat64OrZero", "toFloat64OrNull"} {
				if strings.Contains(sql, bad) {
					t.Errorf("SQL should not use %s\nGot: %s", bad, sql)
				}
			}
		})
	}
}

// A geoip miss (private, missing or non-IPv4 address) must read empty like any
// other non-number, not a real 0 that asn < 1000 or latitude < 10 would match.
func TestGeoipNumericMissIsNotZero(t *testing.T) {
	for _, col := range []string{"asn", "latitude", "longitude"} {
		t.Run(col, func(t *testing.T) {
			sql := mustTranslate(t, `* | geoip(field=src_ip, include=[`+col+`]) | `+col+` < 1000`, dictOpts())
			if !strings.Contains(sql, "ifNull(toString(dictGetOrNull(") {
				t.Errorf("miss should read as '':\n%s", sql)
			}
			for _, bad := range []string{"toUInt32(0)", "toFloat64(0)"} {
				if strings.Contains(sql, bad) {
					t.Errorf("SQL should not default to %s:\n%s", bad, sql)
				}
			}
		})
	}
}

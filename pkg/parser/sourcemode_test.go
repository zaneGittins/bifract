package parser

import (
	"strings"
	"testing"
	"time"
)

func icebergOpts() QueryOptions {
	// Models a fully-evolved table: every field the current build promotes has its
	// `_ice_` column present. Real callers read this set from the table's own
	// schema (Catalog.TablePromotedFields), since an older table may carry fewer.
	promoted := make(map[string]bool)
	for _, f := range IcePromotedFields() {
		promoted[f] = true
	}
	return QueryOptions{
		StartTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            5000,
		SourceMode:         SourceIceberg,
		UseIngestTimestamp: true,
		TableName:          "icebergS3('http://x/t')",
		IcePromoted:        promoted,
	}
}

func translateIceberg(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	res, err := TranslateToSQLWithOrder(pipeline, icebergOpts())
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return res.SQL
}

func TestIcebergSourceMode(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		wantContain    []string
		wantNotContain []string
	}{
		{
			name:  "promoted field equality emits dual predicate",
			query: `src_ip="1.2.3.4"`,
			wantContain: []string{
				"JSONExtractString(norm_log, 'src_ip') = '1.2.3.4'",
				"_ice_src_ip = '1.2.3.4'",
				"_ice_src_ip IS NULL",
				"FROM icebergS3('http://x/t')",
				"ingest_timestamp >=",
			},
			wantNotContain: []string{"fields.`src_ip`::String", "toString(fields)", "fields['src_ip']"},
		},
		{
			name:           "unpromoted field equality extracts from norm_log",
			query:          `status_code="500"`,
			wantContain:    []string{"JSONExtractString(norm_log, 'status_code') = '500'"},
			wantNotContain: []string{"_ice_status_code", "fields.`status_code`::String", "fields['status_code']"},
		},
		{
			name:           "numeric comparison casts norm_log extraction",
			query:          `duration>5`,
			wantContain:    []string{"toFloat64OrZero(JSONExtractString(norm_log, 'duration')) > 5"},
			wantNotContain: []string{"fields.`duration`::String", "fields['duration']"},
		},
		{
			name:           "free-text search targets norm_log",
			query:          `"powershell"`,
			wantContain:    []string{"norm_log"},
			wantNotContain: []string{"toString(fields)", "fields['"},
		},
		{
			name:           "groupby extracts from norm_log, not JSON sub-column",
			query:          `event_id=* | groupby(event_id)`,
			wantContain:    []string{"JSONExtractString(norm_log, 'event_id') AS event_id", "GROUP BY event_id"},
			wantNotContain: []string{"fields.`event_id`::String", "fields['event_id']"},
		},
		{
			name:           "aggregate arg over unpromoted field extracts from norm_log",
			query:          `* | groupby(src_ip) | max(bytes)`,
			wantContain:    []string{"JSONExtractString(norm_log, 'src_ip')", "JSONExtractString(norm_log, 'bytes')"},
			wantNotContain: []string{"fields.`", "fields['"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := translateIceberg(t, tt.query)
			for _, w := range tt.wantContain {
				if !strings.Contains(sql, w) {
					t.Errorf("SQL missing %q\ngot: %s", w, sql)
				}
			}
			for _, w := range tt.wantNotContain {
				if strings.Contains(sql, w) {
					t.Errorf("SQL should not contain %q\ngot: %s", w, sql)
				}
			}
		})
	}
}

// extractFieldName must be the exact inverse of mapFieldRef (which escapes the
// key via escapeString) so group-by alias recovery matches the original field.
func TestIcebergFieldRefRoundTrip(t *testing.T) {
	for _, name := range []string{
		"src_ip", "http.method", "a.b.c",
		`dir\path`,        // backslash: escapeString doubles it
		`o'brien`,         // single quote
		`weird\'mix`,      // backslash + quote adjacency
		`comma,paren)key`, // punctuation that must not confuse the parser
	} {
		if got := extractFieldName(mapFieldRef(name)); got != name {
			t.Errorf("round-trip failed: mapFieldRef(%q) -> extractFieldName = %q", name, got)
		}
	}
}

func TestIcebergRejectsUnsupported(t *testing.T) {
	for _, q := range []string{
		`::Stringuser=bob | lowercase(user)`,
		`* | model_lookup(beacon, dst_ip)`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			// A parse failure is an acceptable rejection too, but we specifically
			// want translate-time rejection when it parses.
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, icebergOpts()); err == nil {
			t.Errorf("expected iceberg rejection for %q, got nil", q)
		}
	}
}

// TestIcebergPromotedSetIsPerTable covers the dormant-archive case: a table whose
// schema predates a growth of the promoted set must never have the newer `_ice_`
// columns referenced. ClickHouse fails such a query outright with
// UNKNOWN_IDENTIFIER, so this is a correctness guard, not a performance one.
func TestIcebergPromotedSetIsPerTable(t *testing.T) {
	translate := func(t *testing.T, query string, promoted map[string]bool) string {
		t.Helper()
		pipeline, err := ParseQuery(query)
		if err != nil {
			t.Fatalf("parse %q: %v", query, err)
		}
		opts := icebergOpts()
		opts.IcePromoted = promoted
		res, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("translate %q: %v", query, err)
		}
		return res.SQL
	}

	t.Run("field absent from this table emits no _ice_ reference", func(t *testing.T) {
		// src_ip is promoted by the build, but this table does not carry the column.
		for _, q := range []string{`src_ip="1.2.3.4"`, `src_ip="1.2.3.4","5.6.7.8"`} {
			sql := translate(t, q, map[string]bool{"dst_ip": true})
			if strings.Contains(sql, "_ice_src_ip") {
				t.Errorf("referenced a column absent from the table: %s", sql)
			}
			if !strings.Contains(sql, "JSONExtractString(norm_log, 'src_ip')") {
				t.Errorf("lost the always-correct norm_log predicate: %s", sql)
			}
		}
	})

	t.Run("nil set is safe and prunes nothing", func(t *testing.T) {
		sql := translate(t, `src_ip="1.2.3.4"`, nil)
		if strings.Contains(sql, "_ice_") {
			t.Errorf("nil promoted set must not emit _ice_ refs: %s", sql)
		}
		if !strings.Contains(sql, "JSONExtractString(norm_log, 'src_ip')") {
			t.Errorf("lost the always-correct norm_log predicate: %s", sql)
		}
	})

	t.Run("field present on this table still prunes", func(t *testing.T) {
		sql := translate(t, `src_ip="1.2.3.4"`, map[string]bool{"src_ip": true})
		if !strings.Contains(sql, "_ice_src_ip = '1.2.3.4'") ||
			!strings.Contains(sql, "_ice_src_ip IS NULL") {
			t.Errorf("dropped pruning for a column the table has: %s", sql)
		}
	})
}

func TestHotModeUnchangedForPromotedField(t *testing.T) {
	// Sanity: hot mode (default) still emits the JSON sub-column form.
	pipeline, err := ParseQuery(`src_ip="1.2.3.4"`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := TranslateToSQLWithOrder(pipeline, QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "fields.`src_ip`::String") {
		t.Errorf("hot mode should use JSON sub-column, got: %s", res.SQL)
	}
	if strings.Contains(res.SQL, "_ice_") || strings.Contains(res.SQL, "fields['src_ip']") {
		t.Errorf("hot mode leaked iceberg codegen, got: %s", res.SQL)
	}
}

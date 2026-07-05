package parser

import (
	"strings"
	"testing"
	"time"
)

func icebergOpts() QueryOptions {
	return QueryOptions{
		StartTime:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:            5000,
		SourceMode:         SourceIceberg,
		UseIngestTimestamp: true,
		TableName:          "icebergS3('http://x/t')",
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
				"fields['src_ip'] = '1.2.3.4'",
				"_ice_src_ip = '1.2.3.4'",
				"_ice_src_ip IS NULL",
				"FROM icebergS3('http://x/t')",
				"ingest_timestamp >=",
				"toString(fields) AS norm_log",
			},
			wantNotContain: []string{"fields.`src_ip`", "hasToken"},
		},
		{
			name:           "unpromoted field equality is plain MAP access",
			query:          `status_code="500"`,
			wantContain:    []string{"fields['status_code'] = '500'"},
			wantNotContain: []string{"_ice_status_code", "fields.`status_code`"},
		},
		{
			name:           "numeric comparison casts MAP value",
			query:          `duration>5`,
			wantContain:    []string{"toFloat64OrZero(fields['duration']) > 5"},
			wantNotContain: []string{"fields.`duration`"},
		},
		{
			name:           "free-text search targets toString(fields)",
			query:          `"powershell"`,
			wantContain:    []string{"toString(fields)"},
			wantNotContain: []string{"lower(norm_log)", "match(norm_log"},
		},
		{
			name:           "groupby uses MAP access, not JSON sub-column",
			query:          `event_id=* | groupby(event_id)`,
			wantContain:    []string{"fields['event_id'] AS event_id", "GROUP BY event_id"},
			wantNotContain: []string{"fields.`event_id`"},
		},
		{
			name:           "aggregate arg over unpromoted field casts MAP value",
			query:          `* | groupby(src_ip) | max(bytes)`,
			wantContain:    []string{"fields['src_ip']", "fields['bytes']"},
			wantNotContain: []string{"fields.`"},
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

func TestIcebergRejectsUnsupported(t *testing.T) {
	for _, q := range []string{
		`user=bob | lowercase(user)`,
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
	if !strings.Contains(res.SQL, "fields.`src_ip`") {
		t.Errorf("hot mode should use JSON sub-column, got: %s", res.SQL)
	}
	if strings.Contains(res.SQL, "_ice_") || strings.Contains(res.SQL, "fields['src_ip']") {
		t.Errorf("hot mode leaked iceberg codegen, got: %s", res.SQL)
	}
}

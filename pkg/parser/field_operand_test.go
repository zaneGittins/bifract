package parser

import (
	"strings"
	"testing"
	"time"
)

func fieldOperandOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "11111111-1111-1111-1111-111111111111",
	}
}

// field(name) makes the right-hand side of a comparison another field instead of
// a literal. Before it, `src_port = dst_port` silently matched the string
// "dst_port".
func TestFieldOperandSQL(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		iceberg        bool
		wantContain    []string
		wantNotContain []string
	}{
		{
			name:        "equality compares the two sub-columns",
			query:       `src_port = field(dst_port)`,
			wantContain: []string{"fields.`src_port`::String = fields.`dst_port`::String"},
			// The field name must never be quoted as a literal value.
			wantNotContain: []string{"'dst_port'"},
		},
		{
			name:        "ordering coerces both sides to Float64",
			query:       `src_bytes > field(dst_bytes)`,
			wantContain: []string{"toFloat64OrZero(fields.`src_bytes`::String) > toFloat64OrZero(fields.`dst_bytes`::String)"},
		},
		{
			name:  "inequality guards both sides against an absent field",
			query: `src_ip != field(dst_ip)`,
			wantContain: []string{
				"fields.`src_ip`::String IS NULL OR fields.`dst_ip`::String IS NULL OR fields.`src_ip`::String != fields.`dst_ip`::String",
			},
		},
		{
			name:        "works in a pipeline stage",
			query:       `* | src_port = field(dst_port)`,
			wantContain: []string{"fields.`src_port`::String = fields.`dst_port`::String"},
		},
		{
			name:  "binds as one operand inside AND/OR",
			query: `* | src_port = field(dst_port) AND action="allow"`,
			wantContain: []string{
				"fields.`src_port`::String = fields.`dst_port`::String",
				"fields.`action`::String = 'allow'",
			},
		},
		{
			name:  "OR between two field comparisons",
			query: `user = field(owner) OR host = field(target)`,
			wantContain: []string{
				"fields.`user`::String = fields.`owner`::String OR fields.`host`::String = fields.`target`::String",
			},
		},
		{
			name:        "NOT negates the whole comparison",
			query:       `NOT src_port = field(dst_port)`,
			wantContain: []string{"NOT (fields.`src_port`::String = fields.`dst_port`::String)"},
		},
		{
			name:        "a pipeline-produced column is coerced, not read as a log field",
			query:       `* | levenshtein(a, b) | _distance > field(threshold)`,
			wantContain: []string{"toFloat64OrZero(toString(damerauLevenshteinDistance("},
		},
		{
			// A grouping key is stored as its alias or as the expression it resolved
			// from depending on whether an aggregate command has rewritten the stage,
			// so recognising it must not depend on that order.
			name:        "group key is recognised without an explicit aggregate",
			query:       `* | groupby(host) | count > field(host)`,
			wantContain: []string{"toFloat64OrZero(host)"},
		},
		{
			name:        "a dotted group key resolves to its quoted alias",
			query:       `* | groupBy(winlog.user) | count() | count > field(winlog.user)`,
			wantContain: []string{"toFloat64OrZero(`winlog.user`)"},
		},
		{
			// _count is UInt64: toFloat64OrZero on it is a code 43, so the bare
			// aggregate has to be coerced through toString like any other
			// pipeline output.
			name:           "an aggregate may be compared with a group key",
			query:          `* | groupBy(user) | count() | count > field(user)`,
			wantContain:    []string{"HAVING toFloat64OrZero(toString(_count)) > toFloat64OrZero(user)"},
			wantNotContain: []string{"toFloat64OrZero(_count)"},
		},
		{
			name:        "a DateTime operand is compared as text, not as a string column",
			query:       `seen_at = field(timestamp)`,
			wantContain: []string{"fields.`seen_at`::String = toString(timestamp)"},
		},
		{
			// toFloat64OrZero(toString(datetime)) silently reads 0; epoch millis order.
			name:           "two DateTime operands order by epoch millis",
			query:          `* | ingest_timestamp > field(timestamp)`,
			wantContain:    []string{"toUnixTimestamp64Milli(ingest_timestamp) > toUnixTimestamp64Milli(timestamp)"},
			wantNotContain: []string{"toFloat64OrZero(toString(timestamp))"},
		},
		{
			name:        "a bare aggregate on the right resolves to its alias",
			query:       `* | groupBy(user) | count() | user = field(count)`,
			wantContain: []string{"toString(_count)"},
			// never a JSON field literally named "count"
			wantNotContain: []string{"fields.`count`"},
		},
		{
			name:        "timestamp is coerced to text before a string comparison",
			query:       `timestamp = field(seen_at)`,
			wantContain: []string{"toString(timestamp) = fields.`seen_at`::String"},
		},
		{
			name:        "archive reads resolve both sides against norm_log",
			query:       `src_port = field(dst_port)`,
			iceberg:     true,
			wantContain: []string{"JSONExtractString(norm_log, 'src_port') = JSONExtractString(norm_log, 'dst_port')"},
		},
	}

	opts := fieldOperandOpts()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			runOpts := opts
			if tt.iceberg {
				runOpts.SourceMode = SourceIceberg
			}
			sql, err := TranslateToSQL(pipeline, runOpts)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			for _, want := range tt.wantContain {
				if !strings.Contains(sql, want) {
					t.Errorf("SQL should contain %q\nGot: %s", want, sql)
				}
			}
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(sql, notWant) {
					t.Errorf("SQL should NOT contain %q\nGot: %s", notWant, sql)
				}
			}
		})
	}
}

func TestFieldOperandErrors(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{
			name:    "multi-value operator",
			query:   `src_port =~ field(dst_port)`,
			wantErr: "field() cannot be used with =~",
		},
		{
			name:    "empty argument",
			query:   `src_port = field()`,
			wantErr: "field() expects a field name",
		},
		{
			name:    "name that is not a field reference",
			query:   `src_port = field("a/b")`,
			wantErr: "invalid field name",
		},
		{
			name:    "per-row field compared with an aggregate",
			query:   `* | groupBy(user) | count() | count > field(threshold)`,
			wantErr: "does not survive the aggregation",
		},
		{
			// A name the query never grouped by is gone after the aggregation, and
			// the message has to say which side is the problem.
			name:    "field that was never grouped",
			query:   `* | groupBy(user) | count() | count > field(other)`,
			wantErr: "other is a per-row field and does not survive the aggregation",
		},
	}

	opts := fieldOperandOpts()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err == nil {
				_, err = TranslateToSQL(pipeline, opts)
			}
			if err == nil {
				t.Fatalf("expected an error for %s", tt.query)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should mention %q", err, tt.wantErr)
			}
		})
	}
}

// Alert queries project only the fields their conditions reference, so the
// right-hand field has to be collected alongside the left-hand one.
func TestFieldOperandIsProjectedForAlerts(t *testing.T) {
	pipeline, err := ParseQuery(`src_port = field(dst_port)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	opts := fieldOperandOpts()
	opts.UseIngestTimestamp = true
	sql, err := TranslateToSQL(pipeline, opts)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	for _, want := range []string{"AS `src_port`", "AS `dst_port`"} {
		if !strings.Contains(sql, want) {
			t.Errorf("alert projection should contain %q\nGot: %s", want, sql)
		}
	}
}

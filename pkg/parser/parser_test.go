package parser

import (
	"strings"
	"testing"
	"time"
)

func TestBasicFieldEquals(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "simple field equals",
			query:   "status=200",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`status`.:String = '200'")
			},
		},
		{
			name:    "field with underscore",
			query:   "event_id=11",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`event_id`.:String = '11'")
			},
		},
		{
			name:    "field with dot",
			query:   "source.ip=192.168.1.1",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`source%2Eip`.:String = '192.168.1.1'")
			},
		},
		{
			name:    "wildcard value means field is non-empty",
			query:   "event_id=*",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "fields.`event_id`.:String != ''")
			},
		},
		{
			name:    "wildcard with groupby",
			query:   "event_id=* | groupby(event_id)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "fields.`event_id`.:String != ''") &&
					strings.Contains(sql, "GROUP BY")
			},
		},
		{
			name:    "negated wildcard means field is empty or missing",
			query:   "event_type!=*",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "fields.`event_type`.:String IS NULL") &&
					strings.Contains(sql, "fields.`event_type`.:String = ''")
			},
		},
		{
			name:    "wildcard followed by implicit AND condition",
			query:   `url=* location="/var/log/webapp.log"`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "fields.`url`.:String != ''") &&
					strings.Contains(sql, "fields.`location`.:String = '/var/log/webapp.log'")
			},
		},
		{
			name:    "not-equal string after pipe includes NULL",
			query:   `event_id=* | program_name!="suricata"`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "fields.`event_id`.:String != ''") &&
					strings.Contains(sql, "fields.`program_name`.:String IS NULL") &&
					strings.Contains(sql, "fields.`program_name`.:String != 'suricata'")
			},
		},
		{
			name:    "negated in function",
			query:   "!in(program_name,values=[sshd,opensearch-dashboards]) | groupby(program_name)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "NOT IN ('sshd', 'opensearch-dashboards')") &&
					strings.Contains(sql, "GROUP BY")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestRegexQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "case insensitive regex",
			query:   "message=/error/i",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "match(fields.`message`.:String, '(?i)error')")
			},
		},
		{
			name:    "regex with dollar anchor",
			query:   "filename=/\\.exe$/i",
			wantErr: false,
			checkSQL: func(sql string) bool {
				// Check that the regex pattern is properly escaped
				return containsSubstr([]string{sql}, "match(fields.`filename`.:String, '(?i)\\\\.exe$')")
			},
		},
		{
			name:    "regex with dot and dollar",
			query:   "target_filename=/\\.exe$/i",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "match(fields.`target_filename`.:String, '(?i)\\\\.exe$')")
			},
		},
		{
			name:    "regex without flags",
			query:   "path=/var\\/log/",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "match(fields.`path`.:String, 'var\\\\/log')")
			},
		},
		{
			name:    "complex regex pattern",
			query:   "url=/https?:\\/\\/.*/i",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "(?i)https?:\\\\/\\\\/")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestBooleanLogic(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "AND operator",
			query:   "status=500 AND service=api",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`status`.:String = '500'") &&
					containsSubstr([]string{sql}, "fields.`service`.:String = 'api'")
			},
		},
		{
			name:    "OR operator",
			query:   "status=404 OR status=500",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`status`.:String = '404'") &&
					containsSubstr([]string{sql}, "fields.`status`.:String = '500'")
			},
		},
		{
			name:    "NOT operator",
			query:   "NOT status=200",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "NOT (fields.`status`.:String = '200')")
			},
		},
		{
			name:    "Complex boolean",
			query:   "status=500 AND (service=api OR service=web)",
			wantErr: false,
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

// extractWhere extracts the application WHERE clause from a full SQL string,
// stripping the time/fractal/ORDER BY boilerplate so tests can assert on the
// exact logical structure.
func extractWhere(sql string) string {
	// The standard prefix ends with "AND " after the timestamp/fractal clauses.
	// Find the last standard "AND " before user conditions by looking for the
	// pattern after the timestamp range.
	const orderBy = " ORDER BY"
	end := strings.Index(sql, orderBy)
	if end < 0 {
		end = len(sql)
	}

	// Find the end of boilerplate: after "AND fractal_id = '...'" or after timestamp range
	// The WHERE clause has: timestamp >= ... AND timestamp <= ... [AND fractal_id = '...'] AND <user conditions>
	// We want to strip everything up to and including the last boilerplate AND.
	markers := []string{
		"AND fractal_id = '' AND ",
	}
	for _, m := range markers {
		if idx := strings.Index(sql, m); idx >= 0 {
			return sql[idx+len(m) : end]
		}
	}
	// If no fractal_id, look after timestamp range
	tsMarker := "' AND "
	idx := strings.Index(sql, "timestamp >=")
	if idx >= 0 {
		// Find second timestamp clause
		secondTs := strings.Index(sql[idx:], "timestamp <=")
		if secondTs >= 0 {
			afterSecondTs := idx + secondTs
			closeQuote := strings.Index(sql[afterSecondTs:], tsMarker)
			if closeQuote >= 0 {
				start := afterSecondTs + closeQuote + len(tsMarker)
				return sql[start:end]
			}
		}
	}
	return sql[:end]
}

func TestNOTGroupParentheses(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantErr   bool
		wantWhere string         // exact WHERE clause match (preferred)
		checkSQL  func(string) bool // fallback for complex assertions
	}{
		// --- Basic NOT ---
		{
			name:      "NOT single condition",
			query:     `NOT user="admin"`,
			wantWhere: "NOT (fields.`user`.:String = 'admin')",
		},
		{
			name:      "NOT single regex",
			query:     `NOT image=/powershell/i`,
			wantWhere: "NOT (match(fields.`image`.:String, '(?i)powershell'))",
		},
		{
			name:      "NOT single condition in parens",
			query:     `NOT (a=1)`,
			wantWhere: "NOT (fields.`a`.:String = '1')",
		},
		{
			name:      "single condition in parens is unwrapped",
			query:     `(a=1)`,
			wantWhere: "fields.`a`.:String = '1'",
		},

		// --- NOT with grouped OR ---
		{
			name:      "NOT (A OR B) wraps entire group",
			query:     `NOT (user="admin" OR user="root")`,
			wantWhere: "NOT (fields.`user`.:String = 'admin' OR fields.`user`.:String = 'root')",
		},
		{
			name:      "NOT (A OR B OR C) wraps entire group",
			query:     `NOT (a=1 OR b=2 OR c=3)`,
			wantWhere: "NOT (fields.`a`.:String = '1' OR fields.`b`.:String = '2' OR fields.`c`.:String = '3')",
		},
		{
			name:      "NOT (A OR B OR C) with many values",
			query:     `NOT (event_id=1 OR event_id=2 OR event_id=3 OR event_id=4 OR event_id=5)`,
			wantWhere: "NOT (fields.`event_id`.:String = '1' OR fields.`event_id`.:String = '2' OR fields.`event_id`.:String = '3' OR fields.`event_id`.:String = '4' OR fields.`event_id`.:String = '5')",
		},

		// --- NOT with grouped AND ---
		{
			name:      "NOT (A AND B) wraps entire group",
			query:     `NOT (a=1 AND b=2)`,
			wantWhere: "NOT (fields.`a`.:String = '1' AND fields.`b`.:String = '2')",
		},
		{
			name:      "NOT (A AND B) with regex and string",
			query:     `NOT (image=/cmd/i AND user="admin")`,
			wantWhere: "NOT (match(fields.`image`.:String, '(?i)cmd') AND fields.`user`.:String = 'admin')",
		},
		{
			name:      "NOT group with implicit AND",
			query:     `NOT (a=1 b=2)`,
			wantWhere: "NOT (fields.`a`.:String = '1' AND fields.`b`.:String = '2')",
		},

		// --- Condition AND NOT group ---
		{
			name:      "A AND NOT (B OR C)",
			query:     `event_id=1 AND NOT (user="admin" OR user="root")`,
			wantWhere: "(fields.`event_id`.:String = '1' AND NOT (fields.`user`.:String = 'admin' OR fields.`user`.:String = 'root'))",
		},
		{
			name:      "A AND NOT (B AND C)",
			query:     `event_id=1 AND NOT (user="admin" AND image=/cmd/i)`,
			wantWhere: "(fields.`event_id`.:String = '1' AND NOT (fields.`user`.:String = 'admin' AND match(fields.`image`.:String, '(?i)cmd')))",
		},

		// --- Multiple NOT groups ---
		{
			name:      "NOT (A OR B) AND NOT (C OR D)",
			query:     `NOT (user="admin" OR user="root") AND NOT (image=/cmd/i OR image=/powershell/i)`,
			wantWhere: "(NOT (fields.`user`.:String = 'admin' OR fields.`user`.:String = 'root') AND NOT (match(fields.`image`.:String, '(?i)cmd') OR match(fields.`image`.:String, '(?i)powershell')))",
		},

		// --- Groups without NOT ---
		{
			name:      "(A OR B) AND (C OR D) preserves both groups",
			query:     `(a=1 OR a=2) AND (b=3 OR b=4)`,
			wantWhere: "((fields.`a`.:String = '1' OR fields.`a`.:String = '2') AND (fields.`b`.:String = '3' OR fields.`b`.:String = '4'))",
		},
		{
			name:      "OR between groups: (A AND B) OR (C AND D)",
			query:     `(a=1 AND b=2) OR (c=3 AND d=4)`,
			wantWhere: "((fields.`a`.:String = '1' AND fields.`b`.:String = '2') OR (fields.`c`.:String = '3' AND fields.`d`.:String = '4'))",
		},

		// --- Mixed ungrouped and groups ---
		{
			name:      "A OR (B AND C) OR D",
			query:     `a=1 OR (b=2 AND c=3) OR d=4`,
			wantWhere: "(fields.`a`.:String = '1' OR (fields.`b`.:String = '2' AND fields.`c`.:String = '3') OR fields.`d`.:String = '4')",
		},
		{
			name:      "(A OR B) OR C OR (D OR E)",
			query:     `(a=1 OR a=2) OR b=3 OR (c=4 OR c=5)`,
			wantWhere: "((fields.`a`.:String = '1' OR fields.`a`.:String = '2') OR fields.`b`.:String = '3' OR (fields.`c`.:String = '4' OR fields.`c`.:String = '5'))",
		},
		{
			name:      "(A OR B) AND NOT (C OR D) AND E",
			query:     `(event_id=1 OR event_id=3) AND NOT (user="admin" OR user="system") AND image=/powershell/i`,
			wantWhere: "((fields.`event_id`.:String = '1' OR fields.`event_id`.:String = '3') AND NOT (fields.`user`.:String = 'admin' OR fields.`user`.:String = 'system') AND match(fields.`image`.:String, '(?i)powershell'))",
		},

		// --- Double negation ---
		{
			name:      "NOT (NOT A OR B)",
			query:     `NOT (NOT image=/cmd/i OR user="admin")`,
			wantWhere: "NOT (NOT (match(fields.`image`.:String, '(?i)cmd')) OR fields.`user`.:String = 'admin')",
		},
		{
			name:      "NOT (A) AND NOT B (paren vs bare)",
			query:     `NOT (a=1) AND NOT b=2`,
			wantWhere: "(NOT (fields.`a`.:String = '1') AND NOT (fields.`b`.:String = '2'))",
		},

		// --- Double/triple-wrapped parens ---
		{
			name:      "triple nested parens (((A OR B)))",
			query:     `(((a=1 OR b=2)))`,
			wantWhere: "(fields.`a`.:String = '1' OR fields.`b`.:String = '2')",
		},
		{
			name:      "NOT with double-wrapped parens NOT ((A OR B OR C))",
			query:     `field=x AND NOT ((a=1 OR b=2 OR c=3))`,
			wantWhere: "(fields.`field`.:String = 'x' AND NOT (fields.`a`.:String = '1' OR fields.`b`.:String = '2' OR fields.`c`.:String = '3'))",
		},

		// --- Operators inside NOT groups ---
		{
			name:      "NOT with != inside group",
			query:     `NOT (field!=value AND other!=test)`,
			wantWhere: "NOT ((fields.`field`.:String IS NULL OR fields.`field`.:String != 'value') AND (fields.`other`.:String IS NULL OR fields.`other`.:String != 'test'))",
		},
		{
			name:      "NOT group with wildcard",
			query:     `NOT (field=* AND other=value)`,
			wantWhere: "NOT (fields.`field`.:String != '' AND fields.`other`.:String = 'value')",
		},
		{
			name:      "NOT group with regex",
			query:     `NOT (image=/cmd/i OR image=/powershell/i)`,
			wantWhere: "NOT (match(fields.`image`.:String, '(?i)cmd') OR match(fields.`image`.:String, '(?i)powershell'))",
		},

		// --- Sigma-realistic patterns ---
		{
			name:  "Sigma: selection AND NOT filter with OR values",
			query: `(parent_image=/.*\\addinutil\.exe$/i AND NOT ((image=/.*\\conhost\.exe$/i OR image=/.*\\werfault\.exe$/i)))`,
			checkSQL: func(sql string) bool {
				w := extractWhere(sql)
				return strings.Contains(w, "match(fields.`parent_image`.:String") &&
					strings.Contains(w, "NOT (match(fields.`image`.:String") &&
					strings.Contains(w, ") OR match(fields.`image`.:String")
			},
		},
		{
			name:  "Sigma: two selections AND'd with NOT filter",
			query: `((image=/powershell/i OR image=/pwsh/i) AND (commandline=/Invoke-WebRequest/i OR commandline=/wget/i) AND NOT (user="SYSTEM"))`,
			checkSQL: func(sql string) bool {
				w := extractWhere(sql)
				return strings.Contains(w, "match(fields.`image`.:String, '(?i)powershell') OR match(fields.`image`.:String, '(?i)pwsh')") &&
					strings.Contains(w, "match(fields.`commandline`.:String, '(?i)Invoke-WebRequest') OR match(fields.`commandline`.:String, '(?i)wget')") &&
					strings.Contains(w, "NOT (fields.`user`.:String = 'SYSTEM')")
			},
		},
		{
			name:  "Sigma: 3 selections + NOT compound filter",
			query: `((image=/ps/i OR image=/pwsh/i) AND (cmd=/iex/i OR cmd=/iwr/i) AND NOT (user="SYSTEM" AND parent=/svchost/i))`,
			checkSQL: func(sql string) bool {
				w := extractWhere(sql)
				return strings.Contains(w, "(match(fields.`image`.:String, '(?i)ps') OR match(fields.`image`.:String, '(?i)pwsh'))") &&
					strings.Contains(w, "(match(fields.`cmd`.:String, '(?i)iex') OR match(fields.`cmd`.:String, '(?i)iwr'))") &&
					strings.Contains(w, "NOT (fields.`user`.:String = 'SYSTEM' AND match(fields.`parent`.:String, '(?i)svchost'))")
			},
		},

		{
			name:    "deeply nested: NOT (group AND group) parses without error",
			query:   `NOT ((a=1 OR a=2) AND (b=3 OR b=4))`,
			wantErr: false,
			// Compound node support correctly wraps NOT around the entire
			// nested expression instead of distributing to individual groups.
			checkSQL: func(sql string) bool {
				w := extractWhere(sql)
				// Should be: NOT ((a group) AND (b group))
				return strings.Contains(w, "NOT (") &&
					!strings.Contains(w, "AND NOT (") &&
					strings.Contains(w, "fields.`a`.:String") &&
					strings.Contains(w, "fields.`b`.:String")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQL() error = %v", err)
				return
			}

			if tt.wantWhere != "" {
				got := extractWhere(sql)
				if got != tt.wantWhere {
					t.Errorf("WHERE clause mismatch for query %q\nwant: %s\ngot:  %s", tt.query, tt.wantWhere, got)
				}
			}

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestGroupByQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "simple groupby",
			query:   "groupby(service)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "groupby with array syntax",
			query:   "groupby([image,target_filename])",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`image`.:String") &&
					containsSubstr([]string{sql}, "fields.`target_filename`.:String") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "groupby with filter",
			query:   "event_id=11 | groupby(service)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`event_id`.:String = '11'") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "groupby multiple fields",
			query:   "groupby(service, level)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`service`.:String") &&
					containsSubstr([]string{sql}, "fields.`level`.:String") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "groupby with limit",
			query:   "event_id=* | groupby(event_id, limit=3)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "GROUP BY") &&
					strings.Contains(sql, "LIMIT 3")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestComplexQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "filter with regex and groupby",
			query:   "event_id=11 | target_filename=/\\.exe$/i | groupby([image,target_filename])",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`event_id`.:String = '11'") &&
					containsSubstr([]string{sql}, "match(fields.`target_filename`.:String, '(?i)\\\\.exe$')") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "table command with specific fields",
			query:   "status=500 | table(timestamp, service, message)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "timestamp") &&
					containsSubstr([]string{sql}, "fields.`service`.:String") &&
					containsSubstr([]string{sql}, "fields.`message`.:String")
			},
		},
		{
			name:    "groupby with table",
			query:   "groupby(service) | table(service, count)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "COUNT(*)") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "sort and limit",
			query:   "status=500 | sort(timestamp) | limit(100)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp") &&
					containsSubstr([]string{sql}, "LIMIT 100")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   10000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			t.Logf("Query: %s", tt.query)
			t.Logf("SQL: %s", sql)

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestComparisonOperators(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "greater than",
			query:   "bytes>1000",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toFloat64OrZero(fields.`bytes`.:String) > 1000")
			},
		},
		{
			name:    "less than",
			query:   "duration<100",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toFloat64OrZero(fields.`duration`.:String) < 100")
			},
		},
		{
			name:    "greater than or equal",
			query:   "count>=10",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toFloat64OrZero(fields.`count`.:String) >= 10")
			},
		},
		{
			name:    "not equal",
			query:   "status!=200",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`status`.:String != '200'")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			sql, err := TranslateToSQL(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestFieldAssignment(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "string literal assignment",
			query:   `status_text := "OK"`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, `'OK' AS status_text`)
			},
		},
		{
			name:    "field reference assignment",
			query:   `response_time_ms := response_time`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`response_time`.:String AS response_time_ms")
			},
		},
		{
			name:    "assignment with table command",
			query:   `status_text := "OK" | table(timestamp, status_text)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, `'OK' AS status_text`)
			},
		},
		{
			name:    "multiple assignments",
			query:   `status_text := "OK" | response_time_ms := response_time | table(timestamp, status_text, response_time_ms)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, `'OK' AS status_text`) &&
					containsSubstr([]string{sql}, "fields.`response_time`.:String AS response_time_ms")
			},
		},
		{
			name:    "assignment with groupBy",
			query:   `user_type := "admin" | groupby(user)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, `AS user_type`) &&
					containsSubstr([]string{sql}, `GROUP BY`)
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Check assignments were parsed correctly
			if len(pipeline.Assignments) == 0 {
				t.Errorf("Expected assignments to be parsed, but got none")
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestNewAggregateFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "max function",
			query:   "* | max(response_time)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "max(toFloat64OrNull(fields.`response_time`.:String)) AS _max")
			},
		},
		{
			name:    "min function",
			query:   "* | min(response_time)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "min(toFloat64OrNull(fields.`response_time`.:String)) AS _min")
			},
		},
		{
			name:    "stdDev function",
			query:   "* | stdDev(response_time)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "stddevPop(toFloat64OrNull(fields.`response_time`.:String)) AS stddev_response_time")
			},
		},
		{
			name:    "percentile function",
			query:   "* | percentile(response_time)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "quantiles(0.5, 0.75, 0.99)(toFloat64OrNull(fields.`response_time`.:String)) AS percentile_response_time")
			},
		},
		{
			name:    "median function",
			query:   "* | median(response_time)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "median(toFloat64OrNull(fields.`response_time`.:String)) AS _median")
			},
		},
		{
			name:    "mad function",
			query:   "* | mad(response_time)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "any(_median_val) AS _median") &&
					containsSubstr([]string{sql}, "median(abs(_mad_val - _median_val)) AS _mad")
			},
		},
		{
			name:    "table with max function",
			query:   "* | table(status, max(response_time))",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "max(toFloat64OrNull(fields.`response_time`.:String)) AS _max") &&
					containsSubstr([]string{sql}, "AS status")
			},
		},
		{
			name:    "table with min function",
			query:   "* | table(status, min(response_time))",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "min(toFloat64OrNull(fields.`response_time`.:String)) AS _min") &&
					containsSubstr([]string{sql}, "AS status")
			},
		},
		{
			name:    "groupBy with max",
			query:   "* | groupby(user) | max(bytes)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "max(toFloat64OrNull(fields.`bytes`.:String)) AS _max") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestHeadTailFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "head with default limit",
			query:   "* | head()",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp ASC") &&
					containsSubstr([]string{sql}, "LIMIT 200")
			},
		},
		{
			name:    "head with custom limit",
			query:   "* | head(10)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp ASC") &&
					containsSubstr([]string{sql}, "LIMIT 10")
			},
		},
		{
			name:    "tail with default limit",
			query:   "* | tail()",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp DESC") &&
					containsSubstr([]string{sql}, "LIMIT 200")
			},
		},
		{
			name:    "tail with custom limit",
			query:   "* | tail(50)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp DESC") &&
					containsSubstr([]string{sql}, "LIMIT 50")
			},
		},
		{
			name:    "head with filter",
			query:   `loglevel="ERROR" | head(5)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp ASC") &&
					containsSubstr([]string{sql}, "LIMIT 5") &&
					containsSubstr([]string{sql}, "fields.`loglevel`.:String = 'ERROR'")
			},
		},
		{
			name:    "tail with filter",
			query:   `status=404 | tail(100)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp DESC") &&
					containsSubstr([]string{sql}, "LIMIT 100") &&
					containsSubstr([]string{sql}, "fields.`status`.:String = '404'")
			},
		},
		{
			name:    "head with table command",
			query:   "* | head(20) | table(timestamp, message)",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "ORDER BY timestamp ASC") &&
					containsSubstr([]string{sql}, "LIMIT 20") &&
					containsSubstr([]string{sql}, "AS message")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestStringFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "regex function basic",
			query:   `* | regex("user=(\\w+)")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "extractAllGroups(raw_log,") &&
					containsSubstr([]string{sql}, "AS regex_match")
			},
		},
		{
			name:    "replace function basic",
			query:   `* | replace("error", "ERROR")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "replaceRegexpAll(raw_log,") &&
					containsSubstr([]string{sql}, "'error'") &&
					containsSubstr([]string{sql}, "'ERROR'")
			},
		},
		{
			name:    "concat function basic",
			query:   `* | concat("user,host")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "concat(") &&
					containsSubstr([]string{sql}, "AS _concat")
			},
		},
		{
			name:    "lowercase function basic",
			query:   `* | lowercase("username")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "lower(fields.`username`.:String)") &&
					containsSubstr([]string{sql}, "AS username")
			},
		},
		{
			name:    "replace with output alias",
			query:   `* | replace("\\d+", "NUM", raw_log, clean_message) | table(timestamp, clean_message)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "replaceRegexpAll(raw_log,") &&
					containsSubstr([]string{sql}, "AS clean_message")
			},
		},
		{
			name:    "multiple string operations",
			query:   `* | replace("error", "ERROR") | lowercase("status") | table(timestamp, status)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "lower(fields.`status`.:String)") &&
					containsSubstr([]string{sql}, "AS status")
			},
		},
		{
			name:    "sprintf with as alias",
			query:   `* | sprintf("%s - %s", username, action, as=user_action)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "printf(") &&
					containsSubstr([]string{sql}, "AS user_action")
			},
		},
		{
			name:    "sprintf default alias",
			query:   `* | sprintf("%s:%d", host, port)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "printf(") &&
					containsSubstr([]string{sql}, "AS _sprintf")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestSpecializedFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "eval function simple assignment",
			query:   `* | eval("result = response_time")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`response_time`.:String AS result")
			},
		},
		{
			name:    "eval function with literal",
			query:   `* | eval("status_text = \"OK\"")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, `'OK' AS status_text`)
			},
		},
		{
			name:    "eval function with addition",
			query:   `* | eval("total = bytes_sent + bytes_received")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toFloat64OrNull(fields.`bytes_sent`.:String) + toFloat64OrNull(fields.`bytes_received`.:String) AS total")
			},
		},
		{
			name:    "in function basic",
			query:   `* | in("status", "404,500")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "fields.`status`.:String IN") &&
					containsSubstr([]string{sql}, "'404'") &&
					containsSubstr([]string{sql}, "'500'")
			},
		},
		{
			name:    "case function with field assignments",
			query:   `event_id=1 | case { user=/gittinsz/ | status:="ok"; user=/noveloa/i | status:="ok2"; * | status := "nope"; } | groupby(status)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "match(fields.`user`.:String,") &&
					containsSubstr([]string{sql}, "gittinsz") &&
					containsSubstr([]string{sql}, "(?i)noveloa") &&
					containsSubstr([]string{sql}, "AS status") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "case function with regex patterns",
			query:   `* | case { user=/admin.*/ | role:="administrator"; user=/guest/i | role:="visitor"; * | role:="unknown"; }`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "match(fields.`user`.:String,") &&
					containsSubstr([]string{sql}, "admin.*") &&
					containsSubstr([]string{sql}, "(?i)guest") &&
					containsSubstr([]string{sql}, "AS role")
			},
		},
		{
			name:    "case function without assignments",
			query:   `* | case { status=200 | "OK"; status=404 | "Not Found"; * | "Unknown"; }`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "CASE WHEN") &&
					containsSubstr([]string{sql}, "fields.`status`.:String = '200'") &&
					containsSubstr([]string{sql}, "THEN 'OK'") &&
					containsSubstr([]string{sql}, "ELSE 'Unknown'")
			},
		},
		{
			name:    "eval with table",
			query:   `* | eval("calculated = bytes * 2") | table(timestamp, calculated)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toFloat64OrNull(fields.`bytes`.:String) * 2 AS calculated")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestTimeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "now function",
			query:   "* | now()",
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "now() AS _now")
			},
		},
		{
			name:    "strftime basic",
			query:   `* | strftime("%H")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "formatDateTime(timestamp,") &&
					containsSubstr([]string{sql}, "'%H'") &&
					containsSubstr([]string{sql}, "AS _time")
			},
		},
		{
			name:    "strftime with alias",
			query:   `* | strftime("%H", as=_hour)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "formatDateTime(timestamp,") &&
					containsSubstr([]string{sql}, "AS _hour")
			},
		},
		{
			name:    "bucket function with count",
			query:   `* | bucket("1h", "count()")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toStartOfHour(timestamp) AS time_bucket") &&
					containsSubstr([]string{sql}, "COUNT(*) AS bucket_count") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
		{
			name:    "bucket with sum",
			query:   `* | bucket("1d", "sum(bytes)")`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "toStartOfDay(timestamp) AS time_bucket") &&
					containsSubstr([]string{sql}, "sum(toFloat64OrNull(fields.`bytes`.:String)) AS bucket_sum") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func TestStatsFunction(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:    "multi with count",
			query:   `* | multi(count())`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "COUNT(*) AS _count")
			},
		},
		{
			name:    "multi with multiple functions",
			query:   `* | multi(count(), avg(response_time), sum(bytes))`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "COUNT(*) AS _count") &&
					containsSubstr([]string{sql}, "avg(toFloat64OrNull(fields.`response_time`.:String)) AS _avg") &&
					containsSubstr([]string{sql}, "sum(toFloat64OrNull(fields.`bytes`.:String)) AS _sum")
			},
		},
		{
			name:    "multi with groupBy",
			query:   `* | groupby(user) | multi(avg(response_time), max(bytes))`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return containsSubstr([]string{sql}, "avg(toFloat64OrNull(fields.`response_time`.:String)) AS _avg") &&
					containsSubstr([]string{sql}, "max(toFloat64OrNull(fields.`bytes`.:String)) AS _max") &&
					containsSubstr([]string{sql}, "GROUP BY")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		MaxRows:   1000,
		FractalID: "test-index",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("TranslateToSQLWithOrder() error = %v", err)
				return
			}

			sql := result.SQL
			if tt.checkSQL != nil && !tt.checkSQL(sql) {
				t.Errorf("SQL check failed for query %q\nGot SQL: %s", tt.query, sql)
			}
		})
	}
}

func containsSubstr(slice []string, substr string) bool {
	for _, s := range slice {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

func TestCommentCommand(t *testing.T) {
	baseOpts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	tests := []struct {
		name     string
		query    string
		opts     QueryOptions
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:  "comment with single tag",
			query: `* | comment(tags=security)`,
			opts: QueryOptions{
				StartTime:        baseOpts.StartTime,
				EndTime:          baseOpts.EndTime,
				MaxRows:          baseOpts.MaxRows,
				HasCommentFilter: true,
				CommentLogIDs:    []string{"abc123", "def456"},
			},
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "log_id IN ('abc123', 'def456')")
			},
		},
		{
			name:  "comment with multiple tags",
			query: `* | comment(tags=security,critical)`,
			opts: QueryOptions{
				StartTime:        baseOpts.StartTime,
				EndTime:          baseOpts.EndTime,
				MaxRows:          baseOpts.MaxRows,
				HasCommentFilter: true,
				CommentLogIDs:    []string{"abc123"},
			},
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "log_id IN ('abc123')")
			},
		},
		{
			name:  "comment with keyword",
			query: `* | comment(keyword="timeout")`,
			opts: QueryOptions{
				StartTime:        baseOpts.StartTime,
				EndTime:          baseOpts.EndTime,
				MaxRows:          baseOpts.MaxRows,
				HasCommentFilter: true,
				CommentLogIDs:    []string{"xyz789"},
			},
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "log_id IN ('xyz789')")
			},
		},
		{
			name:  "comment with no matching log IDs returns empty",
			query: `* | comment(tags=nonexistent)`,
			opts: QueryOptions{
				StartTime:        baseOpts.StartTime,
				EndTime:          baseOpts.EndTime,
				MaxRows:          baseOpts.MaxRows,
				HasCommentFilter: true,
				CommentLogIDs:    []string{},
			},
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "1 = 0")
			},
		},
		{
			name:  "comment without pre-processing fails",
			query: `* | comment(tags=security)`,
			opts:  baseOpts,
			wantErr: true,
		},
		{
			name:  "comment with filter and groupby",
			query: `/error/i | comment(tags=security) | groupby(src_ip)`,
			opts: QueryOptions{
				StartTime:        baseOpts.StartTime,
				EndTime:          baseOpts.EndTime,
				MaxRows:          baseOpts.MaxRows,
				HasCommentFilter: true,
				CommentLogIDs:    []string{"abc123"},
			},
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "log_id IN ('abc123')") &&
					strings.Contains(sql, "GROUP BY")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("ParseQuery() error = %v", err)
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQLWithOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(result.SQL) {
				t.Errorf("SQL check failed for %q\nGot SQL: %s", tt.name, result.SQL)
			}
		})
	}
}

func TestExtractCommentParams(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantTags    []string
		wantKeyword string
		wantFound   bool
	}{
		{
			name:      "single tag",
			query:     `* | comment(tags=security)`,
			wantTags:  []string{"security"},
			wantFound: true,
		},
		{
			name:      "multiple tags via comma",
			query:     `* | comment(tags=security,critical)`,
			wantTags:  []string{"security", "critical"},
			wantFound: true,
		},
		{
			name:        "keyword only",
			query:       `* | comment(keyword="timeout")`,
			wantKeyword: "timeout",
			wantFound:   true,
		},
		{
			name:        "keyword and tags",
			query:       `* | comment(keyword="error",tags=security,critical)`,
			wantTags:    []string{"security", "critical"},
			wantKeyword: "error",
			wantFound:   true,
		},
		{
			name:      "no comment function",
			query:     `* | count()`,
			wantFound: false,
		},
		{
			name:      "no args returns found with empty params",
			query:     `* | comment()`,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			tags, keyword, found := ExtractCommentParams(pipeline)
			if found != tt.wantFound {
				t.Errorf("found = %v, want %v", found, tt.wantFound)
			}
			if keyword != tt.wantKeyword {
				t.Errorf("keyword = %q, want %q", keyword, tt.wantKeyword)
			}
			if len(tags) != len(tt.wantTags) {
				t.Errorf("tags = %v, want %v", tags, tt.wantTags)
			} else {
				for i, tag := range tags {
					if tag != tt.wantTags[i] {
						t.Errorf("tags[%d] = %q, want %q", i, tag, tt.wantTags[i])
					}
				}
			}
		})
	}
}

func TestMatchCommand(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		checkSQL func(string) bool
	}{
		{
			name:  "match with include array - non-strict",
			query: `* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_level,country], strict=false)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault(") &&
					strings.Contains(sql, "threat_level") &&
					strings.Contains(sql, "country") &&
					!strings.Contains(sql, "dictHas(")
			},
		},
		{
			name:  "match with include array - strict",
			query: `* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_level], strict=true)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault(") &&
					strings.Contains(sql, "dictHas(")
			},
		},
		{
			name:    "match without dict param",
			query:   `* | match(field=src_ip, column=ip, include=[score])`,
			wantErr: true,
		},
		{
			name:    "match without include",
			query:   `* | match(dict="x", field=src_ip, column=ip)`,
			wantErr: true,
		},
		{
			name:    "wildcard filter on enriched field",
			query:   `* | match(dict="threat_intel", field=src_ip, column=ip, include=[country]) | country=*`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				// FieldKindPerRow inlines the expression into WHERE
				return strings.Contains(sql, "dictGetOrDefault(") &&
					strings.Contains(sql, "!= ''")
			},
		},
		{
			name:  "match followed by groupby on enriched field",
			query: `* | match(dict="threat_intel", field=src_ip, column=ip, include=[country]) | groupby(country)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				// SELECT must use dictGetOrDefault for country, not fields.`country`.:String
				return strings.Contains(sql, "dictGetOrDefault(") &&
					strings.Contains(sql, " AS country") &&
					strings.Contains(sql, "GROUP BY") &&
					strings.Contains(sql, "country") &&
					!strings.Contains(sql, "fields.`country`.:String")
			},
		},
		{
			name:    "match with filter then groupby on different fields",
			query:   `event_id=1 | match(dict="threat_intel", field=src_ip, column=ip, include=[country],strict=true) | country=US | groupby(src_ip)`,
			wantErr: false,
			checkSQL: func(sql string) bool {
				// The inlined dictGetOrDefault expression must appear in WHERE for the filter
				return strings.Contains(sql, "dictGetOrDefault(") &&
					strings.Contains(sql, "GROUP BY") &&
					strings.Contains(sql, "dictHas(") &&
					strings.Contains(sql, "= 'US'")
			},
		},
	}

	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		Dictionaries: map[string]map[string]string{
			"threat_intel": {"ip": "lookup_abc123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("ParseQuery() error = %v", err)
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQLWithOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(result.SQL) {
				t.Errorf("SQL check failed for %q\nGot SQL: %s", tt.name, result.SQL)
			}
		})
	}
}

func TestLookupIPCommand(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		geoIP    bool
		checkSQL func(string) bool
	}{
		{
			name:    "lookupIP with city fields",
			query:   `* | lookupIP(field=src_ip, include=[country,city])`,
			geoIP:   true,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault('geoip_city_lookup'") &&
					strings.Contains(sql, "toIPv4OrDefault(") &&
					strings.Contains(sql, "AS country") &&
					strings.Contains(sql, "AS city")
			},
		},
		{
			name:    "lookupIP with ASN fields",
			query:   `* | lookupIP(field=client_ip, include=[asn,as_org])`,
			geoIP:   true,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault('geoip_asn_lookup'") &&
					strings.Contains(sql, "AS asn") &&
					strings.Contains(sql, "AS as_org")
			},
		},
		{
			name:    "lookupIP with mixed city and ASN fields",
			query:   `* | lookupIP(field=src_ip, include=[country,asn])`,
			geoIP:   true,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "geoip_city_lookup") &&
					strings.Contains(sql, "geoip_asn_lookup")
			},
		},
		{
			name:    "lookupIP without GeoIP enabled",
			query:   `* | lookupIP(field=src_ip, include=[country])`,
			geoIP:   false,
			wantErr: true,
		},
		{
			name:    "lookupIP without field parameter",
			query:   `* | lookupIP(include=[country])`,
			geoIP:   true,
			wantErr: true,
		},
		{
			name:    "lookupIP without include parameter",
			query:   `* | lookupIP(field=src_ip)`,
			geoIP:   true,
			wantErr: true,
		},
		{
			name:    "lookupIP with unknown column",
			query:   `* | lookupIP(field=src_ip, include=[bogus])`,
			geoIP:   true,
			wantErr: true,
		},
		{
			name:    "lookupIP followed by groupby",
			query:   `* | lookupIP(field=src_ip, include=[country]) | groupby(country) | count()`,
			geoIP:   true,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault('geoip_city_lookup'") &&
					strings.Contains(sql, "GROUP BY") &&
					!strings.Contains(sql, "fields.`country`.:String")
			},
		},
		{
			name:    "lookupip alias works",
			query:   `* | lookupip(field=src_ip, include=[country])`,
			geoIP:   true,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault('geoip_city_lookup'")
			},
		},
		{
			name:    "geoip alias works",
			query:   `* | geoip(field=src_ip, include=[country])`,
			geoIP:   true,
			wantErr: false,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "dictGetOrDefault('geoip_city_lookup'")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := QueryOptions{
				StartTime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTime:      time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
				MaxRows:      1000,
				GeoIPEnabled: tt.geoIP,
			}

			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("ParseQuery() error = %v", err)
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQLWithOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if tt.checkSQL != nil && !tt.checkSQL(result.SQL) {
				t.Errorf("SQL check failed for %q\nGot SQL: %s", tt.name, result.SQL)
			}
		})
	}
}

func TestGraphWorldCommand(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantErr   bool
		checkType string
		checkCfg  func(map[string]interface{}) bool
	}{
		{
			name:      "default lat/lon fields",
			query:     `* | graphWorld()`,
			wantErr:   false,
			checkType: "worldmap",
			checkCfg: func(cfg map[string]interface{}) bool {
				return cfg["latField"] == "latitude" && cfg["lonField"] == "longitude"
			},
		},
		{
			name:      "custom lat/lon fields",
			query:     `* | graphWorld(lat=geo_lat, lon=geo_lon)`,
			wantErr:   false,
			checkType: "worldmap",
			checkCfg: func(cfg map[string]interface{}) bool {
				return cfg["latField"] == "geo_lat" && cfg["lonField"] == "geo_lon"
			},
		},
		{
			name:      "with label field",
			query:     `* | graphWorld(label=country)`,
			wantErr:   false,
			checkType: "worldmap",
			checkCfg: func(cfg map[string]interface{}) bool {
				return cfg["labelField"] == "country"
			},
		},
		{
			name:      "with limit",
			query:     `* | graphWorld(limit=500)`,
			wantErr:   false,
			checkType: "worldmap",
			checkCfg: func(cfg map[string]interface{}) bool {
				return cfg["limit"] == 500
			},
		},
		{
			name:      "limit capped at 50000",
			query:     `* | graphWorld(limit=100000)`,
			wantErr:   false,
			checkType: "worldmap",
			checkCfg: func(cfg map[string]interface{}) bool {
				return cfg["limit"] == 50000
			},
		},
		{
			name:      "graphworld alias",
			query:     `* | graphworld()`,
			wantErr:   false,
			checkType: "worldmap",
		},
		{
			name:      "worldmap alias",
			query:     `* | worldmap()`,
			wantErr:   false,
			checkType: "worldmap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := QueryOptions{
				StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
				MaxRows:   1000,
			}

			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("ParseQuery() error = %v", err)
				return
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQLWithOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if result.ChartType != tt.checkType {
				t.Errorf("ChartType = %q, want %q", result.ChartType, tt.checkType)
			}

			if tt.checkCfg != nil && !tt.checkCfg(result.ChartConfig) {
				t.Errorf("ChartConfig check failed, got %+v", result.ChartConfig)
			}
		})
	}
}

func TestTransformConditionGroupby(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	tests := []struct {
		name     string
		query    string
		checkSQL func(string) bool
	}{
		{
			name:  "lowercase + condition + groupby inlines expression",
			query: `* | lowercase(hostname) | hostname=abc | groupby(image)`,
			checkSQL: func(sql string) bool {
				// WHERE must inline the lower() expression, not use raw JSON ref
				return strings.Contains(sql, "lower(") &&
					strings.Contains(sql, "= 'abc'") &&
					strings.Contains(sql, "GROUP BY")
			},
		},
		{
			name:  "uppercase + condition + groupby inlines expression",
			query: `* | uppercase(status) | status=OK | groupby(user)`,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "upper(") &&
					strings.Contains(sql, "= 'OK'") &&
					strings.Contains(sql, "GROUP BY")
			},
		},
		{
			name:  "strftime + condition + groupby still works",
			query: `* | strftime("%Y-%m-%d") | _time=2026-01-01 | groupby(_time)`,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "formatDateTime(") &&
					strings.Contains(sql, "= '2026-01-01'") &&
					strings.Contains(sql, "GROUP BY")
			},
		},
		{
			name:  "transform condition without groupby",
			query: `* | lowercase(hostname) | hostname=abc`,
			checkSQL: func(sql string) bool {
				return strings.Contains(sql, "lower(") &&
					strings.Contains(sql, "= 'abc'")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}
			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("TranslateToSQLWithOrder() error = %v", err)
			}
			if !tt.checkSQL(result.SQL) {
				t.Errorf("SQL check failed for %q\nGot SQL: %s", tt.name, result.SQL)
			}
		})
	}
}

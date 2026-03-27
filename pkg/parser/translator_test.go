package parser

import (
	"strings"
	"testing"
	"time"
)

func TestBasicQueries(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	tests := []struct {
		name        string
		query       string
		wantContain []string
		wantNotContain []string
	}{
		{
			name:  "Simple wildcard",
			query: "*",
			wantContain: []string{
				"SELECT timestamp, raw_log, log_id, toString(fields) AS fields, fractal_id FROM logs",
				"ORDER BY timestamp DESC",
			},
		},
		{
			name:  "Field filter",
			query: "image=/powershell/i",
			wantContain: []string{
				"match(fields.`image`.:String, '(?i)powershell')",
				"ORDER BY timestamp DESC",
			},
		},
		{
			name:  "Count only",
			query: "* | count()",
			wantContain: []string{
				"SELECT COUNT(*) AS _count FROM",
			},
			wantNotContain: []string{
				"ORDER BY timestamp", // Should NOT order when using count() without groupBy
			},
		},
		{
			name:  "Count with filter",
			query: "image=/powershell/i | count()",
			wantContain: []string{
				"SELECT COUNT(*) AS _count FROM",
				"match(fields.`image`.:String, '(?i)powershell')",
			},
			wantNotContain: []string{
				"ORDER BY timestamp",
			},
		},
		{
			name:  "GroupBy single field",
			query: "* | groupBy(image)",
			wantContain: []string{
				"GROUP BY",
				"fields.`image`.:String AS image",
			},
		},
		{
			name:  "GroupBy with count",
			query: "* | groupBy(image) | count()",
			wantContain: []string{
				"GROUP BY",
				"COUNT(*) AS",
				"fields.`image`.:String",
			},
		},
		{
			name:  "GroupBy multiple fields",
			query: "* | groupBy(image, user)",
			wantContain: []string{
				"GROUP BY",
				"fields.`image`.:String AS image",
				"fields.`user`.:String AS user",
			},
		},
		{
			name:  "Sum function",
			query: "* | sum(bytes)",
			wantContain: []string{
				"sum(toFloat64OrNull(fields.`bytes`.:String)) AS _sum",
			},
		},
		{
			name:  "Sum with groupBy",
			query: "* | groupBy(image) | sum(bytes)",
			wantContain: []string{
				"GROUP BY",
				"sum(toFloat64OrNull(fields.`bytes`.:String))",
				"fields.`image`.:String",
			},
		},
		{
			name:  "Avg function",
			query: "* | avg(response_time)",
			wantContain: []string{
				"avg(toFloat64OrNull(fields.`response_time`.:String)) AS _avg",
			},
		},
		{
			name:  "Table with specific fields",
			query: "* | table(timestamp, image, user)",
			wantContain: []string{
				"SELECT timestamp, fields.`image`.:String AS image, fields.`user`.:String AS user",
			},
		},
		{
			name:  "Table with count",
			query: "* | table(image, count)",
			wantContain: []string{
				"fields.`image`.:String AS image",
				"COUNT(*) AS _count",
				"GROUP BY",
			},
		},
		{
			name:  "Sort ascending",
			query: "* | sort(timestamp, asc)",
			wantContain: []string{
				"ORDER BY timestamp ASC",
			},
		},
		{
			name:  "Sort descending",
			query: "* | sort(timestamp, desc)",
			wantContain: []string{
				"ORDER BY timestamp DESC",
			},
		},
		{
			name:  "Sort by field",
			query: "* | sort(user)",
			wantContain: []string{
				"ORDER BY fields.`user`.:String ASC",
			},
		},
		{
			name:  "Limit",
			query: "* | limit(50)",
			wantContain: []string{
				"LIMIT 50",
			},
		},
		{
			name:  "Complex pipeline",
			query: "image=/powershell/i | groupBy(user) | count() | sort(count, desc) | limit(10)",
			wantContain: []string{
				"match(fields.`image`.:String, '(?i)powershell')",
				"GROUP BY",
				"COUNT(*) AS",
				"LIMIT 10",
				"fields.`user`.:String",
			},
		},
		{
			name:  "Negative regex (!=)",
			query: "command_line!=/mp/i",
			wantContain: []string{
				"NOT match(fields.`command_line`.:String, '(?i)mp')",
			},
		},
		{
			name:  "Multiple piped filters with negative regex",
			query: "event_id=1 | image=/powershell/i | command_line!=/mp/i",
			wantContain: []string{
				"fields.`event_id`.:String = '1'",
				"match(fields.`image`.:String, '(?i)powershell')",
				"NOT match(fields.`command_line`.:String, '(?i)mp')",
			},
		},
		{
			name:  "Multiple filters",
			query: "image=/powershell/i user=admin",
			wantContain: []string{
				"match(fields.`image`.:String, '(?i)powershell')",
				"fields.`user`.:String = 'admin'",
			},
		},
		{
			name:  "Regex with special chars",
			query: `message=/error.*failed/i`,
			wantContain: []string{
				"match(fields.`message`.:String, '(?i)error.*failed')",
			},
		},
		{
			name:  "Numeric comparison",
			query: "status_code>=500",
			wantContain: []string{
				"toFloat64OrZero(fields.`status_code`.:String) >= 500",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("Failed to translate query: %v", err)
			}

			sql := result.SQL

			// Check that all expected strings are present
			for _, want := range tt.wantContain {
				if !strings.Contains(sql, want) {
					t.Errorf("SQL should contain %q\nGot: %s", want, sql)
				}
			}

			// Check that unwanted strings are not present
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(sql, notWant) {
					t.Errorf("SQL should NOT contain %q\nGot: %s", notWant, sql)
				}
			}
		})
	}
}

func TestAggregationWithoutGroupBy(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	// These queries should NOT have ORDER BY because they're aggregations without GROUP BY
	queries := []string{
		"* | count()",
		"image=/powershell/i | count()",
		"* | sum(bytes)",
		"* | avg(response_time)",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			pipeline, err := ParseQuery(query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("Failed to translate query: %v", err)
			}

			sql := result.SQL

			// These queries should not have ORDER BY
			if strings.Contains(sql, "ORDER BY timestamp") {
				t.Errorf("Aggregation without GROUP BY should not have ORDER BY\nQuery: %s\nSQL: %s", query, sql)
			}
		})
	}
}

func TestEdgeCases(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "Empty query",
			query:   "",
			wantErr: false, // Should default to *
		},
		{
			name:    "Just pipes",
			query:   "| | |",
			wantErr: true,
		},
		{
			name:    "Invalid function",
			query:   "* | invalidFunc()",
			wantErr: true,
		},
		{
			name:    "Multiple pipes",
			query:   "* | count() | count()",
			wantErr: false, // Should be valid
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Unexpected parse error: %v", err)
				}
				return
			}

			_, err = TranslateToSQLWithOrder(pipeline, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToSQLWithOrder() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSQLSafetyCheck(t *testing.T) {
	// Verify that the SQL safety check allows legitimate queries
	// that search for logs containing SQL keywords in their values
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	legitimateQueries := []string{
		`message="DROP TABLE logs"`,              // Searching for log containing DROP TABLE
		`message="INSERT INTO users"`,            // Searching for log containing INSERT
		`message=/DELETE FROM/i`,                 // Regex search for DELETE FROM
		`raw_log="UNION SELECT * FROM secrets"`,  // Searching for this string in logs
		`event="CREATE DATABASE test"`,           // Searching for this in event field
		`*`,                                      // Simple wildcard
		`image=/powershell/i | groupBy(user)`,    // Normal query
		`* | count()`,                            // Aggregation
	}

	for _, query := range legitimateQueries {
		t.Run("allow_"+query, func(t *testing.T) {
			pipeline, err := ParseQuery(query)
			if err != nil {
				t.Fatalf("ParseQuery error: %v", err)
			}

			_, err = TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Errorf("Legitimate query %q was incorrectly rejected: %v", query, err)
			}
		})
	}

	// Verify the stripStringLiterals function works correctly
	t.Run("stripStringLiterals", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			{"SELECT * FROM logs WHERE x = 'DROP TABLE'", "SELECT * FROM logs WHERE x = ''"},
			{"SELECT * FROM logs WHERE x = 'it\\'s a test'", "SELECT * FROM logs WHERE x = ''"},
			{"SELECT * FROM logs", "SELECT * FROM logs"},
		}
		for _, tt := range tests {
			result := stripStringLiterals(tt.input)
			if result != tt.expected {
				t.Errorf("stripStringLiterals(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		}
	})

	// Verify that the safety check catches dangerous structural SQL
	t.Run("validateGeneratedSQL", func(t *testing.T) {
		dangerousSQL := []string{
			"SELECT * FROM logs; DROP TABLE logs",
			"SELECT * FROM logs UNION SELECT * FROM system.users",
		}
		for _, sql := range dangerousSQL {
			if err := validateGeneratedSQL(sql); err == nil {
				t.Errorf("Dangerous SQL %q was not rejected", sql)
			}
		}
	})
}

func TestSingleVal(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("singleval with count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | count() | singleval()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("Expected COUNT(*) in SQL, got: %s", result.SQL)
		}
		if result.ChartType != "singleval" {
			t.Errorf("Expected chartType=singleval, got: %s", result.ChartType)
		}
	})

	t.Run("singleval with label", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | avg(response_time) | singleval(label="Avg Response")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if result.ChartType != "singleval" {
			t.Errorf("Expected chartType=singleval, got: %s", result.ChartType)
		}
		if result.ChartConfig["label"] != "Avg Response" {
			t.Errorf("Expected label=Avg Response, got: %v", result.ChartConfig["label"])
		}
	})

	t.Run("singleval with groupby count counts groups", func(t *testing.T) {
		// groupby(status) | count() pushes a second stage that counts groups,
		// producing a single row compatible with singleval().
		pipeline, err := ParseQuery("* | groupBy(status) | count() | singleval()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}
		if result.ChartType != "singleval" {
			t.Errorf("Expected chart type 'singleval', got %q", result.ChartType)
		}
		sql := result.SQL
		// Should have nested subquery: outer COUNT(*) wrapping inner GROUP BY
		if !strings.Contains(sql, "FROM (SELECT") {
			t.Errorf("Expected nested subquery, got: %s", sql)
		}
		if !strings.Contains(sql, "COUNT(*)") {
			t.Errorf("Expected COUNT(*) in query, got: %s", sql)
		}
	})

	t.Run("singleval rejects bare groupBy", func(t *testing.T) {
		// groupby without a second-stage agg still produces multiple rows
		pipeline, err := ParseQuery("* | groupBy(status) | singleval()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Error("Expected error for singleval directly after groupBy")
		}
	})

	t.Run("singleval rejects no aggregation", func(t *testing.T) {
		pipeline, err := ParseQuery("* | singleval()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Error("Expected error for singleval without aggregation")
		}
	})
}

func TestTimeChart(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("basic timechart with count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | timechart(span=5m, function=count())")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "toStartOfFiveMinutes(timestamp) AS time_bucket") {
			t.Errorf("Expected time bucket in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("Expected COUNT(*) in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ORDER BY time_bucket ASC") {
			t.Errorf("Expected ORDER BY time_bucket ASC in SQL, got: %s", result.SQL)
		}
		if result.ChartType != "timechart" {
			t.Errorf("Expected chartType=timechart, got: %s", result.ChartType)
		}
	})

	t.Run("timechart with avg", func(t *testing.T) {
		pipeline, err := ParseQuery("* | timechart(span=1h, function=avg(latency))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "toStartOfHour(timestamp) AS time_bucket") {
			t.Errorf("Expected toStartOfHour in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "avg(toFloat64OrNull(fields.`latency`.:String))") {
			t.Errorf("Expected avg aggregation in SQL, got: %s", result.SQL)
		}
	})

	t.Run("timechart with groupBy", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(status) | timechart(span=5m, function=count())")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "time_bucket") {
			t.Errorf("Expected time_bucket in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "fields.`status`.:String") {
			t.Errorf("Expected status groupBy in SQL, got: %s", result.SQL)
		}
		if result.ChartType != "timechart" {
			t.Errorf("Expected chartType=timechart, got: %s", result.ChartType)
		}
	})
}

func TestChainFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "test-fractal",
	}

	t.Run("basic chain with two steps", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user) { event_id=4624; event_id=4625 }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		// Should have sequenceMatch in HAVING with toDateTime cast
		if !strings.Contains(result.SQL, "sequenceMatch('(?1)(?2)')(toDateTime(timestamp)") {
			t.Errorf("Expected sequenceMatch with toDateTime, got: %s", result.SQL)
		}
		// Should have sequenceCount in SELECT with toDateTime cast
		if !strings.Contains(result.SQL, "sequenceCount('(?1)(?2)')(toDateTime(timestamp)") {
			t.Errorf("Expected sequenceCount with toDateTime, got: %s", result.SQL)
		}
		// Should GROUP BY the chain field (uses alias after rebuild)
		if !strings.Contains(result.SQL, "GROUP BY user") {
			t.Errorf("Expected GROUP BY user, got: %s", result.SQL)
		}
		// Should ORDER BY chain_count
		if !strings.Contains(result.SQL, "ORDER BY chain_count DESC") {
			t.Errorf("Expected ORDER BY chain_count DESC, got: %s", result.SQL)
		}
		if !result.IsAggregated {
			t.Error("Expected IsAggregated=true")
		}
	})

	t.Run("chain with within parameter", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user, within=5m) { event_id=4624; event_id=4625 }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		// Should have time constraint in pattern
		if !strings.Contains(result.SQL, "(?t<=300)") {
			t.Errorf("Expected time constraint (?t<=300) for within=5m, got: %s", result.SQL)
		}
	})

	t.Run("chain with regex conditions", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user) { event_id=1 | image=/explorer/i; event_id=1 | image=/powershell/i }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		// Each step should AND its conditions
		if !strings.Contains(result.SQL, "match(fields.`image`.:String, '(?i)explorer')") {
			t.Errorf("Expected regex match for explorer, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "match(fields.`image`.:String, '(?i)powershell')") {
			t.Errorf("Expected regex match for powershell, got: %s", result.SQL)
		}
		// Should have 2-step pattern
		if !strings.Contains(result.SQL, "(?1)(?2)") {
			t.Errorf("Expected 2-step sequence pattern, got: %s", result.SQL)
		}
	})

	t.Run("chain with filter prefix", func(t *testing.T) {
		pipeline, err := ParseQuery("event_source=Security | chain(user) { event_id=4624; event_id=4625 }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		// Should have event_source filter in WHERE
		if !strings.Contains(result.SQL, "fields.`event_source`.:String = 'Security'") {
			t.Errorf("Expected WHERE filter for event_source, got: %s", result.SQL)
		}
		// Should still have chain logic
		if !strings.Contains(result.SQL, "sequenceMatch") {
			t.Errorf("Expected sequenceMatch, got: %s", result.SQL)
		}
	})

	t.Run("chain with limit", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user) { event_id=4624; event_id=4625 } | limit(10)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "LIMIT 10") {
			t.Errorf("Expected LIMIT 10, got: %s", result.SQL)
		}
	})

	t.Run("chain with three steps and within", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user, within=1h) { event_id=4624; event_id=1 | image=/explorer/i; event_id=1 | image=/powershell/i }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		// 3-step pattern with time constraints between each
		if !strings.Contains(result.SQL, "(?1)(?t<=3600)(?2)(?t<=3600)(?3)") {
			t.Errorf("Expected 3-step pattern with 1h constraints, got: %s", result.SQL)
		}
	})

	t.Run("chain requires at least 2 steps", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user) { event_id=4624 }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Error("Expected error for chain with only 1 step")
		}
	})

	t.Run("chain field order", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user) { event_id=4624; event_id=4625 }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if len(result.FieldOrder) < 2 {
			t.Fatalf("Expected at least 2 fields in order, got %d: %v", len(result.FieldOrder), result.FieldOrder)
		}
		if result.FieldOrder[0] != "user" {
			t.Errorf("Expected first field 'user', got '%s'", result.FieldOrder[0])
		}
		if result.FieldOrder[1] != "chain_count" {
			t.Errorf("Expected second field 'chain_count', got '%s'", result.FieldOrder[1])
		}
	})

	t.Run("chain with multiple identity fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user, source_user, target_user, within=1d) { event_id=4624; event_id=4688 }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		// Multi-identity mode: should use arrayJoin to expand rows
		if !strings.Contains(sql, "arrayJoin(arrayFilter(x -> x != ''") {
			t.Errorf("Expected arrayJoin for multi-identity fields, got: %s", sql)
		}
		// Should reference all three fields in the array
		if !strings.Contains(sql, "fields.`user`.:String") {
			t.Errorf("Expected user field reference, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`source_user`.:String") {
			t.Errorf("Expected source_user field reference, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`target_user`.:String") {
			t.Errorf("Expected target_user field reference, got: %s", sql)
		}
		// Should GROUP BY _entity
		if !strings.Contains(sql, "GROUP BY _entity") {
			t.Errorf("Expected GROUP BY _entity, got: %s", sql)
		}
		// Field order: _entity, chain_count
		if len(result.FieldOrder) < 2 {
			t.Fatalf("Expected at least 2 fields, got %d: %v", len(result.FieldOrder), result.FieldOrder)
		}
		if result.FieldOrder[0] != "_entity" || result.FieldOrder[1] != "chain_count" {
			t.Errorf("Expected field order [_entity, chain_count], got %v", result.FieldOrder)
		}
	})

	t.Run("chain with multi-pipe steps", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user, within=1d) { event_id=1 | image=/explorer/i; event_id=1 | image=/powershell/i | command_line=/-nop/i; event_id=3 | image=/powershell/i }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "sequenceCount") {
			t.Errorf("Expected sequenceCount in SQL, got: %s", sql)
		}
		if !strings.Contains(sql, "sequenceMatch") {
			t.Errorf("Expected sequenceMatch in HAVING, got: %s", sql)
		}
		// Verify the multi-pipe step produces AND conditions
		if !strings.Contains(sql, "AND") {
			t.Errorf("Expected AND conditions for multi-pipe steps, got: %s", sql)
		}
	})

	t.Run("chain with explicit AND keyword", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(computer_name, within=1d) { event_id=1 | image=/fodhelper/i; event_id=1 | image=/powershell/i AND commandline=/anydesk/i }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		t.Logf("SQL: %s", sql)
		if !strings.Contains(sql, "sequenceMatch") {
			t.Errorf("Expected sequenceMatch, got: %s", sql)
		}
		// Step 2 should have all three conditions ANDed
		if !strings.Contains(sql, "match(") {
			t.Errorf("Expected regex match conditions, got: %s", sql)
		}
	})

	t.Run("chain with OR in step", func(t *testing.T) {
		pipeline, err := ParseQuery("* | chain(user, within=1d) { event_id=4624 | logon_type=2 OR logon_type=10; event_id=4688 | image=/cmd.exe/i }")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		t.Logf("SQL: %s", sql)
		if !strings.Contains(sql, "OR") {
			t.Errorf("Expected OR in chain step SQL, got: %s", sql)
		}
	})

	t.Run("chain with NOT in step", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | chain(user, within=1d) { event_id=4624; event_id=4688 | NOT image=/explorer/i }`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		t.Logf("SQL: %s", sql)
		if !strings.Contains(sql, "NOT") {
			t.Errorf("Expected NOT in chain step SQL, got: %s", sql)
		}
	})

	t.Run("chain with parenthesized group in step", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | chain(user, within=1d) { event_id=4624 | (logon_type=2 OR logon_type=10); event_id=4688 }`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		t.Logf("SQL: %s", sql)
		if !strings.Contains(sql, "OR") {
			t.Errorf("Expected OR in parenthesized group, got: %s", sql)
		}
	})
}

func TestHashFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("hash single field", func(t *testing.T) {
		pipeline, err := ParseQuery("* | hash(user)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "cityHash64") {
			t.Errorf("Expected cityHash64 in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "hash_key") {
			t.Errorf("Expected hash_key alias, got: %s", result.SQL)
		}
	})

	t.Run("hash multiple fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | hash(field=user, computer)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "cityHash64") {
			t.Errorf("Expected cityHash64 in SQL, got: %s", result.SQL)
		}
	})
}

func TestCollectFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("collect in multi", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user) | multi(collect(image))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "groupArray") {
			t.Errorf("Expected groupArray in SQL, got: %s", result.SQL)
		}
	})
}

func TestStatsCountWithParams(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("count with distinct and alias", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user) | multi(count(field=event_id, distinct=true, as=unique_events))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "uniqExact") {
			t.Errorf("Expected uniqExact for distinct count, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "unique_events") {
			t.Errorf("Expected unique_events alias, got: %s", result.SQL)
		}
	})

	t.Run("count with field and alias", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user) | multi(count(field=event_id, as=total))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "count(") {
			t.Errorf("Expected count() in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "total") {
			t.Errorf("Expected total alias, got: %s", result.SQL)
		}
	})
}

func TestGroupByInlineStats(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("inline multi with multiple counts", func(t *testing.T) {
		pipeline, err := ParseQuery("event_id=5145 | groupby(subject_user_name, function=multi(count(subject_user_name, distinct=true, as=unique_observed), count(computer_name, distinct=false, as=unique_computer)))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "uniqExact") {
			t.Errorf("Expected uniqExact for distinct count, got: %s", sql)
		}
		if !strings.Contains(sql, "unique_observed") {
			t.Errorf("Expected unique_observed alias, got: %s", sql)
		}
		if !strings.Contains(sql, "unique_computer") {
			t.Errorf("Expected unique_computer alias, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("inline multi with mixed functions", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user, function=multi(count(), avg(bytes), sum(response_time)))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "COUNT(*)") {
			t.Errorf("Expected COUNT(*), got: %s", sql)
		}
		if !strings.Contains(sql, "avg(") {
			t.Errorf("Expected avg(), got: %s", sql)
		}
		if !strings.Contains(sql, "sum(") {
			t.Errorf("Expected sum(), got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("inline single function without multi wrapper", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user, function=avg(bytes))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "avg(") {
			t.Errorf("Expected avg(), got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})
}

func TestAnalyzeFields(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("all fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | analyzeFields()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		for _, want := range []string{"JSONExtractKeysAndValues", "_events", "_distinct_vals", "_mean", "_min", "_max", "_stdev", "field_name", "GROUP BY", "LIMIT 50000", "LIMIT 10000"} {
			if !strings.Contains(sql, want) {
				t.Errorf("Expected %q in SQL, got: %s", want, sql)
			}
		}
		if !result.IsAggregated {
			t.Error("Expected IsAggregated to be true")
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("specific fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | analyzeFields(user, image)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "kv.1 IN ('user', 'image')") {
			t.Errorf("Expected path filter for specific fields, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("with pre-filter", func(t *testing.T) {
		pipeline, err := ParseQuery("service=webapp | analyzeFields()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "'webapp'") {
			t.Errorf("Expected filter condition in SQL, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("custom scan limit", func(t *testing.T) {
		pipeline, err := ParseQuery("* | analyzeFields(limit=10000)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "LIMIT 10000") {
			t.Errorf("Expected custom scan limit, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("limit=max keyword", func(t *testing.T) {
		pipeline, err := ParseQuery("* | analyzeFields(limit=max)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "LIMIT 200000") {
			t.Errorf("Expected max scan limit (200000), got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("with post-filter", func(t *testing.T) {
		pipeline, err := ParseQuery("* | analyzeFields() | _events < 10")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "_events") {
			t.Errorf("Expected _events in SQL, got: %s", sql)
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("field order", func(t *testing.T) {
		pipeline, err := ParseQuery("* | analyzeFields()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		expected := []string{"field_name", "_events", "_distinct_vals", "_mean", "_min", "_max", "_stdev"}
		if len(result.FieldOrder) != len(expected) {
			t.Fatalf("Expected %d fields in order, got %d: %v", len(expected), len(result.FieldOrder), result.FieldOrder)
		}
		for i, f := range expected {
			if result.FieldOrder[i] != f {
				t.Errorf("FieldOrder[%d] = %q, want %q", i, result.FieldOrder[i], f)
			}
		}
	})
}

func TestMathAssignment(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("deferred math expression", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user) | multi(count(field=event_id, distinct=true, as=unique_observed), count(field=event_id, as=total)) | confidence := ((total - unique_observed) / total) * 0.95")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "confidence") {
			t.Errorf("Expected confidence field in SQL, got: %s", sql)
		}
		if !strings.Contains(sql, "0.95") {
			t.Errorf("Expected 0.95 multiplier in SQL, got: %s", sql)
		}
		// Verify it references the computed aliases, not fields.`total`.:String
		if strings.Contains(sql, "fields.`total`.:String") {
			t.Errorf("Should reference 'total' alias not fields.`total`.:String, got: %s", sql)
		}
	})
}

func TestBFSFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "test-fractal",
	}

	t.Run("basic bfs", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid, parent=parent_process_guid, start="ABC123")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "WITH RECURSIVE traversal AS") {
			t.Errorf("Expected recursive CTE, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`process_guid`.:String AS _node_id") {
			t.Errorf("Expected child field as _node_id, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`parent_process_guid`.:String = t._node_id") {
			t.Errorf("Expected parent join condition, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`process_guid`.:String = 'ABC123'") {
			t.Errorf("Expected start value filter, got: %s", sql)
		}
		if !strings.Contains(sql, "ORDER BY _depth ASC") {
			t.Errorf("Expected BFS depth ordering, got: %s", sql)
		}
		if !strings.Contains(sql, "t._depth < 10") {
			t.Errorf("Expected default depth limit of 10, got: %s", sql)
		}
		if result.IsAggregated {
			t.Error("Expected IsAggregated=false")
		}
	})

	t.Run("bfs with filter and graph", func(t *testing.T) {
		query := `event_id=1 | bfs(child=process_guid, parent=parent_process_guid, start="{63047898-81ee-6860-5202-000000002502}") | graph(child=process_guid, parent=parent_process_guid, labels=image)`
		pipeline, err := ParseQuery(query)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		// Should have the filter in both base and recursive case
		if !strings.Contains(sql, "fields.`event_id`.:String = '1'") {
			t.Errorf("Expected event_id filter, got: %s", sql)
		}
		// Should have fractal filter
		if !strings.Contains(sql, "fractal_id = 'test-fractal'") {
			t.Errorf("Expected fractal filter, got: %s", sql)
		}
		// Recursive case should have aliased fractal filter
		if !strings.Contains(sql, "l.fractal_id = 'test-fractal'") {
			t.Errorf("Expected aliased fractal filter in recursive case, got: %s", sql)
		}
		// Recursive case should have aliased field filter
		if !strings.Contains(sql, "l.fields.`event_id`.:String = '1'") {
			t.Errorf("Expected aliased event_id filter in recursive case, got: %s", sql)
		}
		// Should set graph chart type
		if result.ChartType != "graph" {
			t.Errorf("Expected ChartType=graph, got: %s", result.ChartType)
		}
		if result.ChartConfig["childField"] != "process_guid" {
			t.Errorf("Expected childField=process_guid, got: %v", result.ChartConfig["childField"])
		}
	})

	t.Run("bfs with custom depth", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid, parent=parent_process_guid, start="ABC", depth=5)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "t._depth < 5") {
			t.Errorf("Expected depth limit of 5, got: %s", result.SQL)
		}
	})

	t.Run("bfs with table", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid, parent=parent_process_guid, start="ABC") | table(process_guid, image, _depth)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "process_guid") {
			t.Errorf("Expected process_guid in select, got: %s", sql)
		}
		if !strings.Contains(sql, "_depth") {
			t.Errorf("Expected _depth in select, got: %s", sql)
		}
	})

	t.Run("bfs missing params", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Fatal("Expected error for missing params")
		}
		if !strings.Contains(err.Error(), "requires child=, parent=, and start= parameters") {
			t.Errorf("Expected missing params error, got: %v", err)
		}
	})

	t.Run("bfs with limit", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid, parent=parent_process_guid, start="ABC") | limit(50)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "LIMIT 50") {
			t.Errorf("Expected LIMIT 50, got: %s", result.SQL)
		}
	})

	t.Run("bfs with include", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid, parent=parent_process_guid, start="ABC", include=[image,command_line])`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		// Should extract child and parent fields plus include fields
		if !strings.Contains(sql, "fields.`process_guid`.:String AS _process_guid") {
			t.Errorf("Expected child field extraction, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`parent_process_guid`.:String AS _parent_process_guid") {
			t.Errorf("Expected parent field extraction, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`image`.:String AS _image") {
			t.Errorf("Expected image field extraction, got: %s", sql)
		}
		if !strings.Contains(sql, "fields.`command_line`.:String AS _command_line") {
			t.Errorf("Expected command_line field extraction, got: %s", sql)
		}
		// Final SELECT should alias them without underscore prefix
		if !strings.Contains(sql, "_image AS image") {
			t.Errorf("Expected image in final select, got: %s", sql)
		}
		if !strings.Contains(sql, "_command_line AS command_line") {
			t.Errorf("Expected command_line in final select, got: %s", sql)
		}
		// fieldOrder should include the extracted fields
		expectedFields := []string{"timestamp", "_depth", "_path", "process_guid", "parent_process_guid", "image", "command_line"}
		for _, ef := range expectedFields {
			found := false
			for _, fo := range result.FieldOrder {
				if fo == ef {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected %q in FieldOrder, got: %v", ef, result.FieldOrder)
			}
		}
	})

	t.Run("bfs without include still has child and parent", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=process_guid, parent=parent_process_guid, start="ABC")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		// Even without include=, child and parent should be extracted
		if !strings.Contains(sql, "_process_guid AS process_guid") {
			t.Errorf("Expected child field in final select, got: %s", sql)
		}
		if !strings.Contains(sql, "_parent_process_guid AS parent_process_guid") {
			t.Errorf("Expected parent field in final select, got: %s", sql)
		}
		// fieldOrder should have child and parent
		expectedFields := []string{"timestamp", "_depth", "_path", "process_guid", "parent_process_guid"}
		for _, ef := range expectedFields {
			found := false
			for _, fo := range result.FieldOrder {
				if fo == ef {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected %q in FieldOrder, got: %v", ef, result.FieldOrder)
			}
		}
	})
}

func TestDFSFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "test-fractal",
	}

	t.Run("basic dfs", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | dfs(child=process_guid, parent=parent_process_guid, start="ABC123")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "WITH RECURSIVE traversal AS") {
			t.Errorf("Expected recursive CTE, got: %s", sql)
		}
		// DFS should order by _path for pre-order traversal
		if !strings.Contains(sql, "ORDER BY _path ASC") {
			t.Errorf("Expected DFS path ordering, got: %s", sql)
		}
	})

	t.Run("dfs depth cap at 50", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | dfs(child=a, parent=b, start="X", depth=100)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "t._depth < 50") {
			t.Errorf("Expected depth capped at 50, got: %s", result.SQL)
		}
	})

	t.Run("dfs post-traversal depth filter", func(t *testing.T) {
		pipeline, err := ParseQuery(`event_id=1 | dfs(child=process_guid, parent=parent_process_guid, include=[image,commandline], start="ABC123") | _depth > 3`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		// _depth > 3 must be a post-traversal filter (WHERE on the outer SELECT from traversal),
		// NOT in the base/recursive CTE conditions
		if !strings.Contains(sql, "FROM traversal WHERE _depth > 3") {
			t.Errorf("Expected post-traversal _depth filter, got: %s", sql)
		}
	})

	t.Run("bfs post-traversal depth filter", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=a, parent=b, start="X") | _depth <= 2`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "FROM traversal WHERE _depth <= 2") {
			t.Errorf("Expected post-traversal _depth filter, got: %s", sql)
		}
	})

	t.Run("cannot combine bfs with aggregation", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | bfs(child=a, parent=b, start="X") | count()`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Fatal("Expected error combining bfs with aggregation")
		}
	})
}

func TestQualifyColumnRefs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		alias    string
		expected string
	}{
		{
			name:     "timestamp comparison",
			input:    "timestamp >= '2026-01-01'",
			alias:    "l",
			expected: "l.timestamp >= '2026-01-01'",
		},
		{
			name:     "fields reference",
			input:    "fields.`event_id`.:String = '1'",
			alias:    "l",
			expected: "l.fields.`event_id`.:String = '1'",
		},
		{
			name:     "fractal_id",
			input:    "fractal_id = 'abc'",
			alias:    "l",
			expected: "l.fractal_id = 'abc'",
		},
		{
			name:     "string literal preserved",
			input:    "fields.`event`.:String = 'timestamp exceeded'",
			alias:    "l",
			expected: "l.fields.`event`.:String = 'timestamp exceeded'",
		},
		{
			name:     "match function",
			input:    "match(fields.`image`.:String, '(?i)powershell')",
			alias:    "l",
			expected: "match(l.fields.`image`.:String, '(?i)powershell')",
		},
		{
			name:     "compound condition",
			input:    "(fields.`a`.:String = '1' AND fields.`b`.:String = '2')",
			alias:    "l",
			expected: "(l.fields.`a`.:String = '1' AND l.fields.`b`.:String = '2')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qualifyColumnRefs(tt.input, tt.alias)
			if result != tt.expected {
				t.Errorf("qualifyColumnRefs(%q, %q) = %q, want %q", tt.input, tt.alias, result, tt.expected)
			}
		})
	}
}

func TestLenFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("basic len", func(t *testing.T) {
		pipeline, err := ParseQuery("* | len(message)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "length(fields.`message`.:String) AS _len") {
			t.Errorf("Expected length() in SQL, got: %s", result.SQL)
		}
	})

	t.Run("len with computed field", func(t *testing.T) {
		pipeline, err := ParseQuery("* | lowercase(user) | len(user)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "length(lower(") {
			t.Errorf("Expected length wrapping lower expression, got: %s", result.SQL)
		}
	})

	t.Run("len with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery("* | len(image) | _len > 20")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "length(fields.`image`.:String) AS _len") {
			t.Errorf("Expected length() in SELECT, got: %s", result.SQL)
		}
		// _len should be referenced as a computed field, not as a JSON field
		if strings.Contains(result.SQL, "fields.`_len`") {
			t.Errorf("_len should be a computed field reference, not a JSON field: %s", result.SQL)
		}
		// The condition should inline the expression (consistent with FieldKindPerRow behavior)
		if !strings.Contains(result.SQL, "length(") || !strings.Contains(result.SQL, "> 20") {
			t.Errorf("Expected inlined length() > 20 condition, got: %s", result.SQL)
		}
	})
}

func TestComputedFieldPipedConditions(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("levenshtein with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | levenshtein(process_name, "svchost.exe") | _distance < 3`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_distance`") {
			t.Errorf("_distance should be a computed field, not JSON field: %s", result.SQL)
		}
		// The condition should inline the expression (consistent with FieldKindPerRow behavior)
		if !strings.Contains(result.SQL, "damerauLevenshteinDistance(") || !strings.Contains(result.SQL, "< 3") {
			t.Errorf("Expected inlined damerauLevenshteinDistance() < 3 condition, got: %s", result.SQL)
		}
	})

	t.Run("split with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | split(message, " ", 1) | _split = "error"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_split`") {
			t.Errorf("_split should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("base64decode with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | base64Decode(payload) | _decoded != ""`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_decoded`") {
			t.Errorf("_decoded should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("coalesce with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | coalesce(hostname, host) | _coalesced != ""`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_coalesced`") {
			t.Errorf("_coalesced should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("strftime with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%H", as=_hour) | _hour > 2`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_hour`") {
			t.Errorf("_hour should be a computed field, not JSON field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_hour") {
			t.Errorf("Expected _hour in query, got: %s", result.SQL)
		}
	})

	t.Run("hash with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | hash(source_ip, dest_ip) | hash_key = "abc123"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`hash_key`") {
			t.Errorf("hash_key should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("hash custom alias with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | hash(source_ip, as=_hkey) | _hkey = "abc"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_hkey`") {
			t.Errorf("_hkey should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("sprintf with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | sprintf("%s:%s", host, port, as=_endpoint) | _endpoint = "localhost:80"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_endpoint`") {
			t.Errorf("_endpoint should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("concat with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | concat(host) | _concat != ""`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_concat`") {
			t.Errorf("_concat should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("concat custom alias with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | concat(host, _addr) | _addr != ""`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_addr`") {
			t.Errorf("_addr should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("lowercase with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | lowercase(hostname, _lower) | _lower = "web01"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_lower`") {
			t.Errorf("_lower should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("uppercase with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | uppercase(status, _upper) | _upper = "ERROR"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`_upper`") {
			t.Errorf("_upper should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("eval with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | eval(total=price*quantity) | total > 100`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`total`") {
			t.Errorf("total should be a computed field, not JSON field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "total") {
			t.Errorf("Expected total in query, got: %s", result.SQL)
		}
	})

	t.Run("replace with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | replace("[0-9]+", "X", message, cleaned) | cleaned = "errorX"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`cleaned`") {
			t.Errorf("cleaned should be a computed field, not JSON field: %s", result.SQL)
		}
	})

	t.Run("regex named capture with piped condition", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | regex("(?<user>[a-z]+)@(?<domain>[a-z.]+)", field=email) | user = "admin"`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if strings.Contains(result.SQL, "fields.`user`") {
			t.Errorf("user should be a computed field, not JSON field: %s", result.SQL)
		}
	})

}

func TestChainedAggregation(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("groupBy then mad on _count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(image) | mad(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		// Inner query should have COUNT(*) AS _count
		if !strings.Contains(result.SQL, "COUNT(*) AS _count") {
			t.Errorf("Expected COUNT(*) AS _count in inner query, got: %s", result.SQL)
		}
		// Outer query should have median and MAD referencing _count
		if !strings.Contains(result.SQL, "any(_median_val) AS _median") {
			t.Errorf("Expected any(_median_val) AS _median in outer query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "median(abs(toFloat64(_count) - _median_val)) AS _mad") {
			t.Errorf("Expected median(abs(...)) AS _mad in outer query, got: %s", result.SQL)
		}
		// Should have window function intermediate layer
		if !strings.Contains(result.SQL, "median(toFloat64(_count)) OVER () AS _median_val") {
			t.Errorf("Expected window function layer, got: %s", result.SQL)
		}
		// Should NOT reference JSON field for _count
		if strings.Contains(result.SQL, "fields.`_count`") {
			t.Errorf("Should not reference _count as JSON field: %s", result.SQL)
		}
		// Field order should have the outer agg results
		if len(result.FieldOrder) == 0 || result.FieldOrder[0] != "_median" {
			t.Errorf("Expected field order starting with _median, got: %v", result.FieldOrder)
		}
	})

	t.Run("groupBy then median on _count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | median(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "COUNT(*) AS _count") {
			t.Errorf("Expected COUNT(*) AS _count in inner query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "median(toFloat64(_count)) AS _median") {
			t.Errorf("Expected median(_count) in outer query, got: %s", result.SQL)
		}
	})

	t.Run("groupBy with sum then mad on _sum", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | sum(bytes) | mad(_sum)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		// Inner should have sum(...) AS _sum
		if !strings.Contains(result.SQL, "AS _sum") {
			t.Errorf("Expected _sum in inner query, got: %s", result.SQL)
		}
		// Outer should have MAD with window function layer referencing _sum
		if !strings.Contains(result.SQL, "any(_median_val) AS _median") {
			t.Errorf("Expected any(_median_val) AS _median in outer query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "median(abs(toFloat64(_sum) - _median_val)) AS _mad") {
			t.Errorf("Expected median(abs(...)) AS _mad in outer query, got: %s", result.SQL)
		}
	})

	t.Run("groupBy then avg on _count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(image) | avg(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "COUNT(*) AS _count") {
			t.Errorf("Expected COUNT(*) in inner query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "avg(toFloat64(_count)) AS _avg") {
			t.Errorf("Expected avg(_count) in outer query, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_count`") {
			t.Errorf("Should not reference _count as JSON field: %s", result.SQL)
		}
	})

	t.Run("groupBy then sort by _count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(image) | sort(_count, desc)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		// sort should use _count alias, not JSON field reference
		if strings.Contains(result.SQL, "fields.`_count`") {
			t.Errorf("sort should use _count alias, not JSON field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_count DESC") {
			t.Errorf("Expected _count DESC in ORDER BY, got: %s", result.SQL)
		}
	})

	t.Run("groupBy then stddev on _count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(user_name) | count() | stddev(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "COUNT(*) AS _count") {
			t.Errorf("Expected COUNT(*) in inner query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "stddevPop(toFloat64(_count)) AS _stddev") {
			t.Errorf("Expected stddevPop(_count) in outer query, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_count`") {
			t.Errorf("Should not reference _count as JSON field: %s", result.SQL)
		}
	})

	t.Run("groupBy then percentile on _count", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | count() | percentile(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "COUNT(*) AS _count") {
			t.Errorf("Expected COUNT(*) in inner query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "quantiles(0.5, 0.75, 0.99)(toFloat64(_count)) AS _percentile") {
			t.Errorf("Expected quantiles(_count) in outer query, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_count`") {
			t.Errorf("Should not reference _count as JSON field: %s", result.SQL)
		}
	})
}

func TestComputedFieldAggregation(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("strftime then headTail uses computed expression", func(t *testing.T) {
		pipeline, err := ParseQuery(`event_name=* | strftime("%H",as="_hour") | headTail(_hour)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "formatDateTime(timestamp, '%H', 'UTC') AS value") {
			t.Errorf("Expected formatDateTime expression for value, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_hour`") {
			t.Errorf("Should not reference _hour as JSON field: %s", result.SQL)
		}
	})

	t.Run("strftime then groupby uses computed expression", func(t *testing.T) {
		pipeline, err := ParseQuery(`event_name=* | strftime("%H",as="_hour") | groupby(_hour)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "formatDateTime(timestamp, '%H', 'UTC') AS _hour") {
			t.Errorf("Expected formatDateTime expression for _hour, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_hour`") {
			t.Errorf("Should not reference _hour as JSON field: %s", result.SQL)
		}
	})

	t.Run("strftime then frequency uses computed expression", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%H",as="_hour") | frequency(_hour)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "formatDateTime(timestamp, '%H', 'UTC') AS value") {
			t.Errorf("Expected formatDateTime expression for value, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_hour`") {
			t.Errorf("Should not reference _hour as JSON field: %s", result.SQL)
		}
	})

	t.Run("groupBy function=median resolves computed _time", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%H") | groupBy(user, function=median(_time))`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "median(toFloat64OrNull(formatDateTime(timestamp, '%H', 'UTC')))") {
			t.Errorf("Expected median with formatDateTime, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_time`") {
			t.Errorf("Should not reference _time as JSON field: %s", result.SQL)
		}
	})

	t.Run("groupBy function=sum resolves computed _time", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%H") | groupBy(user, function=sum(_time))`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "sum(toFloat64OrNull(formatDateTime(timestamp, '%H', 'UTC')))") {
			t.Errorf("Expected sum with formatDateTime, got: %s", result.SQL)
		}
		if strings.Contains(result.SQL, "fields.`_time`") {
			t.Errorf("Should not reference _time as JSON field: %s", result.SQL)
		}
	})

	t.Run("groupBy function=selectFirst handles camelCase and timestamp", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | groupBy(user, function=selectFirst(timestamp))`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "min(timestamp) AS first_timestamp") {
			t.Errorf("Expected min(timestamp) AS first_timestamp, got: %s", result.SQL)
		}
	})

	t.Run("groupBy function=selectLast handles camelCase", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | groupBy(user, function=selectLast(timestamp))`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "max(timestamp) AS last_timestamp") {
			t.Errorf("Expected max(timestamp) AS last_timestamp, got: %s", result.SQL)
		}
	})

	t.Run("groupBy function=multi resolves computed fields", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%H") | groupBy(user, function=multi(sum(_time), median(_time)))`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
		if !strings.Contains(result.SQL, "sum(toFloat64OrNull(formatDateTime(timestamp, '%H', 'UTC')))") {
			t.Errorf("Expected sum with formatDateTime, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "median(toFloat64OrNull(formatDateTime(timestamp, '%H', 'UTC')))") {
			t.Errorf("Expected median with formatDateTime, got: %s", result.SQL)
		}
	})
}

func TestLevenshteinFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("field vs string literal", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | levenshtein(process_name, "svchost.exe")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "damerauLevenshteinDistance(") {
			t.Errorf("Expected damerauLevenshteinDistance in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AS _distance") {
			t.Errorf("Expected _distance alias, got: %s", result.SQL)
		}
	})

	t.Run("field vs field", func(t *testing.T) {
		pipeline, err := ParseQuery("* | levenshtein(src_host, dst_host)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "fields.`src_host`.:String") {
			t.Errorf("Expected src_host field ref, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "fields.`dst_host`.:String") {
			t.Errorf("Expected dst_host field ref, got: %s", result.SQL)
		}
	})
}

func TestBase64DecodeFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("basic base64Decode", func(t *testing.T) {
		pipeline, err := ParseQuery("* | base64Decode(payload)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "tryBase64Decode(fields.`payload`.:String) AS _decoded") {
			t.Errorf("Expected tryBase64Decode in SQL, got: %s", result.SQL)
		}
	})
}

func TestDedupFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("dedup single field", func(t *testing.T) {
		pipeline, err := ParseQuery("* | dedup(user)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "LIMIT 1 BY") {
			t.Errorf("Expected LIMIT 1 BY in SQL, got: %s", result.SQL)
		}
	})

	t.Run("dedup multiple fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | dedup(src_ip, dst_ip)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "LIMIT 1 BY") {
			t.Errorf("Expected LIMIT 1 BY in SQL, got: %s", result.SQL)
		}
	})
}

func TestCidrFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("cidr filter", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | cidr(src_ip, "10.0.0.0/8")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "isIPAddressInRange(fields.`src_ip`.:String, '10.0.0.0/8')") {
			t.Errorf("Expected isIPAddressInRange in SQL, got: %s", result.SQL)
		}
	})

	t.Run("negated cidr", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | !cidr(src_ip, "10.0.0.0/8")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "NOT isIPAddressInRange(") {
			t.Errorf("Expected NOT isIPAddressInRange in SQL, got: %s", result.SQL)
		}
	})
}

func TestSplitFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("split with index", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | split(path, "/", 2)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "splitByString('/', fields.`path`.:String)[2] AS _split") {
			t.Errorf("Expected splitByString in SQL, got: %s", result.SQL)
		}
	})

	t.Run("split invalid index", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | split(path, "/", abc)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Error("Expected error for non-numeric index")
		}
	})
}

func TestSubstrFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("substr with start and length", func(t *testing.T) {
		pipeline, err := ParseQuery("* | substr(message, 1, 50)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "substring(fields.`message`.:String, 1, 50) AS _substr") {
			t.Errorf("Expected substring in SQL, got: %s", result.SQL)
		}
	})

	t.Run("substr without length", func(t *testing.T) {
		pipeline, err := ParseQuery("* | substr(path, 5)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "substring(fields.`path`.:String, 5) AS _substr") {
			t.Errorf("Expected substring without length in SQL, got: %s", result.SQL)
		}
	})
}

func TestUrldecodeFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("basic urldecode", func(t *testing.T) {
		pipeline, err := ParseQuery("* | urldecode(request_uri)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "decodeURLComponent(fields.`request_uri`.:String) AS _urldecoded") {
			t.Errorf("Expected decodeURLComponent in SQL, got: %s", result.SQL)
		}
	})
}

func TestMedianFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("standalone median", func(t *testing.T) {
		pipeline, err := ParseQuery("* | median(response_time)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "median(toFloat64OrNull(fields.`response_time`.:String)) AS _median") {
			t.Errorf("Expected median expression, got: %s", result.SQL)
		}
	})

	t.Run("grouped median", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | median(response_time)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "median(") {
			t.Errorf("Expected median in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", result.SQL)
		}
	})

	t.Run("median in multi", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | multi(median(response_time), avg(response_time))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "median(toFloat64OrNull(") {
			t.Errorf("Expected median in multi SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "avg(toFloat64OrNull(") {
			t.Errorf("Expected avg in multi SQL, got: %s", result.SQL)
		}
	})
}

func TestMADFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("standalone mad", func(t *testing.T) {
		pipeline, err := ParseQuery("* | mad(response_time)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "any(_median_val) AS _median") {
			t.Errorf("Expected any(_median_val) AS _median, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "median(abs(_mad_val - _median_val)) AS _mad") {
			t.Errorf("Expected median(abs(_mad_val - _median_val)) AS _mad, got: %s", result.SQL)
		}
	})

	t.Run("grouped mad", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | mad(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "median(abs(") {
			t.Errorf("Expected median(abs(...)) MAD expression in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", result.SQL)
		}
	})

	t.Run("mad with computed field", func(t *testing.T) {
		pipeline, err := ParseQuery("* | lowercase(status) | mad(status)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "median(abs(") {
			t.Errorf("Expected median(abs(...)) MAD expression in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "lower(") {
			t.Errorf("Expected lower() for computed field resolution, got: %s", result.SQL)
		}
	})

	t.Run("mad in multi", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupBy(host) | multi(mad(response_time), avg(response_time))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', groupArray(toFloat64OrNull(") {
			t.Errorf("Expected arrayReduce MAD expression in multi SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "mad_response_time") {
			t.Errorf("Expected mad_response_time alias, got: %s", result.SQL)
		}
	})

	t.Run("table with mad", func(t *testing.T) {
		pipeline, err := ParseQuery("* | table(host, mad(response_time))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "arrayReduce('median', arrayMap(") {
			t.Errorf("Expected arrayReduce MAD expression in table SQL, got: %s", result.SQL)
		}
	})
}

func TestCoalesceFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("coalesce two fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | coalesce(user, username)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "multiIf(") {
			t.Errorf("Expected multiIf in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AS _coalesced") {
			t.Errorf("Expected _coalesced alias, got: %s", result.SQL)
		}
	})

	t.Run("coalesce three fields", func(t *testing.T) {
		pipeline, err := ParseQuery("* | coalesce(user, username, account_name)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "fields.`user`.:String") {
			t.Errorf("Expected user field ref, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "fields.`username`.:String") {
			t.Errorf("Expected username field ref, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "fields.`account_name`.:String") {
			t.Errorf("Expected account_name field ref, got: %s", result.SQL)
		}
	})

	t.Run("sprintf with alias", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | sprintf("%s - %s", username, action, as=user_action)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "printf('%s - %s'") {
			t.Errorf("Expected printf format string, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "fields.`username`.:String") {
			t.Errorf("Expected username field ref, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "fields.`action`.:String") {
			t.Errorf("Expected action field ref, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AS user_action") {
			t.Errorf("Expected user_action alias, got: %s", result.SQL)
		}
	})

	t.Run("sprintf default alias", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | sprintf("%s:%d", host, port)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "printf('%s:%d'") {
			t.Errorf("Expected printf format string, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AS _sprintf") {
			t.Errorf("Expected _sprintf default alias, got: %s", result.SQL)
		}
	})

	t.Run("sprintf format only no fields", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | sprintf("hello world")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "printf('hello world') AS _sprintf") {
			t.Errorf("Expected printf with no args, got: %s", result.SQL)
		}
	})

	t.Run("sprintf with quoted alias", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | sprintf("%s-%s", image, commandline, as="_concat")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "AS _concat") {
			t.Errorf("Expected _concat alias (quotes stripped), got: %s", result.SQL)
		}
	})

	t.Run("strftime with alias", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%H", as=_hour)`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "formatDateTime(timestamp, '%H', 'UTC') AS _hour") {
			t.Errorf("Expected formatDateTime with %%H and _hour alias, got: %s", result.SQL)
		}
	})

	t.Run("strftime default alias", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%Y-%m-%d")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "AS _time") {
			t.Errorf("Expected _time default alias, got: %s", result.SQL)
		}
	})

	t.Run("strftime with custom field and timezone", func(t *testing.T) {
		pipeline, err := ParseQuery(`* | strftime("%a", field=created_at, timezone="US/Eastern")`)
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "formatDateTime(toDateTime(fields.`created_at`.:String)") {
			t.Errorf("Expected custom field ref, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "'US/Eastern'") {
			t.Errorf("Expected US/Eastern timezone, got: %s", result.SQL)
		}
	})
}

func TestTopFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("standalone top", func(t *testing.T) {
		pipeline, err := ParseQuery("* | top(image)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "topK(10)") {
			t.Errorf("Expected topK(10) in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "top_image") {
			t.Errorf("Expected top_image alias, got: %s", result.SQL)
		}
	})

	t.Run("top with custom limit", func(t *testing.T) {
		pipeline, err := ParseQuery("* | top(image, limit=5)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "topK(5)") {
			t.Errorf("Expected topK(5) in SQL, got: %s", result.SQL)
		}
	})

	t.Run("top with percent", func(t *testing.T) {
		pipeline, err := ParseQuery("* | top(image, percent=true)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "topKWeightedWithCount") {
			t.Errorf("Expected topKWeightedWithCount in SQL, got: %s", result.SQL)
		}
	})

	t.Run("top with alias", func(t *testing.T) {
		pipeline, err := ParseQuery("* | top(image, as=top_images)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "AS top_images") {
			t.Errorf("Expected top_images alias, got: %s", result.SQL)
		}
	})

	t.Run("top in multi with groupby", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user) | multi(top(image))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "topK") {
			t.Errorf("Expected topK in SQL, got: %s", result.SQL)
		}
	})
}

func TestSkewnessFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("standalone skewness", func(t *testing.T) {
		pipeline, err := ParseQuery("* | skewness(response_time)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "skewPop(toFloat64OrNull(") {
			t.Errorf("Expected skewPop in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AS _skewness") {
			t.Errorf("Expected _skewness alias, got: %s", result.SQL)
		}
	})

	t.Run("skew alias", func(t *testing.T) {
		pipeline, err := ParseQuery("* | skew(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "skewPop") {
			t.Errorf("Expected skewPop in SQL, got: %s", result.SQL)
		}
	})

	t.Run("skewness with groupby", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host) | skewness(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "skewPop") {
			t.Errorf("Expected skewPop in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY in SQL, got: %s", result.SQL)
		}
	})

	t.Run("skewness in multi", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host) | multi(skewness(latency), kurtosis(latency))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "skewPop") {
			t.Errorf("Expected skewPop in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "kurtPop") {
			t.Errorf("Expected kurtPop in SQL, got: %s", result.SQL)
		}
	})

	t.Run("chained skewness", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host, function=count()) | skewness(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "skewPop(toFloat64(_count))") {
			t.Errorf("Expected skewPop on chained _count, got: %s", result.SQL)
		}
	})
}

func TestKurtosisFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("standalone kurtosis", func(t *testing.T) {
		pipeline, err := ParseQuery("* | kurtosis(response_time)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "kurtPop(toFloat64OrNull(") {
			t.Errorf("Expected kurtPop in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AS _kurtosis") {
			t.Errorf("Expected _kurtosis alias, got: %s", result.SQL)
		}
	})

	t.Run("kurt alias", func(t *testing.T) {
		pipeline, err := ParseQuery("* | kurt(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "kurtPop") {
			t.Errorf("Expected kurtPop in SQL, got: %s", result.SQL)
		}
	})

	t.Run("chained kurtosis", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host, function=count()) | kurtosis(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "kurtPop(toFloat64(_count))") {
			t.Errorf("Expected kurtPop on chained _count, got: %s", result.SQL)
		}
	})
}

func TestFrequencyFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("basic frequency", func(t *testing.T) {
		pipeline, err := ParseQuery("* | frequency(event_name)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "AS value") {
			t.Errorf("Expected value alias, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "count(*) AS _count") {
			t.Errorf("Expected count(*) AS _count, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_percentage") {
			t.Errorf("Expected _percentage, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_cumulative_pct") {
			t.Errorf("Expected _cumulative_pct, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ORDER BY _count DESC") {
			t.Errorf("Expected ORDER BY _count DESC, got: %s", result.SQL)
		}
	})
}

func TestModifiedZScoreFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("standalone", func(t *testing.T) {
		pipeline, err := ParseQuery("* | modifiedZScore(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "_mz_val") {
			t.Errorf("Expected _mz_val in SQL, got: %s", sql)
		}
		if !strings.Contains(sql, "median(_mz_val) OVER () AS _median") {
			t.Errorf("Expected median window function, got: %s", sql)
		}
		if !strings.Contains(sql, "median(abs(_mz_val - _median)) OVER () AS _mad") {
			t.Errorf("Expected MAD window function, got: %s", sql)
		}
		if !strings.Contains(sql, "_modified_z") {
			t.Errorf("Expected _modified_z in SQL, got: %s", sql)
		}
		if !strings.Contains(sql, "0.6745") {
			t.Errorf("Expected 0.6745 constant, got: %s", sql)
		}
		// Verify field order includes computed columns
		found := false
		for _, f := range result.FieldOrder {
			if f == "_modified_z" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected _modified_z in field order, got: %v", result.FieldOrder)
		}
	})

	t.Run("alias modifiedz", func(t *testing.T) {
		pipeline, err := ParseQuery("* | modifiedz(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "_modified_z") {
			t.Errorf("Expected _modified_z from alias, got: %s", result.SQL)
		}
	})

	t.Run("chained aggregation", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user, function=count()) | modifiedZScore(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "toFloat64(_count)") {
			t.Errorf("Expected toFloat64(_count) for chained, got: %s", sql)
		}
		if !strings.Contains(sql, "_modified_z") {
			t.Errorf("Expected _modified_z, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY") {
			t.Errorf("Expected GROUP BY from inner query, got: %s", sql)
		}
	})

	t.Run("handles MAD=0", func(t *testing.T) {
		pipeline, err := ParseQuery("* | modifiedZScore(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		// Should use if(_mad = 0, 0, ...) to handle zero MAD
		if !strings.Contains(result.SQL, "if(_mad = 0, 0,") {
			t.Errorf("Expected MAD=0 guard, got: %s", result.SQL)
		}
	})
}

func TestMadOutlierFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("default threshold", func(t *testing.T) {
		pipeline, err := ParseQuery("* | madOutlier(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "_modified_z") {
			t.Errorf("Expected _modified_z, got: %s", sql)
		}
		if !strings.Contains(sql, "_is_outlier") {
			t.Errorf("Expected _is_outlier, got: %s", sql)
		}
		if !strings.Contains(sql, "3.5") {
			t.Errorf("Expected default threshold 3.5, got: %s", sql)
		}
		// Verify field order
		hasOutlier := false
		for _, f := range result.FieldOrder {
			if f == "_is_outlier" {
				hasOutlier = true
			}
		}
		if !hasOutlier {
			t.Errorf("Expected _is_outlier in field order, got: %v", result.FieldOrder)
		}
	})

	t.Run("custom threshold", func(t *testing.T) {
		pipeline, err := ParseQuery("* | madOutlier(latency, 2.5)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "2.5") {
			t.Errorf("Expected custom threshold 2.5, got: %s", result.SQL)
		}
	})

	t.Run("alias outlier", func(t *testing.T) {
		pipeline, err := ParseQuery("* | outlier(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "_is_outlier") {
			t.Errorf("Expected _is_outlier from alias, got: %s", result.SQL)
		}
	})

	t.Run("chained aggregation", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user, function=count()) | madOutlier(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "toFloat64(_count)") {
			t.Errorf("Expected toFloat64(_count), got: %s", sql)
		}
		if !strings.Contains(sql, "_is_outlier") {
			t.Errorf("Expected _is_outlier, got: %s", sql)
		}
	})

	t.Run("filter on _is_outlier applied to outer wrapper", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user_name, function=count()) | madOutlier(_count) | _is_outlier = 0")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		t.Logf("SQL: %s", sql)
		// _is_outlier filter must be on the outermost WHERE, not in inner HAVING
		if strings.Contains(sql, "HAVING") && strings.Contains(sql, "_is_outlier") {
			// Check that _is_outlier is NOT in a HAVING clause
			havingIdx := strings.Index(sql, "HAVING")
			if havingIdx != -1 {
				havingPart := sql[havingIdx:]
				limitIdx := strings.Index(havingPart, "LIMIT")
				if limitIdx == -1 {
					limitIdx = len(havingPart)
				}
				havingClause := havingPart[:limitIdx]
				if strings.Contains(havingClause, "_is_outlier") {
					t.Errorf("_is_outlier should not be in HAVING, got: %s", sql)
				}
			}
		}
		// Must end with outer WHERE on _is_outlier
		if !strings.Contains(sql, "WHERE _is_outlier") {
			t.Errorf("Expected WHERE _is_outlier on outer wrapper, got: %s", sql)
		}
	})
}

func TestIQRFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("standalone", func(t *testing.T) {
		pipeline, err := ParseQuery("* | iqr(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "quantile(0.25)") {
			t.Errorf("Expected quantile(0.25), got: %s", sql)
		}
		if !strings.Contains(sql, "quantile(0.75)") {
			t.Errorf("Expected quantile(0.75), got: %s", sql)
		}
		if !strings.Contains(sql, "AS _q1") {
			t.Errorf("Expected _q1 alias, got: %s", sql)
		}
		if !strings.Contains(sql, "AS _q3") {
			t.Errorf("Expected _q3 alias, got: %s", sql)
		}
		if !strings.Contains(sql, "AS _iqr") {
			t.Errorf("Expected _iqr alias, got: %s", sql)
		}
	})

	t.Run("with groupby", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host) | iqr(latency)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", sql)
		}
		if !strings.Contains(sql, "quantile(0.25)") {
			t.Errorf("Expected quantile(0.25), got: %s", sql)
		}
	})

	t.Run("chained aggregation", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host, function=count()) | iqr(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "toFloat64(_count)") {
			t.Errorf("Expected toFloat64(_count), got: %s", sql)
		}
		if !strings.Contains(sql, "quantile(0.25)") {
			t.Errorf("Expected quantile(0.25) in outer query, got: %s", sql)
		}
	})

	t.Run("in multi", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(host) | multi(iqr(latency), median(latency))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "quantile(0.25)") {
			t.Errorf("Expected quantile(0.25) from multi, got: %s", sql)
		}
		if !strings.Contains(sql, "median(") {
			t.Errorf("Expected median from multi, got: %s", sql)
		}
	})
}

func TestHeadTailFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "test-fractal",
		MaxRows:   1000,
	}

	t.Run("default threshold", func(t *testing.T) {
		pipeline, err := ParseQuery("* | headTail(event_name)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "AS value") {
			t.Errorf("Expected value alias, got: %s", sql)
		}
		if !strings.Contains(sql, "count(*) AS _count") {
			t.Errorf("Expected count(*) AS _count, got: %s", sql)
		}
		if !strings.Contains(sql, "_percentage") {
			t.Errorf("Expected _percentage, got: %s", sql)
		}
		if !strings.Contains(sql, "_cumulative_pct") {
			t.Errorf("Expected _cumulative_pct, got: %s", sql)
		}
		if !strings.Contains(sql, "_segment") {
			t.Errorf("Expected _segment, got: %s", sql)
		}
		if !strings.Contains(sql, "CASE WHEN") {
			t.Errorf("Expected CASE WHEN for segment, got: %s", sql)
		}
		if !strings.Contains(sql, "<= 80") {
			t.Errorf("Expected default threshold 80, got: %s", sql)
		}
		if !strings.Contains(sql, "'head'") {
			t.Errorf("Expected 'head' label, got: %s", sql)
		}
		if !strings.Contains(sql, "'tail'") {
			t.Errorf("Expected 'tail' label, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", sql)
		}
		if !strings.Contains(sql, "ORDER BY _count DESC") {
			t.Errorf("Expected ORDER BY _count DESC, got: %s", sql)
		}
	})

	t.Run("custom threshold", func(t *testing.T) {
		pipeline, err := ParseQuery("* | headTail(src_ip, 90)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "<= 90") {
			t.Errorf("Expected custom threshold 90, got: %s", result.SQL)
		}
	})
}

func TestHistogram(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("basic histogram", func(t *testing.T) {
		pipeline, err := ParseQuery("* | histogram(response_time)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if result.ChartType != "histogram" {
			t.Errorf("Expected chart type 'histogram', got: %s", result.ChartType)
		}
		if !strings.Contains(result.SQL, "_bin_lower") {
			t.Errorf("Expected _bin_lower, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_bin_upper") {
			t.Errorf("Expected _bin_upper, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_bin_count") {
			t.Errorf("Expected _bin_count, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "/ 20") {
			t.Errorf("Expected 20 buckets in SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", result.SQL)
		}
	})

	t.Run("custom buckets", func(t *testing.T) {
		pipeline, err := ParseQuery("* | histogram(response_time, buckets=50)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "/ 50") {
			t.Errorf("Expected 50 buckets in SQL, got: %s", result.SQL)
		}
		field, ok := result.ChartConfig["field"]
		if !ok || field != "response_time" {
			t.Errorf("Expected chartConfig field=response_time, got: %v", result.ChartConfig)
		}
	})

	t.Run("no field error", func(t *testing.T) {
		pipeline, err := ParseQuery("* | histogram()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Fatalf("Expected error for histogram() with no field")
		}
		if !strings.Contains(err.Error(), "requires a field") {
			t.Errorf("Expected 'requires a field' error, got: %v", err)
		}
	})

	t.Run("chained after headTail", func(t *testing.T) {
		pipeline, err := ParseQuery("* | headTail(src_ip) | histogram(_cumulative_pct)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if result.ChartType != "histogram" {
			t.Errorf("Expected chart type 'histogram', got: %s", result.ChartType)
		}
		// Should wrap the headTail query with histogram bucketing
		if !strings.Contains(result.SQL, "_bin_lower") {
			t.Errorf("Expected _bin_lower in wrapped SQL, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_cumulative_pct") {
			t.Errorf("Expected _cumulative_pct from headTail in inner query, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_segment") {
			t.Errorf("Expected _segment from headTail in inner query, got: %s", result.SQL)
		}
	})
}

func TestHeatmap(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("basic heatmap", func(t *testing.T) {
		pipeline, err := ParseQuery("* | heatmap(x=src_ip, y=dest_port)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if result.ChartType != "heatmap" {
			t.Errorf("Expected chart type 'heatmap', got: %s", result.ChartType)
		}
		if !strings.Contains(result.SQL, "_heatmap_x") {
			t.Errorf("Expected _heatmap_x, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "_heatmap_y") {
			t.Errorf("Expected _heatmap_y, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("Expected COUNT(*) as default value, got: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("Expected GROUP BY, got: %s", result.SQL)
		}
	})

	t.Run("heatmap with custom value", func(t *testing.T) {
		pipeline, err := ParseQuery("* | heatmap(x=src_ip, y=dest_port, value=avg(bytes))")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Failed to translate: %v", err)
		}
		if !strings.Contains(result.SQL, "avg(") {
			t.Errorf("Expected avg() in SQL, got: %s", result.SQL)
		}
	})

	t.Run("missing fields error", func(t *testing.T) {
		pipeline, err := ParseQuery("* | heatmap(x=src_ip)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, opts)
		if err == nil {
			t.Fatalf("Expected error for heatmap() with missing y=")
		}
		if !strings.Contains(err.Error(), "requires x= and y=") {
			t.Errorf("Expected 'requires x= and y=' error, got: %v", err)
		}
	})
}

func TestMultiGroupBy(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("two stage groupby count then re-count", func(t *testing.T) {
		// Count per src_ip, then re-group by _count to get count distribution
		pipeline, err := ParseQuery("* | groupby(src_ip, function=count()) | groupby(_count, function=count())")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		sql := result.SQL

		// Should have nested subquery structure
		if !strings.Contains(sql, "FROM (SELECT") {
			t.Errorf("Expected nested subquery, got: %s", sql)
		}
		// Inner query should group by src_ip
		if !strings.Contains(sql, "GROUP BY src_ip") {
			t.Errorf("Expected inner GROUP BY src_ip, got: %s", sql)
		}
		if !result.IsAggregated {
			t.Errorf("Expected IsAggregated=true")
		}
	})

	t.Run("two stage groupby with sum across groups", func(t *testing.T) {
		// Count per country+city, then sum counts per country
		pipeline, err := ParseQuery("* | groupby(country, city, function=count()) | groupby(country) | sum(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		sql := result.SQL

		// Inner stage: GROUP BY country, city with COUNT(*)
		if !strings.Contains(sql, "GROUP BY country, city") {
			t.Errorf("Expected inner GROUP BY country, city, got: %s", sql)
		}
		// Outer stage: GROUP BY country with sum(_count)
		if !strings.Contains(sql, "sum(toFloat64OrNull(_count))") {
			t.Errorf("Expected sum(toFloat64OrNull(_count)) in outer stage, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY country") {
			t.Errorf("Expected GROUP BY country in outer stage, got: %s", sql)
		}
		// Field order should include country and _sum
		for _, expected := range []string{"country", "_sum"} {
			found := false
			for _, f := range result.FieldOrder {
				if f == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected %s in FieldOrder %v", expected, result.FieldOrder)
			}
		}
	})

	t.Run("multi-stage preserves inner stage selects", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(user, function=count()) | groupby(_count, function=count())")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		sql := result.SQL

		// Inner query should select user and COUNT(*)
		if !strings.Contains(sql, "user") {
			t.Errorf("Expected 'user' in inner query, got: %s", sql)
		}
		if !strings.Contains(sql, "COUNT(*)") {
			t.Errorf("Expected COUNT(*) in query, got: %s", sql)
		}
	})

	t.Run("chained agg without second groupby still works", func(t *testing.T) {
		// Single groupby + avg of aggregation output (chained path, not multi-stage)
		pipeline, err := ParseQuery("* | groupby(src_ip, function=count()) | avg(_count)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		sql := result.SQL

		// Should wrap in an outer SELECT with avg
		if !strings.Contains(sql, "avg(toFloat64(_count))") {
			t.Errorf("Expected chained avg(toFloat64(_count)), got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY src_ip") {
			t.Errorf("Expected GROUP BY src_ip in inner query, got: %s", sql)
		}
	})
}

func TestGroupByCountGroups(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	t.Run("groupby then count counts groups", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(computer_name) | count()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		sql := result.SQL
		// Should have nested subquery: inner groups, outer counts groups
		if !strings.Contains(sql, "FROM (SELECT") {
			t.Errorf("Expected nested subquery, got: %s", sql)
		}
		if !strings.Contains(sql, "GROUP BY computer_name") {
			t.Errorf("Expected GROUP BY computer_name in inner query, got: %s", sql)
		}
		if !result.IsAggregated {
			t.Error("Expected IsAggregated=true")
		}
	})

	t.Run("groupby count singleval", func(t *testing.T) {
		pipeline, err := ParseQuery("* | groupby(computer_name) | count() | singleval()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		if result.ChartType != "singleval" {
			t.Errorf("Expected chart type 'singleval', got %q", result.ChartType)
		}
	})

	t.Run("groupby default function is count", func(t *testing.T) {
		// groupby(field) alone should produce COUNT(*) as default
		pipeline, err := ParseQuery("* | groupby(status)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("Expected COUNT(*) in output, got: %s", result.SQL)
		}
		if !result.IsAggregated {
			t.Error("Expected IsAggregated=true")
		}
	})

	t.Run("count with unique=true", func(t *testing.T) {
		pipeline, err := ParseQuery("* | count(computer_name, unique=true)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		if !strings.Contains(result.SQL, "uniqExact") {
			t.Errorf("Expected uniqExact in query, got: %s", result.SQL)
		}
	})

	t.Run("count with field", func(t *testing.T) {
		pipeline, err := ParseQuery("* | count(computer_name)")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		sql := result.SQL
		if !strings.Contains(sql, "count(") {
			t.Errorf("Expected count(field) in query, got: %s", sql)
		}
	})

	t.Run("singleval works with chained outer aggregation", func(t *testing.T) {
		// groupby + chained agg (outerAggregations path) + singleval should work
		pipeline, err := ParseQuery("* | groupby(src_ip, function=count()) | avg(_count) | singleval()")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		result, err := TranslateToSQLWithOrder(pipeline, opts)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}
		if result.ChartType != "singleval" {
			t.Errorf("Expected chart type 'singleval', got %q", result.ChartType)
		}
	})
}

func TestExtractLiteralTokens(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{
			name:    "simple literal",
			pattern: "Convert-GuidToCompressedGuid",
			want:    []string{"convert", "guidtocompressedguid"},
		},
		{
			name:    "case-insensitive prefix stripped",
			pattern: "(?i)Convert-GuidToCompressedGuid",
			want:    []string{"convert", "guidtocompressedguid"},
		},
		{
			name:    "regex metacharacters skipped",
			pattern: "error.*failed",
			want:    []string{"error", "failed"},
		},
		{
			name:    "short tokens filtered out",
			pattern: "a-bb-ccc",
			want:    []string{"ccc"},
		},
		{
			name:    "no usable tokens",
			pattern: `\d+\.\d+`,
			want:    nil,
		},
		{
			name:    "character class skipped",
			pattern: "[A-Z]+foo[0-9]bar",
			want:    []string{"foo", "bar"},
		},
		{
			name:    "duplicate tokens deduplicated",
			pattern: "Error-error-ERROR",
			want:    []string{"error"},
		},
		{
			name:    "alternation breaks tokens",
			pattern: "powershell|cmd",
			want:    []string{"powershell", "cmd"},
		},
		{
			name:    "escaped characters skipped",
			pattern: `foo\.bar\\baz`,
			want:    []string{"foo", "bar", "baz"},
		},
		{
			name:    "complex regex with some literals",
			pattern: `(?i)^.*Convert\-GuidTo.*$`,
			want:    []string{"convert", "guidto"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractLiteralTokens(tt.pattern)
			if len(got) != len(tt.want) {
				t.Fatalf("extractLiteralTokens(%q) = %v, want %v", tt.pattern, got, tt.want)
			}
			for i, w := range tt.want {
				if got[i] != w {
					t.Errorf("token[%d] = %q, want %q", i, got[i], w)
				}
			}
		})
	}
}

func TestBuildRegexMatchSQL(t *testing.T) {
	tests := []struct {
		name       string
		fieldRef   string
		pattern    string
		negate     bool
		wantParts  []string
		wantNoParts []string
	}{
		{
			name:     "raw_log case-sensitive regex adds hasToken pre-filters",
			fieldRef: "raw_log",
			pattern:  "Convert-GuidToCompressedGuid",
			negate:   false,
			wantParts: []string{
				"hasToken(raw_log, 'convert')",
				"hasToken(raw_log, 'guidtocompressedguid')",
				"match(raw_log, 'Convert-GuidToCompressedGuid')",
			},
		},
		{
			name:     "raw_log case-insensitive regex also uses hasToken (preprocessor handles lowering)",
			fieldRef: "raw_log",
			pattern:  "(?i)Convert-GuidToCompressedGuid",
			negate:   false,
			wantParts: []string{
				"hasToken(raw_log, 'convert')",
				"hasToken(raw_log, 'guidtocompressedguid')",
				"match(raw_log, '(?i)Convert-GuidToCompressedGuid')",
			},
			wantNoParts: []string{
				"hasTokenCaseInsensitive",
			},
		},
		{
			name:     "negated regex skips pre-filters",
			fieldRef: "raw_log",
			pattern:  "Convert-GuidToCompressedGuid",
			negate:   true,
			wantParts: []string{
				"NOT match(raw_log, 'Convert-GuidToCompressedGuid')",
			},
			wantNoParts: []string{
				"hasToken",
			},
		},
		{
			name:     "non-raw_log field skips pre-filters",
			fieldRef: "fields.`message`.:String",
			pattern:  "(?i)powershell",
			negate:   false,
			wantParts: []string{
				"match(fields.`message`.:String, '(?i)powershell')",
			},
			wantNoParts: []string{
				"hasToken",
			},
		},
		{
			name:     "no extractable tokens falls back to plain match",
			fieldRef: "raw_log",
			pattern:  `\d+\.\d+`,
			negate:   false,
			wantParts: []string{
				`match(raw_log, '\\d+\\.\\d+')`,
			},
			wantNoParts: []string{
				"hasToken",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildRegexMatchSQL(tt.fieldRef, tt.pattern, tt.negate)
			for _, part := range tt.wantParts {
				if !strings.Contains(got, part) {
					t.Errorf("expected SQL to contain %q, got: %s", part, got)
				}
			}
			for _, part := range tt.wantNoParts {
				if strings.Contains(got, part) {
					t.Errorf("expected SQL to NOT contain %q, got: %s", part, got)
				}
			}
		})
	}
}

func TestRegexTokenPrefilterIntegration(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}

	tests := []struct {
		name        string
		query       string
		wantContain []string
		wantNotContain []string
	}{
		{
			name:  "bare case-insensitive regex on raw_log gets hasToken pre-filters",
			query: "/Convert-GuidToCompressedGuid/i",
			wantContain: []string{
				"hasToken(raw_log, 'convert')",
				"hasToken(raw_log, 'guidtocompressedguid')",
				"match(raw_log, '(?i)Convert-GuidToCompressedGuid')",
			},
			wantNotContain: []string{
				"hasTokenCaseInsensitive",
			},
		},
		{
			name:  "bare case-sensitive regex on raw_log gets hasToken pre-filters",
			query: "/Convert-GuidToCompressedGuid/",
			wantContain: []string{
				"hasToken(raw_log, 'convert')",
				"hasToken(raw_log, 'guidtocompressedguid')",
				"match(raw_log, 'Convert-GuidToCompressedGuid')",
			},
		},
		{
			name:  "bare string search on raw_log gets hasToken pre-filters",
			query: `"powershell.exe"`,
			wantContain: []string{
				"hasToken(raw_log, 'powershell')",
				"hasToken(raw_log, 'exe')",
				"match(raw_log,",
			},
		},
		{
			name:  "bare strings with OR get correct OR logic",
			query: `"prod-billing-9" OR "prod-billing-10"`,
			wantContain: []string{
				"match(raw_log, 'prod-billing-9')",
				"match(raw_log, 'prod-billing-10')",
				" OR ",
			},
		},
		{
			name:  "bare strings with AND get correct AND logic",
			query: `"prod-billing-9" AND "prod-billing-10"`,
			wantContain: []string{
				"match(raw_log, 'prod-billing-9')",
				"match(raw_log, 'prod-billing-10')",
			},
		},
		{
			name:  "three bare strings with OR",
			query: `"foo" OR "bar" OR "baz"`,
			wantContain: []string{
				"match(raw_log, 'foo')",
				"match(raw_log, 'bar')",
				"match(raw_log, 'baz')",
				" OR ",
			},
		},
		{
			name:  "bare strings with OR in pipeline are grouped",
			query: `* | service="test" | "A" OR "B" | user=/admin/i`,
			wantContain: []string{
				"match(raw_log, 'A') OR match(raw_log, 'B')",
				"service",
				"match(fields.`user`.:String, '(?i)admin')",
			},
		},
		{
			name:  "bare strings with AND in pipeline are grouped",
			query: `* | service="test" | "A" AND "B" | user=/admin/i`,
			wantContain: []string{
				"match(raw_log, 'A') AND match(raw_log, 'B')",
				"service",
				"match(fields.`user`.:String, '(?i)admin')",
			},
		},
		{
			name:  "field regex does NOT get hasToken pre-filters",
			query: "image=/powershell/i",
			wantContain: []string{
				"match(fields.`image`.:String, '(?i)powershell')",
			},
			wantNotContain: []string{
				"hasToken",
			},
		},
		{
			name:  "negated raw_log regex does NOT get hasToken pre-filters",
			query: "/powershell/i | command_line!=/cmd/i",
			wantContain: []string{
				"NOT match(fields.`command_line`.:String, '(?i)cmd')",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}
			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("Failed to translate: %v", err)
			}
			for _, want := range tt.wantContain {
				if !strings.Contains(result.SQL, want) {
					t.Errorf("expected SQL to contain %q\ngot: %s", want, result.SQL)
				}
			}
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(result.SQL, notWant) {
					t.Errorf("expected SQL to NOT contain %q\ngot: %s", notWant, result.SQL)
				}
			}
		})
	}
}

func TestAlertAutoProjection(t *testing.T) {
	baseOpts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   10000,
	}
	alertOpts := baseOpts
	alertOpts.UseIngestTimestamp = true

	tests := []struct {
		name           string
		query          string
		opts           QueryOptions
		wantContain    []string
		wantNotContain []string
	}{
		{
			name:  "Alert simple filter projects only referenced fields",
			query: "image=/powershell/i",
			opts:  alertOpts,
			wantContain: []string{
				"SELECT", "timestamp", "log_id",
				"fields.`image`.:String",
			},
			wantNotContain: []string{
				"raw_log", "toString(fields)",
			},
		},
		{
			name:  "Alert multi-field filter projects all referenced fields",
			query: `image=/powershell/i user=admin`,
			opts:  alertOpts,
			wantContain: []string{
				"timestamp", "log_id",
				"fields.`image`.:String",
				"fields.`user`.:String",
			},
			wantNotContain: []string{
				"raw_log", "toString(fields)",
			},
		},
		{
			name:  "Alert bare regex on raw_log projects minimal columns",
			query: `/powershell/`,
			opts:  alertOpts,
			wantContain: []string{
				"log_id", "timestamp",
			},
			wantNotContain: []string{
				"toString(fields) AS fields",
			},
		},
		{
			name:  "Alert with table() command is unchanged",
			query: `image=/powershell/i | table(image, user)`,
			opts:  alertOpts,
			wantContain: []string{
				"fields.`image`.:String",
				"fields.`user`.:String",
			},
		},
		{
			name:  "User query same filter gets full SELECT",
			query: "image=/powershell/i",
			opts:  baseOpts,
			wantContain: []string{
				"raw_log", "log_id", "toString(fields) AS fields",
			},
		},
		{
			name:  "Alert bare wildcard gets full SELECT",
			query: "*",
			opts:  alertOpts,
			wantContain: []string{
				"raw_log", "log_id", "toString(fields) AS fields",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			result, err := TranslateToSQLWithOrder(pipeline, tt.opts)
			if err != nil {
				t.Fatalf("translate error: %v", err)
			}
			for _, want := range tt.wantContain {
				if !strings.Contains(result.SQL, want) {
					t.Errorf("expected SQL to contain %q\ngot: %s", want, result.SQL)
				}
			}
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(result.SQL, notWant) {
					t.Errorf("expected SQL to NOT contain %q\ngot: %s", notWant, result.SQL)
				}
			}
		})
	}
}

func TestJoinFunction(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "test-fractal-123",
	}

	tests := []struct {
		name           string
		query          string
		wantContain    []string
		wantNotContain []string
		wantErr        bool
		errContains    string
	}{
		{
			name:  "Basic inner join",
			query: `action="login_failed" | join(user) { action="login_success" | groupby(user, function=count()) }`,
			wantContain: []string{
				"INNER JOIN",
				"_outer",
				"_join_sub",
				"= _join_sub.user",
				"fractal_id = 'test-fractal-123'",
			},
		},
		{
			name:  "Left join with type parameter",
			query: `* | join(src_ip, type=left) { * | groupby(src_ip, function=count()) }`,
			wantContain: []string{
				"LEFT JOIN",
				"= _join_sub.src_ip",
			},
		},
		{
			name:  "Join with include parameter",
			query: `* | join(user, include=[department,role]) { * | groupby(user, function=count()) }`,
			wantContain: []string{
				"INNER JOIN",
				"_join_sub.department AS _join_department",
				"_join_sub.role AS _join_role",
			},
		},
		{
			name:  "Join with max parameter",
			query: `* | join(user, max=500) { * | groupby(user, function=count()) }`,
			wantContain: []string{
				"INNER JOIN",
				"LIMIT 500",
			},
		},
		{
			name:  "Join enforces fractal isolation on subquery",
			query: `* | join(user) { action="login" | groupby(user) }`,
			wantContain: []string{
				"fractal_id = 'test-fractal-123'",
			},
		},
		{
			name:        "Join with invalid type",
			query:       `* | join(user, type=cross) { * | count() }`,
			wantErr:     true,
			errContains: "type must be 'inner' or 'left'",
		},
		{
			name:        "Join without key",
			query:       `* | join(type=inner) { * | count() }`,
			wantErr:     true,
			errContains: "requires a join key",
		},
		{
			name:        "Nested join rejected",
			query:       `* | join(user) { * | join(ip) { * | count() } }`,
			wantErr:     true,
			errContains: "nested join",
		},
		{
			name:        "Join with empty subquery",
			query:       `* | join(user) { }`,
			wantErr:     true,
			errContains: "subquery cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := ParseQuery(tt.query)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("parse error: %v", err)
			}
			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("expected error containing %q, got: %v", tt.errContains, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("translate error: %v", err)
			}
			for _, want := range tt.wantContain {
				if !strings.Contains(result.SQL, want) {
					t.Errorf("expected SQL to contain %q\ngot: %s", want, result.SQL)
				}
			}
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(result.SQL, notWant) {
					t.Errorf("expected SQL to NOT contain %q\ngot: %s", notWant, result.SQL)
				}
			}
		})
	}
}

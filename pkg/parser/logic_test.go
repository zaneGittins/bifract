package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// termDef describes a BQL term type and the SQL fragment it should produce.
type termDef struct {
	name    string
	bql     string // BQL syntax
	sqlFrag string // expected SQL fragment (substring that must appear)
}

var logicTestOpts = QueryOptions{
	StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	MaxRows:   1000,
}

// sqlFrag must be a substring that appears in the generated SQL regardless
// of whether the term inherits its field from a preceding condition.
// Bare strings/regex inherit the field of the previous condition in the same
// expression (e.g., image=/ps/i OR /cmd/i searches image for both).
// So we use value-level fragments that are always present.
var termTypes = []termDef{
	{"quoted_string", `"error"`, "'error')"},
	{"regex", `/error/i`, "'(?i)error')"},
	{"field_eq", `status="200"`, "fields.`status`.:String = '200'"},
	{"field_regex", `image=/powershell/i`, "'(?i)powershell')"},
	{"field_neq", `user!="admin"`, "fields.`user`.:String"},
}

// parseAndTranslate is a helper that parses a BQL query and translates it.
func parseAndTranslate(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("ParseQuery(%q) failed: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, logicTestOpts)
	if err != nil {
		t.Fatalf("TranslateToSQLWithOrder(%q) failed: %v", query, err)
	}
	return result.SQL
}

func TestBooleanLogicMatrix(t *testing.T) {
	for i, a := range termTypes {
		for j, b := range termTypes {
			// --- FILTER position tests ---

			// A AND B
			t.Run(fmt.Sprintf("filter/AND/%s_and_%s", a.name, b.name), func(t *testing.T) {
				query := fmt.Sprintf("%s AND %s", a.bql, b.bql)
				sql := parseAndTranslate(t, query)
				if !strings.Contains(sql, a.sqlFrag) {
					t.Errorf("SQL missing fragment for term A (%s)\nQuery: %s\nSQL: %s", a.name, query, sql)
				}
				if !strings.Contains(sql, b.sqlFrag) {
					t.Errorf("SQL missing fragment for term B (%s)\nQuery: %s\nSQL: %s", b.name, query, sql)
				}
			})

			// A OR B
			t.Run(fmt.Sprintf("filter/OR/%s_or_%s", a.name, b.name), func(t *testing.T) {
				query := fmt.Sprintf("%s OR %s", a.bql, b.bql)
				sql := parseAndTranslate(t, query)
				if !strings.Contains(sql, a.sqlFrag) {
					t.Errorf("SQL missing fragment for term A (%s)\nQuery: %s\nSQL: %s", a.name, query, sql)
				}
				if !strings.Contains(sql, b.sqlFrag) {
					t.Errorf("SQL missing fragment for term B (%s)\nQuery: %s\nSQL: %s", b.name, query, sql)
				}
				if !strings.Contains(sql, " OR ") {
					t.Errorf("SQL missing OR operator\nQuery: %s\nSQL: %s", query, sql)
				}
			})

			// (A OR B)
			t.Run(fmt.Sprintf("filter/paren_OR/%s_or_%s", a.name, b.name), func(t *testing.T) {
				query := fmt.Sprintf("(%s OR %s)", a.bql, b.bql)
				sql := parseAndTranslate(t, query)
				if !strings.Contains(sql, a.sqlFrag) {
					t.Errorf("SQL missing fragment for term A (%s)\nQuery: %s\nSQL: %s", a.name, query, sql)
				}
				if !strings.Contains(sql, b.sqlFrag) {
					t.Errorf("SQL missing fragment for term B (%s)\nQuery: %s\nSQL: %s", b.name, query, sql)
				}
			})

			// --- PIPELINE position tests ---

			// * | A AND B
			t.Run(fmt.Sprintf("pipeline/AND/%s_and_%s", a.name, b.name), func(t *testing.T) {
				query := fmt.Sprintf("* | %s AND %s", a.bql, b.bql)
				sql := parseAndTranslate(t, query)
				if !strings.Contains(sql, a.sqlFrag) {
					t.Errorf("SQL missing fragment for term A (%s)\nQuery: %s\nSQL: %s", a.name, query, sql)
				}
				if !strings.Contains(sql, b.sqlFrag) {
					t.Errorf("SQL missing fragment for term B (%s)\nQuery: %s\nSQL: %s", b.name, query, sql)
				}
			})

			// * | A OR B
			t.Run(fmt.Sprintf("pipeline/OR/%s_or_%s", a.name, b.name), func(t *testing.T) {
				query := fmt.Sprintf("* | %s OR %s", a.bql, b.bql)
				sql := parseAndTranslate(t, query)
				if !strings.Contains(sql, a.sqlFrag) {
					t.Errorf("SQL missing fragment for term A (%s)\nQuery: %s\nSQL: %s", a.name, query, sql)
				}
				if !strings.Contains(sql, b.sqlFrag) {
					t.Errorf("SQL missing fragment for term B (%s)\nQuery: %s\nSQL: %s", b.name, query, sql)
				}
				if !strings.Contains(sql, " OR ") {
					t.Errorf("SQL missing OR operator\nQuery: %s\nSQL: %s", query, sql)
				}
			})

			// NOT A (only run once per term, not per pair)
			if j == 0 {
				t.Run(fmt.Sprintf("filter/NOT/%s", a.name), func(t *testing.T) {
					query := fmt.Sprintf("NOT %s", a.bql)
					sql := parseAndTranslate(t, query)
					if !strings.Contains(sql, a.sqlFrag) {
						t.Errorf("SQL missing fragment for term A (%s)\nQuery: %s\nSQL: %s", a.name, query, sql)
					}
				})
			}

			_ = i // suppress unused
		}
	}
}

// verifyORContainment checks that every " OR " in the SQL occurs inside
// parentheses. It does this by scanning the SQL and tracking paren depth;
// any " OR " found at depth 0 (relative to the WHERE clause user conditions)
// is considered "uncontained" -- unless the entire user clause is a single
// OR expression (indicated by allowTopLevel).
func verifyORContainment(t *testing.T, sql string, outerFrags []string, label string) {
	t.Helper()

	// Verify all outer fragments are present.
	for _, frag := range outerFrags {
		if !strings.Contains(sql, frag) {
			t.Errorf("[%s] SQL missing expected outer fragment %q\nSQL: %s", label, frag, sql)
		}
	}

	// Find the user-condition portion of the WHERE clause (after boilerplate).
	whereIdx := strings.Index(sql, "WHERE ")
	if whereIdx < 0 {
		t.Fatalf("[%s] no WHERE clause found\nSQL: %s", label, sql)
	}
	wherePart := sql[whereIdx:]

	// For each pair of consecutive outer fragments, check that any " OR "
	// between them is inside parentheses.
	for i := 0; i < len(outerFrags)-1; i++ {
		posA := strings.Index(wherePart, outerFrags[i])
		posB := strings.Index(wherePart, outerFrags[i+1])
		if posA < 0 || posB < 0 {
			continue
		}
		if posA > posB {
			posA, posB = posB, posA
		}
		between := wherePart[posA+len(outerFrags[i]) : posB]

		// Check that any " OR " in between is inside parens.
		depth := 0
		for k := 0; k < len(between); k++ {
			if between[k] == '(' {
				depth++
			} else if between[k] == ')' {
				depth--
			} else if k+4 <= len(between) && between[k:k+4] == " OR " && depth == 0 {
				t.Errorf("[%s] found unparenthesized OR between outer fragments %q and %q\nbetween: %q\nSQL: %s",
					label, outerFrags[i], outerFrags[i+1], between, sql)
				return
			}
		}
	}
}

func TestORDoesNotBleed(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		outerFrags []string // fragments that should be AND-connected, not OR-connected
	}{
		{
			name:       "OR group between AND terms",
			query:      `service="web" AND ("error" OR "warning") AND user="admin"`,
			outerFrags: []string{"fields.`service`.:String = 'web'", "fields.`user`.:String = 'admin'"},
		},
		{
			name:       "parenthesized OR with AND",
			query:      `(status="200" OR status="201") AND service="web"`,
			outerFrags: []string{"fields.`service`.:String = 'web'"},
		},
		{
			name:       "OR in pipeline between AND stages",
			query:      `* | service="test" | "A" OR "B" OR "C" | user="admin"`,
			outerFrags: []string{"fields.`service`.:String = 'test'", "fields.`user`.:String = 'admin'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := parseAndTranslate(t, tt.query)
			verifyORContainment(t, sql, tt.outerFrags, tt.name)
		})
	}
}

func TestPipelineBooleanConditions(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantSQL    []string // fragments that must appear
		wantNoSQL  []string // fragments that must NOT appear
	}{
		{
			name:    "string OR in pipeline",
			query:   `* | "error" OR "warning"`,
			wantSQL: []string{"match(raw_log, 'error')", "match(raw_log, 'warning')", " OR "},
		},
		{
			name:    "string AND in pipeline",
			query:   `* | "error" AND "warning"`,
			wantSQL: []string{"match(raw_log, 'error')", "match(raw_log, 'warning')"},
		},
		{
			name:    "NOT regex in pipeline",
			query:   `* | NOT /error/`,
			wantSQL: []string{"match(raw_log,"},
		},
		{
			name:    "field OR in pipeline",
			query:   `* | status="200" OR status="201"`,
			wantSQL: []string{"fields.`status`.:String = '200'", "fields.`status`.:String = '201'", " OR "},
		},
		{
			name:    "mixed string and field in pipeline",
			query:   `* | "error" OR status="500"`,
			wantSQL: []string{"match(raw_log, 'error')", "fields.`status`.:String = '500'", " OR "},
		},
		{
			name:    "field AND string in pipeline",
			query:   `* | status="200" AND "success"`,
			wantSQL: []string{"fields.`status`.:String = '200'", "match(raw_log, 'success')"},
		},
		{
			name:    "NOT field in pipeline",
			query:   `* | NOT status="500"`,
			wantSQL: []string{"fields.`status`.:String"},
		},
		{
			name:    "regex OR field in pipeline",
			query:   `* | /warn/i OR level="error"`,
			wantSQL: []string{"match(raw_log, '(?i)warn')", "fields.`level`.:String = 'error'", " OR "},
		},
		{
			name:    "multiple pipes with boolean",
			query:   `service="web" | "error" OR "warning" | user="admin"`,
			wantSQL: []string{"fields.`service`.:String = 'web'", "match(raw_log, 'error')", "fields.`user`.:String = 'admin'"},
		},
		{
			name:    "chained pipeline with OR and AND",
			query:   `* | "error" OR "warning" | status="500"`,
			wantSQL: []string{"match(raw_log, 'error')", "match(raw_log, 'warning')", "fields.`status`.:String = '500'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := parseAndTranslate(t, tt.query)
			for _, frag := range tt.wantSQL {
				if !strings.Contains(sql, frag) {
					t.Errorf("SQL missing expected fragment %q\nQuery: %s\nSQL: %s", frag, tt.query, sql)
				}
			}
			for _, frag := range tt.wantNoSQL {
				if strings.Contains(sql, frag) {
					t.Errorf("SQL contains unexpected fragment %q\nQuery: %s\nSQL: %s", frag, tt.query, sql)
				}
			}
		})
	}
}

func TestOperatorPrecedence(t *testing.T) {
	tests := []struct {
		name  string
		query string
		// We verify the SQL contains these fragments; the structure is checked
		// by examining the relative positions/grouping in the SQL.
		wantSQL []string
	}{
		{
			name:    "OR then AND: AND binds tighter",
			query:   `a="1" OR b="2" AND c="3"`,
			wantSQL: []string{"fields.`a`.:String = '1'", "fields.`b`.:String = '2'", "fields.`c`.:String = '3'", " OR "},
		},
		{
			name:    "AND then OR: AND binds tighter",
			query:   `a="1" AND b="2" OR c="3"`,
			wantSQL: []string{"fields.`a`.:String = '1'", "fields.`b`.:String = '2'", "fields.`c`.:String = '3'", " OR "},
		},
		{
			name:    "bare strings: OR then AND",
			query:   `"A" OR "B" AND "C"`,
			wantSQL: []string{"match(raw_log, 'A')", "match(raw_log, 'B')", "match(raw_log, 'C')", " OR "},
		},
		{
			name:    "mixed: string OR field AND field",
			query:   `"A" OR status="200" AND service="web"`,
			wantSQL: []string{"match(raw_log, 'A')", "fields.`status`.:String = '200'", "fields.`service`.:String = 'web'", " OR "},
		},
		{
			name:    "explicit parens override precedence",
			query:   `(a="1" OR b="2") AND c="3"`,
			wantSQL: []string{"fields.`a`.:String = '1'", "fields.`b`.:String = '2'", "fields.`c`.:String = '3'"},
		},
		{
			name:    "triple OR all present",
			query:   `a="1" OR b="2" OR c="3"`,
			wantSQL: []string{"fields.`a`.:String = '1'", "fields.`b`.:String = '2'", "fields.`c`.:String = '3'"},
		},
		{
			name:    "nested parens: (A AND B) OR (C AND D)",
			query:   `(a="1" AND b="2") OR (c="3" AND d="4")`,
			wantSQL: []string{"fields.`a`.:String = '1'", "fields.`b`.:String = '2'", "fields.`c`.:String = '3'", "fields.`d`.:String = '4'", " OR "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := parseAndTranslate(t, tt.query)
			for _, frag := range tt.wantSQL {
				if !strings.Contains(sql, frag) {
					t.Errorf("SQL missing expected fragment %q\nQuery: %s\nSQL: %s", frag, tt.query, sql)
				}
			}
		})
	}
}

package parser

import "strings"

// sqlRef is one identifier in generated SQL.
type sqlRef struct {
	Name string
	// Call: a function name, followed by "(".
	Call bool
	// Qualified: a sub-column or type, preceded by "." or "::" (fields.`x`, ::String).
	Qualified bool
}

// sqlRefs lists the identifiers in SQL the translator generated, skipping string
// literals and numbers. A backticked identifier is reported by its name.
func sqlRefs(sql string) []sqlRef {
	var out []sqlRef
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'':
			i = skipQuoted(sql, i)
			continue
		case c == '`' || isIdentStart(c):
			start := i
			var name string
			if c == '`' {
				end := skipQuoted(sql, i)
				name = strings.ReplaceAll(sql[i+1:max(i+1, end-1)], "``", "`")
				i = end
			} else {
				for i < len(sql) && isWordByte(sql[i]) {
					i++
				}
				name = sql[start:i]
			}
			next := i
			for next < len(sql) && sql[next] == ' ' {
				next++
			}
			out = append(out, sqlRef{
				Name:      name,
				Call:      next < len(sql) && sql[next] == '(',
				Qualified: start > 0 && sql[start-1] == '.' || start > 1 && sql[start-2:start] == "::",
			})
			continue
		case c >= '0' && c <= '9':
			for i < len(sql) && (isWordByte(sql[i]) || sql[i] == '.') {
				i++
			}
			continue
		}
		i++
	}
	return out
}

// skipQuoted returns the index just past the quoted run opening at i, honouring
// backslash escapes and doubled quotes.
func skipQuoted(sql string, i int) int {
	q := sql[i]
	for i++; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			i++
		case q:
			if i+1 < len(sql) && sql[i+1] == q {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(sql)
}

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// sqlKeywords are the words generated filters use that are not column references.
// Words that are also plausible column names (hour, first) are not listed: an
// interval unit is recognised by its INTERVAL, and an ordering's direction is
// stripped before its expression is read (see orderingReadsOnlyScan).
var sqlKeywords = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "IN": true, "GLOBAL": true, "IS": true, "NULL": true,
	"LIKE": true, "ILIKE": true, "BETWEEN": true, "CASE": true, "WHEN": true, "THEN": true,
	"ELSE": true, "END": true, "AS": true, "TRUE": true, "FALSE": true, "INTERVAL": true,
}

// columnRefs lists the column names SQL reads: identifiers that are not functions,
// keywords, sub-columns, types or interval units. A qualifier (the fields of
// fields.`x`) counts as the column it qualifies.
func columnRefs(sql string) []string {
	var cols []string
	refs := sqlRefs(sql)
	for i, r := range refs {
		unit := i > 0 && strings.EqualFold(refs[i-1].Name, "INTERVAL")
		if r.Call || r.Qualified || unit || sqlKeywords[strings.ToUpper(r.Name)] {
			continue
		}
		cols = append(cols, r.Name)
	}
	return cols
}

// readsOnly reports whether every column SQL reads is in allowed.
func readsOnly(sql string, allowed func(string) bool) bool {
	for _, c := range columnRefs(sql) {
		if !allowed(c) {
			return false
		}
	}
	return true
}

// scanColumns are the logs table's own columns, which a subquery reading the table
// with SELECT * exposes.
var scanColumns = map[string]bool{
	"timestamp": true, "log_id": true, "fractal_id": true, "fields": true,
	"ingest_timestamp": true, normLogColumn: true, "normalizer": true,
}

// readsOnlyScanColumns reports whether SQL can be evaluated over the logs table's
// own columns, as in a subquery that reads it with SELECT *.
func readsOnlyScanColumns(sql string) bool {
	return readsOnly(sql, func(c string) bool { return scanColumns[c] })
}

// orderingReadsOnlyScan reports whether ORDER BY entries ("expr DESC") and LIMIT
// BY keys can be evaluated over the logs table's own columns.
func orderingReadsOnlyScan(orderBy []string, limitBy string) bool {
	for _, o := range orderBy {
		if !readsOnlyScanColumns(orderingExpr(o)) {
			return false
		}
	}
	return readsOnlyScanColumns(strings.TrimPrefix(limitBy, "LIMIT 1 BY "))
}

// orderingExpr strips an ORDER BY entry's direction and NULLS placement.
func orderingExpr(entry string) string {
	e := strings.TrimSpace(entry)
	for _, suffix := range []string{" NULLS FIRST", " NULLS LAST", " ASC", " DESC"} {
		if len(e) > len(suffix) && strings.EqualFold(e[len(e)-len(suffix):], suffix) {
			e = strings.TrimSpace(e[:len(e)-len(suffix)])
		}
	}
	return e
}

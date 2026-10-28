package db

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

var (
	fkCreateTableRe = regexp.MustCompile(`(?i)^\s*CREATE TABLE IF NOT EXISTS (\w+)`)
	fkReferencesRe  = regexp.MustCompile(`(?i)REFERENCES\s+(\w+)\s*\(`)
)

// The app applies init-postgres.sql with a single ExecContext, and Postgres runs a
// multi-statement simple query inside one implicit transaction. A foreign key that
// names a table created further down the file therefore does not degrade: it aborts
// the whole statement and rolls back the entire schema, leaving a fresh install with
// no tables at all. Existing installs never notice, because the referenced table is
// already there, so this only ever breaks brand-new deployments.
func TestInitPostgresHasNoForwardReferences(t *testing.T) {
	lines := strings.Split(PostgresSQL, "\n")

	createdAt := map[string]int{}
	for i, l := range lines {
		if m := fkCreateTableRe.FindStringSubmatch(l); m != nil {
			if _, seen := createdAt[strings.ToLower(m[1])]; !seen {
				createdAt[strings.ToLower(m[1])] = i + 1
			}
		}
	}

	current := "(top of file)"
	var problems []string
	for i, l := range lines {
		if m := fkCreateTableRe.FindStringSubmatch(l); m != nil {
			current = m[1]
		}
		for _, m := range fkReferencesRe.FindAllStringSubmatch(l, -1) {
			target := strings.ToLower(m[1])
			def, ok := createdAt[target]
			if !ok || def <= i+1 {
				continue
			}
			problems = append(problems, fmt.Sprintf(
				"line %d (in %s) references %q, which is not created until line %d",
				i+1, current, target, def))
		}
	}

	if len(problems) > 0 {
		t.Errorf("init-postgres.sql has %d forward reference(s); a fresh install would roll back to an empty schema:\n  %s",
			len(problems), strings.Join(problems, "\n  "))
	}
}

var (
	colTableRe   = regexp.MustCompile(`(?i)^\s*CREATE TABLE IF NOT EXISTS (\w+)\s*\(`)
	colLineRe    = regexp.MustCompile(`^\s*"?(\w+)"?\s+\w`)
	colAlterRe   = regexp.MustCompile(`(?i)^\s*ALTER TABLE (?:IF EXISTS )?(?:ONLY )?(\w+)`)
	colAddRe     = regexp.MustCompile(`(?i)ADD COLUMN (?:IF NOT EXISTS )?"?(\w+)"?`)
	colRenameRe  = regexp.MustCompile(`(?i)RENAME COLUMN \w+ TO "?(\w+)"?`)
	indexRe      = regexp.MustCompile(`(?is)^\s*CREATE (?:UNIQUE )?INDEX (?:CONCURRENTLY )?(?:IF NOT EXISTS )?\w+\s+ON (?:ONLY )?(\w+)\s*(?:USING \w+\s*)?(\(.*)$`)
	identRe      = regexp.MustCompile(`[A-Za-z_]\w*`)
	quotedRe     = regexp.MustCompile(`'[^']*'`)
	castRe       = regexp.MustCompile(`::\s*\w+(\s*\[\])?`)
	funcCallRe   = regexp.MustCompile(`[A-Za-z_]\w*\s*\(`)
	notColumnRe  = regexp.MustCompile(`(?i)^(where|and|or|not|is|null|true|false|in|asc|desc|nulls|first|last|like|between|any|all|some|exists|case|when|then|else|end|collate|[a-z_]+_ops)$`)
	constraintRe = regexp.MustCompile(`(?i)^(constraint|primary|unique|foreign|check|exclude)$`)
)

// A fresh install applies init-postgres.sql top to bottom in one transaction, so an
// index naming a column that a later ALTER TABLE adds fails the whole schema, while
// existing installs, which already have the column, never notice.
func TestInitPostgresIndexesNameDefinedColumns(t *testing.T) {
	lines := strings.Split(PostgresSQL, "\n")
	// columns[table][column] is the line that first defines it.
	columns := map[string]map[string]int{}
	define := func(table, col string, line int) {
		table, col = strings.ToLower(table), strings.ToLower(col)
		if columns[table] == nil {
			columns[table] = map[string]int{}
		}
		if _, ok := columns[table][col]; !ok {
			columns[table][col] = line
		}
	}

	var inTable, inAlter string
	for i, l := range lines {
		code := strings.SplitN(l, "--", 2)[0]
		switch {
		case colTableRe.MatchString(code):
			inTable = colTableRe.FindStringSubmatch(code)[1]
			if columns[strings.ToLower(inTable)] == nil {
				columns[strings.ToLower(inTable)] = map[string]int{}
			}
			continue
		case inTable != "":
			if strings.HasPrefix(strings.TrimSpace(code), ")") {
				inTable = ""
			} else if m := colLineRe.FindStringSubmatch(code); m != nil && !constraintRe.MatchString(m[1]) {
				define(inTable, m[1], i+1)
			}
			continue
		}
		if m := colAlterRe.FindStringSubmatch(code); m != nil {
			inAlter = m[1]
		}
		if inAlter != "" {
			for _, m := range colAddRe.FindAllStringSubmatch(code, -1) {
				define(inAlter, m[1], i+1)
			}
			for _, m := range colRenameRe.FindAllStringSubmatch(code, -1) {
				define(inAlter, m[1], i+1)
			}
			if strings.Contains(code, ";") {
				inAlter = ""
			}
		}
	}

	var problems []string
	for i, l := range lines {
		code := strings.SplitN(l, "--", 2)[0]
		m := indexRe.FindStringSubmatch(code)
		if m == nil {
			continue
		}
		table := strings.ToLower(m[1])
		known, ok := columns[table]
		if !ok {
			continue
		}
		body := funcCallRe.ReplaceAllString(castRe.ReplaceAllString(quotedRe.ReplaceAllString(m[2], ""), ""), "(")
		for _, id := range identRe.FindAllString(body, -1) {
			if notColumnRe.MatchString(id) {
				continue
			}
			def, ok := known[strings.ToLower(id)]
			if !ok || def > i+1 {
				where := "is never defined"
				if ok {
					where = fmt.Sprintf("is not added until line %d", def)
				}
				problems = append(problems, fmt.Sprintf("line %d indexes %s.%s, which %s", i+1, table, id, where))
			}
		}
	}
	if len(problems) > 0 {
		t.Errorf("init-postgres.sql indexes columns before defining them; a fresh install would fail:\n  %s",
			strings.Join(problems, "\n  "))
	}
}

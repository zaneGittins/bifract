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

package setup

import (
	"fmt"
	"io/fs"
	"path"
	"strings"
	"testing"
	"testing/fstest"

	dbpkg "bifract/db"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "plain statements",
			sql:  "SELECT 1; SELECT 2;",
			want: []string{"SELECT 1", " SELECT 2"},
		},
		{
			name: "no trailing semicolon keeps the last statement",
			sql:  "SELECT 1;\nSELECT 2",
			want: []string{"SELECT 1", "\nSELECT 2"},
		},
		{
			name: "trailing whitespace after the last semicolon is not a statement",
			sql:  "SELECT 1;\n\n  \t\n",
			want: []string{"SELECT 1"},
		},
		{
			name: "semicolon in a line comment",
			sql:  "-- indexed automatically; on large tables run it by hand\nALTER TABLE logs ADD INDEX i x TYPE minmax;\nSELECT 2;",
			want: []string{
				"-- indexed automatically; on large tables run it by hand\nALTER TABLE logs ADD INDEX i x TYPE minmax",
				"\nSELECT 2",
			},
		},
		{
			name: "line comment ends at the newline",
			sql:  "SELECT 1 -- note\n; SELECT 2",
			want: []string{"SELECT 1 -- note\n", " SELECT 2"},
		},
		{
			name: "semicolon in a block comment",
			sql:  "CREATE TABLE t (a Int8 /* writes locally; reads cross-shard */) ENGINE = Memory; SELECT 1",
			want: []string{"CREATE TABLE t (a Int8 /* writes locally; reads cross-shard */) ENGINE = Memory", " SELECT 1"},
		},
		{
			name: "multi-line block comment",
			sql:  "/* a;\nb;\n*/ SELECT 1; SELECT 2",
			want: []string{"/* a;\nb;\n*/ SELECT 1", " SELECT 2"},
		},
		{
			name: "semicolon in a string literal",
			sql:  "SELECT 'a;b'; SELECT 2",
			want: []string{"SELECT 'a;b'", " SELECT 2"},
		},
		{
			name: "backslash-escaped quote does not end the literal",
			sql:  `SELECT 'it\'s; fine'; SELECT 2`,
			want: []string{`SELECT 'it\'s; fine'`, " SELECT 2"},
		},
		{
			name: "doubled quote does not end the literal",
			sql:  "SELECT 'it''s; fine'; SELECT 2",
			want: []string{"SELECT 'it''s; fine'", " SELECT 2"},
		},
		{
			name: "escaped backslash before the closing quote",
			sql:  `SELECT 'C:\\'; SELECT 2`,
			want: []string{`SELECT 'C:\\'`, " SELECT 2"},
		},
		{
			// A regex from 011_process_edges.sql: backslashes inside a literal.
			name: "regex literal with backslashes",
			sql:  `SELECT match(q, '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'); SELECT 2`,
			want: []string{`SELECT match(q, '^([0-9]{1,3}\\.){3}[0-9]{1,3}$')`, " SELECT 2"},
		},
		{
			name: "comment markers inside a literal are not comments",
			sql:  "SELECT '--', '/*'; SELECT 2",
			want: []string{"SELECT '--', '/*'", " SELECT 2"},
		},
		{
			name: "a quote inside a comment does not open a literal",
			sql:  "-- don't stop here\nSELECT 1; SELECT 2",
			want: []string{"-- don't stop here\nSELECT 1", " SELECT 2"},
		},
		{
			name: "empty input",
			sql:  "",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.sql)
			if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", tt.want) {
				t.Errorf("splitStatements(%q)\n got  %q\n want %q", tt.sql, got, tt.want)
			}
		})
	}
}

// sqlBody strips comments from a statement, leaving what the server executes.
func sqlBody(stmt string) string {
	var b strings.Builder
	for _, line := range strings.Split(stmt, "\n") {
		if i := strings.Index(line, "--"); i >= 0 && !strings.Contains(line[:i], "'") {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return strings.TrimSpace(b.String())
}

// bifract-setup sends each ClickHouse migration statement on its own through
// clickhouse-client. Every segment the splitter produces from a shipped file must
// therefore be a complete statement: balanced parentheses and quotes, and some SQL
// besides comments (clickhouse-client rejects a comment-only query as empty, which
// would fail the upgrade on that migration).
func TestClickHouseMigrationsSplitIntoCompleteStatements(t *testing.T) {
	migrations, err := LoadMigrations(dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) == 0 {
		t.Fatal("no ClickHouse migrations loaded")
	}
	for _, m := range migrations {
		stmts := splitStatements(m.SQL)
		if len(stmts) == 0 {
			t.Errorf("%s: produced no statements", m.Name)
		}
		for i, s := range stmts {
			body := sqlBody(s)
			if body == "" {
				t.Errorf("%s: statement %d is only comments: %q", m.Name, i+1, s)
				continue
			}
			if open, closed := strings.Count(body, "("), strings.Count(body, ")"); open != closed {
				t.Errorf("%s: statement %d has %d '(' and %d ')', so it was cut mid-statement:\n%s", m.Name, i+1, open, closed, body)
			}
			if n := strings.Count(strings.ReplaceAll(body, `\'`, ""), "'"); n%2 != 0 {
				t.Errorf("%s: statement %d has an unterminated string literal:\n%s", m.Name, i+1, body)
			}
		}
	}
}

// Rejoining the pieces must reproduce the file: the splitter only removes the
// separating semicolons, never content.
func TestSplitStatementsLosesNothing(t *testing.T) {
	migrations, err := LoadMigrations(dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		joined := strings.Join(splitStatements(m.SQL), ";")
		want := strings.TrimRight(m.SQL, " \t\r\n")
		want = strings.TrimSuffix(want, ";")
		if strings.TrimRight(joined, " \t\r\n;") != strings.TrimRight(want, " \t\r\n") {
			t.Errorf("%s: rejoined statements differ from the file", m.Name)
		}
	}
}

func TestParseMaxNumber(t *testing.T) {
	tests := []struct {
		name string
		out  string
		want int
	}{
		{"clickhouse-client bare value", "17\n", 17},
		{"psql table output", " coalesce \n----------\n       84\n(1 row)\n\n", 84},
		{"psql max header", " max \n-----\n   3\n(1 row)\n", 3},
		{"empty ledger", "0\n", 0},
		{"no output", "", 0},
		{"error text only", "Code: 60. DB::Exception: Table logs._bifract_migrations does not exist.", 0},
		{"notice before the value", "NOTICE:  relation exists, skipping\n12\n", 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseMaxNumber(tt.out); got != tt.want {
				t.Errorf("parseMaxNumber(%q) = %d, want %d", tt.out, got, tt.want)
			}
		})
	}
}

func TestLoadMigrationsOrderAndNaming(t *testing.T) {
	fsys := fstest.MapFS{
		"m/010_ten.sql":        {Data: []byte("SELECT 10")},
		"m/002_two.sql":        {Data: []byte("SELECT 2")},
		"m/1_one.sql":          {Data: []byte("SELECT 1")},
		"m/README.md":          {Data: []byte("not sql")},
		"m/notes.sql":          {Data: []byte("no number prefix")},
		"m/abc_letters.sql":    {Data: []byte("non-numeric prefix")},
		"m/sub/003_nested.sql": {Data: []byte("SELECT 3")},
	}
	got, err := LoadMigrations(fsys, "m")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range got {
		names = append(names, fmt.Sprintf("%d:%s", m.Number, m.Name))
	}
	want := []string{"1:1_one", "2:002_two", "3:003_nested", "10:010_ten"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Errorf("LoadMigrations = %v, want %v", names, want)
	}
	if got[0].SQL != "SELECT 1" {
		t.Errorf("SQL not loaded: %q", got[0].SQL)
	}

	if _, err := LoadMigrations(fsys, "missing"); err == nil {
		t.Error("a missing directory loaded without error")
	}
}

// The runners track progress as max(number) in _bifract_migrations, so two files
// sharing a number, or a file the loader cannot parse, are migrations that
// silently never run on some installs.
func TestShippedMigrationsAreUniquelyNumbered(t *testing.T) {
	sets := []struct {
		name string
		fsys fs.FS
		dir  string
	}{
		{"postgres", postgresMigrations, "migrations/postgres"},
		{"clickhouse", dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir},
	}
	for _, set := range sets {
		t.Run(set.name, func(t *testing.T) {
			files, err := fs.Glob(set.fsys, path.Join(set.dir, "*.sql"))
			if err != nil {
				t.Fatal(err)
			}
			migrations, err := LoadMigrations(set.fsys, set.dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(migrations) != len(files) {
				t.Errorf("loaded %d of %d .sql files; a file not named NNN_name.sql is never applied", len(migrations), len(files))
			}
			seen := map[int]string{}
			for i, m := range migrations {
				if prev, ok := seen[m.Number]; ok {
					t.Errorf("%s and %s share number %d", prev, m.Name, m.Number)
				}
				seen[m.Number] = m.Name
				if i > 0 && m.Number < migrations[i-1].Number {
					t.Errorf("not sorted: %s after %s", m.Name, migrations[i-1].Name)
				}
				if strings.TrimSpace(m.SQL) == "" {
					t.Errorf("%s is empty", m.Name)
				}
			}
		})
	}
}

// The raw_log stamp boundary has to name a migration that exists, or the stamp
// would mark a level no file reaches.
func TestRawLogSplitMigrationExists(t *testing.T) {
	migrations, err := LoadMigrations(dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Number == dbpkg.RawLogSplitMigration {
			if !strings.Contains(m.SQL, "DROP COLUMN IF EXISTS raw_log") {
				t.Errorf("migration %s is the raw_log split marker but does not drop raw_log", m.Name)
			}
			return
		}
	}
	t.Fatalf("no ClickHouse migration numbered %d", dbpkg.RawLogSplitMigration)
}

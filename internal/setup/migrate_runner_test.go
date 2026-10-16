package setup

import (
	"fmt"
	"strings"
	"testing"

	dbpkg "bifract/db"
)

func pgOut(n int) []byte {
	return []byte(fmt.Sprintf(" coalesce \n----------\n %8d\n(1 row)\n\n", n))
}

func TestRunPostgresMigrationsAppliesOnlyNewOnes(t *testing.T) {
	all, err := LoadMigrations(postgresMigrations, "migrations/postgres")
	if err != nil || len(all) < 3 {
		t.Fatalf("need at least three postgres migrations: %v", err)
	}
	pending := all[len(all)-2:]
	applied := all[len(all)-3].Number

	fake := newFakeExec(t, fakeRule{Match: []string{"SELECT COALESCE(MAX(number), 0)"}, Stdout: pgOut(applied)})
	n, err := RunPostgresMigrations(&DockerOps{Dir: t.TempDir()}, "bifract", "bifractdb")
	if err != nil {
		t.Fatalf("RunPostgresMigrations: %v", err)
	}
	if n != len(pending) {
		t.Errorf("applied %d, want %d", n, len(pending))
	}

	var want []string
	want = append(want, createMigrationsTablePG, "SELECT COALESCE(MAX(number), 0) FROM _bifract_migrations;")
	for _, m := range pending {
		// Postgres runs a file whole: psql -c sends it as one implicit transaction,
		// so a failing statement leaves none of the file applied.
		want = append(want, m.SQL,
			fmt.Sprintf("INSERT INTO _bifract_migrations (number, name) VALUES (%d, '%s');", m.Number, m.Name))
	}
	if got := fake.sql(); fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
		t.Errorf("statements sent:\n%q\nwant:\n%q", got, want)
	}
	for _, c := range fake.calls() {
		if !strings.HasPrefix(c.joined(), "compose exec -T postgres psql -U bifract -d bifractdb -c ") {
			t.Errorf("unexpected invocation: docker %s", c.joined())
		}
	}
}

func TestRunPostgresMigrationsStopsAtFirstFailure(t *testing.T) {
	all, err := LoadMigrations(postgresMigrations, "migrations/postgres")
	if err != nil || len(all) < 3 {
		t.Fatalf("need at least three postgres migrations: %v", err)
	}
	first, broken := all[len(all)-2], all[len(all)-1]

	fake := newFakeExec(t,
		fakeRule{Match: []string{"SELECT COALESCE(MAX(number), 0)"}, Stdout: pgOut(all[len(all)-3].Number)},
		fakeRule{Match: []string{broken.SQL}, Stdout: []byte("ERROR:  boom"), Exit: 1},
	)
	n, err := RunPostgresMigrations(&DockerOps{Dir: t.TempDir()}, "bifract", "bifract")
	if err == nil || !strings.Contains(err.Error(), broken.Name) {
		t.Fatalf("err = %v, want a failure naming %s", err, broken.Name)
	}
	if n != 1 {
		t.Errorf("applied = %d, want 1 (the migration before the failure)", n)
	}
	for _, s := range fake.sql() {
		if strings.Contains(s, fmt.Sprintf("VALUES (%d,", broken.Number)) {
			t.Errorf("recorded the failed migration %s as applied", broken.Name)
		}
	}
	if !strings.Contains(strings.Join(fake.sql(), "\n"), fmt.Sprintf("VALUES (%d, '%s')", first.Number, first.Name)) {
		t.Errorf("did not record %s, which succeeded", first.Name)
	}
}

func TestRunPostgresMigrationsUpToDate(t *testing.T) {
	all, err := LoadMigrations(postgresMigrations, "migrations/postgres")
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeExec(t, fakeRule{Match: []string{"SELECT COALESCE"}, Stdout: pgOut(all[len(all)-1].Number)})
	n, err := RunPostgresMigrations(&DockerOps{Dir: t.TempDir()}, "bifract", "bifract")
	if err != nil || n != 0 {
		t.Fatalf("RunPostgresMigrations = %d, %v; want 0, nil", n, err)
	}
	if got := len(fake.calls()); got != 2 {
		t.Errorf("made %d calls on an up-to-date database, want 2 (create ledger, read it)", got)
	}
}

func TestRunPostgresMigrationsLedgerFailure(t *testing.T) {
	newFakeExec(t, fakeRule{Match: []string{"CREATE TABLE IF NOT EXISTS _bifract_migrations"}, Exit: 1})
	if _, err := RunPostgresMigrations(&DockerOps{Dir: t.TempDir()}, "bifract", "bifract"); err == nil ||
		!strings.Contains(err.Error(), "create migrations table") {
		t.Fatalf("err = %v, want the ledger creation failure", err)
	}
}

// chMigrationRules answers the ClickHouse runner's state queries.
func chMigrationRules(maxApplied int, logsTable, rawLog bool) []fakeRule {
	b := func(v bool) []byte {
		if v {
			return []byte("1\n")
		}
		return []byte("0\n")
	}
	return []fakeRule{
		{Match: []string{"SELECT max(number) FROM logs._bifract_migrations"}, Stdout: []byte(fmt.Sprintf("%d\n", maxApplied))},
		{Match: []string{"FROM system.tables"}, Stdout: b(logsTable)},
		{Match: []string{"FROM system.columns"}, Stdout: b(rawLog)},
	}
}

func chRecorded(sql []string) []int {
	var out []int
	for _, s := range sql {
		var n int
		if _, err := fmt.Sscanf(s, "INSERT INTO logs._bifract_migrations (number, name) VALUES (%d,", &n); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func TestRunClickHouseMigrations(t *testing.T) {
	all, err := LoadMigrations(dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	last := all[len(all)-1].Number
	split := dbpkg.RawLogSplitMigration
	if last <= split+1 {
		t.Skip("needs migrations past the raw_log split")
	}
	// numbers lists the shipped migration numbers in (from, to].
	numbers := func(from, to int) []int {
		var out []int
		for _, m := range all {
			if m.Number > from && m.Number <= to {
				out = append(out, m.Number)
			}
		}
		return out
	}

	tests := []struct {
		name        string
		maxApplied  int
		logsTable   bool
		rawLog      bool
		wantApplied int
		// wantRecorded is every ledger row written, stamps and applied alike.
		wantRecorded []int
		// wantRan is the migrations whose statements were executed.
		wantRan []int
		// wantProbe is whether the live schema was inspected for a stamp.
		wantProbe bool
	}{
		{
			// Provisioned from init-clickhouse.sql with no ledger: replaying 004 or
			// 012 against it fails, because they project raw_log, which is gone.
			name:         "provisioned schema is stamped through the raw_log split, not replayed",
			maxApplied:   1,
			logsTable:    true,
			rawLog:       false,
			wantApplied:  len(numbers(split, last)),
			wantRecorded: append(numbers(1, split), numbers(split, last)...),
			wantRan:      numbers(split, last),
			wantProbe:    true,
		},
		{
			name:         "a schema still carrying raw_log replays history",
			maxApplied:   split - 1,
			logsTable:    true,
			rawLog:       true,
			wantApplied:  len(numbers(split-1, last)),
			wantRecorded: numbers(split-1, last),
			wantRan:      numbers(split-1, last),
			wantProbe:    true,
		},
		{
			name:         "no logs table means nothing to stamp",
			maxApplied:   split - 1,
			logsTable:    false,
			wantApplied:  len(numbers(split-1, last)),
			wantRecorded: numbers(split-1, last),
			wantRan:      numbers(split-1, last),
			wantProbe:    true,
		},
		{
			name:         "a ledger past the split skips the schema probe",
			maxApplied:   last - 1,
			logsTable:    true,
			wantApplied:  1,
			wantRecorded: []int{last},
			wantRan:      []int{last},
			wantProbe:    false,
		},
		{
			name:         "up to date",
			maxApplied:   last,
			wantApplied:  0,
			wantRecorded: nil,
			wantRan:      nil,
			wantProbe:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeExec(t, chMigrationRules(tt.maxApplied, tt.logsTable, tt.rawLog)...)
			n, err := RunClickHouseMigrations(&DockerOps{Dir: t.TempDir()}, "default", "pw")
			if err != nil {
				t.Fatalf("RunClickHouseMigrations: %v", err)
			}
			if n != tt.wantApplied {
				t.Errorf("applied = %d, want %d", n, tt.wantApplied)
			}
			sql := fake.sql()
			if got := chRecorded(sql); fmt.Sprint(got) != fmt.Sprint(tt.wantRecorded) {
				t.Errorf("ledger rows = %v, want %v", got, tt.wantRecorded)
			}

			probed := false
			for _, s := range sql {
				if strings.Contains(s, "system.tables") {
					probed = true
				}
			}
			if probed != tt.wantProbe {
				t.Errorf("schema probed = %v, want %v", probed, tt.wantProbe)
			}

			// Every statement of each migration that should run, one call each, in
			// file order; nothing from a migration that should not.
			var wantStmts []string
			ran := map[int]bool{}
			for _, n := range tt.wantRan {
				ran[n] = true
			}
			for _, m := range all {
				if !ran[m.Number] {
					continue
				}
				for _, s := range splitStatements(m.SQL) {
					if s = strings.TrimSpace(s); s != "" {
						wantStmts = append(wantStmts, s)
					}
				}
			}
			var gotStmts []string
			for _, s := range sql {
				if strings.Contains(s, "_bifract_migrations") || strings.Contains(s, "FROM system.") {
					continue
				}
				gotStmts = append(gotStmts, s)
			}
			if fmt.Sprintf("%q", gotStmts) != fmt.Sprintf("%q", wantStmts) {
				t.Errorf("executed %d statements, want %d in file order", len(gotStmts), len(wantStmts))
			}

			// Unqualified table names in the migrations resolve against --database logs.
			for _, c := range fake.calls() {
				if !strings.Contains(c.joined(), "clickhouse-client --user default --password pw --database logs") {
					t.Errorf("call without the logs database: %s", c.joined())
				}
			}
		})
	}
}

func TestRunClickHouseMigrationsFailureIsNotRecorded(t *testing.T) {
	all, err := LoadMigrations(dbpkg.ClickHouseMigrations, dbpkg.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	broken := all[len(all)-1]
	stmt := strings.TrimSpace(splitStatements(broken.SQL)[0])
	rules := append([]fakeRule{{Match: []string{stmt}, Stdout: []byte("Code: 62. Syntax error"), Exit: 1}},
		chMigrationRules(all[len(all)-3].Number, true, false)...)
	fake := newFakeExec(t, rules...)

	n, err := RunClickHouseMigrations(&DockerOps{Dir: t.TempDir()}, "default", "pw")
	if err == nil || !strings.Contains(err.Error(), broken.Name) {
		t.Fatalf("err = %v, want a failure naming %s", err, broken.Name)
	}
	if n != 1 {
		t.Errorf("applied = %d, want 1", n)
	}
	for _, rec := range chRecorded(fake.sql()) {
		if rec == broken.Number {
			t.Errorf("recorded the failed migration %s", broken.Name)
		}
	}
}

func TestSetMigrationBaseline(t *testing.T) {
	t.Run("bundled ClickHouse gets a baseline too", func(t *testing.T) {
		fake := newFakeExec(t)
		if err := SetMigrationBaseline(&DockerOps{Dir: t.TempDir()}, "bifract", "bifract", "default", "pw", true); err != nil {
			t.Fatal(err)
		}
		want := []string{
			createMigrationsTablePG,
			"INSERT INTO _bifract_migrations (number, name) VALUES (1, '001_initial') ON CONFLICT DO NOTHING;",
			createMigrationsTableCH,
			"INSERT INTO logs._bifract_migrations (number, name) VALUES (1, '001_initial');",
		}
		if got := fake.sql(); fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
			t.Errorf("statements:\n%q\nwant:\n%q", got, want)
		}
	})

	t.Run("external ClickHouse is left to the app", func(t *testing.T) {
		fake := newFakeExec(t)
		if err := SetMigrationBaseline(&DockerOps{Dir: t.TempDir()}, "bifract", "bifract", "default", "pw", false); err != nil {
			t.Fatal(err)
		}
		for _, c := range fake.calls() {
			if strings.Contains(c.joined(), "clickhouse") {
				t.Errorf("exec'd into a ClickHouse container that does not exist: %s", c.joined())
			}
		}
		if got := len(fake.calls()); got != 2 {
			t.Errorf("made %d calls, want the 2 Postgres ones", got)
		}
	})

	t.Run("a failure is reported", func(t *testing.T) {
		newFakeExec(t, fakeRule{Match: []string{"INSERT INTO logs._bifract_migrations"}, Exit: 1})
		err := SetMigrationBaseline(&DockerOps{Dir: t.TempDir()}, "bifract", "bifract", "default", "pw", true)
		if err == nil || !strings.Contains(err.Error(), "ch baseline") {
			t.Errorf("err = %v, want the ClickHouse baseline failure", err)
		}
	})
}

//go:build pglive

// Upgrade check for init-postgres.sql. It needs a Postgres user that can create databases:
//
//	BIFRACT_PG_HOST=localhost BIFRACT_PG_USER=postgres BIFRACT_PG_PASSWORD=... \
//	  go test -tags pglive ./db -run Upgrade -v
//
// BIFRACT_PG_UPGRADE_FROM names the release to upgrade from (default v0.0.9).
package db

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

// Every boot applies init-postgres.sql over the existing schema, so an install upgraded from
// an older release must end with exactly the schema a fresh install gets. An edit to a
// CREATE TABLE IF NOT EXISTS without a matching ALTER reaches fresh installs only.
func TestInitPostgresUpgradeMatchesFresh(t *testing.T) {
	from := os.Getenv("BIFRACT_PG_UPGRADE_FROM")
	if from == "" {
		from = "v0.0.9"
	}
	old, err := exec.Command("git", "show", from+":db/init-postgres.sql").Output()
	if err != nil {
		t.Skipf("cannot read init-postgres.sql at %s: %v", from, err)
	}
	admin := livePGDB(t, "postgres")

	fresh := scratchDB(t, admin, "fresh")
	apply(t, fresh, PostgresSQL)
	upgraded := scratchDB(t, admin, "upgraded")
	apply(t, upgraded, string(old))
	apply(t, upgraded, PostgresSQL)
	// A second boot on the upgraded schema must also leave it unchanged.
	apply(t, upgraded, PostgresSQL)

	want, got := schemaOf(t, fresh), schemaOf(t, upgraded)
	var problems []string
	for k, v := range want {
		if g, ok := got[k]; !ok {
			problems = append(problems, "missing after upgrade: "+k+" "+v)
		} else if g != v {
			problems = append(problems, fmt.Sprintf("differs after upgrade: %s\n      fresh:    %s\n      upgraded: %s", k, v, g))
		}
	}
	for k, v := range got {
		if _, ok := want[k]; !ok {
			problems = append(problems, "only after upgrade: "+k+" "+v)
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Errorf("upgrading from %s does not reach the fresh schema:\n  %s", from, strings.Join(problems, "\n  "))
	}
}

func livePGDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	host := os.Getenv("BIFRACT_PG_HOST")
	if host == "" {
		t.Skip("BIFRACT_PG_HOST not set")
	}
	port := os.Getenv("BIFRACT_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("BIFRACT_PG_USER")
	if user == "" {
		user = "postgres"
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, os.Getenv("BIFRACT_PG_PASSWORD")),
		Host: host + ":" + port, Path: "/" + name, RawQuery: "sslmode=disable"}
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func scratchDB(t *testing.T, admin *sql.DB, suffix string) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("bifract_upgrade_%d_%s", os.Getpid(), suffix)
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	db := livePGDB(t, name)
	t.Cleanup(func() {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
	})
	return db
}

// apply runs the schema as the server does, in one statement.
func apply(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
}

// schemaOf describes every column, constraint and index in the public schema.
func schemaOf(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, q := range []string{
		`SELECT 'column ' || table_name || '.' || column_name,
		        data_type || ' null=' || is_nullable || ' default=' || coalesce(column_default, '')
		   FROM information_schema.columns WHERE table_schema = 'public'`,
		`SELECT 'constraint ' || conrelid::regclass || '.' || conname, pg_get_constraintdef(oid)
		   FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid != 0`,
		`SELECT 'index ' || tablename || '.' || indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'`,
	} {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			out[k] = v
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	return out
}

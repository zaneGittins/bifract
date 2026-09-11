package storage

import (
	"strings"
	"testing"
)

// The schema identity exists to bound what generated DDL can do, so the privileges it
// does NOT hold are the point. Asserted on the statements rather than against a live
// server so a widening is caught at review time.
func TestSchemaIdentityGrantsAreScoped(t *testing.T) {
	stmts := strings.Join(schemaIdentityGrants("", "`logs`", SchemaCHUser), "\n")

	// Every grant is confined to the logs database.
	for _, line := range strings.Split(stmts, "\n") {
		if line == "" {
			continue
		}
		if !strings.Contains(line, "ON `logs`.*") {
			t.Errorf("grant is not scoped to the logs database:\n%s", line)
		}
	}

	// A grant that would let generated DDL read local files, reach the network, or
	// touch access control is the whole thing this identity is for.
	for _, forbidden := range []string{
		"FILE", "URL", "S3", "AZURE", "HDFS", "MYSQL", "POSTGRES", "SOURCES",
		"INTROSPECTION", "ACCESS MANAGEMENT", "CREATE USER", "CREATE DATABASE",
		"DROP DATABASE", "SYSTEM SHUTDOWN", "ALL ON",
	} {
		if strings.Contains(stmts, forbidden) {
			t.Errorf("schema identity must not be granted %s:\n%s", forbidden, stmts)
		}
	}

	// It still has to be able to do its job.
	for _, needed := range []string{"SELECT", "INSERT", "CREATE TABLE", "CREATE DICTIONARY", "DROP TABLE", "ALTER"} {
		if !strings.Contains(stmts, needed) {
			t.Errorf("schema identity needs %s to build dictionary and model objects:\n%s", needed, stmts)
		}
	}
}

// Only recall reads archives, so only recall may reach an object store. Granting the
// search class S3/AZURE/URL would hand an injected search an exfiltration channel.
func TestOnlyRecallReachesObjectStores(t *testing.T) {
	privs := func(user string) string {
		var b strings.Builder
		for _, o := range queryIdentityOptionalGrants(user) {
			b.WriteString(o.priv + " ")
		}
		return b.String()
	}
	search, recall := privs(SearchCHUser), privs(RecallCHUser)

	for _, store := range []string{"S3", "AZURE"} {
		if strings.Contains(search, store) {
			t.Errorf("the search class must not be granted %s: %s", store, search)
		}
		if !strings.Contains(recall, store) {
			t.Errorf("recall needs %s to read archives: %s", store, recall)
		}
	}
	// No archive backend addresses data by URL (see objstore.Backend), so neither
	// class has a use for it and both would gain an outbound channel.
	for name, p := range map[string]string{"search": search, "recall": recall} {
		if strings.Contains(p, "URL") {
			t.Errorf("%s must not be granted URL: %s", name, p)
		}
	}
}

// ClickHouse coalesces grants, so the same privilege shows up alone on one line and
// comma-listed on another. A substring check misses the second form and leaves the
// broad grant in place, which is exactly what this narrowing exists to remove.
func TestStaleInstanceWideGrantsParsesPrivilegeLists(t *testing.T) {
	cases := []struct {
		name string
		show string
		want []string
	}{
		{"alone", "GRANT SELECT ON *.* TO u\n", []string{"SELECT"}},
		{"comma-listed", "GRANT SELECT, CREATE TEMPORARY TABLE ON *.* TO u\n", []string{"SELECT"}},
		{"several stale in one list", "GRANT SELECT, dictGet, URL ON *.* TO u\n", []string{"SELECT", "dictGet", "URL"}},
		{"across lines", "GRANT URL ON *.* TO u\nGRANT SELECT, REMOTE ON *.* TO u\n", []string{"URL", "SELECT"}},
		// The grants this release actually makes must never be revoked.
		{"scoped grants are kept", "GRANT SELECT, dictGet ON `logs`.* TO u\nGRANT SELECT ON system.* TO u\n", nil},
		{"wanted instance-wide grants are kept", "GRANT REMOTE, CREATE TEMPORARY TABLE, S3, AZURE ON *.* TO u\n", nil},
		{"empty", "", nil},
	}
	for _, tc := range cases {
		got := staleInstanceWideGrants(tc.show)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				break
			}
		}
	}
}

package dictionaries

import (
	"strings"
	"testing"
)

// The ClickHouse dictionary source maps a query's columns onto the declared
// structure by position. The declaration is the key column followed by every other
// column in order, so the source query has to select them the same way: any other
// order loads values into the wrong attributes and every lookup silently misses.
func TestDictSourceQueryMatchesDeclarationOrder(t *testing.T) {
	m := &Manager{chDB: "logs"}
	dict := &Dictionary{ID: "abc", CHTableName: "dict_abc", KeyColumn: "name"}
	cols := []DictionaryColumn{{Name: "name"}, {Name: "original_file_name"}, {Name: "publisher"}}

	for _, tc := range []struct {
		key  string
		want string
	}{
		{"name", "SELECT `name`, `original_file_name`, `publisher` FROM"},
		{"original_file_name", "SELECT `original_file_name`, `name`, `publisher` FROM"},
		{"publisher", "SELECT `publisher`, `name`, `original_file_name` FROM"},
	} {
		got := m.dictSourceQuery(dict, tc.key, cols)
		if !strings.HasPrefix(got, tc.want) {
			t.Errorf("key %q: got %q, want prefix %q", tc.key, got, tc.want)
		}
		if !strings.Contains(got, "FINAL WHERE notEmpty(`"+tc.key+"`)") {
			t.Errorf("key %q: source must dedupe with FINAL and drop blank keys: %q", tc.key, got)
		}
	}
}

// On a cluster the source must read the Distributed companion, not the local table:
// the local table holds only the shard's own rows, so every other shard's copy of the
// dictionary would load empty and enrich to the empty string with no error.
func TestDictSourceQueryUsesDistributedOnCluster(t *testing.T) {
	dict := &Dictionary{ID: "abc", CHTableName: "dict_abc", KeyColumn: "name"}
	cols := []DictionaryColumn{{Name: "name"}, {Name: "publisher"}}

	single := (&Manager{chDB: "logs"}).dictSourceQuery(dict, "name", cols)
	if !strings.Contains(single, "`logs`.`dict_abc` FINAL") {
		t.Errorf("single node must read the local table: %q", single)
	}

	cluster := (&Manager{chDB: "logs", distributed: true, ddlCluster: "c"}).dictSourceQuery(dict, "name", cols)
	if !strings.Contains(cluster, "`logs`.`dict_abc_distributed` FINAL") {
		t.Errorf("cluster must read the distributed companion: %q", cluster)
	}
}

// A Distributed insert is forwarded in the background by default, but the editor reads
// the row back immediately after writing it.
func TestClusterInsertIsForeground(t *testing.T) {
	if got := (&Manager{}).insertSettings(); got != "" {
		t.Errorf("single node needs no insert settings, got %q", got)
	}
	if got := (&Manager{distributed: true}).insertSettings(); !strings.Contains(got, "distributed_foreground_insert = 1") {
		t.Errorf("cluster insert must be synchronous, got %q", got)
	}
}

// A global dictionary is owned by one fractal but visible everywhere, so by-name
// resolution has to consider it. Before this, match() could read a global (its
// query has always carried the is_global clause) while any by-name caller failed
// with a bare "no rows in result set" from every fractal but the owner.
func TestGetDictionaryByNameQueryIncludesGlobals(t *testing.T) {
	// The by-name lookup and the mapping lookup must agree on visibility, so both
	// are asserted against the same rule rather than one being fixed in isolation.
	for _, src := range []string{dictByNameFractalSQL, dictByNamePrismSQL} {
		if !strings.Contains(src, "is_global = true") {
			t.Errorf("by-name lookup must consider global dictionaries:\n%s", src)
		}
	}
}

// Two properties of these statements are invisible until Postgres parses them, and
// both shipped broken once. Asserting them on the SQL text catches the regression
// without needing a live database.
func TestGetDictionaryByNameQueryIsTypeSafeAndNullSafe(t *testing.T) {
	for name, src := range map[string]string{"fractal": dictByNameFractalSQL, "prism": dictByNamePrismSQL} {
		col := "fractal_id"
		if name == "prism" {
			col = "prism_id"
		}

		// The scope id must be compared as text everywhere. Using $1 as a uuid in the
		// WHERE and as text in the ORDER BY leaves the parameter's type ambiguous, and
		// Postgres rejects the statement outright: "operator does not exist: uuid = text".
		if strings.Contains(src, col+" = $1") {
			t.Errorf("%s: %s must be compared as ::text so $1 has one type:\n%s", name, col, src)
		}
		if !strings.Contains(src, col+"::text = $1") {
			t.Errorf("%s: expected a ::text comparison on %s:\n%s", name, col, src)
		}

		// A global dictionary has a NULL scope id, so the sort key is NULL and DESC
		// puts NULLS FIRST, which would sort the global ahead of the scope's own
		// dictionary and invert the shadowing the comment promises.
		if !strings.Contains(src, "COALESCE("+col+"::text = $1, false) DESC") {
			t.Errorf("%s: ordering must be NULL-safe or a global shadows the local dictionary:\n%s", name, src)
		}
	}
}

// A global dictionary is owned by one scope and readable by all. Resolving a name
// to someone else's global is right for a reader and destructive for a writer:
// ExecuteDictionaryAction rebuilds the schema and TRUNCATEs before refilling, so a
// dictionary action in fractal B would wipe fractal A's global and republish B's
// rows to every reader of it.
func TestOwnedLookupExcludesGlobalsWhileReadLookupIncludesThem(t *testing.T) {
	for _, src := range []string{dictByNameFractalSQL, dictByNamePrismSQL} {
		if !strings.Contains(src, "is_global = true") {
			t.Errorf("the read lookup must see globals:\n%s", src)
		}
	}
	for _, src := range []string{dictOwnedByNameFractalSQL, dictOwnedByNamePrismSQL} {
		// is_global appears in the shared column list; what must be absent is the
		// predicate that widens the WHERE to globals owned elsewhere.
		if strings.Contains(src, "is_global = true") {
			t.Errorf("the write lookup must NOT fall back to a global owned elsewhere:\n%s", src)
		}
	}
	// Both variants keep the parameter unambiguously text; mixing uuid and text use
	// of $1 makes Postgres reject the statement at Parse time.
	for _, src := range []string{dictOwnedByNameFractalSQL, dictOwnedByNamePrismSQL} {
		if strings.Contains(src, "fractal_id = $1") || strings.Contains(src, "prism_id = $1") {
			t.Errorf("scope id must be compared as ::text:\n%s", src)
		}
	}
}

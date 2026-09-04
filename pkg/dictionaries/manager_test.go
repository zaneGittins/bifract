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

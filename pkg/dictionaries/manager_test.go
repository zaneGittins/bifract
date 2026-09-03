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

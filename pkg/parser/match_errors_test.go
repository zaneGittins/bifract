package parser

import (
	"strings"
	"testing"
	"time"
)

func matchErr(t *testing.T, query string, opts QueryOptions) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err = TranslateToSQL(pipeline, opts); err == nil {
		t.Fatal("expected an error")
	}
	return err.Error()
}

// One message for every unresolved dictionary pointed authors at the Context tab's key
// toggle even when the context had no dictionaries at all, which is what an alert test
// run looked like before it resolved them.
func TestMatchUnresolvedDictionaryErrors(t *testing.T) {
	base := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   100,
	}
	q := `* | match(dict="sensitive_groups", field=target_user, column=group_name, include=[tier])`

	t.Run("no dictionaries in this context", func(t *testing.T) {
		if got := matchErr(t, q, base); !strings.Contains(got, "no dictionaries") {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("dictionary not in scope", func(t *testing.T) {
		opts := base
		opts.Dictionaries = map[string]map[string]string{"other": {"k": "lookup_x"}}
		if got := matchErr(t, q, opts); !strings.Contains(got, "not found in this fractal or prism") {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("column is not a key", func(t *testing.T) {
		opts := base
		opts.Dictionaries = map[string]map[string]string{"sensitive_groups": {"other_col": "lookup_x"}}
		got := matchErr(t, q, opts)
		if !strings.Contains(got, "no key column") || !strings.Contains(got, "Context tab") {
			t.Fatalf("got %q", got)
		}
	})
}

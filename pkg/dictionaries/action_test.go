package dictionaries

import (
	"context"
	"strings"
	"testing"
)

// An action that cannot say where its rows go fails before anything is
// resolved, and a trigger with nothing to write writes nothing: the Manager here
// has no database, so any of these reaching one would panic.
func TestExecuteDictionaryActionShortCircuits(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()
	logs := []map[string]interface{}{{"ioc": "1.2.3.4"}}

	if _, err := m.ExecuteDictionaryAction(ctx, &DictionaryAction{Name: "a"}, "f-1", "", logs); err == nil ||
		!strings.Contains(err.Error(), "no target dictionary") {
		t.Errorf("no dictionary name: err = %v", err)
	}
	if _, err := m.ExecuteDictionaryAction(ctx, &DictionaryAction{Name: "a", DictionaryName: "d"}, "", "", logs); err == nil ||
		!strings.Contains(err.Error(), "required") {
		t.Errorf("no scope: err = %v", err)
	}

	action := &DictionaryAction{Name: "a", DictionaryName: "d"}
	for name, empty := range map[string][]map[string]interface{}{
		"no logs":           nil,
		"logs with no data": {{}, {"fields": map[string]interface{}{}}},
	} {
		n, err := m.ExecuteDictionaryAction(ctx, action, "f-1", "", empty)
		if n != 0 || err != nil {
			t.Errorf("%s: wrote %d, err %v; want nothing written and no error", name, n, err)
		}
	}
}

// A dictionary action exists in exactly one scope.
func TestCreateDictionaryActionNeedsExactlyOneScope(t *testing.T) {
	m := &Manager{}
	for _, scope := range [][2]string{{"", ""}, {"f-1", "p-1"}} {
		if _, err := m.CreateDictionaryAction(context.Background(), "a", "", "d", 0, true, "u", scope[0], scope[1]); err == nil {
			t.Errorf("fractal %q prism %q: accepted", scope[0], scope[1])
		}
	}
}

// Columns accumulate across logs: every field of a later log that an earlier
// one lacked is appended after the ones already seen, and the nested fields map
// contributes its keys rather than becoming a column named "fields".
func TestCollectLogColumns(t *testing.T) {
	logs := []map[string]interface{}{
		{"ioc": "a"},
		{"ioc": "b", "fields": map[string]interface{}{"host": "h1"}},
		{"user": "u", "fields": map[string]interface{}{"host": "h2", "ioc": "dup"}},
	}
	if got := strings.Join(collectLogColumns(logs), ","); got != "ioc,host,user" {
		t.Errorf("columns = %s, want ioc,host,user", got)
	}
}

// A top-level field wins over the same name in the nested fields map, which is
// the precedence alert templates use too.
func TestGetLogField(t *testing.T) {
	entry := map[string]interface{}{
		"ioc":    "top",
		"count":  42,
		"fields": map[string]interface{}{"ioc": "nested", "host": "h1"},
	}
	for field, want := range map[string]string{
		"ioc":     "top",
		"host":    "h1",
		"count":   "42",
		"missing": "",
	} {
		if got := getLogField(entry, field); got != want {
			t.Errorf("getLogField(%q) = %q, want %q", field, got, want)
		}
	}
	// A fields value of another shape is not a nested map and is not searched.
	if got := getLogField(map[string]interface{}{"fields": "host=h1"}, "host"); got != "" {
		t.Errorf("a non-map fields value was searched: %q", got)
	}
}

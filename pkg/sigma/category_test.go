package sigma

import "testing"

// A create_stream_hash rule must not run against process-creation events: real Sysmon
// event 1 also carries IMPHASH in Hashes, so an unscoped rule fires on every process.
func TestTranslateScopesByLogSourceCategory(t *testing.T) {
	cases := []struct{ category, want string }{
		{"process_creation", "bifract_category=process_creation"},
		{"network_connection", "bifract_category=network_connect"},
		{"file_event", "bifract_category=file_write"},
		{"create_remote_thread", "bifract_category=remote_thread"},
		{"dns_query", "bifract_category=dns_query"},
		{"create_stream_hash", "bifract_category=create_stream_hash"},
		{"", ""},
	}
	for _, c := range cases {
		got := CategoryFilter(LogSource{Category: c.category})
		if got != c.want {
			t.Errorf("category %q: got %q, want %q", c.category, got, c.want)
		}
	}
}

func TestTranslatePrependsCategory(t *testing.T) {
	rule := &SigmaRule{
		LogSource: LogSource{Category: "process_creation"},
		Detection: Detection{
			Condition:  "sel",
			Selections: map[string]SelectionGroup{"sel": {FieldConditions: []FieldCondition{{Field: "Image", Values: []string{"cmd.exe"}}}}},
		},
	}
	got, err := Translate(rule, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := `bifract_category=process_creation AND (Image="cmd.exe")`
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

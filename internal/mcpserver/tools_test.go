package mcpserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// The update endpoint replaces the alert rather than patching it, so a field the
// caller did not name must be read back and resent. Getting this wrong silently
// downgraded a critical alert to medium and made a scheduled alert unsaveable.
func TestUpdateAlertResendsTheFieldsItWasNotAskedToChange(t *testing.T) {
	current := `{"success":true,"data":{
		"id":"a1","name":"old","query_string":"level=error","description":"why",
		"alert_type":"scheduled","severity":"critical","enabled":true,
		"labels":["T1110"],"references":["https://example.test"],
		"throttle_time_seconds":60,"throttle_field":"src_ip",
		"schedule_cron":"*/15 * * * *","query_window_seconds":900,
		"dictionary_action_ids":["d1"],
		"webhook_actions":[{"id":"w1"},{"id":"w2"}],
		"fractal_actions":[{"id":"f1"}],
		"email_actions":[]}}`

	s := serve(t, 200, current)
	if _, err := updateAlert(context.Background(), s.client, updateAlertArgs{
		AlertID: "a1",
		Name:    "new",
	}); err != nil {
		t.Fatal(err)
	}

	var sent map[string]any
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatalf("the update body was not JSON: %v", err)
	}
	if s.last.Method != "PUT" {
		t.Errorf("method = %s", s.last.Method)
	}

	if sent["name"] != "new" {
		t.Errorf("the requested change was not applied: name = %v", sent["name"])
	}
	for field, want := range map[string]any{
		"severity":             "critical",
		"alert_type":           "scheduled",
		"schedule_cron":        "*/15 * * * *",
		"query_window_seconds": float64(900),
		"description":          "why",
		"throttle_field":       "src_ip",
	} {
		if sent[field] != want {
			t.Errorf("%s = %v, want %v: an untouched field was reset", field, sent[field], want)
		}
	}

	// Actions are read expanded and written as ids.
	for field, want := range map[string][]string{
		"webhook_action_ids":    {"w1", "w2"},
		"fractal_action_ids":    {"f1"},
		"email_action_ids":      {},
		"dictionary_action_ids": {"d1"},
	} {
		got := stringsOf(sent[field])
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
}

func stringsOf(value any) []string {
	list, _ := value.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

// The type-specific fields are absent rather than zero: the API reads a zero as
// an invalid value for the type instead of as "not applicable".
func TestCreateAlertSendsTypeSpecificFieldsOnlyWhenSet(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{}}`)
	if _, err := createAlert(context.Background(), s.client, createAlertArgs{
		Name:        "plain",
		QueryString: "level=error",
	}); err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"schedule_cron", "query_window_seconds", "window_duration"} {
		if _, present := sent[field]; present {
			t.Errorf("%s was sent for an event alert, where it does not apply", field)
		}
	}
	if sent["alert_type"] != "event" || sent["severity"] != "medium" {
		t.Errorf("defaults not applied: type=%v severity=%v", sent["alert_type"], sent["severity"])
	}
	// An omitted enabled must mean active, which a bare bool's zero value cannot say.
	if sent["enabled"] != true {
		t.Errorf("enabled = %v, want true when the caller said nothing", sent["enabled"])
	}
}

// The API writes a dictionary row from its fields alone and ignores the key, so a
// row that names no key column is stored and then never matches. That is silent,
// which is why it is caught before the write.
func TestAddDictionaryRowsRejectsARowThatCouldNeverMatch(t *testing.T) {
	definition := `{"success":true,"data":{"id":"d1","key_column":"indicator",
		"columns":[{"name":"indicator","is_key":true},{"name":"note"}]}}`

	for _, tc := range []struct {
		name string
		rows []map[string]string
		want string
	}{
		{"no key column", []map[string]string{{"note": "orphan"}}, "key column"},
		{"blank key", []map[string]string{{"indicator": "  ", "note": "x"}}, "key column"},
		{"unknown column", []map[string]string{{"indicator": "x", "nope": "y"}}, "no column for"},
		{"no rows", nil, "nothing to write"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := serve(t, 200, definition)
			_, err := addDictionaryRows(context.Background(), s.client, addDictionaryRowsArgs{
				DictionaryID: "d1",
				Rows:         tc.rows,
			})
			if err == nil {
				t.Fatal("expected a rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestAddDictionaryRowsSendsTheKeyColumnBothWays(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"d1","key_column":"indicator",
		"columns":[{"name":"indicator"},{"name":"note"}]}}`)
	if _, err := addDictionaryRows(context.Background(), s.client, addDictionaryRowsArgs{
		DictionaryID: "d1",
		Rows:         []map[string]string{{"indicator": "evil.test", "note": "seen"}},
	}); err != nil {
		t.Fatal(err)
	}
	var sent struct {
		Rows []struct {
			Key    string            `json:"key"`
			Fields map[string]string `json:"fields"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatal(err)
	}
	if len(sent.Rows) != 1 {
		t.Fatalf("sent %d rows", len(sent.Rows))
	}
	if sent.Rows[0].Key != "evil.test" || sent.Rows[0].Fields["indicator"] != "evil.test" {
		t.Errorf("key=%q fields=%v: the key must appear in both", sent.Rows[0].Key, sent.Rows[0].Fields)
	}
}

// A running Recall job has no rows. Returning it unchanged reads as "no matches",
// which is a different and wrong answer.
func TestGetArchiveSearchDoesNotPresentARunningJobAsEmpty(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"user":{"selected_fractal":"f1"}}}`)
	// The scope lookup and the job read hit the same stub, so prime the scope first.
	if _, err := s.client.FractalID(context.Background()); err != nil {
		t.Fatal(err)
	}

	running := serve(t, 200, `{"success":true,"data":{"id":"j1","status":"running","rows":[]}}`)
	running.client.fractal = "f1"
	got, err := getArchiveSearch(context.Background(), running.client, recallJobArgs{JobID: "j1"})
	if err != nil {
		t.Fatal(err)
	}
	object, _ := got.(map[string]any)
	if object["status"] != "running" {
		t.Fatalf("status = %v", object["status"])
	}
	if _, leaked := object["rows"]; leaked {
		t.Error("an empty row list was handed back for a job that has not finished")
	}
	if note, _ := object["note"].(string); !strings.Contains(note, "Still scanning") {
		t.Errorf("note = %q, which does not say the job is unfinished", note)
	}
}

// BQL has no escape sequence inside a quoted value, so a value carrying one of
// these cannot be represented and must be refused rather than change the query.
func TestBQLValuesThatCouldBreakOutAreRefused(t *testing.T) {
	for _, value := range []string{`a" | delete()`, `a'b`, `a\b`, `a|b`, "a\nb"} {
		if _, err := bqlLiteral(value, "image"); err == nil {
			t.Errorf("%q was quoted rather than refused", value)
		}
	}
	got, err := bqlContains("image", "powershell.exe")
	if err != nil {
		t.Fatal(err)
	}
	if got != `image=~"powershell.exe"` {
		t.Errorf("got %s", got)
	}
}

// pgr() returns a flat edge list. The tree it describes is what a model needs.
func TestTheProvenanceGraphIsRebuiltAsATree(t *testing.T) {
	rows := []any{
		row("spawn", "", "p1", "explorer.exe", 0.1),
		row("spawn", "p1", "p2", "powershell.exe", 0.9),
		row("spawn", "p1", "p3", "notepad.exe", 0.2),
		row("net_connect", "p2", "", "evil.test", 0.95),
		row("file_write", "p2", "", "c:\\temp\\x.dll", 0.4),
		row("reconnect_net", "p2", "p9", "evil.test", 0.99),
	}
	graph := summarizeGraph(rows, 40)

	if graph["processes"] != 3 {
		t.Errorf("processes = %v, want 3", graph["processes"])
	}
	if graph["roots"] != 1 {
		t.Errorf("roots = %v, want 1", graph["roots"])
	}
	tree, _ := graph["process_tree"].(string)
	for _, want := range []string{"explorer.exe", "powershell.exe", "notepad.exe"} {
		if !strings.Contains(tree, want) {
			t.Errorf("the tree omits %s:\n%s", want, tree)
		}
	}
	// The more anomalous sibling is listed first, so the interesting branch is read
	// before the model runs out of attention.
	if strings.Index(tree, "powershell.exe") > strings.Index(tree, "notepad.exe") {
		t.Errorf("siblings are not ranked by anomaly:\n%s", tree)
	}

	activity, _ := graph["notable_activity"].([]map[string]any)
	if len(activity) != 2 {
		t.Fatalf("activity = %d, want the 2 non-spawn, non-reconnect edges", len(activity))
	}
	if activity[0]["target"] != "evil.test" {
		t.Errorf("activity is not ranked by anomaly: %v", activity[0])
	}
	bridges, _ := graph["reconnections"].([]map[string]any)
	if len(bridges) != 1 || bridges[0]["type"] != "net" {
		t.Errorf("reconnections = %v", bridges)
	}
}

func row(eventType, parent, child, label string, anomaly float64) map[string]any {
	return map[string]any{
		"event_type": eventType, "parent": parent, "child": child,
		"label": label, "anomaly_score": anomaly, "host": "web-01",
	}
}

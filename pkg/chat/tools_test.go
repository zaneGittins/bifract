package chat

import (
	"encoding/json"
	"testing"
	"time"

	"bifract/pkg/aitools"
)

func chatTools(t *testing.T) *toolset {
	t.Helper()
	tools, err := newToolset()
	if err != nil {
		t.Fatal(err)
	}
	return tools
}

// A name in the allowlist that no tool answers to would narrow the surface
// without anyone noticing, so it stops the server. This is that check, run
// before a deploy rather than at boot.
func TestEveryExposedToolExists(t *testing.T) {
	tools := chatTools(t)
	if len(tools.byName) != len(exposed) {
		t.Fatalf("exposed %d tools, resolved %d", len(exposed), len(tools.byName))
	}
	if len(tools.defs) != len(exposed) {
		t.Errorf("%d tools resolved but %d schemas rendered", len(tools.byName), len(tools.defs))
	}
}

// These are excluded on purpose, and each for a reason worth restating: chat
// tool results carry ingested log data, which is attacker-controlled text.
// Anything here is something an injected instruction would reach for.
func TestTheDangerousToolsStayOutOfChat(t *testing.T) {
	withheld := map[string]string{
		"delete_alert":               "a deleted detection fails silently",
		"add_dictionary_rows":        "writes watchlists that feed live detections",
		"create_instruction_library": "instruction pages become this model's own prompt",
		"create_instruction_page":    "instruction pages become this model's own prompt",
		"update_instruction_page":    "instruction pages become this model's own prompt",
	}

	tools := chatTools(t)
	for name, why := range withheld {
		if _, reachable := tools.lookup(name); reachable {
			t.Errorf("chat exposes %s: %s", name, why)
		}
	}

	// The names have to still exist somewhere, or this test passes by typo.
	registered := map[string]bool{}
	for _, tool := range aitools.All() {
		registered[tool.Name()] = true
	}
	for name := range withheld {
		if !registered[name] {
			t.Errorf("%s is not a registered tool, so withholding it proves nothing", name)
		}
	}
}

// Confirmation is what stands between an injected instruction and a real write.
func TestEveryExposedWriteIsConfirmed(t *testing.T) {
	for name, tool := range chatTools(t).byName {
		if tool.ReadOnly() {
			continue
		}
		if !tool.NeedsConfirmation() && name != "cancel_archive_search" {
			t.Errorf("%s writes but chat would run it without asking", name)
		}
	}
}

// The picker governs what a query may scan. A model that could widen its own
// range could turn one question into a scan of the whole retention window.
func TestTheUsersTimeRangeOverridesTheModels(t *testing.T) {
	var query aitools.Tool
	for _, tool := range aitools.All() {
		if tool.Name() == "query_logs" {
			query = tool
		}
	}
	if query.Def == nil {
		t.Fatal("query_logs is not registered")
	}

	asked := `{"query":"level=error","start":"2001-01-01T00:00:00Z","end":"2001-01-02T00:00:00Z"}`
	var got struct {
		Query string `json:"query"`
		Start string `json:"start"`
		End   string `json:"end"`
	}
	if err := json.Unmarshal(withUserWindow(query, json.RawMessage(asked), "1h"), &got); err != nil {
		t.Fatal(err)
	}

	if got.Query != "level=error" {
		t.Errorf("the query itself was altered: %q", got.Query)
	}
	start, err := time.Parse(time.RFC3339, got.Start)
	if err != nil {
		t.Fatalf("start = %q: %v", got.Start, err)
	}
	if time.Since(start) > 2*time.Hour {
		t.Errorf("start = %s, which is not the hour the user selected", got.Start)
	}
	if got.End == "2001-01-02T00:00:00Z" {
		t.Error("the model's end time survived")
	}
}

// A tool that takes no range must not sprout one: its schema rejects a property
// it never declared, so adding start/end would break the call outright.
func TestToolsWithoutARangeAreLeftAlone(t *testing.T) {
	for _, tool := range chatTools(t).byName {
		if tool.TakesWindow() {
			continue
		}
		args := json.RawMessage(`{}`)
		if got := string(withUserWindow(tool, args, "1h")); got != string(args) {
			t.Errorf("%s does not take a range but was given %s", tool.Name(), got)
		}
	}
}

// The card shows the arguments and the server runs them, so the two have to be
// the same object. Resolving the range after the user approves would mean the
// window shown is not the window scanned, which for an archive search is the
// difference between a minute and an hour of storage reads.
func TestWhatIsOfferedIsWhatRuns(t *testing.T) {
	var archive aitools.Tool
	for _, tool := range aitools.All() {
		if tool.Name() == "search_archive" {
			archive = tool
		}
	}
	if archive.Def == nil {
		t.Fatal("search_archive is not registered")
	}
	if !archive.NeedsConfirmation() {
		t.Fatal("search_archive is expected to be confirmed")
	}

	// What the model asked for, which is not what the user selected.
	asked := json.RawMessage(`{"query":"level=*","start":"2001-01-01T00:00:00Z","end":"2001-01-02T00:00:00Z"}`)
	offered := withUserWindow(archive, asked, "1h")

	// Running the offered arguments a second time must change nothing: that is
	// what lets the approval path run them verbatim.
	if again := string(withUserWindow(archive, offered, "1h")); again != string(offered) {
		t.Errorf("resolving twice changed the call:\n offered %s\n again   %s", offered, again)
	}

	var got struct{ Start, End string }
	if err := json.Unmarshal(offered, &got); err != nil {
		t.Fatal(err)
	}
	if got.Start == "2001-01-01T00:00:00Z" || got.End == "2001-01-02T00:00:00Z" {
		t.Errorf("the model's window survived into the offer: %s", offered)
	}
}

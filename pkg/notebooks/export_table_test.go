package notebooks

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"bifract/pkg/storage"
)

// A results table has to be valid markdown, which means the separator row
// carries exactly one cell per column. It shipped with a doubled trailing pipe,
// which renders as a literal row of text rather than a table.
func TestExportResultsTableIsValidMarkdown(t *testing.T) {
	for _, cols := range [][]string{{"host"}, {"host", "user"}, {"a", "b", "c"}} {
		row := map[string]any{}
		for _, c := range cols {
			row[c] = c + "-value"
		}
		payload, _ := json.Marshal(map[string]any{"results": []map[string]any{row}})

		var b strings.Builder
		writeResults(&b, json.RawMessage(payload))

		lines := strings.Split(strings.TrimSpace(b.String()), "\n")
		if len(lines) < 3 {
			t.Fatalf("%d columns: expected header, separator and a row, got %q", len(cols), b.String())
		}
		for i, line := range lines[:3] {
			if got := strings.Count(line, "|"); got != len(cols)+1 {
				t.Errorf("%d columns: line %d %q has %d pipes, want %d", len(cols), i, line, got, len(cols)+1)
			}
		}
	}
}

// A locked notebook says so in its export: the reader has to be able to tell a
// frozen record from a draft that is still moving.
func TestExportMarkdownStampsLockState(t *testing.T) {
	locked := parseTime(t, "2026-08-30T12:00:00Z")
	nb := &storage.Notebook{Name: "case", LockedAt: &locked, LockedBy: "zane"}
	out := exportMarkdown(nb, []storage.NotebookSection{{SectionType: "markdown", Content: "note"}}, markdownExportOptions{})
	if !strings.Contains(out, "Locked 2026-08-30 12:00 UTC by zane") {
		t.Errorf("locked export does not name the lock:\n%s", out)
	}

	draft := exportMarkdown(&storage.Notebook{Name: "case"},
		[]storage.NotebookSection{{SectionType: "markdown", Content: "note"}}, markdownExportOptions{})
	if !strings.Contains(draft, "Draft") {
		t.Errorf("unlocked export does not say it is a draft:\n%s", draft)
	}
}

func parseTime(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parsing %s: %v", s, err)
	}
	return parsed
}

package notebooks

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"bifract/pkg/storage"
)

func TestNormalizeEventTime(t *testing.T) {
	if got := normalizeEventTime(nil); got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}

	var zero time.Time
	if got := normalizeEventTime(&zero); got != nil {
		t.Fatalf("zero time: got %v, want nil", got)
	}

	denver, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Skipf("zoneinfo unavailable: %v", err)
	}
	local := time.Date(2026, 8, 28, 14, 30, 0, 0, denver)
	got := normalizeEventTime(&local)
	if got == nil {
		t.Fatal("real time: got nil")
	}
	if got.Location() != time.UTC {
		t.Fatalf("got location %v, want UTC", got.Location())
	}
	if !got.Equal(local) {
		t.Fatalf("normalizing changed the instant: got %v, want %v", got, local)
	}
}

// Evidence references a comment, so it is created by posting a comment with a
// notebook_id. Allowing it here again would reintroduce sections holding a copy
// of a comment that nothing keeps in step.
func TestCreatableSectionTypes(t *testing.T) {
	for _, typ := range []string{"markdown", "query", "ai_summary", "ai_attack_chain"} {
		if !creatableSectionTypes[typ] {
			t.Errorf("section type %q should be creatable", typ)
		}
	}
	for _, typ := range []string{"", "comment_context", "evidence", "chart", "sql"} {
		if creatableSectionTypes[typ] {
			t.Errorf("section type %q should not be creatable", typ)
		}
	}
}

// The comment generator builds section titles from user-written comment text.
// A byte-slice cut there splits a multibyte rune, and Postgres rejects the
// insert as invalid UTF-8, so the comment silently never reaches the notebook.
func TestTruncateRunes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than limit", "abc", 80, "abc"},
		{"exactly the limit", "abcde", 5, "abcde"},
		{"ascii cut", "abcdef", 3, "abc"},
		{"empty", "", 80, ""},
		{"cut lands mid-rune", strings.Repeat("日", 100), 80, strings.Repeat("日", 80)},
		{"emoji cut", strings.Repeat("🔥", 10), 4, strings.Repeat("🔥", 4)},
		{"mixed width", "héllo wörld", 5, "héllo"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateRunes(tc.in, tc.n)
			if got != tc.want {
				t.Fatalf("truncateRunes(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("truncateRunes(%q, %d) produced invalid UTF-8: %q", tc.in, tc.n, got)
			}
		})
	}
}

// Every cut length across a multibyte string must stay valid: this is the
// property the byte-slicing version failed at two out of every three offsets.
func TestTruncateRunesAlwaysValidUTF8(t *testing.T) {
	subject := "日本語テキスト with ASCII and 🔥 emoji"
	for n := 0; n <= utf8.RuneCountInString(subject)+5; n++ {
		got := truncateRunes(subject, n)
		if !utf8.ValidString(got) {
			t.Fatalf("cut at %d produced invalid UTF-8: %q", n, got)
		}
		if want := min(n, utf8.RuneCountInString(subject)); utf8.RuneCountInString(got) != want {
			t.Fatalf("cut at %d gave %d runes, want %d", n, utf8.RuneCountInString(got), want)
		}
	}
}

// The outline line for a filed comment: who wrote it and the start of what they
// said, or the event itself when a star carries no words to quote.
func TestEvidenceTitle(t *testing.T) {
	cases := []struct {
		name    string
		comment storage.Comment
		want    string
	}{
		{"display name and text", storage.Comment{Author: "zane", AuthorDisplayName: "Zane G", Text: "beaconing"}, "Zane G: beaconing"},
		{"falls back to username", storage.Comment{Author: "zane", Text: "beaconing"}, "zane: beaconing"},
		// Titling these with the author read as a notebook full of "Administrator".
		{"star names the event", storage.Comment{Author: "zane", AuthorDisplayName: "Zane G", LogID: "3108992d8c9741ab"}, "Event 3108992d8c97"},
		{"star with no log id falls back to the name", storage.Comment{Author: "zane", AuthorDisplayName: "Zane G"}, "Zane G"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidenceTitle(tc.comment); got != tc.want {
				t.Fatalf("evidenceTitle() = %q, want %q", got, tc.want)
			}
		})
	}

	long := storage.Comment{Author: "zane", Text: strings.Repeat("x", 200)}
	got := evidenceTitle(long)
	if utf8.RuneCountInString(got) > maxSectionTitleChars {
		t.Fatalf("title of %d runes exceeds the column width", utf8.RuneCountInString(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("a cut title should say so: %q", got)
	}
}

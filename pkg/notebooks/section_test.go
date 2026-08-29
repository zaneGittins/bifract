package notebooks

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestValidateEvidenceContent(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantErr bool
	}{
		{"valid", `{"log_id":"a3f5c1d2e4b60718"}`, false},
		{"valid with fields", `{"log_id":"A3F5C1D2","comment_text":"suspicious","query":"host=x"}`, false},
		{"not json", `log_id=a3f5c1d2`, true},
		{"json array", `[{"log_id":"a3f5c1d2"}]`, true},
		{"missing log_id", `{"comment_text":"note"}`, true},
		{"empty log_id", `{"log_id":""}`, true},
		{"log_id not a string", `{"log_id":12345678}`, true},
		{"log_id too short", `{"log_id":"a3f5"}`, true},
		{"log_id not hex", `{"log_id":"a3f5c1d2' OR 1=1--"}`, true},
		{"log_id with quote", `{"log_id":"a3f5c1d2\\' OR 1=1--"}`, true},
		{"log_id too long", `{"log_id":"` + repeat("a", 65) + `"}`, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEvidenceContent(tc.content)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateEvidenceContent(%q) error = %v, wantErr %v", tc.content, err, tc.wantErr)
			}
		})
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, n*len(s))
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

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

// comment_context is what "pin this log" writes, so it has to be creatable
// through the section API rather than only by the comment generator.
func TestCreatableSectionTypes(t *testing.T) {
	for _, typ := range []string{"markdown", "query", "comment_context", "ai_summary", "ai_attack_chain"} {
		if !creatableSectionTypes[typ] {
			t.Errorf("section type %q should be creatable", typ)
		}
	}
	for _, typ := range []string{"", "evidence", "chart", "sql"} {
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

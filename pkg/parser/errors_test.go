package parser

import "testing"

// TestParseErrorPositions verifies that parse/lex errors carry the rune span of
// the offending text so editors can underline it. Offsets are code-point (rune)
// indices; End is exclusive.
func TestParseErrorPositions(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantStart int
		wantEnd   int
		wantSub   string // expected source slice [start,end); "" for a caret
	}{
		{"bad character", "level=info @", 11, 12, "@"},
		{"unexpected eof after operator", "level=", 6, 6, ""},
		{"wrong token mid-pipeline", "status=info | sort by", 14, 18, "sort"},
		{"function name expected", "a=1 | stats badtok(", 6, 11, "stats"},
		{"eof in expression", "x := ", 5, 5, ""},
		{"eof message", "* | groupby(a", 13, 13, ""},
		{"eof before trailing space", "* | groupby(a  \n ", 13, 13, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseQuery(c.query)
			if err == nil {
				t.Fatalf("expected an error for %q", c.query)
			}
			start, end, ok := ErrorPosition(err)
			if !ok {
				t.Fatalf("expected a positioned error for %q, got %v", c.query, err)
			}
			if start != c.wantStart || end != c.wantEnd {
				t.Fatalf("span = [%d,%d), want [%d,%d) for %q (msg=%q)", start, end, c.wantStart, c.wantEnd, c.query, err.Error())
			}
			runes := []rune(c.query)
			if start < 0 || end > len(runes) || start > end {
				t.Fatalf("span [%d,%d) out of bounds for %q (len=%d)", start, end, c.query, len(runes))
			}
			if got := string(runes[start:end]); got != c.wantSub {
				t.Fatalf("underlined %q, want %q for %q", got, c.wantSub, c.query)
			}
		})
	}
}

// TestValidQueriesNoError guards against false-positive errors on valid queries.
func TestValidQueriesNoError(t *testing.T) {
	for _, q := range []string{
		"level=info",
		"status=error AND level=warn",
		`message=~"failed login"`,
	} {
		if _, err := ParseQuery(q); err != nil {
			t.Errorf("unexpected error for valid query %q: %v", q, err)
		}
	}
}

// TestErrorPositionNonPositioned verifies ErrorPosition reports ok=false for a
// plain error that carries no span (the editor falls back to a banner).
func TestErrorPositionNonPositioned(t *testing.T) {
	if _, _, ok := ErrorPosition(errPlain("boom")); ok {
		t.Fatal("expected ok=false for a non-positioned error")
	}
}

type errPlain string

func (e errPlain) Error() string { return string(e) }

// TestStripComments verifies comment lines are blanked in place, so the query
// parses and error offsets still index the original text.
func TestStripComments(t *testing.T) {
	cases := []struct {
		in, want string
		blanked  int
	}{
		{"* | head(5)", "* | head(5)", 0},
		{"// note\n* | head(5)", "       \n* | head(5)", 7},
		{"*\n  // indented\n| head(5)", "*\n             \n| head(5)", 11},
		{"// a\r\n*", "    \r\n*", 4},
		{"// a\r*", "    \r*", 4},
		{"// é\n*", "    \n*", 4},
		{"* | head(5) // mid-line", "* | head(5) // mid-line", 0},
		{`message="a // b"`, `message="a // b"`, 0},
		{"// only", "       ", 7},
	}
	for _, c := range cases {
		got, blanked := StripComments(c.in)
		if got != c.want || blanked != c.blanked {
			t.Fatalf("StripComments(%q) = %q, %d; want %q, %d", c.in, got, blanked, c.want, c.blanked)
		}
		if len([]rune(got)) != len([]rune(c.in)) {
			t.Fatalf("StripComments(%q) moved runes", c.in)
		}
	}

	_, err := ParseQuery(mustStrip("// note\n* | groupby(a b)"))
	start, end, ok := ErrorPosition(err)
	if !ok || start != 22 || end != 23 {
		t.Fatalf("span after a comment line = [%d,%d) ok=%v, want [22,23)", start, end, ok)
	}
}

func mustStrip(q string) string {
	s, _ := StripComments(q)
	return s
}

// TestMidLineSlashes verifies a // that does not start a line is an error
// rather than an empty regex that silently matches everything.
func TestMidLineSlashes(t *testing.T) {
	for _, q := range []string{"* | // groupby(host)", "user=// x", "* | head(5) // note"} {
		if _, err := ParseQuery(q); err == nil {
			t.Fatalf("%q: expected an error", q)
		}
	}
	if _, err := ParseQuery("image=/powershell/i | x := bytes / 1e6"); err != nil {
		t.Fatalf("regex and division must still parse: %v", err)
	}
}

// TestParseErrorMessages verifies errors name what the user typed rather than
// internal token kinds.
func TestParseErrorMessages(t *testing.T) {
	cases := map[string]string{
		"* | groupby(a":   "expected ')', got end of query",
		"user= | head(1)": "expected value, got '|'",
	}
	for q, want := range cases {
		_, err := ParseQuery(q)
		if err == nil || err.Error() != want {
			t.Fatalf("%q: got %v, want %q", q, err, want)
		}
	}
}

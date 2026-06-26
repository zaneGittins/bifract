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

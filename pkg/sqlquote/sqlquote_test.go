package sqlquote

import "testing"

// Each constructor closes what it opens, whatever the value holds: a quote, a
// backslash that would escape the closing quote, or a control character.
func TestConstructorsCannotBeBrokenOutOf(t *testing.T) {
	for in, want := range map[string]string{
		"plain":        `'plain'`,
		"it's":         `'it\'s'`,
		`ends\`:        `'ends\\'`,
		`\' OR 1=1 --`: `'\\\' OR 1=1 --'`,
		"a\nb\tc\x00":  `'a\nb\tc\0'`,
	} {
		if got := String(in); got != want {
			t.Errorf("String(%q) = %s, want %s", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"logs":        "`logs`",
		"a`b":         "`a``b`",
		"x\\`; DROP":  "`x\\\\``; DROP`",
		"dotted.name": "`dotted.name`",
	} {
		if got := Ident(in); got != want {
			t.Errorf("Ident(%q) = %s, want %s", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"it's":  `'it''s'`,
		`a\b`:   `'a\b'`,
		"'; --": `'''; --'`,
	} {
		if got := PGString(in); got != want {
			t.Errorf("PGString(%q) = %s, want %s", in, got, want)
		}
	}
}

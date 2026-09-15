package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The BQL highlighter emits a class per token kind and the stylesheet colours
// them. A class added to one and not the other is invisible: the token renders
// in the default colour with nothing to say it went wrong. This pins the two
// together.
func TestEveryHighlightClassIsStyled(t *testing.T) {
	js, err := os.ReadFile("../../web/static/syntaxHighlight.js")
	if err != nil {
		t.Skipf("highlighter not present: %v", err)
	}
	css, err := os.ReadFile("../../web/static/css/02-query.css")
	if err != nil {
		t.Fatalf("query stylesheet: %v", err)
	}

	// Classes the tokenizer attaches to a segment, as c: 'hl-name'.
	emitted := regexp.MustCompile(`c: '(hl-[a-z-]+)'`)
	styled := string(css)
	seen := map[string]bool{}
	for _, m := range emitted.FindAllStringSubmatch(string(js), -1) {
		class := m[1]
		if seen[class] {
			continue
		}
		seen[class] = true
		// hl-error and hl-match are decorations layered onto another class.
		if class == "hl-error" || class == "hl-match" {
			continue
		}
		// A selector, not a substring: ".hl-binding" is inside ".hl-bindingX", so
		// a plain Contains passes on a stylesheet that styles neither.
		selector := regexp.MustCompile(`\.` + regexp.QuoteMeta(class) + `(?:[^-a-zA-Z0-9_]|$)`)
		if !selector.MatchString(styled) {
			t.Errorf("%s is emitted by the highlighter but has no rule in 02-query.css", class)
		}
	}
	if len(seen) < 10 {
		t.Errorf("only found %d highlight classes; the scan likely broke", len(seen))
	}
}

// A colour the stylesheet names must exist in the palette, or the token renders
// as unstyled text in whichever theme forgot it.
func TestHighlightColoursAreDefinedInBothThemes(t *testing.T) {
	css, err := os.ReadFile("../../web/static/css/02-query.css")
	if err != nil {
		t.Fatalf("query stylesheet: %v", err)
	}
	base, err := os.ReadFile("../../web/static/css/01-base.css")
	if err != nil {
		t.Fatalf("base stylesheet: %v", err)
	}
	used := regexp.MustCompile(`var\((--syntax-[a-z-]+)\)`)
	for _, m := range used.FindAllStringSubmatch(string(css), -1) {
		name := m[1]
		if n := strings.Count(string(base), name+":"); n < 2 {
			t.Errorf("%s is used by the highlighter but defined %d times in 01-base.css; both the dark and light palettes need it", name, n)
		}
	}
}

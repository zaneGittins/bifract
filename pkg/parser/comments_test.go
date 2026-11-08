package parser

import (
	"reflect"
	"strings"
	"testing"
)

// Every caller reads stored text, so a saved rule's comments must never reach the lexer.
func TestParseQueryIgnoresCommentLines(t *testing.T) {
	plain, err := ParseQuery("status=500 | head(5)")
	if err != nil {
		t.Fatal(err)
	}
	commented, err := ParseQuery("// why\nstatus=500\n  // noisy\n| head(5)\n// end")
	if err != nil {
		t.Fatalf("a comment line should parse: %v", err)
	}
	if !reflect.DeepEqual(commandNames(commented), commandNames(plain)) ||
		!reflect.DeepEqual(commented.Filters(), plain.Filters()) {
		t.Fatalf("comments changed the pipeline: %+v, want %+v", commented, plain)
	}
	if _, err := ParseQuery("status=500 // trailing"); err == nil {
		t.Fatal("// after query text is not a comment and must stay an error")
	}
}

func commandNames(p *PipelineNode) []string {
	names := make([]string, 0, len(p.Commands()))
	for _, c := range p.Commands() {
		names = append(names, c.Name)
	}
	return names
}

// An empty pipeline scans everything, so a rule commented out must not become one.
func TestParseQueryRefusesOnlyComments(t *testing.T) {
	for _, q := range []string{"// off", "// off\n\n  // still off\n"} {
		if _, err := ParseQuery(q); err == nil || !strings.Contains(err.Error(), "only comments") {
			t.Fatalf("ParseQuery(%q) error = %v", q, err)
		}
	}
}

// A literal is the query's data, so a line in one that starts with // is not a comment.
func TestCommentsLeaveLiteralsAlone(t *testing.T) {
	for _, q := range []string{
		"msg=\"a\n// b\"",
		"msg='a\n// b'",
		"msg=/foo\n//bar/",
	} {
		if got, blanked := StripComments(q); got != q || blanked != 0 {
			t.Fatalf("StripComments(%q) = %q, %d", q, got, blanked)
		}
	}

	got, blanked := StripComments("// why\nmsg=\"a\n// b\"\n// end")
	if want := "      \nmsg=\"a\n// b\"\n      "; got != want || blanked != 12 {
		t.Fatalf("got %q, %d; want %q, 12", got, blanked, want)
	}
}

// Sub-lexers read fragments such as argument text, where a leading // is not a comment.
func TestOnlyStripCommentsScansForComments(t *testing.T) {
	if _, err := NewLexer("// not skipped").Tokenize(); err == nil {
		t.Fatal("a plain lexer must keep refusing //")
	}
}

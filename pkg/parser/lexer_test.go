package parser

import (
	"testing"
)

func TestRegexLexing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple regex with case insensitive flag",
			input:    "/error/i",
			expected: "(?i)error",
		},
		{
			name:     "regex with escaped dot and anchor",
			input:    "/\\.exe$/i", // Need to escape backslash in Go string
			expected: "(?i)\\.exe$",
		},
		{
			name:     "regex with backslash and forward slash",
			input:    "/var\\/log/",
			expected: "var\\/log",
		},
		{
			name:     "regex without flags",
			input:    "/test/",
			expected: "test",
		},
		{
			name:     "complex URL regex",
			input:    "/https?:\\/\\/.*/i",
			expected: "(?i)https?:\\/\\/.*",
		},
		{
			name:     "slash inside character class (base64 pattern)",
			input:    "/[A-Za-z0-9+/]{50,}/i",
			expected: "(?i)[A-Za-z0-9+/]{50,}",
		},
		{
			name:     "negated character class with slash",
			input:    "/[^/]+/",
			expected: "[^/]+",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer("field=" + tt.input)
			tokens, err := lexer.Tokenize()
			if err != nil {
				t.Errorf("Tokenize() error = %v", err)
				return
			}

			// Find the regex token
			var regexToken *Token
			for _, tok := range tokens {
				if tok.Type == TokenRegex {
					regexToken = &tok
					break
				}
			}

			if regexToken == nil {
				t.Errorf("No regex token found in input %q", tt.input)
				return
			}

			if regexToken.Value != tt.expected {
				t.Errorf("Regex token value = %q, want %q", regexToken.Value, tt.expected)
			} else {
				t.Logf("✓ Input %q → Regex value %q", tt.input, regexToken.Value)
			}
		})
	}
}

func TestFullQueryWithRegex(t *testing.T) {
	// Query string with properly escaped backslash (as it would come from HTTP request)
	query := "event_id=11 | target_filename=/\\.exe$/i | groupby([image,target_filename])"

	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("ParseQuery error: %v", err)
	}

	var havingCond Condition
	ForEachCondition(pipeline, func(c Condition) {
		if c.Field == "target_filename" {
			havingCond = c
		}
	})
	if havingCond.Field == "" {
		t.Fatalf("no target_filename condition in %+v", pipeline.Filters())
	}
	t.Logf("HAVING condition:")
	t.Logf("  Field: %q", havingCond.Field)
	t.Logf("  Operator: %q", havingCond.Operator)
	t.Logf("  Value: %q", havingCond.Value)
	t.Logf("  IsRegex: %v", havingCond.IsRegex)

	if !havingCond.IsRegex {
		t.Error("Expected regex HAVING condition, but IsRegex=false")
	}

	expected := "(?i)\\.exe$"
	if havingCond.Value != expected {
		t.Errorf("Regex value = %q, want %q", havingCond.Value, expected)
	}
}

// A token keeps the text it was written as: a regex's Value is the compiled
// pattern, its Text the /pattern/flags typed.
func TestTokenKeepsItsSourceText(t *testing.T) {
	toks, err := NewLexer(`a=/ab+c/i | "x y"`).Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []struct{ value, text string }{{"a", "a"}, {"=", "="}, {"(?i)ab+c", "/ab+c/i"}, {"|", "|"}, {"x y", `"x y"`}} {
		if toks[i].Value != want.value || toks[i].Text != want.text {
			t.Errorf("token %d = %q written %q, want %q written %q", i, toks[i].Value, toks[i].Text, want.value, want.text)
		}
	}
}

// An error quotes a regex as the user wrote it, flags included.
func TestErrorsQuoteARegexAsWritten(t *testing.T) {
	if got := describeToken(Token{Type: TokenRegex, Value: "(?i)abc", Text: "/abc/i"}); got != "'/abc/i'" {
		t.Errorf("describeToken = %s, want '/abc/i'", got)
	}
	// A regex argument, and one a let binding holds, render as written.
	for _, q := range []string{`* | head(/10\./i)`, `let &r := /10\./i; * | head(&r)`} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		cmds := pl.Commands()
		if len(cmds) != 1 {
			t.Fatalf("%s: %d commands", q, len(cmds))
		}
		cmd := cmds[0]
		if len(cmd.Args) != 1 || cmd.Args[0].String() != `/10\./i` {
			t.Errorf("%s: argument renders as %v, want /10\\./i", q, cmd.Args)
		}
	}
}

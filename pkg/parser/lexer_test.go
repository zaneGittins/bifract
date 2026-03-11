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

	// Check that we have a HAVING condition with regex
	if len(pipeline.HavingConditions) == 0 {
		t.Fatal("Expected HAVING conditions, got none")
	}

	havingCond := pipeline.HavingConditions[0]
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

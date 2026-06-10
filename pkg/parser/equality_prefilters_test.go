package parser

import (
	"strings"
	"testing"
)

func TestEqualityPreFilters(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		value     string
		wantParts []string // all must be substrings of output
		wantEmpty bool     // true if output should be ""
	}{
		{
			name:  "alpha value produces field_tokens compound token",
			field: "process_name",
			value: "curl.exe",
			wantParts: []string{
				"hasToken(field_tokens, 'process_name:curl.exe')",
				"hasToken(raw_log, 'curl')",
				"hasToken(raw_log, 'exe')",
			},
		},
		{
			name:  "result is wrapped in OR group with parentheses",
			field: "process_name",
			value: "curl.exe",
			wantParts: []string{
				"(hasToken(field_tokens, 'process_name:curl.exe') OR",
			},
		},
		{
			name:  "whitespace in value normalized to underscore in field_tokens token",
			field: "commandline",
			value: "net user",
			wantParts: []string{
				"hasToken(field_tokens, 'commandline:net_user')",
				"hasToken(raw_log, 'net')",
				"hasToken(raw_log, 'user')",
			},
		},
		{
			name:  "colon in field name normalized to underscore in field_tokens token",
			field: "http:status",
			value: "running",
			wantParts: []string{
				"hasToken(field_tokens, 'http_status:running')",
				"hasToken(raw_log, 'running')",
			},
		},
		{
			name:      "numeric-only value returns empty (no skip index benefit)",
			field:     "event_id",
			value:     "4688",
			wantEmpty: true,
		},
		{
			name:      "short value under 3 chars returns empty",
			field:     "flag",
			value:     "ok",
			wantEmpty: true,
		},
		{
			name:  "field name and value are lowercased in compound token",
			field: "ProcessName",
			value: "PowerShell.EXE",
			wantParts: []string{
				"hasToken(field_tokens, 'processname:powershell.exe')",
			},
		},
		{
			name:  "type-hinted field gets the same pre-filter as dynamic field",
			field: "original_file_name",
			value: "curl.exe",
			wantParts: []string{
				"hasToken(field_tokens, 'original_file_name:curl.exe')",
				"hasToken(raw_log, 'curl')",
				"hasToken(raw_log, 'exe')",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := equalityPreFilters(tt.field, tt.value)
			if tt.wantEmpty {
				if got != "" {
					t.Errorf("equalityPreFilters(%q, %q) = %q; want empty string", tt.field, tt.value, got)
				}
				return
			}
			for _, part := range tt.wantParts {
				if !strings.Contains(got, part) {
					t.Errorf("equalityPreFilters(%q, %q) = %q; want substring %q", tt.field, tt.value, got, part)
				}
			}
		})
	}
}

func TestReplaceQueryTokenSeparators(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"normal", "normal"},
		{"with space", "with_space"},
		{"with:colon", "with_colon"},
		{"with\ttab", "with_tab"},
		{"with\nnewline", "with_newline"},
		{"multi : : colon", "multi_____colon"},
		{"", ""},
		{"http://example.com", "http_//example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := replaceQueryTokenSeparators(tt.input)
			if got != tt.want {
				t.Errorf("replaceQueryTokenSeparators(%q) = %q; want %q", tt.input, got, tt.want)
			}
		})
	}
}

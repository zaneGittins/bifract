package parser

import (
	"strings"
	"testing"
)

func TestEqualityPreFilters(t *testing.T) {
	tests := []struct {
		name       string
		field      string
		value      string
		wantParts  []string // all must be substrings of output
		wantAbsent []string // none may appear in output
		wantEmpty  bool     // true if output should be ""
	}{
		{
			name:  "alpha value produces raw_log token pre-filter",
			field: "process_name",
			value: "curl.exe",
			wantParts: []string{
				"hasToken(raw_log, 'curl')",
				"hasToken(raw_log, 'exe')",
			},
			// field_tokens is deprecated and no longer queried; never reference it.
			wantAbsent: []string{"field_tokens"},
		},
		{
			name:  "result is wrapped in parentheses",
			field: "process_name",
			value: "curl.exe",
			wantParts: []string{
				"(hasToken(raw_log, 'curl') AND hasToken(raw_log, 'exe'))",
			},
		},
		{
			name:  "whitespace value splits into separate raw_log tokens",
			field: "commandline",
			value: "net user",
			wantParts: []string{
				"hasToken(raw_log, 'net')",
				"hasToken(raw_log, 'user')",
			},
			wantAbsent: []string{"field_tokens"},
		},
		{
			name:      "numeric-only value returns empty (IPs/ids prune via typed sub-column index)",
			field:     "src_ip",
			value:     "192.168.2.225",
			wantEmpty: true,
		},
		{
			name:      "numeric-only value returns empty",
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
			name:  "value tokens are lowercased",
			field: "ProcessName",
			value: "PowerShell.EXE",
			wantParts: []string{
				"hasToken(raw_log, 'powershell')",
			},
			wantAbsent: []string{"field_tokens"},
		},
		{
			name:  "type-hinted field gets the same raw_log pre-filter as dynamic field",
			field: "original_file_name",
			value: "curl.exe",
			wantParts: []string{
				"hasToken(raw_log, 'curl')",
				"hasToken(raw_log, 'exe')",
			},
			wantAbsent: []string{"field_tokens"},
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
			for _, absent := range tt.wantAbsent {
				if strings.Contains(got, absent) {
					t.Errorf("equalityPreFilters(%q, %q) = %q; must not contain %q", tt.field, tt.value, got, absent)
				}
			}
		})
	}
}

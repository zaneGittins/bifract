package storage

import (
	"strings"
	"testing"
)

func TestBuildFieldTokens(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]string
		want   []string // all must appear in output (as substrings)
		absent []string // none must appear in output
	}{
		{
			name:   "normal field produces field:value token",
			fields: map[string]string{"process_name": "curl.exe"},
			want:   []string{"process_name:curl.exe"},
		},
		{
			name:   "output is lowercased",
			fields: map[string]string{"ProcessName": "Curl.EXE"},
			want:   []string{"processname:curl.exe"},
		},
		{
			name:   "empty value is skipped",
			fields: map[string]string{"field_a": "value", "field_b": ""},
			want:   []string{"field_a:value"},
			absent: []string{"field_b"},
		},
		{
			name:   "whitespace in value becomes underscore",
			fields: map[string]string{"commandline": "net user /add"},
			want:   []string{"commandline:net_user_/add"},
		},
		{
			name:   "colon in field name becomes underscore",
			fields: map[string]string{"http:status": "200"},
			want:   []string{"http_status:200"},
		},
		{
			name: "multiple fields produce space-separated tokens",
			fields: map[string]string{
				"event_id": "4688",
				"user":     "SYSTEM",
			},
			want: []string{"event_id:4688", "user:system"},
		},
		{
			name:   "output is deterministic across calls",
			fields: map[string]string{"z": "1", "a": "2", "m": "3"},
			// sorted: a:2 m:3 z:1
			want: []string{"a:2 m:3 z:1"},
		},
		{
			name:   "tab in value becomes underscore",
			fields: map[string]string{"key": "val\tue"},
			want:   []string{"key:val_ue"},
		},
		{
			name:   "empty fields map returns empty string",
			fields: map[string]string{},
			absent: []string{":"},
		},
		{
			name:   "all-empty values returns empty string",
			fields: map[string]string{"a": "", "b": ""},
			absent: []string{":"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildFieldTokens(tt.fields)
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("buildFieldTokens(%v) = %q; want substring %q", tt.fields, got, w)
				}
			}
			for _, a := range tt.absent {
				if strings.Contains(got, a) {
					t.Errorf("buildFieldTokens(%v) = %q; must not contain %q", tt.fields, got, a)
				}
			}
		})
	}
}

func TestReplaceTokenSeparators(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := replaceTokenSeparators(tt.input)
			if got != tt.want {
				t.Errorf("replaceTokenSeparators(%q) = %q; want %q", tt.input, got, tt.want)
			}
		})
	}
}

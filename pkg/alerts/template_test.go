package alerts

import "testing"

func TestResolveTemplateName(t *testing.T) {
	tests := []struct {
		name     string
		template string
		results  []map[string]interface{}
		want     string
	}{
		{"no template", "Simple Alert", nil, "Simple Alert"},
		{"empty results", "Alert {{src_ip}}", nil, "Alert {{src_ip}}"},
		{"empty results slice", "Alert {{src_ip}}", []map[string]interface{}{}, "Alert {{src_ip}}"},
		{
			"single field",
			"Alert from {{src_ip}}",
			[]map[string]interface{}{{"src_ip": "10.0.0.1"}},
			"Alert from 10.0.0.1",
		},
		{
			"multiple fields",
			"{{action}} from {{src_ip}}",
			[]map[string]interface{}{{"action": "Login", "src_ip": "10.0.0.1"}},
			"Login from 10.0.0.1",
		},
		{
			"missing field left as-is",
			"Alert {{missing}}",
			[]map[string]interface{}{{"src_ip": "10.0.0.1"}},
			"Alert {{missing}}",
		},
		{
			"nested fields map",
			"Alert {{host}}",
			[]map[string]interface{}{{"fields": map[string]interface{}{"host": "server1"}}},
			"Alert server1",
		},
		{
			"top-level takes precedence over nested",
			"Alert {{host}}",
			[]map[string]interface{}{{"host": "top-level", "fields": map[string]interface{}{"host": "nested"}}},
			"Alert top-level",
		},
		{
			"integer value",
			"Port {{dst_port}}",
			[]map[string]interface{}{{"dst_port": 443}},
			"Port 443",
		},
		{
			"uses first result only",
			"Alert {{src_ip}}",
			[]map[string]interface{}{{"src_ip": "10.0.0.1"}, {"src_ip": "10.0.0.2"}},
			"Alert 10.0.0.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveTemplateName(tt.template, tt.results)
			if got != tt.want {
				t.Errorf("ResolveTemplateName(%q) = %q, want %q", tt.template, got, tt.want)
			}
		})
	}
}

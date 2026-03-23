package ingest

import (
	"testing"

	"bifract/pkg/normalizers"
)

func TestParseRFC5424(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect map[string]string
	}{
		{
			name:  "standard message",
			input: `<134>1 2024-02-28T10:15:30.123Z server1 myapp 1234 ID47 - User logged in`,
			expect: map[string]string{
				"priority":      "134",
				"facility":      "16",
				"facility_name": "local0",
				"severity":      "6",
				"severity_name": "info",
				"version":       "1",
				"hostname":      "server1",
				"appname":       "myapp",
				"procid":        "1234",
				"msgid":         "ID47",
				"message":       "User logged in",
			},
		},
		{
			name:  "with structured data",
			input: `<165>1 2024-03-01T12:00:00Z router1 - - - [exampleSDID@32473 iut="3" eventSource="Application"] Login failed`,
			expect: map[string]string{
				"priority":        "165",
				"facility":        "20",
				"facility_name":   "local4",
				"severity":        "5",
				"severity_name":   "notice",
				"hostname":        "router1",
				"structured_data": `[exampleSDID@32473 iut="3" eventSource="Application"]`,
				"message":         "Login failed",
			},
		},
		{
			name:  "nil fields use dash",
			input: `<13>1 2024-01-15T08:30:00Z - - - - - Simple message`,
			expect: map[string]string{
				"priority":      "13",
				"facility":      "1",
				"facility_name": "user",
				"severity":      "5",
				"severity_name": "notice",
				"message":       "Simple message",
			},
		},
		{
			name:  "emergency severity",
			input: `<0>1 2024-06-01T00:00:00Z kernel1 kernel - - - Kernel panic`,
			expect: map[string]string{
				"priority":      "0",
				"facility":      "0",
				"facility_name": "kern",
				"severity":      "0",
				"severity_name": "emergency",
				"hostname":      "kernel1",
				"appname":       "kernel",
				"message":       "Kernel panic",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := make(map[string]string)
			ok := tryParseRFC5424(tt.input, fields, nil)
			if !ok {
				t.Fatalf("expected RFC 5424 parse to succeed")
			}
			for k, want := range tt.expect {
				got, exists := fields[k]
				if !exists {
					t.Errorf("missing field %q, want %q", k, want)
				} else if got != want {
					t.Errorf("field %q = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestParseRFC3164(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect map[string]string
	}{
		{
			name:  "standard with tag and pid",
			input: `<34>Oct 11 22:14:15 mymachine sshd[1234]: Failed password for root`,
			expect: map[string]string{
				"priority":      "34",
				"facility":      "4",
				"facility_name": "auth",
				"severity":      "2",
				"severity_name": "critical",
				"hostname":      "mymachine",
				"appname":       "sshd",
				"procid":        "1234",
				"message":       "Failed password for root",
			},
		},
		{
			name:  "tag without pid",
			input: `<13>Feb  5 17:32:18 myhost su: pam_unix(su:session): session opened`,
			expect: map[string]string{
				"priority":      "13",
				"facility":      "1",
				"facility_name": "user",
				"severity":      "5",
				"severity_name": "notice",
				"hostname":      "myhost",
				"appname":       "su",
				"message":       "pam_unix(su:session): session opened",
			},
		},
		{
			name:  "single digit day",
			input: `<46>Mar  3 01:00:00 fw01 kernel: DROP IN=eth0 SRC=10.0.0.1`,
			expect: map[string]string{
				"facility_name": "syslog",
				"severity_name": "info",
				"hostname":      "fw01",
				"appname":       "kernel",
				"message":       "DROP IN=eth0 SRC=10.0.0.1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := make(map[string]string)
			ok := tryParseRFC3164(tt.input, fields, nil)
			if !ok {
				t.Fatalf("expected RFC 3164 parse to succeed")
			}
			for k, want := range tt.expect {
				got, exists := fields[k]
				if !exists {
					t.Errorf("missing field %q, want %q", k, want)
				} else if got != want {
					t.Errorf("field %q = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestParseSyslogLine_Fallback(t *testing.T) {
	// RFC 5424 first, then RFC 3164, then raw fallback
	t.Run("unparseable falls back to message", func(t *testing.T) {
		fields := make(map[string]string)
		parseSyslogLine("this is not syslog at all", fields, nil)
		if msg, ok := fields["message"]; !ok || msg != "this is not syslog at all" {
			t.Errorf("expected raw message fallback, got fields: %v", fields)
		}
	})

	t.Run("rfc5424 preferred over rfc3164", func(t *testing.T) {
		fields := make(map[string]string)
		parseSyslogLine(`<134>1 2024-02-28T10:15:30Z host app 123 - - Test`, fields, nil)
		if _, ok := fields["version"]; !ok {
			t.Error("expected RFC 5424 parse (version field present)")
		}
	})
}

func TestDecodePriority(t *testing.T) {
	tests := []struct {
		pri              int
		wantFacility     string
		wantSeverity     string
		wantFacilityName string
		wantSeverityName string
	}{
		{0, "0", "0", "kern", "emergency"},
		{134, "16", "6", "local0", "info"},
		{34, "4", "2", "auth", "critical"},
		{191, "23", "7", "local7", "debug"},
	}

	for _, tt := range tests {
		t.Run("pri_"+tt.wantFacilityName+"_"+tt.wantSeverityName, func(t *testing.T) {
			fields := make(map[string]string)
			decodePriority(tt.pri, fields, nil)
			if fields["facility"] != tt.wantFacility {
				t.Errorf("facility = %q, want %q", fields["facility"], tt.wantFacility)
			}
			if fields["severity"] != tt.wantSeverity {
				t.Errorf("severity = %q, want %q", fields["severity"], tt.wantSeverity)
			}
			if fields["facility_name"] != tt.wantFacilityName {
				t.Errorf("facility_name = %q, want %q", fields["facility_name"], tt.wantFacilityName)
			}
			if fields["severity_name"] != tt.wantSeverityName {
				t.Errorf("severity_name = %q, want %q", fields["severity_name"], tt.wantSeverityName)
			}
		})
	}
}

func TestExtractStructuredData(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantSD  string
		wantMsg string
	}{
		{
			name:    "no SD with dash",
			input:   "- Hello world",
			wantSD:  "",
			wantMsg: "Hello world",
		},
		{
			name:    "single SD element",
			input:   `[exampleSDID@32473 iut="3"] Message`,
			wantSD:  `[exampleSDID@32473 iut="3"]`,
			wantMsg: "Message",
		},
		{
			name:    "multiple SD elements",
			input:   `[id1@123 a="1"][id2@456 b="2"] The message`,
			wantSD:  `[id1@123 a="1"][id2@456 b="2"]`,
			wantMsg: "The message",
		},
		{
			name:    "SD with escaped quotes",
			input:   `[id@123 msg="say \"hi\""] Done`,
			wantSD:  `[id@123 msg="say \"hi\""]`,
			wantMsg: "Done",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sd, msg := extractStructuredData(tt.input)
			if sd != tt.wantSD {
				t.Errorf("sd = %q, want %q", sd, tt.wantSD)
			}
			if msg != tt.wantMsg {
				t.Errorf("msg = %q, want %q", msg, tt.wantMsg)
			}
		})
	}
}

func TestSyslogWithNormalizer(t *testing.T) {
	norm := &normalizers.CompiledNormalizer{
		Transforms: []normalizers.Transform{normalizers.TransformUppercase},
	}

	fields := make(map[string]string)
	parseSyslogLine(`<134>1 2024-02-28T10:15:30Z host app 123 - - Test message`, fields, norm)

	if _, ok := fields["HOSTNAME"]; !ok {
		t.Error("expected uppercase field names from normalizer")
	}
	if _, ok := fields["SEVERITY_NAME"]; !ok {
		t.Error("expected uppercase severity_name from normalizer")
	}
	if _, ok := fields["MESSAGE"]; !ok {
		t.Error("expected uppercase message field from normalizer")
	}
}

func TestParseRFC3164Timestamp(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"Oct 11 22:14:15", true},
		{"Feb  5 17:32:18", true},
		{"Mar  3 01:00:00", true},
		{"not a timestamp", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ts := parseRFC3164Timestamp(tt.input)
			if tt.valid && ts.IsZero() {
				t.Errorf("expected valid timestamp for %q", tt.input)
			}
			if !tt.valid && !ts.IsZero() {
				t.Errorf("expected zero timestamp for %q", tt.input)
			}
		})
	}
}

func TestParseRFC5424Timestamp(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"2024-02-28T10:15:30Z", true},
		{"2024-02-28T10:15:30.123456Z", true},
		{"2024-02-28T10:15:30+05:00", true},
		{"-", false},
		{"garbage", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ts := parseRFC5424Timestamp(tt.input)
			if tt.valid && ts.IsZero() {
				t.Errorf("expected valid timestamp for %q", tt.input)
			}
			if !tt.valid && !ts.IsZero() {
				t.Errorf("expected zero timestamp for %q", tt.input)
			}
		})
	}
}

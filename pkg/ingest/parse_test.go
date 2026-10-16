package ingest

import (
	"reflect"
	"strings"
	"testing"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
)

func TestParseKVLine(t *testing.T) {
	tests := []struct {
		name string
		line string
		want map[string]string
	}{
		{"plain", "a=1 b=two", map[string]string{"a": "1", "b": "two"}},
		{"double quotes keep spaces and equals", `msg="a = b c" n=1`, map[string]string{"msg": "a = b c", "n": "1"}},
		{"single quotes", `user='alice smith' ok=true`, map[string]string{"user": "alice smith", "ok": "true"}},
		{"other quote kind inside", `q="it's" r='say "hi"'`, map[string]string{"q": "it's", "r": `say "hi"`}},
		{"empty value mid-line", "a= b=2", map[string]string{"a": "", "b": "2"}},
		{"empty value at end", "a=1 b=", map[string]string{"a": "1", "b": ""}},
		{"empty quoted value", `a="" b=2`, map[string]string{"a": "", "b": "2"}},
		{"unterminated quote runs to end of line", `a="open ended b=2`, map[string]string{"a": "open ended b=2"}},
		{"bare words are skipped", "CEF hello a=1 world", map[string]string{"a": "1"}},
		{"duplicate key: last wins", "a=1 a=2", map[string]string{"a": "2"}},
		{"extra spaces", "   a=1    b=2   ", map[string]string{"a": "1", "b": "2"}},
		{"value keeps later equals", "url=http://x/?a=b&c=d", map[string]string{"url": "http://x/?a=b&c=d"}},
		{"dotted and dashed keys kept verbatim", "src.ip=10.0.0.1 dst-port=443", map[string]string{"src.ip": "10.0.0.1", "dst-port": "443"}},
		{"quote right after key closes value", `a="x"b=2`, map[string]string{"a": "x", "b": "2"}},
		{"empty line", "", map[string]string{}},
		// Known limitations, pinned so a change is deliberate:
		// backslash escapes are not interpreted, so an escaped quote ends the value;
		{"escaped quote ends the value", `msg="say \"hi\"" x=1`, map[string]string{"msg": `say \`, "x": "1"}},
		// only spaces separate pairs, so a tab stays inside the value;
		{"tab is not a separator", "a=1\tb=2", map[string]string{"a": "1\tb=2"}},
		// and a pair with no key is stored under the empty name.
		{"missing key", "=orphan a=1", map[string]string{"": "orphan", "a": "1"}},
	}
	h := &IngestHandler{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := map[string]string{}
			h.parseKVLine(tc.line, got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseKVLine(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestParseKVLogs(t *testing.T) {
	h := &IngestHandler{}
	norm := (&normalizers.Normalizer{
		Name:       "kvnorm",
		Version:    1,
		Transforms: []normalizers.Transform{normalizers.TransformLowercase},
	}).Compile()
	token := &ingesttokens.ValidatedToken{Normalizer: norm}

	body := "User=alice timestamp=2026-07-01T10:00:00Z\n\n   \nUser=bob\n"
	logs, err := h.parseKVLogs([]byte(body), token)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("parsed %d logs, want 2 (blank lines skipped)", len(logs))
	}
	if logs[0].RawLog != "User=alice timestamp=2026-07-01T10:00:00Z" {
		t.Errorf("RawLog = %q, want the line verbatim", logs[0].RawLog)
	}
	if logs[0].Fields["user"] != "alice" || logs[0].Normalizer != "kvnorm@1" {
		t.Errorf("normalizer not applied: %v %q", logs[0].Fields, logs[0].Normalizer)
	}
	if !logs[0].Timestamp.Equal(utc("2026-07-01T10:00:00Z")) {
		t.Errorf("Timestamp = %v, want the timestamp field", logs[0].Timestamp)
	}
	if !logs[1].Timestamp.Equal(logs[1].IngestTimestamp) {
		t.Errorf("line without a time should carry its ingest time")
	}
	if logs[0].LogID == logs[1].LogID || logs[0].LogID == "" {
		t.Errorf("log ids not distinct: %q %q", logs[0].LogID, logs[1].LogID)
	}

	if _, err := h.parseKVLogs([]byte("\n \n"), &ingesttokens.ValidatedToken{}); err == nil {
		t.Error("an all-blank body should be an error")
	}
	// A line with no pairs is still a log: the raw text is kept.
	logs, err = h.parseKVLogs([]byte("just some text"), &ingesttokens.ValidatedToken{})
	if err != nil || len(logs) != 1 || logs[0].RawLog != "just some text" || len(logs[0].Fields) != 0 {
		t.Errorf("free text line: %+v, %v", logs, err)
	}
}

func TestParseJSONLogsWithConfig(t *testing.T) {
	h := &IngestHandler{}
	tests := []struct {
		name    string
		body    string
		want    []string // value of field "n" per log
		wantErr bool
	}{
		{"single object", `{"n":"1"}`, []string{"1"}, false},
		{"pretty printed object", "{\n  \"n\": \"1\"\n}\n", []string{"1"}, false},
		{"array", `[{"n":"1"},{"n":"2"}]`, []string{"1", "2"}, false},
		{"pretty printed array", "[\n {\"n\":\"1\"},\n {\"n\":\"2\"}\n]", []string{"1", "2"}, false},
		{"ndjson", "{\"n\":\"1\"}\n{\"n\":\"2\"}\n{\"n\":\"3\"}", []string{"1", "2", "3"}, false},
		{"ndjson skips bad lines", "{\"n\":\"1\"}\nnot json\n[1,2]\n{\"n\":\"4\"}", []string{"1", "4"}, false},
		{"ndjson all bad", "nope\nstill nope", nil, true},
		{"concatenated objects on one line", `{"n":"1"}{"n":"2"}`, nil, true},
		{"scalar", `42`, nil, true},
		{"empty array", `[]`, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs, err := h.parseJSONLogsWithConfig([]byte(tc.body), nil, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(logs) != len(tc.want) {
				t.Fatalf("parsed %d logs, want %d", len(logs), len(tc.want))
			}
			for i, n := range tc.want {
				if logs[i].Fields["n"] != n {
					t.Errorf("log %d n = %q, want %q", i, logs[i].Fields["n"], n)
				}
			}
		})
	}
}

// TestLineScannerLimit: the limit is per line, and a line exactly at it is fine.
func TestLineScannerLimit(t *testing.T) {
	ok := strings.Repeat("a", maxLineBytes-1) + "\nb"
	s := newLineScanner([]byte(ok))
	n := 0
	for s.Scan() {
		n++
	}
	if err := lineScanErr(s); err != nil || n != 2 {
		t.Fatalf("lines=%d err=%v, want 2 lines and no error", n, err)
	}
	s = newLineScanner([]byte(strings.Repeat("a", maxLineBytes+1)))
	for s.Scan() {
	}
	if err := lineScanErr(s); err != errLineTooLong {
		t.Fatalf("err = %v, want errLineTooLong", err)
	}
}

func FuzzParseKVLine(f *testing.F) {
	for _, s := range []string{`a=1 b="x y" c='z'`, `="`, `a="`, "a= =b", `k=v\"`} {
		f.Add(s)
	}
	h := &IngestHandler{}
	f.Fuzz(func(t *testing.T, line string) {
		fields := map[string]string{}
		h.parseKVLine(line, fields)
		for k, v := range fields {
			// Keys and values are always slices of the line, never invented text.
			if !strings.Contains(line, k) || !strings.Contains(line, v) {
				t.Fatalf("pair %q=%q not taken from %q", k, v, line)
			}
			if strings.Contains(k, " ") {
				t.Fatalf("key %q contains a space", k)
			}
		}
	})
}

func FuzzParseJSONLogs(f *testing.F) {
	for _, s := range []string{`{"a":1}`, `[{"a":{"b":[1,2]}}]`, "{\"a\":1}\n{\"b\":2}", `[null]`, `{"timestamp":"2026-01-01T00:00:00Z"}`} {
		f.Add([]byte(s))
	}
	h := &IngestHandler{}
	f.Fuzz(func(t *testing.T, body []byte) {
		logs, err := h.parseJSONLogsWithConfig(body, nil, nil)
		if err != nil {
			return
		}
		for _, l := range logs {
			if l.Timestamp.IsZero() || l.IngestTimestamp.IsZero() || l.LogID == "" {
				t.Fatalf("entry not ready to insert: %+v", l)
			}
			if l.Timestamp.After(maxStorableTime) || l.Timestamp.Before(minStorableTime) {
				t.Fatalf("unstorable timestamp %v", l.Timestamp)
			}
		}
	})
}

func FuzzParseSyslogLine(f *testing.F) {
	for _, s := range []string{
		`<134>1 2024-02-28T10:15:30.123Z server1 myapp 1234 ID47 - User logged in`,
		`<165>1 2024-03-01T12:00:00Z r - - - [x@1 a="]\"" b="2"][y@2] msg`,
		`<13>Feb  5 17:32:18 host sshd[123]: Accepted`,
		`<999>1 - - - - - -`,
		`[`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		fields := map[string]string{}
		parseSyslogLine(line, fields)
		if len(fields) == 0 {
			t.Fatalf("no fields from %q: every line must at least keep its message", line)
		}
	})
}

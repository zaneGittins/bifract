package ingest

import (
	"encoding/json"
	"testing"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestExtractTimestampPrecedence pins which configured field wins. Token fields
// replace both the normalizer's and the global list (they are never merged), a
// normalizer's fields replace the global list, and only the built-in common names
// are tried after any configured list misses.
func TestExtractTimestampPrecedence(t *testing.T) {
	normWith := func(fields ...normalizers.TimestampField) *normalizers.CompiledNormalizer {
		return &normalizers.CompiledNormalizer{Name: "n", TimestampFields: fields}
	}
	tok := func(field, format string) ingesttokens.TsField {
		return ingesttokens.TsField{Field: field, Format: format}
	}

	tests := []struct {
		name   string
		fields map[string]string
		tok    []ingesttokens.TsField
		norm   *normalizers.CompiledNormalizer
		want   time.Time // zero means "not found"
	}{
		{
			name:   "token field beats a common field name",
			fields: map[string]string{"event_time": "1791581172", "timestamp": "2020-01-01T00:00:00Z"},
			tok:    []ingesttokens.TsField{tok("event_time", "unix")},
			want:   time.Unix(1791581172, 0),
		},
		{
			name:   "second token field when the first is unparseable",
			fields: map[string]string{"a": "garbage", "b": "1791581172000"},
			tok:    []ingesttokens.TsField{tok("a", "unix"), tok("b", "unixms")},
			want:   time.Unix(1791581172, 0),
		},
		{
			name:   "token fields suppress the normalizer's",
			fields: map[string]string{"norm_ts": "1791581172", "tok_ts": "nope"},
			tok:    []ingesttokens.TsField{tok("tok_ts", "unix")},
			norm:   normWith(normalizers.TimestampField{Field: "norm_ts", Format: "unix"}),
			want:   time.Time{},
		},
		{
			name:   "token miss still falls back to common names",
			fields: map[string]string{"@timestamp": "2026-03-04T05:06:07.123Z"},
			tok:    []ingesttokens.TsField{tok("absent", "unix")},
			want:   utc("2026-03-04T05:06:07.123Z"),
		},
		{
			name:   "normalizer field used without token fields",
			fields: map[string]string{"UtcTime": "2026-08-29 17:42:05.002", "timestamp": "2020-01-01T00:00:00Z"},
			norm:   normWith(normalizers.TimestampField{Field: "UtcTime", Format: "2006-01-02 15:04:05.000"}),
			want:   utc("2026-08-29T17:42:05.002Z"),
		},
		{
			// system_time is only in the global list, which a normalizer's list replaces.
			name:   "normalizer fields suppress the global list",
			fields: map[string]string{"system_time": "2026-01-01T00:00:00Z"},
			norm:   normWith(normalizers.TimestampField{Field: "absent", Format: time.RFC3339}),
			want:   time.Time{},
		},
		{
			name:   "global list when neither token nor normalizer configures one",
			fields: map[string]string{"system_time": "2026-01-01T10:00:00.123456789Z"},
			norm:   normWith(),
			want:   utc("2026-01-01T10:00:00.123456789Z"),
		},
		{
			name:   "empty value is skipped",
			fields: map[string]string{"timestamp": "", "time": "2026-02-02T02:02:02Z"},
			want:   utc("2026-02-02T02:02:02Z"),
		},
		{
			name:   "ts with a zone-less layout reads as UTC",
			fields: map[string]string{"ts": "2026-05-06 07:08:09"},
			want:   utc("2026-05-06T07:08:09Z"),
		},
		{
			name:   "_time with an offset keeps the instant",
			fields: map[string]string{"_time": "2026-05-06T07:08:09+02:00"},
			want:   utc("2026-05-06T05:08:09Z"),
		},
		{
			name:   "space-separated with millis and offset",
			fields: map[string]string{"time": "2026-05-06 07:08:09.250 -05:00"},
			want:   utc("2026-05-06T12:08:09.25Z"),
		},
		{
			// Bare numbers are not auto-detected: the unit is ambiguous, so an epoch
			// field needs an explicit format on the token or normalizer.
			name:   "bare epoch in a common field is not guessed",
			fields: map[string]string{"timestamp": "1791581172"},
			want:   time.Time{},
		},
		{
			name:   "unrecognised layout",
			fields: map[string]string{"timestamp": "05/06/2026 07:08"},
			want:   time.Time{},
		},
		{
			name:   "nothing timestamp-like",
			fields: map[string]string{"msg": "hello"},
			want:   time.Time{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractTimestamp(tc.fields, tc.tok, tc.norm)
			if !got.Equal(tc.want) {
				t.Fatalf("got %v, want %v", got.UTC(), tc.want.UTC())
			}
		})
	}
}

// TestTimestampOutsideStorableRange: an event time the write path cannot encode
// must not be stored. The driver, log_id and the spool codec all go through
// UnixNano, so a time past 2262-04-11 would wrap to a wrong date, and the
// DateTime64 column starts at 1900. Such a value is treated as absent.
func TestTimestampOutsideStorableRange(t *testing.T) {
	rejected := []struct{ val, format string }{
		{"9999-12-31T00:00:00Z", time.RFC3339},
		{"2262-04-12T00:00:00Z", time.RFC3339},
		{"1899-12-31T23:59:59Z", time.RFC3339},
		{"0001-01-01T00:00:00Z", time.RFC3339},
		// Within ClickHouse's 2299 bound but past what UnixNano can hold.
		{"10000000000", "unix"},
		{"-2208988801", "unix"},
		// Seconds in range but the fraction carries it past the last nanosecond.
		{"9223372036.900000000", "unix"},
	}
	for _, c := range rejected {
		if got := parseTimestampWithFormat(c.val, c.format); !got.IsZero() {
			t.Errorf("%s %q: got %v, want it rejected", c.format, c.val, got.UTC())
		}
	}
	for _, v := range []string{"9999-12-31T00:00:00Z", "1800-06-01 00:00:00"} {
		if got := parseTimestamp(v); !got.IsZero() {
			t.Errorf("fallback %q: got %v, want it rejected", v, got.UTC())
		}
	}

	accepted := []struct {
		val, format string
		want        time.Time
	}{
		{"1900-01-01T00:00:00Z", time.RFC3339, utc("1900-01-01T00:00:00Z")},
		{"2262-04-11T23:47:16Z", time.RFC3339, utc("2262-04-11T23:47:16Z")},
		{"-2208988800", "unix", utc("1900-01-01T00:00:00Z")},
	}
	for _, c := range accepted {
		if got := parseTimestampWithFormat(c.val, c.format); !got.Equal(c.want) {
			t.Errorf("%s %q: got %v, want %v", c.format, c.val, got.UTC(), c.want)
		}
	}

	// End to end: the out-of-range value is skipped in favor of the next candidate.
	got := ExtractTimestamp(map[string]string{
		"timestamp": "9999-12-31T00:00:00Z",
		"time":      "2026-01-01T00:00:00Z",
	}, nil, nil)
	if !got.Equal(utc("2026-01-01T00:00:00Z")) {
		t.Fatalf("got %v, want the next in-range field", got.UTC())
	}
}

func TestParseTimestampWithFormatUnitAliases(t *testing.T) {
	want := time.Unix(1791581172, 0)
	for _, c := range []struct{ val, format string }{
		{"1791581172", "unix"},
		{"1791581172000", "unixmilli"},
		{"1791581172000", "unixmillis"},
		{"1791581172000", "unixms"},
		{"1791581172000000", "unixmicro"},
		{"1791581172000000", "unixmicros"},
		{"1791581172000000", "unixμs"},
		{"1791581172000000000", "unixnano"},
		{"1791581172000000000", "unixnanos"},
		{"1791581172000000000", "unixns"},
	} {
		if got := parseTimestampWithFormat(c.val, c.format); !got.Equal(want) {
			t.Errorf("%s %q = %v, want %v", c.format, c.val, got, want)
		}
	}
	// A layout that does not match is "not found", never a partial parse.
	if got := parseTimestampWithFormat("2026-01-01", time.RFC3339); !got.IsZero() {
		t.Errorf("mismatched layout returned %v", got)
	}
}

// TestBuildLogEntry pins the raw-object to LogEntry path shared by every JSON
// ingest endpoint and the rule tester.
func TestBuildLogEntry(t *testing.T) {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(`{
		"EventID": 4624,
		"TargetUser": "alice",
		"Elevated": true,
		"Nothing": null,
		"Ratio": 0.5,
		"Big": 1000000,
		"Tags": ["a","b"],
		"Process": {"Image": "C:\\cmd.exe"},
		"timestamp": "2026-07-01T12:00:00.5Z"
	}`), &obj); err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	e, err := BuildLogEntry(obj, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"EventID":    "4624",
		"TargetUser": "alice",
		"Elevated":   "true",
		"Nothing":    "",
		"Ratio":      "0.5",
		"Big":        "1000000", // never "1e+06": equality search must match what was sent
		"Tags":       `["a","b"]`,
		"Process":    `{"Image":"C:\\cmd.exe"}`,
		"timestamp":  "2026-07-01T12:00:00.5Z",
	}
	for k, v := range want {
		if e.Fields[k] != v {
			t.Errorf("Fields[%q] = %q, want %q", k, e.Fields[k], v)
		}
	}
	if !e.Timestamp.Equal(utc("2026-07-01T12:00:00.5Z")) {
		t.Errorf("Timestamp = %v", e.Timestamp)
	}
	if e.IngestTimestamp.Before(before) || e.IngestTimestamp.After(time.Now()) {
		t.Errorf("IngestTimestamp = %v, want the build time", e.IngestTimestamp)
	}
	if e.Normalizer != "" {
		t.Errorf("Normalizer stamp = %q with no normalizer", e.Normalizer)
	}
	if e.LogID != storage.GenerateLogID(e.Timestamp, e.RawLog) {
		t.Errorf("LogID is not derived from (timestamp, raw log)")
	}
	// RawLog is the re-encoded object, so it must decode back to the same values.
	var back map[string]interface{}
	if err := json.Unmarshal([]byte(e.RawLog), &back); err != nil {
		t.Fatalf("RawLog is not JSON: %v", err)
	}
	if back["TargetUser"] != "alice" || back["EventID"] != float64(4624) {
		t.Errorf("RawLog lost values: %s", e.RawLog)
	}
}

// TestBuildLogEntryLogIDIsStable: log_id is how a retried or spool-restored event
// is recognised as the same event, so it depends only on the event, not on when
// it was received.
func TestBuildLogEntryLogIDIsStable(t *testing.T) {
	obj := func(ts string) map[string]interface{} {
		return map[string]interface{}{"msg": "x", "timestamp": ts}
	}
	a, _ := BuildLogEntry(obj("2026-07-01T00:00:00Z"), nil, nil)
	time.Sleep(2 * time.Millisecond)
	b, _ := BuildLogEntry(obj("2026-07-01T00:00:00Z"), nil, nil)
	if a.LogID != b.LogID {
		t.Fatalf("same event got two log ids: %s vs %s", a.LogID, b.LogID)
	}
	if a.IngestTimestamp.Equal(b.IngestTimestamp) {
		t.Fatal("setup: ingest times should differ")
	}
	c, _ := BuildLogEntry(obj("2026-07-01T00:00:01Z"), nil, nil)
	if c.LogID == a.LogID {
		t.Fatal("different events share a log id")
	}
}

// TestBuildLogEntryFallsBackToIngestTime: with no usable event time the entry
// carries its ingest time in both columns, never a zero timestamp.
func TestBuildLogEntryFallsBackToIngestTime(t *testing.T) {
	for _, obj := range []map[string]interface{}{
		{"msg": "no time at all"},
		{"timestamp": "not a time"},
		{"timestamp": "9999-01-01T00:00:00Z"},
		{},
	} {
		e, err := BuildLogEntry(obj, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if e.Timestamp.IsZero() || !e.Timestamp.Equal(e.IngestTimestamp) {
			t.Errorf("%v: Timestamp %v, IngestTimestamp %v; want both the ingest time", obj, e.Timestamp, e.IngestTimestamp)
		}
	}
}

// TestBuildLogEntryAppliesNormalizer: transforms run before timestamp extraction,
// so configured timestamp fields name the normalized field.
func TestBuildLogEntryAppliesNormalizer(t *testing.T) {
	norm := (&normalizers.Normalizer{
		Name:          "win",
		Version:       4,
		Transforms:    []normalizers.Transform{normalizers.TransformSnakeCase},
		FieldMappings: []normalizers.FieldMapping{{Sources: []string{"target_user"}, Target: "user"}},
		TimestampFields: []normalizers.TimestampField{
			{Field: "event_time", Format: "unix"},
		},
	}).Compile()

	e, err := BuildLogEntry(map[string]interface{}{
		"TargetUser": "alice",
		"EventTime":  "1791581172",
	}, norm, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Fields["user"] != "alice" || e.Fields["event_time"] != "1791581172" {
		t.Errorf("Fields = %v, want snake_cased and mapped names", e.Fields)
	}
	if _, raw := e.Fields["TargetUser"]; raw {
		t.Errorf("pre-normalization name survived: %v", e.Fields)
	}
	if e.Normalizer != "win@4" {
		t.Errorf("Normalizer stamp = %q, want win@4", e.Normalizer)
	}
	if !e.Timestamp.Equal(time.Unix(1791581172, 0)) {
		t.Errorf("Timestamp = %v, want the normalized event_time", e.Timestamp)
	}
	// The raw log keeps the original names.
	if want := `{"EventTime":"1791581172","TargetUser":"alice"}`; e.RawLog != want {
		t.Errorf("RawLog = %s, want %s", e.RawLog, want)
	}

	// A token field naming the pre-normalization key does not match.
	e, _ = BuildLogEntry(map[string]interface{}{"EventTime": "1791581172"}, norm,
		[]ingesttokens.TsField{{Field: "EventTime", Format: "unix"}})
	if !e.Timestamp.Equal(e.IngestTimestamp) {
		t.Errorf("token field matched a pre-normalization name: %v", e.Timestamp)
	}
}

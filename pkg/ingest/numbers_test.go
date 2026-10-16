package ingest

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"bifract/pkg/ingesttokens"
)

// 9007199254740993 is 2^53+1, the first integer a float64 cannot hold; it decodes
// to ...992. 12345678901234567890 fits a UInt64 but not an Int64 or a float64.
const bigNumbersLog = `{"id": 9007199254740993, "ratio": 0.1, "big": 12345678901234567890, ` +
	`"Proc": {"Pid": 9007199254740995}, "list": [9007199254740997, 0.1], "timestamp": "2026-07-01T00:00:00Z"}`

// A 64-bit ID must reach the fields, the raw log and a normalizer's rename and value
// map with its exact digits, through the whole ingest path.
func TestHandleIngestKeepsBigNumbersExact(t *testing.T) {
	f := newFixture(t, tokenRow{
		normalizer:    `["flatten_leaf"]`,
		normName:      "big",
		normVersion:   1,
		normMappings:  `[{"sources":["id"],"target":"event_id"}]`,
		normValueMaps: `[{"from_field":"event_id","to_field":"known","map":{"9007199254740993":"yes"},"default":"no"}]`,
	})
	w, _ := do(f.h.HandleIngest, http.MethodPost, bigNumbersLog+"\n"+bigNumbersLog+"\n")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	logs := f.q.queued()
	if len(logs) != 2 {
		t.Fatalf("queued %d logs, want 2", len(logs))
	}
	want := map[string]string{
		"event_id": "9007199254740993",
		"known":    "yes",
		"ratio":    "0.1",
		"big":      "12345678901234567890",
		"Pid":      "9007199254740995",
		"list":     "[9007199254740997,0.1]",
	}
	for k, v := range want {
		if got := logs[0].Fields[k]; got != v {
			t.Errorf("Fields[%q] = %q, want %q", k, got, v)
		}
	}
	for _, digits := range []string{`"id":9007199254740993`, `"big":12345678901234567890`, `"Pid":9007199254740995`, `[9007199254740997,0.1]`, `"ratio":0.1`} {
		if !strings.Contains(logs[0].RawLog, digits) {
			t.Errorf("RawLog lost %s: %s", digits, logs[0].RawLog)
		}
	}
	if logs[0].LogID != logs[1].LogID {
		t.Errorf("the same event got two log ids")
	}
}

// Without a normalizer, an unflattened nested object and an array keep their exact
// digits in their serialized field values.
func TestHandleIngestKeepsBigNumbersExactWithoutNormalizer(t *testing.T) {
	for name, body := range map[string]string{
		"object": bigNumbersLog,
		"array":  "[" + bigNumbersLog + "]",
		"ndjson": bigNumbersLog + "\n",
	} {
		f := newFixture(t, tokenRow{})
		w, _ := do(f.h.HandleIngest, http.MethodPost, body)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", name, w.Code, w.Body)
		}
		logs := f.q.queued()
		if len(logs) != 1 {
			t.Fatalf("%s: queued %d logs, want 1", name, len(logs))
		}
		want := map[string]string{
			"id":    "9007199254740993",
			"big":   "12345678901234567890",
			"Proc":  `{"Pid":9007199254740995}`,
			"list":  "[9007199254740997,0.1]",
			"ratio": "0.1",
		}
		for k, v := range want {
			if got := logs[0].Fields[k]; got != v {
				t.Errorf("%s: Fields[%q] = %q, want %q", name, k, got, v)
			}
		}
		if !strings.Contains(logs[0].RawLog, `"id":9007199254740993`) {
			t.Errorf("%s: RawLog lost the id: %s", name, logs[0].RawLog)
		}
	}
}

// Field values keep the plain decimal form numbers have always been stored in (so
// value maps and equality search keep matching), while the raw log keeps the number
// exactly as it was sent.
func TestBuildLogEntryNumberForms(t *testing.T) {
	logs, err := (&IngestHandler{}).parseJSONLogsWithConfig([]byte(
		`{"exp": 1.5e3, "trail": 2.50, "whole": 5.0, "negz": -0.0, "tiny": 1E-7, "neg": -12345678901234567891, "pi": 3.14159265358979323846}`), nil, nil)
	if err != nil || len(logs) != 1 {
		t.Fatalf("logs=%d err=%v", len(logs), err)
	}
	want := map[string]string{
		"exp":   "1500",
		"trail": "2.5",
		"whole": "5",
		"negz":  "-0",
		"tiny":  "0.0000001",
		"neg":   "-12345678901234567891",
		"pi":    "3.14159265358979323846",
	}
	for k, v := range want {
		if got := logs[0].Fields[k]; got != v {
			t.Errorf("Fields[%q] = %q, want %q", k, got, v)
		}
	}
	for _, sent := range []string{`"exp":1.5e3`, `"trail":2.50`, `"whole":5.0`, `"pi":3.14159265358979323846`} {
		if !strings.Contains(logs[0].RawLog, sent) {
			t.Errorf("RawLog does not keep %s as sent: %s", sent, logs[0].RawLog)
		}
	}
}

// An epoch count past 2^53 (any nanosecond timestamp) must resolve to the exact
// instant, not one rounded through a float64.
func TestBuildLogEntryEpochNanosExact(t *testing.T) {
	logs, err := (&IngestHandler{}).parseJSONLogsWithConfig([]byte(`{"ts": 1791581172686968123, "ms": 1.791581172686e12}`), nil,
		[]ingesttokens.TsField{{Field: "ts", Format: "unixnano"}})
	if err != nil || len(logs) != 1 {
		t.Fatalf("logs=%d err=%v", len(logs), err)
	}
	if want := time.Unix(1791581172, 686968123); !logs[0].Timestamp.Equal(want) {
		t.Errorf("Timestamp = %v, want %v", logs[0].Timestamp.UTC(), want.UTC())
	}

	logs, err = (&IngestHandler{}).parseJSONLogsWithConfig([]byte(`{"ms": 1.791581172686e12}`), nil,
		[]ingesttokens.TsField{{Field: "ms", Format: "unixmilli"}})
	if err != nil || len(logs) != 1 {
		t.Fatalf("logs=%d err=%v", len(logs), err)
	}
	if want := time.UnixMilli(1791581172686); !logs[0].Timestamp.Equal(want) {
		t.Errorf("exponent-form millis: Timestamp = %v, want %v", logs[0].Timestamp.UTC(), want.UTC())
	}
}

// Two objects on one line are neither one object nor one NDJSON line, so the body is
// rejected rather than silently keeping only the first.
func TestParseJSONLogsRejectsTrailingData(t *testing.T) {
	h := &IngestHandler{}
	for _, body := range []string{`{"a":1} {"b":2}`, `{"a":1}}`, `[{"a":1}] x`} {
		if logs, err := h.parseJSONLogsWithConfig([]byte(body), nil, nil); err == nil {
			t.Errorf("%q: accepted %d logs, want an error", body, len(logs))
		}
	}
}

func TestParseBulkRequestKeepsBigNumbersExact(t *testing.T) {
	h := &ElasticBulkHandler{handler: &IngestHandler{}}
	logs, _, err := h.parseBulkRequest([]byte(bulkBody(`{"index":{}}`, bigNumbersLog)), nil, nil)
	if err != nil || len(logs) != 1 {
		t.Fatalf("logs=%d err=%v", len(logs), err)
	}
	if got := logs[0].Fields["id"]; got != "9007199254740993" {
		t.Errorf("id = %q", got)
	}
	if !strings.Contains(logs[0].RawLog, `"big":12345678901234567890`) {
		t.Errorf("RawLog lost digits: %s", logs[0].RawLog)
	}
}

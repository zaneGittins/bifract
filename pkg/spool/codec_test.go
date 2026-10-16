package spool

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/pkg/storage"
)

// TestCodecRoundTripPreservesEveryColumn: the spool is the archive's only copy
// between ack and Iceberg commit, so every column the archive writes must survive
// encoding exactly, including the normalizer stamp and sub-second timestamps.
func TestCodecRoundTripPreservesEveryColumn(t *testing.T) {
	in := []storage.LogEntry{
		{
			Timestamp:       time.Date(2026, 7, 1, 12, 30, 45, 123456789, time.UTC),
			IngestTimestamp: time.Date(2026, 7, 2, 0, 0, 0, 1, time.UTC),
			LogID:           "abc123",
			FractalID:       "frac-1",
			RawLog:          `{"msg":"héllo \u0000 wörld","n":1}`,
			Normalizer:      "windows@3",
			Fields:          map[string]string{"msg": "héllo", "": "empty key", "multi\nline": "v\x00v"},
		},
		// No fields at all, and zero-length strings everywhere.
		{
			Timestamp:       time.Unix(0, 0).UTC(),
			IngestTimestamp: time.Unix(1, 0).UTC(),
		},
		// Pre-1970 event time.
		{
			Timestamp:       time.Date(1950, 1, 1, 0, 0, 0, 5, time.UTC),
			IngestTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			RawLog:          strings.Repeat("x", 70000),
			Fields:          map[string]string{"k": strings.Repeat("y", 70000)},
		},
	}
	out, err := decodeBatch(encodeBatch(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("decoded %d entries, want %d", len(out), len(in))
	}
	for i := range in {
		if !out[i].Timestamp.Equal(in[i].Timestamp) || !out[i].IngestTimestamp.Equal(in[i].IngestTimestamp) {
			t.Errorf("entry %d timestamps = %v / %v, want %v / %v", i,
				out[i].Timestamp, out[i].IngestTimestamp, in[i].Timestamp, in[i].IngestTimestamp)
		}
		if out[i].Timestamp.Location() != time.UTC {
			t.Errorf("entry %d decoded in %v, want UTC", i, out[i].Timestamp.Location())
		}
		if out[i].LogID != in[i].LogID || out[i].FractalID != in[i].FractalID ||
			out[i].RawLog != in[i].RawLog || out[i].Normalizer != in[i].Normalizer {
			t.Errorf("entry %d string columns differ: got %+v", i, out[i])
		}
		if len(in[i].Fields) == 0 {
			if len(out[i].Fields) != 0 {
				t.Errorf("entry %d grew fields %v", i, out[i].Fields)
			}
		} else if !reflect.DeepEqual(out[i].Fields, in[i].Fields) {
			t.Errorf("entry %d fields = %v, want %v", i, out[i].Fields, in[i].Fields)
		}
	}
}

// TestCodecDecodesVersion1: frames written before the normalizer stamp existed
// must still drain after an upgrade, with an empty stamp.
func TestCodecDecodesVersion1(t *testing.T) {
	var b []byte
	b = append(b, 1)
	b = appendUint32(b, 1)
	b = appendInt64(b, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano())
	b = appendInt64(b, time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC).UnixNano())
	b = appendString(b, "id1")
	b = appendString(b, "frac")
	b = appendString(b, "raw")
	b = appendUint32(b, 1)
	b = appendString(b, "user")
	b = appendString(b, "alice")

	out, err := decodeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("decoded %d entries, want 1", len(out))
	}
	e := out[0]
	if e.LogID != "id1" || e.FractalID != "frac" || e.RawLog != "raw" || e.Normalizer != "" || e.Fields["user"] != "alice" {
		t.Fatalf("v1 decode = %+v", e)
	}
}

func TestCodecRejectsUnknownVersion(t *testing.T) {
	for _, ver := range []byte{0, codecVersion + 1} {
		payload := encodeBatch(sampleLogs(1, 0))
		payload[0] = ver
		if _, err := decodeBatch(payload); err == nil || !strings.Contains(err.Error(), "codec version") {
			t.Errorf("version %d: err = %v, want unsupported codec version", ver, err)
		}
	}
}

// TestCodecTruncatedPayloadErrors: every strict prefix of a valid payload is an
// error, never a panic and never a silently shorter batch.
func TestCodecTruncatedPayloadErrors(t *testing.T) {
	payload := encodeBatch(sampleLogs(3, 0))
	for n := 0; n < len(payload); n++ {
		if logs, err := decodeBatch(payload[:n]); err == nil {
			t.Fatalf("prefix of %d/%d bytes decoded to %d entries without error", n, len(payload), len(logs))
		}
	}
}

// TestCodecRejectsImpossibleCount: a corrupt record count must be refused before
// it drives a huge allocation.
func TestCodecRejectsImpossibleCount(t *testing.T) {
	var b []byte
	b = append(b, codecVersion)
	b = appendUint32(b, 0xffffffff)
	if _, err := decodeBatch(b); err == nil || !strings.Contains(err.Error(), "exceeds payload capacity") {
		t.Fatalf("err = %v, want a capacity error", err)
	}
}

// FuzzDecodeBatch: arbitrary bytes behind a valid CRC (a misaligned read, a bug in
// an older writer) must never panic or allocate without bound.
func FuzzDecodeBatch(f *testing.F) {
	f.Add(encodeBatch(sampleLogs(2, 0)))
	f.Add([]byte{1, 1, 0, 0, 0})
	f.Add([]byte{codecVersion, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, b []byte) {
		logs, err := decodeBatch(b)
		if err != nil {
			return
		}
		// Whatever decodes must re-encode to something that decodes the same.
		again, err := decodeBatch(encodeBatch(logs))
		if err != nil || len(again) != len(logs) {
			t.Fatalf("re-decode: %d entries, err %v; want %d", len(again), err, len(logs))
		}
	})
}

// TestCodecRejectsImpossibleFieldCount: like the record count, a corrupt field
// count must be refused before it sizes an allocation (found by FuzzDecodeBatch:
// a field count of 0xffffffff exhausted memory).
func TestCodecRejectsImpossibleFieldCount(t *testing.T) {
	var b []byte
	b = append(b, codecVersion)
	b = appendUint32(b, 1)
	b = appendInt64(b, 0)
	b = appendInt64(b, 0)
	for i := 0; i < 4; i++ { // log id, fractal, raw log, normalizer
		b = appendString(b, "")
	}
	b = appendUint32(b, 0xffffffff)
	b = append(b, make([]byte, 16)...)
	if _, err := decodeBatch(b); err == nil || !strings.Contains(err.Error(), "field count") {
		t.Fatalf("err = %v, want a field count error", err)
	}
}

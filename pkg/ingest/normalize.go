package ingest

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

// This file holds the raw-object -> LogEntry path, deliberately free of any HTTP or
// handler state. The rule tester (internal/ruletest) calls BuildLogEntry directly so a
// test log is normalized by exactly the same code that normalizes an ingested one; any
// divergence here would make a passing detection test meaningless.

// BuildLogEntry converts a decoded JSON log object into a LogEntry, applying the
// normalizer's transforms and resolving the event timestamp. IngestTimestamp and the
// derived log_id are stamped here, so callers get an entry ready to insert.
func BuildLogEntry(obj map[string]interface{}, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) (storage.LogEntry, error) {
	entry := storage.LogEntry{}

	rawBytes, err := json.Marshal(obj)
	if err != nil {
		return entry, fmt.Errorf("failed to marshal raw log: %w", err)
	}
	entry.RawLog = string(rawBytes)

	// Build flat fields without any structural transforms.
	built := normalizers.BuildFieldsWithNested(obj)
	entry.Fields = built.Fields

	// Apply normalizer transforms (flatten, snake_case, lowercase, etc.)
	if norm != nil {
		entry.Fields = norm.ApplyTransformsWithNested(entry.Fields, built.NestedKeys)
	}
	entry.Normalizer = norm.Stamp()

	ingestTime := time.Now()
	entry.Timestamp = ExtractTimestamp(entry.Fields, tsFields, norm)

	if entry.Timestamp.IsZero() {
		entry.Timestamp = ingestTime
	}

	entry.IngestTimestamp = ingestTime
	entry.LogID = storage.GenerateLogID(entry.Timestamp, entry.RawLog)

	return entry, nil
}

// ExtractTimestamp tries per-token fields, then normalizer fields, then global settings, then common field names.
func ExtractTimestamp(fields map[string]string, tsFields []ingesttokens.TsField, norm *normalizers.CompiledNormalizer) time.Time {
	// Try per-token configured timestamp fields first
	for _, tsField := range tsFields {
		if val, ok := fields[tsField.Field]; ok && val != "" {
			if ts := parseTimestampWithFormat(val, tsField.Format); !ts.IsZero() {
				return ts
			}
		}
	}

	// Try normalizer's timestamp fields
	if len(tsFields) == 0 && norm != nil && len(norm.TimestampFields) > 0 {
		for _, tsField := range norm.TimestampFields {
			if val, ok := fields[tsField.Field]; ok && val != "" {
				if ts := parseTimestampWithFormat(val, tsField.Format); !ts.IsZero() {
					return ts
				}
			}
		}
	}

	// Fall back to global settings if neither token nor normalizer had fields
	if len(tsFields) == 0 && (norm == nil || len(norm.TimestampFields) == 0) {
		globalTsFields := settings.Get().TimestampFields
		for _, tsField := range globalTsFields {
			if val, ok := fields[tsField.Field]; ok && val != "" {
				if ts := parseTimestampWithFormat(val, tsField.Format); !ts.IsZero() {
					return ts
				}
			}
		}
	}

	// Last resort: try common field names with auto-detection
	fallbackFields := []string{"timestamp", "@timestamp", "time", "ts", "_time"}
	for _, field := range fallbackFields {
		if val, ok := fields[field]; ok && val != "" {
			if ts := parseTimestamp(val); !ts.IsZero() {
				return ts
			}
		}
	}

	return time.Time{}
}

func parseTimestampWithFormat(val, format string) time.Time {
	var unit time.Duration
	switch format {
	case "unix":
		unit = time.Second
	case "unixmilli", "unixmillis", "unixms":
		unit = time.Millisecond
	case "unixmicro", "unixmicros", "unixμs":
		unit = time.Microsecond
	case "unixnano", "unixnanos", "unixns":
		unit = time.Nanosecond
	default:
		t, _ := time.Parse(format, val)
		return t
	}
	t, _ := parseEpoch(val, unit)
	return t
}

// parseEpoch parses an integer or decimal count of unit since the epoch. The
// fraction is read as digits rather than through a float so sub-second precision
// survives exactly (Velociraptor emits 1791581172.686968).
func parseEpoch(val string, unit time.Duration) (time.Time, bool) {
	intPart, fracPart, hasFrac := strings.Cut(strings.TrimSpace(val), ".")
	whole, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	var fracNanos int64
	if hasFrac {
		if fracPart == "" {
			return time.Time{}, false
		}
		for i := 0; i < len(fracPart); i++ {
			if fracPart[i] < '0' || fracPart[i] > '9' {
				return time.Time{}, false
			}
		}
		// Nine digits is nanosecond precision for seconds; finer is noise.
		if len(fracPart) > 9 {
			fracPart = fracPart[:9]
		}
		digits, _ := strconv.ParseInt(fracPart, 10, 64)
		scale := int64(1)
		for range fracPart {
			scale *= 10
		}
		fracNanos = digits * int64(unit) / scale
		if strings.HasPrefix(intPart, "-") {
			fracNanos = -fracNanos
		}
	}
	perSec := int64(time.Second / unit)
	// Outside the range the timestamp column stores, the value is in another unit (a
	// millisecond count read as seconds) or garbage, so let the next field be tried.
	// Checked on whole seconds, before time.Unix can overflow.
	if secs := whole / perSec; secs < minEpochSeconds || secs > maxEpochSeconds {
		return time.Time{}, false
	}
	return time.Unix(whole/perSec, whole%perSec*int64(unit)+fracNanos), true
}

// The DateTime64(3) timestamp column holds 1900-01-01 through 2299-12-31.
var (
	minEpochSeconds = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	maxEpochSeconds = time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
)

// fallbackTimestampLayouts are tried, in order, on the common timestamp field names
// when no configured field matched.
var fallbackTimestampLayouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02T15:04:05.000Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05.000 -07:00",
}

func parseTimestamp(val string) time.Time {
	for _, layout := range fallbackTimestampLayouts {
		if t, err := time.Parse(layout, val); err == nil {
			return t
		}
	}
	return time.Time{}
}

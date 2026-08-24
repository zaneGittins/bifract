package ingest

import (
	"encoding/json"
	"fmt"
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

func parseTimestampWithFormat(val interface{}, format string) time.Time {
	switch v := val.(type) {
	case string:
		switch format {
		case "unix":
			var seconds int64
			if _, err := fmt.Sscanf(v, "%d", &seconds); err == nil {
				return time.Unix(seconds, 0)
			}
		case "unixmilli", "unixmillis", "unixms":
			var millis int64
			if _, err := fmt.Sscanf(v, "%d", &millis); err == nil {
				return time.Unix(0, millis*int64(time.Millisecond))
			}
		case "unixmicro", "unixmicros", "unixμs":
			var micros int64
			if _, err := fmt.Sscanf(v, "%d", &micros); err == nil {
				return time.Unix(0, micros*int64(time.Microsecond))
			}
		case "unixnano", "unixnanos", "unixns":
			var nanos int64
			if _, err := fmt.Sscanf(v, "%d", &nanos); err == nil {
				return time.Unix(0, nanos)
			}
		default:
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}

	case float64:
		switch format {
		case "unix":
			return time.Unix(int64(v), 0)
		case "unixmilli", "unixmillis", "unixms":
			return time.Unix(0, int64(v)*int64(time.Millisecond))
		case "unixmicro", "unixmicros", "unixμs":
			return time.Unix(0, int64(v)*int64(time.Microsecond))
		case "unixnano", "unixnanos", "unixns":
			return time.Unix(0, int64(v))
		default:
			if v > 1e12 {
				return time.Unix(0, int64(v)*int64(time.Millisecond))
			}
			return time.Unix(int64(v), 0)
		}

	case int64:
		switch format {
		case "unix":
			return time.Unix(v, 0)
		case "unixmilli", "unixmillis", "unixms":
			return time.Unix(0, v*int64(time.Millisecond))
		case "unixmicro", "unixmicros", "unixμs":
			return time.Unix(0, v*int64(time.Microsecond))
		case "unixnano", "unixnanos", "unixns":
			return time.Unix(0, v)
		default:
			if v > 1e12 {
				return time.Unix(0, v*int64(time.Millisecond))
			}
			return time.Unix(v, 0)
		}
	}

	return time.Time{}
}

func parseTimestamp(val interface{}) time.Time {
	switch v := val.(type) {
	case string:
		formats := []string{
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

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}

	case float64:
		if v > 1e15 {
			return time.Unix(0, int64(v)*int64(time.Microsecond))
		} else if v > 1e12 {
			return time.Unix(0, int64(v)*int64(time.Millisecond))
		}
		return time.Unix(int64(v), 0)

	case int64:
		if v > 1e15 {
			return time.Unix(0, v*int64(time.Microsecond))
		} else if v > 1e12 {
			return time.Unix(0, v*int64(time.Millisecond))
		}
		return time.Unix(v, 0)
	}

	return time.Time{}
}

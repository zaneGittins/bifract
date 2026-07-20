package storage

import "time"

// chRowTimeLayout is how scanRowMap renders DateTime/DateTime64/Date columns
// into the row map. It carries no zone; ClickHouse returns UTC here.
const chRowTimeLayout = "2006-01-02 15:04:05.000"

// rowTimeLayouts are tried in order. The first is what scanRowMap produces; the
// rest cover rows that reached the map by another path (typed driver scans, or
// values that were serialized elsewhere before landing here).
var rowTimeLayouts = []string{
	chRowTimeLayout,
	"2006-01-02 15:04:05",
	"2006-01-02",
	time.RFC3339Nano,
	time.RFC3339,
}

// RowTime reads a timestamp column out of a row map produced by Query,
// QueryLowPriority, or StreamQuery.
//
// Use this instead of a direct type assertion. scanRowMap formats datetime
// columns into strings, so `row[col].(time.Time)` always fails and, with the
// usual `v, _ :=` idiom, silently yields the zero time -- indistinguishable
// from a missing value. RowTime accepts either representation.
//
// The returned time is UTC. ok is false when the column is absent, nil, or in
// no recognized format.
func RowTime(row map[string]interface{}, col string) (time.Time, bool) {
	v, exists := row[col]
	if !exists || v == nil {
		return time.Time{}, false
	}

	switch t := v.(type) {
	case time.Time:
		if t.IsZero() {
			return time.Time{}, false
		}
		return t.UTC(), true
	case *time.Time:
		if t == nil || t.IsZero() {
			return time.Time{}, false
		}
		return t.UTC(), true
	case string:
		return parseRowTime(t)
	case *string:
		if t == nil {
			return time.Time{}, false
		}
		return parseRowTime(*t)
	}
	return time.Time{}, false
}

func parseRowTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range rowTimeLayouts {
		if parsed, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			if parsed.IsZero() {
				return time.Time{}, false
			}
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

package parser

import (
	"fmt"
	"strings"
)

// A windowed rarity or first_seen model keeps a state row per key per day. Reads
// count the last N days (the window) and remember 2N (the lookback), so a key
// back after at least N days away can be told from one that never left.

// HistoryDayColumn is a windowed state table's day column; the underscore keeps
// it clear of log field names.
const HistoryDayColumn = "_day"

// HistoryRetentionDays is the TTL of a windowed table's days: the lookback plus a
// day of margin.
func HistoryRetentionDays(days int) int { return 2*days + 1 }

// historyWindowStart is the day before a window's first day.
func historyWindowStart(days int) string {
	return fmt.Sprintf("toDate(now(), 'UTC') - %d", days)
}

func historyLookbackPredicate(days int) string {
	return fmt.Sprintf("%s > toDate(now(), 'UTC') - %d", HistoryDayColumn, 2*days)
}

// HistoryPredicate admits a windowed model's days inside its window; qual
// prefixes the column ("" or "alias."). It is "" for days <= 0.
func HistoryPredicate(days int, qual string) string {
	if days <= 0 {
		return ""
	}
	return fmt.Sprintf("%s%s > %s", qual, HistoryDayColumn, historyWindowStart(days))
}

// WithHistory ANDs HistoryPredicate onto scope, which may be "".
func WithHistory(scope string, days int) string {
	p := HistoryPredicate(days, "")
	switch {
	case p == "":
		return scope
	case scope == "":
		return p
	}
	return scope + " AND " + p
}

// historyReturnIndex is the 1-based index in arr, ascending over the lookback, of
// the key's latest return: a day after at least `days` days away. The oldest day
// only counts inside the window, where the lookback covers the days before it.
// 0 means the key never left. day reads an element's day ("" or ".1").
func historyReturnIndex(arr, day string, days int) string {
	return fmt.Sprintf("arrayLastIndex((x, i) -> if(i = 1, x%[2]s > %[3]s, x%[2]s - %[1]s[i - 1]%[2]s > %[4]d), %[1]s, arrayEnumerate(%[1]s))",
		arr, day, historyWindowStart(days), days)
}

// FirstSeenAgg describes one per-entity read of a first_seen (or tlsh) table.
type FirstSeenAgg struct {
	Source, Scope, KeyCol string
	HistoryDays           int  // a windowed first_seen table's window, else 0
	Recorded              bool // add recorded_at (first_seen tables only)
	Days                  bool // add the sorted day list
}

// FirstSeenAggSQL returns one row per entity: KeyCol, first_seen, last_seen,
// event_count, since and optionally days and recorded_at. For a windowed model
// since is the return day and recorded_at when the model first held the entity
// after it, both the epoch for one that never left.
func FirstSeenAggSQL(a FirstSeenAgg) string {
	var cols []string
	if a.HistoryDays <= 0 {
		if a.Days {
			cols = append(cols, "arraySort(groupUniqArrayMerge(365)(days)) AS days")
		}
		if a.Recorded {
			cols = append(cols, "min(first_recorded) AS recorded_at")
		}
		return fmt.Sprintf(`
SELECT %s,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    sum(event_count) AS event_count,
    toDate(first_seen) AS since%s
FROM %s
WHERE %s
GROUP BY %s`, a.KeyCol, extraCols(cols), a.Source, a.Scope, a.KeyCol)
	}

	// Inner aliases differ from the columns they aggregate, which would otherwise
	// nest aggregates (code 184).
	in := HistoryPredicate(a.HistoryDays, "")
	if a.Days {
		cols = append(cols, "_ds AS days")
	}
	if a.Recorded {
		// The earliest recording from the return on, so a late log for an
		// earlier day cannot move it forward.
		cols = append(cols, "if(_ri = 0, toDateTime64(0, 3, 'UTC'), arrayMin(arrayMap(x -> x.2, arraySlice(_dr, _ri)))) AS recorded_at")
	}
	return fmt.Sprintf(`
SELECT _k AS %[1]s,
    _fs AS first_seen,
    _ls AS last_seen,
    _ec AS event_count,
    _dr[_ri].1 AS since%[2]s
FROM (
    SELECT %[1]s AS _k,
        minIf(first_seen, %[3]s) AS _fs,
        maxIf(last_seen, %[3]s) AS _ls,
        sumIf(event_count, %[3]s) AS _ec,
        arraySort(groupUniqArrayIf(%[4]s, %[3]s)) AS _ds,
        arraySort(groupArray((%[4]s, first_recorded))) AS _dr,
        %[5]s AS _ri
    FROM %[6]s
    WHERE %[7]s AND %[8]s
    GROUP BY %[1]s
    HAVING length(_ds) > 0
)`, a.KeyCol, extraCols(cols), in, HistoryDayColumn, historyReturnIndex("_dr", ".1", a.HistoryDays),
		a.Source, a.Scope, historyLookbackPredicate(a.HistoryDays))
}

func extraCols(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	return ",\n    " + strings.Join(cols, ",\n    ")
}

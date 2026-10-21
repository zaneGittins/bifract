package parser

import (
	"fmt"
	"strings"
)

// A windowed rarity or first_seen model (history_days > 0) keeps a state row per
// key per day of event time, in HistoryDayColumn. Two spans matter:
//
//   - the window, the last N days (today, UTC, included): what the model counts,
//     scores and displays;
//   - the lookback, the last 2N days: what it remembers, so that a key seen
//     inside the window can be told apart as one that returned after being away
//     for N days, which is new, or one that was there all along, which is not.
//
// Without the lookback a key seen every day looked new on whichever day happened
// to be the oldest still in the window, which moved forward every midnight. The
// table's TTL keeps the lookback plus a day (HistoryRetentionDays); reads bound
// both spans themselves, because a TTL only drops rows when parts merge.

// HistoryDayColumn is a windowed state table's day column. The underscore keeps it
// clear of log fields: a model key named "day" would otherwise meet the column's
// own alias in the statement that writes it.
const HistoryDayColumn = "_day"

// HistoryRetentionDays is how long a windowed table keeps a day: the lookback and
// one day of margin, so no row a read still admits is ever dropped early.
func HistoryRetentionDays(days int) int { return 2*days + 1 }

// historyWindowStart is the day before a window's first day: a day is inside the
// window when it is greater.
func historyWindowStart(days int) string {
	return fmt.Sprintf("toDate(now(), 'UTC') - %d", days)
}

// historyLookbackPredicate admits the days a windowed read remembers.
func historyLookbackPredicate(days int) string {
	return fmt.Sprintf("%s > toDate(now(), 'UTC') - %d", HistoryDayColumn, 2*days)
}

// HistoryPredicate admits a windowed model's days inside its window; qual
// prefixes the day column ("" or "alias."). It is "" for a model that keeps all
// its history, whose table has no day column.
func HistoryPredicate(days int, qual string) string {
	if days <= 0 {
		return ""
	}
	return fmt.Sprintf("%s%s > %s", qual, HistoryDayColumn, historyWindowStart(days))
}

// WithHistory ANDs HistoryPredicate onto scope, a WHERE predicate that may be "".
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

// historyReturn is the latest element of arr, an ascending array over the
// lookback, that follows an absence of more than days days: the last time the key
// came back. day reads an element's day (the element itself, or a tuple's first
// field with ".1"). The oldest element only counts when it lies inside the window,
// because only then does the lookback cover the days before it; an older one may
// have been seen just before the lookback began. A key with no such element has
// been around all along, and the result is the array type's default (the epoch).
func historyReturn(arr, day string, days int) string {
	return fmt.Sprintf("arrayElement(arrayFilter((x, i) -> if(i = 1, x%[2]s > %[3]s, x%[2]s - %[1]s[i - 1]%[2]s > %[4]d), %[1]s, arrayEnumerate(%[1]s)), -1)",
		arr, day, historyWindowStart(days), days)
}

// FirstSeenAgg describes one per-entity aggregation over a first_seen (or tlsh)
// state table.
type FirstSeenAgg struct {
	Source string // FROM expression yielding the state shape
	Scope  string // WHERE predicate on it
	KeyCol string // entity_key, or digest for a tlsh index
	// HistoryDays reads a windowed first_seen table over its window; 0 for a
	// model that keeps everything, a tlsh index, or a preview's scratch source.
	HistoryDays int
	// Recorded adds recorded_at, when the model first held the entity. Only a
	// first_seen table has the column it reads.
	Recorded bool
	// Days adds the sorted day list, which only the data view needs.
	Days bool
}

// FirstSeenAggSQL returns one row per entity: KeyCol, first_seen, last_seen,
// event_count, since (the day it was first seen, or for a windowed model the day
// it returned after being away for the window), and optionally days and
// recorded_at. Every reader of a first_seen table goes through it, so the data
// view, discovery, health and modelLookup() agree on what is new.
//
// A windowed model counts and displays its window and judges newness over the
// lookback: recorded_at is when the model recorded the entity's return, and the
// epoch, like seeded history, for one that never left.
func FirstSeenAggSQL(a FirstSeenAgg) string {
	if a.HistoryDays <= 0 {
		var cols []string
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

	// Two levels with non-shadowing inner aliases: an alias named like the
	// column it aggregates, used again inside another aggregate, nests aggregates
	// (code 184).
	in := HistoryPredicate(a.HistoryDays, "")
	ret := historyReturn("_dr", ".1", a.HistoryDays)
	var cols []string
	if a.Days {
		cols = append(cols, "_ds AS days")
	}
	if a.Recorded {
		cols = append(cols, ret+".2 AS recorded_at")
	}
	return fmt.Sprintf(`
SELECT _k AS %[1]s,
    _fs AS first_seen,
    _ls AS last_seen,
    _ec AS event_count,
    %[2]s.1 AS since%[3]s
FROM (
    SELECT %[1]s AS _k,
        minIf(first_seen, %[4]s) AS _fs,
        maxIf(last_seen, %[4]s) AS _ls,
        sumIf(event_count, %[4]s) AS _ec,
        arraySort(groupUniqArrayIf(%[5]s, %[4]s)) AS _ds,
        arraySort(groupArray((%[5]s, first_recorded))) AS _dr
    FROM %[6]s
    WHERE %[7]s AND %[8]s
    GROUP BY %[1]s
    HAVING length(_ds) > 0
)`, a.KeyCol, ret, extraCols(cols), in, HistoryDayColumn, a.Source, a.Scope, historyLookbackPredicate(a.HistoryDays))
}

func extraCols(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	return ",\n    " + strings.Join(cols, ",\n    ")
}

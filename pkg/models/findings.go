package models

import (
	"context"
	"fmt"
	"time"
)

// Data views: every scored row, or only the rows the model's alert would raise.
const (
	DataViewAll      = "all"
	DataViewFindings = "findings"
)

// findingsNewDays is how far back a first_seen finding reaches: entities the
// model first recorded within it.
const findingsNewDays = 7

// discoveryDays is the length of the new-per-day series in a model's stats.
const discoveryDays = 30

// DataQuery selects one page of a model's scored rows.
type DataQuery struct {
	View   string
	Search string
	Sort   string
	Order  string
	Limit  int
	Offset int
}

// findingsRule is what a model's alert fires on, read against its scored rows:
// the predicate, the order that puts the most unusual row first, and the rule as
// a phrase. The data view, its count and the stats all take it from here, so the
// Findings tab and the number above it cannot disagree.
type findingsRule struct {
	// Where is a predicate on a scored row; "" means the model sets no rule and
	// singles nothing out.
	Where string
	Order string
	Text  string
}

func findingsRuleFor(mt ModelType, def ModelDefinition) findingsRule {
	switch mt {
	case ModelTypeRarity:
		r := findingsRule{Order: "percent ASC, confidence DESC, partition_val, value_val"}
		if f := rarityFlagPredicates(def); f.Thresholded {
			r.Where, r.Text = f.SQL(), f.Text()
		}
		return r
	case ModelTypeVolumeBaseline:
		z := volumeZThreshold(def)
		return findingsRule{
			Where: fmt.Sprintf("z_score > %g", z),
			Order: "z_score DESC, entity_val",
			Text:  fmt.Sprintf("z-score > %g", z),
		}
	case ModelTypeFirstSeen:
		return findingsRule{
			Where: fmt.Sprintf("recorded_at >= now() - INTERVAL %d DAY", findingsNewDays),
			Order: "recorded_at DESC, first_seen DESC, entity_key",
			Text:  fmt.Sprintf("first recorded by the model in the last %d days", findingsNewDays),
		}
	case ModelTypeBeacon, ModelTypeLongConnection:
		t := networkScoreThreshold(def, mt)
		return findingsRule{
			Where: fmt.Sprintf("final_score > %g", t),
			Order: "final_score DESC, src_ip, dst_ip, dst_port",
			Text:  fmt.Sprintf("score > %g", t),
		}
	}
	return findingsRule{}
}

// volumeZThreshold is the z-score a volume model alerts above.
func volumeZThreshold(def ModelDefinition) float64 {
	if def.Alert != nil && def.Alert.ZThreshold > 0 {
		return def.Alert.ZThreshold
	}
	return 3.5
}

// dataPlan resolves a query against one type's sortable columns into the
// predicate the page is read under and its ORDER BY. A findings page keeps the
// rule's order unless the reader picked a column. none is true when the view
// selects nothing at all. sortExpr maps a column to its ORDER BY expression.
func dataPlan(q DataQuery, allowed map[string]bool, defaultSort string, rule findingsRule, sortExpr func(string) string) (where, order string, none bool) {
	dir := "DESC"
	if q.Order == "asc" {
		dir = "ASC"
	}
	if q.View == DataViewFindings {
		if rule.Where == "" {
			return "", "", true
		}
		if allowed[q.Sort] {
			return rule.Where, sortExpr(q.Sort) + " " + dir, false
		}
		return rule.Where, rule.Order, false
	}
	col := q.Sort
	if !allowed[col] {
		col = defaultSort
	}
	return "", sortExpr(col) + " " + dir, false
}

func plainSort(col string) string { return col }

// pageSQL returns the count and page queries over base, narrowed by where.
func pageSQL(base, where, order string, limit, offset int) (countQ, dataQ string) {
	if where != "" {
		base = "SELECT * FROM (" + base + ")\nWHERE " + where
	}
	return "SELECT count() FROM (" + base + ")",
		fmt.Sprintf("%s ORDER BY %s LIMIT %d OFFSET %d", base, order, limit, offset)
}

// runPage counts and reads one page of base under where.
func (m *Manager) runPage(ctx context.Context, what, base, where, order string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	countQ, dataQ := pageSQL(base, where, order, limit, offset)
	var total uint64
	if err := m.ch.QueryRow(ctx, countQ).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count %s data: %w", what, err)
	}
	rows, err := m.ch.QuerySchema(ctx, dataQ)
	if err != nil {
		return nil, 0, fmt.Errorf("query %s data: %w", what, err)
	}
	convertDaysToStrings(rows)
	return rows, total, nil
}

// dayCount is one day of a discovery series.
type dayCount struct {
	Day   string `json:"day"`
	Count uint64 `json:"count"`
}

// discoveryWindow returns the first day of the series ending on today (UTC).
func discoveryWindow(today time.Time) time.Time {
	t := today.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -(discoveryDays - 1))
}

// fillDiscovery zero-fills per-day counts into the series ending on today.
func fillDiscovery(counts map[string]uint64, today time.Time) []dayCount {
	start := discoveryWindow(today)
	out := make([]dayCount, discoveryDays)
	for i := range out {
		d := start.AddDate(0, 0, i).Format("2006-01-02")
		out[i] = dayCount{Day: d, Count: counts[d]}
	}
	return out
}

// sumSince totals the series from the day n-1 days before its last.
func sumSince(series []dayCount, n int) uint64 {
	var s uint64
	for i := len(series) - n; i < len(series); i++ {
		if i >= 0 {
			s += series[i].Count
		}
	}
	return s
}

// rarityDiscoverySQL groups a rarity model's scored pairs by the first day each
// was seen, counting the pairs the alert would raise in each group. One pass
// over the state yields the findings count and the new-pairs series. flagSQL is
// the rule's predicate, or "" for none.
func rarityDiscoverySQL(scored, flagSQL string) string {
	if flagSQL == "" {
		flagSQL = "0"
	}
	return fmt.Sprintf(`SELECT toString(days[1]) AS first_day, toUInt64(count()) AS n, toUInt64(countIf(%s)) AS flagged
FROM (%s)
GROUP BY first_day`, flagSQL, scored)
}

// firstSeenDiscoverySQL counts the entities a first_seen model first recorded on
// each of the series' days, with how many of those fall in the findings window
// and the last hour (what the alert fires on). Seeded history carries the epoch
// and never counts.
func firstSeenDiscoverySQL(qt, fidEsc string, since time.Time) string {
	return fmt.Sprintf(`SELECT toString(toDate(recorded_at)) AS day, toUInt64(count()) AS n,
    toUInt64(countIf(recorded_at >= now() - INTERVAL %d DAY)) AS week,
    toUInt64(countIf(recorded_at >= now() - INTERVAL 1 HOUR)) AS hour
FROM (
    SELECT entity_key, min(%s) AS recorded_at
    FROM %s FINAL
    WHERE fractal_id = '%s'
    GROUP BY entity_key
    HAVING recorded_at >= toDateTime64('%s', 3, 'UTC')
)
GROUP BY day`, findingsNewDays, FirstRecordedColumn, qt, fidEsc, since.UTC().Format("2006-01-02 15:04:05"))
}

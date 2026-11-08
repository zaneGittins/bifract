package models

import (
	"context"
	"fmt"
	"log"
	"time"

	"bifract/pkg/parser"
)

// Readiness states.
const (
	ReadinessLearning    = "learning"    // too little history for the estimate to mean anything
	ReadinessBackfilling = "backfilling" // a running backfill is still moving the history
	ReadinessReady       = "ready"
)

// defaultEstimateHalfDays is the days each half of the self-check needs. Nothing
// derives it, so AlertConfig.EstimateHalfDays can change it per model.
const defaultEstimateHalfDays = 14

// The bounds of AlertConfig.EstimateHalfDays: the longest window holds two of the
// largest half, and one day is no history to forecast from.
const (
	minEstimateHalfDays = 2
	maxEstimateHalfDays = MaxHistoryDays / 2
)

// ModelReadiness says whether a rarity or first_seen model will work on its data.
type ModelReadiness struct {
	State string `json:"state"`

	// HistoryDays is T, the days from the first to the last day the model reads,
	// and NeededDays what it takes before there is an estimate.
	HistoryDays int `json:"history_days"`
	NeededDays  int `json:"needed_days"`

	// Backfilled is set when the model was seeded. Seeded history never alerts, so
	// the rate then partly says how often new things appeared, not alerts that fired.
	Backfilled bool `json:"backfilled,omitempty"`

	// NewPerDay is N1 / T, unrounded, and unset until the state is ready.
	NewPerDay *float64 `json:"new_per_day,omitempty"`

	SelfCheck *SelfCheck       `json:"self_check,omitempty"`
	Groups    *ReadinessGroups `json:"groups,omitempty"`
}

// SelfCheck compares the rate's forecast with what happened: Forecast is N1 from
// the first half of the history, Actual the keys first seen in the second.
type SelfCheck struct {
	HalfDays int    `json:"half_days"`
	Forecast uint64 `json:"forecast"`
	Actual   uint64 `json:"actual"`
}

// ReadinessGroups counts a grouped rarity model's groups. Ready have the ReadyDays
// the alert needs; AboveCoverage is unset when the alert has no coverage gate.
type ReadinessGroups struct {
	Total         uint64  `json:"total"`
	Ready         uint64  `json:"ready"`
	ReadyDays     int     `json:"ready_days"`
	AboveCoverage *uint64 `json:"above_coverage,omitempty"`
}

func estimateHalfDays(def ModelDefinition) int {
	if def.Alert != nil && def.Alert.EstimateHalfDays > 0 {
		return def.Alert.EstimateHalfDays
	}
	return defaultEstimateHalfDays
}

func validateEstimateHalfDays(def ModelDefinition) error {
	if def.Alert == nil || def.Alert.EstimateHalfDays == 0 {
		return nil
	}
	if d := def.Alert.EstimateHalfDays; d < minEstimateHalfDays || d > maxEstimateHalfDays {
		return fmt.Errorf("estimate_half_days must be between %d and %d", minEstimateHalfDays, maxEstimateHalfDays)
	}
	return nil
}

// estimateNeed is the history an estimate waits for: two halves, or a window that
// cannot hold more. A window is a day short of full until today's first cycle, so
// that day is forgiven rather than flipping the model back to learning each night.
func estimateNeed(def ModelDefinition, mt ModelType) int {
	need := 2 * estimateHalfDays(def)
	if w := def.HistoryDaysFor(mt); w > 0 && w <= need {
		return w - 1
	}
	return need
}

// selfCheckHalves is the history's split: [first, mid) and [mid, end), equal in
// length, which leaves out the newest day of an odd span.
type selfCheckHalves struct {
	half     int
	mid, end time.Time
}

func selfCheckSplit(def ModelDefinition, first time.Time, days int) (selfCheckHalves, bool) {
	half := days / 2
	if half < estimateHalfDays(def) {
		return selfCheckHalves{}, false
	}
	return selfCheckHalves{half: half, mid: first.AddDate(0, 0, half), end: first.AddDate(0, 0, 2*half)}, true
}

func readinessHasGroups(mt ModelType, def ModelDefinition) bool {
	return mt == ModelTypeRarity && def.PartitionKey != ""
}

// readinessSource is one row per key with kept, its sorted days inside the window,
// which is what caps T at the window, and drops days the model cannot have
// recorded (see recordedDay). scored adds the group columns, at the cost of the
// scored read's window step.
func readinessSource(mt ModelType, def ModelDefinition, qt, fidEsc string, from time.Time, scored bool) string {
	hist := def.HistoryDaysFor(mt)
	var src string
	switch {
	case mt != ModelTypeRarity:
		src = firstSeenAgg(qt, fidScope(fidEsc), "entity_key", hist, false, true)
	case scored:
		src = buildRarityScoredSQL(qt, fidEsc, hist)
	default:
		src = "SELECT seen_days AS days FROM (\n" + parser.RarityPairDaysSQL(qt, fidScope(fidEsc), hist, false) + "\n)"
	}
	return fmt.Sprintf(`SELECT *, arrayFilter(d -> %s, days) AS kept
FROM (%s)
WHERE length(kept) > 0`, recordedDay("d", from), src)
}

// newKeyPredicate narrows a windowed first_seen model to keys it would call new:
// one day in the window is not new if the key never left, and since is the epoch
// for those. A rarity window needs no such filter, as its alert fires on the share.
func newKeyPredicate(mt ModelType, def ModelDefinition) string {
	if mt == ModelTypeFirstSeen && def.HistoryDaysFor(mt) > 0 {
		return " AND since > toDate(0)"
	}
	return ""
}

// readinessSummarySQL reads T's bounds, N1 and, for a grouped rarity model, the
// group counts. model_total and coverage repeat on a group's rows, so groups are
// counted distinct.
func readinessSummarySQL(mt ModelType, def ModelDefinition, qt, fidEsc string, from time.Time) string {
	groups := ""
	hasGroups := readinessHasGroups(mt, def)
	if hasGroups {
		groups = fmt.Sprintf(",\n    uniqExact(partition_val) AS groups,\n    uniqExactIf(partition_val, model_total >= %d) AS groups_ready", rarityAlertHistory(def))
		if def.Alert != nil && def.Alert.CoverageThreshold > 0 {
			groups += fmt.Sprintf(",\n    uniqExactIf(partition_val, coverage > %g) AS groups_covered", def.Alert.CoverageThreshold)
		}
	}
	return fmt.Sprintf(`SELECT toUInt64(toUnixTimestamp(toDateTime(min(kept[1]), 'UTC'))) AS oldest,
    toUInt64(toUnixTimestamp(toDateTime(max(kept[-1]), 'UTC'))) AS newest,
    toUInt64(countIf(length(kept) = 1%s)) AS seen_once%s
FROM (%s)`, newKeyPredicate(mt, def), groups, readinessSource(mt, def, qt, fidEsc, from, hasGroups))
}

// selfCheckSQL counts the keys seen on exactly one first-half day (the second
// element of a sorted list decides) and the keys first seen in the second half.
func selfCheckSQL(mt ModelType, def ModelDefinition, qt, fidEsc string, from time.Time, s selfCheckHalves) string {
	mid := "toDate('" + s.mid.Format("2006-01-02") + "')"
	end := "toDate('" + s.end.Format("2006-01-02") + "')"
	return fmt.Sprintf(`SELECT toUInt64(countIf(kept[1] < %[1]s AND (length(kept) = 1 OR kept[2] >= %[1]s)%[3]s)) AS forecast,
    toUInt64(countIf(kept[1] >= %[1]s AND kept[1] < %[2]s%[3]s)) AS actual
FROM (%[4]s)`, mid, end, newKeyPredicate(mt, def), readinessSource(mt, def, qt, fidEsc, from, false))
}

func parseReadinessGroups(r map[string]interface{}, readyDays int) *ReadinessGroups {
	if _, ok := r["groups"]; !ok {
		return nil
	}
	g := &ReadinessGroups{Total: numToUint64(r["groups"]), Ready: numToUint64(r["groups_ready"]), ReadyDays: readyDays}
	if v, ok := r["groups_covered"]; ok {
		n := numToUint64(v)
		g.AboveCoverage = &n
	}
	return g
}

// addReadiness sets result's readiness. The stats stand without it, so a failed
// read is logged and leaves it out.
func (m *Manager) addReadiness(ctx context.Context, model *Model, qt, fid string, result map[string]interface{}) {
	r, err := m.readReadiness(ctx, model, qt, fid)
	if err != nil {
		log.Printf("[Models] readiness %s: %v", model.ID, err)
		return
	}
	result["readiness"] = r
}

func (m *Manager) readReadiness(ctx context.Context, model *Model, qt, fid string) (*ModelReadiness, error) {
	mt, def, from := model.ModelType, model.Definition, recordStart(model)
	rows, err := m.ch.QuerySchema(ctx, readinessSummarySQL(mt, def, qt, fid, from))
	if err != nil {
		return nil, fmt.Errorf("summary: %w", err)
	}
	r := &ModelReadiness{State: ReadinessLearning, NeededDays: estimateNeed(def, mt), Backfilled: model.BackfillDone > 0}
	if len(rows) == 0 {
		return r, nil
	}
	row := rows[0]
	r.Groups = parseReadinessGroups(row, rarityAlertHistory(def))
	first := stateTime(row["oldest"])
	r.HistoryDays = historySpan(first, stateTime(row["newest"]), mt)

	if model.BackfillStatus == "running" {
		r.State = ReadinessBackfilling
		return r, nil
	}
	if r.HistoryDays < r.NeededDays {
		return r, nil
	}
	r.State = ReadinessReady
	rate := float64(numToUint64(row["seen_once"])) / float64(r.HistoryDays)
	r.NewPerDay = &rate

	s, ok := selfCheckSplit(def, first, r.HistoryDays)
	if !ok {
		return r, nil
	}
	rows, err = m.ch.QuerySchema(ctx, selfCheckSQL(mt, def, qt, fid, from, s))
	if err != nil {
		log.Printf("[Models] readiness self-check %s: %v", model.ID, err)
		return r, nil
	}
	if len(rows) > 0 {
		r.SelfCheck = &SelfCheck{HalfDays: s.half, Forecast: numToUint64(rows[0]["forecast"]), Actual: numToUint64(rows[0]["actual"])}
	}
	return r, nil
}

package models

import (
	"strings"
	"testing"
	"time"
)

// The Findings view is what the alert would raise, so each type's rule must be
// the alert's own comparison, threshold and direction (see GenerateQuery).
func TestFindingsRuleMatchesAlert(t *testing.T) {
	cases := []struct {
		name  string
		mt    ModelType
		def   ModelDefinition
		where string
		order string
		alert string
	}{
		{
			name:  "rarity",
			mt:    ModelTypeRarity,
			def:   ModelDefinition{MinSample: 3, Alert: &AlertConfig{CoverageThreshold: 0.8, PercentThreshold: 5}},
			where: "model_total >= 3 AND coverage > 0.8 AND percent < 5",
			order: "percent ASC, coverage DESC, partition_val, value_val",
			alert: "| coverage > 0.80\n| percent < 5.00",
		},
		{
			name:  "volume default threshold",
			mt:    ModelTypeVolumeBaseline,
			def:   ModelDefinition{KeyFields: []string{"host"}},
			where: "z_score > 3.5",
			order: "z_score DESC, entity_val",
			alert: "| z_score > 3.50",
		},
		{
			name:  "volume configured threshold",
			mt:    ModelTypeVolumeBaseline,
			def:   ModelDefinition{KeyFields: []string{"host"}, Alert: &AlertConfig{ZThreshold: 6}},
			where: "z_score > 6",
			order: "z_score DESC, entity_val",
			alert: "| z_score > 6.00",
		},
		{
			name:  "beacon",
			mt:    ModelTypeBeacon,
			def:   ModelDefinition{Beacon: &BeaconParams{ScoreThreshold: 0.7}},
			where: "final_score > 0.7",
			order: "final_score DESC, src_ip, dst_ip, dst_port",
			alert: "| beacon_score > 0.70",
		},
		{
			name:  "long connection default threshold",
			mt:    ModelTypeLongConnection,
			def:   ModelDefinition{},
			where: "final_score > 0.5",
			order: "final_score DESC, src_ip, dst_ip, dst_port",
			alert: "| longconn_score > 0.50",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := findingsRuleFor(c.mt, c.def)
			if r.Where != c.where {
				t.Errorf("where = %q, want %q", r.Where, c.where)
			}
			if r.Order != c.order {
				t.Errorf("order = %q, want %q", r.Order, c.order)
			}
			if r.Text == "" {
				t.Error("a rule with a predicate needs a phrase for the empty state")
			}
			if q := GenerateQuery("m", c.def, c.mt); !strings.Contains(q, c.alert) {
				t.Errorf("alert query no longer filters %q, so findings and the alert disagree:\n%s", c.alert, q)
			}
		})
	}
}

// The findings count above the table and the rarity flag count share one rule.
func TestRarityFindingsUsesFlagPredicates(t *testing.T) {
	def := ModelDefinition{MinSample: 2, Alert: &AlertConfig{CoverageThreshold: 0.9, PercentThreshold: 10}}
	if got, want := findingsRuleFor(ModelTypeRarity, def).Where, rarityFlagPredicates(def).SQL(); got != want {
		t.Fatalf("findings %q != flag predicate %q", got, want)
	}
}

// A model with no rule singles nothing out: rarity without thresholds, and a
// tlsh index, which raises no alert at all.
func TestFindingsWithoutRule(t *testing.T) {
	for _, c := range []struct {
		mt  ModelType
		def ModelDefinition
	}{
		{ModelTypeRarity, ModelDefinition{}},
		{ModelTypeRarity, ModelDefinition{MinSample: 4, Alert: &AlertConfig{AlertOnNew: true}}},
		{ModelTypeTLSH, ModelDefinition{KeyFields: []string{"tlsh"}}},
	} {
		r := findingsRuleFor(c.mt, c.def)
		if r.Where != "" || r.Text != "" {
			t.Errorf("%s: want no rule, got %+v", c.mt, r)
		}
		_, _, none := dataPlan(DataQuery{View: DataViewFindings}, map[string]bool{}, "x", r, plainSort)
		if !none {
			t.Errorf("%s: a findings view without a rule must select nothing", c.mt)
		}
	}
}

func TestFirstSeenFindingsAreNewToModel(t *testing.T) {
	r := findingsRuleFor(ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"host"}})
	// recorded_at, not first_seen: a backfill seeds history with an old event time
	// and the epoch recorded_at, and must never read as new.
	if r.Where != "recorded_at >= now() - INTERVAL 7 DAY" {
		t.Errorf("where = %q", r.Where)
	}
	if !strings.HasPrefix(r.Order, "recorded_at DESC") {
		t.Errorf("newest first, got %q", r.Order)
	}
}

func TestDataPlan(t *testing.T) {
	allowed := map[string]bool{"z_score": true, "entity_val": true}
	rule := findingsRule{Where: "z_score > 3.5", Order: "z_score DESC, entity_val"}
	abs := func(c string) string {
		if c == "z_score" {
			return "abs(z_score)"
		}
		return c
	}
	cases := []struct {
		name        string
		q           DataQuery
		where, want string
	}{
		{"all rows keeps the default sort", DataQuery{View: DataViewAll}, "", "abs(z_score) DESC"},
		{"all rows honors a sort", DataQuery{View: DataViewAll, Sort: "entity_val", Order: "asc"}, "", "entity_val ASC"},
		{"unknown sort falls back", DataQuery{View: DataViewAll, Sort: "1; DROP"}, "", "abs(z_score) DESC"},
		{"findings use the rule order", DataQuery{View: DataViewFindings}, "z_score > 3.5", "z_score DESC, entity_val"},
		{"findings ignore an unknown sort", DataQuery{View: DataViewFindings, Sort: "nope"}, "z_score > 3.5", "z_score DESC, entity_val"},
		{"findings honor a picked column", DataQuery{View: DataViewFindings, Sort: "entity_val", Order: "asc"}, "z_score > 3.5", "entity_val ASC"},
	}
	for _, c := range cases {
		where, order, none := dataPlan(c.q, allowed, "z_score", rule, abs)
		if none || where != c.where || order != c.want {
			t.Errorf("%s: got (%q, %q, %v), want (%q, %q)", c.name, where, order, none, c.where, c.want)
		}
	}
}

func TestPageSQL(t *testing.T) {
	base := "SELECT a, z_score FROM t WHERE x = 1"
	// All rows: the page reads base as before, so its SQL is unchanged.
	countQ, dataQ := pageSQL(base, "", "a DESC", 50, 100)
	if countQ != "SELECT count() FROM ("+base+")" {
		t.Errorf("count: %s", countQ)
	}
	if dataQ != base+" ORDER BY a DESC LIMIT 50 OFFSET 100" {
		t.Errorf("data: %s", dataQ)
	}
	// Findings wrap base, so the predicate reads the scored columns and both
	// queries count and page the same rows.
	countQ, dataQ = pageSQL(base, "z_score > 3.5", "z_score DESC", 25, 0)
	inner := "SELECT * FROM (" + base + ")\nWHERE z_score > 3.5"
	if countQ != "SELECT count() FROM ("+inner+")" {
		t.Errorf("findings count: %s", countQ)
	}
	if dataQ != inner+" ORDER BY z_score DESC LIMIT 25 OFFSET 0" {
		t.Errorf("findings data: %s", dataQ)
	}
}

func TestRarityDiscoverySQL(t *testing.T) {
	scored := buildRarityScoredSQL("`tbl` FINAL", "f1")
	q := rarityDiscoverySQL(scored, "model_total >= 1 AND percent < 5")
	mustContain(t, q, "toString(days[1]) AS first_day", "groups by each pair's first day")
	mustContain(t, q, "toUInt64(countIf(model_total >= 1 AND percent < 5)) AS flagged", "counts with the rule")
	mustContain(t, q, "FROM (", "reads the scored rows")
	mustContain(t, q, "fractal_id = 'f1'", "scoped to the fractal")
	if !strings.Contains(scored, "seen_days AS days") {
		t.Fatal("discovery needs the scored rows' day list")
	}
	mustContain(t, rarityDiscoverySQL(scored, ""), "toUInt64(countIf(0)) AS flagged", "no rule flags nothing")
}

func TestFirstSeenDiscoverySQL(t *testing.T) {
	since := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	q := firstSeenDiscoverySQL("`tbl`", "f1", since)
	mustContain(t, q, "min(first_recorded) AS recorded_at", "new means new to the model")
	mustContain(t, q, "HAVING recorded_at >= toDateTime64('2026-09-08 00:00:00', 3, 'UTC')", "bounded to the series window")
	mustContain(t, q, "INTERVAL 7 DAY", "findings window")
	mustContain(t, q, "INTERVAL 1 HOUR", "what the alert fires on")
	mustContain(t, q, "fractal_id = 'f1'", "scoped to the fractal")
	mustContain(t, q, "`tbl` FINAL", "reads the state, never the logs")
}

func TestFillDiscovery(t *testing.T) {
	today := time.Date(2026, 10, 7, 15, 4, 0, 0, time.UTC)
	series := fillDiscovery(map[string]uint64{"2026-10-07": 3, "2026-10-01": 2, "2026-09-30": 7, "2026-09-07": 9, "2026-08-01": 4}, today)
	if len(series) != discoveryDays {
		t.Fatalf("len = %d", len(series))
	}
	if series[0].Day != "2026-09-08" || series[len(series)-1].Day != "2026-10-07" {
		t.Errorf("window %s..%s", series[0].Day, series[len(series)-1].Day)
	}
	// The last 7 days are Oct 1 through Oct 7.
	if got := sumSince(series, 7); got != 5 {
		t.Errorf("week = %d, want 5", got)
	}
	var total uint64
	for _, d := range series {
		total += d.Count
	}
	if total != 12 {
		t.Errorf("days outside the window leaked in: total %d", total)
	}
}

// A pair whose every connection falls before the window cutoff is not scored, so
// it never overwrites its last result with zeros and an epoch first/last seen.
func TestPairOutsideWindowIsNotScored(t *testing.T) {
	cutoff := int64(1_000_000)
	stale := pairFromRow(map[string]interface{}{
		"src": "10.0.0.1", "dst": "10.0.0.2", "port": "443", "cnt": uint64(5),
		"ts_list": []uint32{999_000, 999_100, 999_200, 999_300, 999_400},
	}, cutoff)
	if stale.inWindow() {
		t.Fatal("a pair with no connection in the window must not be scored")
	}
	live := pairFromRow(map[string]interface{}{
		"src": "10.0.0.1", "dst": "10.0.0.2", "port": "443", "cnt": uint64(5),
		"ts_list": []uint32{999_000, 1_000_100, 1_000_200},
	}, cutoff)
	if !live.inWindow() || live.FirstTs != 1_000_100 || live.LastTs != 1_000_200 {
		t.Fatalf("in-window pair: %+v", live)
	}
}

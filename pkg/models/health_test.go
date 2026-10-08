package models

import (
	"strings"
	"testing"
	"time"
)

func healthModel(mt ModelType, def ModelDefinition) *Model {
	lag := int64(90)
	return &Model{
		ID: "m1", ModelType: mt, Definition: def, Status: "active", CHTableName: "model_m1",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), StateLagSeconds: &lag,
	}
}

func TestDeriveHealthOrder(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	fresh := &stateSummary{oldest: now.Add(-60 * 24 * time.Hour), newest: now.Add(-time.Hour)}

	cases := []struct {
		name  string
		edit  func(*Model)
		sum   *stateSummary
		err   error
		state string
	}{
		{"build error", func(m *Model) { m.Status = "error"; m.ErrorMessage = "boom" }, fresh, nil, HealthError},
		{"cycle failing", func(m *Model) { m.ErrorMessage = "code 241" }, fresh, nil, HealthNotUpdating},
		{"never handed over", func(m *Model) { m.StateLagSeconds = nil }, fresh, nil, HealthNotStarted},
		{"behind", func(m *Model) { m.StateBehind = true }, fresh, nil, HealthBehind},
		{"rebuilding", func(m *Model) { m.Status = "rebuilding"; m.StateLagSeconds = nil }, nil, nil, HealthRebuilding},
		{"backfilling", func(m *Model) { m.BackfillStatus = "running"; m.BackfillWindow = "30d" }, fresh, nil, HealthBackfilling},
		{"failing outranks backfilling", func(m *Model) { m.BackfillStatus = "running"; m.ErrorMessage = "x" }, fresh, nil, HealthNotUpdating},
		{"summary failed", func(m *Model) {}, nil, errTest, HealthUnknown},
		{"summary pending", func(m *Model) {}, nil, nil, HealthChecking},
		{"healthy", func(m *Model) {}, fresh, nil, HealthHealthy},
	}
	for _, c := range cases {
		m := healthModel(ModelTypeFirstSeen, ModelDefinition{})
		c.edit(m)
		if got := deriveHealth(m, c.sum, c.err, now).State; got != c.state {
			t.Errorf("%s: state = %q, want %q", c.name, got, c.state)
		}
	}
}

type testErr string

func (e testErr) Error() string { return string(e) }

var errTest = testErr("read failed")

func TestDeriveHealthStale(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 30, 0, 0, time.UTC)
	hourly := ModelDefinition{TimeBucket: "hour"}

	// An hourly volume model is stale after three empty hours, so every entity
	// scoring 0 is called out rather than looking like a quiet fleet.
	m := healthModel(ModelTypeVolumeBaseline, hourly)
	h := deriveHealth(m, &stateSummary{oldest: now.Add(-40 * 24 * time.Hour), newest: now.Add(-4 * time.Hour)}, nil, now)
	if h.State != HealthStale || !strings.Contains(h.Detail, "empty bucket") {
		t.Errorf("hourly volume 4h old: %+v", h)
	}
	h = deriveHealth(m, &stateSummary{oldest: now.Add(-40 * 24 * time.Hour), newest: now.Add(-90 * time.Minute)}, nil, now)
	if h.State != HealthHealthy {
		t.Errorf("hourly volume 90m old: state %q", h.State)
	}

	// Everything else tolerates two days, so a quiet weekend on a narrow filter
	// does not read as a broken source.
	m = healthModel(ModelTypeRarity, ModelDefinition{})
	for age, want := range map[time.Duration]string{36 * time.Hour: HealthHealthy, 72 * time.Hour: HealthStale} {
		h = deriveHealth(m, &stateSummary{oldest: now.Add(-100 * 24 * time.Hour), newest: now.Add(-age)}, nil, now)
		if h.State != want {
			t.Errorf("rarity newest %v old: state %q, want %q", age, h.State, want)
		}
	}

	// Stale outranks learning: a young model whose source stopped is not learning.
	h = deriveHealth(m, &stateSummary{oldest: now.Add(-6 * 24 * time.Hour), newest: now.Add(-5 * 24 * time.Hour)}, nil, now)
	if h.State != HealthStale {
		t.Errorf("young stopped rarity: state %q", h.State)
	}

	// An empty state is learning while the model is young and stale once it is not.
	young := healthModel(ModelTypeFirstSeen, ModelDefinition{})
	young.CreatedAt = now.Add(-time.Hour)
	if s := deriveHealth(young, &stateSummary{}, nil, now).State; s != HealthLearning {
		t.Errorf("empty young model: state %q", s)
	}
	if s := deriveHealth(healthModel(ModelTypeFirstSeen, ModelDefinition{}), &stateSummary{}, nil, now).State; s != HealthStale {
		t.Errorf("empty old model: state %q", s)
	}
}

func TestDeriveHealthLearning(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	cases := []struct {
		name      string
		mt        ModelType
		def       ModelDefinition
		span      time.Duration
		need      int
		unit      string
		have      int
		wantState string
	}{
		// 10% needs more than 100/10 = 10 days.
		{"rarity 10% at 4 days", ModelTypeRarity, ModelDefinition{}, 3 * day, 11, "day", 4, HealthLearning},
		{"rarity 10% at 11 days", ModelTypeRarity, ModelDefinition{}, 10 * day, 11, "day", 11, HealthHealthy},
		{"rarity 5% at 11 days", ModelTypeRarity, ModelDefinition{Alert: &AlertConfig{PercentThreshold: 5}}, 10 * day, 21, "day", 11, HealthLearning},
		{"volume hourly default", ModelTypeVolumeBaseline, ModelDefinition{TimeBucket: "hour"}, 5 * time.Hour, 7, "hour", 5, HealthLearning},
		{"volume daily min 3", ModelTypeVolumeBaseline, ModelDefinition{MinSample: 3}, 3 * day, 3, "day", 3, HealthHealthy},
		{"first_seen week", ModelTypeFirstSeen, ModelDefinition{}, 2 * day, 7, "day", 3, HealthLearning},
		{"beacon 7d window", ModelTypeBeacon, ModelDefinition{Window: "7d"}, 6 * day, 7, "day", 7, HealthHealthy},
		{"tlsh never learns", ModelTypeTLSH, ModelDefinition{}, 0, 0, "", 1, HealthHealthy},
	}
	for _, c := range cases {
		newest := now.Add(-time.Hour)
		h := deriveHealth(healthModel(c.mt, c.def), &stateSummary{oldest: newest.Add(-c.span), newest: newest}, nil, now)
		if h.HistoryNeeded != c.need || h.HistoryUnit != c.unit || h.History != c.have || h.State != c.wantState {
			t.Errorf("%s: got %d/%d %s state %q, want %d/%d %s state %q",
				c.name, h.History, h.HistoryNeeded, h.HistoryUnit, h.State, c.have, c.need, c.unit, c.wantState)
		}
	}
}

func TestHealthSummarySQL(t *testing.T) {
	today := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	fid := "f'1"

	fs := healthSummarySQL(ModelTypeFirstSeen, ModelDefinition{}, "model_a", "", fid, today)
	for _, want := range []string{
		"toDate(min(first_recorded)) AS d", // findings follow what the alert fires on
		"GROUP BY entity_key",              // unmerged parts would count an entity once per cycle
		"countIf(d = toDate('2026-10-07') - 6)",
		"countIf(d = toDate('2026-10-07') - 0)",
		`fractal_id = 'f\'1'`,
	} {
		if !strings.Contains(fs, want) {
			t.Errorf("first_seen summary missing %q:\n%s", want, fs)
		}
	}
	if strings.Count(fs, "countIf(") != findingsDays {
		t.Errorf("first_seen series has %d days, want %d", strings.Count(fs, "countIf("), findingsDays)
	}

	rar := healthSummarySQL(ModelTypeRarity, ModelDefinition{}, "model_r", "", "f", today)
	for _, want := range []string{"min(arrayMin(finalizeAggregation(days))) AS fd", "GROUP BY partition_val, value_val", "countIf(fd = "} {
		if !strings.Contains(rar, want) {
			t.Errorf("rarity summary missing %q", want)
		}
	}
	// No alert thresholds: no rule, so no count rather than every row.
	if strings.Contains(rar, "AS flagged") {
		t.Errorf("rarity summary without thresholds counts findings:\n%s", rar)
	}
	// With thresholds the count is the Findings tab's own rule.
	rarAlert := healthSummarySQL(ModelTypeRarity, ModelDefinition{Alert: &AlertConfig{CoverageThreshold: 0.9, PercentThreshold: 10}}, "model_r", "", "f", today)
	if want := findingsRuleFor(ModelTypeRarity, ModelDefinition{Alert: &AlertConfig{CoverageThreshold: 0.9, PercentThreshold: 10}}).Where; !strings.Contains(rarAlert, "WHERE "+want+"), 0)) AS flagged") {
		t.Errorf("rarity count does not use the findings rule %q:\n%s", want, rarAlert)
	}

	vol := healthSummarySQL(ModelTypeVolumeBaseline, ModelDefinition{TimeBucket: "hour", Alert: &AlertConfig{ZThreshold: 4}}, "model_v", "", "f", today)
	for _, want := range []string{"WHERE z_score > 4), 0)) AS flagged", "max(bucket)", "toStartOfHour(now('UTC'))"} {
		if !strings.Contains(vol, want) {
			t.Errorf("volume summary missing %q", want)
		}
	}

	net := healthSummarySQL(ModelTypeBeacon, ModelDefinition{}, "model_b", "model_state_b", "f", today)
	for _, want := range []string{"WHERE final_score > 0.8), 0)) AS flagged", "`model_b` FINAL", "max(last_ts)", "FROM `model_state_b`"} {
		if !strings.Contains(net, want) {
			t.Errorf("beacon summary missing %q", want)
		}
	}

	// None of the summaries read raw logs.
	for _, q := range []string{fs, rar, vol, net, healthSummarySQL(ModelTypeTLSH, ModelDefinition{}, "model_t", "", "f", today)} {
		if strings.Contains(q, "`logs`") || strings.Contains(q, " logs ") || strings.Contains(q, "logs_distributed") {
			t.Errorf("summary reads the log table:\n%s", q)
		}
	}
}

func TestParseStateSummary(t *testing.T) {
	s := parseStateSummary(ModelTypeRarity, []map[string]interface{}{{
		"oldest": uint64(1788000000), "newest": uint64(1788900000), "series": []uint64{0, 1, 0, 2, 0, 0, 4}, "flagged": uint64(3),
	}})
	// The count is the rule's, not the sum of the discovery series.
	if s.findings == nil || *s.findings != 3 || len(s.series) != 7 || s.newest.Unix() != 1788900000 {
		t.Errorf("rarity summary parsed as %+v", s)
	}
	empty := parseStateSummary(ModelTypeVolumeBaseline, []map[string]interface{}{{"oldest": uint64(0), "newest": uint64(0), "flagged": uint64(0)}})
	if !empty.newest.IsZero() || !empty.oldest.IsZero() || empty.findings == nil || *empty.findings != 0 || empty.series != nil {
		t.Errorf("empty volume summary parsed as %+v", empty)
	}
	if tl := parseStateSummary(ModelTypeTLSH, []map[string]interface{}{{"oldest": uint64(1788000000), "newest": uint64(1788000000)}}); tl.findings != nil {
		t.Errorf("tlsh has findings: %+v", tl)
	}
}

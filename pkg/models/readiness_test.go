package models

import (
	"context"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

func TestEstimateNeed(t *testing.T) {
	cases := []struct {
		name string
		def  ModelDefinition
		want int
	}{
		{"default is 14 days per half", ModelDefinition{}, 28},
		{"the half is tunable", ModelDefinition{Alert: &AlertConfig{EstimateHalfDays: 7}}, 14},
		{"a long window changes nothing", ModelDefinition{HistoryDays: 90}, 28},
		{"a short window waits until it is nearly full", ModelDefinition{HistoryDays: 7}, 6},
		{"a window of exactly two halves has the same day of slack", ModelDefinition{HistoryDays: 28}, 27},
	}
	for _, c := range cases {
		if got := estimateNeed(c.def, ModelTypeRarity); got != c.want {
			t.Errorf("%s: need %d, want %d", c.name, got, c.want)
		}
	}
}

func TestSelfCheckSplit(t *testing.T) {
	first := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	day := func(n int) time.Time { return first.AddDate(0, 0, n) }
	cases := []struct {
		name     string
		days     int
		def      ModelDefinition
		ok       bool
		half     int
		mid, end time.Time
	}{
		{"equal halves", 30, ModelDefinition{}, true, 15, day(15), day(30)},
		{"an odd span leaves the newest, partial day out", 29, ModelDefinition{}, true, 14, day(14), day(28)},
		{"halves below the minimum say nothing", 27, ModelDefinition{}, false, 0, time.Time{}, time.Time{}},
		{"short halves never get one", 7, ModelDefinition{}, false, 0, time.Time{}, time.Time{}},
		{"the minimum is tunable", 7, ModelDefinition{Alert: &AlertConfig{EstimateHalfDays: 3}}, true, 3, day(3), day(6)},
	}
	for _, c := range cases {
		s, ok := selfCheckSplit(c.def, first, c.days)
		if ok != c.ok || s.half != c.half || !s.mid.Equal(c.mid) || !s.end.Equal(c.end) {
			t.Errorf("%s: %+v ok=%v", c.name, s, ok)
		}
	}
}

func summaryResult(first, last time.Time, singletons uint64) fakedb.CHResult {
	return fakedb.CHResult{
		Cols: cols("oldest", "UInt64", "newest", "UInt64", "seen_once", "UInt64"),
		Rows: [][]any{{uint64(first.Unix()), uint64(last.Unix()), singletons}},
	}
}

func readinessOf(t *testing.T, st map[string]interface{}) *ModelReadiness {
	t.Helper()
	r, ok := st["readiness"].(*ModelReadiness)
	if !ok {
		t.Fatalf("no readiness in %v", st)
	}
	return r
}

var readinessStart = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// T is the recorded span, so the rate divides by it and not by the window, and
// the self-check reads the two halves of that span.
func TestGetStatsReadinessEstimate(t *testing.T) {
	env := newEnv(t)
	def := rarityDef
	def.HistoryDays = 90
	mo := clusterModel(env, ModelTypeRarity, def)
	env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 29), 39))
	env.ch.On("AS forecast", fakedb.CHResult{Cols: cols("forecast", "UInt64", "actual", "UInt64"), Rows: [][]any{{uint64(20), uint64(17)}}})

	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	r := readinessOf(t, st)
	if r.State != ReadinessReady || r.HistoryDays != 30 || r.NeededDays != 28 || r.NewPerDay == nil || *r.NewPerDay != 39.0/30 {
		t.Fatalf("readiness: %+v", r)
	}
	if r.SelfCheck == nil || *r.SelfCheck != (SelfCheck{HalfDays: 15, Forecast: 20, Actual: 17}) {
		t.Fatalf("self check: %+v", r.SelfCheck)
	}
	q := env.ch.QueriedMatching("AS forecast")
	if len(q) != 1 || !strings.Contains(q[0], "toDate('2026-09-16')") || !strings.Contains(q[0], "toDate('2026-10-01')") {
		t.Errorf("self check query: %v", q)
	}
}

// A young model has its learning state and nothing that reads as an estimate.
func TestGetStatsReadinessYoungModel(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 8), 500))

	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	r := readinessOf(t, st)
	if r.State != ReadinessLearning || r.HistoryDays != 9 || r.NeededDays != 28 || r.NewPerDay != nil || r.SelfCheck != nil {
		t.Fatalf("readiness: %+v", r)
	}
	if q := env.ch.QueriedMatching("AS forecast"); len(q) != 0 {
		t.Errorf("a young model ran the self-check: %v", q)
	}
}

// An empty model aggregates to the epoch: no history, not one day of it.
func TestGetStatsReadinessEmptyModel(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}})
	env.ch.On("AS seen_once", summaryResult(time.Unix(0, 0), time.Unix(0, 0), 0))

	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if r := readinessOf(t, st); r.State != ReadinessLearning || r.HistoryDays != 0 {
		t.Fatalf("readiness: %+v", r)
	}
}

// A window too short for two useful halves still gets the rate once it is full.
func TestGetStatsReadinessShortWindow(t *testing.T) {
	env := newEnv(t)
	def := rarityDef
	def.HistoryDays, def.MinSample = 7, 1
	mo := clusterModel(env, ModelTypeRarity, def)
	env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 6), 7))

	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	r := readinessOf(t, st)
	if r.State != ReadinessReady || r.NewPerDay == nil || *r.NewPerDay != 1 || r.SelfCheck != nil {
		t.Fatalf("readiness: %+v", r)
	}
}

// A rate too small to round to two places is still not zero.
func TestGetStatsReadinessKeepsSmallRates(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 299), 1))
	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if r := readinessOf(t, st); r.NewPerDay == nil || *r.NewPerDay != 1.0/300 {
		t.Fatalf("readiness: %+v", r)
	}
}

// A failed self-check costs only the self-check, not the rate read before it.
func TestGetStatsReadinessSelfCheckFailure(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 29), 30))
	env.ch.On("AS forecast", fakedb.CHResult{Err: context.DeadlineExceeded})
	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if r := readinessOf(t, st); r.State != ReadinessReady || r.NewPerDay == nil || r.SelfCheck != nil {
		t.Fatalf("readiness: %+v", r)
	}
}

// A running backfill is still moving the history, so nothing is estimated from it.
func TestGetStatsReadinessBackfill(t *testing.T) {
	cases := []struct {
		status     string
		done       int
		state      string
		backfilled bool
	}{
		{"running", 3, ReadinessBackfilling, true},
		{"completed", 8, ReadinessReady, true},
		{"none", 0, ReadinessReady, false},
	}
	for _, c := range cases {
		env := newEnv(t)
		r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.bfStatus, r.bfDone, r.bfTotal = c.status, int64(c.done), 8 })
		mo, err := env.mgr.Get(context.Background(), r.id)
		if err != nil {
			t.Fatal(err)
		}
		env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 29), 30))
		st, err := env.mgr.GetStats(context.Background(), mo, fracA)
		if err != nil {
			t.Fatal(err)
		}
		got := readinessOf(t, st)
		if got.State != c.state || got.Backfilled != c.backfilled || (got.NewPerDay != nil) != (c.state == ReadinessReady) {
			t.Errorf("%s: %+v", c.status, got)
		}
	}
}

// Group shares use the alert's own gates, and count each group once.
func TestReadinessGroups(t *testing.T) {
	def := rarityDef
	def.MinSample = 3
	def.Alert = &AlertConfig{CoverageThreshold: 0.85, PercentThreshold: 5}
	q := readinessSummarySQL(ModelTypeRarity, def, "`t`", "f1", time.Time{})
	for _, want := range []string{
		"uniqExact(partition_val) AS groups",
		"uniqExactIf(partition_val, model_total >= 21) AS groups_ready",
		"uniqExactIf(partition_val, coverage > 0.85) AS groups_covered",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("missing %q in %s", want, q)
		}
	}

	// With no share set, only min history gates the alert.
	coverageOnly := rarityDef
	coverageOnly.MinSample = 3
	coverageOnly.Alert = &AlertConfig{CoverageThreshold: 0.85}
	if q := readinessSummarySQL(ModelTypeRarity, coverageOnly, "`t`", "f1", time.Time{}); !strings.Contains(q, "model_total >= 3)") {
		t.Errorf("not the alert's gate: %s", q)
	}

	noGate := rarityDef
	noGate.Alert = nil
	if q := readinessSummarySQL(ModelTypeRarity, noGate, "`t`", "f1", time.Time{}); strings.Contains(q, "groups_covered") {
		t.Errorf("no coverage gate, but counted one: %s", q)
	}
	ungrouped := rarityDef
	ungrouped.PartitionKey = ""
	if q := readinessSummarySQL(ModelTypeRarity, ungrouped, "`t`", "f1", time.Time{}); strings.Contains(q, "groups") {
		t.Errorf("no group, but counted them: %s", q)
	}
	if q := readinessSummarySQL(ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"u"}}, "`t`", "f1", time.Time{}); strings.Contains(q, "groups") {
		t.Errorf("first_seen has no groups to count: %s", q)
	}
}

// The window scan is only paid where a group needs it, and every read honours
// the model's window.
func TestReadinessSources(t *testing.T) {
	grouped := ModelDefinition{PartitionKey: "h", ValueKey: "v", HistoryDays: 30}
	ungrouped := ModelDefinition{ValueKey: "v", HistoryDays: 30}
	seen := ModelDefinition{KeyFields: []string{"v"}, HistoryDays: 30}
	s := selfCheckHalves{half: 15, mid: readinessStart, end: readinessStart}
	const windowFn = "OVER w"
	cases := []struct {
		name     string
		q        string
		windowFn bool
		newOnly  bool
	}{
		{"grouped summary", readinessSummarySQL(ModelTypeRarity, grouped, "`t`", "f1", time.Time{}), true, false},
		{"ungrouped summary", readinessSummarySQL(ModelTypeRarity, ungrouped, "`t`", "f1", time.Time{}), false, false},
		{"rarity self-check", selfCheckSQL(ModelTypeRarity, grouped, "`t`", "f1", time.Time{}, s), false, false},
		{"first_seen summary", readinessSummarySQL(ModelTypeFirstSeen, seen, "`t`", "f1", time.Time{}), false, true},
		{"first_seen self-check", selfCheckSQL(ModelTypeFirstSeen, seen, "`t`", "f1", time.Time{}, s), false, true},
	}
	for _, c := range cases {
		if got := strings.Contains(c.q, windowFn); got != c.windowFn {
			t.Errorf("%s: window function %v: %s", c.name, got, c.q)
		}
		if got := strings.Contains(c.q, "since > toDate(0)"); got != c.newOnly {
			t.Errorf("%s: new-only %v: %s", c.name, got, c.q)
		}
		if !strings.Contains(c.q, "toDate(now(), 'UTC') - 30") {
			t.Errorf("%s: read outside the window: %s", c.name, c.q)
		}
	}
}

func TestParseReadinessGroups(t *testing.T) {
	row := map[string]interface{}{"groups": uint64(1000), "groups_ready": uint64(700), "groups_covered": uint64(800)}
	g := parseReadinessGroups(row, 21)
	if g == nil || g.Total != 1000 || g.Ready != 700 || g.ReadyDays != 21 || g.AboveCoverage == nil || *g.AboveCoverage != 800 {
		t.Fatalf("groups: %+v", g)
	}
	delete(row, "groups_covered")
	if g := parseReadinessGroups(row, 21); g.AboveCoverage != nil {
		t.Fatalf("no gate, got %+v", g)
	}
	if g := parseReadinessGroups(map[string]interface{}{}, 21); g != nil {
		t.Fatalf("ungrouped, got %+v", g)
	}
}

// The summary is extra: failing to read it must not cost the rest of the stats.
func TestGetStatsSurvivesReadinessFailure(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("AS total_rows", fakedb.CHResult{Cols: cols("total_rows", "UInt64"), Rows: [][]any{{uint64(4)}}})
	env.ch.On("AS seen_once", fakedb.CHResult{Err: context.DeadlineExceeded})
	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st["readiness"]; ok || st["total_rows"] != uint64(4) {
		t.Fatalf("readiness from a failed read: %v", st)
	}
}

func TestValidateEstimateHalfDays(t *testing.T) {
	for days, ok := range map[int]bool{0: true, 1: false, 2: true, 14: true, MaxHistoryDays / 2: true, MaxHistoryDays/2 + 1: false, -1: false} {
		err := validateEstimateHalfDays(ModelDefinition{Alert: &AlertConfig{EstimateHalfDays: days}})
		if (err == nil) != ok {
			t.Errorf("%d days: %v", days, err)
		}
	}
	if err := validateEstimateHalfDays(ModelDefinition{}); err != nil {
		t.Errorf("no alert: %v", err)
	}
}

// A model holds days from its creation, or its backfill's start when earlier. A
// dataset's logs and a network model's score ranges carry no such bound.
func TestRecordStart(t *testing.T) {
	created := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	earlier, later := created.AddDate(0, 0, -30), created.AddDate(0, 0, 3)
	cases := []struct {
		name string
		mo   Model
		want time.Time
	}{
		{"created", Model{ModelType: ModelTypeRarity, CreatedAt: created}, created},
		{"backfilled", Model{ModelType: ModelTypeFirstSeen, CreatedAt: created, BackfillFrom: &earlier}, earlier},
		{"backfill after creation", Model{ModelType: ModelTypeVolumeBaseline, CreatedAt: created, BackfillFrom: &later}, created},
		{"dataset", Model{ModelType: ModelTypeRarity, CreatedAt: created, DatasetEnd: &later}, time.Time{}},
		{"network", Model{ModelType: ModelTypeBeacon, CreatedAt: created}, time.Time{}},
	}
	for _, c := range cases {
		if got := recordStart(&c.mo); !got.Equal(c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	if got := recordedDay("d", time.Time{}); got != "d > toDate(0) AND d <= toDate(now(), 'UTC')" {
		t.Errorf("unbounded: %s", got)
	}
	if got := recordedDay("d", earlier); got != "d >= toDate('2026-09-01') AND d <= toDate(now(), 'UTC')" {
		t.Errorf("bounded: %s", got)
	}
}

// The estimate's reads drop days before the model's record start.
func TestGetStatsReadinessFromRecordStart(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	from := time.Date(2026, 8, 20, 15, 0, 0, 0, time.UTC)
	mo.BackfillFrom = &from
	env.ch.On("AS seen_once", summaryResult(readinessStart, readinessStart.AddDate(0, 0, 29), 39))
	env.ch.On("AS forecast", fakedb.CHResult{Cols: cols("forecast", "UInt64", "actual", "UInt64"), Rows: [][]any{{uint64(20), uint64(17)}}})
	if _, err := env.mgr.GetStats(context.Background(), mo, fracA); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"AS seen_once", "AS forecast"} {
		q := env.ch.QueriedMatching(marker)
		if len(q) != 1 || !strings.Contains(q[0], "d >= toDate('2026-08-20') AND d <= toDate(now(), 'UTC')") {
			t.Errorf("%s read is not bounded by the record start: %v", marker, q)
		}
	}
}

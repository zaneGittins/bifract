package models

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// A dataset's fractal: its models are emptied with its logs, its network models
// scored over the dataset's range, and its volume models read as of that range.

var (
	labStart = time.Date(2026, 3, 2, 10, 20, 0, 0, time.UTC)
	labEnd   = time.Date(2026, 3, 4, 17, 45, 0, 0, time.UTC)
)

func seedIn(env *fakeEnv, fractalID, name string, mt ModelType, def ModelDefinition) *modelRow {
	return seedModel(env, mt, def, func(r *modelRow) { r.fractalID, r.name = fractalID, name })
}

// A retry re-ingests every log, so state from the attempt before must go or each
// count doubles. Only this fractal's active models are touched, and nothing about
// the model changes but its data and backfill.
func TestResetFractalEmptiesItsModels(t *testing.T) {
	env := newEnv(t)
	wm := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	done := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) {
		r.bfStatus, r.bfDone, r.bfTotal, r.watermark, r.updatedAt = "completed", 8, 8, &wm, old
	})
	net := seedModel(env, ModelTypeBeacon, ModelDefinition{}, func(r *modelRow) { r.name = "net" })
	broken := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.status = "broken", "error" })
	other := seedIn(env, fracB, "other", ModelTypeRarity, rarityDef)
	running, cancel := context.WithCancel(context.Background())
	defer cancel()
	env.mgr.registerCancel(done.id, cancel)

	if err := env.mgr.ResetFractal(context.Background(), fracA); err != nil {
		t.Fatal(err)
	}
	if running.Err() == nil {
		t.Error("a running backfill would go on writing into the emptied table")
	}
	execs := strings.Join(env.ch.Executed(), "\n")
	for _, table := range []string{done.chTable, net.chTable, chModelStateName(net.id)} {
		if !strings.Contains(execs, "TRUNCATE TABLE IF EXISTS `"+table+"`") {
			t.Errorf("%s was not emptied:\n%s", table, execs)
		}
	}
	for _, table := range []string{broken.chTable, other.chTable} {
		if strings.Contains(execs, "`"+table+"`") {
			t.Errorf("%s was touched:\n%s", table, execs)
		}
	}
	got, _ := env.db.get(done.id)
	if got.status != "active" || got.bfStatus != "none" || !got.watermark.Equal(wm) {
		t.Errorf("after reset: %+v", got)
	}
	if !got.updatedAt.After(old) {
		t.Error("updated_at must move, or a Score range running across the reset writes old scores")
	}
}

func TestResetFractalReportsAFailedClear(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeRarity, rarityDef, nil)
	env.ch.FailExec("TRUNCATE", errors.New("no space"))
	if err := env.mgr.ResetFractal(context.Background(), fracA); err == nil {
		t.Fatal("state left behind must fail the reset, or the retry doubles it")
	}
}

func TestBuildingModels(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeRarity, rarityDef, nil)
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.status = "new", "rebuilding" })
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.status = "broken", "error" })
	seedIn(env, fracB, "elsewhere", ModelTypeRarity, rarityDef)
	names, err := env.mgr.BuildingModels(context.Background(), fracA)
	if err != nil || len(names) != 1 || names[0] != "new" {
		t.Fatalf("names %v err %v", names, err)
	}
}

func scriptLongConn(env *fakeEnv) {
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
	env.ch.On("AS ts_list", longConnPairs(2, labEnd))
}

// Only network models need this, and the range is widened to whole hours so the
// dataset's last log is inside it.
func TestScoreFractalScoresNetworkModelsOverTheRange(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeRarity, rarityDef, nil)
	net := seedModel(env, ModelTypeLongConnection, ModelDefinition{}, func(r *modelRow) { r.name = "net" })
	seedIn(env, fracB, "other", ModelTypeLongConnection, ModelDefinition{})
	scriptLongConn(env)

	if warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd); len(warnings) != 0 {
		t.Fatalf("warnings: %v", warnings)
	}
	batches := env.ch.Batches()
	if len(batches) != 1 || !strings.Contains(batches[0].Query, "`"+net.chTable+"`") {
		t.Fatalf("batches: %+v", batches)
	}
	if q := strings.Join(env.ch.Queried(), "\n"); !strings.Contains(q, "2026-03-04 18:00:00") {
		t.Errorf("the last partial hour is not scored:\n%s", q)
	}
}

// Logs landing in bulk put ingest under pressure and a backfill can hold the
// slot, so a busy scorer is waited out rather than reported as a failure.
func TestScoreFractalWaitsOutABusySlot(t *testing.T) {
	old := scoreBusyPoll
	scoreBusyPoll = time.Millisecond
	t.Cleanup(func() { scoreBusyPoll = old })
	env := newEnv(t)
	seedModel(env, ModelTypeLongConnection, ModelDefinition{}, nil)
	scriptLongConn(env)
	env.mgr.bfSem <- struct{}{}
	time.AfterFunc(20*time.Millisecond, func() { <-env.mgr.bfSem })

	if warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd); len(warnings) != 0 {
		t.Fatalf("warnings: %v", warnings)
	}
	if len(env.ch.Batches()) != 1 {
		t.Errorf("batches: %d", len(env.ch.Batches()))
	}
}

func TestScoreFractalGivesUpOnABusySlot(t *testing.T) {
	oldPoll, oldWait := scoreBusyPoll, scoreBusyWait
	scoreBusyPoll, scoreBusyWait = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { scoreBusyPoll, scoreBusyWait = oldPoll, oldWait })
	env := newEnv(t)
	seedModel(env, ModelTypeLongConnection, ModelDefinition{}, nil)
	env.mgr.bfSem <- struct{}{}
	defer func() { <-env.mgr.bfSem }()

	warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "busy") {
		t.Fatalf("warnings: %v", warnings)
	}
}

// One model failing leaves the others scored, says which, and keeps internal
// detail out of what is shown.
func TestScoreFractalReportsEachFailure(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeLongConnection, ModelDefinition{}, func(r *modelRow) { r.name = "beta" })
	env.ch.On("AS network_size", fakedb.CHResult{Err: errors.New("code: 241 on host ch-0.int")})

	warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "beta") || strings.Contains(warnings[0], "ch-0.int") {
		t.Fatalf("warnings: %v", warnings)
	}
}

func TestScoreFractalRefusesARangeOverTheLimit(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeBeacon, ModelDefinition{}, nil)
	warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labStart.Add(100*24*time.Hour))
	if len(warnings) != 1 || !strings.Contains(warnings[0], "90 days") || !strings.Contains(warnings[0], "part of it") {
		t.Fatalf("warnings: %v", warnings)
	}
}

// Rounding up to the hour must not refuse a dataset that fits.
func TestScoreFractalAcceptsExactlyTheLimit(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeLongConnection, ModelDefinition{}, nil)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	start := time.Date(2026, 3, 1, 10, 20, 0, 0, time.UTC)
	if warnings := env.mgr.ScoreFractal(context.Background(), fracA, start, start.Add(previewMaxSpan)); len(warnings) != 0 {
		t.Fatalf("warnings: %v", warnings)
	}
}

// A model that failed to build recorded nothing, which must not pass silently.
func TestScoreFractalNamesInactiveModels(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.status = "broken", "error" })
	warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "broken") {
		t.Fatalf("warnings: %v", warnings)
	}
}

// Some pairs were written, so this must not read as a model left unscored.
func TestScoreFractalSaysWhenItTruncates(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeLongConnection, ModelDefinition{}, nil)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
	env.ch.On("AS ts_list", longConnPairs(scorerPerModelPairCap+1, labEnd))
	warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "first") || strings.Contains(warnings[0], "not scored") {
		t.Fatalf("warnings: %v", warnings)
	}
}

// The wait is one budget for the pass, so many models cannot multiply it.
func TestScoreFractalSharesOneWait(t *testing.T) {
	oldPoll, oldWait := scoreBusyPoll, scoreBusyWait
	scoreBusyPoll, scoreBusyWait = time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { scoreBusyPoll, scoreBusyWait = oldPoll, oldWait })
	env := newEnv(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		seedModel(env, ModelTypeLongConnection, ModelDefinition{}, func(r *modelRow) { r.name = name })
	}
	env.mgr.bfSem <- struct{}{}
	defer func() { <-env.mgr.bfSem }()
	began := time.Now()
	if warnings := env.mgr.ScoreFractal(context.Background(), fracA, labStart, labEnd); len(warnings) != 5 {
		t.Fatalf("warnings: %v", warnings)
	}
	if took := time.Since(began); took >= 5*scoreBusyWait {
		t.Errorf("took %v: each model waited in turn", took)
	}
}

func TestScoreFractalStopsWithItsContext(t *testing.T) {
	env := newEnv(t)
	seedModel(env, ModelTypeLongConnection, ModelDefinition{}, nil)
	env.mgr.bfSem <- struct{}{}
	defer func() { <-env.mgr.bfSem }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env.mgr.ScoreFractal(ctx, fracA, labStart, labEnd)
	if len(env.ch.Batches()) != 0 {
		t.Error("a stopped pass wrote scores")
	}
}

// A dataset's newest bucket is in the past, so scoring as of today would compare
// empty buckets. Its volume models score the bucket holding its last log; every
// other fractal reads as of now.
func TestVolumeAsOf(t *testing.T) {
	end := labEnd
	for bucket, want := range map[string]string{
		"day":  "toDateTime('2026-03-05 00:00:00', 'UTC')",
		"":     "toDateTime('2026-03-05 00:00:00', 'UTC')",
		"hour": "toDateTime('2026-03-04 18:00:00', 'UTC')",
	} {
		mo := &Model{DatasetEnd: &end, Definition: ModelDefinition{TimeBucket: bucket}}
		if got := volumeAsOf(mo); got != want {
			t.Errorf("%q: %s, want %s", bucket, got, want)
		}
	}
	if got := volumeAsOf(&Model{}); got != "" {
		t.Errorf("a live fractal reads as of %q", got)
	}
}

// A dataset's source stopped when it was recorded, which is not a fault.
func TestDatasetModelIsNotStale(t *testing.T) {
	now := labEnd.Add(30 * 24 * time.Hour)
	end := labEnd
	mo := healthModel(ModelTypeFirstSeen, ModelDefinition{})
	sum := &stateSummary{oldest: labStart, newest: labStart.Add(time.Hour)}
	mo.DatasetEnd = &end
	if h := deriveHealth(mo, sum, nil, now); h.State != HealthHealthy && h.State != HealthLearning {
		t.Errorf("health = %+v", h)
	}
	mo.DatasetEnd = nil
	if h := deriveHealth(mo, sum, nil, now); h.State != HealthStale {
		t.Errorf("a live source that stopped: %+v", h)
	}
}

// A dataset older than a network model's window expires from its state at once,
// and what is left is the scores from the import.
func TestDatasetNetworkModelKeepsItsScores(t *testing.T) {
	now := labEnd.Add(30 * 24 * time.Hour)
	end := labEnd
	mo := healthModel(ModelTypeBeacon, ModelDefinition{})
	mo.DatasetEnd = &end
	mo.CreatedAt = labEnd
	h := deriveHealth(mo, &stateSummary{scored: labEnd}, nil, now)
	if h.State != HealthScored || strings.Contains(h.Detail, "live scoring") {
		t.Errorf("health = %+v", h)
	}
}

func TestVolumeDataUsesTheDatasetsEnd(t *testing.T) {
	env := newEnv(t)
	end := labEnd
	env.db.fractalEnds = map[string]*time.Time{fracA: &end}
	r := seedModel(env, ModelTypeVolumeBaseline, ModelDefinition{PartitionKey: "host", TimeBucket: "day"}, nil)
	mo, err := env.mgr.Get(context.Background(), r.id)
	if err != nil {
		t.Fatal(err)
	}
	env.mgr.GetData(context.Background(), mo, fracA, DataQuery{Limit: 10})
	env.mgr.GetStats(context.Background(), mo, fracA)
	env.mgr.GetHistogram(context.Background(), mo, fracA)
	env.mgr.readStateSummary(context.Background(), mo, time.Now())
	for _, q := range env.ch.Queried() {
		if strings.Contains(q, "AS latest_bucket") && !strings.Contains(q, "toDate(toDateTime('2026-03-05 00:00:00', 'UTC'))") {
			t.Errorf("read as of now:\n%s", q)
		}
	}
	if len(env.ch.QueriedMatching("AS latest_bucket")) < 4 {
		t.Errorf("%d scored reads, want one each for data, stats, histogram and health", len(env.ch.QueriedMatching("AS latest_bucket")))
	}
}

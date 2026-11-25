package models

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// Scoring a past range: the preview's raw-log pipeline, written to the results
// table as of the range's end, leaving the rolling state and the cursor alone.

func scoreRangeModel(t *testing.T, mt ModelType) (*fakeEnv, *Model, *modelRow) {
	t.Helper()
	env := newEnv(t)
	r := seedModel(env, mt, ModelDefinition{}, nil)
	mo, err := env.mgr.Get(context.Background(), r.id)
	if err != nil {
		t.Fatal(err)
	}
	return env, mo, r
}

// scoreRangeNow queues a job and runs the queue as a replica would, returning how
// the job ended.
func scoreRangeNow(env *fakeEnv, mo *Model, req ScoreRangeRequest) (*ScoreRangeResult, error) {
	if _, err := env.mgr.QueueScoreRange(context.Background(), mo, req, false); err != nil {
		return nil, err
	}
	env.mgr.runQueuedScoreRanges(context.Background())
	cur, err := env.mgr.Get(context.Background(), mo.ID)
	if err != nil {
		return nil, err
	}
	if j := cur.ScoreRange; j.Status != scoreRangeCompleted {
		return nil, fmt.Errorf("job %s: %s", j.Status, j.Error)
	}
	return cur.ScoreRange.Result, nil
}

// The same long-connection pairs as TestPreviewNetworkLongConnection score the
// same (0.65, 0.639, 0.25, 0), and every row carries the range's end as scored_at.
func TestScoreRangeWritesPreviewScoresAtRangeEnd(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeLongConnection)
	from := time.Date(2026, 3, 2, 10, 20, 0, 0, time.UTC)
	to := time.Date(2026, 3, 4, 17, 45, 0, 0, time.UTC)
	end := uint32(to.Unix())
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1, "10.0.0.53": 60, "198.51.100.7": 10}))
	env.ch.On("AS ts_list", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.3", "198.51.100.7", 1, []uint32{end - 3600}, []float64{1}, 14400),
		statePair("10.1.1.1", "203.0.113.5", 3, []uint32{end - 3600}, []float64{1}, 28800),
		statePair("10.1.1.4", "192.0.2.9", 1, []uint32{end - 3600}, []float64{1}, 3600),
		statePair("10.1.1.2", "10.0.0.53", 2, []uint32{end - 3600}, []float64{5}, 50000),
	}})

	before := time.Now()
	res, err := scoreRangeNow(env, mo, ScoreRangeRequest{From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	wantStart := time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 3, 4, 17, 0, 0, 0, time.UTC)
	if !res.Start.Equal(wantStart) || !res.End.Equal(wantEnd) || res.PairsScored != 4 || res.Truncated {
		t.Errorf("result: %+v", res)
	}

	batches := env.ch.Batches()
	if len(batches) != 1 || !batches[0].Sent {
		t.Fatalf("batches: %+v", batches)
	}
	if !strings.HasPrefix(batches[0].Query, "INSERT INTO `"+r.chTable+"` (") {
		t.Errorf("insert: %s", batches[0].Query)
	}
	want := map[string]float64{"203.0.113.5": 0.65, "10.0.0.53": 0.639, "198.51.100.7": 0.25, "192.0.2.9": 0}
	for _, row := range batches[0].Appended {
		dst := row[2].(string)
		if math.Abs(row[14].(float64)-want[dst]) > 1e-9 || row[0] != fracA {
			t.Errorf("%s: %v", dst, row)
		}
		if !row[19].(time.Time).Equal(wantEnd) {
			t.Errorf("%s scored_at %v, want the range end %v", dst, row[19], wantEnd)
		}
	}

	// Raw logs over the range only, at backfill priority; no DDL, no state.
	for _, q := range env.ch.Queried() {
		if !strings.Contains(q, "os_thread_priority=19") || !strings.Contains(q, wantStart.Format("2006-01-02 15:04:05")) {
			t.Errorf("query not bounded to the range at low priority: ...%s", q[max(0, len(q)-200):])
		}
		if strings.Contains(q, chModelStateName(r.id)) {
			t.Errorf("read the rolling state: %s", q)
		}
	}
	if len(env.ch.Executed()) != 0 {
		t.Errorf("scoring a range ran DDL: %v", env.ch.Executed())
	}
	got, _ := env.db.get(r.id)
	if got.lastScored != nil {
		t.Error("scoring a range moved the live scorer's cursor")
	}
	// The range is the model's pass as of its end, so its pairs are the findings.
	if got.scoredPass == nil || !got.scoredPass.Equal(wantEnd) {
		t.Errorf("pass %v, want the range end", got.scoredPass)
	}
	// Outside a dataset the scores expire like a live pass written now. Either
	// day is right if the run crossed midnight.
	if exp := batches[0].Appended[0][20].(time.Time); !exp.Equal(netResultsExpiry(before)) && !exp.Equal(netResultsExpiry(time.Now())) {
		t.Errorf("expires %v", exp)
	}
}

// A dataset's range scores never expire, since no live pass scores its fractal
// again, and its latest range is its findings even when it ends earlier. In a
// live fractal a past range never moves the pass back: it is history.
func TestScoreRangePass(t *testing.T) {
	end := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)
	live := time.Now().UTC().Truncate(time.Millisecond)
	for _, dataset := range []bool{true, false} {
		env := newEnv(t)
		if dataset {
			env.db.fractalEnds = map[string]*time.Time{fracA: &end}
		}
		r := seedModel(env, ModelTypeLongConnection, ModelDefinition{}, func(r *modelRow) { r.scoredPass = &live })
		mo, err := env.mgr.Get(context.Background(), r.id)
		if err != nil {
			t.Fatal(err)
		}
		env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(10)))
		env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
		env.ch.On("AS ts_list", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
			statePair("10.1.1.1", "203.0.113.5", 3, []uint32{uint32(end.Unix()) - 7200}, []float64{1}, 28800),
		}})
		from, to := end.Add(-48*time.Hour), end
		if _, err := scoreRangeNow(env, mo, ScoreRangeRequest{From: &from, To: &to}); err != nil {
			t.Fatal(err)
		}
		exp := env.ch.Batches()[0].Appended[0][20].(time.Time)
		got, _ := env.db.get(r.id)
		if dataset && (!exp.Equal(netResultsNever) || !got.scoredPass.Equal(end)) {
			t.Errorf("dataset: expires %v, pass %v, want never and the range end", exp, got.scoredPass)
		}
		if !dataset && (exp.Equal(netResultsNever) || !got.scoredPass.Equal(live)) {
			t.Errorf("live: expires %v, pass %v, want the live pass kept", exp, got.scoredPass)
		}
	}
}

// A range ending in the current hour scores to the next one, but the pass is
// never later than now, or it would hide every live pair until then.
func TestScoreRangePassIsNeverInTheFuture(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeLongConnection)
	now := time.Now().UTC()
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(10)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
	env.ch.On("AS ts_list", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 3, []uint32{uint32(now.Unix()) - 60}, []float64{1}, 28800),
	}})
	from, to := now.Add(-time.Minute), now
	if _, err := scoreRangeNow(env, mo, ScoreRangeRequest{From: &from, To: &to}); err != nil {
		t.Fatal(err)
	}
	if got, _ := env.db.get(r.id); got.scoredPass == nil || got.scoredPass.After(time.Now()) {
		t.Errorf("pass %v is in the future", got.scoredPass)
	}
}

// Only active network models score a range, over a valid one; nothing is read
// or written when refused.
func TestScoreRangeGuards(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	long := from.Add(-100 * 24 * time.Hour)
	cases := []struct {
		name string
		edit func(*Model)
		req  ScoreRangeRequest
		want string
	}{
		{"not a network model", func(m *Model) { m.ModelType = ModelTypeRarity }, ScoreRangeRequest{From: &from, To: &to}, "only beacon and long_connection"},
		{"inactive", func(m *Model) { m.Status = "rebuilding" }, ScoreRangeRequest{From: &from, To: &to}, "not active"},
		{"no table", func(m *Model) { m.CHTableName = "" }, ScoreRangeRequest{From: &from, To: &to}, "no data table"},
		{"no range", func(*Model) {}, ScoreRangeRequest{From: &from}, "needs both"},
		{"too long", func(*Model) {}, ScoreRangeRequest{From: &long, To: &to}, "90 days"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, mo, _ := scoreRangeModel(t, ModelTypeBeacon)
			c.edit(mo)
			_, err := scoreRangeNow(env, mo, c.req)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want %q", err, c.want)
			}
			if len(env.ch.Queried()) != 0 || len(env.ch.Batches()) != 0 {
				t.Error("a refused range touched ClickHouse")
			}
		})
	}
}

// An empty range writes nothing; a failed read or write is reported.
func TestScoreRangeEmptyAndFailures(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	req := ScoreRangeRequest{From: &from, To: &to}

	env, mo, _ := scoreRangeModel(t, ModelTypeBeacon)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	res, err := scoreRangeNow(env, mo, req)
	if err != nil || res.PairsScored != 0 || len(env.ch.Batches()) != 0 {
		t.Errorf("empty range: %+v %v batches %d", res, err, len(env.ch.Batches()))
	}

	for name, script := range map[string]func(*fakeEnv){
		"aggregation": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
			env.ch.On("AS ts_list", fakedb.CHResult{Err: errors.New("x")})
		},
		"write": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
			env.ch.On("AS ts_list", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
				statePair("10.1.1.1", "203.0.113.5", 1, []uint32{uint32(to.Unix() - 7200)}, []float64{1}, 30000),
			}})
			env.ch.OnBatch = func(b *fakedb.Batch) { b.SendErr = errors.New("too many parts") }
		},
	} {
		t.Run(name, func(t *testing.T) {
			env, mo, _ := scoreRangeModel(t, ModelTypeLongConnection)
			script(env)
			if _, err := scoreRangeNow(env, mo, req); err == nil || !strings.Contains(err.Error(), name) {
				t.Errorf("err = %v, want it to name the %s", err, name)
			}
		})
	}
}

func longConnPairs(n int, end time.Time) fakedb.CHResult {
	res := fakedb.CHResult{Cols: stateCols, Rows: make([][]any, n)}
	ts := uint32(end.Add(-2 * time.Hour).Unix())
	for i := range res.Rows {
		res.Rows[i] = statePair(fmt.Sprintf("10.1.%d.%d", i/250, i%250), "203.0.113.5", 1, []uint32{ts}, []float64{1}, 30000)
	}
	return res
}

// Every score lands in one insert after the whole read succeeds, so a run cut
// short writes nothing rather than a partial set that looks complete.
func TestScoreRangeWritesOnceAtTheEnd(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	req := ScoreRangeRequest{From: &from, To: &to}

	env, mo, _ := scoreRangeModel(t, ModelTypeLongConnection)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
	env.ch.On("AS ts_list", longConnPairs(2500, to))
	res, err := scoreRangeNow(env, mo, req)
	if err != nil || res.PairsScored != 2500 {
		t.Fatalf("%+v %v", res, err)
	}
	if b := env.ch.Batches(); len(b) != 1 || len(b[0].Appended) != 2500 {
		t.Errorf("want one insert of 2500 rows, got %d batches", len(b))
	}

	env, mo, _ = scoreRangeModel(t, ModelTypeLongConnection)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
	cut := longConnPairs(2500, to)
	cut.IterErr = errors.New("context deadline exceeded")
	env.ch.On("AS ts_list", cut)
	if _, err := scoreRangeNow(env, mo, req); err == nil {
		t.Fatal("a cut-short read succeeded")
	}
	if len(env.ch.Batches()) != 0 {
		t.Error("a cut-short read wrote scores")
	}
}

// A model edited mid-read cancels the run, and the run notices before it writes:
// its scores belong to the old definition.
func TestScoreRangeEditedMidRunWritesNothing(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	env, mo, r := scoreRangeModel(t, ModelTypeLongConnection)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
	env.ch.Respond(func(q string, _ []any) (fakedb.CHResult, bool) {
		if !strings.Contains(q, "AS ts_list") {
			return fakedb.CHResult{}, false
		}
		def := mo.Definition
		def.LongConn = &LongConnParams{ScoreThreshold: 0.9}
		if _, err := env.mgr.Update(context.Background(), r.id, UpdateRequest{Name: mo.Name, Definition: def}); err != nil {
			t.Error(err)
		}
		return longConnPairs(3, to), true
	})
	_, err := scoreRangeNow(env, mo, ScoreRangeRequest{From: &from, To: &to})
	if err == nil || !strings.Contains(err.Error(), "cancelled") || !strings.Contains(err.Error(), "definition changed") {
		t.Errorf("err = %v", err)
	}
	for _, b := range env.ch.Batches() {
		if strings.Contains(b.Query, r.chTable) {
			t.Errorf("an edited model's run wrote scores: %s", b.Query)
		}
	}
}

// Queueing returns at once, before any read, with the range resolved to whole
// hours; the job runs on the replica's worker.
func TestQueueScoreRangeReturnsBeforeReading(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeBeacon)
	from := time.Date(2026, 3, 2, 10, 20, 0, 0, time.UTC)
	to := time.Date(2026, 3, 4, 17, 45, 0, 0, time.UTC)
	job, err := env.mgr.QueueScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(env.ch.Queried()) != 0 {
		t.Error("queueing read ClickHouse")
	}
	got, _ := env.db.get(r.id)
	wantFrom, wantTo := time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC), time.Date(2026, 3, 4, 17, 0, 0, 0, time.UTC)
	if got.sr.status != "queued" || !got.sr.from.Equal(wantFrom) || !got.sr.to.Equal(wantTo) || !got.sr.queued.Equal(*job.QueuedAt) {
		t.Errorf("row: %+v", got.sr)
	}
	select {
	case <-env.mgr.srNudge:
	default:
		t.Error("the worker was not woken")
	}
}

// One job per model: a second is refused while one is queued or running, unless
// it replaces it, and then the replaced run's writes no longer match.
func TestQueueScoreRangeOnePerModel(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeBeacon)
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	req := ScoreRangeRequest{From: &from, To: &to}
	first, err := env.mgr.QueueScoreRange(context.Background(), mo, req, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.mgr.QueueScoreRange(context.Background(), mo, req, false); !errors.Is(err, errScoreRangeActive) {
		t.Errorf("err = %v", err)
	}
	time.Sleep(time.Microsecond)
	second, err := env.mgr.QueueScoreRange(context.Background(), mo, req, true)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := env.db.get(r.id); !got.sr.queued.Equal(*second.QueuedAt) || got.sr.queued.Equal(*first.QueuedAt) {
		t.Errorf("not replaced: %+v", got.sr)
	}
}

// Another replica holding the lock runs the queue; this one leaves it alone.
func TestScoreRangesRunOneReplicaAtATime(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeBeacon)
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	if _, err := env.mgr.QueueScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to}, false); err != nil {
		t.Fatal(err)
	}
	env.db.setLockHeld(true)
	env.mgr.runQueuedScoreRanges(context.Background())
	if got, _ := env.db.get(r.id); got.sr.status != "queued" || len(env.ch.Queried()) != 0 {
		t.Errorf("ran without the lock: %+v", got.sr)
	}
}

// Holding the lock, a job still marked running was left by a replica that
// stopped, so it runs again from the start.
func TestScoreRangeResumesARunLeftByAStoppedReplica(t *testing.T) {
	env, _, r := scoreRangeModel(t, ModelTypeBeacon)
	from := time.Now().UTC().Truncate(time.Hour).Add(-48 * time.Hour)
	to, queued := from.Add(24*time.Hour), time.Now().UTC().Add(-time.Hour)
	env.db.mu.Lock()
	env.db.rows[r.id].sr = srRow{status: "running", phase: "pairs", from: &from, to: &to, queued: &queued}
	env.db.mu.Unlock()
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	env.mgr.runQueuedScoreRanges(context.Background())
	if got, _ := env.db.get(r.id); got.sr.status != "completed" || got.sr.err != "" {
		t.Errorf("row: %+v", got.sr)
	}
}

// A run reports each step as it reaches it, then clears its phase when done.
func TestScoreRangeRecordsProgress(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeLongConnection)
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
	env.ch.On("AS ts_list", longConnPairs(3, to))
	if _, err := scoreRangeNow(env, mo, ScoreRangeRequest{From: &from, To: &to}); err != nil {
		t.Fatal(err)
	}
	want := []string{phaseNetwork, phasePrevalence, phaseFirstSeen, phasePairs, phaseWriting}
	env.db.mu.Lock()
	phases := append([]string(nil), env.db.phases...)
	env.db.mu.Unlock()
	if strings.Join(phases, ",") != strings.Join(want, ",") {
		t.Errorf("phases %v, want %v", phases, want)
	}
	if got, _ := env.db.get(r.id); got.sr.phase != "" {
		t.Errorf("phase left at %q", got.sr.phase)
	}
}

// A cancelled job never runs; Cancel on a finished one changes nothing.
func TestCancelScoreRange(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeBeacon)
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	if _, err := env.mgr.QueueScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to}, false); err != nil {
		t.Fatal(err)
	}
	if err := env.mgr.CancelScoreRange(context.Background(), r.id, ""); err != nil {
		t.Fatal(err)
	}
	env.mgr.runQueuedScoreRanges(context.Background())
	if got, _ := env.db.get(r.id); got.sr.status != "cancelled" || len(env.ch.Queried()) != 0 {
		t.Errorf("row %+v, queries %d", got.sr, len(env.ch.Queried()))
	}
	env.db.mu.Lock()
	env.db.rows[r.id].sr.status = "completed"
	env.db.mu.Unlock()
	if err := env.mgr.CancelScoreRange(context.Background(), r.id, ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := env.db.get(r.id); got.sr.status != "completed" {
		t.Errorf("cancel rewrote a finished job: %+v", got.sr)
	}
}

// A run waits for the raw-log slot a backfill holds rather than failing, and
// a run tells ClickHouse its own budget, not a request's.
func TestScoreRangeWaitsForTheSlot(t *testing.T) {
	env, mo, r := scoreRangeModel(t, ModelTypeBeacon)
	env.mgr.bfCfg.scoreRangeTimeoutSec = 4321
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	if _, err := env.mgr.QueueScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to}, false); err != nil {
		t.Fatal(err)
	}
	env.mgr.bfSem <- struct{}{}
	done := make(chan struct{})
	go func() { env.mgr.runQueuedScoreRanges(context.Background()); close(done) }()
	waitFor(t, "the run to claim the job", func() bool { got, _ := env.db.get(r.id); return got.sr.status == "running" })
	if len(env.ch.Queried()) != 0 {
		t.Error("read without the slot")
	}
	<-env.mgr.bfSem
	<-done
	if got, _ := env.db.get(r.id); got.sr.status != "completed" {
		t.Errorf("row: %+v", got.sr)
	}
	for _, q := range env.ch.Queried() {
		if !strings.Contains(q, "max_execution_time=4321,") {
			t.Errorf("query not bounded by the job's budget: ...%s", q[max(0, len(q)-200):])
		}
	}
}

// An empty preview still says which linked modifier cannot be read.
func TestPreviewNetworkEmptyWindowKeepsModifierWarnings(t *testing.T) {
	env := newEnv(t)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	def := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "missing_lookup", ThreatIntelColumn: "ip"}}
	res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeBeacon, def, lastDay())
	if err != nil {
		t.Fatal(err)
	}
	if w, _ := res.Stats["modifier_warning"].(string); w == "" {
		t.Errorf("no modifier warning on an empty window: %v", res.Stats)
	}
}

func TestScoreRangeReadsRunOnTheAlertIdentity(t *testing.T) {
	env, mo, _ := scoreRangeModel(t, ModelTypeLongConnection)
	from := time.Date(2026, 3, 2, 10, 20, 0, 0, time.UTC)
	to := time.Date(2026, 3, 4, 17, 45, 0, 0, time.UTC)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
	env.ch.On("AS ts_list", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 3, []uint32{uint32(to.Unix()) - 3600}, []float64{1}, 28800),
	}})
	reads := recordReads(env)

	if _, err := scoreRangeNow(env, mo, ScoreRangeRequest{From: &from, To: &to}); err != nil {
		t.Fatal(err)
	}
	assertScoringReads(t, reads(), 3)
}

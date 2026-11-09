package models

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
)

// The scorer end to end: rolling state in, scored rows out, through the same
// ClickHouse shapes the real driver hands back.

var stateCols = cols("src", "String", "dst", "String", "port", "String", "cnt", "UInt64",
	"ts_list", "Array(UInt32)", "size_list", "Array(Float64)", "total_duration", "Float64",
	"first_ts", "UInt32", "last_ts", "UInt32")

// statePair is one pair-read row. Its points are ascending, as the read sorts
// them, and its first and last seen span them.
func statePair(src, dst string, cnt uint64, ts []uint32, sizes []float64, dur float64) []any {
	if !slices.IsSorted(ts) {
		panic("statePair: the pair read returns points in time order")
	}
	return []any{src, dst, "443", cnt, ts, sizes, dur, slices.Min(ts), slices.Max(ts)}
}

func prevalenceRows(m map[string]uint64) fakedb.CHResult {
	res := fakedb.CHResult{Cols: cols("dst", "String", "prev_total", "UInt64")}
	for d, n := range m {
		res.Rows = append(res.Rows, []any{d, n})
	}
	return res
}

func scorerEnv(t *testing.T, cluster bool, mt ModelType, def ModelDefinition) (*fakeEnv, *ScorerEngine, *modelRow) {
	env := newEnv(t)
	if cluster {
		env = newClusterEnv(t)
	}
	r := seedModel(env, mt, def, nil)
	return env, NewScorerEngine(env.db.client, env.ch.client, env.mgr), r
}

// Long-connection scores, hand-computed from the default tiers (1h base, 4h/8h/12h)
// and the prevalence nudge (+0.15 at <= 2% of the network, -0.15 at >= 50%):
//
//	8h on a destination 1 of 100 sources reach:   tier 0.5            + 0.15 = 0.65
//	~13.9h on one 60 of 100 reach:  0.75 + 0.25*(6800/43200) = 0.789  - 0.15 = 0.639
//	4h on one 10 of 100 reach:      tier 0.25, no nudge                      = 0.25
func TestScorerLongConnectionEndToEnd(t *testing.T) {
	env, e, r := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	now := uint32(time.Now().Unix())
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1, "10.0.0.53": 60, "198.51.100.7": 10}))
	env.ch.On("groupArraySortedMerge", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 3, []uint32{now - 7200, now - 3600, now - 600}, []float64{1, 1, 1}, 28800),
		statePair("10.1.1.2", "10.0.0.53", 2, []uint32{now - 120, now - 60}, []float64{5, 5}, 50000),
		statePair("10.1.1.3", "198.51.100.7", 1, []uint32{now - 60}, []float64{1}, 14400),
	}})

	e.scoreCycle()

	batches := env.ch.Batches()
	if len(batches) != 1 || !batches[0].Sent {
		t.Fatalf("batches: %+v", batches)
	}
	if !strings.HasPrefix(batches[0].Query, "INSERT INTO `"+r.chTable+"` (fractal_id, src_ip, dst_ip, dst_port, regularity_score") {
		t.Errorf("insert: %s", batches[0].Query)
	}
	rows := batches[0].Appended
	if len(rows) != 3 {
		t.Fatalf("%d rows written, want 3", len(rows))
	}
	want := map[string]struct {
		regularity, prevalence, mod, final float64
		prevTotal                          uint64
	}{
		"203.0.113.5":  {0.5, 0.01, 0.15, 0.65, 1},
		"10.0.0.53":    {0.789, 0.6, -0.15, 0.639, 60},
		"198.51.100.7": {0.25, 0.1, 0, 0.25, 10},
	}
	for _, row := range rows {
		dst := row[2].(string)
		w, ok := want[dst]
		if !ok {
			t.Errorf("unexpected row for %s", dst)
			continue
		}
		if row[0] != fracA || row[4] != w.regularity || row[9] != w.prevalence || row[10] != w.prevTotal ||
			row[11] != w.mod || math.Abs(row[14].(float64)-w.final) > 1e-9 {
			t.Errorf("%s: %v", dst, row)
		}
	}
	first := rows[0]
	if !first[17].(time.Time).Equal(time.Unix(int64(now-7200), 0)) || !first[18].(time.Time).Equal(time.Unix(int64(now-600), 0)) {
		t.Errorf("first/last seen come from the sorted in-window timestamps: %v %v", first[17], first[18])
	}
	got, _ := env.db.get(r.id)
	if got.lastScored == nil {
		t.Error("cursor not advanced after a successful pass")
	}
	// The pass is recorded as the scored_at its rows carry, which is what the
	// findings and the lookup compare against.
	for _, row := range rows {
		if got.scoredPass == nil || !row[19].(time.Time).Equal(*got.scoredPass) {
			t.Fatalf("row scored_at %v, recorded pass %v", row[19], got.scoredPass)
		}
	}
	if exp := rows[0][20].(time.Time); !exp.Equal(netResultsExpiry(*got.scoredPass)) {
		t.Errorf("rows expire %v, want %d days after their pass", exp, netResultsRetentionDays)
	}
	if got.scoredPass.Nanosecond()%int(time.Millisecond) != 0 {
		t.Errorf("pass %v is finer than the column's milliseconds, so its own rows compare below it", got.scoredPass)
	}
	for _, q := range env.ch.Queried() {
		if strings.Contains(q, "FROM "+chModelStateName(r.id)) && !strings.Contains(q, "fractal_id = '"+fracA+"'") {
			t.Errorf("state read not scoped to the model's fractal: %s", q)
		}
	}
}

// A beacon that fires every hour with a constant payload is as regular as a pair
// can be: every subscore is 1, and the rare destination's nudge cannot push it
// past 1.
func TestScorerPerfectBeacon(t *testing.T) {
	env, e, _ := scorerEnv(t, true, ModelTypeBeacon, ModelDefinition{})
	now := time.Now().Unix()
	start := now - 23*3600 - 1800
	var ts []uint32
	var sizes []float64
	for i := 0; i < 24; i++ {
		ts = append(ts, uint32(start+int64(i)*3600))
		sizes = append(sizes, 512)
	}
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
	env.ch.On("groupArraySortedMerge", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 24, ts, sizes, 0),
	}})
	e.scoreCycle()
	b := env.ch.Batches()
	if len(b) != 1 || len(b[0].Appended) != 1 {
		t.Fatalf("batches: %+v", b)
	}
	row := b[0].Appended[0]
	for i, name := range map[int]string{4: "regularity", 5: "ts", 6: "ds", 7: "dur", 8: "hist", 14: "final"} {
		if row[i] != 1.0 {
			t.Errorf("%s score = %v, want 1", name, row[i])
		}
	}
	// On a cluster the state is read across shards, through its companion.
	if len(env.ch.QueriedMatching("FROM model_diststate_")) != 3 {
		t.Errorf("state reads: %v", env.ch.Queried())
	}
}

// Each model rescores on its own cadence; one scored recently is left alone.
func TestScorerRespectsCadence(t *testing.T) {
	env, e, r := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	recent := time.Now().Add(-10 * time.Minute)
	env.db.mu.Lock()
	env.db.rows[r.id].lastScored = &recent
	env.db.mu.Unlock()
	e.scoreCycle()
	if len(env.ch.Queried()) != 0 {
		t.Error("a model inside its rescore interval was read")
	}
}

// Nothing is advanced on a pass that did not complete: on a failed read or write
// neither the cursor nor the pass moves, so the next tick retries and the
// findings keep the last complete pass. With no state yet (a model warming up,
// or a dataset's fractal, whose range scores are its findings) neither moves.
func TestScorerCursorOnlyAdvancesOnSuccess(t *testing.T) {
	pair := fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 1, []uint32{uint32(time.Now().Unix() - 60)}, []float64{1}, 30000),
	}}
	cases := map[string]func(env *fakeEnv){
		"warming up": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
		},
		"size read fails": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.CHResult{Err: errors.New("unknown table")})
		},
		"prevalence read fails": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(5)))
			env.ch.On("AS prev_total", fakedb.CHResult{Err: errors.New("timeout")})
		},
		"state read fails": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(5)))
			env.ch.On("groupArraySortedMerge", fakedb.CHResult{Cols: stateCols, IterErr: errors.New("connection reset")})
		},
		"insert fails": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(5)))
			env.ch.On("groupArraySortedMerge", pair)
			env.ch.OnBatch = func(b *fakedb.Batch) { b.SendErr = errors.New("too many parts") }
		},
		"insert cannot be prepared": func(env *fakeEnv) {
			env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(5)))
			env.ch.On("groupArraySortedMerge", pair)
			env.ch.PrepareErr = errors.New("no such table")
		},
	}
	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			env, e, r := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
			script(env)
			e.scoreCycle()
			got, _ := env.db.get(r.id)
			if got.lastScored != nil {
				t.Error("cursor advanced")
			}
			if got.scoredPass != nil {
				t.Error("an incomplete pass was recorded")
			}
		})
	}
}

// Only the elected replica scores, and none does under ingest pressure.
func TestScorerYields(t *testing.T) {
	env, e, _ := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	env.db.setLockHeld(true)
	e.scoreCycle()
	env.db.setLockHeld(false)
	e.SetIngestPressureFunc(func() bool { return true })
	e.scoreCycle()
	if len(env.db.pg.Matching("FROM analytics_models")) != 0 || len(env.ch.Queried()) != 0 {
		t.Error("scored without the lock or under pressure")
	}
}

// A pathological model is cut off at the per-cycle pair cap, in bounded batches,
// and the pass still completes.
func TestScorerPairCap(t *testing.T) {
	if testing.Short() {
		t.Skip("scores 50k pairs")
	}
	env, e, r := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	ts := []uint32{uint32(time.Now().Unix() - 60)}
	res := fakedb.CHResult{Cols: stateCols, Rows: make([][]any, scorerPerModelPairCap+5)}
	for i := range res.Rows {
		res.Rows[i] = statePair("10.0.0.1", "10.9.9.9", 1, ts, []float64{1}, 30000)
	}
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(5)))
	env.ch.On("groupArraySortedMerge", res)
	e.scoreCycle()
	total := 0
	for _, b := range env.ch.Batches() {
		if len(b.Appended) > scorerResultFlushRows {
			t.Errorf("batch of %d rows exceeds the flush bound", len(b.Appended))
		}
		total += len(b.Appended)
	}
	if total != scorerPerModelPairCap {
		t.Errorf("%d pairs scored, want the cap %d", total, scorerPerModelPairCap)
	}
	if got, _ := env.db.get(r.id); got.lastScored == nil || got.scoredPass == nil {
		t.Error("a capped pass must still advance the cursor and the pass")
	}
}

func TestScorerLoop(t *testing.T) {
	env, e, _ := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	e.Start(5 * time.Millisecond)
	defer e.Stop()
	waitFor(t, "a scoring tick", func() bool { return len(env.ch.QueriedMatching("AS network_size")) > 0 })
}

// recordReads captures the class and sent id of every read, and how many there were.
func recordReads(env *fakeEnv) func() []string {
	var mu sync.Mutex
	var reads []string
	env.ch.CtxArgs = func(ctx context.Context) []any {
		mu.Lock()
		defer mu.Unlock()
		reads = append(reads, storage.QueryClassOf(ctx)+" "+storage.SentQueryIDOf(ctx))
		return nil
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), reads...)
	}
}

func assertScoringReads(t *testing.T, reads []string, min int) {
	t.Helper()
	if len(reads) < min {
		t.Fatalf("%d reads, want at least %d", len(reads), min)
	}
	for _, read := range reads {
		if !strings.HasPrefix(read, storage.QueryAlertClass+" ") || !strings.HasSuffix(read, "~bfscope~"+fracA+",") {
			t.Errorf("read sent as %q", read)
		}
	}
}

// Scoring reads embed the model's source BQL, so they run read-only and confined to
// the model's fractal, and the scored rows are still written.
func TestScorerReadsRunOnTheAlertIdentity(t *testing.T) {
	env, e, _ := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	now := uint32(time.Now().Unix())
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1}))
	env.ch.On("groupArraySortedMerge", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 3, []uint32{now - 7200, now - 3600, now - 600}, []float64{1, 1, 1}, 28800),
	}})
	reads := recordReads(env)

	e.scoreCycle()

	assertScoringReads(t, reads(), 3)
	if b := env.ch.Batches(); len(b) != 1 || !b[0].Sent {
		t.Fatalf("batches: %+v", b)
	}
}

// analytics_models.fractal_id is nullable and the scorer reads NULL as empty. That
// must stay a read of the unassigned rows, not a read the scope refuses.
func TestScoringReadWithNoFractalIsUnassigned(t *testing.T) {
	env := newEnv(t)
	reads := recordReads(env)
	if _, err := env.ch.client.Query(scoringReadContext(context.Background(), ""), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if got := reads(); len(got) != 1 || !strings.HasSuffix(got[0], "~bfscope~,") {
		t.Errorf("reads: %q", got)
	}
}

// The layout checks read system tables across every replica on a cluster, which the
// reader identity cannot do, so they ask for the privileged connection.
func TestLayoutChecksAreAdminReads(t *testing.T) {
	env := newClusterEnv(t)
	var marks []bool
	env.ch.CtxArgs = func(ctx context.Context) []any {
		marks = append(marks, storage.IsAdminRead(ctx))
		return nil
	}
	_, _ = env.mgr.stateLayoutOK(context.Background(), "m1", "t1", ModelDefinition{KeyFields: []string{"k"}}, ModelTypeFirstSeen)
	_, _ = env.mgr.netStateLayoutOK(context.Background(), "m1")
	if len(marks) != 2 {
		t.Fatalf("%d reads", len(marks))
	}
	for i, admin := range marks {
		if !admin {
			t.Errorf("layout read %d not admin", i)
		}
	}
}

// In a dataset's fractal live scores never expire either: once its window moves
// past the data no pass replaces them.
func TestScorerKeepsDatasetScores(t *testing.T) {
	env, e, _ := scorerEnv(t, false, ModelTypeLongConnection, ModelDefinition{})
	end := time.Now()
	env.db.fractalEnds = map[string]*time.Time{fracA: &end}
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(5)))
	env.ch.On("groupArraySortedMerge", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.1", "203.0.113.5", 1, []uint32{uint32(time.Now().Unix() - 60)}, []float64{1}, 30000),
	}})
	e.scoreCycle()
	if b := env.ch.Batches(); len(b) != 1 || !b[0].Appended[0][20].(time.Time).Equal(netResultsNever) {
		t.Errorf("batches: %+v", b)
	}
}

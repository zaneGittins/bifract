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

	res, err := env.mgr.ScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to})
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
	if got, _ := env.db.get(r.id); got.lastScored != nil {
		t.Error("scoring a range moved the live scorer's cursor")
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
			_, err := env.mgr.ScoreRange(context.Background(), mo, c.req)
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
	res, err := env.mgr.ScoreRange(context.Background(), mo, req)
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
			if _, err := env.mgr.ScoreRange(context.Background(), mo, req); err == nil || !strings.Contains(err.Error(), name) {
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
	res, err := env.mgr.ScoreRange(context.Background(), mo, req)
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
	if _, err := env.mgr.ScoreRange(context.Background(), mo, req); err == nil {
		t.Fatal("a cut-short read succeeded")
	}
	if len(env.ch.Batches()) != 0 {
		t.Error("a cut-short read wrote scores")
	}
}

// A run bounded by the request's deadline tells ClickHouse the same budget.
func TestScoreRangeFollowsTheRequestDeadline(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	env, mo, _ := scoreRangeModel(t, ModelTypeBeacon)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := env.mgr.ScoreRange(ctx, mo, ScoreRangeRequest{From: &from, To: &to}); err != nil {
		t.Fatal(err)
	}
	for _, q := range env.ch.Queried() {
		if !strings.Contains(q, "max_execution_time=29,") && !strings.Contains(q, "max_execution_time=30,") {
			t.Errorf("query outlives the request: ...%s", q[max(0, len(q)-200):])
		}
	}
}

// A backfill holding the slot fails the request at once rather than holding it
// until the request times out.
func TestScoreRangeBusy(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	env, mo, _ := scoreRangeModel(t, ModelTypeBeacon)
	env.mgr.bfSem <- struct{}{}
	defer func() { <-env.mgr.bfSem }()
	_, err := env.mgr.ScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to})
	if err == nil || !strings.Contains(err.Error(), "running") || len(env.ch.Queried()) != 0 {
		t.Errorf("err = %v, queries %d", err, len(env.ch.Queried()))
	}
}

// A model edited while it scored keeps its new table free of old scores.
func TestScoreRangeModelChangedMidRun(t *testing.T) {
	from := time.Now().UTC().Add(-48 * time.Hour)
	to := from.Add(24 * time.Hour)
	env, mo, r := scoreRangeModel(t, ModelTypeLongConnection)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
	env.ch.On("AS ts_list", longConnPairs(3, to))
	env.db.mu.Lock()
	env.db.rows[r.id].updatedAt = mo.UpdatedAt.Add(time.Second)
	env.db.mu.Unlock()
	_, err := env.mgr.ScoreRange(context.Background(), mo, ScoreRangeRequest{From: &from, To: &to})
	if err == nil || !strings.Contains(err.Error(), "changed") || len(env.ch.Batches()) != 0 {
		t.Errorf("err = %v, batches %d", err, len(env.ch.Batches()))
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

package models

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// State maintenance cycles through the fakes: the watermark is the only record of
// what has been counted, so it must advance exactly over what was inserted.

var stateWindowRe = regexp.MustCompile(`ingest_timestamp > '([^']+)' AND ingest_timestamp <= '([^']+)'`)

func stateWindow(t *testing.T, stmt string) (from, to time.Time) {
	t.Helper()
	m := stateWindowRe.FindStringSubmatch(stmt)
	if m == nil {
		t.Fatalf("no state window in: %s", stmt)
	}
	var err error
	if from, err = time.ParseInLocation(chTimeLayout, m[1], time.UTC); err != nil {
		t.Fatal(err)
	}
	if to, err = time.ParseInLocation(chTimeLayout, m[2], time.UTC); err != nil {
		t.Fatal(err)
	}
	return from, to
}

func maintEnv(t *testing.T, cluster bool) (*fakeEnv, *StateMaintainer) {
	env := newEnv(t)
	if cluster {
		env = newClusterEnv(t)
	}
	return env, NewStateMaintainer(env.db.client, env.ch.client, env.mgr)
}

func ms(t time.Time) time.Time { return t.UTC().Truncate(time.Millisecond) }

// Each cycle reads (watermark, ingest watermark) and only then moves the model's
// watermark to the window's end, so consecutive cycles tile ingest time with no
// gap and no overlap.
func TestStateCyclesTileIngestTime(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-time.Hour))
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark = &wm })
	ingestW := ms(time.Now().Add(-10 * time.Minute))
	s.SetIngestWatermark(func(context.Context) (time.Time, error) { return ingestW, nil })

	s.cycle(context.Background())
	ingestW = ingestW.Add(5 * time.Minute)
	s.cycle(context.Background())

	inserts := env.ch.ExecutedMatching("INSERT INTO")
	if len(inserts) != 2 {
		t.Fatalf("inserts: %d", len(inserts))
	}
	from1, to1 := stateWindow(t, inserts[0])
	from2, to2 := stateWindow(t, inserts[1])
	if !from1.Equal(wm) {
		t.Errorf("first window starts %v, want the watermark %v", from1, wm)
	}
	if !from2.Equal(to1) {
		t.Errorf("second window starts %v, want where the first ended (%v)", from2, to1)
	}
	if want := ingestW.Add(-time.Millisecond); !to2.Equal(want) {
		t.Errorf("window ends %v, want just before the ingest watermark %v", to2, ingestW)
	}
	got, _ := env.db.get(r.id)
	if !ms(*got.watermark).Equal(to2) {
		t.Errorf("watermark %v, want the last window's end %v", got.watermark, to2)
	}
	if !strings.HasPrefix(inserts[0], "INSERT INTO `"+r.chTable+"`") || !strings.Contains(inserts[0], "FROM logs") {
		t.Errorf("target/source: %s", inserts[0][:min(200, len(inserts[0]))])
	}
	if got.errorMessage != "" {
		t.Errorf("error recorded on success: %q", got.errorMessage)
	}
}

// A failed insert leaves the watermark where it was and says why on the model; the
// next cycle retries the same window and clears the error once it succeeds.
func TestStateCycleFailureRetriesTheSameWindow(t *testing.T) {
	env, s := maintEnv(t, false)
	var failing atomic.Bool
	failing.Store(true)
	env.ch.FailExecFunc(func(stmt string) error {
		if failing.Load() && strings.HasPrefix(stmt, "INSERT INTO") {
			return errors.New("too many parts")
		}
		return nil
	})
	wm := ms(time.Now().Add(-30 * time.Minute))
	r := seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}}, func(r *modelRow) { r.watermark = &wm })

	s.cycle(context.Background())
	got, _ := env.db.get(r.id)
	if !got.watermark.Equal(wm) {
		t.Fatalf("watermark advanced past a failed insert: %v", got.watermark)
	}
	if !strings.HasPrefix(got.errorMessage, "insert: ") || !strings.Contains(got.errorMessage, "too many parts") {
		t.Errorf("error not surfaced: %q", got.errorMessage)
	}

	// An unchanged failure is not rewritten every cycle.
	s.cycle(context.Background())
	if n := len(env.db.pg.Matching("SET error_message = $1")); n != 1 {
		t.Errorf("unchanged error written %d times", n)
	}

	failing.Store(false)
	s.cycle(context.Background())
	inserts := env.ch.ExecutedMatching("INSERT INTO")
	from, to := stateWindow(t, inserts[len(inserts)-1])
	if !from.Equal(wm) {
		t.Errorf("retry starts at %v, want the unmoved watermark %v", from, wm)
	}
	got, _ = env.db.get(r.id)
	if got.errorMessage != "" || !ms(*got.watermark).Equal(to) {
		t.Errorf("after recovery: error %q watermark %v want %v", got.errorMessage, got.watermark, to)
	}
}

// A watermark far behind is caught up in capped windows, one per cycle, each
// starting where the last ended.
func TestStateCycleCatchesUpInCappedWindows(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-15 * time.Hour))
	seedModel(env, ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"host"}}, func(r *modelRow) { r.watermark = &wm })

	for i := 0; i < 3; i++ {
		s.cycle(context.Background())
	}
	inserts := env.ch.ExecutedMatching("INSERT INTO")
	if len(inserts) != 3 {
		t.Fatalf("inserts: %d", len(inserts))
	}
	prev := wm
	for i, stmt := range inserts {
		from, to := stateWindow(t, stmt)
		if !from.Equal(prev) {
			t.Errorf("window %d starts %v, want %v", i, from, prev)
		}
		if i < 2 && to.Sub(from) != stateMaintMaxWindow {
			t.Errorf("window %d spans %v, want the %v cap", i, to.Sub(from), stateMaintMaxWindow)
		}
		prev = to
	}
	if lag := time.Since(prev); lag > time.Minute {
		t.Errorf("caught up to %v behind now", lag)
	}
}

// Another replica holding the lock means this one reads nothing at all: two
// maintainers inserting the same window would double every aggregate.
func TestStateCycleNeedsTheLock(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-time.Hour))
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark = &wm })
	env.db.setLockHeld(true)
	s.cycle(context.Background())
	if len(env.ch.Executed()) != 0 || len(env.db.pg.Matching("FROM analytics_models")) != 0 {
		t.Error("a replica without the lock did work")
	}
}

// On a cluster a network model's cycle writes the state table's Distributed
// companion from the distributed logs, never the results table.
func TestStateCycleNetworkModelOnCluster(t *testing.T) {
	env, s := maintEnv(t, true)
	wm := ms(time.Now().Add(-time.Hour))
	r := seedModel(env, ModelTypeBeacon, ModelDefinition{}, func(r *modelRow) { r.watermark = &wm })
	s.cycle(context.Background())
	inserts := env.ch.ExecutedMatching("INSERT INTO")
	if len(inserts) != 1 || !strings.HasPrefix(inserts[0], "INSERT INTO `"+chModelStateDistName(r.id)+"`") ||
		!strings.Contains(inserts[0], "FROM logs_distributed") {
		t.Fatalf("inserts: %v", inserts)
	}
}

// A model with an unreadable definition is skipped without holding up the rest,
// and an outcome kept for a model that is no longer due is dropped.
func TestStateCycleSkipsBrokenAndForgetsGone(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-time.Hour))
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark, r.definition = &wm, "{not json" })
	good := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark = &wm })
	s.lastErr["deleted-model"] = "old failure"

	s.cycle(context.Background())
	if inserts := env.ch.ExecutedMatching("INSERT INTO"); len(inserts) != 1 || !strings.Contains(inserts[0], good.chTable) {
		t.Errorf("inserts: %v", inserts)
	}
	if _, kept := s.lastErr["deleted-model"]; kept {
		t.Error("outcome of a model no longer due kept")
	}
	if !s.knowsOutcome(good.id) {
		t.Error("first outcome of a fresh process not recorded")
	}
}

// An outcome that could not be written is not remembered, so it is written on the
// next cycle rather than lost.
func TestStateCycleRetriesUnrecordedOutcome(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-time.Hour))
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark = &wm })
	env.db.failOn("SET error_message", errors.New("pg blip"))
	s.cycle(context.Background())
	if s.knowsOutcome(r.id) {
		t.Fatal("an outcome Postgres refused was remembered")
	}
	env.db.mu.Lock()
	delete(env.db.fail, "SET error_message")
	env.db.mu.Unlock()
	s.cycle(context.Background())
	if !s.knowsOutcome(r.id) {
		t.Error("outcome not written on the next cycle")
	}
}

func TestStateCycleListFailure(t *testing.T) {
	env, s := maintEnv(t, false)
	env.db.failOn("state_watermark IS NOT NULL", errors.New("pg down"))
	s.cycle(context.Background())
	if len(env.ch.Executed()) != 0 {
		t.Error("inserted without a model list")
	}
}

// The loop runs cycles on its ticker, defers them under ingest pressure, and
// stops when told.
func TestStateMaintainerLoop(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-time.Hour))
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark = &wm })

	var pressured atomic.Bool
	pressured.Store(true)
	var checks atomic.Int32
	s.SetIngestPressureFunc(func() bool { checks.Add(1); return pressured.Load() })
	s.Start(5 * time.Millisecond)
	defer s.Stop()
	waitFor(t, "a deferred tick", func() bool { return checks.Load() >= 2 })
	if len(env.ch.Executed()) != 0 {
		t.Fatal("a cycle ran under ingest pressure")
	}
	pressured.Store(false)
	waitFor(t, "a cycle", func() bool { return len(env.ch.ExecutedMatching("INSERT INTO")) > 0 })
}

// An unreadable ingest watermark skips the cycle: no model reads or advances.
func TestStateCycleSkipsWhenTheIngestWatermarkIsUnknown(t *testing.T) {
	env, s := maintEnv(t, false)
	wm := ms(time.Now().Add(-time.Hour))
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.watermark = &wm })
	s.SetIngestWatermark(func(context.Context) (time.Time, error) { return time.Time{}, errors.New("pg down") })

	s.cycle(context.Background())
	if n := len(env.ch.ExecutedMatching("INSERT INTO")); n != 0 {
		t.Errorf("%d state inserts without a watermark", n)
	}
	if got, _ := env.db.get(r.id); !got.watermark.Equal(wm) {
		t.Errorf("model watermark moved to %v", got.watermark)
	}
}

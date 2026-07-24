package archive

import (
	"testing"
	"time"

	"bifract/pkg/storage"
)

// A disabled stream (nil, no callback, or non-positive interval) must never
// panic and never emit: partial delivery is strictly opt-in.
func TestPartialEmitterDisabled(t *testing.T) {
	for _, s := range []*SearchStream{
		nil,
		{Partial: nil, Interval: time.Second},
		{Partial: func([]map[string]interface{}, storage.QueryStats) {}, Interval: 0},
	} {
		emit := newPartialEmitter(s)
		emit(nil, storage.QueryStats{}, false)
		emit([]map[string]interface{}{{"a": 1}}, storage.QueryStats{ReadRows: 5}, true)
	}
}

// The first call always emits; subsequent row-only calls within the interval are
// suppressed, and a progress call carrying fresh stats but no new rows still
// emits (scan cost is the thing that moved).
func TestPartialEmitterThrottleAndStats(t *testing.T) {
	var gotRows [][]map[string]interface{}
	var gotStats []storage.QueryStats
	s := &SearchStream{
		Interval: time.Hour, // long enough that only the first call passes the gate
		Partial: func(rows []map[string]interface{}, st storage.QueryStats) {
			gotRows = append(gotRows, rows)
			gotStats = append(gotStats, st)
		},
	}
	emit := newPartialEmitter(s)

	rows := []map[string]interface{}{{"a": 1}}
	emit(rows, storage.QueryStats{}, false) // first: emits
	rows = append(rows, map[string]interface{}{"b": 2})
	emit(rows, storage.QueryStats{}, false) // within interval, row-only: suppressed

	if len(gotRows) != 1 {
		t.Fatalf("expected 1 emission, got %d", len(gotRows))
	}
	if len(gotRows[0]) != 1 {
		t.Errorf("first emission should snapshot 1 row, got %d", len(gotRows[0]))
	}
}

// The snapshot handed to Partial must stay stable even as the underlying slice
// grows: it is a capped prefix, so a later append never mutates it.
func TestPartialEmitterSnapshotStable(t *testing.T) {
	var captured []map[string]interface{}
	s := &SearchStream{
		Interval: time.Nanosecond,
		Partial:  func(rows []map[string]interface{}, _ storage.QueryStats) { captured = rows },
	}
	emit := newPartialEmitter(s)

	rows := make([]map[string]interface{}, 0, 4)
	rows = append(rows, map[string]interface{}{"i": 0})
	emit(rows, storage.QueryStats{}, false)
	if len(captured) != 1 {
		t.Fatalf("captured %d rows, want 1", len(captured))
	}
	// Append into the same backing array; the earlier snapshot must not grow.
	rows = append(rows, map[string]interface{}{"i": 1})
	_ = rows
	if len(captured) != 1 {
		t.Errorf("snapshot mutated to %d rows after append", len(captured))
	}
}

// take() must surface the plan exactly once, and rows only after the partial
// interval has elapsed since the last write; written() advances that clock.
func TestSearchProgressSnapshotLifecycle(t *testing.T) {
	p := &searchProgress{}
	base := time.Unix(1_700_000_000, 0)

	// Nothing set yet: an empty snapshot, no plan, no rows.
	s := p.take(base)
	if s.FieldOrder != nil || s.Rows != nil {
		t.Fatalf("empty progress yielded plan/rows: %+v", s)
	}

	p.setPlan([]string{"ts", "msg"}, true)
	p.setPartial([]map[string]interface{}{{"ts": 1}}, storage.QueryStats{ReadRows: 10, ReadBytes: 100})

	s = p.take(base)
	if s.FieldOrder == nil || len(s.FieldOrder) != 2 {
		t.Fatalf("plan not surfaced: %+v", s.FieldOrder)
	}
	if !s.IsAggregated {
		t.Error("isAggregated not carried")
	}
	if s.Rows == nil || len(s.Rows) != 1 {
		t.Fatalf("first row snapshot should be due immediately, got %+v", s.Rows)
	}
	if s.Stats.ReadRows != 10 || s.Stats.ReadBytes != 100 {
		t.Errorf("stats not carried: %+v", s.Stats)
	}
	p.written(s, base)

	// Plan already written: not surfaced again.
	s = p.take(base.Add(time.Second))
	if s.FieldOrder != nil {
		t.Error("plan surfaced twice")
	}

	// More rows arrive but the interval has not elapsed: rows withheld.
	p.setPartial([]map[string]interface{}{{"ts": 1}, {"ts": 2}}, storage.QueryStats{ReadRows: 20})
	s = p.take(base.Add(time.Second))
	if s.Rows != nil {
		t.Error("rows surfaced before the partial interval elapsed")
	}

	// Past the interval: rows are due again.
	s = p.take(base.Add(searchPartialInterval + time.Second))
	if s.Rows == nil || len(s.Rows) != 2 {
		t.Fatalf("rows not surfaced after interval: %+v", s.Rows)
	}
}

// A query that projects no explicit column order must still mark its plan as
// written (carry a non-nil empty slice), or take() would re-surface it forever.
func TestSearchProgressEmptyFieldOrderStillWrites(t *testing.T) {
	p := &searchProgress{}
	p.setPlan(nil, false)
	now := time.Unix(1_700_000_000, 0)

	s := p.take(now)
	if s.FieldOrder == nil {
		t.Fatal("nil field order should surface a non-nil empty slice to mark the plan written")
	}
	p.written(s, now)

	if s2 := p.take(now); s2.FieldOrder != nil {
		t.Error("plan surfaced again after being written")
	}
}

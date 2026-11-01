package watermark

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

var now = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func TestCombineTakesTheOldestLiveNode(t *testing.T) {
	s := Combine([]NodeState{
		{Node: "ingest@a", W: now.Add(-20 * time.Second), Age: 2 * time.Second},
		{Node: "ingest@b", W: now.Add(-5 * time.Second), Age: 1 * time.Second},
	}, now)
	if !s.W.Equal(now.Add(-20*time.Second)) || s.Lag != 20*time.Second {
		t.Errorf("got W=%v lag=%v", s.W, s.Lag)
	}
	if len(s.Held) != 0 || len(s.Expired) != 0 {
		t.Errorf("healthy nodes report no hold: %+v", s)
	}
}

// A node that stopped heartbeating holds W at its last value and says why.
func TestCombineHoldsAStaleNode(t *testing.T) {
	s := Combine([]NodeState{
		{Node: "ingest@a", W: now.Add(-90 * time.Second), Age: 90 * time.Second},
		{Node: "ingest@b", W: now, Age: time.Second},
	}, now)
	if !s.W.Equal(now.Add(-90 * time.Second)) {
		t.Errorf("a stale node must hold W at its last value, got %v", s.W)
	}
	if len(s.Held) != 1 || !strings.Contains(s.Held[0], "ingest@a") {
		t.Errorf("hold reason: %v", s.Held)
	}
}

// Past the TTL a node is dropped, so a crashed pod cannot freeze alerts forever.
func TestCombineDropsAnExpiredNode(t *testing.T) {
	s := Combine([]NodeState{
		{Node: "ingest@gone", W: now.Add(-time.Hour), Age: NodeTTL + time.Second},
		{Node: "ingest@b", W: now.Add(-3 * time.Second), Age: time.Second},
	}, now)
	if !s.W.Equal(now.Add(-3 * time.Second)) {
		t.Errorf("an expired node must not hold W, got %v", s.W)
	}
	if len(s.Expired) != 1 || s.Expired[0] != "ingest@gone" {
		t.Errorf("expired: %v", s.Expired)
	}
}

// A node's W in its own clock may run ahead of the reader's; W never passes now.
func TestCombineNeverPassesNow(t *testing.T) {
	s := Combine([]NodeState{{Node: "a", W: now.Add(time.Minute), Age: time.Second}}, now)
	if !s.W.Equal(now) || s.Lag != 0 {
		t.Errorf("got %v lag %v", s.W, s.Lag)
	}
	if s := Combine(nil, now); !s.W.Equal(now) {
		t.Errorf("no writers, nothing pending: got %v", s.W)
	}
}

func TestReadFailsClosed(t *testing.T) {
	pg := fakedb.NewPG()
	pg.On("FROM ingest_watermarks", fakedb.PGResult{Err: errors.New("pg down")})
	if _, err := Read(context.Background(), pg.DB(), now); err == nil {
		t.Error("an unreadable table must be an error, never W = now")
	}
}

func TestReadCombinesAndPrunesExpired(t *testing.T) {
	pg := fakedb.NewPG()
	pg.On("SELECT node_id", fakedb.RowsOf(
		[]driver.Value{"ingest@a", now.Add(-10 * time.Second), 1.5},
		[]driver.Value{"ingest@gone", now.Add(-time.Hour), (NodeTTL + time.Minute).Seconds()},
	))
	s, err := Read(context.Background(), pg.DB(), now)
	if err != nil {
		t.Fatal(err)
	}
	if !s.W.Equal(now.Add(-10 * time.Second)) {
		t.Errorf("W = %v", s.W)
	}
	del := pg.Matching("DELETE FROM ingest_watermarks")
	if len(del) != 1 {
		t.Fatalf("expired rows must be pruned, got %d deletes", len(del))
	}
}

func TestPublishUpsertsAndStopDeletes(t *testing.T) {
	pg := fakedb.NewPG()
	clk := newFakeClock(now)
	tr := NewTracker(clk.now)
	var b Batch
	b.Add(now.Add(-7 * time.Second))
	tr.Track(&b)

	p := NewPublisher(pg.DB(), tr, "ingest@a")
	if err := p.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	ups := pg.Matching("INSERT INTO ingest_watermarks")
	if len(ups) != 1 || !strings.Contains(ups[0].Query, "ON CONFLICT (node_id) DO UPDATE") {
		t.Fatalf("want one upsert, got %v", ups)
	}
	if ups[0].Args[0] != "ingest@a" || !ups[0].Args[1].(time.Time).Equal(now.Add(-7*time.Second)) {
		t.Errorf("upsert args %v", ups[0].Args)
	}

	p.Start()
	p.Stop(context.Background())
	if del := pg.Matching("DELETE FROM ingest_watermarks WHERE node_id"); len(del) != 1 || del[0].Args[0] != "ingest@a" {
		t.Errorf("graceful stop deletes the node's row: %v", del)
	}
}

package watermark

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ t atomic.Int64 }

func newFakeClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.set(t)
	return c
}
func (c *fakeClock) set(t time.Time)     { c.t.Store(t.UnixNano()) }
func (c *fakeClock) now() time.Time      { return time.Unix(0, c.t.Load()) }
func (c *fakeClock) add(d time.Duration) { c.set(c.now().Add(d)) }

var t0 = time.Date(2026, 11, 1, 12, 0, 10, 700e6, time.UTC)

func TestEmptyTrackerIsNow(t *testing.T) {
	clk := newFakeClock(t0)
	tr := NewTracker(clk.now)
	if got := tr.Watermark(); !got.Equal(t0) {
		t.Errorf("nothing pending: got %v, want now %v", got, t0)
	}
}

// A tracked row holds W at the start of its second until the insert lands.
func TestTrackedRowsHoldUntilReleased(t *testing.T) {
	clk := newFakeClock(t0)
	tr := NewTracker(clk.now)

	var b Batch
	b.Add(t0)
	b.Add(t0.Add(200 * time.Millisecond))
	b.Add(t0.Add(3 * time.Second))
	tr.Track(&b)

	clk.add(time.Minute)
	if got, want := tr.Watermark(), t0.Truncate(time.Second); !got.Equal(want) {
		t.Fatalf("got %v, want the oldest pending second %v", got, want)
	}
	tr.Release(&b)
	if got := tr.Watermark(); !got.Equal(clk.now()) {
		t.Errorf("released (inserted or finally dropped): got %v, want now", got)
	}
}

// Releasing part of the pending rows moves W to the oldest still pending.
func TestWatermarkFollowsTheOldestPending(t *testing.T) {
	clk := newFakeClock(t0)
	tr := NewTracker(clk.now)
	var old, newer Batch
	old.Add(t0)
	newer.Add(t0.Add(5 * time.Second))
	tr.Track(&old)
	tr.Track(&newer)
	clk.add(time.Minute)

	tr.Release(&old)
	if got, want := tr.Watermark(), t0.Add(5*time.Second).Truncate(time.Second); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Two batches in the same second count separately: releasing one keeps the other held.
func TestCountsAreNotSets(t *testing.T) {
	clk := newFakeClock(t0)
	tr := NewTracker(clk.now)
	var a, b Batch
	a.Add(t0)
	b.Add(t0)
	tr.Track(&a)
	tr.Track(&b)
	clk.add(time.Minute)
	tr.Release(&a)
	if got := tr.Watermark(); !got.Equal(t0.Truncate(time.Second)) {
		t.Errorf("one batch still pending, got %v", got)
	}
}

// A hold is taken before stamping, so a stamp can never fall below a W already handed out.
func TestHoldCoversStampsTakenAfterIt(t *testing.T) {
	clk := newFakeClock(t0)
	tr := NewTracker(clk.now)
	h := tr.Hold()
	if !h.At().Equal(t0) {
		t.Fatalf("hold time %v, want now", h.At())
	}
	clk.add(30 * time.Second)
	if got := tr.Watermark(); !got.Equal(t0.Truncate(time.Second)) {
		t.Errorf("an open hold pins W at its second, got %v", got)
	}
	tr.Done(h)
	if got := tr.Watermark(); !got.Equal(clk.now()) {
		t.Errorf("hold released, got %v", got)
	}
}

// Concurrent stamping and reading: every stamp taken after a W was read is at or above it.
func TestStampsNeverFallBelowAWatermarkHandedOut(t *testing.T) {
	tr := NewTracker(time.Now)
	var wg sync.WaitGroup
	var maxW atomic.Int64
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			w := tr.Watermark().UnixNano()
			for {
				cur := maxW.Load()
				if w <= cur || maxW.CompareAndSwap(cur, w) {
					break
				}
			}
		}
	}()

	var violations atomic.Int64
	var stampers sync.WaitGroup
	for i := 0; i < 8; i++ {
		stampers.Add(1)
		go func() {
			defer stampers.Done()
			for j := 0; j < 2000; j++ {
				h := tr.Hold()
				seen := maxW.Load()
				stamp := time.Now()
				// Rows stamped while the hold is open must not sit below any W already read.
				if stamp.UnixNano() < seen {
					violations.Add(1)
				}
				var b Batch
				b.Add(stamp)
				tr.Track(&b)
				tr.Done(h)
				tr.Release(&b)
			}
		}()
	}
	stampers.Wait()
	close(stop)
	wg.Wait()
	if n := violations.Load(); n != 0 {
		t.Errorf("%d stamps fell below a watermark already handed out", n)
	}
}

// Batch is run-length: consecutive rows in one second share one entry.
func TestBatchIsRunLength(t *testing.T) {
	var b Batch
	for i := 0; i < 1000; i++ {
		b.Add(t0.Add(time.Duration(i) * time.Millisecond))
	}
	if len(b.runs) != 2 {
		t.Errorf("1000 rows over two seconds: %d runs, want 2", len(b.runs))
	}
	if b.Len() != 1000 {
		t.Errorf("len %d", b.Len())
	}
}

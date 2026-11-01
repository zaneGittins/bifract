// Package watermark publishes how far each writing process has committed logs, so
// readers keyed on ingest_timestamp never read past rows that are stamped but not
// yet visible in ClickHouse.
//
// W, a process's watermark, means every row it stamped before W is visible. It is
// the start of the oldest second with a pending stamp, or now when nothing is
// pending. Stamps must be taken inside a Hold (or by Track under the tracker's
// lock), so no row can be stamped below a W already handed out.
package watermark

import (
	"sync"
	"time"
)

// Tracker counts pending stamps per second. All methods are safe for concurrent use
// and take the lock once per call, so callers update it per batch, not per row.
type Tracker struct {
	mu      sync.Mutex
	pending map[int64]int64 // unix second -> pending rows and holds
	now     func() time.Time
}

// NewTracker returns a tracker reading time from now.
func NewTracker(now func() time.Time) *Tracker {
	return &Tracker{pending: make(map[int64]int64), now: now}
}

// Local is the process-wide tracker. Every insert into the logs table, queued or
// direct, registers here; the process's publisher reports it.
var Local = NewTracker(time.Now)

// Hold pins W at or below the current time until Done. Take it before stamping rows
// that are tracked later (an HTTP request parses before it enqueues), or before an
// insert that stamps rows itself.
type Hold struct {
	at time.Time
}

// At is the time the hold was taken; stamps taken under it are at or after it.
func (h Hold) At() time.Time { return h.at }

// Hold registers a pending stamp at now and returns it.
func (t *Tracker) Hold() Hold {
	t.mu.Lock()
	at := t.now()
	t.pending[at.Unix()]++
	t.mu.Unlock()
	return Hold{at: at}
}

// Done releases a hold.
func (t *Tracker) Done(h Hold) {
	t.mu.Lock()
	t.dec(h.at.Unix(), 1)
	t.mu.Unlock()
}

// Track registers a batch's stamps as pending.
func (t *Tracker) Track(b *Batch) {
	t.mu.Lock()
	for _, r := range b.runs {
		t.pending[r.sec] += r.n
	}
	t.mu.Unlock()
}

// Release drops a batch's stamps once it is inserted or finally dropped.
func (t *Tracker) Release(b *Batch) {
	t.mu.Lock()
	for _, r := range b.runs {
		t.dec(r.sec, r.n)
	}
	t.mu.Unlock()
}

func (t *Tracker) dec(sec, n int64) {
	if c := t.pending[sec] - n; c > 0 {
		t.pending[sec] = c
	} else {
		delete(t.pending, sec)
	}
}

// Watermark returns W. It reads the clock under the lock, so a Hold taken after it
// returns is never earlier than the value it handed out.
func (t *Tracker) Watermark() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if len(t.pending) == 0 {
		return now
	}
	oldest := int64(-1)
	for sec := range t.pending {
		if oldest < 0 || sec < oldest {
			oldest = sec
		}
	}
	if w := time.Unix(oldest, 0); w.Before(now) {
		return w
	}
	return now
}

// Batch is a run-length set of stamps. Rows arrive mostly in stamp order, so a
// batch holds one entry per distinct second rather than one per row.
type Batch struct {
	runs []run
}

type run struct{ sec, n int64 }

// Add records one stamp.
func (b *Batch) Add(ts time.Time) {
	sec := ts.Unix()
	if k := len(b.runs) - 1; k >= 0 && b.runs[k].sec == sec {
		b.runs[k].n++
		return
	}
	b.runs = append(b.runs, run{sec: sec, n: 1})
}

// Len is the number of stamps recorded.
func (b *Batch) Len() int64 {
	var n int64
	for _, r := range b.runs {
		n += r.n
	}
	return n
}

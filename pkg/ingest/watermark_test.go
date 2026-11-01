package ingest

import (
	"sync/atomic"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
	"bifract/pkg/watermark"
)

// frozenClock pins a tracker's notion of now, so W is comparable across calls.
func frozenClock(t time.Time) func() time.Time { return func() time.Time { return t } }

var wmNow = time.Date(2026, 11, 1, 12, 0, 40, 0, time.UTC)

// An enqueued row holds the watermark at its stamp until a worker inserts it.
func TestEnqueuedRowsHoldTheWatermarkUntilInserted(t *testing.T) {
	tr := watermark.NewTracker(frozenClock(wmNow))
	q := newBareQueue(16)
	q.tracker = tr

	stamp := wmNow.Add(-30 * time.Second)
	logs := entries(3, "f1")
	for i := range logs {
		logs[i].IngestTimestamp = stamp
	}
	if !q.Enqueue(logs) {
		t.Fatal("enqueue rejected")
	}
	if got := tr.Watermark(); !got.Equal(stamp) {
		t.Fatalf("buffered rows: W = %v, want their stamp %v", got, stamp)
	}

	client, _ := storagetest.ClickHouse()
	q.db = client
	got := q.queued()
	runWorker(q, &partBucket{key: partKeyOf(&got[0]), entries: got})
	if w := tr.Watermark(); !w.Equal(wmNow) {
		t.Errorf("inserted: W = %v, want now", w)
	}
}

// A row the queue stamps itself is covered from the moment it is stamped.
func TestEnqueueStampsUnderTheHold(t *testing.T) {
	tr := watermark.NewTracker(frozenClock(wmNow))
	q := newBareQueue(16)
	q.tracker = tr
	if !q.Enqueue(entries(1, "f1")) {
		t.Fatal("enqueue rejected")
	}
	if got := q.queued(); !got[0].IngestTimestamp.Equal(wmNow) {
		t.Errorf("stamp = %v, want the hold time", got[0].IngestTimestamp)
	}
	if got := tr.Watermark(); !got.Equal(wmNow.Truncate(time.Second)) {
		t.Errorf("W = %v", got)
	}
}

// A batch the worker finally drops releases its rows: they will never be visible,
// so holding W for them would freeze every reader.
func TestFinallyDroppedRowsReleaseTheWatermark(t *testing.T) {
	tr := watermark.NewTracker(frozenClock(wmNow))
	client, ch := storagetest.ClickHouse()
	failLogsInserts(ch, func(int32) bool { return true }, connReset)
	q := &IngestQueue{db: client, retry: fastRetry(20 * time.Millisecond), tracker: tr}

	logs := entries(2, "f1")
	q.track(logs)
	runWorker(q, &partBucket{key: partKeyOf(&logs[0]), entries: logs})
	if q.Metrics.InsertErrors.Load() != 2 {
		t.Fatalf("precondition: the batch was dropped, errors=%d", q.Metrics.InsertErrors.Load())
	}
	if got := tr.Watermark(); !got.Equal(wmNow) {
		t.Errorf("dropped rows still hold W at %v", got)
	}
}

// A rejected Enqueue leaves nothing pending.
func TestRejectedEnqueueLeavesNothingPending(t *testing.T) {
	tr := watermark.NewTracker(frozenClock(wmNow))
	q := newBareQueue(1)
	q.tracker = tr
	q.ch <- nil // fill the only slot so the send fails after tracking
	q.bufSize = 0
	logs := entries(2, "f1")
	logs[0].IngestTimestamp = wmNow.Add(-time.Minute)
	if q.Enqueue(logs) {
		t.Fatal("want a rejection")
	}
	if got := tr.Watermark(); !got.Equal(wmNow) {
		t.Errorf("rejected rows hold W at %v", got)
	}
}

// Direct inserts stamp under the process hold, so W stays at or below the stamp
// while the insert is in flight.
func TestInsertLogsStampsUnderTheProcessHold(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	var wDuring atomic.Int64
	ch.OnBatch = func(*fakedb.Batch) { wDuring.Store(watermark.Local.Watermark().UnixNano()) }
	logs := entries(1, "f1")
	logs[0].IngestTimestamp = time.Time{}
	if err := client.InsertLogs(t.Context(), logs); err != nil {
		t.Fatal(err)
	}
	stamp := logs[0].IngestTimestamp
	if stamp.IsZero() {
		t.Fatal("row was not stamped")
	}
	if wDuring.Load() > stamp.UnixNano() {
		t.Errorf("W during the insert %v passed the row's stamp %v", time.Unix(0, wDuring.Load()), stamp)
	}
}

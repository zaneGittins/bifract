package ingest

import (
	"testing"
	"time"

	"bifract/pkg/storage"
)

// TestPartKeyOfMatchesClickHouseToDate verifies the Go partition key matches
// the table's PARTITION BY (fractal_id, toDate(timestamp)). The ClickHouse
// server runs UTC, so day boundaries must be computed in UTC: a local-time
// truncation would split a bucket across two partitions near midnight.
func TestPartKeyOfMatchesClickHouseToDate(t *testing.T) {
	mk := func(fid string, ts time.Time) partKey {
		return partKeyOf(&storage.LogEntry{FractalID: fid, Timestamp: ts})
	}

	day := func(s string) int64 {
		d, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return d.Unix()
	}

	tests := []struct {
		name string
		ts   string
		want int64
	}{
		{"midday UTC", "2026-07-18T12:00:00Z", day("2026-07-18T00:00:00Z")},
		{"start of day", "2026-07-18T00:00:00Z", day("2026-07-18T00:00:00Z")},
		{"end of day", "2026-07-18T23:59:59Z", day("2026-07-18T00:00:00Z")},
		// A non-UTC input must bucket by its UTC day, matching toDate().
		{"negative offset crosses UTC day", "2026-07-18T20:00:00-05:00", day("2026-07-19T00:00:00Z")},
		{"positive offset crosses UTC day", "2026-07-19T02:00:00+05:00", day("2026-07-18T00:00:00Z")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, err := time.Parse(time.RFC3339, tc.ts)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := mk("f1", ts).day; got != tc.want {
				t.Errorf("day = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPartKeySeparatesFractalAndDay is the invariant the accumulator depends
// on: entries land in the same bucket only when they share a partition.
func TestPartKeySeparatesFractalAndDay(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	e := func(fid string, ts time.Time) partKey {
		return partKeyOf(&storage.LogEntry{FractalID: fid, Timestamp: ts})
	}

	same := e("f1", base)
	if e("f1", base.Add(6*time.Hour)) != same {
		t.Error("same fractal and UTC day must share a bucket")
	}
	if e("f2", base) == same {
		t.Error("different fractals must not share a bucket")
	}
	if e("f1", base.Add(24*time.Hour)) == same {
		t.Error("different days must not share a bucket")
	}
}

// TestAccumulatorGroupsByPartition drives the accumulator directly and asserts
// the property the whole change exists for: an ungrouped batch spanning N
// partitions produces exactly N buckets, each single-partition, rather than
// one mixed insert that ClickHouse would shatter into N small parts.
func TestAccumulatorGroupsByPartition(t *testing.T) {
	const (
		fractals = 5
		days     = 4
		perPart  = 30
	)

	q := &IngestQueue{
		ch:      make(chan []storage.LogEntry, 8),
		flushCh: make(chan *partBucket, fractals*days),
		// Force flush on the tick rather than on size, so this exercises the
		// age path and the assertion is about grouping, not thresholds.
		batchRows:     1 << 30,
		batchBytes:    1 << 30,
		flushInterval: time.Millisecond,
		bufferBytes:   1 << 30,
		maxKeys:       1 << 20,
	}

	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	var batch []storage.LogEntry
	for f := 0; f < fractals; f++ {
		for d := 0; d < days; d++ {
			for i := 0; i < perPart; i++ {
				batch = append(batch, storage.LogEntry{
					FractalID: string(rune('a' + f)),
					Timestamp: base.AddDate(0, 0, -d),
					RawLog:    "x",
				})
			}
		}
	}
	// Interleave so arrival order never matches partition order, which is the
	// real-world shape: many agents across many fractals reporting at once.
	for i := range batch {
		j := (i * 7) % len(batch)
		batch[i], batch[j] = batch[j], batch[i]
	}

	q.wg.Add(1)
	go q.accumulator()

	q.ch <- batch
	close(q.ch)

	seen := make(map[partKey]int)
	for b := range q.flushCh {
		for _, e := range b.entries {
			if got := partKeyOf(&e); got != b.key {
				t.Fatalf("bucket %v contains entry for %v: buckets must be single-partition", b.key, got)
			}
		}
		if _, dup := seen[b.key]; dup {
			t.Errorf("partition %v emitted more than once", b.key)
		}
		seen[b.key] = len(b.entries)
	}
	q.wg.Wait()

	if len(seen) != fractals*days {
		t.Errorf("got %d buckets, want %d (one per partition)", len(seen), fractals*days)
	}
	for k, n := range seen {
		if n != perPart {
			t.Errorf("bucket %v has %d entries, want %d", k, n, perPart)
		}
	}

	if got := q.accumRows.Load(); got != 0 {
		t.Errorf("accumRows = %d after drain, want 0", got)
	}
	if got := q.accumBytes.Load(); got != 0 {
		t.Errorf("accumBytes = %d after drain, want 0", got)
	}
}

// TestAccumulatorFlushesOnRowBound verifies a busy partition flushes on size
// without waiting for the interval, which is what keeps live search fresh.
func TestAccumulatorFlushesOnRowBound(t *testing.T) {
	const batchRows = 50

	q := &IngestQueue{
		ch:            make(chan []storage.LogEntry, 4),
		flushCh:       make(chan *partBucket, 4),
		batchRows:     batchRows,
		batchBytes:    1 << 30,
		flushInterval: time.Hour, // must not be what triggers the flush
		bufferBytes:   1 << 30,
		maxKeys:       1 << 20,
	}

	q.wg.Add(1)
	go q.accumulator()

	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	batch := make([]storage.LogEntry, batchRows)
	for i := range batch {
		batch[i] = storage.LogEntry{FractalID: "f1", Timestamp: ts, RawLog: "x"}
	}
	q.ch <- batch

	select {
	case b := <-q.flushCh:
		if len(b.entries) != batchRows {
			t.Errorf("flushed %d entries, want %d", len(b.entries), batchRows)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no flush on row bound; size trigger did not fire")
	}

	close(q.ch)
	q.wg.Wait()
}

// TestAccumulatorEnforcesKeyCap verifies the open-key ceiling force-flushes
// rather than letting a wide backfill buffer without limit.
func TestAccumulatorEnforcesKeyCap(t *testing.T) {
	const maxKeys = 4

	q := &IngestQueue{
		ch:            make(chan []storage.LogEntry, 4),
		flushCh:       make(chan *partBucket, 64),
		batchRows:     1 << 30,
		batchBytes:    1 << 30,
		flushInterval: time.Hour,
		bufferBytes:   1 << 30,
		maxKeys:       maxKeys,
	}

	q.wg.Add(1)
	go q.accumulator()

	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	batch := make([]storage.LogEntry, 0, maxKeys*4)
	for d := 0; d < maxKeys*4; d++ {
		batch = append(batch, storage.LogEntry{
			FractalID: "f1",
			Timestamp: base.AddDate(0, 0, -d),
			RawLog:    "x",
		})
	}
	q.ch <- batch
	close(q.ch)

	var flushed int
	for range q.flushCh {
		flushed++
	}
	q.wg.Wait()

	// Every key is eventually emitted; the cap governs when, not whether.
	if flushed != maxKeys*4 {
		t.Errorf("flushed %d buckets, want %d", flushed, maxKeys*4)
	}
}

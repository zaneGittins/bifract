package watermark

import (
	"testing"
	"time"
)

// A queued batch costs one pass over its stamps to track and one to release.
func BenchmarkTrackRelease5000(b *testing.B) {
	tr := NewTracker(time.Now)
	stamps := make([]time.Time, 5000)
	base := time.Now()
	for i := range stamps {
		stamps[i] = base.Add(time.Duration(i) * 200 * time.Microsecond)
	}
	b.ReportAllocs()
	for b.Loop() {
		var batch Batch
		for _, s := range stamps {
			batch.Add(s)
		}
		tr.Track(&batch)
		tr.Release(&batch)
	}
}

// Every ingest request and direct insert takes one hold.
func BenchmarkHoldParallel(b *testing.B) {
	tr := NewTracker(time.Now)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tr.Done(tr.Hold())
		}
	})
}

package query

import (
	"testing"
	"time"
)

func TestHistogramWindowSnapsOutward(t *testing.T) {
	// A 24h range starting mid-bucket must widen to whole 15-minute buckets.
	start := time.Date(2026, 7, 25, 10, 7, 33, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	bucketSec, bucketCount, snapStart, snapEnd := histogramWindow(start, end)
	if bucketSec != 900 {
		t.Fatalf("bucketSec = %d, want 900", bucketSec)
	}
	if !snapStart.After(start.Add(-time.Duration(bucketSec)*time.Second)) || snapStart.After(start) {
		t.Fatalf("snapStart %v not floored into the bucket containing %v", snapStart, start)
	}
	if snapEnd.Before(end) {
		t.Fatalf("snapEnd %v must cover end %v", snapEnd, end)
	}
	if snapStart.Unix()%int64(bucketSec) != 0 || snapEnd.Unix()%int64(bucketSec) != 0 {
		t.Fatalf("snapped bounds not bucket-aligned: %v .. %v", snapStart, snapEnd)
	}
	if got := int(snapEnd.Sub(snapStart).Seconds()) / bucketSec; got != bucketCount {
		t.Fatalf("bucketCount = %d, want %d", bucketCount, got)
	}
}

func TestHistogramWindowStableWithinBucket(t *testing.T) {
	// The whole point of snapping: a relative range re-run seconds later must
	// produce identical bounds, so the ClickHouse query cache can serve it.
	base := time.Date(2026, 7, 25, 10, 7, 33, 0, time.UTC)
	_, _, s1, e1 := histogramWindow(base.Add(-24*time.Hour), base)
	_, _, s2, e2 := histogramWindow(base.Add(-24*time.Hour).Add(90*time.Second), base.Add(90*time.Second))
	if !s1.Equal(s2) || !e1.Equal(e2) {
		t.Fatalf("bounds drifted within one bucket: %v..%v vs %v..%v", s1, e1, s2, e2)
	}
}

func TestHistogramWindowCapsBuckets(t *testing.T) {
	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	_, bucketCount, _, _ := histogramWindow(start, end)
	if bucketCount > maxHistogramBuckets {
		t.Fatalf("bucketCount = %d exceeds cap %d", bucketCount, maxHistogramBuckets)
	}
}

func TestHistogramChunksCoverRangeExactlyOnce(t *testing.T) {
	start := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	for _, bucketCount := range []int{1, 3, 16, 96, 240, 365, 4999} {
		bucketSec := 900
		chunks := histogramChunks(start, bucketSec, bucketCount)
		if len(chunks) == 0 {
			t.Fatalf("bucketCount %d: no chunks", bucketCount)
		}
		// Newest-first: the first chunk must end at the range end.
		if chunks[0].hiIdx != bucketCount {
			t.Fatalf("bucketCount %d: first chunk hiIdx = %d, want %d", bucketCount, chunks[0].hiIdx, bucketCount)
		}
		covered := make([]int, bucketCount)
		for i, c := range chunks {
			if c.loIdx >= c.hiIdx {
				t.Fatalf("bucketCount %d: empty chunk %d", bucketCount, i)
			}
			if i > 0 && c.hiIdx != chunks[i-1].loIdx {
				t.Fatalf("bucketCount %d: chunk %d does not abut its predecessor", bucketCount, i)
			}
			wantStart := start.Add(time.Duration(c.loIdx*bucketSec) * time.Second)
			if !c.start.Equal(wantStart) {
				t.Fatalf("bucketCount %d: chunk %d start %v, want %v", bucketCount, i, c.start, wantStart)
			}
			for b := c.loIdx; b < c.hiIdx; b++ {
				covered[b]++
			}
		}
		if chunks[len(chunks)-1].loIdx != 0 {
			t.Fatalf("bucketCount %d: last chunk loIdx = %d, want 0", bucketCount, chunks[len(chunks)-1].loIdx)
		}
		for b, n := range covered {
			if n != 1 {
				t.Fatalf("bucketCount %d: bucket %d covered %d times", bucketCount, b, n)
			}
		}
	}
}

func TestHistogramChunksGrowAndStayFew(t *testing.T) {
	start := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	chunks := histogramChunks(start, 10800, 240) // 30 days at 3h buckets
	if len(chunks) > 8 {
		t.Fatalf("30d range produced %d chunks, want a handful", len(chunks))
	}
	first := chunks[0].hiIdx - chunks[0].loIdx
	if first > 240/histogramChunkDiv+1 {
		t.Fatalf("first chunk spans %d buckets, want a small newest slice", first)
	}
	for i := 1; i < len(chunks)-1; i++ {
		prev := chunks[i-1].hiIdx - chunks[i-1].loIdx
		cur := chunks[i].hiIdx - chunks[i].loIdx
		if cur < prev {
			t.Fatalf("chunk %d (%d buckets) smaller than chunk %d (%d buckets)", i, cur, i-1, prev)
		}
	}
}

// Chunked folding must produce exactly what one full-range scan would, including
// for a row landing on a shared chunk boundary.
func TestFoldHistogramRowsMatchesSingleScan(t *testing.T) {
	start := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	bucketSec, bucketCount := 900, 96

	rows := make([]map[string]interface{}, 0, bucketCount)
	for i := 0; i < bucketCount; i++ {
		rows = append(rows, map[string]interface{}{
			"bucket": start.Add(time.Duration(i*bucketSec) * time.Second),
			"cnt":    uint64(i + 1),
		})
	}

	single := bucketHistogram(rows, start, bucketSec, bucketCount)

	chunked := make([]int, bucketCount)
	for _, c := range histogramChunks(start, bucketSec, bucketCount) {
		// Each chunk query returns rows for its own span plus the boundary row the
		// newer chunk owns (inclusive upper bound at second precision).
		chunkRows := make([]map[string]interface{}, 0)
		for _, r := range rows {
			ts := r["bucket"].(time.Time)
			if !ts.Before(c.start) && !ts.After(c.end) {
				chunkRows = append(chunkRows, r)
			}
		}
		foldHistogramRows(chunked, chunkRows, start, bucketSec, c.loIdx, c.hiIdx)
	}

	for i := range single {
		if single[i] != chunked[i] {
			t.Fatalf("bucket %d: chunked %d != single %d", i, chunked[i], single[i])
		}
	}
}

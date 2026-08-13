package query

import (
	"testing"
	"time"
)

// Chunks must tile the range exactly: no gap (a dropped row) and no overlap (a
// duplicated row) at any boundary, whatever the range length.
func TestStreamChunksTileRangeExactly(t *testing.T) {
	end := time.Date(2026, 8, 12, 14, 11, 20, 500*1000*1000, time.UTC)
	for _, span := range []time.Duration{
		30 * time.Minute, time.Hour, 24 * time.Hour, 7 * 24 * time.Hour,
		30 * 24 * time.Hour, 90 * 24 * time.Hour, 365 * 24 * time.Hour,
	} {
		start := end.Add(-span)
		chunks := streamChunks(start, end)
		if len(chunks) == 0 {
			t.Fatalf("span %s: no chunks", span)
		}
		if !chunks[0].end.Equal(end) {
			t.Errorf("span %s: newest chunk ends at %s, want %s", span, chunks[0].end, end)
		}
		if chunks[0].endExclusive {
			t.Errorf("span %s: newest chunk must keep the inclusive upper bound", span)
		}
		for i := 1; i < len(chunks); i++ {
			if !chunks[i].end.Equal(chunks[i-1].start) {
				t.Errorf("span %s: chunk %d ends at %s but previous starts at %s (gap or overlap)",
					span, i, chunks[i].end, chunks[i-1].start)
			}
			if !chunks[i].endExclusive {
				t.Errorf("span %s: chunk %d must use an exclusive upper bound or it duplicates the boundary row", span, i)
			}
		}
		if oldest := chunks[len(chunks)-1].start; !oldest.Equal(start) {
			t.Errorf("span %s: oldest chunk starts at %s, want %s", span, oldest, start)
		}
	}
}

// The count must stay small however long the range is: the point of doubling is
// that a 90d search costs a handful of queries, not one per fixed slice.
func TestStreamChunksStayFew(t *testing.T) {
	end := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	for _, span := range []time.Duration{
		24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour,
		90 * 24 * time.Hour, 10 * 365 * 24 * time.Hour,
	} {
		if n := len(streamChunks(end.Add(-span), end)); n > 6 {
			t.Errorf("span %s produced %d chunks, want <= 6", span, n)
		}
	}
}

func TestStreamChunksRejectsEmptyRange(t *testing.T) {
	now := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	if got := streamChunks(now, now); got != nil {
		t.Errorf("zero-length range produced %d chunks, want none", len(got))
	}
	if got := streamChunks(now, now.Add(-time.Hour)); got != nil {
		t.Errorf("inverted range produced %d chunks, want none", len(got))
	}
}

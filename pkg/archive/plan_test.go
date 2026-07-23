package archive

import (
	"testing"
	"time"
)

// assertTiling checks the properties the resume cursor depends on: chunks tile
// [from, to) contiguously with no gaps or overlaps, and each chunk sits inside a
// single UTC day (or the ingest_date pruning premise breaks).
func assertTiling(t *testing.T, from, to time.Time, chunks [][2]time.Time) {
	t.Helper()
	if len(chunks) == 0 {
		t.Fatalf("no chunks produced for [%s, %s)", from, to)
	}
	if !chunks[0][0].Equal(from) {
		t.Errorf("first chunk starts at %s, want %s", chunks[0][0], from)
	}
	if last := chunks[len(chunks)-1][1]; !last.Equal(to) {
		t.Errorf("last chunk ends at %s, want %s", last, to)
	}
	for i, c := range chunks {
		if !c[1].After(c[0]) {
			t.Errorf("chunk %d is empty or inverted: [%s, %s)", i, c[0], c[1])
		}
		if i > 0 && !c[0].Equal(chunks[i-1][1]) {
			t.Fatalf("chunk %d starts at %s but previous ended at %s (gap or overlap)", i, c[0], chunks[i-1][1])
		}
		if c[0].UTC().Format("2006-01-02") != c[1].Add(-time.Nanosecond).UTC().Format("2006-01-02") {
			t.Errorf("chunk %d [%s, %s) spans more than one UTC day", i, c[0], c[1])
		}
	}
}

// TestPlanChunksUnderTarget: a window whose every candidate is under the target
// is never bisected -- one chunk per UTC day, same as dayChunks.
func TestPlanChunksUnderTarget(t *testing.T) {
	from, to := ts("2026-04-12T00:00:00Z"), ts("2026-04-15T00:00:00Z")
	count := func(f, tt time.Time) (int64, error) { return 1_000, nil }

	chunks, err := planChunksWith(from, to, count)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("want 3 day chunks, got %d: %v", len(chunks), chunks)
	}
	assertTiling(t, from, to, chunks)
}

// TestPlanChunksBisectsDenseDay: a day over the target is split until each leaf is
// under it, while the tiling and single-day properties hold.
func TestPlanChunksBisectsDenseDay(t *testing.T) {
	from, to := ts("2026-04-12T00:00:00Z"), ts("2026-04-13T00:00:00Z")
	// Row count proportional to the window's fraction of a full day, so a full day
	// is 8x the target and must bisect three levels down to ~1/8-day leaves.
	fullDay := 8 * int64(restoreChunkRowTarget)
	count := func(f, tt time.Time) (int64, error) {
		frac := float64(tt.Sub(f)) / float64(24*time.Hour)
		return int64(frac * float64(fullDay)), nil
	}

	chunks, err := planChunksWith(from, to, count)
	if err != nil {
		t.Fatal(err)
	}
	assertTiling(t, from, to, chunks)
	if len(chunks) != 8 {
		t.Fatalf("a day at 8x target should bisect into 8 chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		n, _ := count(c[0], c[1])
		if n > restoreChunkRowTarget {
			t.Errorf("chunk %d holds %d rows, over the %d target", i, n, restoreChunkRowTarget)
		}
	}
}

// TestPlanChunksLocalizedSpike: only the sub-window that is actually dense should
// bisect deeply; the rest of the day stays coarse. This is the property that keeps
// a single burst from exploding chunk count across an otherwise sparse day.
func TestPlanChunksLocalizedSpike(t *testing.T) {
	day := ts("2026-04-12T00:00:00Z")
	from, to := day, day.AddDate(0, 0, 1)
	// A spike in the first hour holds 4x the target; the rest of the day is empty.
	spikeEnd := day.Add(time.Hour)
	count := func(f, tt time.Time) (int64, error) {
		overlap := earliest(tt, spikeEnd).Sub(latest(f, day))
		if overlap <= 0 {
			return 0, nil
		}
		frac := float64(overlap) / float64(time.Hour)
		return int64(frac * 4 * float64(restoreChunkRowTarget)), nil
	}

	chunks, err := planChunksWith(from, to, count)
	if err != nil {
		t.Fatal(err)
	}
	assertTiling(t, from, to, chunks)
	// Every leaf must be under target.
	for i, c := range chunks {
		if n, _ := count(c[0], c[1]); n > restoreChunkRowTarget {
			t.Errorf("chunk %d holds %d rows, over target", i, n)
		}
	}
	// Localization: the dense first hour forces fine splitting, but the empty
	// afternoon must NOT fragment -- the whole second half of the day is one chunk.
	// (A uniform-density scheme would have split it too.)
	last := chunks[len(chunks)-1]
	if !last[0].Equal(day.Add(12*time.Hour)) || !last[1].Equal(to) {
		t.Errorf("empty second half should be one chunk [12h, 24h), got last chunk [%s, %s)", last[0], last[1])
	}
}

// TestBisectWindowFloor: a window at the minChunkDuration floor that is still over
// target is emitted as a single chunk rather than recursing forever.
func TestBisectWindowFloor(t *testing.T) {
	from := ts("2026-04-12T00:00:00Z")
	to := from.Add(minChunkDuration) // exactly at the floor
	count := func(f, tt time.Time) (int64, error) { return restoreChunkRowTarget * 100, nil }

	var out [][2]time.Time
	if err := bisectWindow(from, to, count, &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("a floor-width window should emit exactly one chunk, got %d", len(out))
	}
	if !out[0][0].Equal(from) || !out[0][1].Equal(to) {
		t.Errorf("floor chunk = [%s, %s), want [%s, %s)", out[0][0], out[0][1], from, to)
	}
}

func earliest(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

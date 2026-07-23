package archive

import (
	"strings"
	"testing"
	"time"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestDayChunks pins the chunk boundaries a restore executes. Each chunk must map
// to exactly one ingest_date partition (that is what makes the Iceberg read prune
// to a single partition), and the first/last chunks must be clipped to the
// requested bounds so a sub-day window is not rounded outward into data the
// operator did not ask for.
func TestDayChunks(t *testing.T) {
	tests := []struct {
		name     string
		from, to string
		want     [][2]string
	}{
		{
			name: "sub-day window stays one clipped chunk",
			from: "2026-04-12T03:00:00Z", to: "2026-04-12T09:00:00Z",
			want: [][2]string{{"2026-04-12T03:00:00Z", "2026-04-12T09:00:00Z"}},
		},
		{
			name: "exact single day",
			from: "2026-04-12T00:00:00Z", to: "2026-04-13T00:00:00Z",
			want: [][2]string{{"2026-04-12T00:00:00Z", "2026-04-13T00:00:00Z"}},
		},
		{
			name: "partial start and end days are clipped, middle is whole",
			from: "2026-04-12T06:00:00Z", to: "2026-04-14T18:00:00Z",
			want: [][2]string{
				{"2026-04-12T06:00:00Z", "2026-04-13T00:00:00Z"},
				{"2026-04-13T00:00:00Z", "2026-04-14T00:00:00Z"},
				{"2026-04-14T00:00:00Z", "2026-04-14T18:00:00Z"},
			},
		},
		{
			name: "month boundary",
			from: "2026-04-30T12:00:00Z", to: "2026-05-01T12:00:00Z",
			want: [][2]string{
				{"2026-04-30T12:00:00Z", "2026-05-01T00:00:00Z"},
				{"2026-05-01T00:00:00Z", "2026-05-01T12:00:00Z"},
			},
		},
		{
			// Both instants land on UTC day 2026-04-13 even though they straddle
			// midnight locally, so chunking follows UTC (the archive's ingest_date
			// axis) rather than the caller's zone.
			name: "chunks follow UTC days, not the input offset",
			from: "2026-04-12T20:00:00-05:00", to: "2026-04-13T04:00:00-05:00",
			want: [][2]string{
				{"2026-04-13T01:00:00Z", "2026-04-13T09:00:00Z"},
			},
		},
		{
			name: "non-UTC input crossing a UTC midnight splits",
			from: "2026-04-12T20:00:00-05:00", to: "2026-04-13T20:00:00-05:00",
			want: [][2]string{
				{"2026-04-13T01:00:00Z", "2026-04-14T00:00:00Z"},
				{"2026-04-14T00:00:00Z", "2026-04-14T01:00:00Z"},
			},
		},
		{
			name: "empty window yields no chunks",
			from: "2026-04-12T00:00:00Z", to: "2026-04-12T00:00:00Z",
			want: nil,
		},
		{
			name: "inverted window yields no chunks",
			from: "2026-04-14T00:00:00Z", to: "2026-04-12T00:00:00Z",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dayChunks(ts(tt.from), ts(tt.to))
			if len(got) != len(tt.want) {
				t.Fatalf("got %d chunks, want %d: %v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				if !got[i][0].Equal(ts(w[0])) || !got[i][1].Equal(ts(w[1])) {
					t.Errorf("chunk %d: got [%s, %s), want [%s, %s)",
						i, got[i][0].Format(time.RFC3339), got[i][1].Format(time.RFC3339), w[0], w[1])
				}
			}
		})
	}
}

// TestDayChunksContiguous guards the property the resume cursor depends on: chunks
// tile the window with no gaps and no overlaps, so persisting one chunk's end as
// the next attempt's start can neither skip nor re-copy data.
func TestDayChunksContiguous(t *testing.T) {
	from, to := ts("2026-01-15T07:30:00Z"), ts("2026-04-15T19:45:00Z")
	chunks := dayChunks(from, to)
	if len(chunks) < 80 {
		t.Fatalf("expected ~90 chunks over a 3 month window, got %d", len(chunks))
	}
	if !chunks[0][0].Equal(from) {
		t.Errorf("first chunk starts at %s, want %s", chunks[0][0], from)
	}
	if last := chunks[len(chunks)-1][1]; !last.Equal(to) {
		t.Errorf("last chunk ends at %s, want %s", last, to)
	}
	for i := 1; i < len(chunks); i++ {
		if !chunks[i][0].Equal(chunks[i-1][1]) {
			t.Fatalf("chunk %d starts at %s but previous ended at %s (gap or overlap)",
				i, chunks[i][0], chunks[i-1][1])
		}
	}
	// Every chunk must sit inside a single UTC day, or it would span more than one
	// ingest_date partition and the pruning premise breaks.
	for i, c := range chunks {
		if c[0].UTC().Format("2006-01-02") != c[1].Add(-time.Nanosecond).UTC().Format("2006-01-02") {
			t.Errorf("chunk %d [%s, %s) spans more than one UTC day", i, c[0], c[1])
		}
	}
}

// TestRestoreJobStart covers resume-cursor selection, including the guards that
// keep a stale or out-of-range cursor from moving the window.
func TestRestoreJobStart(t *testing.T) {
	from, to := ts("2026-04-12T00:00:00Z"), ts("2026-04-15T00:00:00Z")
	base := restoreJob{From: from, To: to}

	if got := base.start(); !got.Equal(from) {
		t.Errorf("no cursor: got %s, want %s", got, from)
	}

	mid := ts("2026-04-13T00:00:00Z")
	withCursor := base
	withCursor.Cursor = mid
	if got := withCursor.start(); !got.Equal(mid) {
		t.Errorf("valid cursor: got %s, want %s", got, mid)
	}

	// A cursor at or before the window start carries no information.
	before := base
	before.Cursor = ts("2026-04-11T00:00:00Z")
	if got := before.start(); !got.Equal(from) {
		t.Errorf("cursor before window: got %s, want %s", got, from)
	}

	// A cursor at or past the end would produce an empty window; fall back to From
	// so the job re-verifies rather than silently completing having done nothing.
	after := base
	after.Cursor = ts("2026-04-20T00:00:00Z")
	if got := after.start(); !got.Equal(from) {
		t.Errorf("cursor past window: got %s, want %s", got, from)
	}
}

// TestBuildRestoreInsert guards the two clauses a restore silently depends on.
//
// LIMIT 1 BY log_id collapses duplicates that exist inside the archive itself.
// The hot-store anti-join cannot do this, and archive-side duplicates are normal:
// the spool replays at-least-once from its checkpoint after a crash. Without this
// clause a restore inserts every copy into a plain MergeTree and permanently
// inflates row counts, which was observed end-to-end (4000 archived rows holding
// 2000 distinct log_ids restored as 4000 rows) before the clause was added.
func TestBuildRestoreInsert(t *testing.T) {
	sql := buildRestoreInsert("logs", "'f'", "icebergS3('u')", "fractal_id = 'f'")

	for _, want := range []string{
		"LIMIT 1 BY log_id",
		"max_partitions_per_insert_block = 1000",
		"norm_log::JSON",
		"INSERT INTO logs (timestamp, log_id, fields, fractal_id, ingest_timestamp, normalizer)",
		// The dedup set is capped, and MUST throw on overflow rather than break:
		// a truncated set would silently stop excluding log_ids and double-insert.
		"max_rows_in_set = 200000000",
		"set_overflow_mode = 'throw'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("generated insert is missing %q:\n%s", want, sql)
		}
	}

	// 'break' overflow mode would truncate the dedup set and double-insert; guard
	// against a future edit swapping it in.
	if strings.Contains(sql, "set_overflow_mode = 'break'") {
		t.Errorf("dedup set must not use break overflow mode (silently double-inserts):\n%s", sql)
	}

	// raw_log is not restored: the logs table no longer has the column.
	if strings.Contains(sql, "raw_log") {
		t.Errorf("restore insert should not reference raw_log:\n%s", sql)
	}

	// LIMIT 1 BY must sit after WHERE and before SETTINGS, or ClickHouse rejects it.
	wherePos := strings.Index(sql, "WHERE")
	limitPos := strings.Index(sql, "LIMIT 1 BY")
	setPos := strings.Index(sql, "SETTINGS")
	if !(wherePos < limitPos && limitPos < setPos) {
		t.Errorf("clause order wrong (want WHERE < LIMIT 1 BY < SETTINGS):\n%s", sql)
	}
}

// TestBuildRestoreInsertTargetFractal checks that the projected fractal_id is the
// target literal, not the archive's own fractal_id column. A restore into a
// different fractal that still copied the source fractal_id through would land the
// rows under the source, defeating the separate-lifecycle guarantee.
func TestBuildRestoreInsertTargetFractal(t *testing.T) {
	sql := buildRestoreInsert("logs", "'investigation'", "icebergS3('u')",
		"fractal_id = 'source'")

	// The SELECT list must project the literal target in the fractal_id position,
	// i.e. "..., log_id, norm_log::JSON, 'investigation', ingest_timestamp, ...".
	if !strings.Contains(sql, "norm_log::JSON, 'investigation', ingest_timestamp") {
		t.Errorf("target fractal literal not projected into the SELECT:\n%s", sql)
	}
	// The read filter still scopes to the source fractal.
	if !strings.Contains(sql, "fractal_id = 'source'") {
		t.Errorf("read filter should still target the source fractal:\n%s", sql)
	}
}

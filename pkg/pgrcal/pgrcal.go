// Package pgrcal calibrates pgr() severity cutoffs from the scores a deployment actually
// produces, so the admin sets a share of activity to flag rather than a raw 0-1 cutoff.
//
// The distribution is accumulated from real pgr() results (post-diffusion, which is what an
// analyst sees) instead of being measured by a background scan: every pgr() has already computed
// these numbers, so recording them costs nothing and needs no extra ClickHouse work.
package pgrcal

import (
	"context"
	"database/sql"
	"math"
	"sync"
	"time"
)

const (
	// buckets discretises the 0-1 score to 0.1% resolution. 1% was too coarse: a saturated
	// distribution puts most of its mass in one bucket, and no share below that bucket's own
	// weight can then be cut. Scores are rounded to 4dp upstream, so this keeps most of the
	// real structure.
	buckets = 1001
	// decayAt halves every bucket once the histogram reaches this many observations, so the
	// distribution tracks recent behaviour instead of averaging over a deployment's whole life.
	decayAt = 20_000_000
	// minSamples is the smallest histogram that can produce a trustworthy percentile. Sized so the
	// tail being cut still holds ~100 observations at the default 2% share, which is enough for a
	// stable boundary and is reached in a handful of graphs rather than a hundred.
	minSamples = 5_000
	// fallbackHigh / fallbackMed are the pre-calibration cutoffs.
	fallbackHigh = 0.9
	fallbackMed  = 0.7
	// medMultiple derives the medium band from the high share; medium is display gradation only.
	medMultiple = 5
	// flushInterval bounds how often accumulated counts reach Postgres.
	flushInterval = 30 * time.Second
)

// Cutoff is a resolved severity boundary pair plus the evidence behind it.
type Cutoff struct {
	High       float64 `json:"high"`
	Med        float64 `json:"med"`
	Calibrated bool    `json:"calibrated"` // false = not enough samples yet, defaults in use
	Samples    int64   `json:"samples"`
}

// Recorder accumulates scores in memory and flushes them to Postgres periodically. A pgr() query
// must never wait on calibration bookkeeping, so Observe only takes a mutex over a small array.
type Recorder struct {
	db *sql.DB

	mu      sync.Mutex
	pending [buckets]int64
	dirty   bool
}

func NewRecorder(db *sql.DB) *Recorder { return &Recorder{db: db} }

// Observe records one graph's scores. Safe to call from any goroutine; never blocks on I/O.
func (r *Recorder) Observe(scores []float64) {
	if r == nil || len(scores) == 0 {
		return
	}
	r.mu.Lock()
	for _, s := range scores {
		if math.IsNaN(s) {
			continue
		}
		b := int(math.Round(s * 1000))
		if b < 0 {
			b = 0
		} else if b >= buckets {
			b = buckets - 1
		}
		r.pending[b]++
	}
	r.dirty = true
	r.mu.Unlock()
}

// Start flushes pending counts until ctx is cancelled.
func (r *Recorder) Start(ctx context.Context) {
	if r == nil || r.db == nil {
		return
	}
	go func() {
		t := time.NewTicker(flushInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				r.flush(context.Background())
				return
			case <-t.C:
				r.flush(ctx)
			}
		}
	}()
}

func (r *Recorder) flush(ctx context.Context) {
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return
	}
	snapshot := r.pending
	r.pending = [buckets]int64{}
	r.dirty = false
	r.mu.Unlock()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	for b, n := range snapshot {
		if n == 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO pgr_score_histogram (bucket, edge_count, updated_at) VALUES ($1, $2, now())
			 ON CONFLICT (bucket) DO UPDATE SET edge_count = pgr_score_histogram.edge_count + EXCLUDED.edge_count, updated_at = now()`,
			b, n); err != nil {
			return
		}
	}
	if err := tx.Commit(); err != nil {
		return
	}
	r.decay(ctx)
}

// decay halves the histogram once it grows past decayAt so old behaviour stops dominating.
func (r *Recorder) decay(ctx context.Context) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT coalesce(sum(edge_count), 0) FROM pgr_score_histogram`).Scan(&total); err != nil || total < decayAt {
		return
	}
	r.db.ExecContext(ctx, `UPDATE pgr_score_histogram SET edge_count = edge_count / 2`)
}

// Cutoffs derives the severity boundaries for a sensitivity share (percent of edges to flag as
// high). Walks the histogram from the top until the requested share is covered.
func Cutoffs(ctx context.Context, db *sql.DB, sensitivityPercent float64) Cutoff {
	out := Cutoff{High: fallbackHigh, Med: fallbackMed}
	if db == nil {
		return out
	}
	rows, err := db.QueryContext(ctx, `SELECT bucket, edge_count FROM pgr_score_histogram ORDER BY bucket DESC`)
	if err != nil {
		return out
	}
	defer rows.Close()

	type bc struct {
		bucket int
		count  int64
	}
	var ordered []bc
	var total int64
	for rows.Next() {
		var b bc
		if err := rows.Scan(&b.bucket, &b.count); err != nil {
			return out
		}
		ordered = append(ordered, b)
		total += b.count
	}
	out.Samples = total
	if total < minSamples {
		return out
	}

	pick := func(share float64) float64 {
		want := int64(float64(total) * share / 100)
		if want < 1 {
			want = 1
		}
		var seen int64
		for _, b := range ordered {
			seen += b.count
			if seen >= want {
				return float64(b.bucket) / 1000
			}
		}
		return 0
	}
	high := pick(sensitivityPercent)
	med := pick(sensitivityPercent * medMultiple)
	// Medium must sit strictly below high, or the bands collapse and everything reads high.
	if med >= high {
		med = math.Max(0, high-0.001)
	}
	out.High, out.Med, out.Calibrated = high, med, true
	return out
}

package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

const (
	// searchHeartbeat is how often a running job refreshes updated_at (and, on the
	// same tick, publishes live scan cost and checks whether it has been canceled
	// out from under it). Matched to the Recall tab's poll interval so progress
	// the UI shows is at most one tick behind.
	searchHeartbeat = 1 * time.Second
	// searchStaleAfter is how long a running job may go without a heartbeat before
	// the reaper declares its worker dead and fails the job. Comfortably above
	// searchHeartbeat so a live worker is never reaped.
	searchStaleAfter = 60 * time.Second
	// searchPartialInterval throttles how often partial rows are re-serialized and
	// written back to Postgres. Scan cost is two integers and rides every
	// heartbeat, but the result payload is up to max_rows of full log rows, so it
	// moves on a slower cadence to keep a long search from churning the job row.
	searchPartialInterval = 2 * time.Second
)

// SearchWorker drains the archive_search_jobs queue: it claims one pending job
// at a time (FOR UPDATE SKIP LOCKED, safe across every worker process, and
// bounded by the global cap in beginClaim), runs the BQL search against the
// fractal's Iceberg archive via Catalog.Search, and writes the bounded result
// set + terminal status back to the row so the Recall tab can render (and
// reattach to) it.
//
// Like the restore worker it runs independently of the drain enable gate (a
// Recall may be wanted while ongoing archiving is paused) and builds the
// object-store catalog + ClickHouse client lazily on the first claimed job, so a
// disabled, job-free archive never touches object storage.
type SearchWorker struct {
	cfg  Config
	db   *sql.DB
	poll time.Duration

	// Lazily initialized on the first claimed job.
	cat *Catalog
	ch  *storage.ClickHouseClient
}

// NewSearchWorker builds a worker over the shared config and Postgres handle.
func NewSearchWorker(cfg Config, db *sql.DB) *SearchWorker {
	return &SearchWorker{cfg: cfg, db: db, poll: 2 * time.Second}
}

type searchJob struct {
	ID        int64
	FractalID string
	Query     string
	From, To  time.Time
	MaxRows   int
}

// Run polls for and executes search jobs until ctx is cancelled.
func (w *SearchWorker) Run(ctx context.Context) {
	if w.db == nil {
		return
	}
	var lastReap time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		// Fail jobs whose worker died mid-search (stale heartbeat), so they stop
		// counting against the user's in-flight limit and the UI resolves them.
		// Runs on a coarse cadence; any archiver may reap since the guard is on
		// updated_at, not ownership.
		if now := time.Now(); now.Sub(lastReap) >= 30*time.Second {
			lastReap = now
			w.reapStale(ctx)
		}
		job, ok, err := claimSearchJob(ctx, w.db, recallConcurrency(ctx, w.db))
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[Recall] claim error: %v", err)
			}
			if !sleep(ctx, w.poll) {
				return
			}
			continue
		}
		if !ok {
			if !sleep(ctx, w.poll) {
				return
			}
			continue
		}
		w.execute(ctx, job)
	}
}

// claimSearchJob atomically transitions the oldest pending job to running, once
// the global recall concurrency cap has a free slot.
func claimSearchJob(ctx context.Context, db *sql.DB, limit int) (searchJob, bool, error) {
	tx, ok, err := beginClaim(ctx, db, searchClaimLock, "archive_search_jobs", limit)
	if err != nil || !ok {
		return searchJob{}, false, err
	}
	defer tx.Rollback()

	var j searchJob
	err = tx.QueryRowContext(ctx, `
		UPDATE archive_search_jobs SET status = 'running', started_at = NOW(), updated_at = NOW()
		WHERE id = (
			SELECT id FROM archive_search_jobs
			WHERE status = 'pending'
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, fractal_id, query, from_ts, to_ts, max_rows`).
		Scan(&j.ID, &j.FractalID, &j.Query, &j.From, &j.To, &j.MaxRows)
	if err == sql.ErrNoRows {
		return searchJob{}, false, nil
	}
	if err != nil {
		return searchJob{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return searchJob{}, false, err
	}
	return j, true, nil
}

// execute runs one claimed job and records the terminal status + results. The
// run is bounded by RecallTimeout and carries a fixed ClickHouse query_id so a
// cancel (external status flip) or timeout can interrupt it with KILL QUERY.
func (w *SearchWorker) execute(ctx context.Context, j searchJob) {
	log.Printf("[Recall] job %d: fractal %s [%s, %s) rows<=%d", j.ID, j.FractalID, chTime(j.From), chTime(j.To), j.MaxRows)

	if err := w.ensureDeps(ctx); err != nil {
		// Failed before any query ran, so there is no scan cost to report.
		w.finishFailed(j.ID, err.Error(), nil, storage.QueryStats{})
		return
	}

	// The timeout is read live from the settings table at job start, so an admin
	// change on the settings page applies to the next search with no restart. (The
	// byte-scan ceiling is enforced as a pre-flight admission gate at submit time,
	// not here: ClickHouse does not enforce max_bytes_to_read on iceberg table
	// functions, so a mid-scan guard would be a silent no-op.)
	timeout := recallTimeout(ctx, w.db, w.cfg.RecallTimeout)

	jctx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		jctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		jctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	queryID := fmt.Sprintf("recall_%d", j.ID)

	// Live state published by the ClickHouse read loop and drained by the watcher.
	// An archive scan can run for minutes, so rows and scan cost are streamed onto
	// the job row as they arrive rather than only at the end.
	prog := &searchProgress{}

	// Heartbeat + cancel watcher: refreshes updated_at while running (so the reaper
	// leaves us alone), publishes whatever the search has produced so far, and on
	// the same tick notices if the row has left 'running' (an operator canceled
	// it), in which case it kills the query and cancels jctx.
	done := make(chan struct{})
	go w.watch(j.ID, queryID, cancel, done, prog)

	res, err := w.cat.Search(jctx, w.ch, w.cfg.Obj, SearchRequest{
		FractalID: j.FractalID,
		Query:     j.Query,
		From:      j.From,
		To:        j.To,
		MaxRows:   j.MaxRows,
		QueryID:   queryID,
		Stream: &SearchStream{
			Planned:  prog.setPlan,
			Partial:  prog.setPartial,
			Interval: searchPartialInterval,
		},
	})
	close(done)

	if err != nil {
		// Search reports the rows and scan cost it managed to collect even when it
		// fails, so a timed-out search still shows what it found and how far it got.
		var stats storage.QueryStats
		if res != nil {
			stats = res.Stats
		}
		if errors.Is(jctx.Err(), context.DeadlineExceeded) {
			w.killQuery(queryID)
			w.finishFailed(j.ID, fmt.Sprintf("search timed out after %s", timeout), res, stats)
			return
		}
		// Watcher-driven cancel: the row is already 'canceled' and the guarded
		// writes below would no-op. Persist the partial rows anyway (the write is
		// guarded on 'canceled', so it cannot resurrect a job) so cancelling a
		// long scan keeps whatever it had already returned.
		if errors.Is(err, context.Canceled) || jctx.Err() != nil {
			w.savePartialOnCancel(j.ID, res)
			log.Printf("[Recall] job %d canceled", j.ID)
			return
		}
		w.finishFailed(j.ID, err.Error(), res, stats)
		return
	}

	resultsJSON, err := json.Marshal(res.Rows)
	if err != nil {
		w.finishFailed(j.ID, fmt.Sprintf("encode results: %v", err), nil, res.Stats)
		return
	}
	fieldOrderJSON, _ := json.Marshal(res.FieldOrder)

	// Fresh context so the outcome persists even if the run context is cancelled
	// (shutdown) right after the query completes. Guard on status = 'running' so a
	// concurrent cancel or reap is never clobbered by a late success.
	cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ccancel()
	r, err := w.db.ExecContext(cctx, `
		UPDATE archive_search_jobs
		SET status = 'succeeded', row_count = $1, is_aggregated = $2, limit_hit = $3,
		    field_order = $4, results = $5, read_rows = $6, read_bytes = $7,
		    error = NULL, finished_at = NOW(), updated_at = NOW()
		WHERE id = $8 AND status = 'running'`,
		len(res.Rows), res.IsAggregated, res.LimitHit, string(fieldOrderJSON), string(resultsJSON),
		int64(res.Stats.ReadRows), int64(res.Stats.ReadBytes), j.ID)
	if err != nil {
		log.Printf("[Recall] job %d: persist results failed: %v", j.ID, err)
		return
	}
	if n, _ := r.RowsAffected(); n == 0 {
		log.Printf("[Recall] job %d: no longer running (canceled); results discarded", j.ID)
		return
	}
	log.Printf("[Recall] job %d complete: %d rows (aggregated=%v limitHit=%v), scanned %d rows / %d bytes",
		j.ID, len(res.Rows), res.IsAggregated, res.LimitHit, res.Stats.ReadRows, res.Stats.ReadBytes)
}

// searchProgress is a running search's live state: the result shape as soon as
// the query is planned, then rows and scan cost as ClickHouse returns them.
//
// It is written by the ClickHouse read loop and read by the watcher goroutine.
// The read loop must never block on Postgres, so publishing here is the handoff:
// the watcher persists whatever it finds on its own cadence.
type searchProgress struct {
	mu           sync.Mutex
	fieldOrder   []string
	isAggregated bool
	planSet      bool
	planWritten  bool

	rows          []map[string]interface{}
	stats         storage.QueryStats
	rowsDirty     bool
	rowsWrittenAt time.Time
}

// searchSnapshot is what one heartbeat tick should persist. FieldOrder and Rows
// are non-nil only when that part is actually due to be written, so the common
// tick writes two integers.
type searchSnapshot struct {
	Stats        storage.QueryStats
	IsAggregated bool
	FieldOrder   []string
	Rows         []map[string]interface{}
}

func (p *searchProgress) setPlan(fieldOrder []string, isAggregated bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fieldOrder, p.isAggregated, p.planSet = fieldOrder, isAggregated, true
}

func (p *searchProgress) setPartial(rows []map[string]interface{}, stats storage.QueryStats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(rows) != len(p.rows) {
		p.rowsDirty = true
	}
	p.rows, p.stats = rows, stats
}

// take returns the work due at now. Nothing is marked as written until the
// caller confirms with written(), so a failed Postgres write is simply retried
// on the next tick.
func (p *searchProgress) take(now time.Time) searchSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := searchSnapshot{Stats: p.stats, IsAggregated: p.isAggregated}
	if p.planSet && !p.planWritten {
		// A query that projects no explicit column order still has to mark the
		// plan written, so carry a non-nil empty slice rather than nil.
		s.FieldOrder = p.fieldOrder
		if s.FieldOrder == nil {
			s.FieldOrder = []string{}
		}
	}
	if p.rowsDirty && now.Sub(p.rowsWrittenAt) >= searchPartialInterval {
		s.Rows = p.rows
	}
	return s
}

func (p *searchProgress) written(s searchSnapshot, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.FieldOrder != nil {
		p.planWritten = true
	}
	if s.Rows != nil {
		p.rowsDirty = len(p.rows) != len(s.Rows)
		p.rowsWrittenAt = now
	}
}

// watch heartbeats a running job, publishes its live progress, and detects an
// external cancel (the row leaving 'running'). On cancel it kills the ClickHouse
// query and cancels the job context so Search returns promptly. It exits when
// done is closed.
//
// row_count is written only alongside the rows it counts, never on a bare
// heartbeat, so a reader can never see a count that its results payload does not
// support.
func (w *SearchWorker) watch(id int64, queryID string, cancel context.CancelFunc, done <-chan struct{}, prog *searchProgress) {
	t := time.NewTicker(searchHeartbeat)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-t.C:
			snap := prog.take(now)
			var fieldOrderJSON, resultsJSON interface{}
			var rowCount interface{}
			if snap.FieldOrder != nil {
				if b, err := json.Marshal(snap.FieldOrder); err == nil {
					fieldOrderJSON = string(b)
				}
			}
			if snap.Rows != nil {
				if b, err := json.Marshal(snap.Rows); err == nil {
					resultsJSON, rowCount = string(b), len(snap.Rows)
				}
			}
			hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, err := w.db.ExecContext(hctx, `
				UPDATE archive_search_jobs
				SET updated_at = NOW(), read_rows = $2, read_bytes = $3, is_aggregated = $4,
				    field_order = COALESCE($5::jsonb, field_order),
				    results = COALESCE($6::jsonb, results),
				    row_count = COALESCE($7::bigint, row_count)
				WHERE id = $1 AND status = 'running'`,
				id, int64(snap.Stats.ReadRows), int64(snap.Stats.ReadBytes), snap.IsAggregated,
				fieldOrderJSON, resultsJSON, rowCount)
			hcancel()
			if err != nil {
				continue // transient; retry next tick
			}
			if n, _ := r.RowsAffected(); n == 0 {
				w.killQuery(queryID)
				cancel()
				return
			}
			prog.written(snap, now)
		}
	}
}

// savePartialOnCancel keeps the rows a canceled search had already returned. The
// write is guarded on 'canceled' so it can only ever decorate a row the cancel
// already claimed, never revive a job or race a reap.
func (w *SearchWorker) savePartialOnCancel(id int64, res *SearchResult) {
	if res == nil || len(res.Rows) == 0 {
		return
	}
	resultsJSON, err := json.Marshal(res.Rows)
	if err != nil {
		return
	}
	fieldOrderJSON, _ := json.Marshal(res.FieldOrder)
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_search_jobs
		SET row_count = $1, is_aggregated = $2, field_order = $3, results = $4,
		    read_rows = $5, read_bytes = $6, updated_at = NOW()
		WHERE id = $7 AND status = 'canceled'`,
		len(res.Rows), res.IsAggregated, string(fieldOrderJSON), string(resultsJSON),
		int64(res.Stats.ReadRows), int64(res.Stats.ReadBytes), id)
}

// killQuery best-effort interrupts a running Recall query by its query_id.
func (w *SearchWorker) killQuery(queryID string) {
	if w.ch == nil {
		return
	}
	kctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.ch.Exec(kctx, fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", queryID)); err != nil {
		log.Printf("[Recall] kill %s: %v", queryID, err)
	}
}

// reapStale fails running jobs whose worker stopped heartbeating (restart or
// crash), releasing the user's in-flight slot and resolving the UI. The
// updated_at guard means a live worker's own job is never reaped; the dead
// worker's ClickHouse query is cancelled by ClickHouse when its connection drops.
func (w *SearchWorker) reapStale(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := w.db.ExecContext(rctx, fmt.Sprintf(`
		UPDATE archive_search_jobs
		SET status = 'failed', error = 'search worker stopped responding (restarted or crashed)',
		    finished_at = NOW(), updated_at = NOW()
		WHERE status = 'running' AND updated_at < NOW() - INTERVAL '%d seconds'`, int(searchStaleAfter.Seconds())))
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[Recall] reap error: %v", err)
		}
		return
	}
	if n, _ := r.RowsAffected(); n > 0 {
		log.Printf("[Recall] reaped %d stale running job(s)", n)
	}
}

// ensureDeps lazily builds the catalog + ClickHouse client. A disk backend is
// rejected up front: it is pod-local and unreadable by ClickHouse.
func (w *SearchWorker) ensureDeps(ctx context.Context) error {
	if w.cat != nil && w.ch != nil {
		return nil
	}
	if w.cfg.Obj.Backend == objstore.BackendDisk {
		return fmt.Errorf("archive search requires an object-storage backend (s3, minio, or azure); the disk backend is pod-local and cannot be read by ClickHouse")
	}
	ApplyBackendEnv(w.cfg.Obj)
	if w.cat == nil {
		cat, err := NewCatalog(ctx, Namespace, w.cfg.PGDSN, w.cfg.Obj)
		if err != nil {
			return fmt.Errorf("open catalog: %w", err)
		}
		w.cat = cat
	}
	if w.ch == nil {
		ch, err := NewCHClient(w.cfg)
		if err != nil {
			return fmt.Errorf("connect clickhouse: %w", err)
		}
		w.ch = ch
	}
	return nil
}

// finishFailed records a terminal failure with a bounded error message, keeping
// whatever scan cost the query incurred and whatever rows it had already
// returned before it failed. res may be nil (the failure happened before the
// query ran), in which case the results payload is left as-is.
func (w *SearchWorker) finishFailed(id int64, msg string, res *SearchResult, stats storage.QueryStats) {
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	// Rows and their count move together so a partial payload is never described
	// by a count it does not support.
	var resultsJSON, fieldOrderJSON, rowCount interface{}
	if res != nil && len(res.Rows) > 0 {
		if b, err := json.Marshal(res.Rows); err == nil {
			resultsJSON, rowCount = string(b), len(res.Rows)
			fo, _ := json.Marshal(res.FieldOrder)
			fieldOrderJSON = string(fo)
		}
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_search_jobs
		SET status = 'failed', error = $1, read_rows = $2, read_bytes = $3,
		    field_order = COALESCE($4::jsonb, field_order),
		    results = COALESCE($5::jsonb, results),
		    row_count = COALESCE($6::bigint, row_count),
		    finished_at = NOW(), updated_at = NOW()
		WHERE id = $7 AND status = 'running'`,
		msg, int64(stats.ReadRows), int64(stats.ReadBytes),
		fieldOrderJSON, resultsJSON, rowCount, id)
	log.Printf("[Recall] job %d failed after scanning %d rows / %d bytes: %s", id, stats.ReadRows, stats.ReadBytes, msg)
}

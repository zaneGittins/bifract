package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// SearchWorker drains the archive_search_jobs queue: it claims one pending job
// at a time (FOR UPDATE SKIP LOCKED, safe across the N archiver sidecars), runs
// the BQL search against the fractal's Iceberg archive via Catalog.Search, and
// writes the bounded result set + terminal status back to the row so the Recall
// tab can render (and reattach to) it.
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
	for {
		if ctx.Err() != nil {
			return
		}
		job, ok, err := claimSearchJob(ctx, w.db)
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

// claimSearchJob atomically transitions the oldest pending job to running.
func claimSearchJob(ctx context.Context, db *sql.DB) (searchJob, bool, error) {
	var j searchJob
	err := db.QueryRowContext(ctx, `
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
	return j, true, nil
}

// execute runs one claimed job and records the terminal status + results.
func (w *SearchWorker) execute(ctx context.Context, j searchJob) {
	log.Printf("[Recall] job %d: fractal %s [%s, %s) rows<=%d", j.ID, j.FractalID, chTime(j.From), chTime(j.To), j.MaxRows)

	if err := w.ensureDeps(ctx); err != nil {
		w.finishFailed(j.ID, err.Error())
		return
	}

	res, err := w.cat.Search(ctx, w.ch, w.cfg.Obj, j.FractalID, j.Query, j.From, j.To, j.MaxRows)
	if err != nil {
		w.finishFailed(j.ID, err.Error())
		return
	}

	resultsJSON, err := json.Marshal(res.Rows)
	if err != nil {
		w.finishFailed(j.ID, fmt.Sprintf("encode results: %v", err))
		return
	}
	fieldOrderJSON, _ := json.Marshal(res.FieldOrder)

	// Fresh context so the outcome persists even if the run context is cancelled
	// (shutdown) right after the query completes.
	cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = w.db.ExecContext(cctx, `
		UPDATE archive_search_jobs
		SET status = 'succeeded', row_count = $1, is_aggregated = $2, limit_hit = $3,
		    field_order = $4, results = $5, error = NULL, finished_at = NOW(), updated_at = NOW()
		WHERE id = $6`,
		len(res.Rows), res.IsAggregated, res.LimitHit, string(fieldOrderJSON), string(resultsJSON), j.ID)
	if err != nil {
		log.Printf("[Recall] job %d: persist results failed: %v", j.ID, err)
		return
	}
	log.Printf("[Recall] job %d complete: %d rows (aggregated=%v limitHit=%v)", j.ID, len(res.Rows), res.IsAggregated, res.LimitHit)
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

// finishFailed records a terminal failure with a bounded error message.
func (w *SearchWorker) finishFailed(id int64, msg string) {
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_search_jobs
		SET status = 'failed', error = $1, finished_at = NOW(), updated_at = NOW()
		WHERE id = $2`, msg, id)
	log.Printf("[Recall] job %d failed: %s", id, msg)
}

package models

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// BackfillHealth is the signal the backfill engine yields to. It is satisfied by
// the ingest queue (CPU + disk backpressure), so a backfill pauses exactly when
// ingestion would also reject load. Keeping this an interface avoids an import
// cycle between pkg/models and pkg/ingest.
type BackfillHealth interface {
	Healthy() bool
}

// backfill pause backoff bounds (between pressure re-checks).
const (
	bfBackoffMin = 5 * time.Second
	bfBackoffMax = 60 * time.Second
)

// backfillConfig holds the throttle knobs, all env-overridable with safe defaults.
type backfillConfig struct {
	concurrency      int   // global single-flight cap (>=1)
	maxThreads       int   // per-chunk max_threads
	maxGroupByBytes  int64 // max_bytes_before_external_group_by (spill, avoid OOM)
	chunkTimeoutSec  int   // per-chunk max_execution_time / ctx timeout
	osThreadPriority int   // per-chunk os_thread_priority (nice): 0..19, higher = lower OS priority
	chunkSleepMs     int   // cooldown slept between chunks so interactive load can drain
}

func loadBackfillConfig() backfillConfig {
	cfg := backfillConfig{
		concurrency:      1,
		maxThreads:       2,
		maxGroupByBytes:  2_000_000_000,
		chunkTimeoutSec:  300,
		osThreadPriority: 19,  // lowest OS scheduling priority: interactive queries always win CPU
		chunkSleepMs:     500, // brief yield between chunks for merges + health re-check
	}
	if v, ok := envInt("BIFRACT_MODEL_BACKFILL_CONCURRENCY"); ok && v >= 1 {
		cfg.concurrency = v
	}
	if v, ok := envInt("BIFRACT_MODEL_BACKFILL_MAX_THREADS"); ok && v >= 1 {
		cfg.maxThreads = v
	}
	if v, ok := envInt("BIFRACT_MODEL_BACKFILL_CHUNK_TIMEOUT"); ok && v >= 1 {
		cfg.chunkTimeoutSec = v
	}
	if v, ok := envInt64("BIFRACT_MODEL_BACKFILL_MAX_GROUPBY_BYTES"); ok && v >= 0 {
		cfg.maxGroupByBytes = v
	}
	// os_thread_priority is clamped to [0, 19]: we never let a backfill request a
	// higher-than-normal OS priority (negative nice), only an equal or lower one.
	if v, ok := envInt("BIFRACT_MODEL_BACKFILL_OS_PRIORITY"); ok {
		if v < 0 {
			v = 0
		}
		if v > 19 {
			v = 19
		}
		cfg.osThreadPriority = v
	}
	if v, ok := envInt("BIFRACT_MODEL_BACKFILL_CHUNK_SLEEP_MS"); ok && v >= 0 {
		cfg.chunkSleepMs = v
	}
	return cfg
}

func envInt(key string) (int, bool) {
	s := os.Getenv(key)
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

func envInt64(key string) (int64, bool) {
	s := os.Getenv(key)
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// timeChunk is a half-open [start, end) interval over event timestamp, aligned
// to UTC calendar days so each chunk maps to one logs partition (toDate(timestamp)).
type timeChunk struct {
	start time.Time
	end   time.Time
}

// backfillChunks splits [end-days, end] into day-aligned half-open intervals,
// oldest first. The first/last chunks may be partial days.
func backfillChunks(end time.Time, days int) []timeChunk {
	end = end.UTC()
	start := end.Add(-time.Duration(days) * 24 * time.Hour)
	var chunks []timeChunk
	cur := start
	for cur.Before(end) {
		dayEnd := time.Date(cur.Year(), cur.Month(), cur.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
		chunkEnd := dayEnd
		if chunkEnd.After(end) {
			chunkEnd = end
		}
		chunks = append(chunks, timeChunk{start: cur, end: chunkEnd})
		cur = chunkEnd
	}
	return chunks
}

// StartBackfill launches a historical backfill for a model. It returns quickly;
// the work runs in a background goroutine.
//
// Re-running a backfill against already-seeded data would double-count (the
// AggregatingMergeTree sums event_count), so the action is status-gated:
//   - none:               fresh start over the requested window.
//   - failed / cancelled: RESUME from the saved cursor (window is ignored), so
//     previously-seeded days are not re-inserted.
//   - running / completed: rejected.
//
// The status transition is an atomic conditional UPDATE, so concurrent callers
// cannot both launch a run.
func (m *Manager) StartBackfill(ctx context.Context, model *Model, window string) error {
	if model.Status != "active" {
		return fmt.Errorf("model is not active (status=%s)", model.Status)
	}
	if model.CHTableName == "" {
		return fmt.Errorf("model has no data table yet")
	}

	switch model.BackfillStatus {
	case "", "none":
		days, ok := BackfillWindowDays(window)
		if !ok {
			return fmt.Errorf("invalid backfill window: %s", window)
		}
		// Anchor the dedup boundary at the model's creation time: the live MV owns
		// every row ingested at/after creation, so the backfill takes only rows
		// ingested strictly before it. started_at is the stable event-time end so
		// chunking is deterministic across restarts (resume-safe).
		anchor := model.CreatedAt.UTC()
		startedAt := time.Now().UTC()
		total := len(backfillChunks(startedAt, days))
		res, err := m.pg.Exec(ctx,
			`UPDATE analytics_models
			    SET backfill_status='running', backfill_window=$1, backfill_total=$2,
			        backfill_done=0, backfill_anchor=$3, backfill_started_at=$4, backfill_error=''
			  WHERE id=$5 AND status='active' AND backfill_status IN ('none','')`,
			window, total, anchor, startedAt, model.ID)
		if err != nil {
			return fmt.Errorf("mark backfill running: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("backfill already started")
		}
	case "failed", "cancelled":
		// Resume: keep window/anchor/started_at/done so we continue from the cursor.
		res, err := m.pg.Exec(ctx,
			`UPDATE analytics_models SET backfill_status='running', backfill_error=''
			  WHERE id=$1 AND status='active' AND backfill_status IN ('failed','cancelled')`,
			model.ID)
		if err != nil {
			return fmt.Errorf("resume backfill: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("backfill cannot be resumed")
		}
	default: // running, completed
		return fmt.Errorf("backfill already %s", model.BackfillStatus)
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	m.registerCancel(model.ID, cancel)
	go m.runBackfill(bgCtx, model.ID)
	return nil
}

// CancelBackfill stops an in-flight backfill for a model, if one is running.
// The goroutine records a 'cancelled' status; partial data is kept.
func (m *Manager) CancelBackfill(id string) {
	m.bfMu.Lock()
	cancel := m.bfCancels[id]
	m.bfMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *Manager) registerCancel(id string, cancel context.CancelFunc) {
	m.bfMu.Lock()
	if old := m.bfCancels[id]; old != nil {
		old()
	}
	m.bfCancels[id] = cancel
	m.bfMu.Unlock()
}

func (m *Manager) unregisterCancel(id string) {
	m.bfMu.Lock()
	delete(m.bfCancels, id)
	m.bfMu.Unlock()
}

// runBackfill executes the chunked INSERT...SELECT. It performs NO DDL: it only
// inserts into the model's existing data table, so it can never orphan a table
// or materialized view.
func (m *Manager) runBackfill(ctx context.Context, id string) {
	defer m.unregisterCancel(id)

	// Global single-flight: wait for a slot (cancellable).
	select {
	case m.bfSem <- struct{}{}:
		defer func() { <-m.bfSem }()
	case <-ctx.Done():
		m.finishBackfill(id, "cancelled", "")
		return
	}

	model, err := m.Get(context.Background(), id)
	if err != nil {
		log.Printf("model %s: backfill load model: %v", id, err)
		m.finishBackfill(id, "failed", "model lookup failed")
		return
	}
	if model.Status != "active" || model.CHTableName == "" {
		m.finishBackfill(id, "failed", "model not active")
		return
	}

	window, anchor, startedAt, done, err := m.getBackfillState(context.Background(), id)
	if err != nil {
		log.Printf("model %s: backfill state: %v", id, err)
		m.finishBackfill(id, "failed", "backfill state load failed")
		return
	}
	days, ok := BackfillWindowDays(window)
	if !ok {
		m.finishBackfill(id, "failed", "invalid backfill window")
		return
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}

	chunks := backfillChunks(startedAt, days)

	// Cluster-aware tables: read all shards via the distributed logs table, write
	// the distributed model table so rows balance across shards. Single-node uses
	// the plain local tables. Both targets already exist (created with the model).
	sourceTable := "`" + m.ch.ReadTable() + "`"
	targetTable := "`" + m.readTableName(model) + "`"

	// os_thread_priority sets the OS nice() of the chunk's query threads so the
	// Linux scheduler hands CPU to interactive (priority-0) query threads first;
	// this is what keeps the platform responsive while a backfill runs, since the
	// health check only pauses *between* chunks and a single chunk can run for
	// minutes. priority is the in-ClickHouse scheduler hint (secondary).
	settings := fmt.Sprintf(
		" SETTINGS max_threads=%d, max_execution_time=%d, max_bytes_before_external_group_by=%d, priority=10, os_thread_priority=%d",
		m.bfCfg.maxThreads, m.bfCfg.chunkTimeoutSec, m.bfCfg.maxGroupByBytes, m.bfCfg.osThreadPriority)
	anchorLit := anchor.UTC().Format("2006-01-02 15:04:05")

	for i, ch := range chunks {
		if i < done {
			continue // resume: skip already-completed chunks
		}
		if err := m.waitForHealth(ctx); err != nil {
			m.finishBackfill(id, "cancelled", "")
			return
		}

		whereExtra := fmt.Sprintf(
			"timestamp >= '%s' AND timestamp < '%s' AND ingest_timestamp < '%s'",
			ch.start.UTC().Format("2006-01-02 15:04:05"),
			ch.end.UTC().Format("2006-01-02 15:04:05"),
			anchorLit)

		sqlStr, err := BuildBackfillInsert(model.Definition, model.ModelType, targetTable, sourceTable, whereExtra)
		if err != nil {
			m.finishBackfill(id, "failed", fmt.Sprintf("build query: %v", err))
			return
		}
		sqlStr += settings

		chunkCtx, chunkCancel := context.WithTimeout(ctx, time.Duration(m.bfCfg.chunkTimeoutSec)*time.Second)
		err = m.ch.Exec(chunkCtx, sqlStr)
		chunkCancel()
		if err != nil {
			if ctx.Err() != nil {
				m.finishBackfill(id, "cancelled", "")
				return
			}
			log.Printf("model %s: backfill chunk %d/%d failed: %v", id, i+1, len(chunks), err)
			m.finishBackfill(id, "failed", err.Error())
			return
		}

		done = i + 1
		if _, err := m.pg.Exec(context.Background(),
			`UPDATE analytics_models SET backfill_done=$1 WHERE id=$2`, done, id); err != nil {
			log.Printf("model %s: backfill progress update: %v", id, err)
		}

		// Cooldown between chunks: lets ClickHouse drain interactive queries and
		// background merges, and gives waitForHealth a clean window to observe
		// pressure before the next heavy INSERT...SELECT. Cancellable.
		if m.bfCfg.chunkSleepMs > 0 && i+1 < len(chunks) {
			select {
			case <-ctx.Done():
				m.finishBackfill(id, "cancelled", "")
				return
			case <-time.After(time.Duration(m.bfCfg.chunkSleepMs) * time.Millisecond):
			}
		}
	}

	m.finishBackfill(id, "completed", "")
}

// waitForHealth blocks until the cluster is healthy enough to run a chunk, or
// the context is cancelled. With no health signal configured it returns nil.
func (m *Manager) waitForHealth(ctx context.Context) error {
	if m.bfHealth == nil {
		return ctx.Err()
	}
	backoff := bfBackoffMin
	for {
		if m.bfHealth.Healthy() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < bfBackoffMax {
			backoff *= 2
			if backoff > bfBackoffMax {
				backoff = bfBackoffMax
			}
		}
	}
}

func (m *Manager) getBackfillState(ctx context.Context, id string) (window string, anchor, startedAt time.Time, done int, err error) {
	var anchorNT, startedNT sql.NullTime
	err = m.pg.QueryRow(ctx,
		`SELECT backfill_window, backfill_anchor, backfill_started_at, backfill_done
		   FROM analytics_models WHERE id=$1`, id).
		Scan(&window, &anchorNT, &startedNT, &done)
	if anchorNT.Valid {
		anchor = anchorNT.Time
	}
	if startedNT.Valid {
		startedAt = startedNT.Time
	}
	return
}

func (m *Manager) finishBackfill(id, status, errMsg string) {
	_, _ = m.pg.Exec(context.Background(),
		`UPDATE analytics_models SET backfill_status=$1, backfill_error=$2 WHERE id=$3`,
		status, errMsg, id)
}

// RecoverBackfills resumes any backfill left 'running' by a crash. Each resumes
// from its persisted cursor; models that are no longer active are marked failed.
// Call once at startup.
func (m *Manager) RecoverBackfills(ctx context.Context) {
	rows, err := m.pg.Query(ctx,
		`SELECT id FROM analytics_models WHERE backfill_status='running'`)
	if err != nil {
		log.Printf("models: recover backfills: %v", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	for _, id := range ids {
		model, err := m.Get(ctx, id)
		if err != nil {
			continue
		}
		if model.Status != "active" || model.CHTableName == "" {
			m.finishBackfill(id, "failed", "interrupted; model not active on restart")
			continue
		}
		log.Printf("model %s: resuming backfill from chunk %d/%d", id, model.BackfillDone, model.BackfillTotal)
		bgCtx, cancel := context.WithCancel(context.Background())
		m.registerCancel(id, cancel)
		go m.runBackfill(bgCtx, id)
	}
}

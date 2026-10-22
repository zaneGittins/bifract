package models

import (
	"bifract/pkg/parser"
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
// to UTC calendar days to bound the rows any one chunk aggregates. Chunks no
// longer map onto partitions: logs partitions on ingest time, and it is the
// ingest_timestamp < anchor guard on the same query that prunes.
type timeChunk struct {
	start time.Time
	end   time.Time
}

// backfillChunks splits [start, end) into day-aligned half-open intervals,
// oldest first. The first/last chunks may be partial days.
func backfillChunks(start, end time.Time) []timeChunk {
	end = end.UTC()
	var chunks []timeChunk
	cur := start.UTC()
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

// StartBackfill launches a historical backfill for a model over the request's
// window or explicit range. It returns quickly; the work runs in a background
// goroutine.
//
// Re-running a backfill against already-seeded data would double-count (the
// AggregatingMergeTree sums event_count), so the action is status-gated:
//   - none:               fresh start over the requested span.
//   - failed / cancelled: RESUME from the saved cursor (request is ignored), so
//     previously-seeded days are not re-inserted.
//   - running / completed: rejected.
//
// The status transition is an atomic conditional UPDATE, so concurrent callers
// cannot both launch a run.
func (m *Manager) StartBackfill(ctx context.Context, model *Model, req BackfillRequest) error {
	var startedAt *time.Time
	if model.ModelType.IsScheduled() {
		// Scheduled (network) models self-seed: the MV + TTL maintain the rolling
		// window from ingest, so there is nothing to backfill.
		return fmt.Errorf("backfill is not applicable to %s models (the rolling window self-seeds)", model.ModelType)
	}
	if model.Status != "active" {
		return fmt.Errorf("model is not active (status=%s)", model.Status)
	}
	if model.CHTableName == "" {
		return fmt.Errorf("model has no data table yet")
	}

	switch model.BackfillStatus {
	case "", "none":
		// The anchor is where live state took over the current table (set when the
		// table was created), so the backfill takes only rows ingested before it.
		// created_at is the fallback for models whose table predates the anchor.
		// The resolved [from, to) is stored so chunking is deterministic across
		// restarts (resume-safe).
		//
		// Truncated to what Postgres stores, because started_at is the run's
		// identity: the run's writes match on it (see backfillRun).
		started := time.Now().UTC().Truncate(time.Microsecond)
		from, to, label, err := req.Span(started)
		if err != nil {
			return err
		}
		// A windowed model keeps twice its window; days older than that would
		// be written only for the TTL to drop.
		if n := model.Definition.HistoryDaysFor(model.ModelType); n > 0 {
			if keep := started.Add(-time.Duration(parser.HistoryRetentionDays(n)-1) * 24 * time.Hour); from.Before(keep) {
				from = keep
			}
			if !from.Before(to) {
				return fmt.Errorf("the whole range is older than the %d days this model keeps", 2*n)
			}
		}
		from, to = from.Truncate(time.Microsecond), to.Truncate(time.Microsecond)
		startedAt = &started
		total := len(backfillChunks(from, to))
		res, err := m.pg.Exec(ctx,
			`UPDATE analytics_models
			    SET backfill_status='running', backfill_window=$1, backfill_total=$2,
			        backfill_done=0, backfill_anchor=COALESCE(backfill_anchor, created_at),
			        backfill_started_at=$3, backfill_from=$4, backfill_to=$5, backfill_error=''
			  WHERE id=$6 AND status='active' AND backfill_status IN ('none','')`,
			label, total, started, from, to, model.ID)
		if err != nil {
			return fmt.Errorf("mark backfill running: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("backfill already started")
		}
	case "failed", "cancelled":
		// Resume: keep window/anchor/started_at/done so we continue from the cursor.
		// The kept started_at is read back in the same statement: it is the run's
		// identity, and the model passed in may be stale.
		var started sql.NullTime
		err := m.pg.QueryRow(ctx,
			`UPDATE analytics_models SET backfill_status='running', backfill_error=''
			  WHERE id=$1 AND status='active' AND backfill_status IN ('failed','cancelled')
			  RETURNING backfill_started_at`,
			model.ID).Scan(&started)
		if err == sql.ErrNoRows {
			return fmt.Errorf("backfill cannot be resumed")
		}
		if err != nil {
			return fmt.Errorf("resume backfill: %w", err)
		}
		if started.Valid {
			startedAt = &started.Time
		}
	default: // running, completed
		return fmt.Errorf("backfill already %s", model.BackfillStatus)
	}

	m.launchBackfill(model.ID, startedAt)
	return nil
}

// backfillRun is one run of a model's backfill. startedAt is the run's
// backfill_started_at: every fresh start sets a new one, a resume keeps it, and a
// rebuild clears it when it re-arms the backfill. The run's writes to Postgres
// match on it and on backfill_status='running', so a run that was superseded
// (cancelled by a rebuild, which resets the row before the run notices) never
// overwrites the reset or the progress of the run that replaced it.
type backfillRun struct {
	id        string
	cancel    context.CancelFunc
	startedAt *time.Time
}

// startedAtArg is the run's started_at as a query argument, NULL when it has none.
func (r *backfillRun) startedAtArg() interface{} {
	if r.startedAt == nil {
		return nil
	}
	return r.startedAt.UTC()
}

// current reports whether the stored started_at is still this run's.
func (r *backfillRun) current(stored time.Time) bool {
	if r.startedAt == nil {
		return stored.IsZero()
	}
	return stored.Equal(*r.startedAt)
}

// launchBackfill registers a run and starts it in the background.
func (m *Manager) launchBackfill(id string, startedAt *time.Time) {
	ctx, cancel := context.WithCancel(context.Background())
	run := m.registerCancel(id, cancel)
	run.startedAt = startedAt
	go m.runBackfill(ctx, run)
}

// CancelBackfill stops an in-flight backfill for a model, if one is running.
// The goroutine records a 'cancelled' status; partial data is kept.
func (m *Manager) CancelBackfill(id string) {
	m.bfMu.Lock()
	run := m.bfCancels[id]
	m.bfMu.Unlock()
	if run != nil {
		run.cancel()
	}
}

// registerCancel records a model's run, cancelling the one it replaces.
func (m *Manager) registerCancel(id string, cancel context.CancelFunc) *backfillRun {
	run := &backfillRun{id: id, cancel: cancel}
	m.bfMu.Lock()
	if old := m.bfCancels[id]; old != nil {
		old.cancel()
	}
	m.bfCancels[id] = run
	m.bfMu.Unlock()
	return run
}

// unregisterCancel drops a run's registration, unless a newer run has replaced
// it: a superseded run finishing must not leave its successor uncancellable.
func (m *Manager) unregisterCancel(run *backfillRun) {
	m.bfMu.Lock()
	if m.bfCancels[run.id] == run {
		delete(m.bfCancels, run.id)
	}
	m.bfMu.Unlock()
}

// runBackfill executes the chunked INSERT...SELECT. It performs NO DDL: it only
// inserts into the model's existing data table, so it can never orphan a table
// or materialized view.
func (m *Manager) runBackfill(ctx context.Context, run *backfillRun) {
	id := run.id
	// Global single-flight: wait for a slot (cancellable).
	select {
	case m.bfSem <- struct{}{}:
		defer func() { <-m.bfSem }()
	case <-ctx.Done():
		m.finishBackfill(run, "cancelled", "")
		return
	}

	model, err := m.Get(context.Background(), id)
	if err != nil {
		log.Printf("model %s: backfill load model: %v", id, err)
		m.finishBackfill(run, "failed", "model lookup failed")
		return
	}
	if model.Status != "active" || model.CHTableName == "" {
		m.finishBackfill(run, "failed", "model not active")
		return
	}

	from, to, anchor, startedAt, done, err := m.getBackfillState(context.Background(), id)
	if err != nil {
		log.Printf("model %s: backfill state: %v", id, err)
		m.finishBackfill(run, "failed", "backfill state load failed")
		return
	}
	if !run.current(startedAt) {
		// Superseded while it queued for a slot: the row is a re-armed backfill
		// or a newer run, neither of which this run may write.
		log.Printf("model %s: backfill run superseded before it started", id)
		m.unregisterCancel(run)
		return
	}
	if from.IsZero() || !from.Before(to) {
		m.finishBackfill(run, "failed", "invalid backfill range")
		return
	}
	if anchor.IsZero() {
		// A run from before the anchor was recorded. created_at is the fallback a
		// fresh start applies; the zero time would bound every chunk to rows
		// ingested before year 1, so the run would complete having read nothing.
		anchor = model.CreatedAt
	}
	// Resolved once: every chunk renders the same predicates, and compiling per
	// chunk would re-read the scope's dictionaries on each pass.
	def, err := m.ResolveSource(context.Background(), model.Definition, model.FractalID, "")
	if err != nil {
		m.finishBackfill(run, "failed", fmt.Sprintf("source query: %v", err))
		return
	}
	chunks := backfillChunks(from, to)

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
	anchorLit := anchor.UTC().Format(chTimeLayout)

	for i, ch := range chunks {
		if i < done {
			continue // resume: skip already-completed chunks
		}
		if err := m.waitForHealth(ctx); err != nil {
			m.finishBackfill(run, "cancelled", "")
			return
		}

		whereExtra := fmt.Sprintf(
			"timestamp >= '%s' AND timestamp < '%s' AND ingest_timestamp < '%s'",
			ch.start.UTC().Format("2006-01-02 15:04:05"),
			ch.end.UTC().Format("2006-01-02 15:04:05"),
			anchorLit)

		sqlStr, err := BuildBackfillInsert(def, model.ModelType, targetTable, sourceTable, whereExtra, model.FractalID)
		if err != nil {
			m.finishBackfill(run, "failed", fmt.Sprintf("build query: %v", err))
			return
		}
		sqlStr += settings

		chunkCtx, chunkCancel := context.WithTimeout(ctx, time.Duration(m.bfCfg.chunkTimeoutSec)*time.Second)
		err = m.ch.ExecSchema(chunkCtx, sqlStr)
		chunkCancel()
		if err != nil {
			if ctx.Err() != nil {
				m.finishBackfill(run, "cancelled", "")
				return
			}
			log.Printf("model %s: backfill chunk %d/%d failed: %v", id, i+1, len(chunks), err)
			m.finishBackfill(run, "failed", err.Error())
			return
		}

		done = i + 1
		res, err := m.pg.Exec(context.Background(),
			`UPDATE analytics_models SET backfill_done=$1
			  WHERE id=$2 AND backfill_status='running' AND backfill_started_at IS NOT DISTINCT FROM $3`,
			done, id, run.startedAtArg())
		if err != nil {
			log.Printf("model %s: backfill progress update: %v", id, err)
		} else if n, _ := res.RowsAffected(); n == 0 {
			// The row is no longer this run's: a rebuild re-armed it, or a newer
			// run owns it. Stop before inserting into a table it does not own.
			log.Printf("model %s: backfill run superseded after chunk %d/%d", id, done, len(chunks))
			m.unregisterCancel(run)
			return
		}

		// Cooldown between chunks: lets ClickHouse drain interactive queries and
		// background merges, and gives waitForHealth a clean window to observe
		// pressure before the next heavy INSERT...SELECT. Cancellable.
		if m.bfCfg.chunkSleepMs > 0 && i+1 < len(chunks) {
			select {
			case <-ctx.Done():
				m.finishBackfill(run, "cancelled", "")
				return
			case <-time.After(time.Duration(m.bfCfg.chunkSleepMs) * time.Millisecond):
			}
		}
	}

	m.finishBackfill(run, "completed", "")
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

func (m *Manager) getBackfillState(ctx context.Context, id string) (from, to, anchor, startedAt time.Time, done int, err error) {
	var fromNT, toNT, anchorNT, startedNT sql.NullTime
	err = m.pg.QueryRow(ctx,
		`SELECT backfill_from, backfill_to, backfill_anchor, backfill_started_at, backfill_done
		   FROM analytics_models WHERE id=$1`, id).
		Scan(&fromNT, &toNT, &anchorNT, &startedNT, &done)
	if fromNT.Valid && toNT.Valid {
		from, to = fromNT.Time.UTC(), toNT.Time.UTC()
	}
	if anchorNT.Valid {
		anchor = anchorNT.Time
	}
	if startedNT.Valid {
		startedAt = startedNT.Time
	}
	return
}

// finishBackfill records a run's terminal status. Every exit from runBackfill
// goes through here, and the run's cancel is dropped before the status leaves
// 'running': a new run can only register once it has, so the finishing run can
// never remove the cancel of the run that replaced it, which would leave that one
// running on through a later rebuild or delete.
//
// The write applies only while the row is still this run's (see backfillRun). A
// rebuild cancels the run and then re-arms the backfill without waiting for it;
// the cancelled run's 'cancelled' landing after that would leave a backfill with
// no window, which can neither resume nor start fresh.
func (m *Manager) finishBackfill(run *backfillRun, status, errMsg string) {
	m.unregisterCancel(run)
	res, err := m.pg.Exec(context.Background(),
		`UPDATE analytics_models SET backfill_status=$1, backfill_error=$2
		  WHERE id=$3 AND backfill_status='running' AND backfill_started_at IS NOT DISTINCT FROM $4`,
		status, errMsg, run.id, run.startedAtArg())
	if err != nil {
		log.Printf("model %s: record backfill %s: %v", run.id, status, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		log.Printf("model %s: backfill run superseded; its %s status was not recorded", run.id, status)
	}
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
			m.finishBackfill(&backfillRun{id: id, startedAt: model.BackfillStartedAt}, "failed", "interrupted; model not active on restart")
			continue
		}
		log.Printf("model %s: resuming backfill from chunk %d/%d", id, model.BackfillDone, model.BackfillTotal)
		m.launchBackfill(id, model.BackfillStartedAt)
	}
}

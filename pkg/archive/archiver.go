package archive

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"bifract/pkg/spool"
	"bifract/pkg/storage"
)

// Archiver drains the durable spool into per-fractal Iceberg tables.
//
// Each fractal buffers independently and is committed on its own once it reaches
// RollBytes, so Parquet file size tracks RollBytes no matter how many fractals
// are active. The spool checkpoint is a separate concern: it may only advance
// past data that is committed, so it moves during a full flush (RollInterval or
// shutdown), never on an individual fractal's roll. A crash re-processes from the
// last checkpoint; log_id dedup on restore makes the re-processing idempotent.
type Archiver struct {
	cfg     Config
	reader  *spool.Reader
	cat     *Catalog
	mem     memory.Allocator
	enabled func() bool
	db      *sql.DB // for the archive_status heartbeat (may be nil)

	pending      map[string][]storage.LogEntry
	pendingBytes map[string]int64 // per-fractal buffered size, keyed like pending
	totalBytes   int64            // sum of pendingBytes, for the memory backstop

	lastReadCP    spool.Checkpoint
	committedCP   spool.Checkpoint // last checkpoint persisted to the spool
	lastFlush     time.Time
	lastHeartbeat time.Time

	// appliedClearGen is the "clear archive spool" generation this archiver has
	// re-synced its Reader to (see syncSpoolClear). It trails the marker the peer
	// ingest Writer stamps after resetting the shared spool.
	appliedClearGen int64
}

// NewArchiver constructs an archiver over the spool and catalog. enabled is
// polled each loop iteration so the archiver pauses draining when the runtime
// archive flag is toggled off (dormant-but-present); pass a func returning true
// for an always-on archiver. db is used for the archive_status heartbeat and may
// be nil.
func NewArchiver(cfg Config, reader *spool.Reader, cat *Catalog, enabled func() bool, db *sql.DB) *Archiver {
	if enabled == nil {
		enabled = func() bool { return true }
	}
	return &Archiver{
		cfg:          cfg,
		reader:       reader,
		cat:          cat,
		mem:          memory.DefaultAllocator,
		enabled:      enabled,
		db:           db,
		pending:      make(map[string][]storage.LogEntry),
		pendingBytes: make(map[string]int64),
		lastFlush:    time.Now(),
	}
}

// heartbeatInterval is how often the archiver refreshes archive_status while running.
const heartbeatInterval = 30 * time.Second

// Run drains the spool until ctx is cancelled. It returns ctx.Err() on shutdown
// after a final flush attempt.
func (a *Archiver) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			if err := a.flush(ctx); err != nil {
				log.Printf("[Archiver] final flush failed: %v", err)
			}
			return ctx.Err()
		default:
		}

		// Paused: archiving toggled off at runtime. Stop draining but stay alive;
		// spooled data remains durable and resumes from the checkpoint on re-enable.
		if !a.enabled() {
			if !sleep(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}

		// Apply/await an admin "clear archive spool" before draining, so a cleared
		// spool is never partially shipped to the archive.
		if !a.syncSpoolClear(ctx) {
			if !sleep(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}

		a.maybeHeartbeat(ctx)

		batch, err := a.reader.Next()
		switch {
		case err == spool.ErrNoData:
			// Caught up. Flush if the roll interval elapsed, then wait. This also
			// advances the checkpoint after a quiet period in which every fractal
			// happened to roll individually.
			if time.Since(a.lastFlush) >= a.cfg.RollInterval {
				if ferr := a.flush(ctx); ferr != nil {
					log.Printf("[Archiver] time-roll flush failed: %v", ferr)
				}
			}
			if !sleep(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		case err != nil:
			log.Printf("[Archiver] spool read error: %v", err)
			if !sleep(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}

		for i := range batch.Logs {
			e := batch.Logs[i]
			a.pending[e.FractalID] = append(a.pending[e.FractalID], e)
			n := approxSize(&e)
			a.pendingBytes[e.FractalID] += n
			a.totalBytes += n
		}
		a.lastReadCP = batch.Next

		if err := a.roll(ctx); err != nil {
			log.Printf("[Archiver] roll failed: %v", err)
			// Whatever did not commit stays buffered; retry next cycle. Back off
			// to avoid a hot loop against a failing object store.
			if !sleep(ctx, a.cfg.PollInterval) {
				return ctx.Err()
			}
		}
	}
}

// roll commits whatever is ready: any fractal that has reached RollBytes, then,
// if the buffer is still over MaxPendingBytes, the largest remaining fractals
// until it is not. A full flush (which is also what advances the spool
// checkpoint) happens once RollInterval has elapsed.
func (a *Archiver) roll(ctx context.Context) error {
	for fractalID, n := range a.pendingBytes {
		if n >= a.cfg.RollBytes {
			if err := a.commitAndDrop(ctx, fractalID); err != nil {
				return err
			}
		}
	}

	for a.totalBytes >= a.cfg.MaxPendingBytes {
		fractalID, n := a.largestPending()
		if fractalID == "" {
			break
		}
		// Not silent: this is the backstop producing a smaller-than-target file,
		// and it is the signal to raise BIFRACT_ARCHIVE_MAX_PENDING_BYTES.
		log.Printf("[Archiver] pending buffer at %d bytes (cap %d): committing fractal %s early at %d bytes, below the %d roll target",
			a.totalBytes, a.cfg.MaxPendingBytes, fractalID, n, a.cfg.RollBytes)
		if err := a.commitAndDrop(ctx, fractalID); err != nil {
			return err
		}
	}

	if time.Since(a.lastFlush) >= a.cfg.RollInterval {
		return a.flush(ctx)
	}
	return nil
}

// largestPending returns the buffered fractal holding the most bytes.
func (a *Archiver) largestPending() (string, int64) {
	var best string
	var bestN int64
	for fractalID, n := range a.pendingBytes {
		if n > bestN {
			best, bestN = fractalID, n
		}
	}
	return best, bestN
}

// commitAndDrop appends one fractal's buffer to its Iceberg table and removes it
// from the pending set. Dropping on success is what keeps a partially-failed
// flush from re-committing (and so duplicating) the fractals that already
// succeeded: a retry resumes with only what is left.
func (a *Archiver) commitAndDrop(ctx context.Context, fractalID string) error {
	logs := a.pending[fractalID]
	if len(logs) > 0 {
		if err := a.commitFractal(ctx, fractalID, logs); err != nil {
			return fmt.Errorf("commit fractal %s: %w", fractalID, err)
		}
	}
	a.totalBytes -= a.pendingBytes[fractalID]
	delete(a.pending, fractalID)
	delete(a.pendingBytes, fractalID)
	return nil
}

// flush commits every buffered fractal and then advances and truncates the
// spool. The checkpoint moves only once the buffer is empty, so it never runs
// ahead of what is durably in Iceberg. On a mid-way failure the committed
// fractals are already dropped from the buffer and the checkpoint is left
// alone, so the retry re-commits nothing and re-reads nothing.
func (a *Archiver) flush(ctx context.Context) error {
	for fractalID := range a.pending {
		if err := a.commitAndDrop(ctx, fractalID); err != nil {
			return err
		}
	}
	a.lastFlush = time.Now()

	// Everything read so far is committed. Advance the durable checkpoint, then
	// drop consumed segments. Skipped when nothing new has been read since the
	// last advance, so an idle archiver does no Postgres or filesystem work.
	if a.lastReadCP == a.committedCP {
		return nil
	}
	if err := a.reader.Commit(a.lastReadCP); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	if err := a.reader.Truncate(a.lastReadCP); err != nil {
		log.Printf("[Archiver] truncate warning: %v", err)
	}
	a.committedCP = a.lastReadCP
	markCommit(ctx, a.db)
	return nil
}

// syncSpoolClear applies a completed "clear archive spool" before draining.
//
// The peer ingest Writer performs the actual reset (while archiving is disabled)
// and stamps the shared spool's clear marker. This archiver watches that marker:
// when it advances, the spool underneath has been emptied, so the Reader's open
// handle and any in-memory pending buffer reference deleted segments and must be
// dropped, or a cleared spool would still be shipped to the archive.
//
// It returns false when a clear has been REQUESTED (the settings generation) but
// the Writer has not finished it yet (marker still behind), so Run holds off
// draining until the reset completes.
func (a *Archiver) syncSpoolClear(ctx context.Context) bool {
	if marker := spool.ReadClearGeneration(a.cfg.SpoolPath); marker > a.appliedClearGen {
		if err := a.reader.Reload(); err != nil {
			log.Printf("[Archiver] spool reload after clear failed: %v", err)
			return false
		}
		// Drop pre-clear data buffered in memory and re-anchor the checkpoints to
		// the reset spool so nothing stale is flushed.
		a.pending = make(map[string][]storage.LogEntry)
		a.pendingBytes = make(map[string]int64)
		a.totalBytes = 0
		a.lastReadCP = a.reader.Position()
		a.committedCP = a.reader.Position()
		a.appliedClearGen = marker
		log.Printf("[Archiver] applied spool clear (generation %d)", marker)
	}
	// A clear requested but not yet applied by the writer: hold off draining.
	return spoolClearRequested(ctx, a.db) <= a.appliedClearGen
}

// spoolClearRequested reads the global clear-spool generation from settings (0
// when unset or db is nil), matched per loop against the applied marker.
func spoolClearRequested(ctx context.Context, db *sql.DB) int64 {
	if db == nil {
		return 0
	}
	var v string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key=$1", spool.ClearGenerationSettingKey).Scan(&v); err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// maybeHeartbeat refreshes archive_status liveness at most every
// heartbeatInterval. Deliberately a bare Postgres UPDATE: it must not touch
// object storage. The footprint columns are written by the maintain pass
// instead, which already loads every table (see Catalog.Maintain) - collecting
// them here meant one metadata.json GET per fractal per sidecar every 30s, which
// scales with fractals x ingest replicas and grows as those files grow.
// Best-effort; failures are logged, not fatal.
func (a *Archiver) maybeHeartbeat(ctx context.Context) {
	if a.db == nil || time.Since(a.lastHeartbeat) < heartbeatInterval {
		return
	}
	a.lastHeartbeat = time.Now()
	if err := writeLiveness(ctx, a.db); err != nil {
		log.Printf("[Archiver] heartbeat write failed: %v", err)
	}
}

// commitFractal appends one fractal's buffered records as a single Iceberg
// snapshot.
func (a *Archiver) commitFractal(ctx context.Context, fractalID string, logs []storage.LogEntry) error {
	tbl, err := a.cat.EnsureTable(ctx, fractalID)
	if err != nil {
		return err
	}
	rec := buildRecord(a.mem, logs)
	defer rec.Release()

	rdr, err := array.NewRecordReader(arrowSchema(), []arrow.RecordBatch{rec})
	if err != nil {
		return err
	}
	defer rdr.Release()

	txn := tbl.NewTransaction()
	if err := txn.Append(ctx, rdr, nil); err != nil {
		return err
	}
	if _, err := txn.Commit(ctx); err != nil {
		return err
	}
	log.Printf("[Archiver] committed %d logs for fractal %s", len(logs), fractalID)
	return nil
}

// approxSize estimates an entry's live heap footprint. It deliberately counts
// the Go overhead raw byte lengths miss - the LogEntry struct, per-string
// headers, and per-map-entry bucket cost - because this number drives the
// MaxPendingBytes memory backstop, not just a file-size target. A field-dense
// entry is mostly overhead, and undercounting it is what turns a generous-looking
// buffer cap into an OOM. Generous by design rather than exact.
func approxSize(e *storage.LogEntry) int64 {
	const (
		entryOverhead = 200 // LogEntry struct + string headers + map header
		fieldOverhead = 80  // per map entry: bucket slot + key/value headers
	)
	s := entryOverhead + len(e.RawLog) + len(e.LogID) + len(e.FractalID) + len(e.Normalizer)
	for k, v := range e.Fields {
		s += fieldOverhead + len(k) + len(v)
	}
	return int64(s)
}

// sleep waits d or until ctx is cancelled. Returns false if cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

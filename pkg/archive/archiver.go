package archive

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"bifract/pkg/spool"
	"bifract/pkg/storage"
)

// Archiver drains the durable spool into per-fractal Iceberg tables. It buffers
// records per fractal and flushes them all on a size or time boundary, advancing
// the spool checkpoint only after every fractal in the batch commits. A crash
// re-processes from the last checkpoint; log_id dedup on restore makes the
// re-processing idempotent.
type Archiver struct {
	cfg     Config
	reader  *spool.Reader
	cat     *Catalog
	mem     memory.Allocator
	enabled func() bool
	db      *sql.DB // for the archive_status heartbeat (may be nil)

	pending       map[string][]storage.LogEntry
	pendingBytes  int64
	lastReadCP    spool.Checkpoint
	lastFlush     time.Time
	lastHeartbeat time.Time
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
		cfg:       cfg,
		reader:    reader,
		cat:       cat,
		mem:       memory.DefaultAllocator,
		enabled:   enabled,
		db:        db,
		pending:   make(map[string][]storage.LogEntry),
		lastFlush: time.Now(),
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

		a.maybeHeartbeat(ctx)

		batch, err := a.reader.Next()
		switch {
		case err == spool.ErrNoData:
			// Caught up. Flush if the roll interval elapsed, then wait.
			if len(a.pending) > 0 && time.Since(a.lastFlush) >= a.cfg.RollInterval {
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
			a.pendingBytes += approxSize(&e)
		}
		a.lastReadCP = batch.Next

		if a.pendingBytes >= a.cfg.RollBytes {
			if err := a.flush(ctx); err != nil {
				log.Printf("[Archiver] size-roll flush failed: %v", err)
				// Keep pending; retry next cycle. Back off to avoid a hot loop.
				if !sleep(ctx, a.cfg.PollInterval) {
					return ctx.Err()
				}
			}
		}
	}
}

// flush appends every buffered fractal's records to its Iceberg table and, only
// if all commits succeed, advances and truncates the spool. On any failure it
// leaves the buffer intact for a later retry (idempotent via log_id on restore).
func (a *Archiver) flush(ctx context.Context) error {
	if len(a.pending) == 0 {
		return nil
	}
	for fractalID, logs := range a.pending {
		if len(logs) == 0 {
			continue
		}
		if err := a.commitFractal(ctx, fractalID, logs); err != nil {
			return fmt.Errorf("commit fractal %s: %w", fractalID, err)
		}
	}
	// All fractals committed: advance the durable checkpoint, then drop consumed
	// segments.
	if err := a.reader.Commit(a.lastReadCP); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	if err := a.reader.Truncate(a.lastReadCP); err != nil {
		log.Printf("[Archiver] truncate warning: %v", err)
	}
	a.pending = make(map[string][]storage.LogEntry)
	a.pendingBytes = 0
	a.lastFlush = time.Now()
	markCommit(ctx, a.db)
	return nil
}

// maybeHeartbeat refreshes archive_status at most every heartbeatInterval so the
// admin UI shows liveness + footprint. Best-effort; failures are logged, not fatal.
func (a *Archiver) maybeHeartbeat(ctx context.Context) {
	if a.db == nil || time.Since(a.lastHeartbeat) < heartbeatInterval {
		return
	}
	a.lastHeartbeat = time.Now()
	fractals, bytes, records, err := a.cat.Stats(ctx)
	if err != nil {
		log.Printf("[Archiver] heartbeat stats failed: %v", err)
		return
	}
	if err := writeHeartbeat(ctx, a.db, fractals, bytes, records); err != nil {
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

func approxSize(e *storage.LogEntry) int64 {
	s := len(e.RawLog) + len(e.LogID) + len(e.FractalID) + 40
	for k, v := range e.Fields {
		s += len(k) + len(v)
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

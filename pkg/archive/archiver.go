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
	icetable "github.com/apache/iceberg-go/table"

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

	pending  map[string]*fractalBuffer
	totalMem int64 // sum of every buffer's mem, for the MaxPendingBytes backstop

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
		cfg:       cfg,
		reader:    reader,
		cat:       cat,
		mem:       memory.DefaultAllocator,
		enabled:   enabled,
		db:        db,
		pending:   make(map[string]*fractalBuffer),
		lastFlush: time.Now(),
	}
}

// fractalBuffer is one fractal's un-committed batch, measured two ways: payload
// predicts the Parquet file the commit will write (RollBytes), mem predicts
// whether the process fits its cgroup (MaxPendingBytes).
//
// Using mem for both was a real defect. It charges a per-FIELD constant, so a
// field-dense fractal reached the roll threshold at a fraction of the data
// volume a raw-heavy one needed and wrote correspondingly smaller files - which
// is compaction work created purely by an artifact of the estimate.
type fractalBuffer struct {
	logs    []storage.LogEntry
	payload int64
	mem     int64
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
			buf := a.pending[e.FractalID]
			if buf == nil {
				buf = &fractalBuffer{}
				a.pending[e.FractalID] = buf
			}
			buf.logs = append(buf.logs, e)
			buf.payload += payloadSize(&e)
			n := approxSize(&e)
			buf.mem += n
			a.totalMem += n
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
	for fractalID, buf := range a.pending {
		if buf.payload >= a.cfg.RollBytes {
			if err := a.commitAndDrop(ctx, fractalID); err != nil {
				return err
			}
		}
	}

	for a.totalMem >= a.cfg.MaxPendingBytes {
		fractalID, buf := a.largestPending()
		if buf == nil {
			break
		}
		// Not silent: this is the backstop producing a smaller-than-target file,
		// and it is the signal to raise BIFRACT_ARCHIVE_MAX_PENDING_BYTES. Both
		// measures are logged because their ratio is what sizing that cap needs.
		log.Printf("[Archiver] pending buffer at %d bytes of memory (cap %d): committing fractal %s early at %d payload bytes (%d in memory), below the %d roll target",
			a.totalMem, a.cfg.MaxPendingBytes, fractalID, buf.payload, buf.mem, a.cfg.RollBytes)
		if err := a.commitAndDrop(ctx, fractalID); err != nil {
			return err
		}
	}

	if time.Since(a.lastFlush) >= a.cfg.RollInterval {
		return a.flush(ctx)
	}
	return nil
}

// largestPending returns the buffered fractal holding the most MEMORY. The
// backstop it serves exists to get the process back under its memory cap, so
// memory -- not payload -- is what picks the victim.
func (a *Archiver) largestPending() (string, *fractalBuffer) {
	var bestID string
	var best *fractalBuffer
	for fractalID, buf := range a.pending {
		if best == nil || buf.mem > best.mem {
			bestID, best = fractalID, buf
		}
	}
	return bestID, best
}

// commitAndDrop appends one fractal's buffer to its Iceberg table and removes it
// from the pending set. Dropping on success is what keeps a partially-failed
// flush from re-committing (and so duplicating) the fractals that already
// succeeded: a retry resumes with only what is left.
func (a *Archiver) commitAndDrop(ctx context.Context, fractalID string) error {
	buf := a.pending[fractalID]
	if buf == nil {
		return nil
	}
	if len(buf.logs) > 0 {
		if err := a.commitFractal(ctx, fractalID, buf); err != nil {
			return fmt.Errorf("commit fractal %s: %w", fractalID, err)
		}
	}
	a.totalMem -= buf.mem
	delete(a.pending, fractalID)
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
		a.pending = make(map[string]*fractalBuffer)
		a.totalMem = 0
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
func (a *Archiver) commitFractal(ctx context.Context, fractalID string, buf *fractalBuffer) error {
	tbl, err := a.cat.EnsureTable(ctx, fractalID)
	if err != nil {
		return err
	}
	rec := buildRecord(a.mem, buf.logs)
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
	updated, err := txn.Commit(ctx)
	if err != nil {
		return err
	}
	writeVersionHint(ctx, updated)
	files, onDisk := addedFiles(updated)
	// The written size is the only figure here that is not an estimate, and its
	// ratio to payload is what tells an operator where to set RollBytes to land
	// on a file size compaction will leave alone.
	log.Printf("[Archiver] committed %d logs for fractal %s: %d payload bytes -> %d file(s), %d bytes on disk",
		len(buf.logs), fractalID, buf.payload, files, onDisk)
	return nil
}

// addedFiles reports the data files and compressed bytes the latest snapshot
// added, from its summary. Zeroes when the summary is absent, which only happens
// for a table with no snapshot yet.
func addedFiles(tbl *icetable.Table) (files int, onDisk int64) {
	snap := tbl.CurrentSnapshot()
	if snap == nil || snap.Summary == nil || snap.Summary.Properties == nil {
		return 0, 0
	}
	return snap.Summary.Properties.GetInt("added-data-files", 0),
		int64(snap.Summary.Properties.GetInt("added-files-size", 0))
}

// payloadSize is the entry's real data volume: the bytes that reach the Parquet
// writer, with no allowance for Go's in-memory overhead. It drives the roll
// threshold so that threshold means the same volume whatever the field density
// (see fractalBuffer). Deliberately NOT interchangeable with approxSize.
func payloadSize(e *storage.LogEntry) int64 {
	s := len(e.RawLog) + len(e.LogID) + len(e.FractalID) + len(e.Normalizer)
	for k, v := range e.Fields {
		s += len(k) + len(v)
	}
	return int64(s)
}

// approxSize estimates an entry's live heap footprint. It deliberately counts
// the Go overhead raw byte lengths miss - the LogEntry struct, per-string
// headers, and per-map-entry bucket cost - because this number drives the
// MaxPendingBytes memory backstop and nothing else. A field-dense entry is mostly
// overhead, and undercounting it is what turns a generous-looking buffer cap into
// an OOM. Generous by design rather than exact, which is exactly why it must not
// be used to size files; see payloadSize.
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

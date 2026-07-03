package spool

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"

	"bifract/pkg/storage"
)

// WriterOptions configures a spool Writer.
type WriterOptions struct {
	// Dir is the spool directory (shared volume). Created if absent.
	Dir string
	// MaxSegmentBytes rolls the active segment once it reaches this size.
	// Zero uses defaultMaxSegmentBytes.
	MaxSegmentBytes int64
	// MaxSegmentAge rolls the active segment after this much wall time even if
	// it has not reached MaxSegmentBytes. Zero disables time-based rotation.
	MaxSegmentAge time.Duration
}

// Writer appends batches to the spool. It is safe for concurrent use: multiple
// ingest goroutines may call Append, and their fsyncs are coalesced into a
// group commit so per-batch fsync does not cap throughput.
type Writer struct {
	dir        string
	maxSegSize int64
	maxSegAge  time.Duration

	mu       sync.Mutex // guards file, seq, segSize, segOpened, writeOff
	file     *os.File
	seq      uint64
	segSize  int64
	segStart time.Time
	writeOff int64 // total bytes written to the active file (may exceed synced)

	syncMu     sync.Mutex // serializes fsync; guards syncedOff
	syncedFile *os.File    // file the syncedOff refers to
	syncedOff  int64
}

// NewWriter opens (or creates) a spool in dir and positions itself to append to
// a fresh segment after any existing ones.
func NewWriter(opts WriterOptions) (*Writer, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("spool: writer dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("spool: create dir: %w", err)
	}
	maxSeg := opts.MaxSegmentBytes
	if maxSeg <= 0 {
		maxSeg = defaultMaxSegmentBytes
	}
	w := &Writer{
		dir:        opts.Dir,
		maxSegSize: maxSeg,
		maxSegAge:  opts.MaxSegmentAge,
	}
	// Start after the highest existing segment so we never reopen a segment the
	// reader may already consider sealed.
	seqs, err := listSegments(opts.Dir)
	if err != nil {
		return nil, err
	}
	var next uint64
	if len(seqs) > 0 {
		next = seqs[len(seqs)-1] + 1
	}
	if err := w.openSegment(next); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) openSegment(seq uint64) error {
	f, err := os.OpenFile(segmentPath(w.dir, seq), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("spool: open segment: %w", err)
	}
	w.file = f
	w.seq = seq
	w.segSize = 0
	w.segStart = time.Now()
	w.writeOff = 0
	return nil
}

// Append durably writes a batch of log entries. It returns only after the bytes
// are fsync'd to disk (fail-closed durability boundary). An empty batch is a
// no-op.
func (w *Writer) Append(logs []storage.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	payload := encodeBatch(logs)
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)

	w.mu.Lock()
	// Roll the active segment first if this frame would exceed the size cap, or
	// the age cap has elapsed, and the current segment is non-empty.
	if w.segSize > 0 && (w.segSize+int64(len(frame)) > w.maxSegSize ||
		(w.maxSegAge > 0 && time.Since(w.segStart) >= w.maxSegAge)) {
		if err := w.rotateLocked(); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	if _, err := w.file.Write(frame); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("spool: write frame: %w", err)
	}
	w.segSize += int64(len(frame))
	w.writeOff += int64(len(frame))
	file := w.file
	off := w.writeOff
	w.mu.Unlock()

	// Group commit: coalesce concurrent fsyncs. Any writer whose bytes are
	// already covered by a completed fsync returns without syncing again.
	return w.syncTo(file, off)
}

// syncTo ensures the given file has been fsync'd at least up to off.
func (w *Writer) syncTo(file *os.File, off int64) error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	if w.syncedFile == file && w.syncedOff >= off {
		return nil
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("spool: fsync: %w", err)
	}
	// After a successful sync, everything written to this file so far is durable.
	w.mu.Lock()
	synced := w.writeOff
	curFile := w.file
	w.mu.Unlock()
	if file == curFile {
		w.syncedFile = file
		w.syncedOff = synced
	} else {
		// The active segment rotated concurrently; the synced file is fully
		// durable but no longer the target for future coalescing.
		w.syncedFile = file
		w.syncedOff = off
	}
	return nil
}

// rotateLocked closes the active segment and opens the next one. Caller holds mu.
func (w *Writer) rotateLocked() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("spool: fsync on rotate: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("spool: close segment: %w", err)
	}
	return w.openSegment(w.seq + 1)
}

// DiskUsage returns the total bytes consumed by the spool on disk.
func (w *Writer) DiskUsage() (int64, error) {
	return DiskUsage(w.dir)
}

// Close flushes and closes the active segment.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Sync()
	if cerr := w.file.Close(); err == nil {
		err = cerr
	}
	w.file = nil
	return err
}

package spool

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"bifract/pkg/storage"
)

// ErrNoData is returned by Next when the spool has no complete frame available
// yet (the reader has caught up to the writer). Callers should back off and retry.
var ErrNoData = errors.New("spool: no data available")

const checkpointFile = "checkpoint.json"

// Checkpoint marks a position in the spool: everything strictly before it has
// been consumed. It is advanced by the archiver only after a durable Iceberg
// commit, so a crash re-processes rather than skips.
type Checkpoint struct {
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

// Batch is a decoded spool frame together with the checkpoint that marks the
// position immediately after it.
type Batch struct {
	Logs []storage.LogEntry
	// Next is the checkpoint to persist once this batch has been durably
	// archived. Passing it to Commit advances the consumed position past this
	// batch.
	Next Checkpoint
}

// Reader tails a spool directory. It is single-consumer: exactly one archiver
// process reads a given spool.
type Reader struct {
	dir    string
	cur    Checkpoint
	file   *os.File // open handle to segment cur.Segment, or nil
	fileNo uint64
}

// NewReader opens a spool for reading, resuming from the persisted checkpoint if
// present, otherwise from the earliest available segment.
func NewReader(dir string) (*Reader, error) {
	if dir == "" {
		return nil, fmt.Errorf("spool: reader dir is required")
	}
	r := &Reader{dir: dir}
	cp, ok, err := loadCheckpoint(dir)
	if err != nil {
		return nil, err
	}
	if ok {
		r.cur = cp
	} else {
		seqs, err := listSegments(dir)
		if err != nil {
			return nil, err
		}
		if len(seqs) > 0 {
			r.cur = Checkpoint{Segment: seqs[0], Offset: 0}
		}
	}
	return r, nil
}

// Position returns the reader's current checkpoint (position of the next frame
// to be read).
func (r *Reader) Position() Checkpoint { return r.cur }

// Next returns the next batch, or ErrNoData if the reader has caught up. It does
// NOT advance the persisted checkpoint; call Commit(batch.Next) after the batch
// is durably archived.
func (r *Reader) Next() (Batch, error) {
	for {
		if err := r.ensureFile(); err != nil {
			return Batch{}, err
		}
		if r.file == nil {
			return Batch{}, ErrNoData
		}
		batch, advanced, err := r.readFrameAt(r.cur.Offset)
		if err != nil {
			return Batch{}, err
		}
		if advanced {
			// Advance the live read cursor past this frame. (The persisted
			// checkpoint is separate and only moves on Commit, for crash
			// recovery.)
			r.cur = batch.Next
			return batch, nil
		}
		// No complete frame at the current offset. If a higher segment exists,
		// the current one is sealed: move to the next segment. Otherwise we have
		// caught up to the active segment.
		next, ok, err := r.nextSegmentAfter(r.cur.Segment)
		if err != nil {
			return Batch{}, err
		}
		if !ok {
			return Batch{}, ErrNoData
		}
		r.closeFile()
		r.cur = Checkpoint{Segment: next, Offset: 0}
	}
}

// readFrameAt attempts to read one complete frame at off in the active segment.
// advanced is false when no complete frame is present (caller decides whether to
// roll to the next segment or report ErrNoData).
func (r *Reader) readFrameAt(off int64) (batch Batch, advanced bool, err error) {
	// Determine the current file size so we never trust an on-disk length past
	// the end of the segment. A misaligned or torn read must become an
	// "incomplete frame" (retry / roll) rather than a giant allocation.
	fi, err := r.file.Stat()
	if err != nil {
		return Batch{}, false, err
	}
	size := fi.Size()
	if off+frameHeaderSize > size {
		return Batch{}, false, nil // not even a full header yet
	}
	var hdr [frameHeaderSize]byte
	if _, err := r.file.ReadAt(hdr[:], off); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Batch{}, false, nil
		}
		return Batch{}, false, err
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[0:])
	wantCRC := binary.LittleEndian.Uint32(hdr[4:])
	// Bound the payload by the bytes actually present. If the header claims more
	// than the segment holds, the frame is not yet complete (active segment) or
	// the tail is torn (sealed segment) - either way, do not allocate for it.
	if off+frameHeaderSize+int64(payloadLen) > size {
		return Batch{}, false, nil
	}
	payload := make([]byte, payloadLen)
	if _, err := r.file.ReadAt(payload, off+frameHeaderSize); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Partial frame: writer may still be flushing (active segment) or a
			// torn tail. Treat as "not yet available"; the caller rolls forward
			// only when a higher, sealed segment exists.
			return Batch{}, false, nil
		}
		return Batch{}, false, err
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		// Incomplete/torn write still becoming visible; retry later.
		return Batch{}, false, nil
	}
	logs, err := decodeBatch(payload)
	if err != nil {
		return Batch{}, false, fmt.Errorf("spool: decode frame at seg %d off %d: %w", r.cur.Segment, off, err)
	}
	nextOff := off + frameHeaderSize + int64(payloadLen)
	return Batch{
		Logs: logs,
		Next: Checkpoint{Segment: r.cur.Segment, Offset: nextOff},
	}, true, nil
}

// ensureFile opens the segment referenced by the current checkpoint, if needed.
func (r *Reader) ensureFile() error {
	if r.file != nil && r.fileNo == r.cur.Segment {
		return nil
	}
	r.closeFile()
	if r.cur.Segment == 0 {
		// No segment has been established yet; discover the earliest one.
		seqs, err := listSegments(r.dir)
		if err != nil {
			return err
		}
		if len(seqs) == 0 {
			return nil
		}
		r.cur = Checkpoint{Segment: seqs[0], Offset: 0}
	}
	f, err := os.Open(segmentPath(r.dir, r.cur.Segment))
	if err != nil {
		if os.IsNotExist(err) {
			// Checkpointed segment was truncated away; resume from earliest.
			seqs, lerr := listSegments(r.dir)
			if lerr != nil {
				return lerr
			}
			if len(seqs) == 0 {
				return nil
			}
			r.cur = Checkpoint{Segment: seqs[0], Offset: 0}
			f, err = os.Open(segmentPath(r.dir, r.cur.Segment))
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	r.file = f
	r.fileNo = r.cur.Segment
	return nil
}

func (r *Reader) nextSegmentAfter(seq uint64) (uint64, bool, error) {
	seqs, err := listSegments(r.dir)
	if err != nil {
		return 0, false, err
	}
	for _, s := range seqs {
		if s > seq {
			return s, true, nil
		}
	}
	return 0, false, nil
}

func (r *Reader) closeFile() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

// Commit persists the checkpoint so a restart resumes past already-archived
// batches. It writes atomically (temp file + rename).
func (r *Reader) Commit(cp Checkpoint) error {
	return saveCheckpoint(r.dir, cp)
}

// Truncate deletes sealed segments fully consumed by cp (segment sequence
// strictly less than cp.Segment). The segment cp points into is retained.
func (r *Reader) Truncate(cp Checkpoint) error {
	seqs, err := listSegments(r.dir)
	if err != nil {
		return err
	}
	for _, s := range seqs {
		if s < cp.Segment {
			if err := os.Remove(segmentPath(r.dir, s)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("spool: truncate segment %d: %w", s, err)
			}
		}
	}
	return nil
}

// Reload drops the open segment handle and re-reads the persisted checkpoint from
// disk, so a Reader picks up an out-of-band spool reset (segments + checkpoint
// removed by Writer.Reset in the peer ingest process). Dropping the handle is the
// point: without it, a Reader holding an open handle to a now-deleted segment
// would keep reading the unlinked inode and drain data the operator cleared. With
// the checkpoint gone it resumes from the earliest surviving segment, or reports
// ErrNoData until the writer produces one.
func (r *Reader) Reload() error {
	r.closeFile()
	cp, ok, err := loadCheckpoint(r.dir)
	if err != nil {
		return err
	}
	if ok {
		r.cur = cp
		return nil
	}
	r.cur = Checkpoint{}
	seqs, err := listSegments(r.dir)
	if err != nil {
		return err
	}
	if len(seqs) > 0 {
		r.cur = Checkpoint{Segment: seqs[0], Offset: 0}
	}
	return nil
}

// Close releases the open segment handle.
func (r *Reader) Close() error {
	r.closeFile()
	return nil
}

func loadCheckpoint(dir string) (Checkpoint, bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, checkpointFile))
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return Checkpoint{}, false, fmt.Errorf("spool: parse checkpoint: %w", err)
	}
	return cp, true, nil
}

func saveCheckpoint(dir string, cp Checkpoint) error {
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, checkpointFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, checkpointFile))
}

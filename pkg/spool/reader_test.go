package spool

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bifract/pkg/storage"
)

func mustWriter(t *testing.T, opts WriterOptions) *Writer {
	t.Helper()
	w, err := NewWriter(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func mustReader(t *testing.T, dir string) *Reader {
	t.Helper()
	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// TestReaderResumesInsideFirstSegment is the restart case on a fresh or freshly
// reset spool, where the writer's first segment is number 0. A reader restarting
// from a checkpoint inside segment 0 must resume at that offset; rewinding to the
// start of the segment would archive every already-committed batch a second time.
func TestReaderResumesInsideFirstSegment(t *testing.T) {
	dir := t.TempDir()
	w := mustWriter(t, WriterOptions{Dir: dir})
	for i := 0; i < 5; i++ {
		if err := w.Append(sampleLogs(2, i*2)); err != nil {
			t.Fatal(err)
		}
	}

	r1 := mustReader(t, dir)
	var cp Checkpoint
	for i := 0; i < 3; i++ {
		b, err := r1.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		cp = b.Next
	}
	if cp.Segment != 0 || cp.Offset == 0 {
		t.Fatalf("setup: checkpoint %+v should point inside segment 0", cp)
	}
	if err := r1.Commit(cp); err != nil {
		t.Fatal(err)
	}
	r1.Close()

	r2 := mustReader(t, dir)
	if got := r2.Position(); got != cp {
		t.Fatalf("Position after restart = %+v, want %+v", got, cp)
	}
	rest := drainAll(t, r2)
	if len(rest) != 4 {
		t.Fatalf("resumed reader returned %d logs, want the 4 not yet committed", len(rest))
	}
	if rest[0].LogID != "log-00000006" {
		t.Fatalf("first resumed log = %s, want log-00000006 (no rewind, no gap)", rest[0].LogID)
	}

	// Reload is the other path that re-reads the persisted checkpoint.
	if err := r2.Commit(cp); err != nil {
		t.Fatal(err)
	}
	if err := r2.Reload(); err != nil {
		t.Fatal(err)
	}
	if got := drainAll(t, r2); len(got) != 4 {
		t.Fatalf("after Reload the reader returned %d logs, want 4", len(got))
	}
}

// TestReaderEmptyAndMissing covers a spool with no segments yet: the archiver
// starts before ingest has written anything and must simply wait.
func TestReaderEmptyAndMissing(t *testing.T) {
	if _, err := NewReader(""); err == nil {
		t.Fatal("NewReader with no dir should fail")
	}
	if _, err := NewReader(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("NewReader on a missing dir should fail rather than report an empty spool")
	}

	dir := t.TempDir()
	r := mustReader(t, dir)
	if _, err := r.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("Next on empty spool = %v, want ErrNoData", err)
	}

	// The first segment appears after the reader started.
	w := mustWriter(t, WriterOptions{Dir: dir})
	if err := w.Append(sampleLogs(3, 0)); err != nil {
		t.Fatal(err)
	}
	if got := drainAll(t, r); len(got) != 3 {
		t.Fatalf("reader picked up %d logs from a segment created after it started, want 3", len(got))
	}
}

// TestReaderTornTailOnActiveSegment: a frame the writer is still writing must
// read as "not yet available", and become readable once complete, without the
// reader skipping it. Cuts cover a partial header, a bare header, and a payload
// one byte short.
func TestReaderTornTailOnActiveSegment(t *testing.T) {
	first := encodeFrame(sampleLogs(1, 0))
	second := encodeFrame(sampleLogs(2, 1))
	for _, cut := range []int{3, frameHeaderSize, len(second) - 1} {
		dir := t.TempDir()
		seg := segmentPath(dir, 0)
		if err := os.WriteFile(seg, append(append([]byte(nil), first...), second[:cut]...), 0o644); err != nil {
			t.Fatal(err)
		}

		r := mustReader(t, dir)
		if b, err := r.Next(); err != nil || len(b.Logs) != 1 {
			t.Fatalf("cut %d: first complete frame: logs=%d err=%v", cut, len(b.Logs), err)
		}
		if _, err := r.Next(); !errors.Is(err, ErrNoData) {
			t.Fatalf("cut %d: partial frame = %v, want ErrNoData", cut, err)
		}
		if got := r.Position(); got.Offset != int64(len(first)) {
			t.Fatalf("cut %d: cursor moved to %+v past an incomplete frame", cut, got)
		}

		// The writer finishes the frame.
		f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(second[cut:]); err != nil {
			t.Fatal(err)
		}
		f.Close()

		b, err := r.Next()
		if err != nil {
			t.Fatalf("cut %d: completed frame: %v", cut, err)
		}
		if len(b.Logs) != 2 || b.Logs[0].LogID != "log-00000001" {
			t.Fatalf("cut %d: completed frame decoded to %+v", cut, b.Logs)
		}
	}
}

// TestReaderSkipsTornTailOfSealedSegment: once a later segment exists the writer
// can never finish the earlier one (it only appends to the newest), so a torn tail
// there is abandoned and reading moves on rather than stalling forever.
func TestReaderSkipsTornTailOfSealedSegment(t *testing.T) {
	dir := t.TempDir()
	torn := encodeFrame(sampleLogs(1, 1))
	if err := os.WriteFile(segmentPath(dir, 0), append(encodeFrame(sampleLogs(1, 0)), torn[:len(torn)/2]...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segmentPath(dir, 1), encodeFrame(sampleLogs(1, 2)), 0o644); err != nil {
		t.Fatal(err)
	}
	got := drainAll(t, mustReader(t, dir))
	if len(got) != 2 || got[0].LogID != "log-00000000" || got[1].LogID != "log-00000002" {
		t.Fatalf("got %d logs %+v, want log 0 then log 2", len(got), got)
	}
}

// TestReaderCRCMismatchIsNotDecoded: a frame whose payload does not match its
// checksum is never handed to the archiver, even when it would decode.
func TestReaderCRCMismatchIsNotDecoded(t *testing.T) {
	dir := t.TempDir()
	frame := encodeFrame(sampleLogs(1, 0))
	frame[len(frame)-1] ^= 0xff // flip a payload byte (inside the last field value)
	if err := os.WriteFile(segmentPath(dir, 0), frame, 0o644); err != nil {
		t.Fatal(err)
	}
	r := mustReader(t, dir)
	if b, err := r.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("corrupt frame: got %d logs, err %v; want ErrNoData", len(b.Logs), err)
	}
}

// TestReaderUndecodableFrameFails: a frame with a valid checksum that does not
// decode is real corruption, not a torn write, and must surface as an error naming
// where it is rather than being skipped (which would silently lose data).
func TestReaderUndecodableFrameFails(t *testing.T) {
	dir := t.TempDir()
	payload := []byte{99, 0, 0, 0, 0} // unsupported codec version
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)
	if err := os.WriteFile(segmentPath(dir, 3), frame, 0o644); err != nil {
		t.Fatal(err)
	}
	r := mustReader(t, dir)
	_, err := r.Next()
	if err == nil || !strings.Contains(err.Error(), "seg 3 off 0") {
		t.Fatalf("Next = %v, want a decode error locating seg 3 off 0", err)
	}
	if got := r.Position(); got != (Checkpoint{Segment: 3}) {
		t.Fatalf("cursor advanced past an undecodable frame to %+v", got)
	}
}

// TestReaderCheckpointedSegmentTruncatedAway: if the segment a checkpoint names is
// gone (a peer truncated it), the reader resumes from the earliest surviving one.
func TestReaderCheckpointedSegmentTruncatedAway(t *testing.T) {
	dir := t.TempDir()
	if err := saveCheckpoint(dir, Checkpoint{Segment: 2, Offset: 40}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segmentPath(dir, 5), encodeFrame(sampleLogs(1, 7)), 0o644); err != nil {
		t.Fatal(err)
	}
	got := drainAll(t, mustReader(t, dir))
	if len(got) != 1 || got[0].LogID != "log-00000007" {
		t.Fatalf("got %+v, want log 7 from the earliest surviving segment", got)
	}
}

func TestReaderCorruptCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, checkpointFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A corrupt checkpoint must stop the archiver rather than silently restart
	// from the earliest segment and re-archive everything.
	if _, err := NewReader(dir); err == nil {
		t.Fatal("NewReader accepted a corrupt checkpoint")
	}
	r := &Reader{dir: dir}
	if err := r.Reload(); err == nil {
		t.Fatal("Reload accepted a corrupt checkpoint")
	}
}

// TestCommitIsReadByNextReader pins the checkpoint file round trip and that
// Commit leaves no temp file behind.
func TestCommitIsReadByNextReader(t *testing.T) {
	dir := t.TempDir()
	r := mustReader(t, dir)
	want := Checkpoint{Segment: 12, Offset: 3456}
	if err := r.Commit(want); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointFile+".tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp checkpoint left behind: %v", err)
	}
	if got := mustReader(t, dir).Position(); got != want {
		t.Fatalf("Position = %+v, want %+v", got, want)
	}
}

// TestTruncateKeepsCheckpointSegment: only segments strictly before the
// checkpoint's are fully consumed; the one it points into still holds unread data.
func TestTruncateKeepsCheckpointSegment(t *testing.T) {
	dir := t.TempDir()
	for _, seq := range []uint64{1, 2, 3, 4} {
		if err := os.WriteFile(segmentPath(dir, seq), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	r := mustReader(t, dir)
	if err := r.Truncate(Checkpoint{Segment: 3, Offset: 10}); err != nil {
		t.Fatal(err)
	}
	seqs, _ := listSegments(dir)
	if len(seqs) != 2 || seqs[0] != 3 || seqs[1] != 4 {
		t.Fatalf("segments after truncate = %v, want [3 4]", seqs)
	}
}

func encodeFrame(logs []storage.LogEntry) []byte {
	payload := encodeBatch(logs)
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)
	return frame
}

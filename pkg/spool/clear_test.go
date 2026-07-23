package spool

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWriterResetDiscardsData verifies that Reset empties the spool: after it, a
// fresh Reader sees nothing, and new appends land in the reset stream.
func TestWriterResetDiscardsData(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterOptions{Dir: dir, MaxSegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	// Force several segments so Reset has real files to remove.
	for i := 0; i < 20; i++ {
		if err := w.Append(sampleLogs(1, i)); err != nil {
			t.Fatal(err)
		}
	}
	if seqs, _ := listSegments(dir); len(seqs) < 2 {
		t.Fatalf("expected multiple segments before reset, got %d", len(seqs))
	}

	if err := w.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// A checkpoint from before the reset must be gone.
	if _, err := os.Stat(filepath.Join(dir, checkpointFile)); !os.IsNotExist(err) {
		t.Errorf("checkpoint should be removed after reset")
	}
	// A fresh reader sees an empty spool.
	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := drainAll(t, r); len(got) != 0 {
		t.Fatalf("reader should see no data after reset, got %d", len(got))
	}

	// New appends after reset are readable.
	if err := w.Append(sampleLogs(3, 999)); err != nil {
		t.Fatal(err)
	}
	r2, _ := NewReader(dir)
	if got := drainAll(t, r2); len(got) != 3 {
		t.Fatalf("post-reset appends should be readable, got %d want 3", len(got))
	}
}

// TestReaderReloadAfterReset is the cross-process case: a long-lived Reader
// (archiver) that has an open handle and a checkpoint into the OLD spool must,
// after the Writer resets underneath it, drop the stale handle on Reload and see
// the cleared state -- never drain the deleted (unlinked) data.
func TestReaderReloadAfterReset(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterOptions{Dir: dir, MaxSegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := w.Append(sampleLogs(1, i)); err != nil {
			t.Fatal(err)
		}
	}

	// Reader consumes some, holding an open handle + committed checkpoint mid-spool.
	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := r.Commit(b.Next); err != nil {
		t.Fatal(err)
	}

	// Writer resets the spool out from under the reader (the peer-process clear).
	if err := w.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Without Reload the reader might read its now-unlinked handle; Reload drops it
	// and re-syncs to the cleared spool.
	if err := r.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := drainAll(t, r); len(got) != 0 {
		t.Fatalf("after reset+reload the reader must see no data, got %d", len(got))
	}

	// The reader then picks up data written after the reset.
	if err := w.Append(sampleLogs(2, 5000)); err != nil {
		t.Fatal(err)
	}
	if got := drainAll(t, r); len(got) != 2 {
		t.Fatalf("reader should read post-reset appends, got %d want 2", len(got))
	}
}

// TestClearGenerationMarker round-trips the marker the clear handshake relies on.
func TestClearGenerationMarker(t *testing.T) {
	dir := t.TempDir()
	if got := ReadClearGeneration(dir); got != 0 {
		t.Errorf("missing marker should read 0, got %d", got)
	}
	if err := WriteClearGeneration(dir, 7); err != nil {
		t.Fatal(err)
	}
	if got := ReadClearGeneration(dir); got != 7 {
		t.Errorf("marker round-trip = %d, want 7", got)
	}
	// Reset must not remove the marker (the ingest loop stamps it after Reset).
	w, _ := NewWriter(WriterOptions{Dir: dir})
	if err := w.Append(sampleLogs(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	if got := ReadClearGeneration(dir); got != 7 {
		t.Errorf("Reset should preserve the clear marker, got %d want 7", got)
	}
}

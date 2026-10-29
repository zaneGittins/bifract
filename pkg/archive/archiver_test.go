package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"bifract/pkg/spool"
	"bifract/pkg/storage"
)

// TestFinalFlushContextOutlivesShutdown: the final flush runs after SIGTERM has
// cancelled the run context. Flushing on that context fails every commit, and the
// next start re-archives the same rows into Iceberg.
func TestFinalFlushContextOutlivesShutdown(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	ctx, done := finalFlushContext(parent)
	defer done()
	if err := ctx.Err(); err != nil {
		t.Fatalf("final flush context already done: %v", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("final flush has no deadline, so a hung commit would block shutdown")
	}
}

// TestShutdownWhileIdleAdvancesCheckpoint: an archiver caught up and sleeping
// when SIGTERM lands must still persist the checkpoint for rows it already
// committed, or the next start archives them again.
func TestShutdownWhileIdleAdvancesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := spool.NewWriter(spool.WriterOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]storage.LogEntry{{LogID: "a", FractalID: "f1", RawLog: "{}"}}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	r, err := spool.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// The batch was read and its fractal already rolled into Iceberg.
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	a := NewArchiver(Config{SpoolPath: dir, RollInterval: time.Hour, PollInterval: time.Hour},
		r, nil, func() bool { cancel(); return true }, nil)
	a.lastReadCP = b.Next

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	fresh, err := spool.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, err := fresh.Next(); !errors.Is(err, spool.ErrNoData) {
		t.Fatalf("restart re-reads committed rows (Next err = %v)", err)
	}
}

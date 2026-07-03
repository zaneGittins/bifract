package spool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"bifract/pkg/storage"
)

func sampleLogs(n, base int) []storage.LogEntry {
	logs := make([]storage.LogEntry, n)
	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		r := base + i
		logs[i] = storage.LogEntry{
			Timestamp:       ts.Add(time.Duration(r) * time.Millisecond),
			IngestTimestamp: ts.Add(time.Duration(r) * time.Millisecond),
			RawLog:          fmt.Sprintf(`{"n":%d,"msg":"line %d"}`, r, r),
			LogID:           fmt.Sprintf("log-%08d", r),
			FractalID:       fmt.Sprintf("frac-%d", r%3),
			Fields: map[string]string{
				"computer_name": fmt.Sprintf("host-%d", r%5),
				"event_id":      fmt.Sprintf("%d", 4600+r%10),
			},
		}
	}
	return logs
}

func drainAll(t *testing.T, r *Reader) []storage.LogEntry {
	t.Helper()
	var out []storage.LogEntry
	for {
		b, err := r.Next()
		if err == ErrNoData {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, b.Logs...)
		if err := r.Commit(b.Next); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	want := sampleLogs(1000, 0)
	// Append in a few batches.
	for i := 0; i < len(want); i += 100 {
		if err := w.Append(want[i : i+100]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := drainAll(t, r)
	if len(got) != len(want) {
		t.Fatalf("got %d logs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].LogID != want[i].LogID || got[i].RawLog != want[i].RawLog ||
			got[i].FractalID != want[i].FractalID ||
			got[i].Fields["computer_name"] != want[i].Fields["computer_name"] ||
			!got[i].Timestamp.Equal(want[i].Timestamp) {
			t.Fatalf("record %d mismatch: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestRotationAndTruncate(t *testing.T) {
	dir := t.TempDir()
	// Tiny segment cap forces many rotations.
	w, err := NewWriter(WriterOptions{Dir: dir, MaxSegmentBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	want := sampleLogs(500, 0)
	for i := 0; i < len(want); i += 10 {
		if err := w.Append(want[i : i+10]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	seqs, _ := listSegments(dir)
	if len(seqs) < 3 {
		t.Fatalf("expected multiple segments from rotation, got %d", len(seqs))
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Read half, commit + truncate, then verify old segments removed.
	var read int
	var lastCP Checkpoint
	for read < 250 {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		read += len(b.Logs)
		lastCP = b.Next
		if err := r.Commit(b.Next); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Truncate(lastCP); err != nil {
		t.Fatal(err)
	}
	after, _ := listSegments(dir)
	for _, s := range after {
		if s < lastCP.Segment {
			t.Fatalf("segment %d should have been truncated (cp seg %d)", s, lastCP.Segment)
		}
	}
	// Remaining records still readable.
	rest := drainAll(t, r)
	if read+len(rest) != len(want) {
		t.Fatalf("read %d + rest %d != %d", read, len(rest), len(want))
	}
}

func TestCrashRestartResume(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterOptions{Dir: dir, MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	want := sampleLogs(300, 0)
	for i := 0; i < len(want); i += 10 {
		if err := w.Append(want[i : i+10]); err != nil {
			t.Fatal(err)
		}
	}

	// Reader 1 consumes part, commits, then "crashes" (drops handle).
	r1, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	var consumed int
	for consumed < 120 {
		b, err := r1.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		consumed += len(b.Logs)
		if err := r1.Commit(b.Next); err != nil {
			t.Fatal(err)
		}
	}
	r1.Close()

	// Writer keeps appending after the crash.
	more := sampleLogs(200, 300)
	for i := 0; i < len(more); i += 10 {
		if err := w.Append(more[i : i+10]); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Reader 2 resumes from the persisted checkpoint: no dup, no loss.
	r2, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	rest := drainAll(t, r2)
	if consumed+len(rest) != len(want)+len(more) {
		t.Fatalf("consumed %d + rest %d != total %d", consumed, len(rest), len(want)+len(more))
	}
	// First resumed record must be exactly the (consumed)th appended log id.
	if len(rest) > 0 {
		wantFirst := fmt.Sprintf("log-%08d", consumed)
		if rest[0].LogID != wantFirst {
			t.Fatalf("resume gap/dup: first resumed %s want %s", rest[0].LogID, wantFirst)
		}
	}
}

func TestConcurrentAppendGroupCommit(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 8
	const perG = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				logs := sampleLogs(1, g*100000+i)
				if err := w.Append(logs); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	w.Close()

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := drainAll(t, r)
	if len(got) != goroutines*perG {
		t.Fatalf("got %d logs, want %d", len(got), goroutines*perG)
	}
}

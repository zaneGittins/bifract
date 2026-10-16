package spool

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNewWriterNeverReopensExistingSegment: on restart the writer starts a new
// segment after the highest one present. Appending to an older segment would put
// data behind a reader that already treats it as sealed and moved past it.
func TestNewWriterNeverReopensExistingSegment(t *testing.T) {
	dir := t.TempDir()
	for _, seq := range []uint64{4, 9} {
		if err := os.WriteFile(segmentPath(dir, seq), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w := mustWriter(t, WriterOptions{Dir: dir})
	if err := w.Append(sampleLogs(1, 0)); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{4, 9} {
		if fi, err := os.Stat(segmentPath(dir, seq)); err != nil || fi.Size() != 0 {
			t.Fatalf("segment %d was written to after restart (size %v, err %v)", seq, fi.Size(), err)
		}
	}
	if fi, err := os.Stat(segmentPath(dir, 10)); err != nil || fi.Size() == 0 {
		t.Fatalf("expected the append in segment 10: %v", err)
	}
}

func TestNewWriterValidatesDir(t *testing.T) {
	if _, err := NewWriter(WriterOptions{}); err == nil {
		t.Fatal("NewWriter with no dir should fail")
	}
	// A dir that cannot be created (its parent is a file).
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWriter(WriterOptions{Dir: filepath.Join(parent, "spool")}); err == nil {
		t.Fatal("NewWriter under a regular file should fail")
	}
	// Created when absent.
	dir := filepath.Join(t.TempDir(), "a", "b")
	mustWriter(t, WriterOptions{Dir: dir})
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("spool dir not created: %v", err)
	}
}

// TestAppendEmptyBatchWritesNothing: an empty batch must not leave a zero-record
// frame for the archiver to commit.
func TestAppendEmptyBatchWritesNothing(t *testing.T) {
	dir := t.TempDir()
	w := mustWriter(t, WriterOptions{Dir: dir})
	if err := w.Append(nil); err != nil {
		t.Fatal(err)
	}
	if n, _ := w.DiskUsage(); n != 0 {
		t.Fatalf("empty append wrote %d bytes", n)
	}
}

// TestAppendAfterCloseFails: once the writer is closed an Append must fail, so the
// fail-closed tee rejects the batch instead of acking data that was never written.
func TestAppendAfterCloseFails(t *testing.T) {
	w := mustWriter(t, WriterOptions{Dir: t.TempDir()})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if err := w.Append(sampleLogs(1, 0)); err == nil {
		t.Fatal("Append after Close returned nil: a batch would be acked without being spooled")
	}
}

// TestAgeRotation: with MaxSegmentAge set, a quiet spool still seals segments so
// the reader can truncate them.
func TestAgeRotation(t *testing.T) {
	dir := t.TempDir()
	w := mustWriter(t, WriterOptions{Dir: dir, MaxSegmentAge: time.Nanosecond})
	for i := 0; i < 3; i++ {
		if err := w.Append(sampleLogs(1, i)); err != nil {
			t.Fatal(err)
		}
	}
	seqs, _ := listSegments(dir)
	if len(seqs) != 3 {
		t.Fatalf("segments = %v, want one per append once each has aged out", seqs)
	}
	if got := drainAll(t, mustReader(t, dir)); len(got) != 3 {
		t.Fatalf("read back %d logs across aged segments, want 3", len(got))
	}
}

// TestOversizedFrameStillWritten: a single batch larger than the segment cap goes
// into its own segment rather than being refused or split.
func TestOversizedFrameStillWritten(t *testing.T) {
	dir := t.TempDir()
	w := mustWriter(t, WriterOptions{Dir: dir, MaxSegmentBytes: 64})
	for i := 0; i < 2; i++ {
		if err := w.Append(sampleLogs(5, i*5)); err != nil {
			t.Fatal(err)
		}
	}
	seqs, _ := listSegments(dir)
	if len(seqs) != 2 {
		t.Fatalf("segments = %v, want 2 (one oversized frame each, no empty segment)", seqs)
	}
	if got := drainAll(t, mustReader(t, dir)); len(got) != 10 {
		t.Fatalf("read back %d logs, want 10", len(got))
	}
}

// TestDiskUsageCountsOnlySegments: the spool backpressure signal must reflect the
// data awaiting archive, not the checkpoint, marker or stray files.
func TestDiskUsageCountsOnlySegments(t *testing.T) {
	if n, err := DiskUsage(filepath.Join(t.TempDir(), "missing")); err != nil || n != 0 {
		t.Fatalf("missing dir: %d, %v; want 0, nil", n, err)
	}

	dir := t.TempDir()
	write := func(name string, size int) {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(segmentName(1), 100)
	write(segmentName(2), 50)
	write(checkpointFile, 1000)
	write(clearMarkerFile, 1000)
	write("seg-notanumber.spool", 1000)
	write("other.txt", 1000)
	if err := os.Mkdir(filepath.Join(dir, segmentName(3)), 0o755); err != nil {
		t.Fatal(err)
	}
	if n, err := DiskUsage(dir); err != nil || n != 150 {
		t.Fatalf("DiskUsage = %d, %v; want 150", n, err)
	}
	seqs, err := listSegments(dir)
	if err != nil || len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("listSegments = %v, %v; want [1 2]", seqs, err)
	}
}

func TestSegmentNames(t *testing.T) {
	// Zero padding keeps lexical and numeric order the same.
	if a, b := segmentName(9), segmentName(10); a >= b {
		t.Fatalf("%s sorts after %s", a, b)
	}
	cases := []struct {
		name string
		seq  uint64
		ok   bool
	}{
		{segmentName(0), 0, true},
		{segmentName(18446744073709551615), 18446744073709551615, true},
		{"seg-7.spool", 7, true},
		{"seg-.spool", 0, false},
		{"seg--1.spool", 0, false},
		{"seg-1.spool.tmp", 0, false},
		{"xseg-1.spool", 0, false},
		{checkpointFile, 0, false},
	}
	for _, c := range cases {
		seq, ok := parseSegmentSeq(c.name)
		if ok != c.ok || seq != c.seq {
			t.Errorf("parseSegmentSeq(%q) = %d, %v; want %d, %v", c.name, seq, ok, c.seq, c.ok)
		}
	}
}

func TestClearGenerationUnreadable(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, clearMarkerFile), []byte(" 12\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ReadClearGeneration(dir); got != 12 {
		t.Fatalf("marker with whitespace = %d, want 12", got)
	}
	if err := os.WriteFile(filepath.Join(dir, clearMarkerFile), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ReadClearGeneration(dir); got != 0 {
		t.Fatalf("garbage marker = %d, want 0", got)
	}
	if err := WriteClearGeneration(filepath.Join(dir, "missing"), 1); err == nil {
		t.Fatal("WriteClearGeneration into a missing dir should fail")
	}
}

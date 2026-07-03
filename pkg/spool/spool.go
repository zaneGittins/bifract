// Package spool implements a durable, append-only, segmented on-disk spool of
// normalized log batches. It is the durability boundary for the Iceberg archive:
// the ingest tee appends a batch here (with fsync) before the request is acked,
// and the archiver sidecar tails the same directory to drain batches into Iceberg.
//
// The spool is deliberately dependency-light (standard library only) so it can be
// linked into bifract-server without pulling in Arrow/Iceberg. The on-disk format
// is a sequence of segment files, each a stream of length-prefixed, CRC-guarded
// frames; every frame holds one encoded batch of storage.LogEntry records.
package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	segmentPrefix = "seg-"
	segmentSuffix = ".spool"
	// frameHeaderSize is the fixed per-frame header: 4-byte payload length +
	// 4-byte CRC32 of the payload.
	frameHeaderSize = 8
	// defaultMaxSegmentBytes rolls a segment once it reaches this size so the
	// reader can treat lower-numbered segments as sealed and truncate them.
	defaultMaxSegmentBytes = 128 << 20 // 128 MiB
)

// segmentName returns the file name for a segment sequence number. Zero-padded
// so lexical and numeric ordering agree.
func segmentName(seq uint64) string {
	return fmt.Sprintf("%s%020d%s", segmentPrefix, seq, segmentSuffix)
}

// parseSegmentSeq extracts the sequence number from a segment file name, or
// returns ok=false if the name is not a spool segment.
func parseSegmentSeq(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	mid := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	seq, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// listSegments returns the sorted (ascending) sequence numbers of all segment
// files currently in dir.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if seq, ok := parseSegmentSeq(e.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// DiskUsage returns the total bytes consumed by all segment files in dir. Used
// to drive the spoolPressure backpressure signal. Missing directory reports 0.
func DiskUsage(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := parseSegmentSeq(e.Name()); !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}

func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, segmentName(seq))
}

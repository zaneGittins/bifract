package ingestcli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A leading run of large records used to skew the sampled estimate badly.
// CountLogs must be exact regardless of where the big lines sit.
func TestCountLogsExactWithSkewedLineSizes(t *testing.T) {
	var b strings.Builder
	const big, small = 20, 5000
	for i := 0; i < big; i++ {
		fmt.Fprintf(&b, "{\"a\":%q}\n", strings.Repeat("x", 64*1024))
	}
	for i := 0; i < small; i++ {
		fmt.Fprintf(&b, "{\"a\":%d}\n", i)
	}
	path := writeTemp(t, "skewed.json", b.String())

	got, err := CountLogs(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(big + small); got != want {
		t.Errorf("CountLogs = %d, want %d", got, want)
	}
}

func TestCountLogsNoTrailingNewline(t *testing.T) {
	path := writeTemp(t, "logs.json", "{\"a\":1}\n{\"a\":2}\n{\"a\":3}")
	got, err := CountLogs(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Errorf("CountLogs = %d, want 3", got)
	}
}

// drain collects every batch a reader emits.
func drain(t *testing.T, read func(chan<- Batch) error) []Batch {
	t.Helper()
	ch := make(chan Batch, 1024)
	errCh := make(chan error, 1)
	go func() {
		errCh <- read(ch)
		close(ch)
	}()
	var out []Batch
	for b := range ch {
		out = append(out, b)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("read: %v", err)
	}
	return out
}

// Batches must be bounded by bytes as well as log count, so that large records
// do not produce multi-megabyte requests that are expensive to retry.
func TestReadNDJSONSplitsOnBytes(t *testing.T) {
	const records = 400
	value := strings.Repeat("y", 32*1024) // ~32KB per record
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "{\"i\":%d,\"v\":%q}\n", i, value)
	}
	path := writeTemp(t, "big.json", b.String())

	stats := &Stats{}
	batches := drain(t, func(ch chan<- Batch) error {
		return readNDJSON(path, 5000, 0, ch, stats)
	})

	total := 0
	for _, batch := range batches {
		total += len(batch.Logs)
		body, err := json.Marshal(batch.Logs)
		if err != nil {
			t.Fatal(err)
		}
		// maxBatchBytes bounds the accumulated source bytes; one final record
		// may push a batch past it, so allow a single record of slack.
		if len(body) > maxBatchBytes+2*len(value) {
			t.Errorf("batch body %d bytes exceeds cap %d", len(body), maxBatchBytes)
		}
	}
	if total != records {
		t.Errorf("got %d logs, want %d", total, records)
	}
	if len(batches) < 2 {
		t.Errorf("expected byte-based splitting, got %d batch(es)", len(batches))
	}
}

// Small records must still batch by count, not fragment.
func TestReadNDJSONBatchesByCount(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 250; i++ {
		fmt.Fprintf(&b, "{\"i\":%d}\n", i)
	}
	path := writeTemp(t, "small.json", b.String())

	stats := &Stats{}
	batches := drain(t, func(ch chan<- Batch) error {
		return readNDJSON(path, 100, 0, ch, stats)
	})

	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(batches))
	}
	for i, want := range []int{100, 100, 50} {
		if len(batches[i].Logs) != want {
			t.Errorf("batch %d has %d logs, want %d", i, len(batches[i].Logs), want)
		}
	}
}

func TestReadJSONArrayBatches(t *testing.T) {
	logs := make([]map[string]interface{}, 250)
	for i := range logs {
		logs[i] = map[string]interface{}{"i": i}
	}
	data, err := json.Marshal(logs)
	if err != nil {
		t.Fatal(err)
	}
	path := writeTemp(t, "arr.json", string(data))

	stats := &Stats{}
	batches := drain(t, func(ch chan<- Batch) error {
		return readJSONArray(path, 100, 0, ch, stats)
	})

	total := 0
	for _, b := range batches {
		total += len(b.Logs)
	}
	if total != 250 {
		t.Errorf("got %d logs, want 250", total)
	}
}

// Every log must carry its source path, including the ones in a byte-split batch.
func TestReadNDJSONTagsSourcePath(t *testing.T) {
	path := writeTemp(t, "tagged.json", "{\"a\":1}\n{\"a\":2}\n")
	stats := &Stats{}
	batches := drain(t, func(ch chan<- Batch) error {
		return readNDJSON(path, 5000, 0, ch, stats)
	})
	for _, b := range batches {
		for _, l := range b.Logs {
			if l["bifract_ingest_path"] != path {
				t.Errorf("missing bifract_ingest_path: %v", l)
			}
		}
	}
}

package datasetfile

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

const exportedLine = `{"timestamp":"2026-10-29T14:04:25.816Z","log_id":"abc","fractal_id":"dc863dc8","fields":{"hostname":"WS02","event_id":"1","pid":4294967297}}`

func compress(t *testing.T, text string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// An exported line goes back in as the log it was: its fields plus the event time,
// without the exporting install's ids, and with 64-bit numbers exact.
func TestReadLogsUnwrapsExports(t *testing.T) {
	r, err := NewReader(bytes.NewReader(compress(t, exportedLine+"\n\n"+exportedLine+"\n")))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var logs []map[string]any
	var times []time.Time
	n, err := ReadLogs(r, func(l Log) error {
		logs = append(logs, l.Fields)
		times = append(times, l.Time)
		if l.Size != len(exportedLine) {
			t.Errorf("size %d, want %d", l.Size, len(exportedLine))
		}
		return nil
	})
	if err != nil || n != 2 || len(logs) != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got := logs[0]
	if got["hostname"] != "WS02" || got["timestamp"] != "2026-10-29T14:04:25.816Z" {
		t.Errorf("log = %v", got)
	}
	if _, has := got["log_id"]; has {
		t.Errorf("the exporting install's log_id was kept: %v", got)
	}
	if _, has := got["fractal_id"]; has {
		t.Errorf("the exporting install's fractal_id was kept: %v", got)
	}
	if num, ok := got["pid"]; !ok || num.(interface{ String() string }).String() != "4294967297" {
		t.Errorf("pid = %v", got["pid"])
	}
	want := time.Date(2026, 10, 29, 14, 4, 25, 816e6, time.UTC)
	if !times[0].Equal(want) {
		t.Errorf("ts = %v, want %v", times[0], want)
	}
}

// A damaged or foreign file stops the read rather than importing what it can.
func TestReadLogsRejectsOtherLines(t *testing.T) {
	for name, text := range map[string]string{
		"not json":         exportedLine + "\n{oops\n",
		"plain log":        `{"timestamp":"2026-10-29T14:04:25Z","hostname":"WS02"}` + "\n",
		"extra column":     `{"timestamp":"2026-10-29T14:04:25Z","log_id":"a","fields":{},"count":3}` + "\n",
		"no log_id":        `{"timestamp":"2026-10-29T14:04:25Z","fields":{}}` + "\n",
		"bad timestamp":    `{"timestamp":"yesterday","log_id":"a","fields":{}}` + "\n",
		"fields not a map": `{"timestamp":"2026-10-29T14:04:25Z","log_id":"a","fields":"x"}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadLogs(strings.NewReader(text), func(Log) error { return nil })
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("err = %v, want ErrFormat", err)
			}
		})
	}
}

// A log that already has a timestamp field keeps it; only a missing one is added.
func TestUnwrapKeepsFieldTimestamp(t *testing.T) {
	log, _, ok := Unwrap(map[string]any{
		"timestamp": "2026-10-29T14:04:25Z", "log_id": "a",
		"fields": map[string]any{"timestamp": "Oct 29 14:04:25"},
	})
	if !ok || log["timestamp"] != "Oct 29 14:04:25" {
		t.Errorf("log = %v ok=%v", log, ok)
	}
}

func TestCountLogs(t *testing.T) {
	n, err := CountLogs(strings.NewReader(exportedLine + "\n\n" + exportedLine))
	if err != nil || n != 2 {
		t.Errorf("n=%d err=%v", n, err)
	}
}

// A file compressed with a window larger than any dataset uses is refused instead
// of making the server allocate it.
func TestNewReaderRefusesHugeWindow(t *testing.T) {
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf, zstd.WithWindowSize(1<<29), zstd.WithSingleSegment(false))
	if err != nil {
		t.Fatal(err)
	}
	w.Write(bytes.Repeat([]byte(exportedLine+"\n"), 1000))
	w.Close()
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := CountLogs(r); err == nil {
		t.Error("a 512 MiB window was accepted")
	}
}

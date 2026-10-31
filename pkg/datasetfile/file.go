// Package datasetfile downloads and reads Exchange dataset files: zstd-compressed
// NDJSON of logs in the shape Bifract exports. The server and the CLI share it.
package datasetfile

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"

	"bifract/pkg/normalizers"
)

// maxLineBytes matches the ingest endpoint's per-line limit.
const maxLineBytes = 10 << 20

// ErrFormat reports a file that is not a dataset of exported logs.
var ErrFormat = errors.New("not a dataset file")

// exportKeys are the top-level keys of a line written by Bifract's export.
var exportKeys = map[string]bool{"timestamp": true, "log_id": true, "fractal_id": true, "fields": true}

// Unwrap turns an exported line ({timestamp, log_id, fractal_id, fields}) back into
// the log it was: its fields, with the timestamp added when the fields lack one.
// The exporting install's log and fractal ids are dropped. ok is false for any
// other shape, which is left alone.
func Unwrap(obj map[string]any) (log map[string]any, ts time.Time, ok bool) {
	fields, isMap := obj["fields"].(map[string]any)
	raw, isString := obj["timestamp"].(string)
	if !isMap || !isString {
		return obj, time.Time{}, false
	}
	if _, hasID := obj["log_id"]; !hasID {
		return obj, time.Time{}, false
	}
	for k := range obj {
		if !exportKeys[k] {
			return obj, time.Time{}, false
		}
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return obj, time.Time{}, false
	}
	if _, has := fields["timestamp"]; !has {
		fields["timestamp"] = raw
	}
	return fields, ts, true
}

// NewReader decompresses a dataset file. Close it when done.
func NewReader(r io.Reader) (*zstd.Decoder, error) {
	// Datasets use at most zstd --long=27; a file asking for a larger window is
	// refused rather than allocated.
	return zstd.NewReader(r, zstd.WithDecoderMaxWindow(1<<27), zstd.WithDecoderConcurrency(1))
}

// Log is one log read from a dataset.
type Log struct {
	Fields map[string]any
	Time   time.Time
	// Size is the length of the exported line it came from.
	Size int
}

// ReadLogs calls fn with each log in a decompressed dataset, in file order. Every
// line must be an exported log; anything else stops the read with an error naming
// the line.
func ReadLogs(r io.Reader, fn func(Log) error) (int64, error) {
	var n int64
	err := eachLine(r, func(line []byte) error {
		var obj map[string]any
		if err := normalizers.DecodeJSON(line, &obj); err != nil {
			return fmt.Errorf("%w: line %d is not JSON: %v", ErrFormat, n+1, err)
		}
		fields, ts, ok := Unwrap(obj)
		if !ok {
			return fmt.Errorf("%w: line %d is not an exported log with timestamp, log_id and fields", ErrFormat, n+1)
		}
		n++
		return fn(Log{Fields: fields, Time: ts, Size: len(line)})
	})
	return n, err
}

// CountLogs counts the non-empty lines of a decompressed dataset.
func CountLogs(r io.Reader) (int64, error) {
	var n int64
	err := eachLine(r, func([]byte) error { n++; return nil })
	return n, err
}

func eachLine(r io.Reader, fn func(line []byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), maxLineBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		if err == bufio.ErrTooLong {
			return fmt.Errorf("%w: a line is longer than %d MiB", ErrFormat, maxLineBytes>>20)
		}
		return err
	}
	return nil
}

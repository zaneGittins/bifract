package ingestcli

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// FileFormat represents a detected file format.
type FileFormat int

const (
	FormatNDJSON FileFormat = iota
	FormatJSONArray
	FormatJSONObject
	FormatCSV
	FormatUnknown
)

func (f FileFormat) String() string {
	switch f {
	case FormatNDJSON:
		return "NDJSON"
	case FormatJSONArray:
		return "JSON array"
	case FormatJSONObject:
		return "JSON object"
	case FormatCSV:
		return "CSV"
	default:
		return "unknown"
	}
}

// DetectFormat determines the file format by extension and content inspection.
func DetectFormat(path string) (FileFormat, error) {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".csv" || ext == ".tsv" {
		return FormatCSV, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return FormatUnknown, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	// Skip whitespace/newlines to find first content byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return FormatUnknown, fmt.Errorf("empty file")
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		reader.UnreadByte()
		break
	}

	firstByte, _ := reader.ReadByte()
	reader.UnreadByte()

	if firstByte == '[' {
		return FormatJSONArray, nil
	}

	if firstByte == '{' {
		// Read first line to check if it's valid JSON (NDJSON)
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return FormatUnknown, err
		}
		line = strings.TrimSpace(line)
		if json.Valid([]byte(line)) {
			return FormatNDJSON, nil
		}
		return FormatJSONObject, nil
	}

	return FormatUnknown, fmt.Errorf("unable to detect format (first byte: %c)", firstByte)
}

// CountLogs does a fast pre-scan to count entries in a file.
func CountLogs(path string) (int64, error) {
	format, err := DetectFormat(path)
	if err != nil {
		return 0, err
	}

	switch format {
	case FormatJSONObject:
		return 1, nil
	case FormatCSV:
		return countCSVRows(path)
	default:
		return countLines(path)
	}
}

// countLines counts newlines exactly. Extrapolating from a leading sample
// skews badly when line sizes vary (a 870MB Hayabusa export estimated 115K
// lines against an actual 300K), and a full scan of it costs only ~130ms.
func countLines(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if info.Size() == 0 {
		return 0, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var count int64
	var last byte
	buf := make([]byte, 4*1024*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			count += int64(bytes.Count(buf[:n], []byte{'\n'}))
			last = buf[n-1]
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}

	// A final line without a trailing newline still counts.
	if last != '\n' {
		count++
	}
	return count, nil
}

func countCSVRows(path string) (int64, error) {
	count, err := countLines(path)
	if err != nil {
		return 0, err
	}
	// Subtract 1 for the header row
	if count > 0 {
		count--
	}
	return count, nil
}

// maxBatchBytes caps the raw payload of one batch. A log count alone is not
// enough: 5,000 Hayabusa records is a 15MB request, too coarse to retry.
const maxBatchBytes = 4 << 20

// batcher flushes on whichever limit trips first, log count or raw bytes.
type batcher struct {
	ch        chan<- Batch
	batchSize int
	logs      []map[string]interface{}
	bytes     int
}

func newBatcher(ch chan<- Batch, batchSize int) *batcher {
	return &batcher{
		ch:        ch,
		batchSize: batchSize,
		logs:      make([]map[string]interface{}, 0, batchSize),
	}
}

func (b *batcher) add(log map[string]interface{}, size int) {
	b.logs = append(b.logs, log)
	b.bytes += size
	if len(b.logs) >= b.batchSize || b.bytes >= maxBatchBytes {
		b.flush()
	}
}

func (b *batcher) flush() {
	if len(b.logs) == 0 {
		return
	}
	b.ch <- Batch{Logs: b.logs}
	b.logs = make([]map[string]interface{}, 0, b.batchSize)
	b.bytes = 0
}

// ReadFile reads a file and sends log batches to the channel.
// It auto-detects the format and streams where possible.
func ReadFile(path string, batchSize, limit int, batchCh chan<- Batch, stats *Stats) error {
	format, err := DetectFormat(path)
	if err != nil {
		return fmt.Errorf("detect format: %w", err)
	}

	stats.mu.Lock()
	stats.CurrentFile = filepath.Base(path)
	stats.mu.Unlock()

	switch format {
	case FormatNDJSON:
		return readNDJSON(path, batchSize, limit, batchCh, stats)
	case FormatJSONArray:
		return readJSONArray(path, batchSize, limit, batchCh, stats)
	case FormatJSONObject:
		return readJSONObject(path, batchCh, stats)
	case FormatCSV:
		return readCSV(path, batchSize, limit, batchCh, stats)
	default:
		return fmt.Errorf("unsupported format")
	}
}

func readNDJSON(path string, batchSize, limit int, batchCh chan<- Batch, stats *Stats) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 16*1024*1024)

	b := newBatcher(batchCh, batchSize)
	count := 0

	for scanner.Scan() {
		// Unmarshal straight off the scanner's buffer: json.Unmarshal copies
		// every string it stores, so nothing outlives the next Scan.
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		if limit > 0 && count >= limit {
			break
		}

		var log map[string]interface{}
		if err := json.Unmarshal(line, &log); err != nil {
			stats.Errors.Add(1)
			continue
		}

		log["bifract_ingest_path"] = path
		b.add(log, len(line))
		count++
	}

	b.flush()

	return scanner.Err()
}

func readJSONArray(path string, batchSize, limit int, batchCh chan<- Batch, stats *Stats) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	// Read opening bracket
	t, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read array start: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected JSON array, got %v", t)
	}

	b := newBatcher(batchCh, batchSize)
	count := 0

	for dec.More() {
		if limit > 0 && count >= limit {
			break
		}

		// RawMessage first so the batcher gets a real byte size. A failure here
		// desyncs the decoder, so stop rather than spin on the same bad bytes.
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			b.flush()
			return fmt.Errorf("malformed JSON after %d logs: %w", count, err)
		}

		var log map[string]interface{}
		if err := json.Unmarshal(raw, &log); err != nil {
			stats.Errors.Add(1)
			continue
		}

		log["bifract_ingest_path"] = path
		b.add(log, len(raw))
		count++
	}

	b.flush()

	return nil
}

func readJSONObject(path string, batchCh chan<- Batch, stats *Stats) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var log map[string]interface{}
	if err := json.Unmarshal(data, &log); err != nil {
		return fmt.Errorf("invalid JSON object: %w", err)
	}

	log["bifract_ingest_path"] = path
	batchCh <- Batch{Logs: []map[string]interface{}{log}}
	return nil
}

func readCSV(path string, batchSize, limit int, batchCh chan<- Batch, stats *Stats) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // Allow variable field counts per row

	// TSV support
	if strings.ToLower(filepath.Ext(path)) == ".tsv" {
		r.Comma = '\t'
	}

	// Read headers
	headers, err := r.Read()
	if err != nil {
		return fmt.Errorf("read CSV headers: %w", err)
	}

	// Normalize header names: replace spaces with underscores
	for i, h := range headers {
		headers[i] = strings.ReplaceAll(strings.TrimSpace(h), " ", "_")
	}

	b := newBatcher(batchCh, batchSize)
	count := 0

	for {
		if limit > 0 && count >= limit {
			break
		}

		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			stats.Errors.Add(1)
			continue
		}

		log := make(map[string]interface{}, len(headers)+1)
		size := 0
		for i, header := range headers {
			if i < len(record) {
				log[header] = record[i]
				size += len(header) + len(record[i])
			}
		}

		log["bifract_ingest_path"] = path
		b.add(log, size)
		count++
	}

	b.flush()

	return nil
}

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

func countLines(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return 0, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// For small files (<= 2MB), count exactly.
	const sampleSize = 2 * 1024 * 1024
	if fileSize <= sampleSize {
		var count int64
		buf := make([]byte, sampleSize)
		for {
			n, err := f.Read(buf)
			count += int64(bytes.Count(buf[:n], []byte{'\n'}))
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, err
			}
		}
		return count, nil
	}

	// For large files, sample the first 2MB and estimate from file size.
	buf := make([]byte, sampleSize)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, err
	}

	lines := int64(bytes.Count(buf[:n], []byte{'\n'}))
	if lines == 0 {
		return 1, nil
	}

	avgBytesPerLine := float64(n) / float64(lines)
	return int64(float64(fileSize) / avgBytesPerLine), nil
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

	batch := make([]map[string]interface{}, 0, batchSize)
	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if limit > 0 && count >= limit {
			break
		}

		var log map[string]interface{}
		if err := json.Unmarshal([]byte(line), &log); err != nil {
			stats.Errors.Add(1)
			continue
		}

		log["bifract_ingest_path"] = path
		batch = append(batch, log)
		count++

		if len(batch) >= batchSize {
			batchCh <- Batch{Logs: batch}
			batch = make([]map[string]interface{}, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		batchCh <- Batch{Logs: batch}
	}

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

	batch := make([]map[string]interface{}, 0, batchSize)
	count := 0

	for dec.More() {
		if limit > 0 && count >= limit {
			break
		}

		var log map[string]interface{}
		if err := dec.Decode(&log); err != nil {
			stats.Errors.Add(1)
			continue
		}

		log["bifract_ingest_path"] = path
		batch = append(batch, log)
		count++

		if len(batch) >= batchSize {
			batchCh <- Batch{Logs: batch}
			batch = make([]map[string]interface{}, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		batchCh <- Batch{Logs: batch}
	}

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

	batch := make([]map[string]interface{}, 0, batchSize)
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
		for i, header := range headers {
			if i < len(record) {
				log[header] = record[i]
			}
		}

		log["bifract_ingest_path"] = path
		batch = append(batch, log)
		count++

		if len(batch) >= batchSize {
			batchCh <- Batch{Logs: batch}
			batch = make([]map[string]interface{}, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		batchCh <- Batch{Logs: batch}
	}

	return nil
}

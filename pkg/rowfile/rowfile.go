// Package rowfile streams the rows of a tabular file: CSV, TSV, a JSON array of
// objects, NDJSON, or a plain list of one value per line. Every format is read a
// row at a time, so a file of any size costs one row of memory.
package rowfile

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Formats are the file shapes understood, in the order they are offered.
var Formats = []string{"csv", "tsv", "json", "ndjson", "lines"}

// BOM is the byte-order mark a Windows editor leaves at the head of a file. It
// would otherwise become part of the first column's name.
const BOM = "\uFEFF"

// Reader yields a file's rows, one at a time.
type Reader interface {
	// Next returns the next row, or io.EOF at the end of the file. A row that
	// carries nothing usable returns a skip reason instead.
	Next() (row map[string]string, skip string, err error)
	// Columns is the file's own column order, where it has one. A JSON object has
	// none Go preserves, so it is nil there.
	Columns() []string
}

// ValidFormat reports whether format is one NewReader accepts.
func ValidFormat(format string) bool {
	return slices.Contains(Formats, format)
}

// NewReader starts a reader on r. listColumn names the one column a lines file
// fills, as given; it is ignored by every other format.
func NewReader(format string, r *bufio.Reader, listColumn string) (Reader, error) {
	// A byte-order mark is not part of the data in any format, and JSON refuses it.
	if head, _ := r.Peek(len(BOM)); string(head) == BOM {
		_, _ = r.Discard(len(BOM))
	}
	switch format {
	case "csv":
		return newDelimitedReader(r, ',')
	case "tsv":
		return newDelimitedReader(r, '\t')
	case "json":
		return newJSONArrayReader(r)
	case "ndjson":
		return &ndjsonReader{lines: newLineReader(r)}, nil
	case "lines":
		if listColumn == "" {
			listColumn = "value"
		}
		return &listReader{lines: newLineReader(r), column: listColumn}, nil
	}
	return nil, fmt.Errorf("unknown format %q", format)
}

// Decompressed unwraps a gzip stream, which a large list is usually shipped as.
// Detected from the magic bytes rather than a name, since a file saved from a
// browser or served from a URL often keeps neither.
func Decompressed(r *bufio.Reader) (*bufio.Reader, error) {
	magic, err := r.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(magic) < 2 || magic[0] != 0x1f || magic[1] != 0x8b {
		return r, nil
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("looks gzip-compressed but cannot be decompressed: %w", err)
	}
	return bufio.NewReaderSize(gz, 1<<16), nil
}

// Sniff reads the shape out of the first bytes of a file with nothing useful in
// its name.
func Sniff(head []byte) string {
	trimmed := bytes.TrimLeft(head, " \t\r\n"+BOM)
	switch {
	case len(trimmed) == 0:
		return "lines"
	case trimmed[0] == '[':
		return "json"
	case trimmed[0] == '{':
		return "ndjson"
	}
	line := trimmed
	if end := bytes.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	switch {
	case bytes.ContainsRune(line, '\t'):
		return "tsv"
	case bytes.ContainsRune(line, ','):
		return "csv"
	}
	return "lines"
}

// delimitedReader reads CSV or TSV, whose first record names the columns.
type delimitedReader struct {
	csv     *csv.Reader
	headers []string
}

func newDelimitedReader(r *bufio.Reader, comma rune) (*delimitedReader, error) {
	reader := csv.NewReader(r)
	reader.Comma = comma
	// A short record pads and a long one is truncated to the header, below. One
	// ragged line in a large file should cost that line, not the whole load.
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	reader.LazyQuotes = true

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("the file has no header row: %w", err)
	}
	cleaned := make([]string, len(headers))
	for i, h := range headers {
		cleaned[i] = strings.TrimSpace(h)
	}
	return &delimitedReader{csv: reader, headers: cleaned}, nil
}

func (d *delimitedReader) Columns() []string { return d.headers }

func (d *delimitedReader) Next() (map[string]string, string, error) {
	record, err := d.csv.Read()
	if err != nil {
		return nil, "", err
	}
	row := make(map[string]string, len(d.headers))
	for i, header := range d.headers {
		if header == "" || i >= len(record) {
			continue
		}
		row[header] = record[i]
	}
	return row, "", nil
}

// jsonArrayReader streams the elements of a JSON array, so a file that is one
// large array is still read a row at a time.
type jsonArrayReader struct {
	decoder *json.Decoder
}

func newJSONArrayReader(r *bufio.Reader) (*jsonArrayReader, error) {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("the file is not readable as JSON: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, errors.New("the file is JSON but not an array of objects. Use the ndjson format for one object per line")
	}
	return &jsonArrayReader{decoder: decoder}, nil
}

func (j *jsonArrayReader) Columns() []string { return nil }

func (j *jsonArrayReader) Next() (map[string]string, string, error) {
	if !j.decoder.More() {
		return nil, "", io.EOF
	}
	var element any
	if err := j.decoder.Decode(&element); err != nil {
		return nil, "", fmt.Errorf("JSON element could not be read: %w", err)
	}
	row, skip := jsonRow(element)
	return row, skip, nil
}

// ndjsonReader reads one JSON object per line.
type ndjsonReader struct {
	lines *lineReader
}

func (n *ndjsonReader) Columns() []string { return nil }

func (n *ndjsonReader) Next() (map[string]string, string, error) {
	line, err := n.lines.next()
	if err != nil {
		return nil, "", err
	}
	var element any
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	if err := decoder.Decode(&element); err != nil {
		return nil, "not JSON: " + Truncate(line), nil
	}
	row, skip := jsonRow(element)
	return row, skip, nil
}

// listReader reads a plain list, one value per line, which is how an indicator
// feed usually arrives. Blank lines and # comments are not values.
type listReader struct {
	lines  *lineReader
	column string
	read   bool
}

func (l *listReader) Columns() []string { return []string{l.column} }

func (l *listReader) Next() (map[string]string, string, error) {
	for {
		line, err := l.lines.next()
		if err != nil {
			return nil, "", err
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// A single-column CSV reads as a list, so its header would otherwise load
		// as an indicator. A first line that is the column's own name is one.
		first := !l.read
		l.read = true
		if first && strings.EqualFold(line, l.column) {
			continue
		}
		return map[string]string{l.column: line}, "", nil
	}
}

// lineReader reads lines of any length. bufio.Scanner would stop at its buffer
// limit part way through a file and report the end of it.
type lineReader struct {
	r *bufio.Reader
}

func newLineReader(r *bufio.Reader) *lineReader { return &lineReader{r: r} }

func (l *lineReader) next() (string, error) {
	for {
		line, err := l.r.ReadString('\n')
		trimmed := strings.TrimSpace(line)
		if err != nil {
			if errors.Is(err, io.EOF) && trimmed != "" {
				return trimmed, nil
			}
			return "", err
		}
		if trimmed != "" {
			return trimmed, nil
		}
	}
}

// jsonRow flattens one decoded element into string fields. A nested value keeps
// its JSON text: a dictionary column is a string, and dropping it would lose
// data the file carried.
func jsonRow(element any) (map[string]string, string) {
	object, ok := element.(map[string]any)
	if !ok {
		return nil, "not an object: " + Truncate(fmt.Sprint(element))
	}
	row := make(map[string]string, len(object))
	for name, value := range object {
		row[name] = jsonValue(value)
	}
	return row, ""
}

func jsonValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		if typed {
			return "true"
		}
		return "false"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(encoded)
}

// ColumnName maps a file's column onto a name the dictionary schema accepts:
// a letter or underscore, then letters, digits, underscores and hyphens, and
// never the _bf_ prefix the row table reserves.
func ColumnName(raw string) string {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		}
		return '_'
	}, strings.TrimSpace(raw))

	if cleaned == "" {
		cleaned = "column"
	}
	if first := cleaned[0]; !(first >= 'a' && first <= 'z' || first >= 'A' && first <= 'Z' || first == '_') {
		cleaned = "_" + cleaned
	}
	if strings.HasPrefix(strings.ToLower(cleaned), "_bf_") {
		cleaned = "f" + cleaned
	}
	// Every rune left is one byte, so this cannot split one.
	if len(cleaned) > 255 {
		cleaned = cleaned[:255]
	}
	return cleaned
}

// ErrLineTooLong is returned by a LimitLines reader.
var ErrLineTooLong = errors.New("a line is too long")

// LimitLines fails a read once a line runs past max bytes. Every reader here
// holds a whole line, CSV field or JSON value in memory, so on input from
// someone else this is what bounds the memory one row can take.
func LimitLines(r io.Reader, max int) io.Reader {
	return &lineLimit{r: r, max: max}
}

type lineLimit struct {
	r   io.Reader
	max int
	run int
}

func (l *lineLimit) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	for data := p[:n]; len(data) > 0; {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			l.run += len(data)
			break
		}
		if l.run+i > l.max {
			return 0, fmt.Errorf("%w: over %d bytes", ErrLineTooLong, l.max)
		}
		l.run, data = 0, data[i+1:]
	}
	if l.run > l.max {
		return 0, fmt.Errorf("%w: over %d bytes", ErrLineTooLong, l.max)
	}
	return n, err
}

// Truncate shortens a value for a skip reason or an error message.
func Truncate(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "..."
}

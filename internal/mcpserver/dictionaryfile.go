package mcpserver

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"bifract/pkg/aitools"
	"bifract/pkg/rowfile"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// uploadChunkRows and uploadChunkBytes bound one request, whichever binds
	// first. A chunk is the unit of retry, so it is sized to be re-sent cheaply
	// rather than to minimise round trips.
	uploadChunkRows  = 20_000
	uploadChunkBytes = 4 << 20
	// uploadAttempts is per chunk. Re-sending is safe: rows are keyed, so a chunk
	// that landed before the response was lost is overwritten by itself.
	uploadAttempts = 4
	uploadBackoff  = 750 * time.Millisecond
	// uploadChunkTimeout bounds one request. It is not BIFRACT_TIMEOUT: that
	// bounds an interactive call, and a chunk is an insert of up to 20k rows.
	uploadChunkTimeout = 5 * time.Minute
	// maxColumns caps how wide a file may make a dictionary. A file that carries
	// a distinct field per row would otherwise widen the table without bound.
	maxColumns = 128
	// maxSkipsReported keeps the summary readable when a whole file is wrong.
	maxSkipsReported = 5
)

// uploadFormats are the file shapes understood, in the order they are offered.
var uploadFormats = append([]string{"auto"}, rowfile.Formats...)

type uploadDictionaryFileArgs struct {
	Path string `json:"path" jsonschema:"Path to the file on the machine this MCP server runs on. A .gz file is decompressed as it is read."`
	// One of the two identifies the target. A name that matches nothing is
	// created, which is what loading a new watchlist means.
	DictionaryID   string `json:"dictionary_id,omitempty" jsonschema:"The dictionary to load into, from list_dictionaries. Give this or dictionary_name."`
	DictionaryName string `json:"dictionary_name,omitempty" jsonschema:"Name of the dictionary to load into. One that does not exist yet is created from the file's columns."`
	FractalID      string `json:"fractal_id,omitempty" jsonschema:"The fractal to act in, from list_fractals. Needed only for an instance-wide key, which belongs to no fractal of its own; a key issued for one fractal or prism ignores it."`
	IsGlobal       bool   `json:"is_global,omitempty" jsonschema:"When creating the dictionary, make it visible to every fractal rather than only the one it is created in."`
	Format         string `json:"format,omitempty" jsonschema:"File format: auto (default) csv tsv json ndjson or lines. lines is one value per line, for a plain indicator list."`
	KeyField       string `json:"key_field,omitempty" jsonschema:"The column in the file holding the lookup key, when it is not already named as the dictionary's key column. Its values are written to that column."`
	Column         string `json:"column,omitempty" jsonschema:"For the lines format only: the column each line's value is written to. Defaults to the dictionary's key column."`
	DryRun         bool   `json:"dry_run,omitempty" jsonschema:"Parse the file and report what would be written without writing anything."`
}

// addDictionaryFileTool registers the one tool that reads the caller's
// filesystem. It stays out of aitools because the MCP server runs on the
// analyst's machine, where the path names their own file, while the chat backend
// runs on the server, where the same tool would turn a prompt into a read of any
// file the instance can see.
func addDictionaryFileTool(s *mcp.Server, c *Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "upload_dictionary_file",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: new(bool)},
		Description: "Load a local file into a dictionary: a watchlist of indicators, an asset " +
			"inventory, a user-to-team mapping.\n\n" +
			"Give the path and the dictionary. The file is read here, on the machine this server " +
			"runs on, and streamed to Bifract in chunks, so its size is not bounded by this " +
			"conversation: a file of millions of rows is one call. Never read a file yourself and " +
			"pass its rows to add_dictionary_rows; that is what this tool is for.\n\n" +
			"CSV, TSV, JSON array, NDJSON, and a plain one-value-per-line list are recognised from " +
			"the extension and content, and .gz is decompressed as it is read. Columns the " +
			"dictionary does not have are added; a column name the schema will not take is " +
			"renamed and the rename reported. Rows are keyed, so re-uploading a corrected file " +
			"updates rather than duplicates, and rows the file no longer lists stay as they are.\n\n" +
			"This changes what live detections match on. Use dry_run first on a file whose shape " +
			"is not known. Where the key is instance-wide, pass fractal_id: it belongs to no " +
			"fractal, and a load with none named would go wherever the server defaults to. Pass " +
			"is_global to create a watchlist every fractal can see.\n\n" +
			"Returns what was written and where: rows, the fractal and scope of the dictionary, " +
			"columns added, and anything skipped.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in uploadDictionaryFileArgs) (*mcp.CallToolResult, any, error) {
		out, failure := uploadDictionaryFile(ctx, c, in, progressReporter(ctx, req))
		if out == nil {
			// Nothing was attempted, so there is nothing to report but why.
			return nil, nil, failure
		}
		text, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, nil, err
		}
		// A load that failed part way through still has to say what landed, which
		// a bare error would throw away.
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(text)}},
			IsError: failure != nil,
		}, nil, nil
	})
}

// progressReporter reports rows loaded so far, for a client that asked to be
// told. One that did not gets a reporter that does nothing, so the upload path
// has no branch of its own.
func progressReporter(ctx context.Context, req *mcp.CallToolRequest) func(rows int, message string) {
	token := req.Params.GetProgressToken()
	if token == nil || req.Session == nil {
		return func(int, string) {}
	}
	return func(rows int, message string) {
		_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
			ProgressToken: token,
			Progress:      float64(rows),
			Message:       message,
		})
	}
}

func uploadDictionaryFile(ctx context.Context, c *Client, in uploadDictionaryFileArgs, report func(int, string)) (any, error) {
	path := strings.TrimSpace(in.Path)
	if path == "" {
		return nil, errors.New("no path given, so there is nothing to load")
	}
	if !filepath.IsAbs(path) {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("%s cannot be resolved to a full path: %w", path, err)
		}
		path = absolute
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w. The path is read on the machine this MCP server runs on", path, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory. Give the file to load", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("%s is empty", path)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", path, err)
	}
	defer file.Close()

	source, err := rowfile.Decompressed(bufio.NewReaderSize(file, 1<<16))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}

	format, err := resolveFormat(in.Format, path, source)
	if err != nil {
		return nil, err
	}

	// Resolved before anything is written: a key that belongs to no fractal would
	// otherwise load into whichever one the server falls back to, and a watchlist
	// in the wrong fractal detects nothing while looking like it worked.
	ctx, err = c.ResolveScope(ctx, in.FractalID)
	if err != nil {
		return nil, err
	}

	dict, created, err := resolveTargetDictionary(ctx, c, in, in.DryRun)
	if err != nil {
		return nil, err
	}

	// A dictionary that has no key column yet takes one from this file: the
	// named key field, or its leading column once the first row names it.
	key := aitools.Field[string](dict, "key_column")
	if key == "" && in.KeyField != "" {
		key = rowfile.ColumnName(in.KeyField)
	}

	// A lines file fills one column: the one named, else the key, else "value"
	// for a new dictionary that has no key column yet.
	listColumn := strings.TrimSpace(in.Column)
	if listColumn == "" {
		listColumn = key
	}
	if listColumn == "" {
		listColumn = "value"
	}
	reader, err := rowfile.NewReader(format, source, rowfile.ColumnName(listColumn))
	if err != nil {
		return nil, err
	}
	var rows rowfile.Reader = reader
	if format != "lines" {
		rows = mappedReader{Reader: reader, mapping: uploadMapping{keyField: strings.TrimSpace(in.KeyField), key: key}}
	}

	up := &uploader{
		client:  c,
		dict:    dict,
		order:   rows.Columns(),
		key:     key,
		dryRun:  in.DryRun,
		report:  report,
		started: time.Now(),
	}
	if !created && in.IsGlobal && !aitools.Field[bool](dict, "is_global") {
		up.notes = append(up.notes, "is_global applies only when the dictionary is created. "+
			aitools.Field[string](dict, "name")+" already exists and is not global; change that in the dictionary editor")
	}

	if err := up.run(ctx, rows); err != nil {
		return up.summary(path, format, created, err), err
	}
	return up.summary(path, format, created, nil), nil
}

// resolveFormat settles what the file is, from the argument, then the extension,
// then its first bytes. Sniffing is last so a caller can always override it.
func resolveFormat(requested, path string, r *bufio.Reader) (string, error) {
	if named := strings.ToLower(strings.TrimSpace(requested)); named != "" && named != "auto" {
		if !slices.Contains(uploadFormats, named) {
			return "", fmt.Errorf("unknown format %q. One of: %s", requested, strings.Join(uploadFormats[1:], ", "))
		}
		return named, nil
	}

	name := strings.ToLower(strings.TrimSuffix(filepath.Base(path), ".gz"))
	switch filepath.Ext(name) {
	case ".csv":
		return "csv", nil
	case ".tsv", ".tab":
		return "tsv", nil
	case ".ndjson", ".jsonl":
		return "ndjson", nil
	case ".json":
		return "json", nil
	}

	head, err := r.Peek(8 << 10)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
		return "", err
	}
	return rowfile.Sniff(head), nil
}

// resolveTargetDictionary finds the dictionary to load into, creating one where a
// name was given for a dictionary that does not exist yet. It reports whether it
// created it, which belongs in the summary: a typo in a name would otherwise
// leave a second, near-empty watchlist and read as a successful load.
func resolveTargetDictionary(ctx context.Context, c *Client, in uploadDictionaryFileArgs, dryRun bool) (any, bool, error) {
	if id := strings.TrimSpace(in.DictionaryID); id != "" {
		dict, err := c.Get(ctx, "/dictionaries/"+url.PathEscape(id), nil)
		return dict, false, err
	}

	name := strings.TrimSpace(in.DictionaryName)
	if name == "" {
		return nil, false, errors.New("no dictionary given: pass dictionary_id, or dictionary_name to load into one by name")
	}

	listed, err := c.Get(ctx, "/dictionaries", nil)
	if err != nil {
		return nil, false, err
	}
	existing, _ := listed.([]any)
	for _, entry := range existing {
		if !strings.EqualFold(aitools.Field[string](entry, "name"), name) {
			continue
		}
		// Listed entries are summaries; the load needs the columns.
		dict, err := c.Get(ctx, "/dictionaries/"+url.PathEscape(aitools.Field[string](entry, "id")), nil)
		return dict, false, err
	}

	if dryRun {
		// A dry run writes nothing, the dictionary included.
		return map[string]any{"name": name, "columns": []any{}, "is_global": in.IsGlobal}, true, nil
	}

	// Created with no columns: the first one the file names becomes the key, which
	// is the rule the dictionary editor follows too.
	dict, err := c.Post(ctx, "/dictionaries", map[string]any{
		"name":        name,
		"description": "Loaded from " + filepath.Base(in.Path),
		"is_global":   in.IsGlobal,
	})
	if err != nil {
		return nil, false, fmt.Errorf("could not create the dictionary %q: %w", name, err)
	}
	return dict, true, nil
}

// uploadMapping says how a file's columns line up with the dictionary's.
type uploadMapping struct {
	// keyField is the file column holding the key, renamed to key as rows are
	// read. Empty when the file already names the key column.
	keyField string
	// key is the dictionary's key column.
	key string
}

// mappedReader applies the key rename to every row a file yields.
type mappedReader struct {
	rowfile.Reader
	mapping uploadMapping
}

func (m mappedReader) Next() (map[string]string, string, error) {
	row, skip, err := m.Reader.Next()
	if err != nil || skip != "" {
		return row, skip, err
	}
	return applyMapping(row, m.mapping), "", nil
}

// applyMapping renames the file's key column to the dictionary's, so a file that
// calls it "indicator" loads into a dictionary keyed on "ioc".
func applyMapping(row map[string]string, m uploadMapping) map[string]string {
	if m.keyField == "" || m.key == "" || m.keyField == m.key {
		return row
	}
	if value, ok := row[m.keyField]; ok {
		delete(row, m.keyField)
		row[m.key] = value
	}
	return row
}

// uploader turns a stream of rows into chunked CSV requests.
type uploader struct {
	client *Client
	dict   any
	key    string
	dryRun bool
	report func(int, string)

	started time.Time
	// loaded is the dictionary as it stands once the load has finished.
	loaded  any
	order   []string
	pending []map[string]string
	bytes   int

	rowsRead    int
	rowsWritten int
	chunks      int
	skipped     int
	skips       []string
	notes       []string
	renames     map[string]string
	columns     map[string]bool
	// folded maps a lower-cased column name to the one the dictionary already
	// has. A file that writes IOC where the dictionary holds ioc would otherwise
	// add a second column and key nothing.
	folded  map[string]string
	widened bool
}

func (u *uploader) run(ctx context.Context, rows rowfile.Reader) error {
	u.renames = map[string]string{}
	u.columns = map[string]bool{}
	u.folded = map[string]string{}
	for _, column := range aitools.Field[[]any](u.dict, "columns") {
		if name := aitools.Field[string](column, "name"); name != "" {
			u.columns[name] = true
			u.folded[strings.ToLower(name)] = name
		}
	}
	if u.key != "" {
		u.folded[strings.ToLower(u.key)] = u.key
	}

	// Whatever landed stays landed, so a load that stops part way through still
	// finishes with the refresh every chunk deferred.
	defer u.finalize(ctx)

	for {
		row, skip, err := rows.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A file that goes wrong at row 900,000 should not throw away the
			// 899,999 rows already read, so send the chunk in hand first.
			_ = u.flush(ctx)
			return fmt.Errorf("%w. %d rows were loaded before this point", err, u.rowsWritten)
		}
		u.rowsRead++
		if skip != "" {
			u.skip(skip)
			continue
		}
		if err := u.add(ctx, row); err != nil {
			return err
		}
	}
	return u.flush(ctx)
}

// add normalises a row's columns and buffers it, sending the chunk once it is
// full.
func (u *uploader) add(ctx context.Context, row map[string]string) error {
	normalized := make(map[string]string, len(row))
	for raw, value := range row {
		name := rowfile.ColumnName(raw)
		if existing, ok := u.folded[strings.ToLower(name)]; ok {
			name = existing
		}
		if name != raw {
			u.renames[raw] = name
		}
		if !u.columns[name] && len(u.columns) >= maxColumns {
			u.widened = true
			continue
		}
		u.columns[name] = true
		u.folded[strings.ToLower(name)] = name
		normalized[name] = value
		u.bytes += len(name) + len(value) + 2
	}

	// A dictionary created for this file has no key yet: the first column it is
	// given becomes one, so the file's own leading column is what should lead.
	if u.key == "" {
		u.key = u.leadColumn(normalized)
	}
	if normalized[u.key] == "" {
		u.skip(fmt.Sprintf("no value for the key column %q", u.key))
		return nil
	}

	u.pending = append(u.pending, normalized)
	if len(u.pending) >= uploadChunkRows || u.bytes >= uploadChunkBytes {
		return u.flush(ctx)
	}
	return nil
}

func (u *uploader) skip(reason string) {
	u.skipped++
	if len(u.skips) < maxSkipsReported {
		u.skips = append(u.skips, reason)
	}
}

// flush sends the buffered rows as one CSV request, retrying a failure the
// server might not repeat. Reload is deferred: it reads the whole dictionary
// back, so a file of many chunks would otherwise pay for it many times.
func (u *uploader) flush(ctx context.Context) error {
	if len(u.pending) == 0 {
		return nil
	}
	count := len(u.pending)
	body, err := u.encode()
	u.chunks++
	u.pending, u.bytes = u.pending[:0], 0
	if err != nil {
		return err
	}
	if u.dryRun {
		u.rowsWritten += count
		return nil
	}

	id := aitools.Field[string](u.dict, "id")
	query := url.Values{"reload": {"false"}}
	var last error
	for attempt := 1; attempt <= uploadAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, uploadChunkTimeout)
		_, last = u.client.Upload(attemptCtx, "/dictionaries/"+url.PathEscape(id)+"/import", query, "text/csv", true, body)
		cancel()
		if last == nil {
			u.rowsWritten += count
			u.report(u.rowsWritten, fmt.Sprintf("loaded %d rows", u.rowsWritten))
			return nil
		}
		if ctx.Err() != nil || !worthRetrying(last) || attempt == uploadAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * uploadBackoff):
		}
	}
	return fmt.Errorf("chunk %d (%d rows) failed after %d attempts: %w. %d rows were loaded before it",
		u.chunks, count, uploadAttempts, last, u.rowsWritten)
}

// encode renders the buffered rows as a CSV with its own header, gzipped. Each
// chunk carries the columns its own rows use, so a file whose later records
// carry a field the first ones did not still loads it.
func (u *uploader) encode() ([]byte, error) {
	present := map[string]bool{}
	for _, row := range u.pending {
		for name := range row {
			present[name] = true
		}
	}
	headers := make([]string, 0, len(present))
	for name := range present {
		if name != u.key {
			headers = append(headers, name)
		}
	}
	sort.Strings(headers)
	// The key leads, so a dictionary created for this file keys on it: the first
	// column a dictionary is given becomes its key.
	headers = append([]string{u.key}, headers...)

	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	writer := csv.NewWriter(gz)
	if err := writer.Write(headers); err != nil {
		return nil, err
	}
	record := make([]string, len(headers))
	for _, row := range u.pending {
		for i, name := range headers {
			record[i] = row[name]
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// finalize refreshes the live dictionary and its row count once, which every
// chunk deferred, then reads back what the dictionary now holds. Best effort on
// both: the rows are already committed, and the objects reload on their own
// within their lifetime.
func (u *uploader) finalize(ctx context.Context) {
	if u.dryRun || u.rowsWritten == 0 {
		return
	}
	path := "/dictionaries/" + url.PathEscape(aitools.Field[string](u.dict, "id"))
	if _, err := u.client.Post(ctx, path+"/reload", nil); err != nil {
		u.notes = append(u.notes,
			"the rows were loaded but the live dictionary refresh failed, so lookups pick them up "+
				"within the dictionary's lifetime rather than at once: "+err.Error())
		return
	}
	// Read back rather than report what was sent: the count is what the analyst
	// will see, and keyed rows mean a file that repeats a key holds fewer.
	if loaded, err := u.client.Get(ctx, path, nil); err == nil {
		u.loaded = loaded
	}
}

func (u *uploader) summary(path, format string, created bool, failure error) any {
	// Where it landed matters as much as what landed: an instance-wide key names
	// its scope per call, and a watchlist in the wrong fractal detects nothing
	// while reading as a successful load.
	target := u.dict
	if u.loaded != nil {
		target = u.loaded
	}
	out := map[string]any{
		"file":          path,
		"format":        format,
		"dictionary":    aitools.Field[string](target, "name"),
		"dictionary_id": aitools.Field[string](target, "id"),
		"fractal_id":    aitools.Field[string](target, "fractal_id"),
		"is_global":     aitools.Field[bool](target, "is_global"),
		"key_column":    u.key,
		"rows_read":     u.rowsRead,
		"rows_written":  u.rowsWritten,
		"chunks":        u.chunks,
		"elapsed":       time.Since(u.started).Round(time.Millisecond).String(),
	}
	if prism := aitools.Field[string](target, "prism_id"); prism != "" {
		out["prism_id"] = prism
	}
	if u.loaded != nil {
		count := int(aitools.Field[float64](u.loaded, "row_count"))
		out["dictionary_row_count"] = count
		if count < u.rowsWritten {
			u.notes = append(u.notes,
				"the dictionary holds fewer rows than were written because rows are keyed: "+
					"the file repeats keys, and each key keeps its last row")
		}
	}
	if u.dryRun {
		out["dry_run"] = true
		u.notes = append(u.notes, "Nothing was written. Call again without dry_run to load the file.")
	}
	if created {
		out["dictionary_created"] = !u.dryRun
		if u.dryRun {
			out["dictionary_would_be_created"] = true
		}
	}
	if u.skipped > 0 {
		out["rows_skipped"] = u.skipped
		out["skipped_examples"] = u.skips
	}
	if len(u.notes) > 0 {
		out["notes"] = u.notes
	}
	if len(u.renames) > 0 {
		out["columns_renamed"] = u.renames
	}
	if u.widened {
		out["columns_dropped"] = fmt.Sprintf(
			"the file names more than %d columns; those past it were not loaded", maxColumns)
	}
	if failure != nil {
		out["failed"] = failure.Error()
	}
	return out
}

// worthRetrying separates a server that is busy or briefly unreachable from a
// request it will refuse however often it is sent.
func worthRetrying(err error) bool {
	var api *aitools.APIError
	if errors.As(err, &api) {
		return api.Retryable()
	}
	// Anything that never reached a handler: a dropped connection, a timeout.
	return true
}

// leadColumn is the column a new dictionary keys on: the file's first, or where
// the format has no order of its own, the lowest-named, so two runs of the same
// file key on the same column.
func (u *uploader) leadColumn(row map[string]string) string {
	for _, raw := range u.order {
		if name := rowfile.ColumnName(raw); row[name] != "" {
			return name
		}
	}
	first := ""
	for name := range row {
		if first == "" || name < first {
			first = name
		}
	}
	return first
}

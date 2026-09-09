package mcpserver

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"bifract/pkg/aitools"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fakeBifract is the dictionary half of the API, enough to load a file into.
type fakeBifract struct {
	mu sync.Mutex

	keyColumn string
	columns   []string
	// fractal is what the key itself carries. Empty is an instance-wide key,
	// which belongs to no fractal and has to be told which one to act in.
	fractal  string
	isGlobal bool
	// instanceWide is a key issued for no scope, which names the one it means per
	// request.
	instanceWide bool
	// dedupe counts rows by key, as the keyed row table does.
	dedupe bool
	// scopes records the X-Bifract-Scope of every request, so a redirected load
	// can be seen to have been redirected.
	scopes []string
	// created records that the dictionary was made by the upload rather than
	// looked up.
	created bool

	rows    []map[string]string
	imports int
	reloads int
	// fail answers the next n import calls with status, for the retry paths.
	failWith  int
	failTimes int
	// failFrom refuses every import from this one on, for the paths where a load
	// stops part way through.
	failFrom int
}

func (f *fakeBifract) start(t *testing.T) *Client {
	t.Helper()
	if f.fractal == "" && !f.instanceWide {
		f.fractal = "fractal-1"
	}
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	return NewClient(Config{URL: server.URL, APIKey: "k", Timeout: 0})
}

func (f *fakeBifract) definition() map[string]any {
	columns := make([]any, 0, len(f.columns))
	for _, name := range f.columns {
		columns = append(columns, map[string]any{"name": name, "type": "string"})
	}
	return map[string]any{
		"id": "dict-1", "name": "iocs", "key_column": f.keyColumn, "columns": columns,
		"fractal_id": f.fractal, "is_global": f.isGlobal, "row_count": float64(f.rowCount()),
	}
}

func (f *fakeBifract) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	write := func(payload any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": payload})
	}

	f.scopes = append(f.scopes, r.Header.Get("X-Bifract-Scope"))

	switch {
	case r.URL.Path == "/api/v1/auth/user":
		// Exempt from the scope header on the real server: it reports what the
		// credential itself carries.
		write(map[string]any{"user": map[string]any{"selected_fractal": f.fractal}})
	case r.URL.Path == "/api/v1/dictionaries" && r.Method == http.MethodGet:
		write([]any{})
	case r.URL.Path == "/api/v1/dictionaries" && r.Method == http.MethodPost:
		f.created = true
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.isGlobal, _ = body["is_global"].(bool)
		write(f.definition())
	case r.URL.Path == "/api/v1/dictionaries/dict-1" && r.Method == http.MethodGet:
		write(f.definition())
	case strings.HasSuffix(r.URL.Path, "/reload"):
		f.reloads++
		write(map[string]any{"reloaded": true})
	case strings.HasSuffix(r.URL.Path, "/import"):
		f.importRows(w, r)
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}
}

func (f *fakeBifract) importRows(w http.ResponseWriter, r *http.Request) {
	f.imports++
	if f.failTimes > 0 || (f.failFrom > 0 && f.imports >= f.failFrom) {
		if f.failTimes > 0 {
			f.failTimes--
		}
		http.Error(w, "refused", f.failWith)
		return
	}
	if got := r.URL.Query().Get("reload"); got != "false" {
		http.Error(w, "a chunked load must defer the reload, got reload="+got, http.StatusBadRequest)
		return
	}

	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "body is not gzip", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	}

	reader := csv.NewReader(body)
	records, err := reader.ReadAll()
	if err != nil || len(records) == 0 {
		http.Error(w, "unreadable CSV", http.StatusBadRequest)
		return
	}
	headers := records[0]
	if f.keyColumn == "" {
		// The server makes the first column of an empty dictionary its key.
		f.keyColumn = headers[0]
	}
	for _, header := range headers {
		if !contains(f.columns, header) {
			f.columns = append(f.columns, header)
		}
	}
	for _, record := range records[1:] {
		row := map[string]string{}
		for i, header := range headers {
			row[header] = record[i]
		}
		f.rows = append(f.rows, row)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true, "data": map[string]int{"imported": len(records) - 1},
	})
}

// rowCount is what the dictionary holds: distinct keys where the table dedupes.
func (f *fakeBifract) rowCount() int {
	if !f.dedupe {
		return len(f.rows)
	}
	keys := map[string]bool{}
	for _, row := range f.rows {
		keys[row[f.keyColumn]] = true
	}
	return len(keys)
}

func contains(all []string, want string) bool {
	for _, v := range all {
		if v == want {
			return true
		}
	}
	return false
}

func write(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// upload runs the tool the way the server would, without a progress token.
func upload(t *testing.T, c *Client, in uploadDictionaryFileArgs) (map[string]any, error) {
	t.Helper()
	out, err := uploadDictionaryFile(t.Context(), c, in, func(int, string) {})
	if out == nil {
		return nil, err
	}
	summary, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("summary is %T, want a map", out)
	}
	return summary, err
}

// A file larger than one chunk still arrives whole, and the live dictionary is
// refreshed once at the end rather than once per chunk: a reload reads the whole
// table back, so paying for it per chunk is quadratic in the size of the file.
func TestALargeFileIsSentInChunksAndReloadedOnce(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc", "source"}}
	client := fake.start(t)

	var file strings.Builder
	file.WriteString("ioc,source\n")
	rows := uploadChunkRows + 1
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&file, "value-%d,feed\n", i)
	}

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "iocs.csv", file.String()), DictionaryID: "dict-1",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if summary["rows_written"] != rows {
		t.Errorf("wrote %v rows, want %d", summary["rows_written"], rows)
	}
	if len(fake.rows) != rows {
		t.Errorf("the server received %d rows, want %d", len(fake.rows), rows)
	}
	if fake.imports != 2 {
		t.Errorf("sent %d chunks, want 2 for %d rows", fake.imports, rows)
	}
	if fake.reloads != 1 {
		t.Errorf("reloaded %d times, want exactly 1", fake.reloads)
	}
	if fake.rows[0]["ioc"] != "value-0" || fake.rows[rows-1]["ioc"] != fmt.Sprintf("value-%d", rows-1) {
		t.Errorf("rows arrived out of order or incomplete: %v ... %v", fake.rows[0], fake.rows[rows-1])
	}
}

// A gzip file is decompressed as it is read, from its magic bytes rather than
// its name: a feed downloaded through a browser often keeps neither extension.
func TestAGzipFileIsReadWithoutAMatchingName(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)

	path := filepath.Join(t.TempDir(), "feed.dat")
	handle, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(handle)
	if _, err := io.WriteString(gz, "ioc\nevil.example\ngood.example\n"); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}

	summary, err := upload(t, client, uploadDictionaryFileArgs{Path: path, DictionaryID: "dict-1"})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if summary["rows_written"] != 2 {
		t.Errorf("wrote %v rows, want 2: %v", summary["rows_written"], summary)
	}
}

func TestFormatsAreRecognisedFromTheFile(t *testing.T) {
	for _, tc := range []struct {
		name    string
		file    string
		content string
		want    []map[string]string
	}{
		{
			name:    "ndjson",
			file:    "feed.ndjson",
			content: "{\"ioc\":\"a\",\"count\":3}\n{\"ioc\":\"b\",\"tags\":[\"x\",\"y\"]}\n",
			want: []map[string]string{
				{"ioc": "a", "count": "3"},
				{"ioc": "b", "tags": `["x","y"]`},
			},
		},
		{
			name:    "json array",
			file:    "feed.json",
			content: `[{"ioc":"a","seen":true},{"ioc":"b","seen":null}]`,
			want: []map[string]string{
				{"ioc": "a", "seen": "true"},
				{"ioc": "b", "seen": ""},
			},
		},
		{
			name:    "plain list with comments",
			file:    "feed.txt",
			content: "# a feed\n\nevil.example\n  spaced.example  \n",
			want: []map[string]string{
				{"ioc": "evil.example"},
				{"ioc": "spaced.example"},
			},
		},
		{
			name:    "tab separated",
			file:    "feed.tsv",
			content: "ioc\tsource\na\tfeed\n",
			want:    []map[string]string{{"ioc": "a", "source": "feed"}},
		},
		{
			name:    "csv with a byte-order mark",
			file:    "feed.csv",
			content: bom + "ioc,source\na,feed\n",
			want:    []map[string]string{{"ioc": "a", "source": "feed"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
			client := fake.start(t)

			if _, err := upload(t, client, uploadDictionaryFileArgs{
				Path: write(t, tc.file, tc.content), DictionaryID: "dict-1",
			}); err != nil {
				t.Fatalf("upload: %v", err)
			}
			if len(fake.rows) != len(tc.want) {
				t.Fatalf("received %d rows, want %d: %v", len(fake.rows), len(tc.want), fake.rows)
			}
			for i, want := range tc.want {
				for column, value := range want {
					if fake.rows[i][column] != value {
						t.Errorf("row %d column %q is %q, want %q", i, column, fake.rows[i][column], value)
					}
				}
			}
		})
	}
}

// The file's key column is often named something else. Its values have to reach
// the dictionary's key column, or every row loads unmatchable.
func TestKeyFieldIsRenamedToTheDictionaryKey(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)

	if _, err := upload(t, client, uploadDictionaryFileArgs{
		Path:         write(t, "feed.csv", "indicator,source\nevil.example,feed\n"),
		DictionaryID: "dict-1",
		KeyField:     "indicator",
	}); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(fake.rows) != 1 || fake.rows[0]["ioc"] != "evil.example" {
		t.Fatalf("the key did not reach the ioc column: %v", fake.rows)
	}
	if _, present := fake.rows[0]["indicator"]; present {
		t.Error("the file's own key column should be renamed, not duplicated")
	}
}

// A header the schema will not take is renamed rather than failing the load, and
// the rename is reported: silently loading into a column nobody can name is
// worse than either.
func TestUnusableColumnNamesAreRenamedAndReported(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path:         write(t, "feed.csv", "ioc,Source IP,_bf_seq\na,10.0.0.1,x\n"),
		DictionaryID: "dict-1",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	renamed, _ := summary["columns_renamed"].(map[string]string)
	if renamed["Source IP"] != "Source_IP" {
		t.Errorf("Source IP became %q, want Source_IP", renamed["Source IP"])
	}
	if renamed["_bf_seq"] != "f_bf_seq" {
		t.Errorf("the reserved prefix was not moved out of the way: %v", renamed)
	}
	if fake.rows[0]["Source_IP"] != "10.0.0.1" {
		t.Errorf("the renamed column lost its value: %v", fake.rows[0])
	}
}

// A row with no key cannot be looked up, so it is counted and reported rather
// than written as a row that can never match.
func TestRowsWithNoKeyAreSkipped(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc", "source"}}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path:         write(t, "feed.csv", "ioc,source\na,feed\n,feed\nb,feed\n"),
		DictionaryID: "dict-1",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if summary["rows_written"] != 2 {
		t.Errorf("wrote %v rows, want 2", summary["rows_written"])
	}
	if summary["rows_skipped"] != 1 {
		t.Errorf("skipped %v rows, want 1", summary["rows_skipped"])
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "feed.csv", "ioc\na\nb\n"), DictionaryID: "dict-1", DryRun: true,
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if fake.imports != 0 || fake.reloads != 0 {
		t.Errorf("a dry run reached the server: %d imports, %d reloads", fake.imports, fake.reloads)
	}
	if summary["rows_written"] != 2 {
		t.Errorf("a dry run should still report what it would write, got %v", summary["rows_written"])
	}
}

// A busy server is retried, since abandoning a load half way through leaves the
// dictionary in a state nobody asked for.
func TestABusyServerIsRetried(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}, failWith: http.StatusServiceUnavailable, failTimes: 2}
	client := fake.start(t)

	if _, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "feed.csv", "ioc\na\n"), DictionaryID: "dict-1",
	}); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if fake.imports != 3 {
		t.Errorf("sent %d requests, want 3: two refusals and the one that landed", fake.imports)
	}
	if len(fake.rows) != 1 {
		t.Errorf("the retry did not deliver the rows: %v", fake.rows)
	}
}

// A refusal the server will repeat is reported at once. Retrying it wastes the
// analyst's time and, on a large file, a great deal of it.
func TestARejectedChunkIsNotRetried(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}, failWith: http.StatusBadRequest, failTimes: 5}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "feed.csv", "ioc\na\n"), DictionaryID: "dict-1",
	})
	if err == nil {
		t.Fatal("a refused chunk must be reported")
	}
	if fake.imports != 1 {
		t.Errorf("sent %d requests, want 1: a 400 is not worth repeating", fake.imports)
	}
	if summary["failed"] == nil {
		t.Error("the summary should carry the failure, so a partial load can be seen")
	}
}

// Naming a dictionary that does not exist creates it, keyed on the file's own
// first column rather than on whichever name happens to sort first.
func TestAnUnknownDictionaryNameIsCreatedKeyedOnTheLeadingColumn(t *testing.T) {
	fake := &fakeBifract{}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path:           write(t, "feed.csv", "sha256,alias\nabc,x\n"),
		DictionaryName: "malware hashes",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if !fake.created {
		t.Fatal("a name that matches nothing should create the dictionary")
	}
	if summary["dictionary_created"] != true {
		t.Error("the summary must say the dictionary was created, so a typo is visible")
	}
	if fake.keyColumn != "sha256" {
		t.Errorf("keyed on %q, want the file's first column sha256", fake.keyColumn)
	}
}

func TestAMissingFileIsReportedBeforeAnythingIsWritten(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)

	_, err := upload(t, client, uploadDictionaryFileArgs{
		Path: filepath.Join(t.TempDir(), "absent.csv"), DictionaryID: "dict-1",
	})
	if err == nil {
		t.Fatal("a path that does not exist must be an error")
	}
	if !strings.Contains(err.Error(), "MCP server runs on") {
		t.Errorf("the error should say whose filesystem was searched: %v", err)
	}
	if fake.imports != 0 {
		t.Error("nothing should have been sent")
	}
}

// A load that fails part way through reports what landed rather than only that
// it failed. The rows already written are real, and an analyst has to know how
// far it got before re-running the file.
func TestAPartialLoadIsReportedThroughTheToolResult(t *testing.T) {
	fake := &fakeBifract{
		keyColumn: "ioc", columns: []string{"ioc"},
		failWith: http.StatusBadRequest, failFrom: 2,
	}
	client := fake.start(t)

	var file strings.Builder
	file.WriteString("ioc\n")
	for i := 0; i < uploadChunkRows+1; i++ {
		fmt.Fprintf(&file, "value-%d\n", i)
	}

	session := connect(t, client)
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: "upload_dictionary_file",
		Arguments: map[string]any{
			"path":          write(t, "iocs.csv", file.String()),
			"dictionary_id": "dict-1",
		},
	})
	if err != nil {
		t.Fatalf("the call itself failed: %v", err)
	}
	if !result.IsError {
		t.Error("a load that could not finish must be reported as an error")
	}

	text := result.Content[0].(*mcp.TextContent).Text
	var summary map[string]any
	if err := json.Unmarshal([]byte(text), &summary); err != nil {
		t.Fatalf("the result is not the summary: %s", text)
	}
	if summary["rows_written"] != float64(uploadChunkRows) {
		t.Errorf("reported %v rows written, want the %d of the chunk that landed",
			summary["rows_written"], uploadChunkRows)
	}
	if summary["failed"] == nil {
		t.Error("the summary must carry the failure")
	}
}

// A dry run must not create the dictionary either: naming one that does not
// exist is exactly the case an analyst wants to check before committing to it.
func TestADryRunDoesNotCreateTheDictionary(t *testing.T) {
	fake := &fakeBifract{}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path:           write(t, "feed.csv", "sha256\nabc\n"),
		DictionaryName: "new list",
		DryRun:         true,
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if fake.created {
		t.Error("a dry run created the dictionary")
	}
	if summary["dictionary_would_be_created"] != true {
		t.Errorf("the summary should say the dictionary would be created: %v", summary)
	}
}

// A file that writes IOC where the dictionary holds ioc loads into the column
// that is already there. Adding a second one that differs only in case would key
// nothing and match nothing.
func TestAColumnIsFoldedOntoTheOneTheDictionaryHas(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc", "source"}}
	client := fake.start(t)

	if _, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "feed.csv", "IOC,Source\nevil.example,feed\n"), DictionaryID: "dict-1",
	}); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(fake.columns) != 2 {
		t.Errorf("the dictionary grew to %v, want the two columns it had", fake.columns)
	}
	if fake.rows[0]["ioc"] != "evil.example" || fake.rows[0]["source"] != "feed" {
		t.Errorf("values did not reach the existing columns: %v", fake.rows[0])
	}
}

// An instance-wide key belongs to no fractal, so a load that names none would go
// wherever the server falls back to. A watchlist in the wrong fractal detects
// nothing while reading as a successful load, so it is refused instead.
func TestAnInstanceWideKeyMustNameItsFractal(t *testing.T) {
	fake := &fakeBifract{instanceWide: true, keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)
	path := write(t, "feed.csv", "ioc\na\n")

	_, err := upload(t, client, uploadDictionaryFileArgs{Path: path, DictionaryID: "dict-1"})
	if err == nil {
		t.Fatal("a load with no fractal to act in must be refused")
	}
	if !strings.Contains(err.Error(), "fractal_id") {
		t.Errorf("the refusal should say how to fix it: %v", err)
	}
	if fake.imports != 0 {
		t.Error("nothing should have been written")
	}

	// Naming one redirects every call in the load.
	fake.fractal = "velociraptor-1"
	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path: path, DictionaryID: "dict-1", FractalID: "velociraptor-1",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	for _, scope := range fake.scopes {
		if scope != "" && scope != "fractal:velociraptor-1" {
			t.Errorf("a call went out scoped to %q", scope)
		}
	}
	if summary["fractal_id"] != "velociraptor-1" {
		t.Errorf("the summary reports fractal %v, want the one named", summary["fractal_id"])
	}
}

// A key issued for one fractal needs no fractal named, and must not have to
// name one.
func TestAScopedKeyLoadsWithoutNamingAFractal(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "feed.csv", "ioc\na\n"), DictionaryID: "dict-1",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if summary["fractal_id"] != "fractal-1" {
		t.Errorf("the summary must say where the rows landed, got %v", summary["fractal_id"])
	}
}

// A watchlist every fractal consults has to be created global: it cannot be made
// global afterwards through this tool.
func TestADictionaryCanBeCreatedGlobal(t *testing.T) {
	fake := &fakeBifract{}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path:           write(t, "feed.csv", "ioc\na\n"),
		DictionaryName: "shared iocs",
		IsGlobal:       true,
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if !fake.isGlobal {
		t.Error("is_global did not reach the create call")
	}
	if summary["is_global"] != true {
		t.Errorf("the summary must report the scope it created: %v", summary)
	}
}

// Rows are keyed, so a file that repeats a key holds fewer rows than it wrote.
// Reporting only what was sent reads as data lost.
func TestARepeatedKeyIsExplainedRatherThanLookingLikeLoss(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}, dedupe: true}
	client := fake.start(t)

	summary, err := upload(t, client, uploadDictionaryFileArgs{
		Path: write(t, "feed.csv", "ioc\na\nb\na\n"), DictionaryID: "dict-1",
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if summary["rows_written"] != 3 {
		t.Errorf("wrote %v rows, want the 3 the file held", summary["rows_written"])
	}
	if summary["dictionary_row_count"] != 2 {
		t.Errorf("the dictionary holds %v rows, want the 2 distinct keys", summary["dictionary_row_count"])
	}
	if !strings.Contains(fmt.Sprint(summary["notes"]), "keyed") {
		t.Errorf("the difference must be explained, got %v", summary["notes"])
	}
}

func TestADirectoryIsRefused(t *testing.T) {
	fake := &fakeBifract{keyColumn: "ioc", columns: []string{"ioc"}}
	if _, err := upload(t, fake.start(t), uploadDictionaryFileArgs{
		Path: t.TempDir(), DictionaryID: "dict-1",
	}); err == nil {
		t.Fatal("a directory is not a file to load")
	}
}

// The tool reads the caller's filesystem, so it may only ever run where the
// caller is: in chat, aitools runs on the server, and the same tool would read
// files off the instance.
func TestTheFileToolIsNotServedToTheInProcessChatClient(t *testing.T) {
	for _, tool := range aitools.All() {
		if tool.Name() == "upload_dictionary_file" {
			t.Fatal("upload_dictionary_file reads local files and must stay out of aitools.All()")
		}
	}
	found := false
	for _, tool := range tools(t) {
		if tool.Name == "upload_dictionary_file" {
			found = true
		}
	}
	if !found {
		t.Error("upload_dictionary_file is not registered on the MCP server")
	}
}

package ingestcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
)

const exportedRow = `{"timestamp":"2026-10-29T14:04:25.816Z","log_id":"abc","fractal_id":"dc863dc8","fields":{"hostname":"WS02","event_id":"1"}}`

func zstdBytes(t *testing.T, text string) []byte {
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

// A .zst file of exported logs is read like NDJSON, and each line goes back in as
// the log it was: its fields and timestamp, without the exporting install's ids.
func TestReadZstdExport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lab.jsonl.zst")
	if err := os.WriteFile(path, zstdBytes(t, exportedRow+"\n"+exportedRow+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if f, err := DetectFormat(path); err != nil || f != FormatZstd {
		t.Fatalf("DetectFormat = %v, %v", f, err)
	}
	if n, err := CountLogs(path); err != nil || n != 2 {
		t.Fatalf("CountLogs = %d, %v", n, err)
	}
	batches := drain(t, func(ch chan<- Batch) error { return ReadFile(path, 5000, 0, ch, &Stats{}) })
	var logs []map[string]interface{}
	for _, b := range batches {
		logs = append(logs, b.Logs...)
	}
	if len(logs) != 2 {
		t.Fatalf("%d logs", len(logs))
	}
	l := logs[0]
	if l["hostname"] != "WS02" || l["timestamp"] != "2026-10-29T14:04:25.816Z" {
		t.Errorf("log = %v", l)
	}
	for _, k := range []string{"log_id", "fractal_id", "fields", "bifract_ingest_path"} {
		if _, has := l[k]; has {
			t.Errorf("log has %s: %v", k, l)
		}
	}
}

// An exported line in a plain NDJSON file is restored the same way; any other
// line is sent as is, tagged with its path, and numbers keep every digit.
func TestReadNDJSONUnwrapsExports(t *testing.T) {
	path := writeTemp(t, "export.jsonl", exportedRow+"\n{\"a\":18446744073709551615}\n")
	batches := drain(t, func(ch chan<- Batch) error { return readNDJSON(path, 5000, 0, ch, &Stats{}) })
	logs := batches[0].Logs
	if logs[0]["hostname"] != "WS02" || logs[0]["bifract_ingest_path"] != nil {
		t.Errorf("exported line = %v", logs[0])
	}
	if logs[1]["bifract_ingest_path"] != path {
		t.Errorf("plain line = %v", logs[1])
	}
	if out, _ := json.Marshal(logs[1]["a"]); string(out) != "18446744073709551615" {
		t.Errorf("a 64-bit number was sent as %s", out)
	}
}

func TestIngestibleNames(t *testing.T) {
	for name, want := range map[string]bool{
		"a.json": true, "a.JSONL": true, "lab.jsonl.zst": true, "lab.ndjson.zst": true,
		"backup.tar.zst": false, "a.zst": false, "notes.txt": false,
	} {
		if ingestible(filepath.Join("dir", name)) != want {
			t.Errorf("ingestible(%s) = %v", name, !want)
		}
	}
}

// Only an index the user chose may reach addresses inside their network.
func TestExchangeIndexURL(t *testing.T) {
	t.Setenv("BIFRACT_EXCHANGE_URL", "")
	if url, custom, err := exchangeIndexURL(""); err != nil || custom || url == "" {
		t.Errorf("default = %q, %v, %v", url, custom, err)
	}
	if _, custom, _ := exchangeIndexURL("http://mirror.lan/index.yaml"); !custom {
		t.Error("--exchange is not custom")
	}
	t.Setenv("BIFRACT_EXCHANGE_URL", "http://mirror.lan/index.yaml")
	if _, custom, _ := exchangeIndexURL(""); !custom {
		t.Error("BIFRACT_EXCHANGE_URL is not custom")
	}
	t.Setenv("BIFRACT_EXCHANGE_URL", "off")
	if _, _, err := exchangeIndexURL(""); err == nil {
		t.Error("off was accepted")
	}
}

type exchangeServer struct {
	srv       *httptest.Server
	file      []byte
	downloads atomic.Int32
}

// newExchangeServer serves an index listing one dataset, and that dataset's file.
func newExchangeServer(t *testing.T, file []byte, logs int) *exchangeServer {
	t.Helper()
	es := &exchangeServer{file: file}
	sum := sha256.Sum256(file)
	es.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/index.yaml":
			fmt.Fprintf(w, `items:
  - id: lab-001
    kind: dataset
    name: Lab
    dataset:
      url: %s/lab.jsonl.zst
      sha256: %s
      bytes: %d
      logs: %d
      start: 2026-10-27T00:00:00Z
      end: 2026-10-30T00:00:00Z
  - {id: soc, kind: policy, name: SOC, path: soc.yaml}
`, es.srv.URL, hex.EncodeToString(sum[:]), len(file), logs)
		case "/lab.jsonl.zst":
			es.downloads.Add(1)
			w.Write(es.file)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(es.srv.Close)
	return es
}

func TestFetchDatasetCaches(t *testing.T) {
	es := newExchangeServer(t, zstdBytes(t, exportedRow+"\n"), 1)
	cache := t.TempDir()
	fetch := func() string {
		t.Helper()
		path, n, err := fetchDataset(context.Background(), es.srv.Client(), es.srv.URL+"/index.yaml", "lab-001", cache, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("%d logs, want the index's 1", n)
		}
		return path
	}
	path := fetch()
	if got, _ := os.ReadFile(path); !bytes.Equal(got, es.file) {
		t.Fatal("the cached file differs from the download")
	}
	if fetch() != path || es.downloads.Load() != 1 {
		t.Errorf("a verified cached file was downloaded again (%d downloads)", es.downloads.Load())
	}
	// A damaged cached copy is replaced rather than ingested.
	os.WriteFile(path, []byte("damaged"), 0o600)
	fetch()
	if es.downloads.Load() != 2 {
		t.Errorf("a damaged cached file was used (%d downloads)", es.downloads.Load())
	}
	if left, _ := filepath.Glob(filepath.Join(cache, "*")); len(left) != 1 {
		t.Errorf("cache holds %v", left)
	}
}

func TestFetchDatasetRefuses(t *testing.T) {
	es := newExchangeServer(t, zstdBytes(t, exportedRow+"\n"), 2)
	cache := t.TempDir()
	for id, want := range map[string]string{
		"nope":    "no dataset",
		"soc":     "not a dataset",
		"lab-001": "holds 1 logs",
	} {
		_, _, err := fetchDataset(context.Background(), es.srv.Client(), es.srv.URL+"/index.yaml", id, cache, io.Discard)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", id, err, want)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(cache, "*")); len(left) != 0 {
		t.Errorf("a file that failed its checks was cached: %v", left)
	}
}

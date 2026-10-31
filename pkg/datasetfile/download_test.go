package datasetfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bifract/pkg/egress"
	"bifract/pkg/exchange"
)

func datasetFor(url string, body []byte) *exchange.Dataset {
	sum := sha256.Sum256(body)
	return &exchange.Dataset{URL: url, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(body)), Logs: 1}
}

func serve(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/d.jsonl.zst" {
			http.NotFound(w, r)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

var testClient = egress.NewClient(egress.Options{AllowPrivate: true})

func TestDownloadVerifies(t *testing.T) {
	body := []byte("dataset bytes")
	srv := serve(t, body)
	var seen int64
	var buf bytes.Buffer
	if err := Download(context.Background(), testClient, datasetFor(srv.URL+"/d.jsonl.zst", body), &buf, func(n int64) { seen = n }); err != nil {
		t.Fatal(err)
	}
	if buf.String() != string(body) || seen != int64(len(body)) {
		t.Errorf("got %q, progress %d", buf.String(), seen)
	}
}

// A file that is not exactly what the Exchange lists is refused: a different hash,
// a truncated body, or one longer than listed (read no further than one byte past).
func TestDownloadRejectsMismatch(t *testing.T) {
	body := []byte("dataset bytes")
	srv := serve(t, body)
	url := srv.URL + "/d.jsonl.zst"
	cases := map[string]*exchange.Dataset{}
	wrongHash := datasetFor(url, body)
	wrongHash.SHA256 = strings.Repeat("0", 64)
	cases["hash"] = wrongHash
	longer := datasetFor(url, body)
	longer.Bytes = int64(len(body)) + 5
	cases["truncated"] = longer
	shorter := datasetFor(url, body[:4])
	cases["too long"] = shorter
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Download(context.Background(), testClient, d, &buf, nil)
			if !errors.Is(err, ErrChecksum) {
				t.Fatalf("err = %v, want ErrChecksum", err)
			}
			if int64(buf.Len()) > d.Bytes+1 {
				t.Errorf("read %d bytes for a %d-byte file", buf.Len(), d.Bytes)
			}
		})
	}
}

func TestDownloadMissing(t *testing.T) {
	srv := serve(t, nil)
	err := Download(context.Background(), testClient, datasetFor(srv.URL+"/gone", []byte("x")), &bytes.Buffer{}, nil)
	if !errors.Is(err, ErrDownload) || !strings.Contains(err.Error(), "404") {
		t.Errorf("err = %v", err)
	}
}

// The default client refuses addresses inside the network, so an index cannot
// point a server's download at its own infrastructure.
func TestDownloadBlocksPrivateAddresses(t *testing.T) {
	srv := serve(t, []byte("x"))
	client := egress.NewClient(egress.Options{})
	err := Download(context.Background(), client, datasetFor(srv.URL+"/d.jsonl.zst", []byte("x")), &bytes.Buffer{}, nil)
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Errorf("err = %v", err)
	}
}

// A server that stops sending fails the download instead of holding it forever.
func TestDownloadStalls(t *testing.T) {
	old := stallTimeout
	stallTimeout = 100 * time.Millisecond
	t.Cleanup(func() { stallTimeout = old })
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("some"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	err := Download(context.Background(), testClient, datasetFor(srv.URL, []byte("some more")), &bytes.Buffer{}, nil)
	if err == nil || !strings.Contains(err.Error(), "no data received") {
		t.Errorf("err = %v", err)
	}
}

func TestDownloadFileRemovesFailures(t *testing.T) {
	body := []byte("dataset bytes")
	srv := serve(t, body)
	dir := t.TempDir()
	path, err := DownloadFile(context.Background(), testClient, datasetFor(srv.URL+"/d.jsonl.zst", body), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(body) {
		t.Errorf("file holds %q", got)
	}
	os.Remove(path)
	bad := datasetFor(srv.URL+"/d.jsonl.zst", body)
	bad.SHA256 = strings.Repeat("0", 64)
	if _, err := DownloadFile(context.Background(), testClient, bad, dir, nil); err == nil {
		t.Fatal("expected an error")
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*")); len(left) != 0 {
		t.Errorf("left behind %v", left)
	}
}

// Only old downloads are removed: a fresh one may belong to a running import.
func TestRemoveStale(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, age time.Duration) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		at := time.Now().Add(-age)
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
		return p
	}
	old := write("bifract-dataset-1.jsonl.zst", 2*time.Hour)
	fresh := write("bifract-dataset-2.jsonl.zst", time.Minute)
	other := write("keep.jsonl.zst", 2*time.Hour)
	RemoveStale(dir, time.Hour)
	for p, want := range map[string]bool{old: false, fresh: true, other: true} {
		if _, err := os.Stat(p); (err == nil) != want {
			t.Errorf("%s exists = %v, want %v", filepath.Base(p), err == nil, want)
		}
	}
}

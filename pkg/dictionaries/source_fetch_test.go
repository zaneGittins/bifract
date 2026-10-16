package dictionaries

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// A source is fetched through the egress guard, so a URL that reaches the
// server's own network is refused unless an administrator allowed private
// space. The refusal is shown to whoever can see the list, so it says why
// without naming the address the host resolved to.
func TestDownloadSourceRespectsTheEgressGuard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ioc\n1.2.3.4\n"))
	}))
	defer srv.Close()
	ctx := context.Background()
	host, _ := url.Parse(srv.URL)

	_, err := downloadSource(ctx, srv.URL, false)
	if err == nil {
		t.Fatal("a loopback source was fetched with private space disallowed")
	}
	if !IsInvalidInput(err) || !strings.Contains(err.Error(), "not allowed") {
		t.Errorf("err = %v, want a caller-facing refusal", err)
	}
	if strings.Contains(err.Error(), host.Host) || strings.Contains(err.Error(), "127.0.0.1") {
		t.Errorf("the refusal %q names where the request went", err)
	}

	body, err := downloadSource(ctx, srv.URL, true)
	if err != nil || string(body) != "ioc\n1.2.3.4\n" {
		t.Fatalf("with private space allowed: body %q, err %v", body, err)
	}
}

func TestDownloadSourceReportsTheStatus(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	_, err := downloadSource(context.Background(), srv.URL, true)
	if err == nil || !IsInvalidInput(err) || !strings.Contains(err.Error(), "404") {
		t.Errorf("err = %v, want an invalid-input error carrying the status", err)
	}
}

// The body is held in memory so an unchanged file can be recognised before
// anything is written, so one past the cap is refused rather than truncated:
// a truncated file would load as a shorter list and delete the rest.
func TestDownloadSourceRefusesAnOversizedBody(t *testing.T) {
	chunk := bytes.Repeat([]byte("x"), 1<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for sent := 0; sent <= maxSourceBytes; sent += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	_, err := downloadSource(context.Background(), srv.URL, true)
	if err == nil || !IsInvalidInput(err) || !strings.Contains(err.Error(), "larger than") {
		t.Errorf("err = %v, want the size refusal", err)
	}
}

// A load the caller stopped waiting for goes on: it runs on a context of its
// own, bounded by the sync timeout rather than by the request.
func TestRunDetachedOutlivesTheCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)

	result := make(chan error, 1)
	go func() {
		result <- runDetached(ctx, func(loadCtx context.Context) error {
			close(started)
			<-release
			if _, ok := loadCtx.Deadline(); !ok {
				finished <- errors.New("the load has no deadline of its own")
				return nil
			}
			finished <- loadCtx.Err()
			return nil
		})
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, errSourceLoading) {
		t.Fatalf("caller gone: err = %v, want errSourceLoading", err)
	}
	close(release)
	select {
	case err := <-finished:
		if err != nil {
			t.Errorf("the load was cut short with the request: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the load never finished")
	}
}

func TestRunDetachedReturnsTheLoadsOutcome(t *testing.T) {
	want := badInput("the source holds no usable rows")
	if err := runDetached(context.Background(), func(context.Context) error { return want }); err != want {
		t.Errorf("err = %v, want the load's own error", err)
	}
	if err := runDetached(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

// A failed refresh is shown to everyone who can see the list: a problem with
// the source is described, anything else stays in the server log.
func TestSourceErrorText(t *testing.T) {
	if got := sourceErrorText(badInput("the file has no column %q", "Path")); got != `the file has no column "Path"` {
		t.Errorf("caller-fixable: %q", got)
	}
	got := sourceErrorText(errors.New("failed to batch insert rows: code 241 on clickhouse-0.internal"))
	if strings.Contains(got, "clickhouse-0") || !strings.Contains(got, "server log") {
		t.Errorf("internal failure shown as %q", got)
	}
}

// The lock must be the same number on every replica for the same list, and a
// different number for a different list, or two replicas refresh one list at
// once and two lists block each other.
func TestSourceLockIDIsStablePerList(t *testing.T) {
	a := "8c1168bb-23b0-44e5-9486-58ed89ecc9b4"
	b := "8c1168bb-23b0-44e5-9486-58ed89ecc9b5"
	if sourceLockID(a) != sourceLockID(a) {
		t.Error("the lock id is not deterministic")
	}
	if sourceLockID(a) == sourceLockID(b) {
		t.Error("two lists share a lock id")
	}
	// Pinned: a change here would let a replica on the old release and one on
	// the new refresh the same list together during a rollout.
	if got := sourceLockID(a); got != 2937650746597958839 {
		t.Errorf("sourceLockID(%s) = %d; the derivation changed", a, got)
	}
}

// Large lists are usually shipped gzipped, often under a name that does not say
// so; the body is recognised by its content.
func TestSourceReadsAGzippedBody(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte("ioc,source\n1.2.3.4,feed\n5.6.7.8,feed\n"))
	gz.Close()

	src := &Source{URL: "https://example.com/list", KeyField: "ioc"}
	if err := src.normalize(KindValue); err != nil {
		t.Fatal(err)
	}
	f, err := parseSource(buf.Bytes(), src, KindValue)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := f.each(func(r DictionaryRow) error { keys = append(keys, r.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	if f.format != "csv" || strings.Join(keys, ",") != "1.2.3.4,5.6.7.8" {
		t.Errorf("format %q keys %v", f.format, keys)
	}

	// Something that claims to be gzip and is not is refused, not read as text.
	corrupt := append([]byte{0x1f, 0x8b}, []byte("not really gzip")...)
	if _, err := parseSource(corrupt, src, KindValue); err == nil || !IsInvalidInput(err) {
		t.Errorf("corrupt gzip: err = %v, want an invalid-input refusal", err)
	}
}

// A file that breaks part way through fails the load as the caller's problem,
// before anything is written.
func TestSourceMalformedJSONIsRefused(t *testing.T) {
	src := &Source{URL: "https://example.com/a.json", KeyField: "ip"}
	_ = src.normalize(KindValue)
	if _, err := parseSource([]byte(`[{"ip":"1.2.3.4"}, {"ip": `), src, KindValue); err == nil || !IsInvalidInput(err) {
		t.Errorf("err = %v, want an invalid-input refusal", err)
	}
}

// File columns are cleaned into identifiers, and two that clean to the same
// name keep the first rather than declaring one column twice.
func TestSourceColumnsThatCleanToOneNameKeepTheFirst(t *testing.T) {
	src := &Source{URL: "https://example.com/a.csv", KeyField: "k"}
	_ = src.normalize(KindValue)
	f, err := parseSource([]byte("k,first seen,first_seen\na,mon,tue\n"), src, KindValue)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.columns) != 2 || f.columns[1].Name != "first_seen" {
		t.Fatalf("columns = %+v, want k and one first_seen", f.columns)
	}
	var rows []DictionaryRow
	_ = f.each(func(r DictionaryRow) error { rows = append(rows, r); return nil })
	if rows[0].Fields["first_seen"] != "mon" {
		t.Errorf("first_seen = %q, want the value of the first column that claimed the name", rows[0].Fields["first_seen"])
	}
}

// A JSON source with no listed columns keeps every field it finds, but one whose
// records carry more fields than a list may hold is refused while reading.
func TestSourceJSONColumnCap(t *testing.T) {
	var b strings.Builder
	b.WriteString(`[{"k":"a"`)
	for i := 0; i < maxImportColumns+1; i++ {
		fmt.Fprintf(&b, `,"c%d":"v"`, i)
	}
	b.WriteString(`}]`)
	src := &Source{URL: "https://example.com/a.json", KeyField: "k"}
	_ = src.normalize(KindValue)
	if _, err := parseSource([]byte(b.String()), src, KindValue); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Errorf("err = %v, want the column cap", err)
	}
}

// A list with no source is never due, whatever its status says.
func TestSourceDueNeedsASource(t *testing.T) {
	if sourceDue(&Dictionary{SourceStatus: &SourceStatus{}}, time.Now()) {
		t.Error("a list with no source was due a refresh")
	}
}

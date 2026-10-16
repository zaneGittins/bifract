package ingest

import (
	"bytes"
	"compress/gzip"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/ingesttokens"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// Tokens the fixture's fake Postgres knows. goodToken resolves to tokenRow;
// brokenToken makes the lookup fail as if Postgres were unreachable; any other
// token is unknown.
const (
	goodToken   = "bifract_ingest_good"
	brokenToken = "bifract_ingest_db_down"
)

// tokenRow is the ingest_tokens/normalizers join row ValidateToken scans.
type tokenRow struct {
	id, fractalID, parser string
	tsFields              string // JSON
	normalizer            string // JSON transforms; empty = no normalizer
	normName              string
	normVersion           int64
	normTsFields          string // JSON
}

func (r tokenRow) values() []driver.Value {
	v := []driver.Value{r.id, r.fractalID, r.parser, []byte(r.tsFields), nil, nil, nil, nil, nil, nil, nil}
	if r.normalizer != "" {
		v[4], v[5], v[6] = "norm-1", r.normName, r.normVersion
		v[7], v[8], v[9], v[10] = []byte(r.normalizer), []byte(`[]`), []byte(`[]`), []byte(r.normTsFields)
	}
	return v
}

type fixture struct {
	h  *IngestHandler
	q  *IngestQueue
	pg *fakedb.PG
}

// newFixture wires a handler to a fake Postgres and a bare queue whose channel the
// test drains directly, so assertions see exactly what reached the queue.
func newFixture(t *testing.T, row tokenRow) *fixture {
	t.Helper()
	if row.id == "" {
		row.id = "tok-1"
	}
	if row.fractalID == "" {
		row.fractalID = "frac-1"
	}
	if row.tsFields == "" {
		row.tsFields = "[]"
	}
	pgc, pg := storagetest.Postgres()
	pg.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		if !strings.Contains(q, "FROM ingest_tokens t") {
			return fakedb.PGResult{}, false
		}
		switch args[0] {
		case ingesttokens.HashToken(goodToken):
			return fakedb.Row(row.values()...), true
		case ingesttokens.HashToken(brokenToken):
			return fakedb.PGResult{Err: driver.ErrBadConn}, true
		}
		return fakedb.PGResult{}, true // no rows
	})
	q := newBareQueue(16)
	return &fixture{
		h:  NewIngestHandler(q, 1<<20, ingesttokens.NewTokenCache(time.Minute), ingesttokens.NewStorage(pgc)),
		q:  q,
		pg: pg,
	}
}

// newBareQueue is an IngestQueue with no workers or monitors: Enqueue fills ch and
// the test reads it back.
func newBareQueue(slots int) *IngestQueue {
	return &IngestQueue{
		ch:          make(chan []storage.LogEntry, slots),
		bufSize:     slots,
		batchRows:   defaultBatchRows,
		batchBytes:  defaultBatchBytes,
		bufferBytes: 1 << 30,
		maxKeys:     defaultMaxKeys,
	}
}

// queued drains every batch Enqueue accepted.
func (q *IngestQueue) queued() []storage.LogEntry {
	var out []storage.LogEntry
	for {
		select {
		case b := <-q.ch:
			out = append(out, b...)
		default:
			return out
		}
	}
}

// readTracker is a request body that records whether the handler read it, to
// prove a rejection happened before the body was consumed.
type readTracker struct {
	r    io.Reader
	read atomic.Bool
}

func (b *readTracker) Read(p []byte) (int, error) {
	b.read.Store(true)
	return b.r.Read(p)
}
func (b *readTracker) Close() error { return nil }

type reqOpt func(*http.Request)

func withHeader(k, v string) reqOpt { return func(r *http.Request) { r.Header.Set(k, v) } }
func withAuth(v string) reqOpt      { return withHeader("Authorization", v) }
func noAuth() reqOpt                { return func(r *http.Request) { r.Header.Del("Authorization") } }

// do sends body to handler with the good token unless overridden, and returns the
// recorder and whether the body was read.
func do(handler http.HandlerFunc, method, body string, opts ...reqOpt) (*httptest.ResponseRecorder, bool) {
	return doBytes(handler, method, []byte(body), opts...)
}

func doBytes(handler http.HandlerFunc, method string, body []byte, opts ...reqOpt) (*httptest.ResponseRecorder, bool) {
	tr := &readTracker{r: bytes.NewReader(body)}
	req := httptest.NewRequest(method, "/ingest", nil)
	req.Body = tr
	req.Header.Set("Authorization", "Bearer "+goodToken)
	for _, o := range opts {
		o(req)
	}
	w := httptest.NewRecorder()
	handler(w, req)
	return w, tr.read.Load()
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func decodeIngestResponse(t *testing.T, w *httptest.ResponseRecorder) IngestResponse {
	t.Helper()
	var resp IngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON (%d): %q", w.Code, w.Body.String())
	}
	return resp
}

// waitForUsage waits for the asynchronous token usage update and returns its args.
func waitForUsage(t *testing.T, pg *fakedb.PG) []driver.Value {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls := pg.Matching("UPDATE ingest_tokens"); len(calls) > 0 {
			return calls[0].Args
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("token usage stats were never updated")
	return nil
}

// noUsage asserts no usage update was recorded (rejected requests must not count).
func noUsage(t *testing.T, pg *fakedb.PG) {
	t.Helper()
	time.Sleep(20 * time.Millisecond)
	if calls := pg.Matching("UPDATE ingest_tokens"); len(calls) > 0 {
		t.Fatalf("usage stats updated for a rejected request: %v", calls[0].Args)
	}
}

// quotaFor returns a QuotaManager with one fractal's quota state preloaded.
func quotaFor(fractalID string, quota, used int64, action string) *QuotaManager {
	qm := &QuotaManager{state: map[string]*fractalQuotaState{}}
	qm.state[fractalID] = &fractalQuotaState{quotaBytes: quota, action: action, baseBytes: used}
	return qm
}

package ingest

import (
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"bifract/internal/fakedb"
	"bifract/pkg/fractals"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage/storagetest"
)

func TestInternalOnlyMiddleware(t *testing.T) {
	tests := []struct {
		remote  string
		headers map[string]string
		allowed bool
	}{
		{"10.1.2.3:5000", nil, true},
		{"172.16.0.1:5000", nil, true},
		{"172.31.255.255:5000", nil, true},
		{"192.168.1.1:5000", nil, true},
		{"127.0.0.1:5000", nil, true},
		{"[::1]:5000", nil, true},
		{"[fd12::1]:5000", nil, true},
		{"[fe80::1%eth0]:5000", nil, true},
		{"10.0.0.1", nil, true}, // bare IP without a port
		{"172.32.0.1:5000", nil, false},
		{"8.8.8.8:5000", nil, false},
		{"[2001:db8::1]:5000", nil, false},
		{"not-an-ip:5000", nil, false},
		{"", nil, false},
		// A proxied request arrives from the proxy's private address but came
		// from outside, so any proxy header refuses it.
		{"10.0.0.2:5000", map[string]string{"X-Forwarded-For": "10.0.0.9"}, false},
		{"10.0.0.2:5000", map[string]string{"X-Real-IP": "127.0.0.1"}, false},
	}
	for _, tc := range tests {
		called := false
		h := InternalOnlyMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = tc.remote
		for k, v := range tc.headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if called != tc.allowed {
			t.Errorf("%s %v: allowed=%v, want %v", tc.remote, tc.headers, called, tc.allowed)
		}
		if !tc.allowed && w.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", tc.remote, w.Code)
		}
	}
}

type internalFixture struct {
	h      *InternalIngestHandler
	q      *IngestQueue
	router http.Handler
}

func newInternalFixture(t *testing.T, defaultNormalizer bool) *internalFixture {
	t.Helper()
	pgc, pg := storagetest.Postgres()
	pg.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		switch {
		case strings.Contains(q, "FROM fractals") && strings.Contains(q, "WHERE name = $1"):
			if args[0] != "caddy" {
				return fakedb.PGResult{}, true
			}
			now := time.Now()
			return fakedb.Row("frac-caddy", "caddy", "", false, true, "", now, now,
				nil, nil, nil, "reject", int64(0), int64(0), nil, nil), true
		case strings.Contains(q, "FROM normalizers WHERE is_default"):
			if !defaultNormalizer {
				return fakedb.PGResult{}, true
			}
			now := time.Now()
			return fakedb.Row("n1", "web", "", []byte(`["snake_case"]`), []byte(`[]`), []byte(`[]`), []byte(`[]`),
				true, "", now, now, int64(3)), true
		}
		return fakedb.PGResult{}, false
	})
	q := newBareQueue(8)
	h := NewInternalIngestHandler(q, 1<<16, fractals.NewManager(pgc, nil), normalizers.NewManager(pgc))
	r := chi.NewRouter()
	r.Post("/internal/{fractal}", h.HandleInternalIngest)
	return &internalFixture{h: h, q: q, router: r}
}

func (f *internalFixture) post(path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return w
}

func TestHandleInternalIngest(t *testing.T) {
	f := newInternalFixture(t, true)
	w := f.post("/internal/caddy", "{\"RemoteIP\":\"1.2.3.4\"}\n{\"RemoteIP\":\"5.6.7.8\"}")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	logs := f.q.queued()
	if len(logs) != 2 || logs[0].FractalID != "frac-caddy" {
		t.Fatalf("queued %+v", logs)
	}
	// The default normalizer is applied and stamped.
	if logs[0].Fields["remote_ip"] != "1.2.3.4" || logs[0].Normalizer != "web@3" {
		t.Errorf("default normalizer not applied: %v %q", logs[0].Fields, logs[0].Normalizer)
	}

	// No default normalizer: ingested as-is.
	f = newInternalFixture(t, false)
	if w := f.post("/internal/caddy", `{"RemoteIP":"1.2.3.4"}`); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	if logs := f.q.queued(); logs[0].Fields["RemoteIP"] != "1.2.3.4" || logs[0].Normalizer != "" {
		t.Errorf("fields = %v stamp %q", logs[0].Fields, logs[0].Normalizer)
	}
}

func TestHandleInternalIngestErrors(t *testing.T) {
	tests := []struct {
		name       string
		path, body string
		setup      func(*internalFixture)
		wantStatus int
		wantErr    string
	}{
		{name: "unknown fractal", path: "/internal/nope", body: `{"a":1}`, wantStatus: 404, wantErr: `"nope" not found`},
		{name: "too large", path: "/internal/caddy", body: strings.Repeat("x", 1<<17), wantStatus: 413, wantErr: "byte limit"},
		{name: "unparseable", path: "/internal/caddy", body: "nope", wantStatus: 400, wantErr: "Failed to parse"},
		{name: "empty array", path: "/internal/caddy", body: "[]", wantStatus: 400, wantErr: "No valid logs"},
		{name: "queue full", path: "/internal/caddy", body: `{"a":1}`, setup: func(f *internalFixture) {
			f.q.accumBytes.Store(f.q.bufferBytes + 1)
		}, wantStatus: 429, wantErr: "queue full"},
		{name: "under pressure", path: "/internal/caddy", body: `{"a":1}`, setup: func(f *internalFixture) {
			f.q.diskState.active.Store(1)
		}, wantStatus: 429, wantErr: "under pressure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newInternalFixture(t, false)
			if tc.setup != nil {
				tc.setup(f)
			}
			w := f.post(tc.path, tc.body)
			if w.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.wantStatus, w.Body)
			}
			var resp IngestResponse
			json.Unmarshal(w.Body.Bytes(), &resp)
			if !strings.Contains(resp.Error, tc.wantErr) {
				t.Errorf("error = %q, want %q", resp.Error, tc.wantErr)
			}
			if tc.wantStatus == 429 && w.Header().Get("Retry-After") != "2" {
				t.Errorf("Retry-After = %q", w.Header().Get("Retry-After"))
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued", n)
			}
		})
	}

	// Called outside a route that binds {fractal}.
	f := newInternalFixture(t, false)
	w := httptest.NewRecorder()
	f.h.HandleInternalIngest(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing fractal name: status %d, want 400", w.Code)
	}
}

// TestRateLimitMiddlewareForwardedFor: the limiter keys on the client behind a
// trusted proxy, but a client connecting directly cannot pick a fresh key per
// request by sending its own X-Forwarded-For.
func TestRateLimitMiddlewareForwardedFor(t *testing.T) {
	h := RateLimitMiddleware(NewRateLimiter(0.001, 1))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	send := func(peer, xff string) int {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = peer + ":1234"
		r.Header.Set("X-Forwarded-For", xff)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if code := send("203.0.113.50", "198.51.100.1"); code != http.StatusOK {
		t.Fatalf("first request: %d", code)
	}
	if code := send("203.0.113.50", "198.51.100.2"); code != http.StatusTooManyRequests {
		t.Errorf("spoofed X-Forwarded-For from an untrusted peer got a fresh bucket: %d", code)
	}

	// Behind the bundled proxy each client has its own bucket.
	if code := send("172.18.0.5", "198.51.100.3"); code != http.StatusOK {
		t.Fatalf("first client behind proxy: %d", code)
	}
	if code := send("172.18.0.5", "198.51.100.4"); code != http.StatusOK {
		t.Errorf("second client behind proxy limited by the first: %d", code)
	}
}

// TestRateLimitMiddleware: limits apply per client IP, and a limited request is a
// 429 with Retry-After like every other ingest rejection.
func TestRateLimitMiddleware(t *testing.T) {
	served := 0
	h := RateLimitMiddleware(NewRateLimiter(0.001, 1))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served++ }))
	send := func(ip string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = ip + ":1234"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := send("1.1.1.1"); w.Code != http.StatusOK {
		t.Fatalf("first request: %d", w.Code)
	}
	w := send("1.1.1.1")
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("second request: %d Retry-After %q", w.Code, w.Header().Get("Retry-After"))
	}
	var resp IngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Success || resp.Error == "" {
		t.Fatalf("429 body = %s", w.Body)
	}
	if w := send("2.2.2.2"); w.Code != http.StatusOK {
		t.Fatalf("another client was limited by the first: %d", w.Code)
	}
	if served != 2 {
		t.Fatalf("served %d, want 2", served)
	}
}

package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/ingesttokens"
)

func TestHandleIngestAcceptsAndRoutesToTokenFractal(t *testing.T) {
	f := newFixture(t, tokenRow{fractalID: "frac-token"})
	// A log naming another fractal must still land in the token's fractal: the
	// token is the only routing authority.
	body := `[{"msg":"one","fractal_id":"frac-other","timestamp":"2026-07-01T00:00:00Z"},{"msg":"two"}]`

	w, _ := do(f.h.HandleIngest, http.MethodPost, body)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	resp := decodeIngestResponse(t, w)
	if !resp.Success || resp.Count != 2 {
		t.Fatalf("response = %+v", resp)
	}

	logs := f.q.queued()
	if len(logs) != 2 {
		t.Fatalf("queued %d logs, want 2", len(logs))
	}
	for _, l := range logs {
		if l.FractalID != "frac-token" {
			t.Errorf("log routed to %q, want the token's fractal", l.FractalID)
		}
		if l.IngestTimestamp.IsZero() || l.LogID == "" {
			t.Errorf("log not ready to insert: %+v", l)
		}
	}
	if logs[0].Fields["fractal_id"] != "frac-other" {
		t.Errorf("the log's own fractal_id field should be kept as data, got %v", logs[0].Fields)
	}

	args := waitForUsage(t, f.pg)
	if args[0] != "tok-1" || args[1] != int64(2) {
		t.Fatalf("usage update args = %v, want [tok-1 2]", args)
	}
}

func TestHandleIngestFormats(t *testing.T) {
	tests := []struct {
		name   string
		parser string
		body   string
		want   []map[string]string // expected fields per queued log, subset match
	}{
		{"json object", "json", `{"a":"1"}`, []map[string]string{{"a": "1"}}},
		{"json array", "", `[{"a":"1"},{"a":"2"}]`, []map[string]string{{"a": "1"}, {"a": "2"}}},
		{"ndjson", "json", "{\"a\":\"1\"}\n\n{\"a\":\"2\"}\r\n", []map[string]string{{"a": "1"}, {"a": "2"}}},
		{"kv", "kv", "user=alice action=\"log in\"\nuser=bob", []map[string]string{
			{"user": "alice", "action": "log in"}, {"user": "bob"}}},
		{"syslog", "syslog", "<34>1 2026-07-01T00:00:00Z host app - - - hi", []map[string]string{
			{"hostname": "host", "appname": "app", "message": "hi"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{parser: tc.parser})
			w, _ := do(f.h.HandleIngest, http.MethodPost, tc.body)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			logs := f.q.queued()
			if len(logs) != len(tc.want) {
				t.Fatalf("queued %d logs, want %d", len(logs), len(tc.want))
			}
			for i, want := range tc.want {
				for k, v := range want {
					if logs[i].Fields[k] != v {
						t.Errorf("log %d field %q = %q, want %q (fields %v)", i, k, logs[i].Fields[k], v, logs[i].Fields)
					}
				}
			}
		})
	}
}

// TestHandleIngestRejectsBeforeReadingBody: auth and backpressure rejections must
// not spend CPU reading and parsing a body that will be thrown away.
func TestHandleIngestRejectsBeforeReadingBody(t *testing.T) {
	tests := []struct {
		name       string
		opts       []reqOpt
		pressure   bool
		wantStatus int
		retryAfter string
	}{
		{"missing token", []reqOpt{noAuth()}, false, http.StatusUnauthorized, ""},
		{"unknown token", []reqOpt{withAuth("Bearer nope")}, false, http.StatusUnauthorized, ""},
		{"wrong scheme", []reqOpt{withAuth("Basic " + goodToken)}, false, http.StatusUnauthorized, ""},
		// A token that could not be checked is retryable: shippers drop a batch on 401.
		{"token store down", []reqOpt{withAuth("Bearer " + brokenToken)}, false, http.StatusServiceUnavailable, "2"},
		{"backend under pressure", nil, true, http.StatusTooManyRequests, "2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			if tc.pressure {
				f.q.diskState.active.Store(1)
			}
			w, read := do(f.h.HandleIngest, http.MethodPost, `{"a":1}`, tc.opts...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.wantStatus, w.Body)
			}
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			if read {
				t.Error("body was read before the request was rejected")
			}
			if resp := decodeIngestResponse(t, w); resp.Success || resp.Error == "" {
				t.Errorf("response = %+v, want an error", resp)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued on a rejected request", n)
			}
			noUsage(t, f.pg)
		})
	}
}

func TestHandleIngestBodyErrors(t *testing.T) {
	big := bytes.Repeat([]byte(`{"a":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`+"\n"), 1<<20/40)
	tests := []struct {
		name       string
		body       []byte
		opts       []reqOpt
		wantStatus int
		wantErr    string
	}{
		{"over the limit", big, nil, http.StatusRequestEntityTooLarge, "byte limit"},
		// A small compressed body that inflates past the limit is refused too.
		{"gzip bomb", gzipBytes(t, big), []reqOpt{withHeader("Content-Encoding", "gzip")}, http.StatusRequestEntityTooLarge, "byte limit"},
		{"bad gzip", []byte("not gzip"), []reqOpt{withHeader("Content-Encoding", "gzip")}, http.StatusBadRequest, "read request body"},
		{"empty", nil, nil, http.StatusBadRequest, "Failed to parse"},
		{"not json", []byte("hello world"), nil, http.StatusBadRequest, "Failed to parse"},
		{"empty array", []byte("[]"), nil, http.StatusBadRequest, "No valid logs"},
		// One non-object element fails the whole array, and the single-line NDJSON
		// fallback cannot rescue it.
		{"array with a scalar", []byte(`[{"a":1}, 5]`), nil, http.StatusBadRequest, "Failed to parse"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			w, _ := doBytes(f.h.HandleIngest, http.MethodPost, tc.body, tc.opts...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.wantStatus, w.Body)
			}
			if resp := decodeIngestResponse(t, w); !strings.Contains(resp.Error, tc.wantErr) {
				t.Errorf("error = %q, want it to mention %q", resp.Error, tc.wantErr)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued", n)
			}
		})
	}
}

func TestHandleIngestGzip(t *testing.T) {
	f := newFixture(t, tokenRow{})
	body := gzipBytes(t, []byte(`{"a":"1"}`+"\n"+`{"a":"2"}`))
	w, _ := doBytes(f.h.HandleIngest, http.MethodPost, body, withHeader("Content-Encoding", "GZIP"))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if n := len(f.q.queued()); n != 2 {
		t.Fatalf("queued %d, want 2", n)
	}
}

// TestHandleIngestRejectsOverlongLine: a line past the scanner limit used to end
// the scan silently, so every line after it was dropped while the request was
// still acknowledged. The whole request must be refused instead.
func TestHandleIngestRejectsOverlongLine(t *testing.T) {
	long := `{"a":"` + strings.Repeat("x", maxLineBytes) + `"}`
	body := "{\"n\":1}\n" + long + "\n{\"n\":3}\n"
	for _, parser := range []string{"json", "kv", "syslog"} {
		t.Run(parser, func(t *testing.T) {
			f := newFixture(t, tokenRow{parser: parser})
			f.h.maxBodySize = 0
			w, _ := do(f.h.HandleIngest, http.MethodPost, body)
			if w.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status %d, want 413: %.200s", w.Code, w.Body)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Fatalf("%d logs queued from a partially parsed request", n)
			}
		})
	}
}

func TestHandleIngestQuotaAndQueueRejections(t *testing.T) {
	t.Run("quota exceeded", func(t *testing.T) {
		f := newFixture(t, tokenRow{fractalID: "frac-q"})
		f.h.SetQuotaManager(quotaFor("frac-q", 100, 95, "reject"))
		w, _ := do(f.h.HandleIngest, http.MethodPost, `{"msg":"more than five bytes"}`)
		if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "30" {
			t.Fatalf("status %d Retry-After %q, want 429 / 30", w.Code, w.Header().Get("Retry-After"))
		}
		if n := len(f.q.queued()); n != 0 {
			t.Fatalf("%d logs queued past the quota", n)
		}
		noUsage(t, f.pg)
	})
	t.Run("rollover quota never rejects", func(t *testing.T) {
		f := newFixture(t, tokenRow{fractalID: "frac-q"})
		f.h.SetQuotaManager(quotaFor("frac-q", 100, 1000, "rollover"))
		if w, _ := do(f.h.HandleIngest, http.MethodPost, `{"msg":"x"}`); w.Code != http.StatusOK {
			t.Fatalf("status %d, want 200", w.Code)
		}
	})
	t.Run("queue full", func(t *testing.T) {
		f := newFixture(t, tokenRow{})
		f.q.accumBytes.Store(f.q.bufferBytes + 1)
		w, _ := do(f.h.HandleIngest, http.MethodPost, `{"msg":"x"}`)
		if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "2" {
			t.Fatalf("status %d Retry-After %q, want 429 / 2", w.Code, w.Header().Get("Retry-After"))
		}
		noUsage(t, f.pg)
	})
	t.Run("method", func(t *testing.T) {
		f := newFixture(t, tokenRow{})
		if w, _ := do(f.h.HandleIngest, http.MethodGet, ""); w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET status %d, want 405", w.Code)
		}
	})
}

// TestValidateIngestToken pins the accepted Authorization schemes and that a
// validated token is cached, so the hot path does not query Postgres per request.
func TestValidateIngestToken(t *testing.T) {
	f := newFixture(t, tokenRow{fractalID: "frac-v", parser: "kv"})
	for _, header := range []string{
		"Bearer " + goodToken,
		"bearer " + goodToken,
		"BEARER " + goodToken,
		"ApiKey " + goodToken, // go-elasticsearch (Velociraptor) sends this
		"APIKey " + goodToken,
		"Splunk " + goodToken, // Splunk HEC clients
	} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", header)
		tok, err := f.h.validateIngestToken(r)
		if err != nil {
			t.Fatalf("%q: %v", header, err)
		}
		if tok.FractalID != "frac-v" || tok.ParserType != "kv" || tok.TokenID != "tok-1" {
			t.Fatalf("%q: token = %+v", header, tok)
		}
	}
	if n := len(f.pg.Matching("FROM ingest_tokens")); n != 1 {
		t.Fatalf("token looked up %d times, want once then served from cache", n)
	}

	for _, header := range []string{"", "Bearer", "Bearer ", "Token " + goodToken, goodToken, "Bearer  " + goodToken} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", header)
		if _, err := f.h.validateIngestToken(r); !errors.Is(err, ingesttokens.ErrTokenRejected) {
			t.Errorf("%q: err = %v, want ErrTokenRejected", header, err)
		}
	}

	// Without a cache every request goes to Postgres.
	f2 := newFixture(t, tokenRow{})
	f2.h.tokenCache = nil
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer "+goodToken)
		if _, err := f2.h.validateIngestToken(r); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(f2.pg.Matching("FROM ingest_tokens")); n != 2 {
		t.Fatalf("uncached lookups = %d, want 2", n)
	}
}

// TestValidateIngestTokenCompilesNormalizer: the token's normalizer and timestamp
// fields come back from the same join and are applied to its logs.
func TestValidateIngestTokenCompilesNormalizer(t *testing.T) {
	f := newFixture(t, tokenRow{
		tsFields:    `[{"field":"when","format":"unix"}]`,
		normalizer:  `["snake_case"]`,
		normName:    "sysmon",
		normVersion: 7,
	})
	w, _ := do(f.h.HandleIngest, http.MethodPost, `{"EventID":"1","when":"1791581172"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	logs := f.q.queued()
	if len(logs) != 1 {
		t.Fatalf("queued %d", len(logs))
	}
	if logs[0].Fields["event_id"] != "1" || logs[0].Normalizer != "sysmon@7" {
		t.Errorf("normalizer not applied: fields %v stamp %q", logs[0].Fields, logs[0].Normalizer)
	}
	if !logs[0].Timestamp.Equal(time.Unix(1791581172, 0)) {
		t.Errorf("token timestamp field not applied: %v", logs[0].Timestamp)
	}
}

func TestTokenErrorStatus(t *testing.T) {
	w := httptest.NewRecorder()
	status, msg := tokenErrorStatus(w, fmt.Errorf("wrapped: %w", ingesttokens.ErrTokenRejected))
	if status != http.StatusUnauthorized || msg == "" || w.Header().Get("Retry-After") != "" {
		t.Errorf("rejected token -> %d %q Retry-After %q; want 401 without Retry-After", status, msg, w.Header().Get("Retry-After"))
	}
	w = httptest.NewRecorder()
	status, _ = tokenErrorStatus(w, errors.New("connection refused"))
	if status != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "2" {
		t.Errorf("lookup failure -> %d Retry-After %q; want 503 / 2", status, w.Header().Get("Retry-After"))
	}
}

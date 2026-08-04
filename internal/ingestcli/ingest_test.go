package ingestcli

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testClient uses a compressed retry policy so the loop can be exercised
// without waiting out production backoff.
func testClient(url string) *Client {
	return &Client{
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		url:            url,
		token:          "test",
		initialBackoff: time.Millisecond,
		maxBackoff:     5 * time.Millisecond,
		throttleBudget: 5 * time.Second,
	}
}

// A 429 is backpressure, not bad data. The client must keep retrying rather
// than abandon the batch, which previously discarded every log in it.
func TestSendBatchRetriesThroughThrottling(t *testing.T) {
	const throttles = 40 // far beyond maxRetries
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= throttles {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	stats := &Stats{}
	logs := []map[string]interface{}{{"a": 1}, {"a": 2}}

	if err, throttled := testClient(srv.URL).SendBatch(logs, stats); err != nil {
		t.Fatalf("SendBatch: %v", err)
	} else if !throttled {
		t.Error("throttling not reported to the pacer")
	}

	if got := calls.Load(); got != throttles+1 {
		t.Errorf("made %d attempts, want %d", got, throttles+1)
	}
	if stats.LogsSent.Load() != int64(len(logs)) {
		t.Errorf("LogsSent = %d, want %d", stats.LogsSent.Load(), len(logs))
	}
	if stats.Throttled.Load() == 0 {
		t.Error("Throttled counter not incremented")
	}
}

// 5xx keeps the bounded retry budget: a broken server should not hang the run.
func TestSendBatchGivesUpOnPersistentServerError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	err, throttled := testClient(srv.URL).SendBatch([]map[string]interface{}{{"a": 1}}, &Stats{})
	if err == nil {
		t.Fatal("expected error after retry budget exhausted")
	}
	if !throttled {
		t.Error("5xx should signal server pressure to the pacer")
	}
	if got := calls.Load(); got != maxRetries+1 {
		t.Errorf("made %d attempts, want %d", got, maxRetries+1)
	}
}

// 4xx other than 429/413 is a client error: fail immediately, no retries.
func TestSendBatchDoesNotRetryClientError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	err, _ := testClient(srv.URL).SendBatch([]map[string]interface{}{{"a": 1}}, &Stats{})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("made %d attempts, want 1", got)
	}
}

// Payloads above the threshold go out gzipped and must decode server-side.
func TestSendBatchGzipsLargePayloads(t *testing.T) {
	var gotEncoding string
	var decoded []map[string]interface{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEncoding = r.Header.Get("Content-Encoding")
		var reader io.Reader = r.Body
		if gotEncoding == "gzip" {
			gr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("gzip.NewReader: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer gr.Close()
			reader = gr
		}
		body, _ := io.ReadAll(reader)
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("unmarshal: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	logs := make([]map[string]interface{}, 200)
	for i := range logs {
		logs[i] = map[string]interface{}{"i": i, "msg": strings.Repeat("z", 128)}
	}

	if err, _ := testClient(srv.URL).SendBatch(logs, &Stats{}); err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if gotEncoding != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", gotEncoding)
	}
	if len(decoded) != len(logs) {
		t.Errorf("server received %d logs, want %d", len(decoded), len(logs))
	}
}

// A server that predates gzip support on /api/v1/ingest rejects compressed
// bodies. The client must fall back to plain JSON and stop compressing.
func TestSendBatchFallsBackWhenServerRejectsGzip(t *testing.T) {
	var gzipAttempts, plainAttempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			gzipAttempts.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"success":false,"error":"Failed to parse logs."}`))
			return
		}
		plainAttempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	logs := make([]map[string]interface{}, 200)
	for i := range logs {
		logs[i] = map[string]interface{}{"i": i, "msg": strings.Repeat("z", 128)}
	}

	c := testClient(srv.URL)
	if err, _ := c.SendBatch(logs, &Stats{}); err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if err, _ := c.SendBatch(logs, &Stats{}); err != nil {
		t.Fatalf("second batch: %v", err)
	}

	// Compression is attempted once, then latched off for the whole run.
	if got := gzipAttempts.Load(); got != 1 {
		t.Errorf("gzip attempts = %d, want 1", got)
	}
	if got := plainAttempts.Load(); got != 2 {
		t.Errorf("plain attempts = %d, want 2", got)
	}
}

// Small payloads are not worth compressing and must stay plain JSON.
func TestSendBatchSkipsGzipForSmallPayloads(t *testing.T) {
	var gotEncoding string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEncoding = r.Header.Get("Content-Encoding")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err, _ := testClient(srv.URL).SendBatch([]map[string]interface{}{{"a": 1}}, &Stats{}); err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if gotEncoding != "" {
		t.Errorf("Content-Encoding = %q, want empty", gotEncoding)
	}
}

// A batch that is throttled for the entire budget must give up rather than
// block the run forever.
func TestSendBatchGivesUpAfterThrottleBudget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := testClient(srv.URL)
	c.throttleBudget = 50 * time.Millisecond

	err, throttled := c.SendBatch([]map[string]interface{}{{"a": 1}}, &Stats{})
	if err == nil {
		t.Fatal("expected error once the throttle budget expired")
	}
	if !throttled {
		t.Error("throttling not reported to the pacer")
	}
}

func TestRetryAfterNeverShrinksBackoff(t *testing.T) {
	c := NewClient(&Config{URL: "http://example.invalid"})
	resp := &http.Response{Header: http.Header{}}

	resp.Header.Set("Retry-After", "1")
	if got := c.retryAfter(resp, 8*time.Second); got != 8*time.Second {
		t.Errorf("retryAfter = %v, want 8s (backoff must win)", got)
	}

	resp.Header.Set("Retry-After", "20")
	if got := c.retryAfter(resp, 2*time.Second); got != 20*time.Second {
		t.Errorf("retryAfter = %v, want 20s (server hint must win)", got)
	}

	resp.Header.Set("Retry-After", "600")
	if got := c.retryAfter(resp, time.Second); got != maxBackoff {
		t.Errorf("retryAfter = %v, want clamp to %v", got, maxBackoff)
	}
}

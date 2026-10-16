package ingest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func bulkBody(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func decodeBulk(t *testing.T, w *httptest.ResponseRecorder) ElasticBulkResponse {
	t.Helper()
	var resp ElasticBulkResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bulk response is not JSON (%d): %s", w.Code, w.Body)
	}
	return resp
}

// decodeBulkError reads the Elasticsearch error envelope clients parse on failure.
func decodeBulkError(t *testing.T, w *httptest.ResponseRecorder) (typ, reason string, status int) {
	t.Helper()
	var env struct {
		Error struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
		Status int `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("error envelope is not JSON (%d): %s", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	return env.Error.Type, env.Error.Reason, env.Status
}

// itemResult returns the item's one action and its result, failing the test when
// the item answers no action or several.
func itemResult(t *testing.T, it ElasticBulkItem) (string, ElasticIndexResult) {
	t.Helper()
	var action string
	var res ElasticIndexResult
	n := 0
	for name, r := range map[string]ElasticIndexResult{"index": it.Index, "create": it.Create, "delete": it.Delete, "update": it.Update} {
		if r != (ElasticIndexResult{}) {
			action, res = name, r
			n++
		}
	}
	if n != 1 {
		t.Fatalf("item answers %d actions: %+v", n, it)
	}
	return action, res
}

func TestParseBulkRequest(t *testing.T) {
	h := &ElasticBulkHandler{handler: &IngestHandler{}}
	body := bulkBody(
		`{"index":{"_index":"velo-artifacts","_id":"doc-1"}}`,
		`{"Name":"one","timestamp":"2026-07-01T00:00:00Z"}`,
		`{"create":{"_index":"velo-create"}}`,
		`{"Name":"two"}`,
		// delete has no source line; the next line is an action again. It is not
		// supported, but still answered so later items stay aligned.
		`{"delete":{"_index":"x","_id":"gone"}}`,
		`{"index":{}}`,
		`not json`,
		`{"create":{"_id":"c-2"}}`,
		`[1,2,3]`,
		// update's partial document is consumed but never ingested as a log.
		`{"update":{"_id":"u"}}`,
		`{"doc":{"Name":"partial"}}`,
		`garbage action line`,
		`{"index":{"_index":"after-garbage"}}`,
		`{"Name":"three"}`,
	)
	logs, items, err := h.parseBulkRequest([]byte(body), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	names := []string{}
	for _, l := range logs {
		names = append(names, l.Fields["Name"])
	}
	if strings.Join(names, ",") != "one,two,three" {
		t.Fatalf("ingested %v, want one,two,three", names)
	}
	if !logs[0].Timestamp.Equal(utc("2026-07-01T00:00:00Z")) {
		t.Errorf("source timestamp not extracted: %v", logs[0].Timestamp)
	}

	type res struct {
		action, index, id, result string
		status                    int
	}
	var got []res
	for _, it := range items {
		action, r := itemResult(t, it)
		got = append(got, res{action, r.Index, r.ID, r.Result, r.Status})
		if (r.Status >= 400) != (r.Error != nil) {
			t.Errorf("%s item status %d with error %+v", action, r.Status, r.Error)
		}
	}
	want := []res{
		{"index", "velo-artifacts", "doc-1", "created", 201},
		{"create", "velo-create", "", "created", 201},
		{"delete", "x", "gone", "", 400},
		{"index", "bifract", "", "", 400},
		{"create", "bifract", "c-2", "", 400},
		{"update", "bifract", "u", "", 400},
		{"index", "after-garbage", "", "created", 201},
	}
	if len(got) != len(want) {
		t.Fatalf("items = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("item %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseBulkRequestEdges(t *testing.T) {
	h := &ElasticBulkHandler{handler: &IngestHandler{}}
	for _, tc := range []struct {
		name        string
		body        string
		logs, items int
	}{
		{"empty", "", 0, 0},
		{"action without source at EOF", `{"index":{}}`, 0, 1},
		{"blank source line", "{\"index\":{}}\n\n{\"a\":1}\n", 0, 1},
		{"update at EOF", `{"update":{}}`, 0, 1},
		{"delete then index", "{\"delete\":{}}\n{\"index\":{}}\n{\"a\":1}\n", 1, 2},
		{"no trailing newline", "{\"index\":{}}\n{\"a\":1}", 1, 1},
		{"CRLF", "{\"index\":{}}\r\n{\"a\":1}\r\n", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs, items, err := h.parseBulkRequest([]byte(tc.body), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(logs) != tc.logs || len(items) != tc.items {
				t.Fatalf("logs=%d items=%d, want %d/%d", len(logs), len(items), tc.logs, tc.items)
			}
		})
	}
}

func TestHandleBulkSuccess(t *testing.T) {
	f := newFixture(t, tokenRow{fractalID: "frac-velo"})
	b := NewElasticBulkHandler(f.h)
	body := bulkBody(
		`{"index":{"_index":"some-other-index"}}`, `{"a":"1"}`,
		`{"index":{"_index":"some-other-index"}}`, `{"a":"2"}`,
	)
	w, _ := do(b.HandleBulk, http.MethodPut, body, withAuth("ApiKey "+goodToken))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	resp := decodeBulk(t, w)
	if resp.Errors || len(resp.Items) != 2 || resp.Items[0].Index.Status != 201 {
		t.Fatalf("response = %+v", resp)
	}
	// _index is echoed for the client but never routes: the token picks the fractal.
	logs := f.q.queued()
	if len(logs) != 2 || logs[0].FractalID != "frac-velo" || logs[1].FractalID != "frac-velo" {
		t.Fatalf("queued %+v", logs)
	}
	if args := waitForUsage(t, f.pg); args[1] != int64(2) {
		t.Fatalf("usage count = %v, want 2", args[1])
	}
}

// TestHandleBulkReportsDocumentErrors: bulk clients only inspect items when
// errors is true, so any failed document must set it, including when every
// document failed and nothing was queued.
func TestHandleBulkReportsDocumentErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		queued int
	}{
		{"some failed", bulkBody(`{"index":{}}`, `{"a":1}`, `{"index":{}}`, `oops`), 1},
		{"all failed", bulkBody(`{"index":{}}`, `oops`, `{"create":{}}`, `[1]`), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			w, _ := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodPost, tc.body)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d", w.Code)
			}
			if resp := decodeBulk(t, w); !resp.Errors {
				t.Fatalf("errors=false with failed items: %+v", resp.Items)
			}
			if n := len(f.q.queued()); n != tc.queued {
				t.Fatalf("queued %d, want %d", n, tc.queued)
			}
		})
	}

	// Nothing to do is not an error.
	f := newFixture(t, tokenRow{})
	w, _ := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodPost, "")
	if resp := decodeBulk(t, w); w.Code != http.StatusOK || resp.Errors || len(resp.Items) != 0 {
		t.Fatalf("empty bulk: %d %+v", w.Code, resp)
	}
}

func TestHandleBulkRejections(t *testing.T) {
	tests := []struct {
		name       string
		opts       []reqOpt
		setup      func(*fixture)
		wantStatus int
		retryAfter string
		bodyRead   bool
		reason     string
	}{
		{name: "no token", opts: []reqOpt{noAuth()}, wantStatus: http.StatusUnauthorized, reason: "invalid ingest token"},
		{name: "token store down", opts: []reqOpt{withAuth("Bearer " + brokenToken)}, wantStatus: http.StatusServiceUnavailable, retryAfter: "2", reason: "could not be checked"},
		{name: "under pressure", setup: func(f *fixture) { f.q.cpuState.active.Store(1) }, wantStatus: http.StatusTooManyRequests, retryAfter: "2", reason: "pressure"},
		{name: "too large", setup: func(f *fixture) { f.h.maxBodySize = 10 }, wantStatus: http.StatusRequestEntityTooLarge, bodyRead: true, reason: "byte limit"},
		{name: "bad gzip", opts: []reqOpt{withHeader("Content-Encoding", "gzip")}, wantStatus: http.StatusBadRequest, bodyRead: true, reason: "read request body"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			if tc.setup != nil {
				tc.setup(f)
			}
			w, read := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodPost, bulkBody(`{"index":{}}`, `{"a":1}`), tc.opts...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.wantStatus, w.Body)
			}
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			if read != tc.bodyRead {
				t.Errorf("body read = %v, want %v", read, tc.bodyRead)
			}
			typ, reason, status := decodeBulkError(t, w)
			if typ == "" || status != tc.wantStatus || !strings.Contains(reason, tc.reason) {
				t.Errorf("envelope = %q %q %d, want reason containing %q", typ, reason, status, tc.reason)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued", n)
			}
		})
	}

	f := newFixture(t, tokenRow{})
	if w, _ := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodGet, ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status %d, want 405", w.Code)
	}
}

// TestHandleBulkBackpressureMarksItemsRetryable: when the batch cannot be taken,
// every document that parsed is reported as a 429 item (so the client retries
// exactly those), documents that failed to parse keep their 400, and the
// response carries Retry-After like every other ingest rejection.
func TestHandleBulkBackpressureMarksItemsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setup      func(*fixture)
		retryAfter string
		reason     string
	}{
		{"queue full", func(f *fixture) { f.q.accumBytes.Store(f.q.bufferBytes + 1) }, "2", "Ingestion queue full"},
		{"quota exceeded", func(f *fixture) { f.h.SetQuotaManager(quotaFor("frac-1", 10, 10, "reject")) }, "30", "quota"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			tc.setup(f)
			body := bulkBody(`{"index":{}}`, `{"a":1}`, `{"create":{}}`, `{"a":2}`, `{"index":{}}`, `bad`)
			w, _ := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodPost, body)
			if w.Code != http.StatusTooManyRequests {
				t.Fatalf("status %d, want 429", w.Code)
			}
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			resp := decodeBulk(t, w)
			if !resp.Errors || len(resp.Items) != 3 {
				t.Fatalf("response = %+v", resp)
			}
			if it := resp.Items[0].Index; it.Status != 429 || it.Result != "" || it.Error == nil || !strings.Contains(it.Error.Reason, tc.reason) {
				t.Errorf("index item = %+v", it)
			}
			if it := resp.Items[1].Create; it.Status != 429 || it.Error == nil || !strings.Contains(it.Error.Reason, tc.reason) {
				t.Errorf("create item = %+v", it)
			}
			if it := resp.Items[2].Index; it.Status != 400 {
				t.Errorf("unparseable item became %+v, want to stay 400", it)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued", n)
			}
			noUsage(t, f.pg)
		})
	}
}

func TestHandleBulkRejectsOverlongLine(t *testing.T) {
	f := newFixture(t, tokenRow{})
	f.h.maxBodySize = 0
	body := bulkBody(`{"index":{}}`, `{"a":"`+strings.Repeat("x", maxLineBytes)+`"}`, `{"index":{}}`, `{"a":2}`)
	w, _ := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodPost, body)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", w.Code)
	}
	if n := len(f.q.queued()); n != 0 {
		t.Fatalf("%d logs queued from a truncated bulk body", n)
	}
}

// FuzzParseBulkRequest: no input may panic, every item answers exactly one action,
// and every queued log must have exactly one 201 item, since clients match items to
// documents to decide what to resend.
func FuzzParseBulkRequest(f *testing.F) {
	f.Add([]byte(bulkBody(`{"index":{"_index":"a","_id":"1"}}`, `{"x":1}`, `{"delete":{}}`, `{"create":{}}`, `bad`)))
	f.Add([]byte("{\"index\":null}\n{\"x\":1}\n"))
	f.Add([]byte("{\"index\":{\"_index\":5}}\n{}\n"))
	h := &ElasticBulkHandler{handler: &IngestHandler{}}
	f.Fuzz(func(t *testing.T, body []byte) {
		logs, items, err := h.parseBulkRequest(body, nil, nil)
		if err != nil {
			return
		}
		created := 0
		for _, it := range items {
			if _, r := itemResult(t, it); r.Status == 201 {
				created++
			}
		}
		if created != len(logs) {
			t.Fatalf("%d logs but %d created items", len(logs), created)
		}
	})
}

// TestHandleBulkItemsAlignWithActions: bulk clients pair items with actions by
// position, so every action line gets exactly one item, in order, under its own
// action key and nothing else, with failures in the Elasticsearch error shape.
// Unsupported actions and missing documents are permanent (400) failures.
func TestHandleBulkItemsAlignWithActions(t *testing.T) {
	f := newFixture(t, tokenRow{})
	body := bulkBody(
		`{"index":{"_index":"i","_id":"1"}}`, `{"a":1}`,
		`{"delete":{"_index":"i","_id":"gone"}}`,
		`{"update":{"_index":"i","_id":"u"}}`, `{"doc":{"index":{"a":3}}}`,
		`{"create":{"_id":"2"}}`, `{"a":2}`,
		`{"create":{"_id":"blank"}}`, ``,
		`{"index":{"_id":"3"}}`, `{"a":3}`,
		`{"index":{"_id":"eof"}}`,
	)
	w, _ := do(NewElasticBulkHandler(f.h).HandleBulk, http.MethodPost, strings.TrimSuffix(body, "\n"))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var resp struct {
		Errors bool                                    `json:"errors"`
		Items  []map[string]map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response: %v: %s", err, w.Body)
	}
	want := []struct {
		action, id string
		status     int
	}{
		{"index", "1", 201},
		{"delete", "gone", 400},
		{"update", "u", 400},
		{"create", "2", 201},
		{"create", "blank", 400},
		{"index", "3", 201},
		{"index", "eof", 400},
	}
	if !resp.Errors {
		t.Error("errors=false with failed items")
	}
	if len(resp.Items) != len(want) {
		t.Fatalf("%d items, want %d: %s", len(resp.Items), len(want), w.Body)
	}
	for i, wi := range want {
		item := resp.Items[i]
		res, ok := item[wi.action]
		if len(item) != 1 || !ok {
			t.Errorf("item %d has keys %v, want only %q", i, keys(item), wi.action)
			continue
		}
		var id string
		var status int
		_ = json.Unmarshal(res["_id"], &id)
		_ = json.Unmarshal(res["status"], &status)
		if id != wi.id || status != wi.status {
			t.Errorf("item %d = %s %q %d, want %s %q %d", i, wi.action, id, status, wi.action, wi.id, wi.status)
		}
		if status < 300 {
			if _, has := res["error"]; has {
				t.Errorf("item %d succeeded but carries an error", i)
			}
			continue
		}
		var e struct{ Type, Reason string }
		if err := json.Unmarshal(res["error"], &e); err != nil || e.Type == "" || e.Reason == "" {
			t.Errorf("item %d error is not {type, reason}: %s", i, res["error"])
		}
	}
	if n := len(f.q.queued()); n != 3 {
		t.Errorf("queued %d logs, want 3 (update bodies are never logs)", n)
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

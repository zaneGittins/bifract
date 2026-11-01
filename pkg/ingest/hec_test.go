package ingest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/storage"
)

func decodeHEC(t *testing.T, w *httptest.ResponseRecorder) HECResponse {
	t.Helper()
	var resp HECResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not HEC JSON (%d): %q", w.Code, w.Body.String())
	}
	return resp
}

func withQuery(q string) reqOpt { return func(r *http.Request) { r.URL.RawQuery = q } }

func splunkAuth() reqOpt { return withAuth("Splunk " + goodToken) }

func basicAuth(user, pass string) reqOpt {
	return func(r *http.Request) { r.SetBasicAuth(user, pass) }
}

// TestHECAuthSchemes: HEC senders pass the token as "Splunk <token>", or as the
// Basic auth password with any user name.
func TestHECAuthSchemes(t *testing.T) {
	for name, opt := range map[string]reqOpt{
		"splunk":        splunkAuth(),
		"bearer":        withAuth("Bearer " + goodToken),
		"basic":         basicAuth("x", goodToken),
		"basic no user": basicAuth("", goodToken),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			if w, _ := do(NewHECHandler(f.h).HandleEvent, http.MethodPost, `{"event":"x"}`, opt); w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
		})
	}
	// Basic stays HEC-only: the other ingest endpoints do not read it.
	f := newFixture(t, tokenRow{})
	if w, _ := do(f.h.HandleIngest, http.MethodPost, `{"a":1}`, basicAuth("x", goodToken)); w.Code != http.StatusUnauthorized {
		t.Fatalf("native ingest with Basic: %d", w.Code)
	}
}

func TestHECEventMapsEnvelope(t *testing.T) {
	f := newFixture(t, tokenRow{fractalID: "frac-hec"})
	h := NewHECHandler(f.h)
	// Concatenated with and without whitespace, as forwarders batch them.
	body := `{"time":1791581172.686968,"host":"web-1","source":"/var/log/app.log","sourcetype":"app:json","index":"main",` +
		`"event":{"msg":"login","user":"alice","host":"shadowed","id":18446744073709551615},"fields":{"region":"eu","tags":["a","b"]}}` +
		`{"time":"1791581173","event":"plain text line"}` + "\n" +
		`{"event":{"n":1}}`

	w, _ := do(h.HandleEvent, http.MethodPost, body, splunkAuth(), withQuery("sourcetype=default_st&host=default-host"))
	if w.Code != http.StatusOK || w.Body.String() != `{"text":"Success","code":0}`+"\n" {
		t.Fatalf("status %d body %q", w.Code, w.Body)
	}

	logs := f.q.queued()
	if len(logs) != 3 {
		t.Fatalf("queued %d logs, want 3", len(logs))
	}
	for _, l := range logs {
		if l.FractalID != "frac-hec" || l.LogID == "" {
			t.Errorf("log not routed to the token's fractal: %+v", l)
		}
	}

	first := logs[0]
	want := map[string]string{
		"msg": "login", "user": "alice", "id": "18446744073709551615",
		// Envelope metadata is what HEC users filter on, so it wins over an event key.
		"host": "web-1", "source": "/var/log/app.log", "sourcetype": "app:json", "index": "main",
		"region": "eu", "tags": `["a","b"]`,
	}
	for k, v := range want {
		if first.Fields[k] != v {
			t.Errorf("first.%s = %q, want %q", k, first.Fields[k], v)
		}
	}
	if !first.Timestamp.Equal(time.Unix(1791581172, 686968000)) {
		t.Errorf("time = %v, want the envelope time to the microsecond", first.Timestamp)
	}
	if first.RawLog != `{"host":"shadowed","id":18446744073709551615,"msg":"login","user":"alice"}` {
		t.Errorf("raw log = %s, want the event as sent", first.RawLog)
	}

	second := logs[1]
	if second.RawLog != "plain text line" || second.Fields["message"] != "plain text line" {
		t.Errorf("string event: raw %q fields %v", second.RawLog, second.Fields)
	}
	if !second.Timestamp.Equal(time.Unix(1791581173, 0)) {
		t.Errorf("string time = %v", second.Timestamp)
	}
	// Query parameters fill in only what the envelope and event leave out.
	if second.Fields["sourcetype"] != "default_st" || second.Fields["host"] != "default-host" {
		t.Errorf("query defaults not applied: %v", second.Fields)
	}
	if logs[2].Fields["n"] != "1" || logs[2].Timestamp.IsZero() {
		t.Errorf("third: %+v", logs[2])
	}

	if args := waitForUsage(t, f.pg); args[1] != int64(3) {
		t.Fatalf("usage count = %v, want 3", args[1])
	}
}

// TestHECEnvelopeEdges pins how metadata, time and fields combine with the event.
func TestHECEnvelopeEdges(t *testing.T) {
	queue := func(t *testing.T, body, query string) []storage.LogEntry {
		t.Helper()
		f := newFixture(t, tokenRow{})
		w, _ := do(NewHECHandler(f.h).HandleEvent, http.MethodPost, body, splunkAuth(), withQuery(query))
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
		return f.q.queued()
	}

	t.Run("query defaults never replace event keys", func(t *testing.T) {
		logs := queue(t, `{"event":{"host":"from-event"}}{"event":{"a":1}}`, "host=from-query")
		if logs[0].Fields["host"] != "from-event" || logs[1].Fields["host"] != "from-query" {
			t.Fatalf("hosts = %q, %q", logs[0].Fields["host"], logs[1].Fields["host"])
		}
	})
	t.Run("metadata is part of the log id", func(t *testing.T) {
		logs := queue(t, `{"time":100,"host":"a","event":"started"}{"time":100,"host":"b","event":"started"}`+
			`{"time":100,"host":"a","event":"started","fields":{"x":"1"}}`, "")
		if logs[0].LogID == logs[1].LogID || logs[0].LogID == logs[2].LogID {
			t.Fatalf("distinct events share a log id: %s %s %s", logs[0].LogID, logs[1].LogID, logs[2].LogID)
		}
		again := queue(t, `{"time":100,"host":"a","event":"started"}`, "")
		if again[0].LogID != logs[0].LogID {
			t.Fatalf("log id is not deterministic: %s vs %s", again[0].LogID, logs[0].LogID)
		}
	})
	t.Run("html characters stay literal", func(t *testing.T) {
		logs := queue(t, `{"event":{"cmd":"a<b && c>d"},"fields":{"t":["x>y"]}}{"event":["p<q"]}`, "")
		if want := `{"cmd":"a<b && c>d"}`; logs[0].RawLog != want {
			t.Errorf("object raw log = %s, want %s", logs[0].RawLog, want)
		}
		if want := `["x>y"]`; logs[0].Fields["t"] != want {
			t.Errorf("fields t = %q, want %q", logs[0].Fields["t"], want)
		}
		if want := `["p<q"]`; logs[1].RawLog != want || logs[1].Fields["message"] != want {
			t.Errorf("array event: raw %s message %q, want %s", logs[1].RawLog, logs[1].Fields["message"], want)
		}
	})
	t.Run("exponent time", func(t *testing.T) {
		logs := queue(t, `{"time":1.791581172686968E9,"event":"x"}`, "")
		if !logs[0].Timestamp.Equal(time.Unix(1791581172, 686968000)) {
			t.Fatalf("time = %v", logs[0].Timestamp)
		}
	})
}

// TestHECStringEventsUseTokenParser: a string event is read by the token's parser,
// as the same line sent to /api/v1/ingest would be.
func TestHECStringEventsUseTokenParser(t *testing.T) {
	tests := []struct {
		parser, event string
		want          map[string]string
	}{
		{"json", `{\"a\":1}`, map[string]string{"a": "1"}},
		{"json", `not json`, map[string]string{"message": "not json"}},
		{"kv", `user=bob action=\"log in\"`, map[string]string{"user": "bob", "action": "log in"}},
		{"syslog", `<34>Oct 11 22:14:15 mymachine su: failed`, map[string]string{"hostname": "mymachine", "severity_name": "critical"}},
	}
	for _, tc := range tests {
		t.Run(tc.parser+" "+tc.event, func(t *testing.T) {
			f := newFixture(t, tokenRow{parser: tc.parser})
			w, _ := do(NewHECHandler(f.h).HandleEvent, http.MethodPost, `{"host":"h1","event":"`+tc.event+`"}`, splunkAuth())
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			logs := f.q.queued()
			if len(logs) != 1 {
				t.Fatalf("queued %d", len(logs))
			}
			for k, v := range tc.want {
				if logs[0].Fields[k] != v {
					t.Errorf("%s = %q, want %q (fields %v)", k, logs[0].Fields[k], v, logs[0].Fields)
				}
			}
			if logs[0].Fields["host"] != "h1" {
				t.Errorf("host metadata lost: %v", logs[0].Fields)
			}
		})
	}
}

// TestHECEventErrors: like Splunk, a bad event stops the batch and is reported by
// position, and the events before it are kept.
func TestHECEventErrors(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		code    int
		invalid int // -1: no invalid-event-number
		queued  int
	}{
		{"empty", "  \n", 5, -1, 0},
		{"malformed", `{"event":"a"}{"event":`, 6, 1, 1},
		{"not an object", `{"event":"a"}["x"]`, 6, 1, 1},
		{"missing event", `{"event":"a"}{"event":"b"}{"host":"x"}`, 12, 2, 2},
		{"null event", `{"event":null}`, 12, 0, 0},
		{"blank event", `{"event":""}`, 13, 0, 0},
		{"fields not an object", `{"event":"a"}{"event":"b","fields":["x"]}`, 15, 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			w, _ := do(NewHECHandler(f.h).HandleEvent, http.MethodPost, tc.body, splunkAuth())
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400: %s", w.Code, w.Body)
			}
			resp := decodeHEC(t, w)
			if resp.Code != tc.code || resp.Text == "" {
				t.Errorf("response = %+v, want code %d", resp, tc.code)
			}
			switch {
			case tc.invalid < 0 && resp.InvalidEventNumber != nil:
				t.Errorf("unexpected invalid-event-number %d", *resp.InvalidEventNumber)
			case tc.invalid >= 0 && (resp.InvalidEventNumber == nil || *resp.InvalidEventNumber != tc.invalid):
				t.Errorf("invalid-event-number = %v, want %d", resp.InvalidEventNumber, tc.invalid)
			}
			if n := len(f.q.queued()); n != tc.queued {
				t.Errorf("queued %d, want %d", n, tc.queued)
			}
		})
	}
}

// TestHECPartialBatchRejected: when the events before a bad one cannot be queued,
// the answer is the retryable 503, not the 400, so the sender resends them.
func TestHECPartialBatchRejected(t *testing.T) {
	f := newFixture(t, tokenRow{})
	f.q.accumBytes.Store(f.q.bufferBytes + 1)
	w, _ := do(NewHECHandler(f.h).HandleEvent, http.MethodPost, `{"event":"a"}{"bad"`, splunkAuth())
	if w.Code != http.StatusServiceUnavailable || decodeHEC(t, w).Code != 9 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if n := len(f.q.queued()); n != 0 {
		t.Fatalf("%d logs queued", n)
	}
}

func TestHECRawRejectsOverlongLine(t *testing.T) {
	f := newFixture(t, tokenRow{})
	f.h.maxBodySize = 0
	body := "short\n" + strings.Repeat("x", maxLineBytes+1) + "\nafter\n"
	w, _ := do(NewHECHandler(f.h).HandleRaw, http.MethodPost, body, splunkAuth())
	if w.Code != http.StatusRequestEntityTooLarge || decodeHEC(t, w).Code != 6 {
		t.Fatalf("status %d: %.200s", w.Code, w.Body)
	}
	if n := len(f.q.queued()); n != 0 {
		t.Fatalf("%d logs queued from a truncated raw body", n)
	}
}

func TestHECRaw(t *testing.T) {
	f := newFixture(t, tokenRow{parser: "kv"})
	body := "user=alice action=login\n\n  \nuser=bob action=logout\n"
	w, _ := do(NewHECHandler(f.h).HandleRaw, http.MethodPost, body, splunkAuth(),
		withQuery("host=fw-1&source=udp:514&sourcetype=fw&index=net&channel=abc"))
	if w.Code != http.StatusOK || decodeHEC(t, w).Code != 0 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	logs := f.q.queued()
	if len(logs) != 2 {
		t.Fatalf("queued %d, want 2 (blank lines skipped)", len(logs))
	}
	if logs[1].RawLog != "user=bob action=logout" || logs[1].Fields["action"] != "logout" {
		t.Errorf("second line: raw %q fields %v", logs[1].RawLog, logs[1].Fields)
	}
	for k, v := range map[string]string{"host": "fw-1", "source": "udp:514", "sourcetype": "fw", "index": "net"} {
		if logs[0].Fields[k] != v {
			t.Errorf("%s = %q, want %q", k, logs[0].Fields[k], v)
		}
	}
	if _, ok := logs[0].Fields["channel"]; ok {
		t.Errorf("channel is transport, not data: %v", logs[0].Fields)
	}

	f = newFixture(t, tokenRow{})
	w, _ = do(NewHECHandler(f.h).HandleRaw, http.MethodPost, "\n", splunkAuth())
	if w.Code != http.StatusBadRequest || decodeHEC(t, w).Code != 5 {
		t.Fatalf("empty raw body: %d %s", w.Code, w.Body)
	}
}

func TestHECGzip(t *testing.T) {
	f := newFixture(t, tokenRow{})
	body := gzipBytes(t, []byte(`{"event":"zipped"}`))
	w, _ := doBytes(NewHECHandler(f.h).HandleEvent, http.MethodPost, body, splunkAuth(), withHeader("Content-Encoding", "gzip"))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if logs := f.q.queued(); len(logs) != 1 || logs[0].Fields["message"] != "zipped" {
		t.Fatalf("queued %+v", logs)
	}
}

func TestHECRejections(t *testing.T) {
	tests := []struct {
		name       string
		opts       []reqOpt
		setup      func(*fixture)
		wantStatus int
		code       int
		retryAfter string
		bodyRead   bool
	}{
		{name: "no token", opts: []reqOpt{noAuth()}, wantStatus: http.StatusUnauthorized, code: 2},
		{name: "unknown scheme", opts: []reqOpt{withAuth("Token " + goodToken)}, wantStatus: http.StatusUnauthorized, code: 3},
		{name: "scheme without token", opts: []reqOpt{withAuth("Splunk ")}, wantStatus: http.StatusUnauthorized, code: 3},
		{name: "basic without password", opts: []reqOpt{basicAuth("x", "")}, wantStatus: http.StatusUnauthorized, code: 3},
		{name: "unknown token", opts: []reqOpt{withAuth("Splunk bifract_ingest_nope")}, wantStatus: http.StatusForbidden, code: 4},
		{name: "token store down", opts: []reqOpt{withAuth("Splunk " + brokenToken)}, wantStatus: http.StatusServiceUnavailable, code: 9, retryAfter: "2"},
		// Splunk answers busy with 503, which every HEC client retries.
		{name: "under pressure", setup: func(f *fixture) { f.q.cpuState.active.Store(1) }, wantStatus: http.StatusServiceUnavailable, code: 9, retryAfter: "2"},
		{name: "too large", setup: func(f *fixture) { f.h.maxBodySize = 5 }, wantStatus: http.StatusRequestEntityTooLarge, code: 6, bodyRead: true},
		{name: "bad gzip", opts: []reqOpt{withHeader("Content-Encoding", "gzip")}, wantStatus: http.StatusBadRequest, code: 6, bodyRead: true},
		{name: "quota", setup: func(f *fixture) { f.h.SetQuotaManager(quotaFor("frac-1", 10, 10, "reject")) }, wantStatus: http.StatusServiceUnavailable, code: 9, retryAfter: "30", bodyRead: true},
		{name: "queue full", setup: func(f *fixture) { f.q.accumBytes.Store(f.q.bufferBytes + 1) }, wantStatus: http.StatusServiceUnavailable, code: 9, retryAfter: "2", bodyRead: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			if tc.setup != nil {
				tc.setup(f)
			}
			opts := append([]reqOpt{splunkAuth()}, tc.opts...)
			w, read := do(NewHECHandler(f.h).HandleEvent, http.MethodPost, `{"event":"hello"}`, opts...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.wantStatus, w.Body)
			}
			if resp := decodeHEC(t, w); resp.Code != tc.code || resp.Text == "" {
				t.Errorf("response = %+v, want code %d", resp, tc.code)
			}
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			if read != tc.bodyRead {
				t.Errorf("body read = %v, want %v", read, tc.bodyRead)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued", n)
			}
			noUsage(t, f.pg)
		})
	}

	f := newFixture(t, tokenRow{})
	if w, _ := do(NewHECHandler(f.h).HandleEvent, http.MethodGet, ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status %d, want 405", w.Code)
	}
}

func TestHECHealth(t *testing.T) {
	f := newFixture(t, tokenRow{})
	h := NewHECHandler(f.h)
	w, _ := do(h.HandleHealth, http.MethodGet, "", noAuth())
	if resp := decodeHEC(t, w); w.Code != http.StatusOK || resp.Code != 17 || !strings.Contains(resp.Text, "healthy") {
		t.Fatalf("healthy: %d %+v", w.Code, resp)
	}
	f.q.cpuState.active.Store(1)
	w, _ = do(h.HandleHealth, http.MethodGet, "", noAuth())
	if resp := decodeHEC(t, w); w.Code != http.StatusServiceUnavailable || resp.Code != 18 {
		t.Fatalf("under pressure: %d %+v", w.Code, resp)
	}
}

// FuzzHECParseEvents: no input may panic, and a reported invalid event number is
// always the count of events kept before it.
func FuzzHECParseEvents(f *testing.F) {
	f.Add([]byte(`{"time":"1.5","event":{"a":1},"fields":{"b":[1]}}{"event":"x"}`))
	f.Add([]byte(`{"event":null}{"event":""}["x"]`))
	f.Add([]byte(`{"event":7}` + "\n" + `{"event":{"n":18446744073709551615}}`))
	f.Add([]byte(`{"event":"<34>Oct 11 22:14:15 host su: x"}{"event":"a=1 b='2"}`))
	h := NewHECHandler(&IngestHandler{})
	f.Fuzz(func(t *testing.T, body []byte) {
		for _, parser := range []string{"json", "kv", "syslog"} {
			logs, herr := h.parseEvents(body, hecMeta{host: "q"}, &ingesttokens.ValidatedToken{ParserType: parser})
			if herr != nil && herr.resp.InvalidEventNumber != nil && *herr.resp.InvalidEventNumber != len(logs) {
				t.Fatalf("%s: invalid-event-number %d with %d events kept", parser, *herr.resp.InvalidEventNumber, len(logs))
			}
			if herr == nil && len(logs) == 0 {
				t.Fatalf("%s: no events and no error", parser)
			}
		}
	})
}

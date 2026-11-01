//go:build integration

// Splunk HEC senders only change the URL and token: the envelope's metadata
// becomes fields, and both the event and raw endpoints land in the token's fractal.
//
//	go test -tags integration ./test/integration/ -run TestSplunkHEC -v

package integration

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestSplunkHEC(t *testing.T) {
	t.Parallel()
	c := New(t)
	fractalID, scoped := newFractal(t, c, "hec")
	token := newShipper(t, scoped, fractalID).Key
	marker := fmt.Sprintf("api-suite-hec-%d", time.Now().UnixNano())

	// HEC lives at the server root, not under /api/v1, and answers in its own
	// shape. Busy is a 429 (rate limit) or a 503 (backpressure); a sender retries both.
	hec := func(method, path string, auth func(*http.Request), body []byte, gzipped bool) (int, map[string]any) {
		t.Helper()
		for attempt := 0; ; attempt++ {
			req, err := http.NewRequest(method, c.BaseURL+path, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			auth(req)
			if gzipped {
				req.Header.Set("Content-Encoding", "gzip")
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", method, path, err)
			}
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()
			busy := res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusServiceUnavailable
			if busy && attempt < 20 {
				time.Sleep(time.Second)
				continue
			}
			var out map[string]any
			if err := json.Unmarshal(raw, &out); err != nil {
				t.Fatalf("%s %s: %d, body is not JSON: %s", method, path, res.StatusCode, truncate(raw))
			}
			return res.StatusCode, out
		}
	}

	splunk := func(token string) func(*http.Request) {
		return func(r *http.Request) { r.Header.Set("Authorization", "Splunk "+token) }
	}

	// 0. Load balancers and senders poll health without a token.
	if code, out := hec(http.MethodGet, "/services/collector/health", func(*http.Request) {}, nil, false); code != 200 || out["code"] != float64(17) {
		t.Fatalf("health: %d %v", code, out)
	}

	// 1. Concatenated envelopes: an object event, and a string event whose marker
	//    rides in the indexed fields.
	now := float64(time.Now().UnixMilli()) / 1000
	events := fmt.Sprintf(`{"time":%.3f,"host":"web-1","sourcetype":"app","event":{"suite_marker":%q,"n":1}}`+
		`{"time":%.3f,"host":"web-1","sourcetype":"app","event":"text line","fields":{"suite_marker":%q}}`, now, marker, now, marker)
	if code, out := hec(http.MethodPost, "/services/collector/event", splunk(token), []byte(events), false); code != 200 || out["code"] != float64(0) {
		t.Fatalf("event endpoint: %d %v", code, out)
	}

	// 2. Raw lines, gzipped, with the metadata in the query string.
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	fmt.Fprintf(zw, "{\"suite_marker\":%q,\"n\":3}\n", marker)
	zw.Close()
	if code, out := hec(http.MethodPost, "/services/collector/raw?host=web-1&sourcetype=app", splunk(token), zipped.Bytes(), true); code != 200 || out["code"] != float64(0) {
		t.Fatalf("raw endpoint: %d %v", code, out)
	}

	// 3. The bare path is the event endpoint, and the token can ride as the Basic
	//    auth password, as with curl -u x:$TOKEN.
	basic := func(r *http.Request) { r.SetBasicAuth("x", token) }
	alias := fmt.Sprintf(`{"time":%.3f,"host":"web-1","sourcetype":"app","event":{"suite_marker":%q,"n":4}}`, now, marker)
	if code, out := hec(http.MethodPost, "/services/collector", basic, []byte(alias), false); code != 200 || out["code"] != float64(0) {
		t.Fatalf("bare path with Basic auth: %d %v", code, out)
	}

	// 4. A token Bifract does not know is refused in HEC's shape, as Splunk does.
	if code, out := hec(http.MethodPost, "/services/collector/event", splunk("bifract_ingest_unknown"), []byte(`{"event":"x"}`), false); code != 403 || out["code"] != float64(4) {
		t.Fatalf("unknown token: %d %v", code, out)
	}

	// 5. All four are queryable by the Splunk metadata. The default normalizer
	//    maps host to computer_name, as it does for any other source.
	Eventually(t, "the HEC events to become queryable", 90*time.Second, func() bool {
		var res struct {
			Success bool `json:"success"`
			Count   int  `json:"count"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query":      fmt.Sprintf(`suite_marker=%q computer_name="web-1" sourcetype="app"`, marker),
			"fractal_id": fractalID,
			"start":      time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339),
			"end":        time.Now().Add(1 * time.Minute).UTC().Format(time.RFC3339),
		}, &res)
		return res.Success && res.Count >= 4
	})
}

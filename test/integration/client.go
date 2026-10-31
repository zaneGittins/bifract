//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"bifract/pkg/api"
)

// Client is a thin wrapper over net/http for talking to a running Bifract. It
// carries the credential and the scope, unwraps the response envelope, and
// fails the test on an unexpected status. Everything a reader of these tests
// needs to learn stays in the recipe: the method, the path, and the body.
type Client struct {
	BaseURL string
	Key     string
	Scope   string

	http *http.Client
}

// Config names the environment the suite runs against.
const (
	EnvURL     = "BIFRACT_API_URL"
	EnvKey     = "BIFRACT_API_KEY"
	EnvFractal = "BIFRACT_FRACTAL_ID"
)

// New returns a client for the instance under test, skipping the test when the
// suite has not been pointed at one. These tests talk to a real server on
// purpose, so refusing to run is the honest outcome, not a failure.
func New(t *testing.T) *Client {
	t.Helper()

	key := os.Getenv(EnvKey)
	if key == "" {
		t.Skipf("set %s to a tenant-admin API key to run the API suite (see test/integration/README.md)", EnvKey)
	}
	base := os.Getenv(EnvURL)
	if base == "" {
		base = "http://localhost:8080"
	}

	c := &Client{
		BaseURL: strings.TrimSuffix(base, "/"),
		Key:     key,
		Scope:   scopeFor(os.Getenv(EnvFractal)),
		http:    &http.Client{Timeout: 60 * time.Second},
	}
	c.requireReachable(t)
	return c
}

// fractalFromEnv returns the configured fractal, if the suite was given one.
func fractalFromEnv() string { return os.Getenv(EnvFractal) }

func scopeFor(fractalID string) string {
	if fractalID == "" {
		return ""
	}
	return "fractal:" + fractalID
}

// InScope returns a copy of the client acting on a different fractal, so a
// recipe can provision one and immediately work inside it.
func (c *Client) InScope(fractalID string) *Client {
	next := *c
	next.Scope = scopeFor(fractalID)
	return &next
}

func (c *Client) requireReachable(t *testing.T) {
	t.Helper()

	res, err := c.http.Get(c.BaseURL + "/api/v1/health")
	if err != nil {
		t.Skipf("no Bifract at %s (%v); start the stack or point %s elsewhere", c.BaseURL, err, EnvURL)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Skipf("%s answered %d for /api/v1/health; is this a Bifract instance?", c.BaseURL, res.StatusCode)
	}
}

// Do sends a request and decodes the envelope's data into out, which may be
// nil. It fails the test on any non-2xx answer, so a recipe reads as the
// sequence of calls it is describing rather than a ladder of error checks.
func (c *Client) Do(t *testing.T, method, path string, body, out any) {
	t.Helper()

	status, raw := c.call(t, method, path, body)
	if status < 200 || status > 299 {
		t.Fatalf("%s %s: %d\n%s", method, path, status, truncate(raw))
	}
	decodeEnvelope(t, method, path, raw, out)
}

// DoRaw is Do for the handful of endpoints that answer their own type rather
// than the shared envelope, such as a query result.
func (c *Client) DoRaw(t *testing.T, method, path string, body, out any) {
	t.Helper()

	status, raw := c.call(t, method, path, body)
	if status < 200 || status > 299 {
		t.Fatalf("%s %s: %d\n%s", method, path, status, truncate(raw))
	}
	if out == nil {
		return
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("%s %s: decoding into %T: %v\n%s", method, path, out, err, truncate(raw))
	}
}

// Raw sends a body that is not JSON and returns the response as bytes, for the
// endpoints that trade in YAML documents or file uploads rather than objects.
func (c *Client) Raw(t *testing.T, method, path, contentType string, body []byte) []byte {
	t.Helper()

	status, raw := c.send(t, method, path, contentType, body)
	if status < 200 || status > 299 {
		t.Fatalf("%s %s: %d\n%s", method, path, status, truncate(raw))
	}
	return raw
}

// WithKey returns a copy of the client using a different credential, so a recipe
// can mint an ingest token and immediately send logs as that token rather than
// as the administrator who created it.
func (c *Client) WithKey(key string) *Client {
	next := *c
	next.Key = key
	return &next
}

// Status sends a request and returns the status code without failing, for the
// cases a recipe is asserting a refusal rather than a result.
func (c *Client) Status(t *testing.T, method, path string, body any) int {
	t.Helper()

	status, _ := c.call(t, method, path, body)
	return status
}

// Failure sends a request expecting an error, and returns the machine-readable
// code the envelope carries.
func (c *Client) Failure(t *testing.T, method, path string, body any) (int, api.ErrorCode) {
	t.Helper()

	status, raw := c.call(t, method, path, body)
	if status >= 200 && status <= 299 {
		t.Fatalf("%s %s: expected a failure, got %d", method, path, status)
	}
	var env api.Response[json.RawMessage]
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("%s %s: error body is not the standard envelope: %s", method, path, truncate(raw))
	}
	return status, env.Code
}

func (c *Client) call(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()

	var payload []byte
	contentType := ""
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding the %s %s body: %v", method, path, err)
		}
		payload, contentType = encoded, "application/json"
	}
	return c.send(t, method, path, contentType, payload)
}

// send makes one request, retrying while the server answers 429: that is
// backpressure asking the caller to wait, as any log shipper would, and not
// the answer a recipe is asserting.
func (c *Client) send(t *testing.T, method, path, contentType string, body []byte) (int, []byte) {
	t.Helper()

	backoff := 500 * time.Millisecond
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var payload io.Reader
		if body != nil {
			payload = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, c.BaseURL+"/api/v1"+path, payload)
		if err != nil {
			t.Fatalf("building %s %s: %v", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer "+c.Key)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if c.Scope != "" {
			req.Header.Set("X-Bifract-Scope", c.Scope)
		}

		res, err := c.http.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		raw, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatalf("reading the %s %s body: %v", method, path, err)
		}
		if res.StatusCode != http.StatusTooManyRequests || time.Now().After(deadline) {
			return res.StatusCode, raw
		}
		time.Sleep(backoff)
		backoff = min(2*backoff, 5*time.Second)
	}
}

// decodeEnvelope unwraps the standard response envelope. Asserting the shape
// here means every recipe also checks that the endpoint it touches still
// answers in the documented form.
func decodeEnvelope(t *testing.T, method, path string, raw []byte, out any) {
	t.Helper()

	if out == nil {
		return
	}
	var env api.Response[json.RawMessage]
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("%s %s: body is not the standard envelope: %s", method, path, truncate(raw))
	}
	if !env.Success {
		t.Fatalf("%s %s: answered success=false: %s", method, path, env.Error)
	}
	if len(env.Data) == 0 {
		t.Fatalf("%s %s: envelope carried no data", method, path)
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		t.Fatalf("%s %s: decoding data into %T: %v\n%s", method, path, out, err, truncate(env.Data))
	}
}

func truncate(b []byte) string {
	const max = 600
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + fmt.Sprintf("... (%d bytes)", len(b))
}

// Eventually retries until check passes or the deadline expires. Ingestion is
// asynchronous and batched, so a recipe that writes and reads back has to wait
// for the queue rather than assume. Deadlines are sized for production timings;
// polling often lets a stack tuned for tests finish in a fraction of them.
func Eventually(t *testing.T, what string, within time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

// newFractal creates a fractal for one recipe and deletes it afterwards, so
// recipes running in parallel never share what they write. It returns the
// fractal's id and a client scoped to it.
func newFractal(t *testing.T, c *Client, name string) (string, *Client) {
	t.Helper()

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-%s-%d", name, time.Now().UnixNano()),
		"description": "Created by the API test suite",
	}, &fractal)
	if fractal.ID == "" {
		t.Fatal("the created fractal came back without an id")
	}
	t.Cleanup(func() {
		if code := c.Status(t, "DELETE", "/fractals/"+fractal.ID, nil); code >= 300 {
			t.Logf("could not clean up fractal %s: %d", fractal.ID, code)
		}
	})
	return fractal.ID, c.InScope(fractal.ID)
}

// newShipper mints a JSON ingest token for a fractal and returns a client that
// sends logs as it.
func newShipper(t *testing.T, scoped *Client, fractalID string) *Client {
	t.Helper()

	var token struct {
		Token string `json:"token"`
	}
	scoped.Do(t, "POST", "/fractals/"+fractalID+"/ingest-tokens", map[string]any{
		"name": "api-suite", "parser_type": "json",
	}, &token)
	return scoped.InScope("").WithKey(token.Token)
}

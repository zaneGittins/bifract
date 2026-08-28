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
	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("reading the %s %s body: %v", method, path, err)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		t.Fatalf("%s %s: %d\n%s", method, path, res.StatusCode, truncate(raw))
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

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding the %s %s body: %v", method, path, err)
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, c.BaseURL+"/api/v1"+path, payload)
	if err != nil {
		t.Fatalf("building %s %s: %v", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Scope != "" {
		req.Header.Set("X-Bifract-Scope", c.Scope)
	}

	res, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("reading the %s %s body: %v", method, path, err)
	}
	return res.StatusCode, raw
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
// for the queue rather than assume.
func Eventually(t *testing.T, what string, within time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

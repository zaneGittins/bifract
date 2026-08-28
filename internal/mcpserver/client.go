package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// maxErrorBody caps how much of a failed response is quoted back. An HTML error
// page from a proxy in front of Bifract would otherwise fill the model's context.
const maxErrorBody = 400

// Client calls the Bifract API on behalf of the tools. Every failure becomes an
// error whose text is written to be read by a model rather than a stack trace.
type Client struct {
	cfg  Config
	http *http.Client

	// Answers that do not change for the life of the process. Tool calls can run
	// concurrently, so the caches are guarded.
	mu      sync.Mutex
	fractal string
	static  map[string]any
}

// NewClient builds the one client the process shares, so connections are pooled
// across tool calls.
func NewClient(cfg Config) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: &http.Transport{TLSClientConfig: cfg.TLS},
			// A redirect would drop the Authorization header or replay it at
			// another host; neither is something to do silently with a key.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Config exposes the resolved settings for tools that report them.
func (c *Client) Config() Config { return c.cfg }

// FractalID is the fractal this session acts in, for the few endpoints that name
// it in the path. Resolved once; a failure is not cached, so a transient outage
// does not disable every scoped tool until restart.
func (c *Client) FractalID(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fractal != "" {
		return c.fractal, nil
	}

	// /auth/user is exempt from the scope header, so it reports what the key
	// carries. That wins over BIFRACT_FRACTAL_ID, as it does on the server.
	bound, err := c.boundFractal(ctx)
	if err != nil {
		return "", err
	}
	if bound == "" {
		// An instance-wide key belongs to no fractal and names one per request.
		bound = c.cfg.FractalScope()
	}
	if bound == "" {
		return "", errors.New(
			"this session is not scoped to a single fractal, which this call needs. " +
				"Set BIFRACT_FRACTAL_ID to the one to act in (call list_fractals for the ids), " +
				"or use a key issued for that fractal")
	}
	c.fractal = bound
	return bound, nil
}

// boundFractal is the fractal the credential itself carries, empty for an
// instance-wide key or one issued for a prism.
func (c *Client) boundFractal(ctx context.Context) (string, error) {
	identity, err := c.Get(ctx, "/auth/user", nil)
	if err != nil {
		return "", err
	}
	user := identity
	if nested := field[map[string]any](identity, "user"); nested != nil {
		user = nested
	}
	return field[string](user, "selected_fractal"), nil
}

// Get calls path with optional query parameters.
func (c *Client) Get(ctx context.Context, path string, query url.Values) (any, error) {
	return c.Do(ctx, http.MethodGet, path, query, nil)
}

// Post sends body as JSON.
func (c *Client) Post(ctx context.Context, path string, body any) (any, error) {
	return c.Do(ctx, http.MethodPost, path, nil, body)
}

// Put sends body as JSON.
func (c *Client) Put(ctx context.Context, path string, body any) (any, error) {
	return c.Do(ctx, http.MethodPut, path, nil, body)
}

// Delete removes the resource at path.
func (c *Client) Delete(ctx context.Context, path string) (any, error) {
	return c.Do(ctx, http.MethodDelete, path, nil, nil)
}

// Static fetches a GET whose answer is fixed by the build, such as the embedded
// ATT&CK matrix, and remembers it. A tool that needs it on every call should not
// pay for it on every call.
func (c *Client) Static(ctx context.Context, path string) (any, error) {
	c.mu.Lock()
	cached, ok := c.static[path]
	c.mu.Unlock()
	if ok {
		return cached, nil
	}

	// Fetched outside the lock: a slow request must not block an unrelated one.
	payload, err := c.Get(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.static == nil {
		c.static = map[string]any{}
	}
	c.static[path] = payload
	return payload, nil
}

// Do performs the request and returns the payload the tools care about, with the
// transport envelope already removed.
func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body any) (any, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("could not encode the request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	target := c.cfg.APIBase() + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, fmt.Errorf("could not build the request for %s %s: %w", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	if c.cfg.Scope != "" {
		req.Header.Set("X-Bifract-Scope", c.cfg.Scope)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.transportError(method, path, err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("%s %s: the response could not be read: %w", method, path, err)
	}
	return decode(method, path, resp.StatusCode, payload)
}

func (c *Client) transportError(method, path string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
		return fmt.Errorf(
			"timed out after %s calling %s %s. Narrow the time range or raise BIFRACT_TIMEOUT",
			c.cfg.Timeout.Round(time.Second), method, path)
	}
	return fmt.Errorf(
		"cannot reach Bifract at %s: %w. Check BIFRACT_URL, and set BIFRACT_CA_CERT "+
			"(or BIFRACT_VERIFY_SSL=false) if the instance uses a certificate this machine does not trust",
		c.cfg.URL, err)
}

func isTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// decode turns a response into either the payload or an explanation.
func decode(method, path string, status int, payload []byte) (any, error) {
	switch {
	case status == http.StatusUnauthorized:
		return nil, errors.New("unauthorized. BIFRACT_API_KEY is invalid, disabled, or expired")
	case status == http.StatusForbidden:
		return nil, fmt.Errorf("forbidden: %s. The API key's permissions do not cover this", detail(payload))
	case status == http.StatusNotFound:
		return nil, fmt.Errorf("%s %s: not found. Check the id, and that it exists in this fractal", method, path)
	case status >= 400:
		return nil, fmt.Errorf("%s %s failed (%d): %s", method, path, status, detail(payload))
	}

	if len(bytes.TrimSpace(payload)) == 0 {
		return map[string]any{}, nil
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("%s %s returned a non-JSON response: %s", method, path, truncate(string(payload)))
	}

	// Belt and braces: failures answer 4xx now, but an endpoint that has not been
	// migrated to the envelope could still report one in the body.
	if envelope, ok := decoded.(map[string]any); ok {
		if success, present := envelope["success"].(bool); present && !success {
			if message := failure(envelope); message != "" {
				return nil, errors.New(message)
			}
			return nil, fmt.Errorf("%s %s was rejected by the server", method, path)
		}
	}
	return Unwrap(decoded), nil
}

// Unwrap drops the {"success", "data"} envelope, which spends the model's context
// teaching it nothing. Responses with their own shape pass through untouched.
//
// A truncated page keeps its counts: a model shown 100 of 4,000 alerts and told
// nothing will reason as though it saw all of them.
func Unwrap(payload any) any {
	envelope, ok := payload.(map[string]any)
	if !ok {
		return payload
	}
	if _, wrapped := envelope["success"]; !wrapped {
		return payload
	}

	data, present := envelope["data"]
	if !present {
		rest := make(map[string]any, len(envelope))
		for k, v := range envelope {
			if k != "success" {
				rest[k] = v
			}
		}
		return rest
	}

	items, isList := data.([]any)
	page, paged := envelope["page"].(map[string]any)
	if isList && paged {
		total, offset := number(page["total"]), number(page["offset"])
		if total > offset+len(items) {
			return map[string]any{
				"items":       items,
				"showing":     fmt.Sprintf("%d-%d of %d", offset+1, offset+len(items), total),
				"more":        true,
				"next_offset": offset + len(items),
				"note":        "This is one page. Ask for the next with offset, or narrow the request.",
			}
		}
	}
	return data
}

// failure is the message an error envelope carries, with its machine-readable
// code, which is what a caller should branch on.
func failure(envelope map[string]any) string {
	message, _ := envelope["error"].(string)
	if message == "" {
		message, _ = envelope["message"].(string)
	}
	code, _ := envelope["code"].(string)
	if code != "" && message != "" {
		return message + " [" + code + "]"
	}
	return message
}

// detail pulls the message out of a Bifract error body. Handlers answer with
// either an envelope or bare text, so try both.
func detail(payload []byte) string {
	text := strings.TrimSpace(string(payload))
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return truncate(text)
	}
	envelope, ok := decoded.(map[string]any)
	if !ok {
		return truncate(text)
	}
	if message := failure(envelope); message != "" {
		return truncate(message)
	}
	for _, key := range []string{"message", "detail"} {
		if value, _ := envelope[key].(string); value != "" {
			return truncate(value)
		}
	}
	return truncate(text)
}

func truncate(s string) string {
	if len(s) <= maxErrorBody {
		return s
	}
	return s[:maxErrorBody]
}

// number reads a JSON number, which always decodes as float64, as an int.
func number(v any) int {
	f, _ := v.(float64)
	return int(f)
}

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
	"sync"
	"time"

	"bifract/pkg/aitools"
)

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
	if nested := aitools.Field[map[string]any](identity, "user"); nested != nil {
		user = nested
	}
	return aitools.Field[string](user, "selected_fractal"), nil
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
	return aitools.Decode(method, path, resp.StatusCode, payload)
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

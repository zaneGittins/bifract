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

	"bifract/pkg/aitools"
)

// Client calls the Bifract API on behalf of the tools. Every failure becomes an
// error whose text is written to be read by a model rather than a stack trace.
type Client struct {
	cfg  Config
	http *http.Client
	// upload has no deadline of its own: BIFRACT_TIMEOUT bounds an interactive
	// call, while a bulk load costs whatever the body it carries costs, so the
	// caller's context is what bounds it. The transport is shared, so both
	// clients pool the same connections.
	upload *http.Client

	// Answers that do not change for the life of the process. Tool calls can run
	// concurrently, so the caches are guarded.
	mu     sync.Mutex
	static map[string]any
	// carried is the scope the credential itself has, and carriedKnown that it
	// has been looked up: no scope is an answer, so the two cannot be one field.
	carried      keyScope
	carriedKnown bool
}

// keyScope is what a credential is scoped to by itself, both empty for an
// instance-wide key.
type keyScope struct {
	fractal string
	prism   string
}

// NewClient builds the one client the process shares, so connections are pooled
// across tool calls.
func NewClient(cfg Config) *Client {
	transport := &http.Transport{TLSClientConfig: cfg.TLS}
	// A redirect would drop the Authorization header or replay it at another
	// host; neither is something to do silently with a key.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		cfg:    cfg,
		http:   &http.Client{Timeout: cfg.Timeout, Transport: transport, CheckRedirect: noRedirect},
		upload: &http.Client{Transport: transport, CheckRedirect: noRedirect},
	}
}

// Config exposes the resolved settings for tools that report them.
func (c *Client) Config() Config { return c.cfg }

// scopeOverrideKey carries a per-call scope on the context.
type scopeOverrideKey struct{}

// withScope makes every call on ctx act in scope ("fractal:<id>" or
// "prism:<id>") rather than in the session's configured one. Only a key that
// belongs to no scope of its own is redirected by it; the server ignores the
// header for a key issued for one scope, which is what stops a tool from
// reaching outside the credential it runs under.
func withScope(ctx context.Context, scope string) context.Context {
	return context.WithValue(ctx, scopeOverrideKey{}, scope)
}

// scopeOverride is the scope a call was redirected to, empty when it acts in the
// session's own.
func scopeOverride(ctx context.Context) string {
	override, _ := ctx.Value(scopeOverrideKey{}).(string)
	return override
}

// scope is the scope header this call sends.
func (c *Client) scope(ctx context.Context) string {
	if override := scopeOverride(ctx); override != "" {
		return override
	}
	return c.cfg.Scope
}

// ResolveScope settles which fractal a tool call acts in, and refuses one that
// cannot be settled. An instance-wide key belongs to no fractal, so a call that
// names none would be answered in whichever one the server falls back to: a
// watchlist or an alert that lands there reads as a success and watches nothing.
func (c *Client) ResolveScope(ctx context.Context, fractalID string) (context.Context, error) {
	if id := strings.TrimSpace(fractalID); id != "" {
		if !validScopeID(id) {
			return nil, fmt.Errorf("%q is not a fractal id. Call list_fractals for the ids", id)
		}
		return withScope(ctx, "fractal:"+id), nil
	}
	if c.cfg.Scope != "" {
		return ctx, nil
	}
	carried, err := c.credential(ctx)
	if err != nil {
		return nil, err
	}
	if carried.fractal == "" && carried.prism == "" {
		return nil, errors.New(
			"this key is instance-wide and belongs to no fractal, so a call that names none would " +
				"be answered in whichever fractal the server falls back to. Pass fractal_id " +
				"(call list_fractals for the ids), or set BIFRACT_FRACTAL_ID for the session")
	}
	return ctx, nil
}

// credential is the scope the key itself carries. Resolved once: it cannot
// change for the life of the process, and the scope guard runs before every
// tool call. A failure is not cached, so a transient outage does not disable
// every tool until restart.
func (c *Client) credential(ctx context.Context) (keyScope, error) {
	c.mu.Lock()
	if c.carriedKnown {
		defer c.mu.Unlock()
		return c.carried, nil
	}
	c.mu.Unlock()

	// Fetched outside the lock, so a slow request does not block an unrelated
	// call. /auth/user is exempt from the scope header, so it reports what the
	// key carries rather than what the session asked for.
	payload, err := c.Get(ctx, "/auth/user", nil)
	if err != nil {
		return keyScope{}, err
	}
	user := payload
	if nested := aitools.Field[map[string]any](payload, "user"); nested != nil {
		user = nested
	}
	carried := keyScope{
		fractal: aitools.Field[string](user, "selected_fractal"),
		prism:   aitools.Field[string](user, "selected_prism"),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.carried, c.carriedKnown = carried, true
	return carried, nil
}

// FractalID is the fractal this session acts in, for the few endpoints that name
// it in the path.
func (c *Client) FractalID(ctx context.Context) (string, error) {
	// A call redirected to another scope answers for that one, and must not be
	// remembered as the session's.
	if id, ok := strings.CutPrefix(scopeOverride(ctx), "fractal:"); ok {
		return id, nil
	}

	// What the key carries wins over BIFRACT_FRACTAL_ID, as it does on the server.
	carried, err := c.credential(ctx)
	if err != nil {
		return "", err
	}
	if carried.fractal != "" {
		return carried.fractal, nil
	}
	// An instance-wide key belongs to no fractal and names one per request.
	if id := c.cfg.FractalScope(); id != "" {
		return id, nil
	}
	return "", errors.New(
		"this session is not scoped to a single fractal, which this call needs. " +
			"Set BIFRACT_FRACTAL_ID to the one to act in (call list_fractals for the ids), " +
			"or use a key issued for that fractal")
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
	if scope := c.scope(ctx); scope != "" {
		req.Header.Set("X-Bifract-Scope", scope)
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

// Upload posts a body the endpoint reads as a file rather than as JSON. body is
// sent verbatim under contentType, gzip-encoded when compressed is set, and the
// deadline comes from ctx.
func (c *Client) Upload(ctx context.Context, path string, query url.Values, contentType string, compressed bool, body []byte) (any, error) {
	target := c.cfg.APIBase() + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("could not build the upload request for %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	if scope := c.scope(ctx); scope != "" {
		req.Header.Set("X-Bifract-Scope", scope)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", contentType)
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	// Set so the server sees a length rather than a chunked body it has to read
	// to the end before it can reject an oversized one.
	req.ContentLength = int64(len(body))

	resp, err := c.upload.Do(req)
	if err != nil {
		return nil, c.transportError(http.MethodPost, path, err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("POST %s: the response could not be read: %w", path, err)
	}
	return aitools.Decode(http.MethodPost, path, resp.StatusCode, payload)
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

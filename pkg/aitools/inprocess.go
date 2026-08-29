package aitools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"bifract/pkg/api"
)

// credentialHeaders are forwarded so the sub-request presents exactly what the
// originating request presented, whichever scheme authenticated it, and the
// router resolves the same principal again. Nothing else is copied: a synthetic
// request must not inherit an encoding or a content type describing a body it
// does not have.
var credentialHeaders = []string{"Cookie", "Authorization", "X-API-Key", "X-Bifract-Scope"}

// InProcess dispatches tool calls through the server's own router on behalf of
// the user whose request is being served. No credential of its own exists: the
// caller's cookie is replayed, so every guard, RBAC check and scope resolution
// that runs for a browser request runs here unchanged and a tool can never
// reach past what that user signed in as.
//
// The ceiling narrows it further. Even a user who could write is capped at what
// the tool declared it needs, so a tool body that reaches for a route it never
// declared is refused by the router rather than by review.
type InProcess struct {
	handler http.Handler
	origin  *http.Request
	ceiling api.Access
	fractal string
}

// NewInProcess builds a client that runs tool calls as the caller of origin,
// capped at ceiling, acting in fractal.
func NewInProcess(handler http.Handler, origin *http.Request, ceiling api.Access, fractal string) *InProcess {
	return &InProcess{handler: handler, origin: origin, ceiling: ceiling, fractal: fractal}
}

func (p *InProcess) Get(ctx context.Context, path string, query url.Values) (any, error) {
	return p.Do(ctx, http.MethodGet, path, query, nil)
}

func (p *InProcess) Post(ctx context.Context, path string, body any) (any, error) {
	return p.Do(ctx, http.MethodPost, path, nil, body)
}

func (p *InProcess) Put(ctx context.Context, path string, body any) (any, error) {
	return p.Do(ctx, http.MethodPut, path, nil, body)
}

func (p *InProcess) Delete(ctx context.Context, path string) (any, error) {
	return p.Do(ctx, http.MethodDelete, path, nil, nil)
}

// Static has nothing to cache in process: the handler it would call is already
// local, so a second call costs a function call rather than a round trip.
func (p *InProcess) Static(ctx context.Context, path string) (any, error) {
	return p.Get(ctx, path, nil)
}

// FractalID is the fractal the conversation is scoped to.
func (p *InProcess) FractalID(context.Context) (string, error) {
	if p.fractal == "" {
		return "", errors.New(
			"this conversation is scoped to a prism, not a single fractal, which this tool needs. " +
				"Switch to a fractal to use it")
	}
	return p.fractal, nil
}

// Do builds the request the browser would have sent and serves it through the
// router.
func (p *InProcess) Do(ctx context.Context, method, path string, query url.Values, body any) (any, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("could not encode the request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	target := APIPrefix + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	// Built on a context that carries cancellation but none of the originating
	// request's values: authorization must be resolved again from the forwarded
	// cookie, never inherited, so a session that has since expired cannot be
	// answered out of a context this call assembled.
	requestCtx, cancel := detach(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(api.WithCeiling(requestCtx, p.ceiling), method, target, reader)
	if err != nil {
		return nil, fmt.Errorf("could not build the request for %s %s: %w", method, path, err)
	}
	for _, name := range credentialHeaders {
		if value := p.origin.Header.Get(name); value != "" {
			req.Header.Set(name, value)
		}
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// Kept so rate limiting, audit and anything else that reasons about the
	// caller sees the real client rather than the server talking to itself.
	req.RemoteAddr = p.origin.RemoteAddr
	req.Host = p.origin.Host
	req.TLS = p.origin.TLS

	rec := &recorder{status: http.StatusOK}
	p.handler.ServeHTTP(rec, req)
	if rec.truncated {
		return nil, fmt.Errorf("%s %s answered with more than %d bytes, which is too much to work with. Narrow the request",
			method, path, maxResponseBody)
	}
	return Decode(method, path, rec.status, rec.body.Bytes())
}

// detach returns a context that cancels with parent but carries none of its
// values.
func detach(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := context.AfterFunc(parent, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// maxResponseBody bounds what one tool call may buffer, matching the cap the
// networked client gets from its own reader. A handler that ignores its limits
// must not be able to exhaust the server's memory through a tool.
const maxResponseBody = 64 << 20

// recorder collects a handler's response. Only the status and the body are
// wanted; a tool has no use for headers.
type recorder struct {
	status    int
	body      bytes.Buffer
	written   bool
	truncated bool
	header    http.Header
}

func (r *recorder) Header() http.Header {
	if r.header == nil {
		r.header = http.Header{}
	}
	return r.header
}

func (r *recorder) WriteHeader(status int) {
	if !r.written {
		r.status = status
		r.written = true
	}
}

func (r *recorder) Write(p []byte) (int, error) {
	r.written = true
	if r.body.Len()+len(p) > maxResponseBody {
		r.truncated = true
		return len(p), nil
	}
	return r.body.Write(p)
}

// Flush is a no-op that exists so a handler which streams its response reaches
// this writer instead of failing a type assertion on http.Flusher.
func (r *recorder) Flush() {}

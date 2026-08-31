package aitools

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
)

// recorded is the shape of the last request a fake client was given.
type recorded struct {
	Method string
	Path   string
}

// fakeClient answers every call with one payload and keeps the last request, so
// a tool test asserts on what the tool actually sent. Responses run through
// Decode, so envelope handling matches the real client exactly.
type fakeClient struct {
	status  int
	body    string
	fractal string
	// byPath overrides body for one path, for tools that call more than one
	// endpoint. A path with no entry falls back to body.
	byPath map[string]string

	last  recorded
	sent  []byte
	calls int
	// seen records every request in order, for tools whose behaviour is which
	// calls they make rather than what one call carried.
	seen []recorded
}

func serve(t *testing.T, status int, body string) *fakeClient {
	t.Helper()
	return &fakeClient{status: status, body: body}
}

func (f *fakeClient) do(method, path string, body any) (any, error) {
	f.calls++
	f.last = recorded{Method: method, Path: path}
	f.seen = append(f.seen, f.last)
	f.sent = nil
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		f.sent = encoded
	}
	payload := f.body
	if override, ok := f.byPath[path]; ok {
		payload = override
	}
	return Decode(method, path, f.status, []byte(payload))
}

func (f *fakeClient) Get(_ context.Context, path string, query url.Values) (any, error) {
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	return f.do("GET", path, nil)
}

func (f *fakeClient) Post(_ context.Context, path string, body any) (any, error) {
	return f.do("POST", path, body)
}

func (f *fakeClient) Put(_ context.Context, path string, body any) (any, error) {
	return f.do("PUT", path, body)
}

func (f *fakeClient) Delete(_ context.Context, path string) (any, error) {
	return f.do("DELETE", path, nil)
}

func (f *fakeClient) Static(ctx context.Context, path string) (any, error) {
	return f.Get(ctx, path, nil)
}

func (f *fakeClient) FractalID(ctx context.Context) (string, error) {
	if f.fractal != "" {
		return f.fractal, nil
	}
	identity, err := f.Get(ctx, "/auth/user", nil)
	if err != nil {
		return "", err
	}
	user := identity
	if nested := Field[map[string]any](identity, "user"); nested != nil {
		user = nested
	}
	if f.fractal = Field[string](user, "selected_fractal"); f.fractal == "" {
		return "", errors.New("this session is not scoped to a single fractal")
	}
	return f.fractal, nil
}

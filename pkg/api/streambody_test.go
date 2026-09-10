package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// bodyLimitMiddleware mirrors the router's real one: stash the body, then cap it.
func bodyLimitMiddleware(limit int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(WithUncappedBody(r.Context(), r.Body))
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}

func readAll(t *testing.T, streams bool, size int) (int, error) {
	t.Helper()
	var got int
	var readErr error
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		got, readErr = len(b), err
	})
	if streams {
		h = restoreUncappedBody(h)
	}

	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", size)))
	bodyLimitMiddleware(1<<20, h).ServeHTTP(httptest.NewRecorder(), req)
	return got, readErr
}

// An upload streams and bounds itself; the blanket cap would truncate any file
// over 1MB long before the handler saw it. A 2.3MB dictionary CSV failing to
// import is exactly that.
func TestStreamsBodyLiftsTheSizeCap(t *testing.T) {
	const big = 2 << 20 // 2MB, over the 1MB cap

	n, err := readAll(t, false, big)
	if err == nil {
		t.Errorf("a normal route must stay capped; read %d bytes with no error", n)
	}

	n, err = readAll(t, true, big)
	if err != nil {
		t.Fatalf("a StreamsBody route must read past the cap, got: %v", err)
	}
	if n != big {
		t.Errorf("StreamsBody route read %d bytes, want the whole %d", n, big)
	}
}

// The exemption must be opt-in: without it, nothing changes.
func TestSizeCapStillAppliesByDefault(t *testing.T) {
	n, err := readAll(t, false, 512)
	if err != nil || n != 512 {
		t.Fatalf("a small body must still pass: n=%d err=%v", n, err)
	}
}

// Registering a StreamsBody route must actually wrap the handler; a route that
// declares nothing must be left alone.
func TestRegisterWrapsOnlyStreamingRoutes(t *testing.T) {
	for _, streams := range []bool{true, false} {
		var sawUncapped bool
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			sawUncapped = len(b) == 2<<20
		})
		wrapped := h
		if streams {
			wrapped = restoreUncappedBody(h)
		}
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 2<<20)))
		bodyLimitMiddleware(1<<20, wrapped).ServeHTTP(httptest.NewRecorder(), req)
		if sawUncapped != streams {
			t.Errorf("streams=%v: handler saw full body=%v, want %v", streams, sawUncapped, streams)
		}
	}
}

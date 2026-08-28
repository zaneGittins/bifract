package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testPage = `<!DOCTYPE html>
<html>
<head>
    <link rel="icon" href="/static/favicon.svg">
    <link rel="stylesheet" href="/static/css/01-base.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.min.css" crossorigin="" />
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body><img src="/static/logo.png"><script src="/static/app.js"></script></body>
</html>`

func newTestAssets(t *testing.T) (*staticAssets, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "static", "css"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("index.html", testPage)
	write("static/app.js", "console.log(1)")
	write("static/css/01-base.css", "body{}")
	return newStaticAssets(root, "v1.2.3"), root
}

func get(h http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestStaticAssetsStampsLocalAssetURLs(t *testing.T) {
	s, _ := newTestAssets(t)
	body := get(s, "/").Body.String()

	for _, want := range []string{
		`href="/static/favicon.svg?v=v1.2.3"`,
		`href="/static/css/01-base.css?v=v1.2.3"`,
		`src="/static/logo.png?v=v1.2.3"`,
		`src="/static/app.js?v=v1.2.3"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing stamped asset %s", want)
		}
	}

	// Absolute URLs belong to a third party and must not be rewritten.
	for _, want := range []string{
		`href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.min.css"`,
		`src="https://cdn.jsdelivr.net/npm/chart.js"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("rewrote an external URL, expected %s intact", want)
		}
	}
	if strings.Contains(body, "cdn.jsdelivr.net") && strings.Contains(body, "jsdelivr.net/npm/chart.js?v=") {
		t.Error("external script was stamped")
	}
}

func TestStaticAssetsCacheHeaders(t *testing.T) {
	s, _ := newTestAssets(t)

	// The HTML carries the token, so it must always be revalidated.
	page := get(s, "/")
	if got := page.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("index Cache-Control = %q, want no-cache", got)
	}
	if page.Header().Get("ETag") == "" {
		t.Error("index served without an ETag, so it cannot answer 304")
	}

	// A build-scoped asset URL can never go stale, so it is cached for a year.
	if got := get(s, "/static/app.js?v=v1.2.3").Header().Get("Cache-Control"); got != immutableCache {
		t.Errorf("versioned asset Cache-Control = %q, want %q", got, immutableCache)
	}

	// A bare or stale URL falls back to revalidating, matching prior behaviour.
	for _, target := range []string{"/static/app.js", "/static/app.js?v=v1.0.0"} {
		if got := get(s, target).Header().Get("Cache-Control"); got != "no-cache" {
			t.Errorf("%s Cache-Control = %q, want no-cache", target, got)
		}
	}
}

func TestStaticAssetsPageRevalidates(t *testing.T) {
	s, _ := newTestAssets(t)
	etag := get(s, "/").Header().Get("ETag")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("If-None-Match", etag)
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotModified {
		t.Errorf("unchanged build returned %d, want 304", rec.Code)
	}
}

func TestStaticAssetsRerendersOnEdit(t *testing.T) {
	s, root := newTestAssets(t)
	first := get(s, "/").Body.String()

	edited := strings.Replace(testPage, "<body>", "<body><p>edited</p>", 1)
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	// Force a distinct mtime; the cache keys on it.
	later := time.Now().Add(time.Second)
	if err := os.Chtimes(filepath.Join(root, "index.html"), later, later); err != nil {
		t.Fatal(err)
	}

	second := get(s, "/").Body.String()
	if second == first || !strings.Contains(second, "edited") {
		t.Error("edited page served from cache")
	}
}

func TestStaticAssetsNoDirectoryListing(t *testing.T) {
	s, _ := newTestAssets(t)
	for _, target := range []string{"/static/", "/static/css/"} {
		if code := get(s, target).Code; code != http.StatusNotFound {
			t.Errorf("%s returned %d, want 404", target, code)
		}
	}
}

// TestStaticAssetsStampsRealIndex guards the actual shipped UI: every local
// asset it references must come out build-scoped, or that asset silently falls
// back to revalidating on every page load.
func TestStaticAssetsStampsRealIndex(t *testing.T) {
	raw, err := os.ReadFile("../../web/index.html")
	if err != nil {
		t.Skipf("web/index.html not readable: %v", err)
	}
	before := len(localAssetRef.FindAll(raw, -1))
	if before == 0 {
		t.Fatal("no local asset references found in web/index.html")
	}

	s := newStaticAssets("../../web", "v1.2.3")
	body := get(s, "/").Body.String()

	if after := strings.Count(body, "?v=v1.2.3"); after != before {
		t.Errorf("stamped %d of %d local assets", after, before)
	}
	if n := len(localAssetRef.FindAllString(body, -1)); n != 0 {
		t.Errorf("%d local asset references left unstamped", n)
	}
	t.Logf("stamped %d local assets in web/index.html", before)
}

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// noDirFS wraps an http.FileSystem and refuses to open directories. This makes
// http.FileServer return 404 for a directory path rather than rendering an
// index listing that enumerates the contents of ./web.
type noDirFS struct{ fs http.FileSystem }

func (n noDirFS) Open(name string) (http.File, error) {
	f, err := n.fs.Open(name)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.IsDir() {
		f.Close()
		return nil, fs.ErrNotExist
	}
	return f, nil
}

// localAssetRef matches a src or href pointing at an app-owned asset. Absolute
// URLs (CDN scripts) and paths that already carry a query are left alone.
var localAssetRef = regexp.MustCompile(`(src|href)="(/static/[^"?]*)"`)

// immutableCache is a year, the longest max-age browsers honour. It is safe
// only because the URL carries the build token: a new build changes every asset
// URL, so a cached entry can never be served for a newer build.
const immutableCache = "private, max-age=31536000, immutable"

// staticAssets serves the web UI, stamping every asset URL in served HTML with
// the build token so the asset itself can be cached immutably. index.html alone
// pulls ~90 subresources; revalidating each one on every page load made a view
// a burst of ~90 upstream requests, which is both slow over a remote link and
// fragile, since one failed request leaves the page half rendered.
type staticAssets struct {
	root       string
	token      string
	fileServer http.Handler

	mu    sync.RWMutex
	pages map[string]*renderedPage
}

type renderedPage struct {
	body    []byte
	etag    string
	modTime time.Time
}

func newStaticAssets(root, version string) *staticAssets {
	return &staticAssets{
		root:       root,
		token:      assetToken(version),
		fileServer: http.FileServer(noDirFS{http.Dir(root)}),
		pages:      make(map[string]*renderedPage),
	}
}

// assetToken is the cache key for a build. Release builds use the binary
// version; dev builds have no version to key on, so they fall back to process
// start, which changes on every rebuild since ./web ships inside the image.
func assetToken(version string) string {
	if version != "" && version != "dev" {
		return version
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func (s *staticAssets) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/":
		s.servePage(w, r, "index.html")
	// Pretty path for shared wallboards: /shared/<token> serves the standalone
	// public render page. The token is read client-side and sent to the
	// anonymous API, so no auth is involved here.
	case strings.HasPrefix(r.URL.Path, "/shared/"):
		s.servePage(w, r, "shared.html")
	case strings.HasSuffix(r.URL.Path, ".html"):
		s.servePage(w, r, strings.TrimPrefix(path.Clean(r.URL.Path), "/"))
	default:
		s.serveAsset(w, r)
	}
}

func (s *staticAssets) serveAsset(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("v") == s.token {
		w.Header().Set("Cache-Control", immutableCache)
	} else {
		// Reached without a current token: either a stale URL from a previous
		// build or a direct fetch. Revalidate rather than pin it.
		w.Header().Set("Cache-Control", "no-cache")
	}
	s.fileServer.ServeHTTP(w, r)
}

func (s *staticAssets) servePage(w http.ResponseWriter, r *http.Request, name string) {
	page, err := s.page(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// The HTML carries the build token, so it is the one response that must be
	// revalidated on every load. It answers 304 when the build has not changed.
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("ETag", page.etag)
	http.ServeContent(w, r, name, page.modTime, bytes.NewReader(page.body))
}

// page returns the rendered form of an HTML file, re-rendering when the file on
// disk changes so a bind-mounted ./web still picks up edits.
func (s *staticAssets) page(name string) (*renderedPage, error) {
	full := filepath.Join(s.root, filepath.FromSlash(name))
	st, err := os.Stat(full)
	if err != nil {
		return nil, err
	}
	if st.IsDir() {
		return nil, fs.ErrNotExist
	}

	s.mu.RLock()
	cached, ok := s.pages[name]
	s.mu.RUnlock()
	if ok && cached.modTime.Equal(st.ModTime()) {
		return cached, nil
	}

	raw, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	suffix := []byte("?v=" + url.QueryEscape(s.token) + `"`)
	body := localAssetRef.ReplaceAllFunc(raw, func(m []byte) []byte {
		out := make([]byte, 0, len(m)+len(suffix))
		out = append(out, m[:len(m)-1]...) // drop the closing quote
		return append(out, suffix...)
	})

	sum := sha256.Sum256(body)
	rendered := &renderedPage{
		body:    body,
		etag:    `"` + hex.EncodeToString(sum[:16]) + `"`,
		modTime: st.ModTime(),
	}
	s.mu.Lock()
	s.pages[name] = rendered
	s.mu.Unlock()
	return rendered, nil
}

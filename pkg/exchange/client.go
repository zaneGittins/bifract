package exchange

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"bifract/pkg/egress"
)

// DefaultIndexURL is the exchange kept in the Bifract repository.
const DefaultIndexURL = "https://raw.githubusercontent.com/zaneGittins/bifract/main/exchange/index.yaml"

const (
	// indexTTL is how long a fetched index, or a file from it, is reused.
	indexTTL = 10 * time.Minute
	// failedRetry is how soon a failed index fetch is tried again.
	failedRetry   = time.Minute
	fetchTimeout  = 30 * time.Second
	maxIndexBytes = 1 << 20
	// maxFileBytes matches the largest body the import endpoints accept.
	maxFileBytes = 1 << 20
	// maxCachedFiles bounds the file cache to a few hundred MiB at worst.
	maxCachedFiles = 256
)

// ErrDisabled reports an install with the exchange turned off.
var ErrDisabled = errors.New("the exchange is turned off on this server")

// Client fetches an exchange index and its files.
type Client struct {
	indexURL     string
	allowPrivate bool
	http         *http.Client
	now          func() time.Time

	mu sync.Mutex
	// cached is the last good index, served while a refresh runs or after one fails.
	cached *Index
	// nextCheck is when the index is next fetched: a TTL after a success, sooner
	// after a failure.
	nextCheck time.Time
	lastErr   error
	// refreshing is closed when the fetch in flight finishes; nil when none is.
	refreshing chan struct{}
	files      map[string]cachedFile
}

type cachedFile struct {
	content   string
	fetchedAt time.Time
}

// NewClient returns a client for the index at indexURL; an empty URL disables it.
// allowPrivate admits addresses inside the network, for an operator's own mirror.
func NewClient(indexURL string, allowPrivate bool) *Client {
	return &Client{
		indexURL:     indexURL,
		allowPrivate: allowPrivate,
		http:         egress.NewClient(egress.Options{AllowPrivate: allowPrivate, Timeout: fetchTimeout}),
		now:          time.Now,
		files:        map[string]cachedFile{},
	}
}

// AllowsPrivate reports whether this index, and what it points to, may be inside
// the network: true only for an operator's own mirror.
func (c *Client) AllowsPrivate() bool { return c.allowPrivate }

// Enabled reports whether an index URL is configured.
func (c *Client) Enabled() bool { return c.indexURL != "" }

// Source is a mirror's index URL as shown to users, without any query or
// credentials. The default index needs no mention, so it is empty for that.
func (c *Client) Source() string {
	if c.indexURL == DefaultIndexURL {
		return ""
	}
	return redact(c.indexURL)
}

// Index returns the index. A stale copy is served at once while one shared fetch
// refreshes it; without a copy, callers wait on that fetch.
func (c *Client) Index(ctx context.Context) (*Index, error) {
	if !c.Enabled() {
		return nil, ErrDisabled
	}
	c.mu.Lock()
	if c.now().Before(c.nextCheck) {
		idx, err := c.cached, c.lastErr
		c.mu.Unlock()
		if idx != nil {
			return idx, nil
		}
		return nil, err
	}
	if c.refreshing == nil {
		c.refreshing = make(chan struct{})
		go c.refresh(c.refreshing)
	}
	done, stale := c.refreshing, c.cached
	c.mu.Unlock()
	if stale != nil {
		return stale, nil
	}

	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached != nil {
		return c.cached, nil
	}
	return nil, c.lastErr
}

// refresh fetches the index on its own context, so it finishes for later callers
// even if the request that started it goes away.
func (c *Client) refresh(done chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()
	data, err := c.fetch(ctx, c.indexURL, maxIndexBytes)
	var idx *Index
	if err == nil {
		idx, err = ReadIndex(data)
	}

	c.mu.Lock()
	if err == nil {
		c.cached, c.lastErr = idx, nil
		c.nextCheck = c.now().Add(indexTTL)
	} else {
		log.Printf("[Exchange] Refreshing the index failed: %v", err)
		c.lastErr = err
		c.nextCheck = c.now().Add(failedRetry)
	}
	c.refreshing = nil
	c.mu.Unlock()
	close(done)
}

// waitRefresh blocks until no fetch is in flight. Tests use it to observe one.
func (c *Client) waitRefresh() {
	c.mu.Lock()
	done := c.refreshing
	c.mu.Unlock()
	if done != nil {
		<-done
	}
}

// Content returns an item's file, fetched relative to the index.
func (c *Client) Content(ctx context.Context, it *Item) (string, error) {
	if !c.Enabled() {
		return "", ErrDisabled
	}
	if err := validPath(it.Path); err != nil {
		return "", err
	}
	c.mu.Lock()
	f, ok := c.files[it.Path]
	c.mu.Unlock()
	if ok && c.now().Sub(f.fetchedAt) < indexTTL {
		return f.content, nil
	}

	base, err := url.Parse(c.indexURL)
	if err != nil {
		return "", err
	}
	// Files share the index's query, which may carry a mirror's access token.
	ref := base.ResolveReference(&url.URL{Path: it.Path})
	ref.RawQuery = base.RawQuery
	data, err := c.fetch(ctx, ref.String(), maxFileBytes)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	if len(c.files) >= maxCachedFiles {
		c.files = map[string]cachedFile{}
	}
	c.files[it.Path] = cachedFile{content: string(data), fetchedAt: c.now()}
	c.mu.Unlock()
	return string(data), nil
}

func (c *Client) fetch(ctx context.Context, rawURL string, limit int64) ([]byte, error) {
	shown := redact(rawURL)
	resp, err := egress.Get(ctx, c.http, rawURL)
	if err != nil {
		return nil, fmt.Errorf("fetching %s failed: %s", shown, egress.Describe(err))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s failed: %s", shown, egress.Describe(err))
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", shown, limit)
	}
	return data, nil
}

// redact drops the query, fragment and credentials, where an operator may keep a
// token for a private mirror.
func redact(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return strings.SplitN(rawURL, "?", 2)[0]
	}
	u.RawQuery, u.Fragment, u.User = "", "", nil
	return u.String()
}

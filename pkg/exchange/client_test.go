package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const serverIndex = `
items:
  - id: sysmon
    kind: normalizer
    name: Sysmon
    path: normalizers/sysmon.yaml
  - id: big
    kind: policy
    name: Big
    path: policies/big.yaml
  - id: rmm-search
    kind: search
    name: RMM tools run
    path: searches/rmm.yaml
`

type fakeExchange struct {
	srv       *httptest.Server
	indexHits atomic.Int32
	fileHits  atomic.Int32
	fail      atomic.Bool
	// gate, when set, holds index requests until it is closed.
	gate chan struct{}
}

// exchangeServer serves an index under /repo/exchange/ and counts requests.
func exchangeServer(t *testing.T) *fakeExchange {
	t.Helper()
	f := &fakeExchange{}
	mux := http.NewServeMux()
	mux.HandleFunc("/repo/exchange/index.yaml", func(w http.ResponseWriter, r *http.Request) {
		f.indexHits.Add(1)
		if f.gate != nil {
			<-f.gate
		}
		if f.fail.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(serverIndex))
	})
	mux.HandleFunc("/repo/exchange/normalizers/sysmon.yaml", func(w http.ResponseWriter, r *http.Request) {
		f.fileHits.Add(1)
		w.Write([]byte("name: Sysmon\n"))
	})
	mux.HandleFunc("/repo/exchange/searches/rmm.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("name: RMM tools run\nquery: event_id=1\n"))
	})
	mux.HandleFunc("/repo/exchange/policies/big.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", maxFileBytes+1)))
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeExchange) client() *Client {
	return NewClient(f.srv.URL+"/repo/exchange/index.yaml", true)
}

// fakeClock lets a test move time without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func withClock(c *Client) *fakeClock {
	clock := &fakeClock{t: time.Now()}
	c.now = clock.now
	return clock
}

func TestClientCachesIndex(t *testing.T) {
	f := exchangeServer(t)
	c := f.client()
	clock := withClock(c)

	for range 3 {
		idx, err := c.Index(context.Background())
		if err != nil || len(idx.Items) != 3 {
			t.Fatalf("Index: %v, %v", idx, err)
		}
	}
	if f.indexHits.Load() != 1 {
		t.Errorf("index fetched %d times, want 1", f.indexHits.Load())
	}

	clock.add(indexTTL + time.Second)
	if _, err := c.Index(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.waitRefresh()
	if f.indexHits.Load() != 2 {
		t.Errorf("index fetched %d times after the TTL, want 2", f.indexHits.Load())
	}
}

// A slow upstream must not hold callers up while there is a copy to serve.
func TestClientServesStaleCopyDuringRefresh(t *testing.T) {
	f := exchangeServer(t)
	c := f.client()
	clock := withClock(c)
	if _, err := c.Index(context.Background()); err != nil {
		t.Fatal(err)
	}

	f.gate = make(chan struct{})
	defer close(f.gate)
	clock.add(indexTTL + time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 5 {
			if idx, err := c.Index(context.Background()); err != nil || len(idx.Items) != 3 {
				t.Errorf("Index during refresh: %v, %v", idx, err)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("callers waited on the refresh instead of getting the cached copy")
	}
}

func TestClientCoalescesFirstFetch(t *testing.T) {
	f := exchangeServer(t)
	f.gate = make(chan struct{})
	c := f.client()

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Index(context.Background()); err != nil {
				t.Errorf("Index: %v", err)
			}
		}()
	}
	// Every caller is either waiting on the one fetch or about to join it.
	for f.indexHits.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(f.gate)
	wg.Wait()
	if f.indexHits.Load() != 1 {
		t.Errorf("index fetched %d times for concurrent callers, want 1", f.indexHits.Load())
	}
}

// The shared fetch must not die with the request that happened to start it.
func TestClientRefreshOutlivesCaller(t *testing.T) {
	f := exchangeServer(t)
	f.gate = make(chan struct{})
	c := f.client()

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := c.Index(ctx); errc <- err }()
	for f.indexHits.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("a cancelled caller should get its context error")
	}
	close(f.gate)
	c.waitRefresh()
	if idx, err := c.Index(context.Background()); err != nil || len(idx.Items) != 3 {
		t.Fatalf("the fetch should have finished for the next caller: %v, %v", idx, err)
	}
	if f.indexHits.Load() != 1 {
		t.Errorf("index fetched %d times, want 1", f.indexHits.Load())
	}
}

func TestClientKeepsLastCopyAndBacksOff(t *testing.T) {
	f := exchangeServer(t)
	c := f.client()
	clock := withClock(c)
	if _, err := c.Index(context.Background()); err != nil {
		t.Fatal(err)
	}

	f.fail.Store(true)
	clock.add(indexTTL + time.Second)
	c.Index(context.Background())
	c.waitRefresh()
	for range 3 {
		idx, err := c.Index(context.Background())
		if err != nil || len(idx.Items) != 3 {
			t.Fatalf("expected the last good index, got %v, %v", idx, err)
		}
	}
	if f.indexHits.Load() != 2 {
		t.Errorf("a failed refresh was retried at once: %d fetches, want 2", f.indexHits.Load())
	}

	clock.add(failedRetry + time.Second)
	c.Index(context.Background())
	c.waitRefresh()
	if f.indexHits.Load() != 3 {
		t.Errorf("no retry after the back-off: %d fetches, want 3", f.indexHits.Load())
	}
}

func TestClientBacksOffWithoutCopy(t *testing.T) {
	f := exchangeServer(t)
	f.fail.Store(true)
	c := f.client()
	clock := withClock(c)

	for range 3 {
		if _, err := c.Index(context.Background()); err == nil {
			t.Fatal("expected an error with no copy to serve")
		}
	}
	if f.indexHits.Load() != 1 {
		t.Errorf("index fetched %d times inside the back-off, want 1", f.indexHits.Load())
	}
	clock.add(failedRetry + time.Second)
	f.fail.Store(false)
	if _, err := c.Index(context.Background()); err != nil {
		t.Fatalf("expected a retry after the back-off: %v", err)
	}
}

func TestClientContent(t *testing.T) {
	f := exchangeServer(t)
	c := f.client()
	idx, err := c.Index(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		got, err := c.Content(context.Background(), idx.Find("sysmon"))
		if err != nil || got != "name: Sysmon\n" {
			t.Fatalf("Content = %q, %v", got, err)
		}
	}
	if f.fileHits.Load() != 1 {
		t.Errorf("file fetched %d times, want 1", f.fileHits.Load())
	}

	if _, err := c.Content(context.Background(), idx.Find("big")); err == nil || !strings.Contains(err.Error(), "larger") {
		t.Errorf("an oversized file should be refused, got %v", err)
	}
}

// An operator may put a token in a private mirror's URL; viewers must not see it.
func TestClientHidesURLQuery(t *testing.T) {
	f := exchangeServer(t)
	f.fail.Store(true)
	c := NewClient(f.srv.URL+"/repo/exchange/index.yaml?token=s3cret", true)

	if strings.Contains(c.Source(), "s3cret") || !strings.HasSuffix(c.Source(), "/repo/exchange/index.yaml") {
		t.Errorf("Source() = %q", c.Source())
	}
	if _, err := c.Index(context.Background()); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error should hide the query: %v", err)
	}
}

func TestClientDisabled(t *testing.T) {
	c := NewClient("", false)
	if c.Enabled() {
		t.Fatal("an empty URL should disable the exchange")
	}
	if _, err := c.Index(context.Background()); err == nil {
		t.Error("Index on a disabled exchange should fail")
	}
}

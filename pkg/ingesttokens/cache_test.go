package ingesttokens

import (
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/pgnotify"
)

func TestTokenCache(t *testing.T) {
	c := NewTokenCache(time.Hour)
	tok := &ValidatedToken{TokenID: "t1", FractalID: fractalA}

	if got, ok := c.Get("h1"); ok || got != nil {
		t.Fatalf("empty cache hit: %v", got)
	}
	c.Set("h1", tok, c.Generation())
	if got, ok := c.Get("h1"); !ok || got != tok {
		t.Fatalf("Get after Set = (%v, %v)", got, ok)
	}
	if _, ok := c.Get("h2"); ok {
		t.Fatal("hit for a hash that was never set")
	}

	// Invalidation is how a revoked or re-scoped token stops being honoured
	// before its TTL runs out.
	c.InvalidateAll()
	if _, ok := c.Get("h1"); ok {
		t.Fatal("hit after InvalidateAll")
	}
}

func TestTokenCacheExpiry(t *testing.T) {
	// A non-positive TTL stores entries that are already stale.
	c := NewTokenCache(-time.Second)
	c.Set("h1", &ValidatedToken{TokenID: "t1"}, c.Generation())
	if _, ok := c.Get("h1"); ok {
		t.Fatal("expired entry served")
	}
}

// A lookup that began before an invalidation read the old grant, so its result
// must not be cached after it.
func TestTokenCacheDropsLookupsThatStraddleInvalidation(t *testing.T) {
	c := NewTokenCache(time.Hour)
	gen := c.Generation()
	c.InvalidateAll()
	c.Set("h1", &ValidatedToken{TokenID: "t1"}, gen)
	if _, ok := c.Get("h1"); ok {
		t.Fatal("a lookup from before the invalidation was cached")
	}
	c.Set("h1", &ValidatedToken{TokenID: "t1"}, c.Generation())
	if _, ok := c.Get("h1"); !ok {
		t.Fatal("a current lookup was not cached")
	}
}

// Every process keeps its own cache, so invalidating one tells the rest, and a
// notice from another process clears this one.
func TestTokenCacheSharesInvalidation(t *testing.T) {
	pg := fakedb.NewPG()
	pg.On("pg_notify", fakedb.Row(""))
	bus := pgnotify.New(pg.DB())
	c := NewTokenCache(time.Hour)
	c.Watch(bus)

	c.InvalidateAll()
	if n := len(pg.Matching("pg_notify")); n != 1 {
		t.Fatalf("%d notices published, want 1", n)
	}

	c.Set("h1", &ValidatedToken{TokenID: "t1"}, c.Generation())
	// What the listener does when another process announces a change.
	bus.Deliver(pgnotify.TopicIngestTokens)
	if _, ok := c.Get("h1"); ok {
		t.Fatal("a notice from another process left the token cached")
	}
	if n := len(pg.Matching("pg_notify")); n != 1 {
		t.Errorf("receiving a notice published again (%d notices)", n)
	}
}

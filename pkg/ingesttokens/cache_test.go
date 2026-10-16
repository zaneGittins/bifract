package ingesttokens

import (
	"testing"
	"time"
)

func TestTokenCache(t *testing.T) {
	c := NewTokenCache(time.Hour)
	tok := &ValidatedToken{TokenID: "t1", FractalID: fractalA}

	if got, ok := c.Get("h1"); ok || got != nil {
		t.Fatalf("empty cache hit: %v", got)
	}
	c.Set("h1", tok)
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
	c.Set("h1", &ValidatedToken{TokenID: "t1"})
	if _, ok := c.Get("h1"); ok {
		t.Fatal("expired entry served")
	}
}

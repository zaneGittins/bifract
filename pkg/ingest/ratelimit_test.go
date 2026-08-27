package ingest

import (
	"testing"
	"time"
)

// A live rate change must apply to the buckets already handed out, or a lowered
// limit would not bind until the holder went idle long enough to be evicted.
func TestSetRateAppliesToExistingBuckets(t *testing.T) {
	rl := NewRateLimiter(1000, 1000)
	if !rl.Allow("k") {
		t.Fatal("first request should pass")
	}

	rl.SetRate(1, 1)
	rl.Allow("k")
	if rl.Allow("k") {
		t.Fatal("existing bucket kept the old burst after SetRate")
	}

	// Raising the limit refills at the new rate rather than instantly.
	rl.SetRate(1000, 1000)
	time.Sleep(5 * time.Millisecond)
	if !rl.Allow("k") {
		t.Fatal("bucket did not refill at the raised rate")
	}
}

// Re-rating to the same values must not disturb the buckets in flight.
func TestSetRateNoopKeepsTokens(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	if !rl.Allow("k") {
		t.Fatal("first request should pass")
	}
	rl.SetRate(1, 1)
	if rl.Allow("k") {
		t.Fatal("a no-op SetRate must not refill the bucket")
	}
}

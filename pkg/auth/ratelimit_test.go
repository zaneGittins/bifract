package auth

import (
	"testing"
	"time"
)

func TestLoginRateLimiterBlocksAfterMaxFailures(t *testing.T) {
	rl := newLoginRateLimiter()
	const ip = "203.0.113.7"

	if rl.check(ip) {
		t.Fatal("an address with no history is blocked")
	}
	for i := 1; i < loginMaxFailures; i++ {
		if rl.recordFailure(ip) {
			t.Fatalf("blocked after %d failures, want %d", i, loginMaxFailures)
		}
		if rl.check(ip) {
			t.Fatalf("check reports blocked after %d failures", i)
		}
	}
	if !rl.recordFailure(ip) {
		t.Fatalf("not blocked after %d failures", loginMaxFailures)
	}
	if !rl.check(ip) {
		t.Fatal("check does not report the block")
	}

	// The block is per address: nobody else is collateral.
	if rl.check("203.0.113.8") {
		t.Error("an unrelated address is blocked")
	}

	a := rl.attempts[ip]
	if d := time.Until(a.blockedUntil); d <= loginBlockDuration-time.Minute || d > loginBlockDuration {
		t.Errorf("block lasts %v, want about %v", d, loginBlockDuration)
	}
}

func TestLoginRateLimiterResetClearsHistory(t *testing.T) {
	rl := newLoginRateLimiter()
	const ip = "203.0.113.7"
	for i := 0; i < loginMaxFailures; i++ {
		rl.recordFailure(ip)
	}
	rl.reset(ip)
	if rl.check(ip) {
		t.Fatal("still blocked after reset")
	}
	// The count starts over rather than resuming one short of the limit.
	if rl.recordFailure(ip) {
		t.Fatal("a single failure after reset blocked")
	}
}

// Failures are counted in a window from the first one. Once it has closed, the
// next failure starts a fresh count.
func TestLoginRateLimiterWindowExpiry(t *testing.T) {
	rl := newLoginRateLimiter()
	const ip = "203.0.113.7"
	for i := 1; i < loginMaxFailures; i++ {
		rl.recordFailure(ip)
	}
	rl.attempts[ip].firstFailure = time.Now().Add(-loginWindowDuration - time.Second)

	if rl.recordFailure(ip) {
		t.Fatal("a failure after the window closed completed the old count")
	}
	if got := rl.attempts[ip].failures; got != 1 {
		t.Errorf("failures = %d after the window reset, want 1", got)
	}

	// check also drops an expired, unblocked entry.
	rl.attempts[ip].firstFailure = time.Now().Add(-loginWindowDuration - time.Second)
	if rl.check(ip) {
		t.Fatal("expired entry reported as blocked")
	}
	if _, ok := rl.attempts[ip]; ok {
		t.Error("check left an expired entry behind")
	}
}

// A block runs its full duration even when the failure window that caused it
// closed long ago; the window only governs counting.
func TestLoginRateLimiterBlockOutlivesWindow(t *testing.T) {
	rl := newLoginRateLimiter()
	const ip = "203.0.113.7"
	rl.attempts[ip] = &loginAttempt{
		failures:     loginMaxFailures,
		firstFailure: time.Now().Add(-2 * loginWindowDuration),
		blockedUntil: time.Now().Add(time.Minute),
	}
	if !rl.check(ip) {
		t.Fatal("block lifted because the window closed")
	}

	rl.attempts[ip].blockedUntil = time.Now().Add(-time.Second)
	if rl.check(ip) {
		t.Fatal("still blocked after the block expired")
	}
}

func TestLoginRateLimiterPrune(t *testing.T) {
	rl := newLoginRateLimiter()
	now := time.Now()
	old := now.Add(-loginWindowDuration - time.Minute)

	rl.attempts["stale"] = &loginAttempt{failures: 2, firstFailure: old}
	rl.attempts["in-window"] = &loginAttempt{failures: 2, firstFailure: now.Add(-time.Minute)}
	rl.attempts["blocked"] = &loginAttempt{failures: loginMaxFailures, firstFailure: old, blockedUntil: now.Add(time.Minute)}
	rl.attempts["block-lapsed"] = &loginAttempt{failures: loginMaxFailures, firstFailure: old, blockedUntil: now.Add(-time.Minute)}

	rl.prune(now)

	for ip, keep := range map[string]bool{"stale": false, "in-window": true, "blocked": true, "block-lapsed": false} {
		if _, ok := rl.attempts[ip]; ok != keep {
			t.Errorf("%s: kept = %v, want %v", ip, ok, keep)
		}
	}
	if !rl.check("blocked") {
		t.Error("pruning lifted an active block")
	}
}

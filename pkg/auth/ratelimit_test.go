package auth

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoginRateLimiterBlocksAfterMaxFailures(t *testing.T) {
	rl := newLoginRateLimiter()
	const ip = "203.0.113.7"

	if blocked(rl, ip) {
		t.Fatal("an address with no history is blocked")
	}
	for i := 1; i < loginMaxFailures; i++ {
		if rl.recordFailure(ip) {
			t.Fatalf("blocked after %d failures, want %d", i, loginMaxFailures)
		}
		if blocked(rl, ip) {
			t.Fatalf("check reports blocked after %d failures", i)
		}
	}
	if !rl.recordFailure(ip) {
		t.Fatalf("not blocked after %d failures", loginMaxFailures)
	}
	if !blocked(rl, ip) {
		t.Fatal("check does not report the block")
	}

	// The block is per address: nobody else is collateral.
	if blocked(rl, "203.0.113.8") {
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
	if blocked(rl, ip) {
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

	// A try begun after the window closed starts the count over too.
	rl.attempts[ip].failures = loginMaxFailures - 1
	rl.attempts[ip].firstFailure = time.Now().Add(-loginWindowDuration - time.Second)
	if blocked(rl, ip) {
		t.Fatal("expired entry reported as blocked")
	}
	if a, ok := rl.attempts[ip]; ok {
		t.Errorf("expired entry %+v kept after a try in a new window", a)
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
	if !blocked(rl, ip) {
		t.Fatal("block lifted because the window closed")
	}

	rl.attempts[ip].blockedUntil = time.Now().Add(-time.Second)
	if blocked(rl, ip) {
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
	rl.attempts["running"] = &loginAttempt{inFlight: 1}

	rl.prune(now)

	for ip, keep := range map[string]bool{"stale": false, "in-window": true, "blocked": true, "block-lapsed": false, "running": true} {
		if _, ok := rl.attempts[ip]; ok != keep {
			t.Errorf("%s: kept = %v, want %v", ip, ok, keep)
		}
	}
	if !blocked(rl, "blocked") {
		t.Error("pruning lifted an active block")
	}
}

// blocked reports whether a try on key would be refused.
func blocked(rl *loginRateLimiter, key string) bool {
	try, ok := rl.begin(key)
	try.end()
	return !ok
}

// Tries still running count against the limit, so a burst of concurrent
// requests gets no more guesses than the same requests one after another.
func TestLoginRateLimiterCountsTriesInFlight(t *testing.T) {
	rl := newLoginRateLimiter()
	const ip = "203.0.113.7"
	var tries []*loginTry
	for i := 0; i < loginMaxFailures; i++ {
		try, ok := rl.begin(ip)
		if !ok {
			t.Fatalf("try %d refused, want %d allowed", i+1, loginMaxFailures)
		}
		tries = append(tries, try)
	}
	if _, ok := rl.begin(ip); ok {
		t.Fatal("a try beyond the limit began while the others were still running")
	}

	// A try that ends without failing gives its place back.
	tries[0].end()
	tries[0].end()
	try, ok := rl.begin(ip)
	if !ok {
		t.Fatal("a released place was not reusable")
	}
	tries[0] = try

	for _, try := range tries {
		try.fail()
	}
	if !blocked(rl, ip) {
		t.Fatal("not blocked after the running tries all failed")
	}
	if got := rl.attempts[ip].failures; got != loginMaxFailures {
		t.Errorf("failures = %d, want %d", got, loginMaxFailures)
	}
}

func TestLoginRateLimiterConcurrentTries(t *testing.T) {
	rl := newLoginRateLimiter()
	var begun, wg sync.WaitGroup
	var started atomic.Int32
	release := make(chan struct{})
	for i := 0; i < 50; i++ {
		begun.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			try, ok := rl.begin("alice")
			begun.Done()
			if !ok {
				return
			}
			started.Add(1)
			<-release
			try.fail()
		}()
	}
	begun.Wait() // every request has passed the limiter before any has failed
	close(release)
	wg.Wait()
	if n := started.Load(); n != loginMaxFailures {
		t.Errorf("%d concurrent tries ran, want %d", n, loginMaxFailures)
	}
}

// Reset after a success clears failures but keeps other tries' reservations.
func TestLoginRateLimiterResetKeepsRunningTries(t *testing.T) {
	rl := newLoginRateLimiter()
	running, _ := rl.begin("alice")
	rl.recordFailure("alice")
	rl.reset("alice")
	if a := rl.attempts["alice"]; a == nil || a.inFlight != 1 || a.failures != 0 {
		t.Fatalf("entry after reset = %+v, want one try running and no failures", a)
	}
	running.end()
	if a, ok := rl.attempts["alice"]; ok {
		t.Errorf("entry %+v kept after its last try ended with nothing to remember", a)
	}
}

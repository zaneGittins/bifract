package ingest

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// tokenBucket implements a single token bucket.
type tokenBucket struct {
	tokens    float64
	maxTokens float64
	rate      float64 // tokens per second
	lastTime  time.Time
	mu        sync.Mutex
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{
		tokens:    float64(burst),
		maxTokens: float64(burst),
		rate:      rate,
		lastTime:  time.Now(),
	}
}

func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()
	tb.lastTime = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}
	return false
}

// RateLimiter implements a per-IP token bucket rate limiter.
// Each unique client IP gets its own token bucket so one aggressive
// client cannot exhaust the quota for all others.
type RateLimiter struct {
	buckets map[string]*tokenBucket
	mu      sync.Mutex
	rate    float64
	burst   int
}

// NewRateLimiter creates a per-IP rate limiter.
// rate is the sustained requests/second per IP, burst is the max burst size per IP.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    rate,
		burst:   burst,
	}
	go rl.cleanup()
	return rl
}

// Allow checks if a request from the given IP is allowed.
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	tb, ok := rl.buckets[ip]
	if !ok {
		tb = newTokenBucket(rl.rate, rl.burst)
		rl.buckets[ip] = tb
	}
	rl.mu.Unlock()
	return tb.allow()
}

// cleanup removes stale per-IP buckets every 5 minutes.
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, tb := range rl.buckets {
			tb.mu.Lock()
			idle := now.Sub(tb.lastTime) > 10*time.Minute
			tb.mu.Unlock()
			if idle {
				delete(rl.buckets, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// clientIP extracts the real client IP from proxy headers, falling back to RemoteAddr.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if parts := strings.SplitN(xff, ",", 2); len(parts) > 0 {
			if ip := strings.TrimSpace(parts[0]); ip != "" {
				return ip
			}
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// RateLimitMiddleware creates an HTTP middleware that enforces per-IP rate
// limiting on ingestion endpoints. Returns 429 with Retry-After header when exceeded.
func RateLimitMiddleware(rl *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := clientIP(r)
			if !rl.Allow(ip) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(IngestResponse{
					Success: false,
					Error:   "Rate limit exceeded. Retry after backoff.",
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

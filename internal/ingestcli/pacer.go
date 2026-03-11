package ingestcli

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pacerInterval     = 5 * time.Second
	minConcurrency    = 1
	throttleDecay     = 0.5  // multiplicative decrease factor
	throttleThreshold = 0.05 // 5% 429 rate triggers decrease
	stableRequired    = 3    // consecutive clean windows before increasing
)

// AdaptivePacer controls dynamic concurrency for ingestion workers.
// In adaptive mode, it adjusts the concurrency limit based on server
// feedback (429 responses) using AIMD: additive increase on sustained
// success, multiplicative decrease when throttled.
// In fixed mode (manual flags), it acts as a static counting semaphore.
type AdaptivePacer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	inflight int
	limit    int
	maxLimit int
	adaptive bool

	windowSuccesses atomic.Int64
	windowThrottles atomic.Int64

	consecutiveStable int

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewAdaptivePacer creates a pacer with the given concurrency limit.
// When adaptive is true, a background goroutine adjusts the limit
// based on 429 feedback every 5 seconds.
func NewAdaptivePacer(initialLimit int, adaptive bool) *AdaptivePacer {
	p := &AdaptivePacer{
		limit:    initialLimit,
		maxLimit: initialLimit,
		adaptive: adaptive,
		stopCh:   make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)

	if adaptive {
		go p.tuneLoop()
	}
	return p
}

// Acquire blocks until a concurrency slot is available.
func (p *AdaptivePacer) Acquire() {
	p.mu.Lock()
	for p.inflight >= p.limit {
		p.cond.Wait()
	}
	p.inflight++
	p.mu.Unlock()
}

// Release frees a concurrency slot and records whether the request
// was throttled (429). This feeds the AIMD algorithm.
func (p *AdaptivePacer) Release(wasThrottled bool) {
	if wasThrottled {
		p.windowThrottles.Add(1)
	} else {
		p.windowSuccesses.Add(1)
	}
	p.mu.Lock()
	p.inflight--
	p.cond.Signal()
	p.mu.Unlock()
}

// CurrentLimit returns the current concurrency limit.
func (p *AdaptivePacer) CurrentLimit() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.limit
}

// Stop shuts down the background tuning goroutine.
func (p *AdaptivePacer) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
}

func (p *AdaptivePacer) tuneLoop() {
	ticker := time.NewTicker(pacerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.evaluate()
		case <-p.stopCh:
			return
		}
	}
}

func (p *AdaptivePacer) evaluate() {
	successes := p.windowSuccesses.Swap(0)
	throttles := p.windowThrottles.Swap(0)
	total := successes + throttles

	if total == 0 {
		return
	}

	throttleRate := float64(throttles) / float64(total)

	p.mu.Lock()
	oldLimit := p.limit

	if throttleRate > throttleThreshold {
		// Multiplicative decrease
		newLimit := int(math.Ceil(float64(p.limit) * throttleDecay))
		if newLimit < minConcurrency {
			newLimit = minConcurrency
		}
		p.limit = newLimit
		p.consecutiveStable = 0
	} else if throttles == 0 {
		p.consecutiveStable++
		if p.consecutiveStable >= stableRequired && p.limit < p.maxLimit {
			p.limit++
			p.consecutiveStable = 0
		}
	} else {
		p.consecutiveStable = 0
	}

	newLimit := p.limit
	p.mu.Unlock()

	if newLimit != oldLimit {
		p.cond.Broadcast()
	}
}

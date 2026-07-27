package feeds

import (
	"sync"
	"testing"
)

func TestShouldRunDeletePass(t *testing.T) {
	tests := []struct {
		name       string
		incomplete bool
		listed     int
		retained   int
		errCount   int
		want       bool
	}{
		{"normal prune", false, 100, 95, 0, true},
		{"nothing removed", false, 100, 100, 0, true},
		{"repo genuinely empty", false, 0, 0, 0, true},
		{"all rules filtered out by min level", false, 100, 0, 0, true},
		{"a few rules failed but most succeeded", false, 100, 95, 5, true},
		{"every rule errored", false, 100, 0, 100, false},
		{"stopped early", true, 1700, 75, 1, false},
		{"stopped before the first rule", true, 1700, 0, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := shouldRunDeletePass(tt.incomplete, tt.listed, tt.retained, tt.errCount)
			if got != tt.want {
				t.Errorf("shouldRunDeletePass(%v, %d, %d, %d) = %v, want %v", tt.incomplete, tt.listed, tt.retained, tt.errCount, got, tt.want)
			}
			if !got && reason == "" {
				t.Error("skipped without a reason")
			}
		})
	}
}

func TestAcquireIsExclusive(t *testing.T) {
	s := &Syncer{inFlight: make(map[string]bool)}

	if !s.acquire("feed-a") {
		t.Fatal("first acquire failed")
	}
	if s.acquire("feed-a") {
		t.Error("second acquire of the same feed succeeded")
	}
	if !s.acquire("feed-b") {
		t.Error("a different feed was blocked")
	}
	s.release("feed-a")
	if !s.acquire("feed-a") {
		t.Error("acquire failed after release")
	}
}

// Only one of many concurrent callers may hold a feed at a time.
func TestAcquireConcurrent(t *testing.T) {
	s := &Syncer{inFlight: make(map[string]bool)}

	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.acquire("feed") {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if won != 1 {
		t.Errorf("%d goroutines acquired the same feed, want 1", won)
	}
}

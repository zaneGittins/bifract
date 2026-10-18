package feeds

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestShouldRunDeletePass(t *testing.T) {
	tests := []struct {
		name       string
		incomplete bool
		listed     int
		usable     int
		errCount   int
		want       bool
	}{
		{"normal prune", false, 100, 95, 0, true},
		{"nothing removed", false, 100, 100, 0, true},
		{"no rule files listed", false, 0, 0, 0, false},
		{"all rules filtered out by min level", false, 100, 0, 0, true},
		{"a few rules failed but most succeeded", false, 100, 95, 5, true},
		{"every rule errored", false, 100, 0, 100, false},
		{"stopped early", true, 1700, 75, 1, false},
		{"stopped before the first rule", true, 1700, 0, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := shouldRunDeletePass(tt.incomplete, tt.listed, tt.usable, tt.errCount)
			if got != tt.want {
				t.Errorf("shouldRunDeletePass(%v, %d, %d, %d) = %v, want %v", tt.incomplete, tt.listed, tt.usable, tt.errCount, got, tt.want)
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

func TestMeetsMinLevel(t *testing.T) {
	tests := []struct {
		rule, min string
		want      bool
	}{
		{"high", "", true},
		{"", "high", true}, // native rules carry no level
		{"high", "high", true},
		{"critical", "high", true},
		{"medium", "high", false},
		{"informational", "low", false},
		{"HIGH", "medium", true}, // case-insensitive
		{"High", "CRITICAL", false},
		// An unknown rule level ranks below everything, so a threshold drops it.
		{"urgent", "informational", false},
	}
	for _, tt := range tests {
		if got := meetsMinLevel(tt.rule, tt.min); got != tt.want {
			t.Errorf("meetsMinLevel(%q, %q) = %v, want %v", tt.rule, tt.min, got, tt.want)
		}
	}
}

func TestMeetsMinStatus(t *testing.T) {
	tests := []struct {
		rule, min string
		want      bool
	}{
		{"experimental", "", true},
		{"", "stable", true},
		{"stable", "test", true},
		{"test", "test", true},
		{"experimental", "test", false},
		{"deprecated", "experimental", false},
		{"unsupported", "unsupported", true},
		{"Stable", "TEST", true},
	}
	for _, tt := range tests {
		if got := meetsMinStatus(tt.rule, tt.min); got != tt.want {
			t.Errorf("meetsMinStatus(%q, %q) = %v, want %v", tt.rule, tt.min, got, tt.want)
		}
	}
}

func TestScheduleInterval(t *testing.T) {
	for schedule := range ValidSchedules {
		got := ScheduleInterval(schedule)
		if (schedule == ScheduleNever) != (got == 0) {
			t.Errorf("ScheduleInterval(%q) = %v", schedule, got)
		}
	}
	if ScheduleInterval("bogus") != 0 {
		t.Error("an unknown schedule must never sync")
	}
}

func TestIsDue(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) *time.Time { ts := now.Add(-d); return &ts }
	tests := []struct {
		name     string
		schedule string
		last     *time.Time
		want     bool
	}{
		{"never schedule, never synced", ScheduleNever, nil, false},
		{"never schedule, long ago", ScheduleNever, ago(365 * 24 * time.Hour), false},
		{"hourly, never synced", ScheduleHourly, nil, true},
		{"hourly, 59m ago", ScheduleHourly, ago(59 * time.Minute), false},
		{"hourly, 61m ago", ScheduleHourly, ago(61 * time.Minute), true},
		{"daily, 23h ago", ScheduleDaily, ago(23 * time.Hour), false},
		{"daily, 25h ago", ScheduleDaily, ago(25 * time.Hour), true},
		{"weekly, 6d ago", ScheduleWeekly, ago(6 * 24 * time.Hour), false},
		{"monthly, 31d ago", ScheduleMonthly, ago(31 * 24 * time.Hour), true},
		{"exactly one interval", ScheduleHourly, ago(time.Hour), false},
		{"unknown schedule", "bogus", nil, false},
	}
	s := &Syncer{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.isDue(&Feed{SyncSchedule: tt.schedule, LastSyncedAt: tt.last}, now); got != tt.want {
				t.Errorf("isDue = %v, want %v", got, tt.want)
			}
		})
	}
}

// Stop must cancel a sync in progress rather than wait out its 30-minute budget.
func TestSyncContextCancelledByStop(t *testing.T) {
	s := NewSyncer(nil, nil, nil)
	ctx, cancel := s.syncContext()
	defer cancel()
	if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > feedSyncTimeout {
		t.Errorf("deadline = %v, %v; want within %v", dl, ok, feedSyncTimeout)
	}
	s.Stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("sync context survived Stop")
	}

	// A finished sync's watcher goroutine exits without Stop.
	s2 := NewSyncer(nil, nil, nil)
	ctx2, cancel2 := s2.syncContext()
	cancel2()
	if ctx2.Err() == nil {
		t.Error("cancel did not cancel the context")
	}
}

func TestStartStop(t *testing.T) {
	s := NewSyncer(nil, nil, nil)
	s.Start()
	done := make(chan struct{})
	go func() { s.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}

func TestParseBifractRule(t *testing.T) {
	s := &Syncer{}
	tests := []struct {
		name, content, wantType, wantErr string
	}{
		{"defaults to event", "name: n\nqueryString: a=1\n", "event", ""},
		{"keeps the declared type", "name: n\nqueryString: a=1 | count()\nalertType: compound\n", "compound", ""},
		{"needs a name", "queryString: a=1\n", "", "missing required field: name"},
		{"blank name", "name: '  '\nqueryString: a=1\n", "", "missing required field: name"},
		{"needs a query", "name: n\n", "", "missing required field: queryString"},
		{"not YAML", "name: [unclosed\n", "", "invalid YAML"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, _, query, alertType, level, status, _, _, err := s.parseRule(tt.content, nil)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || name != "n" || query == "" || alertType != tt.wantType {
				t.Fatalf("parseRule = %q %q %q, %v", name, query, alertType, err)
			}
			// Native rules have no level or status, so feed thresholds never drop them.
			if level != "" || status != "" {
				t.Errorf("level/status = %q/%q, want empty", level, status)
			}
		})
	}
}

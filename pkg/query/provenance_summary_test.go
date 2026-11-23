package query

import (
	"testing"
	"time"
)

// ClickHouse answers minIf over no matching rows with the Date default, not
// NULL. A fractal whose summary has hosts but no days yet must not be read as
// one: taken literally, 1970 makes a minutes-old baseline look decades old,
// turns on the node-stability terms, and scores every everyday edge as
// near-certain anomaly.
func TestSummaryFirstDayRefusesTheDateDefault(t *testing.T) {
	for _, value := range []string{"1970-01-01", "", "soon"} {
		if day, err := summaryFirstDay(value); err == nil {
			t.Errorf("summaryFirstDay(%q) = %v, want it refused so pgr scans", value, day)
		}
	}
	day, err := summaryFirstDay("2026-11-01")
	if err != nil || !day.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("summaryFirstDay(2026-11-01) = %v, %v", day, err)
	}
}

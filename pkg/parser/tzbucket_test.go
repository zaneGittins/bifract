package parser

import (
	"strings"
	"testing"
	"time"
)

func TestBucketTimezone(t *testing.T) {
	cases := []struct{ q, tz, want string }{
		{"* | bucket(1d, count())", "", "toStartOfDay(timestamp)"},
		{"* | bucket(1d, count())", "UTC", "toStartOfDay(timestamp)"},
		{"* | bucket(1d, count())", "America/Denver", "toStartOfDay(timestamp, 'America/Denver')"},
		{"* | bucket(1h, count())", "Asia/Kathmandu", "toStartOfHour(timestamp, 'Asia/Kathmandu')"},
		{"* | bucket(1w, count())", "Europe/Dublin", "toStartOfWeek(timestamp, 0, 'Europe/Dublin')"},
		{"* | bucket(1w, count())", "", "toStartOfWeek(timestamp)"},
		{"* | bucket(5m, count())", "Asia/Kolkata", "toStartOfFiveMinutes(timestamp, 'Asia/Kolkata')"},
		{"* | bucket(15m, count())", "Asia/Kolkata", "toStartOfFifteenMinutes(timestamp, 'Asia/Kolkata')"},
		{"* | bucket(1m, count())", "Asia/Kolkata", "toStartOfMinute(timestamp, 'Asia/Kolkata')"},
		{"* | bucket(6h, count())", "America/Denver", "toStartOfInterval(timestamp, INTERVAL 6 HOUR, 'America/Denver')"},
		{"* | timechart(span=1d, count())", "America/Denver", "toStartOfDay(timestamp, 'America/Denver')"},
	}
	for _, c := range cases {
		p, err := ParseQuery(c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		res, err := TranslateToSQLWithOrder(p, QueryOptions{
			StartTime: time.Now().Add(-time.Hour), EndTime: time.Now(),
			FractalID: "11111111-1111-1111-1111-111111111111", MaxRows: 10,
			DisplayTimezone: c.tz,
		})
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if !strings.Contains(res.SQL, c.want) {
			t.Errorf("%s tz=%q:\n want %s\n  got %s", c.q, c.tz, c.want, res.SQL)
		}
		wantCfg := c.tz
		if wantCfg == "" {
			wantCfg = "UTC"
		}
		if got := res.ChartConfig["bucketTimezone"]; got != wantCfg {
			t.Errorf("%s tz=%q: chart_config bucketTimezone = %v, want %s", c.q, c.tz, got, wantCfg)
		}
	}
}

// A zone the tzdata cannot resolve buckets in UTC rather than failing the
// query, and the reported zone is the one actually used.
func TestBucketTimezoneUnresolvable(t *testing.T) {
	p, _ := ParseQuery("* | bucket(1d, count())")
	res, err := TranslateToSQLWithOrder(p, QueryOptions{
		StartTime: time.Now().Add(-time.Hour), EndTime: time.Now(),
		FractalID: "11111111-1111-1111-1111-111111111111", MaxRows: 10,
		DisplayTimezone: "x' OR 1=1 --",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "toStartOfDay(timestamp)") {
		t.Errorf("unresolvable zone should bucket in UTC: %s", res.SQL)
	}
	if strings.Contains(res.SQL, "1=1") {
		t.Errorf("zone leaked into SQL: %s", res.SQL)
	}
	if got := res.ChartConfig["bucketTimezone"]; got != "UTC" {
		t.Errorf("bucketTimezone = %v, want UTC", got)
	}
}

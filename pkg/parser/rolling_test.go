package parser

import (
	"strings"
	"testing"
	"time"
)

func rollingSQL(t *testing.T, q string) string {
	t.Helper()
	p, err := ParseQuery(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sql, err := TranslateToSQL(p, QueryOptions{StartTime: start, EndTime: start.Add(24 * time.Hour), MaxRows: 100, FractalID: "f1"})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return sql
}

func rollingErr(t *testing.T, q string) error {
	t.Helper()
	p, err := ParseQuery(q)
	if err != nil {
		return err
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = TranslateToSQL(p, QueryOptions{StartTime: start, EndTime: start.Add(24 * time.Hour), MaxRows: 100, FractalID: "f1"})
	return err
}

// previous() means the event before in time, which only rolling() orders.
func TestPreviousOnlyInsideRolling(t *testing.T) {
	for _, q := range []string{
		`* | previous(user)`,
		`* | groupBy(host, function=previous(user))`,
		`* | p := previous(user)`,
	} {
		err := rollingErr(t, q)
		if err == nil || !strings.Contains(err.Error(), "rolling()") {
			t.Errorf("%s: want an error pointing at rolling(), got %v", q, err)
		}
	}
}

func TestRollingRejects(t *testing.T) {
	for q, want := range map[string]string{
		`* | groupBy(user) | rolling(user)`:                                 "before",
		`* | rolling(user, function=stddev(bytes))`:                         "stddev",
		`* | rolling(user, function=[count(), count(host)])`:                "_count",
		`* | rolling(user, function=previous(host, as=timestamp))`:          "timestamp",
		`* | rolling(user, function=[previous(host), previous(timestamp)])`: "_previous",
	} {
		err := rollingErr(t, q)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error naming %q, got %v", q, want, err)
		}
	}
}

// A filter before rolling() selects the events it reads; one after it filters
// its output, so it must stay outside the window's subquery.
func TestRollingFilterPosition(t *testing.T) {
	sql := rollingSQL(t, `event_id=3 | rolling(user, function=count()) | host="a"`)
	window := sql[strings.Index(sql, " FROM logs WHERE "):]
	window = window[:strings.Index(window, " WINDOW ")]
	if !strings.Contains(window, "fields.`event_id`::String = '3'") {
		t.Errorf("the filter before rolling() is not in the window's subquery:\n%s", sql)
	}
	if strings.Contains(window, "fields.`host`") {
		t.Errorf("the filter after rolling() narrowed the rows it counts:\n%s", sql)
	}
	if !strings.Contains(sql, "fields.`host`::String = 'a'") {
		t.Errorf("the filter after rolling() was dropped:\n%s", sql)
	}
	if !strings.Contains(sql, "PARTITION BY fields.`user`::String ORDER BY timestamp ASC, log_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
		t.Errorf("window is not per user in time order:\n%s", sql)
	}
}

func TestRollingOutputs(t *testing.T) {
	sql := rollingSQL(t, `* | rolling([src_ip, dst_ip], function=[count(), sum(bytes), avg(bytes, as=mean), min(bytes), max(timestamp), count(host, unique=true, as=hosts), previous(timestamp), previous(host, as=prev_host)])`)
	for _, want := range []string{
		"COUNT(*) OVER _rolling AS _count",
		"sum(toFloat64OrNull(fields.`bytes`::String)) OVER _rolling AS _sum",
		"avg(toFloat64OrNull(fields.`bytes`::String)) OVER _rolling AS mean",
		"min(toFloat64OrNull(fields.`bytes`::String)) OVER _rolling AS _min",
		"max(timestamp) OVER _rolling AS _max",
		"uniqExact(fields.`host`::String) OVER _rolling AS hosts",
		"lagInFrame(toNullable(timestamp)) OVER _rolling AS _previous",
		"lagInFrame(fields.`host`::String) OVER _rolling AS prev_host",
		"PARTITION BY fields.`src_ip`::String, fields.`dst_ip`::String",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
	// Without function= it counts, as groupBy() does.
	if sql := rollingSQL(t, `* | rolling(user)`); !strings.Contains(sql, "COUNT(*) OVER _rolling AS _count") {
		t.Errorf("rolling(user) does not count:\n%s", sql)
	}
}

// A per-row column named _count is not the aggregation's output: projecting it
// beside the aggregate failed with code 215.
func TestPerRowCountIsNotGroupByOutput(t *testing.T) {
	for _, q := range []string{
		`* | len(user, as=_count) | groupBy(user, function=max(_count))`,
		`* | rolling(user) | groupBy(user, function=max(_count))`,
	} {
		sql := rollingSQL(t, q)
		if strings.Contains(sql, "SELECT user, _count, _max") {
			t.Errorf("%s: the per-row _count is projected beside the aggregate:\n%s", q, sql)
		}
	}
}

// lookupIP()'s columns are Strings, so previous() of one is ” on a group's first
// event, as it is for a log field, and isEmpty() finds it.
func TestPreviousOfLookupIPIsEmptyString(t *testing.T) {
	p, err := ParseQuery(`* | lookupIP(field=src_ip, include=[country, latitude]) | rolling(user, function=[previous(country, as=pc), previous(latitude, as=plat)])`)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sql, err := TranslateToSQL(p, QueryOptions{StartTime: start, EndTime: start.Add(time.Hour), MaxRows: 100, FractalID: "f1", GeoIPEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "toNullable") {
		t.Errorf("previous() of a lookupIP() column is NULL, not '', on a first event:\n%s", sql)
	}
}

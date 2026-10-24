package parser

import (
	"strings"
	"testing"
	"time"
)

func newestTestOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "f1",
	}
}

// The single-scan histogram filters exactly as the plain histogram does, so its
// counts match it bucket for bucket, and it collects the newest event times from
// that same filtered scan rather than from a second one.
func TestHistogramNewestFiltersLikeHistogram(t *testing.T) {
	opts := newestTestOpts()
	from := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 1, 18, 0, 0, 0, time.UTC)

	pl, err := ParseQuery(`a="x" OR cidr(dst_ip,"10.0.0.0/8")`)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := BuildHistogramSQL(pl, opts, 60)
	if err != nil {
		t.Fatal(err)
	}
	pl, _ = ParseQuery(`a="x" OR cidr(dst_ip,"10.0.0.0/8")`)
	sql, err := BuildHistogramNewestSQL(pl, opts, 60, 25, from, to)
	if err != nil {
		t.Fatal(err)
	}

	where := hist[strings.Index(hist, " WHERE "):strings.Index(hist, " GROUP BY")]
	if !strings.HasSuffix(sql, where) {
		t.Errorf("filter differs from the histogram's\nhistogram: %s\nnewest:    %s", hist, sql)
	}
	for _, want := range []string{
		// Buckets keyed exactly as the histogram keys them, laid out from the
		// window start so array index k is bucket k of the window.
		"countResample(1767225600, 1767312000, 60)(toUnixTimestamp(toStartOfInterval(timestamp, INTERVAL 60 SECOND))) AS hist_counts",
		"groupArraySortedIf(25)(-toUnixTimestamp64Milli(timestamp), timestamp >= '2026-01-01 06:00:00.000' AND timestamp < '2026-01-01 18:00:00.000') AS newest_ms",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "GROUP BY") || strings.Contains(sql, "AS bucket") {
		t.Errorf("one aggregate row expected, not a per-bucket result:\n%s", sql)
	}
}

// A binding read more than once lives in a WITH clause the filter refers to; the
// single-scan histogram carries it like the plain one.
func TestHistogramNewestCarriesBindingCTEs(t *testing.T) {
	pl, err := ParseQuery(`let &a := * | groupby(image) | table(image); * | in(image, &a) | in(parent_image, &a)`)
	if err != nil {
		t.Fatal(err)
	}
	opts := newestTestOpts()
	sql, err := BuildHistogramNewestSQL(pl, opts, 60, 10, opts.StartTime, opts.EndTime)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sql, "WITH _b_a AS (") || !strings.Contains(sql, "FROM _b_a") {
		t.Errorf("reads _b_a without defining it:\n%s", sql)
	}
}

// EventTimes narrows the source scan to rows at exactly those instants, at the
// column's millisecond precision, and leaves the rest of the query untouched.
func TestEventTimesRestrictTheScan(t *testing.T) {
	opts := newestTestOpts()
	opts.MaxRows = 50
	plain, err := TranslateToSQLWithOrder(mustParse(t, `level="error" | table(user)`), opts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.SQL, "timestamp IN (") {
		t.Fatalf("restriction without EventTimes:\n%s", plain.SQL)
	}

	opts.EventTimes = []time.Time{
		time.Date(2026, 1, 1, 10, 0, 0, 123e6, time.UTC),
		time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC),
	}
	got, err := TranslateToSQLWithOrder(mustParse(t, `level="error" | table(user)`), opts)
	if err != nil {
		t.Fatal(err)
	}
	want := "timestamp IN ('2026-01-01 10:00:00.123', '2026-01-01 09:30:00.000')"
	if !strings.Contains(got.SQL, want) {
		t.Errorf("missing %q:\n%s", want, got.SQL)
	}
	if strings.Replace(got.SQL, " AND "+want, "", 1) != plain.SQL {
		t.Errorf("EventTimes changed more than the scan predicate\nplain: %s\ngot:   %s", plain.SQL, got.SQL)
	}
	if !got.SourceWhereComplete {
		t.Error("the restriction is a scan predicate; the scan must still be the whole row selection")
	}
}

// The counts are laid out from the window start, so a start between bucket
// boundaries would drop its first partial bucket; and ClickHouse rejects an empty
// newest set.
func TestHistogramNewestRejectsBadArguments(t *testing.T) {
	opts := newestTestOpts()
	if _, err := BuildHistogramNewestSQL(mustParse(t, `level="a"`), opts, 60, 0, opts.StartTime, opts.EndTime); err == nil {
		t.Error("newest 0 accepted")
	}
	opts.StartTime = opts.StartTime.Add(30 * time.Second)
	if _, err := BuildHistogramNewestSQL(mustParse(t, `level="a"`), opts, 60, 5, opts.StartTime, opts.EndTime); err == nil {
		t.Error("a start between bucket boundaries accepted")
	}
}

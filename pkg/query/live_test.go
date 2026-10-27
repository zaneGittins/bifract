package query

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLiveRange(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 7, 345_000_000, time.UTC)
	windowStart := time.Date(2026, 10, 27, 11, 45, 0, 0, time.UTC) // now - 15m, floored to 10s
	cases := []struct {
		name      string
		since     time.Time
		wantStart time.Time
	}{
		{"first poll starts the whole window", time.Time{}, windowStart},
		{"since older than the window is clamped", now.Add(-3 * time.Hour), windowStart},
		{"overlap is re-read and floored to a bucket", now.Add(-5 * time.Second), time.Date(2026, 10, 27, 11, 58, 0, 0, time.UTC)},
		{"since in the future is treated as now", now.Add(time.Hour), time.Date(2026, 10, 27, 11, 58, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := liveRange(tc.since, now)
			if !start.Equal(tc.wantStart) {
				t.Errorf("start = %s, want %s", start, tc.wantStart)
			}
			if !end.Equal(now.Truncate(time.Millisecond)) {
				t.Errorf("end = %s, want %s", end, now)
			}
			if start.Unix()%int64(liveBucket/time.Second) != 0 {
				t.Errorf("start %s is not on a bucket boundary", start)
			}
		})
	}
}

func TestPrepareLiveReadsHotTableOnIngestTime(t *testing.T) {
	h := newTestQueryHandler(250)
	since := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	prep := prepare(t, h, purposeLive, jsonBody(t, QueryRequest{Query: "event_id=1", FractalID: "f1", Start: since}), adminCtx())

	if !prep.liveSupported {
		t.Fatal("a plain filter must be live-capable")
	}
	if !strings.Contains(prep.sql, "FROM logs_hot") {
		t.Errorf("live rows must read logs_hot:\n%s", prep.sql)
	}
	if !strings.Contains(prep.sql, "ingest_timestamp >= '") || !strings.Contains(prep.sql, "ingest_timestamp < '") {
		t.Errorf("live rows must be bounded on ingest time, end exclusive:\n%s", prep.sql)
	}
	if strings.Contains(prep.sql, " timestamp >= '") {
		t.Errorf("live rows must not be bounded on event time:\n%s", prep.sql)
	}
	if !strings.Contains(prep.histogramSQL, "toStartOfInterval(ingest_timestamp, INTERVAL 10 SECOND)") {
		t.Errorf("live histogram must bucket arrival time in 10s buckets:\n%s", prep.histogramSQL)
	}
	if !strings.Contains(prep.histogramSQL, "FROM logs_hot") {
		t.Errorf("live histogram must read logs_hot:\n%s", prep.histogramSQL)
	}
	if got := prep.endTime.Sub(prep.startTime); got < liveOverlap+30*time.Second || got > liveOverlap+30*time.Second+liveBucket+time.Second {
		t.Errorf("a cursor 30s old reads the gap plus the overlap, got %s", got)
	}
}

func TestPrepareLiveClampsToWindow(t *testing.T) {
	h := newTestQueryHandler(250)
	prep := prepare(t, h, purposeLive, jsonBody(t, QueryRequest{Query: "*", FractalID: "f1", Start: "2020-01-01T00:00:00Z"}), adminCtx())
	if got := prep.endTime.Sub(prep.startTime); got > liveWindow+liveBucket {
		t.Errorf("a stale cursor must not widen the read past the live window, got %s", got)
	}
}

func TestPrepareLiveUnsupportedShapes(t *testing.T) {
	h := newTestQueryHandler(250)
	for _, q := range []string{
		"* | groupBy(user)",
		"* | count()",
		"* | sort(user)",
		"* | groupBy(user) | piechart()",
	} {
		prep := prepare(t, h, purposeLive, jsonBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx())
		if prep.liveSupported {
			t.Errorf("%q must not stream live; it re-runs as a search instead", q)
		}
	}
}

func TestHandleQueryLiveReportsUnsupported(t *testing.T) {
	h := newTestQueryHandler(250)
	rec := httptest.NewRecorder()
	h.HandleQueryLive(rec, testRequest(http.MethodPost, jsonBody(t, QueryRequest{Query: "* | groupBy(user)", FractalID: "f1"}), adminCtx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"live_unsupported":true`) {
		t.Errorf("an aggregation must report live_unsupported, got %s", rec.Body.String())
	}
}

func TestPrepareLiveRefusesBadCursor(t *testing.T) {
	rec := httptest.NewRecorder()
	prep := newTestQueryHandler(250).prepareQuery(rec, testRequest(http.MethodPost, jsonBody(t, QueryRequest{Query: "*", FractalID: "f1", Start: "yesterday"}), adminCtx()), purposeLive)
	if prep != nil || rec.Code != http.StatusBadRequest {
		t.Fatalf("an unparseable cursor must be refused, got %d %s", rec.Code, rec.Body.String())
	}
}

// Source commands and comment() resolve against event time or run their own
// queries before translation, so live hands them to the search path untouched.
func TestPrepareLiveUnsupportedBeforeResolving(t *testing.T) {
	h := newTestQueryHandler(250)
	for _, q := range []string{
		`pgr(start="abc")`,
		`comment(tags="ioc")`,
	} {
		prep := prepare(t, h, purposeLive, jsonBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx())
		if prep.liveSupported || prep.sql != "" {
			t.Errorf("%q must be refused before it is resolved or translated", q)
		}
	}
}

func TestPrepareLiveCapsRows(t *testing.T) {
	prep := prepare(t, newTestQueryHandler(10000), purposeLive, jsonBody(t, QueryRequest{Query: "*", FractalID: "f1"}), adminCtx())
	if prep.queryMaxRows != liveMaxRows || !strings.Contains(prep.sql, fmt.Sprintf("LIMIT %d", liveMaxRows)) {
		t.Errorf("a poll returns at most what the table keeps, got %d:\n%s", prep.queryMaxRows, prep.sql)
	}
	small := prepare(t, newTestQueryHandler(250), purposeLive, jsonBody(t, QueryRequest{Query: "*", FractalID: "f1"}), adminCtx())
	if small.queryMaxRows != 250 {
		t.Errorf("a lower server cap still applies, got %d", small.queryMaxRows)
	}
}

func TestBuildLiveResponse(t *testing.T) {
	start := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	prep := &preparedQuery{
		startTime:    start,
		endTime:      start.Add(25 * time.Second),
		fieldOrder:   []string{"timestamp", "norm_log"},
		queryMaxRows: 2,
		warnings:     []string{"partial"},
	}
	hist := []map[string]interface{}{
		{"bucket": start, "cnt": uint64(3)},
		{"bucket": start.Add(20 * time.Second), "cnt": uint64(1)},
	}
	rows := []map[string]interface{}{{"log_id": "a"}, {"log_id": "b"}}

	resp := buildLiveResponse(prep, rows, hist, true)
	if resp.Cursor != "2026-10-27T12:00:25Z" {
		t.Errorf("cursor must be the exclusive window end, got %s", resp.Cursor)
	}
	if got := resp.Histogram.Buckets; len(got) != 3 || got[0] != 3 || got[1] != 0 || got[2] != 1 {
		t.Errorf("buckets must cover [start, end) exactly, got %v", got)
	}
	if !resp.LimitHit || len(resp.Warnings) != 1 {
		t.Errorf("limit and warnings must reach the client: %+v", resp)
	}

	// An end on a bucket boundary has no bucket after it.
	prep.endTime = start.Add(20 * time.Second)
	if n := len(buildLiveResponse(prep, rows, hist, true).Histogram.Buckets); n != 2 {
		t.Errorf("an end on a boundary must not add an empty bucket, got %d", n)
	}

	if r := buildLiveResponse(prep, nil, nil, false); r.Histogram != nil || r.Results == nil {
		t.Errorf("a failed histogram drops only the histogram: %+v", r)
	}
}

package query

import (
	"context"
	"log"
	"net/http"
	"time"

	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

// Live search polls logs_hot by arrival time. Each poll re-reads liveOverlap
// behind the client's cursor because ingest_timestamp is stamped on receipt, so
// a row can become visible after a later-stamped one was already read; the
// client drops the repeats by log_id. BIF-33's ingest watermark replaces the
// overlap once it exists.
const (
	liveWindow  = 15 * time.Minute
	liveOverlap = 2 * time.Minute
	liveBucket  = 10 * time.Second
	// liveMaxRows matches the rows the client keeps; more would only be dropped.
	liveMaxRows = 1000
)

const liveBucketSec = int(liveBucket / time.Second)

// liveRange is the ingest-time window one poll reads: from liveOverlap behind
// since, never earlier than the live window, floored to a histogram bucket so
// every returned bucket is a complete recount; to now, exclusive.
func liveRange(since, now time.Time) (start, end time.Time) {
	end = now.UTC().Truncate(time.Millisecond)
	floor := end.Add(-liveWindow)
	switch {
	case since.IsZero():
		start = floor
	case since.After(end):
		start = end.Add(-liveOverlap)
	default:
		start = since.Add(-liveOverlap)
	}
	if start.Before(floor) {
		start = floor
	}
	return start.Truncate(liveBucket), end
}

// LiveHistogram is the recount of every arrival-time bucket a poll covered. The
// client overwrites these buckets rather than adding them, so the overlap never
// double counts.
type LiveHistogram struct {
	BucketStart   string `json:"bucket_start"`
	BucketSeconds int    `json:"bucket_seconds"`
	Buckets       []int  `json:"buckets"`
}

// LiveResponse is one poll's result. Cursor is the exclusive end of the window
// read, which the client sends back as start on the next poll.
type LiveResponse struct {
	Success         bool                     `json:"success"`
	Error           string                   `json:"error,omitempty"`
	ErrorType       string                   `json:"error_type,omitempty"`
	LiveUnsupported bool                     `json:"live_unsupported,omitempty"`
	Results         []map[string]interface{} `json:"results,omitempty"`
	FieldOrder      []string                 `json:"field_order,omitempty"`
	Cursor          string                   `json:"cursor,omitempty"`
	Histogram       *LiveHistogram           `json:"histogram,omitempty"`
	LimitHit        bool                     `json:"limit_hit,omitempty"`
	Warnings        []string                 `json:"warnings,omitempty"`
	ExecutionMs     int64                    `json:"execution_ms"`
}

// HandleQueryLive runs one live poll: the query's newest matches that arrived in
// the poll window, plus a recount of the window's arrival buckets. Stateless, so
// any app replica can serve any poll. Queries that cannot stream (aggregations,
// sorts, charts) report live_unsupported and the client re-runs them as a
// search instead.
func (h *QueryHandler) HandleQueryLive(w http.ResponseWriter, r *http.Request) {
	prep := h.prepareQuery(w, r, purposeLive)
	if prep == nil {
		return
	}
	if !prep.liveSupported {
		respondJSON(w, http.StatusOK, LiveResponse{Success: true, LiveUnsupported: true})
		return
	}

	searchID := newSearchID()
	ctx, cancel := searchContext(storage.TagContext(r.Context(), requestTag(r, prep.req)), settings.Get().QueryTimeoutSeconds, prep.fractalScope())
	defer func() {
		abandoned := ctx.Err() != nil
		cancel()
		if abandoned {
			h.abandonSearch(searchID)
		}
	}()

	// The timeline is not worth failing the poll for, so the histogram runs on its
	// own context and is dropped, not waited on, when the rows fail.
	histCtx, histCancel := context.WithCancel(ctx)
	defer histCancel()
	type result struct {
		rows []map[string]interface{}
		err  error
	}
	histCh := make(chan result, 1)
	start := time.Now()
	go func() {
		rows, err := h.db.QueryUserSearchWithID(histCtx, searchID+"-hist", prep.histogramSQL)
		histCh <- result{rows, err}
	}()
	rows, err := h.db.QueryUserSearchWithID(ctx, searchID+"-main", prep.sql)
	if err != nil {
		histCancel()
		if r.Context().Err() == context.Canceled {
			return
		}
		log.Printf("[QueryHandler] Live poll failed: %v", err)
		resp := LiveResponse{Error: executionErrorMessage(err), ErrorType: "execution", ExecutionMs: time.Since(start).Milliseconds()}
		status := http.StatusInternalServerError
		if ctx.Err() == context.DeadlineExceeded {
			resp.Error, resp.ErrorType, status = "Query timeout: add more specific filters", "timeout", http.StatusRequestTimeout
		}
		respondJSON(w, status, resp)
		return
	}
	hist := <-histCh
	if hist.err != nil {
		log.Printf("[QueryHandler] Live histogram failed (non-critical): %v", hist.err)
	}

	resp := buildLiveResponse(prep, rows, hist.rows, hist.err == nil)
	resp.ExecutionMs = time.Since(start).Milliseconds()
	// One audit record per Live run rather than one every poll: the first poll is
	// the one with no cursor.
	if prep.req.Start == "" {
		h.auditSearch(r, prep.req.Query, prep.selectedIndex, resp.ExecutionMs, len(rows))
	}
	respondJSON(w, http.StatusOK, resp)
}

// buildLiveResponse assembles a successful poll. histOK false drops only the
// timeline; the rows still go out.
func buildLiveResponse(prep *preparedQuery, rows, histRows []map[string]interface{}, histOK bool) LiveResponse {
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	resp := LiveResponse{
		Success:    true,
		Results:    rows,
		FieldOrder: prep.fieldOrder,
		Cursor:     prep.endTime.Format(time.RFC3339Nano),
		LimitHit:   prep.queryMaxRows > 0 && len(rows) >= prep.queryMaxRows,
		Warnings:   prep.warnings,
	}
	if histOK {
		// Buckets covering [start, end): the window end is exclusive.
		count := int((prep.endTime.Sub(prep.startTime) + liveBucket - 1) / liveBucket)
		resp.Histogram = &LiveHistogram{
			BucketStart:   prep.startTime.Format(time.RFC3339),
			BucketSeconds: liveBucketSec,
			Buckets:       bucketHistogram(histRows, prep.startTime, liveBucketSec, count),
		}
	}
	return resp
}

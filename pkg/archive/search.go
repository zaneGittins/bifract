package archive

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// SearchResult is the outcome of a Recall search over an Iceberg archive.
type SearchResult struct {
	Rows         []map[string]interface{}
	FieldOrder   []string
	IsAggregated bool
	LimitHit     bool
	// Stats is what ClickHouse read from object storage to answer the query.
	// Recorded on the job so a user can tell a query that pruned well from one
	// that scanned the whole window, which is the difference between narrowing
	// the time range and narrowing the predicate.
	Stats storage.QueryStats
}

// SearchStream receives a search's state while it is still running, so a caller
// can surface partial results and live scan cost instead of a bare spinner.
//
// Rows handed to Partial are always a prefix of the final result set: ClickHouse
// emits sorted and aggregated output only once those stages complete, so a
// partial view is never reordered or revised by later blocks.
//
// Both callbacks run inline with the ClickHouse read loop and must return
// promptly. Partial receives a snapshot that stays valid after it returns (the
// backing array is only appended to), so a caller may hand it to another
// goroutine to persist without copying.
type SearchStream struct {
	// Planned fires once, after translation and before execution, with the
	// result shape. It lets a caller render column headers before any row lands.
	Planned func(fieldOrder []string, isAggregated bool)
	// Partial fires at most once per Interval while the query runs.
	Partial func(rows []map[string]interface{}, stats storage.QueryStats)
	// Interval throttles Partial. Zero or negative disables partial delivery
	// entirely (the query still runs; only the final result is reported).
	Interval time.Duration
}

// SearchRequest is one Recall search: a BQL query over a single fractal's
// archive for an ingest-time window.
type SearchRequest struct {
	FractalID string
	Query     string
	From, To  time.Time
	// MaxRows caps an aggregation-free result set; <= 0 means the default 250.
	MaxRows int
	// QueryID, when non-empty, is applied as the ClickHouse query_id so the run
	// can be interrupted with KILL QUERY (cancel/timeout) from another process.
	QueryID string
	// Stream, when non-nil, receives incremental progress.
	Stream *SearchStream
}

// Search runs a BQL query against a single fractal's Iceberg archive for an
// ingest-time window, reading directly through a ClickHouse iceberg*() table
// function. It reuses the hot BQL translator in SourceIceberg mode
// (JSONExtractString(norm_log) field access, promoted `_ice_` column pruning,
// norm_log free-text content) and filters
// on ingest_timestamp -- the archive's partition axis -- so ClickHouse prunes
// whole ingest-date partitions. Aggregation-free results are capped at MaxRows
// with LimitHit set when the cap is reached.
func (c *Catalog) Search(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, req SearchRequest) (*SearchResult, error) {
	// Read the promoted-column set from this table's own schema: a dormant fractal
	// may predate the current build's set, and querying a column it lacks is a hard
	// failure rather than a lost optimization.
	loc, promoted, err := c.TablePromotedFields(ctx, req.FractalID)
	if err != nil {
		return nil, fmt.Errorf("archive: no Iceberg table for fractal %s: %w", req.FractalID, err)
	}
	tf, err := chIcebergTableFunc(obj, loc, ch.Topology().FanoutCluster)
	if err != nil {
		return nil, err
	}

	pipeline, err := parser.ParseQuery(req.Query)
	if err != nil {
		return nil, fmt.Errorf("archive: parse query: %w", err)
	}
	maxRows := req.MaxRows
	if maxRows <= 0 {
		maxRows = 250
	}

	res, err := parser.TranslateToSQLWithOrder(pipeline, parser.QueryOptions{
		StartTime:          req.From,
		EndTime:            req.To,
		FractalID:          req.FractalID,
		MaxRows:            maxRows,
		SourceMode:         parser.SourceIceberg,
		UseIngestTimestamp: true, // archive is partitioned by ingest_date; prune on it
		TableName:          tf,
		IcePromoted:        promoted,
	})
	if err != nil {
		return nil, err
	}

	stream := req.Stream
	if stream != nil && stream.Planned != nil {
		stream.Planned(res.FieldOrder, res.IsAggregated)
	}

	// Field access reads the `norm_log` String column (via JSONExtractString) plus
	// the `_ice_` promoted columns and their Parquet bloom filters. norm_log is a
	// plain String, so it sidesteps the ClickHouse Iceberg Map-decode bug (Code
	// 117, upstream #91580) that broke field-dense fractals under a Map column.
	// Scheduled under the recall workload so an archive scan cannot take the cores
	// and memory that interactive search and ingestion need. This is recall's only
	// enforceable ceiling: ClickHouse does not apply max_bytes_to_read to iceberg
	// table functions, so the byte budget can gate admission but not a running scan.
	ctx = storage.RecallContext(ctx)

	var rows []map[string]interface{}
	emit := newPartialEmitter(stream)
	stats, err := ch.QueryStream(ctx, req.QueryID, res.SQL,
		func(row map[string]interface{}) {
			rows = append(rows, row)
			emit(rows, storage.QueryStats{}, false)
		},
		func(s storage.QueryStats) { emit(rows, s, true) },
	)
	if err != nil {
		// Carry the partial scan cost out with the error: a search that times out
		// is the case where "how much did it read" matters most. The rows read
		// before the failure come with it, so a canceled or timed-out search can
		// still show what it found.
		return &SearchResult{Rows: rows, FieldOrder: res.FieldOrder, IsAggregated: res.IsAggregated, Stats: stats},
			fmt.Errorf("archive: search query failed: %w", err)
	}
	limitHit := !res.IsAggregated && len(rows) >= maxRows
	if limitHit && len(rows) > maxRows {
		rows = rows[:maxRows]
	}
	return &SearchResult{
		Rows:         rows,
		FieldOrder:   res.FieldOrder,
		IsAggregated: res.IsAggregated,
		LimitHit:     limitHit,
		Stats:        stats,
	}, nil
}

// newPartialEmitter returns a throttled dispatcher for SearchStream.Partial, or
// a no-op when partial delivery is disabled. The returned func is called from
// the ClickHouse read loop on every row and every progress packet; it forwards
// at most once per Interval.
//
// haveStats distinguishes the two call sites: only progress packets carry scan
// cost, so a row-driven emission reuses the last reported stats rather than
// overwriting them with a zero value.
func newPartialEmitter(stream *SearchStream) func(rows []map[string]interface{}, stats storage.QueryStats, haveStats bool) {
	if stream == nil || stream.Partial == nil || stream.Interval <= 0 {
		return func([]map[string]interface{}, storage.QueryStats, bool) {}
	}
	var (
		mu    sync.Mutex
		last  time.Time
		cur   storage.QueryStats
		sent  int
		first = true
	)
	return func(rows []map[string]interface{}, stats storage.QueryStats, haveStats bool) {
		mu.Lock()
		if haveStats {
			cur = stats
		}
		now := time.Now()
		// Nothing new to say: same row count and no fresh scan cost.
		if !first && len(rows) == sent && !haveStats {
			mu.Unlock()
			return
		}
		if !first && now.Sub(last) < stream.Interval {
			mu.Unlock()
			return
		}
		first, last, sent = false, now, len(rows)
		snapshot, s := rows[:len(rows):len(rows)], cur
		mu.Unlock()
		stream.Partial(snapshot, s)
	}
}

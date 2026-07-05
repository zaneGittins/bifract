package archive

import (
	"context"
	"fmt"
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
}

// Search runs a BQL query against a single fractal's Iceberg archive for an
// ingest-time window, reading directly through a ClickHouse iceberg*() table
// function. It reuses the hot BQL translator in SourceIceberg mode (MAP field
// access, promoted `_ice_` column pruning, toString(fields) content) and filters
// on ingest_timestamp -- the archive's partition axis -- so ClickHouse prunes
// whole ingest-date partitions. Aggregation-free results are capped at maxRows
// with LimitHit set when the cap is reached.
func (c *Catalog) Search(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID, query string, from, to time.Time, maxRows int) (*SearchResult, error) {
	loc, err := c.TableLocation(ctx, fractalID)
	if err != nil {
		return nil, fmt.Errorf("archive: no Iceberg table for fractal %s: %w", fractalID, err)
	}
	tf, err := chIcebergTableFunc(obj, loc)
	if err != nil {
		return nil, err
	}

	pipeline, err := parser.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("archive: parse query: %w", err)
	}
	if maxRows <= 0 {
		maxRows = 1000
	}

	res, err := parser.TranslateToSQLWithOrder(pipeline, parser.QueryOptions{
		StartTime:          from,
		EndTime:            to,
		FractalID:          fractalID,
		MaxRows:            maxRows,
		SourceMode:         parser.SourceIceberg,
		UseIngestTimestamp: true, // archive is partitioned by ingest_date; prune on it
		TableName:          tf,
	})
	if err != nil {
		return nil, err
	}

	// ClickHouse 26.6+ (the pinned/bundled version) reads the iceberg `fields` Map,
	// the `_ice_` promoted columns, and their Parquet bloom filters correctly, so
	// no read work-arounds are needed. (CH 26.2 required disabling PREWHERE and
	// bloom push-down; those bugs are fixed as of 26.6.)
	rows, err := ch.Query(ctx, res.SQL)
	if err != nil {
		return nil, fmt.Errorf("archive: search query failed: %w", err)
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
	}, nil
}

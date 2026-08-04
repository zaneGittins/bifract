package storage

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	// hydrateChunkSize bounds how many log_ids go into a single IN list, matching
	// the ceiling GetLogFieldsByIDs enforces.
	hydrateChunkSize = 500

	// hydrateMaxDateGroups bounds how many logs partitions a cold-path hydration
	// will touch. Event timestamps are unbounded relative to the ingest window an
	// alert matched on, so a backfill replay could otherwise fan out into one
	// query per historical day.
	hydrateMaxDateGroups = 8

	// hydrateWindowSlack widens the hot-path ingest window. The translator writes
	// the alert's own bounds second-truncated and unzoned, so an exact match is
	// not reproducible here; a slightly wide window can never miss a row.
	hydrateWindowSlack = time.Second
)

// hydrateTables is the set of tables hydration may read. The caller passes the
// table the originating query used, so this guards the one interpolated token.
var hydrateTables = map[string]bool{
	"logs": true, "logs_distributed": true,
	"logs_hot": true, "logs_hot_distributed": true,
}

// LogKey identifies a single log row for a hydration lookup.
type LogKey struct {
	LogID     string
	FractalID string
	Timestamp time.Time
}

// hydrateBatch is one partition-aligned batch of keys.
type hydrateBatch struct {
	from   time.Time
	to     time.Time
	logIDs []string
}

// HydrateLogFields batch-fetches parsed field maps, keyed by log_id, for rows that
// came back from a projection-pruned query (see the alert auto-projection in
// pkg/parser/translator.go, which drops norm_log).
//
// table must be the table the originating query read, so a cluster deployment
// does not hydrate from logs_distributed what it matched on logs_hot_distributed.
// byIngest selects which time column bounds the lookup: the hot table is keyed on
// ingest_timestamp, which from/to bound directly; the main logs table is keyed on
// timestamp, so the keys are grouped by their own event date instead and from/to
// are ignored. Rows that cannot be found are absent from the returned map.
func (c *ClickHouseClient) HydrateLogFields(ctx context.Context, table string, byIngest bool, keys []LogKey, from, to time.Time) (map[string]map[string]interface{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if !hydrateTables[table] {
		return nil, fmt.Errorf("hydrate log fields: unsupported table %q", table)
	}

	fractalIDs := observedFractalIDs(keys)
	var batches []hydrateBatch
	tsCol := "timestamp"
	if byIngest {
		tsCol = "ingest_timestamp"
		batches = []hydrateBatch{{
			from:   from.Add(-hydrateWindowSlack),
			to:     to.Add(hydrateWindowSlack),
			logIDs: dedupeLogIDs(keys),
		}}
	} else {
		batches = groupKeysByDate(keys)
	}

	query := fmt.Sprintf(
		"SELECT log_id, norm_log FROM %s WHERE %s >= toDateTime64(?, 3, 'UTC') AND %s <= toDateTime64(?, 3, 'UTC') AND log_id IN (?)",
		table, tsCol, tsCol)
	if len(fractalIDs) > 0 {
		query += " AND fractal_id IN (?)"
	}
	// Guards against re-ingested duplicate log_ids; on a Distributed table this is
	// applied on the initiator after the shard merge.
	query += " LIMIT 1 BY log_id"

	// Hydration is background work on the alert path and must never starve user
	// queries, same rationale as QueryLowPriority.
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(c.applyQuerySettings(ctx, clickhouse.Settings{
		"priority":       5,
		"max_query_size": maxGeneratedQuerySize,
	})))

	out := make(map[string]map[string]interface{}, len(keys))
	for _, b := range batches {
		for _, chunk := range chunkStrings(b.logIDs, hydrateChunkSize) {
			args := []interface{}{chTimeArg(b.from), chTimeArg(b.to), chunk}
			if len(fractalIDs) > 0 {
				args = append(args, fractalIDs)
			}
			if err := c.collectLogFields(ctx, out, query, args); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// collectLogFields runs one hydration query and merges its rows into out.
func (c *ClickHouseClient) collectLogFields(ctx context.Context, out map[string]map[string]interface{}, query string, args []interface{}) error {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("hydrate log fields: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var logID, normLog string
		if err := rows.Scan(&logID, &normLog); err != nil {
			return fmt.Errorf("hydrate log fields scan: %w", err)
		}
		// norm_log can be empty when the column DEFAULT never fired; parseLogFields
		// then yields an empty map, which the caller treats as "no hydration".
		if fields := c.parseLogFields(ctx, normLog); len(fields) > 0 {
			out[logID] = fields
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("hydrate log fields iterate: %w", err)
	}
	return nil
}

// chTimeArg renders a bound argument for toDateTime64(?, 3, 'UTC').
func chTimeArg(t time.Time) string {
	return t.UTC().Format(chRowTimeLayout)
}

// observedFractalIDs returns the distinct fractal ids actually present in keys.
// For a prism alert this is usually a small subset of the prism's membership,
// which prunes the partition key harder than the configured list would.
func observedFractalIDs(keys []LogKey) []string {
	seen := make(map[string]bool)
	var out []string
	for _, k := range keys {
		if k.FractalID == "" || seen[k.FractalID] {
			continue
		}
		seen[k.FractalID] = true
		out = append(out, k.FractalID)
	}
	return out
}

// dedupeLogIDs returns the distinct non-empty log ids in keys, preserving order.
func dedupeLogIDs(keys []LogKey) []string {
	seen := make(map[string]bool, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k.LogID == "" || seen[k.LogID] {
			continue
		}
		seen[k.LogID] = true
		out = append(out, k.LogID)
	}
	return out
}

// groupKeysByDate buckets keys by UTC event date so each cold-path query hits a
// single logs partition date, narrowed to the actual min/max within the bucket.
// Keys with no usable timestamp are dropped rather than widened into a
// neighbouring range. Buckets beyond hydrateMaxDateGroups are dropped largest-first.
func groupKeysByDate(keys []LogKey) []hydrateBatch {
	index := make(map[time.Time]int)
	var batches []hydrateBatch
	seen := make(map[string]bool, len(keys))

	for _, k := range keys {
		if k.LogID == "" || k.Timestamp.IsZero() || seen[k.LogID] {
			continue
		}
		seen[k.LogID] = true
		ts := k.Timestamp.UTC()
		day := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		i, ok := index[day]
		if !ok {
			index[day] = len(batches)
			batches = append(batches, hydrateBatch{from: ts, to: ts, logIDs: []string{k.LogID}})
			continue
		}
		b := &batches[i]
		b.logIDs = append(b.logIDs, k.LogID)
		if ts.Before(b.from) {
			b.from = ts
		}
		if ts.After(b.to) {
			b.to = ts
		}
	}

	if len(batches) > hydrateMaxDateGroups {
		sort.SliceStable(batches, func(i, j int) bool { return len(batches[i].logIDs) > len(batches[j].logIDs) })
		dropped := 0
		for _, b := range batches[hydrateMaxDateGroups:] {
			dropped += len(b.logIDs)
		}
		log.Printf("[Hydrate] %d log dates exceed the %d-partition budget, leaving %d rows unhydrated",
			len(batches), hydrateMaxDateGroups, dropped)
		batches = batches[:hydrateMaxDateGroups]
	}
	return batches
}

// chunkStrings splits s into slices of at most size elements.
func chunkStrings(s []string, size int) [][]string {
	if len(s) == 0 {
		return nil
	}
	if len(s) <= size {
		return [][]string{s}
	}
	var out [][]string
	for i := 0; i < len(s); i += size {
		end := i + size
		if end > len(s) {
			end = len(s)
		}
		out = append(out, s[i:end])
	}
	return out
}

// ParseLogFields exposes parseLogFields for callers that already hold a norm_log
// string and need it as a field map without a round trip.
func (c *ClickHouseClient) ParseLogFields(ctx context.Context, fieldsStr string) map[string]interface{} {
	return c.parseLogFields(ctx, fieldsStr)
}

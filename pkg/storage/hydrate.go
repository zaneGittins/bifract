package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	// hydrateChunkSize bounds how many log_ids go into a single IN list, matching
	// the ceiling GetLogFieldsByIDs enforces.
	hydrateChunkSize = 500

	// hydrateWindowSlack widens the ingest window. The translator writes
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
}

// HydrateLogFields batch-fetches parsed field maps, keyed by log_id, for rows that
// came back from a projection-pruned query (see the alert auto-projection in
// pkg/parser/translator.go, which drops norm_log).
//
// table must be the table the originating query read, so a cluster deployment
// does not hydrate from logs_distributed what it matched on logs_hot_distributed.
// from/to must be an ingest window, which is what the caller matched on; both
// tables are partitioned on ingest_timestamp, so it prunes either way. A zero
// window is treated as unbounded rather than as an empty one.
// Rows that cannot be found are absent from the returned map.
func (c *ClickHouseClient) HydrateLogFields(ctx context.Context, table string, keys []LogKey, from, to time.Time) (map[string]map[string]interface{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if !hydrateTables[table] {
		return nil, fmt.Errorf("hydrate log fields: unsupported table %q", table)
	}

	fractalIDs := observedFractalIDs(keys)
	logIDs := dedupeLogIDs(keys)

	// An unset window would render as a range around year zero and match nothing,
	// silently returning every row unhydrated. Drop the bound instead and let the
	// log_id bloom and the fractal prune carry the lookup.
	bounded := !from.IsZero() && !to.IsZero()
	var bounds []interface{}
	if bounded {
		bounds = []interface{}{
			chTimeArg(from.Add(-hydrateWindowSlack)),
			chTimeArg(to.Add(hydrateWindowSlack)),
		}
	}
	query := hydrateQuery(table, bounded, len(fractalIDs) > 0)

	// Hydration is background work on the alert path and must never starve user
	// queries, same rationale as QueryLowPriority.
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(c.applyQuerySettings(ctx, clickhouse.Settings{
		"priority":       5,
		"max_query_size": maxGeneratedQuerySize,
	})))

	out := make(map[string]map[string]interface{}, len(keys))
	for _, chunk := range chunkStrings(logIDs, hydrateChunkSize) {
		args := append(append([]interface{}{}, bounds...), chunk)
		if len(fractalIDs) > 0 {
			args = append(args, fractalIDs)
		}
		if err := c.collectLogFields(ctx, out, query, args); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// hydrateQuery builds the lookup SQL. table is validated by the caller against
// hydrateTables, which guards the one interpolated token.
func hydrateQuery(table string, bounded, scoped bool) string {
	q := "SELECT log_id, norm_log FROM " + table + " WHERE "
	if bounded {
		q += "ingest_timestamp >= toDateTime64(?, 3, 'UTC') AND ingest_timestamp <= toDateTime64(?, 3, 'UTC') AND "
	}
	q += "log_id IN (?)"
	if scoped {
		q += " AND fractal_id IN (?)"
	}
	// Guards against re-ingested duplicate log_ids; on a Distributed table this is
	// applied on the initiator after the shard merge.
	return q + " LIMIT 1 BY log_id"
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

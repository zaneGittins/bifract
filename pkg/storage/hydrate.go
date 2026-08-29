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

// commentLogChunkSize bounds how many log_ids go into a single display-row IN list.
const commentLogChunkSize = 500

// displayWindowSlack widens the event-time window. The caller's bound comes from
// a denormalized copy of the event time (comments.log_timestamp), which has been
// through a JSON round trip and a Postgres column of different precision, so an
// exact match is not something to bet a lookup on.
const displayWindowSlack = time.Minute

// GetLogDisplayRowsByIDs batch-fetches display rows (timestamp, log_id, and
// norm_log as fields) for a set of log ids, keyed by log_id. Rows that cannot be
// found are absent from the returned map.
//
// from/to bound the event timestamp, which leads the logs ORDER BY, so the
// lookup prunes granules rather than bloom-checking every granule of every
// partition in the fractal. The bound is an optimisation, never a filter the
// caller asked for: any id the bounded pass misses is retried unbounded, so a
// drifted or wrong window costs an extra query rather than silently losing a
// log. A zero window skips straight to the unbounded pass.
func (c *ClickHouseClient) GetLogDisplayRowsByIDs(ctx context.Context, fractalID string, logIDs []string, from, to time.Time) (map[string]map[string]interface{}, error) {
	ids := dedupeLogIDs(logKeysFromIDs(logIDs))
	if len(ids) == 0 {
		return nil, nil
	}

	// Prefetch is background work behind a user action and must never starve
	// interactive queries, same rationale as HydrateLogFields.
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(c.applyQuerySettings(ctx, clickhouse.Settings{
		"priority":       5,
		"max_query_size": maxGeneratedQuerySize,
	})))

	out := make(map[string]map[string]interface{}, len(ids))

	if !from.IsZero() && !to.IsZero() {
		if err := c.fetchLogDisplayRows(ctx, out, fractalID, ids,
			from.Add(-displayWindowSlack), to.Add(displayWindowSlack)); err != nil {
			return nil, err
		}
		ids = missingLogIDs(ids, out)
		if len(ids) == 0 {
			return out, nil
		}
	}

	if err := c.fetchLogDisplayRows(ctx, out, fractalID, ids, time.Time{}, time.Time{}); err != nil {
		return nil, err
	}
	return out, nil
}

// fetchLogDisplayRows runs the chunked lookup for one window and merges the rows
// it finds into out. A zero from/to omits the event-time predicate.
func (c *ClickHouseClient) fetchLogDisplayRows(ctx context.Context, out map[string]map[string]interface{}, fractalID string, ids []string, from, to time.Time) error {
	bounded := !from.IsZero() && !to.IsZero()

	query := "SELECT timestamp, log_id, norm_log AS fields FROM " + c.ReadTable() + " WHERE log_id IN (?)"
	if fractalID != "" {
		query += " AND fractal_id = ?"
	}
	if bounded {
		query += " AND timestamp >= toDateTime64(?, 3, 'UTC') AND timestamp <= toDateTime64(?, 3, 'UTC')"
	}
	// Guards against re-ingested duplicate log_ids, matching HydrateLogFields.
	query += " LIMIT 1 BY log_id"

	for _, chunk := range chunkStrings(ids, commentLogChunkSize) {
		args := []interface{}{chunk}
		if fractalID != "" {
			args = append(args, fractalID)
		}
		if bounded {
			args = append(args, chTimeArg(from), chTimeArg(to))
		}
		if err := c.collectLogDisplayRows(ctx, out, query, args); err != nil {
			return err
		}
	}
	return nil
}

// missingLogIDs returns the ids that found no row.
func missingLogIDs(ids []string, found map[string]map[string]interface{}) []string {
	var out []string
	for _, id := range ids {
		if _, ok := found[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// collectLogDisplayRows runs one display-row query and merges its rows into out.
func (c *ClickHouseClient) collectLogDisplayRows(ctx context.Context, out map[string]map[string]interface{}, query string, args []interface{}) error {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("fetch log display rows: %w", err)
	}
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	for rows.Next() {
		row, err := scanRowMap(columnTypes, rows)
		if err != nil {
			return fmt.Errorf("fetch log display rows scan: %w", err)
		}
		if id, ok := row["log_id"].(string); ok && id != "" {
			out[id] = row
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("fetch log display rows iterate: %w", err)
	}
	return nil
}

// logKeysFromIDs adapts a plain id list to the LogKey shape dedupeLogIDs takes.
func logKeysFromIDs(ids []string) []LogKey {
	keys := make([]LogKey, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, LogKey{LogID: id})
	}
	return keys
}

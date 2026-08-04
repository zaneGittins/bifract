package alerts

import (
	"context"
	"log"
	"strings"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// hydrationRowCeiling bounds how many rows a single trigger will hydrate. It
// matches the system alerts fractal action's own MaxLogsPerTrigger, so an alert
// configured with no cap never pulls more full rows than that action forwards.
const hydrationRowCeiling = 1000

// logHydrator is the narrow ClickHouse dependency hydration needs, mirroring the
// engineNotifWriter seam so the row handling is testable without a live server.
type logHydrator interface {
	HydrateLogFields(ctx context.Context, table string, byIngest bool, keys []storage.LogKey, from, to time.Time) (map[string]map[string]interface{}, error)
}

// hydrationLimit returns how many result rows are worth hydrating for this alert,
// or 0 when no consumer needs full fields. Only fractal actions receive hydrated
// rows, so the limit is the largest number of rows any of them will forward.
func (e *Engine) hydrationLimit(ctx context.Context, alert *Alert) int {
	limit := 0
	for _, fa := range alert.FractalActions {
		if !fa.Enabled {
			continue
		}
		// MaxLogsPerTrigger <= 0 means unlimited (see FractalActionClient.Send).
		if fa.MaxLogsPerTrigger <= 0 {
			return hydrationRowCeiling
		}
		if fa.MaxLogsPerTrigger > limit {
			limit = fa.MaxLogsPerTrigger
		}
	}
	// The system alerts fractal receives every trigger, capped at 1000.
	if e.getAlertsFractalID(ctx) != "" && limit < hydrationRowCeiling {
		limit = hydrationRowCeiling
	}
	if limit > hydrationRowCeiling {
		limit = hydrationRowCeiling
	}
	return limit
}

// hydrateRows re-fetches the full field set for triggered rows by log_id and
// returns a copy of results carrying it under the "fields" key.
//
// Alert queries project only the fields they filter on (see the auto-projection
// in pkg/parser/translator.go), which keeps the per-tick scan cheap but leaves
// forwarded logs lossy. Refetching on trigger moves that cost off the evaluation
// path, where it would be paid by every alert on every tick.
//
// The input rows are never mutated: webhook, email, and dictionary actions still
// hold them and must keep seeing the pruned shape. Any failure degrades to the
// pruned rows rather than dropping the forward.
func (e *Engine) hydrateRows(ctx context.Context, results []map[string]interface{}, opts parser.QueryOptions, limit int) []map[string]interface{} {
	if e.hydrator == nil || limit <= 0 || len(results) == 0 {
		return results
	}
	if limit > len(results) {
		limit = len(results)
	}

	keys := make([]storage.LogKey, 0, limit)
	for _, row := range results[:limit] {
		if !isPrunedLogRow(row) {
			continue
		}
		key := storage.LogKey{LogID: row["log_id"].(string)}
		key.FractalID, _ = row["fractal_id"].(string)
		key.Timestamp, _ = storage.RowTime(row, "timestamp")
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return results
	}

	// Mirror the table the evaluation read. The hot table is keyed on
	// ingest_timestamp, which the alert's own window bounds; the main logs table
	// is keyed on timestamp, which is unrelated to that window.
	byIngest := strings.HasPrefix(opts.TableName, "logs_hot")

	fields, err := e.hydrator.HydrateLogFields(ctx, opts.TableName, byIngest, keys, opts.StartTime, opts.EndTime)
	if err != nil {
		log.Printf("[Alert Engine] Hydration failed, forwarding pruned rows: %v", err)
		return results
	}
	if len(fields) == 0 {
		return results
	}

	hydrated := make([]map[string]interface{}, len(results))
	copy(hydrated, results)
	for i, row := range results[:limit] {
		logID, _ := row["log_id"].(string)
		f, ok := fields[logID]
		if !ok || len(f) == 0 {
			continue
		}
		enriched := make(map[string]interface{}, len(row)+1)
		for k, v := range row {
			enriched[k] = v
		}
		enriched["fields"] = f
		hydrated[i] = enriched
	}
	return hydrated
}

// isPrunedLogRow reports whether a row is a log row that lost its fields to the
// alert auto-projection. Aggregate rows have no log_id; command pipelines and
// Recall keep the full projection, which carries norm_log (as a raw string) or
// an already-parsed fields map. Hydrating any of those is wasted work.
func isPrunedLogRow(row map[string]interface{}) bool {
	if logID, _ := row["log_id"].(string); logID == "" {
		return false
	}
	if _, ok := row["fields"]; ok {
		return false
	}
	_, hasNormLog := row["norm_log"]
	return !hasNormLog
}

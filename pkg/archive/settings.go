package archive

import (
	"context"
	"database/sql"
	"strconv"
	"time"
)

// Recall knobs are admin-editable from the settings page (pkg/settings owns the
// HTTP surface and validation). The workers read the same keys straight from the
// settings table so they stay decoupled from that package and pick up changes
// live, without a restart. Defaults MUST match pkg/settings.
const (
	defaultRecallTimeout     = 15 * time.Minute
	defaultRecallConcurrency = 5
	// RecallWorkerPool is how many search-worker loops are spawned. The live
	// recall_concurrency setting (the claim-time cap) can be raised up to this
	// without a restart; extra idle workers simply find no free slot. Must be >=
	// pkg/settings.maxRecallConcurrency.
	RecallWorkerPool = 16
)

// recallTimeout returns the live per-search timeout: the recall_timeout_seconds
// setting when present and sane, else the configured fallback (env/default).
func recallTimeout(ctx context.Context, db *sql.DB, fallback time.Duration) time.Duration {
	if n := getIntSetting(ctx, db, "recall_timeout_seconds", 0); n >= 30 {
		return time.Duration(n) * time.Second
	}
	if fallback > 0 {
		return fallback
	}
	return defaultRecallTimeout
}

// recallConcurrency returns the live recall search concurrency cap, clamped to
// [1, RecallWorkerPool].
func recallConcurrency(ctx context.Context, db *sql.DB) int {
	n := getIntSetting(ctx, db, "recall_concurrency", defaultRecallConcurrency)
	if n < 1 {
		return 1
	}
	if n > RecallWorkerPool {
		return RecallWorkerPool
	}
	return n
}

func getIntSetting(ctx context.Context, db *sql.DB, key string, def int) int {
	if v := getSetting(ctx, db, key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// getSetting reads one settings-table value, returning "" on any miss or error
// so callers fall back to their default. Runs on a short deadline so a slow
// Postgres never stalls a claim loop or a job start.
func getSetting(ctx context.Context, db *sql.DB, key string) string {
	if db == nil {
		return ""
	}
	qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var value string
	if err := db.QueryRowContext(qctx, `SELECT value FROM settings WHERE key = $1`, key).Scan(&value); err != nil {
		return ""
	}
	return value
}

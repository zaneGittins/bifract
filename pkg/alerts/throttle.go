package alerts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"unicode/utf8"

	"github.com/lib/pq"
)

const (
	// maxThrottleKeyLen bounds a stored key so a throttle field holding
	// something long (a command line) still fits the throttle_key columns.
	maxThrottleKeyLen = 200

	// globalThrottleKey is the key an alert with no throttle field uses, so its
	// whole result set shares one suppression window.
	globalThrottleKey = "global"
)

// throttleResult is the split of one result set into the rows an evaluation may
// deliver to actions and the rows still inside a suppression window.
type throttleResult struct {
	deliver []map[string]interface{}
	// keys holds the distinct keys of deliver, the windows to open once the
	// actions have run.
	keys []string
	// suppressed counts the rows withheld.
	suppressed int
	// auditKey describes the batch for the alert_executions row.
	auditKey string
}

// rowThrottleKey returns the suppression key for one result row. A throttle
// field suppresses per value, so two hosts hitting the same alert hold
// independent windows; without one, every row shares the global key.
func rowThrottleKey(alert *Alert, row map[string]interface{}) string {
	if alert.ThrottleField == "" {
		return globalThrottleKey
	}
	val, ok := row[alert.ThrottleField]
	if !ok {
		return globalThrottleKey
	}
	return truncateThrottleKey(fmt.Sprintf("%s=%v", alert.ThrottleField, val))
}

// truncateThrottleKey shortens an oversized key on a rune boundary (a split rune
// is not valid input for a text column) and appends a digest of the full key so
// two long values that share a prefix keep separate windows.
func truncateThrottleKey(key string) string {
	if len(key) <= maxThrottleKeyLen {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	cut := maxThrottleKeyLen - 17
	for cut > 0 && !utf8.RuneStart(key[cut]) {
		cut--
	}
	return key[:cut] + ":" + hex.EncodeToString(sum[:8])
}

// applyThrottle splits results by suppression key, reading the open windows from
// Postgres.
func (e *Engine) applyThrottle(ctx context.Context, alert *Alert, results []map[string]interface{}) throttleResult {
	return throttleSplit(alert, results, func(keys []string) map[string]struct{} {
		return e.suppressedThrottleKeys(ctx, alert.ID, keys)
	})
}

// throttleSplit withholds the rows whose key is inside its window and delivers
// every other row, so a value that has not fired recently still reaches its
// actions when another value in the same batch is suppressed. blocked reports
// which of the batch's keys are currently open.
func throttleSplit(alert *Alert, results []map[string]interface{}, blocked func(keys []string) map[string]struct{}) throttleResult {
	if alert.ThrottleTimeSeconds <= 0 || len(results) == 0 {
		return throttleResult{deliver: results}
	}

	rowKeys := make([]string, len(results))
	distinct := make([]string, 0, len(results))
	seen := make(map[string]struct{}, len(results))
	for i, row := range results {
		key := rowThrottleKey(alert, row)
		rowKeys[i] = key
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			distinct = append(distinct, key)
		}
	}

	out := throttleResult{auditKey: auditThrottleKey(alert, distinct)}

	open := blocked(distinct)
	if len(open) == 0 {
		out.deliver = results
		out.keys = distinct
		return out
	}

	out.deliver = make([]map[string]interface{}, 0, len(results))
	out.keys = make([]string, 0, len(distinct))
	kept := make(map[string]struct{}, len(distinct))
	for i, row := range results {
		if _, ok := open[rowKeys[i]]; ok {
			out.suppressed++
			continue
		}
		out.deliver = append(out.deliver, row)
		if _, ok := kept[rowKeys[i]]; !ok {
			kept[rowKeys[i]] = struct{}{}
			out.keys = append(out.keys, rowKeys[i])
		}
	}
	return out
}

// auditThrottleKey names the batch in the execution audit row: the key itself
// when the batch holds one, otherwise how many values it spanned.
func auditThrottleKey(alert *Alert, distinct []string) string {
	switch {
	case len(distinct) == 1:
		return distinct[0]
	case alert.ThrottleField != "":
		return truncateThrottleKey(fmt.Sprintf("%s (%d values)", alert.ThrottleField, len(distinct)))
	default:
		return globalThrottleKey
	}
}

var missingThrottleTable sync.Once

// logThrottleStoreErr reports a failure to reach the throttle table. A missing
// table means the install has not run its migrations, which every firing
// evaluation would otherwise report once per cycle forever, so that case is
// named once and then stays quiet.
func logThrottleStoreErr(what string, err error) {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		missingThrottleTable.Do(func() {
			log.Printf("[Alert Engine] Table alert_throttles is missing, so throttles suppress nothing: run bifract-setup to apply pending migrations")
		})
		return
	}
	log.Printf("[Alert Engine] %s: %v", what, err)
}

// suppressedThrottleKeys returns which of keys are still inside their window.
// State lives in Postgres rather than process memory: an in-process cache
// released every window on restart and was invisible to whichever replica held
// the evaluation lock next. A read failure suppresses nothing, since a delivered
// duplicate is recoverable and a dropped detection is not.
func (e *Engine) suppressedThrottleKeys(ctx context.Context, alertID string, keys []string) map[string]struct{} {
	blocked := make(map[string]struct{})
	if e.pg == nil || len(keys) == 0 {
		return blocked
	}

	rows, err := e.pg.Query(ctx,
		`SELECT throttle_key FROM alert_throttles
		 WHERE alert_id = $1 AND throttle_key = ANY($2) AND expires_at > NOW()`,
		alertID, pq.Array(keys))
	if err != nil {
		logThrottleStoreErr("throttle lookup failed for alert "+alertID, err)
		return blocked
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			log.Printf("[Alert Engine] Throttle lookup scan failed for alert %s: %v", alertID, err)
			return map[string]struct{}{}
		}
		blocked[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		log.Printf("[Alert Engine] Throttle lookup failed for alert %s: %v", alertID, err)
		return map[string]struct{}{}
	}
	return blocked
}

// openThrottleWindows starts (or extends) the suppression window for every key
// just delivered.
func (e *Engine) openThrottleWindows(ctx context.Context, alert *Alert, keys []string) {
	if e.pg == nil || alert.ThrottleTimeSeconds <= 0 || len(keys) == 0 {
		return
	}

	_, err := e.pg.Exec(ctx,
		`INSERT INTO alert_throttles (alert_id, throttle_key, expires_at)
		 SELECT $1, key, NOW() + make_interval(secs => $3) FROM unnest($2::text[]) AS key
		 ON CONFLICT (alert_id, throttle_key) DO UPDATE SET expires_at = EXCLUDED.expires_at`,
		alert.ID, pq.Array(keys), float64(alert.ThrottleTimeSeconds))
	if err != nil {
		// The alert was deleted mid-evaluation; nothing left to throttle.
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return
		}
		logThrottleStoreErr("failed to record throttle for alert "+alert.Name, err)
	}
}

// cleanupThrottles drops expired windows. Rows are per (alert, value), so a
// high-cardinality throttle field would otherwise accumulate them indefinitely.
func (e *Engine) cleanupThrottles(ctx context.Context) {
	if e.pg == nil {
		return
	}
	result, err := e.pg.Exec(ctx, `DELETE FROM alert_throttles WHERE expires_at < NOW() - INTERVAL '1 hour'`)
	if err != nil {
		logThrottleStoreErr("throttle cleanup failed", err)
		return
	}
	if count, _ := result.RowsAffected(); count > 0 {
		log.Printf("[Alert Engine] Retention cleanup removed %d expired throttles", count)
	}
}

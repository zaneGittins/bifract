package schemafields

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// overflowLockID elects a single replica to run the monitor. The query is
// read-only so concurrent runs would be correct, just wasteful: it is the most
// expensive query this package issues, and every replica repeating it every
// interval would be pure duplicated load.
const overflowLockID int64 = 0x6269667261637402

// overflowCheckedAtKey records the last completed sweep in the settings table so
// the UI can say when the figure was measured rather than implying it is live.
const overflowCheckedAtKey = "schema_overflow_checked_at"

// overflowSampleSize bounds the scan. Unlike the norm_log sample, cost here is
// dominated by opening one file per JSON path per part rather than by rows, so a
// smaller sample buys much less than it does elsewhere. It stays modest because
// detecting *which* paths have spilled needs presence, not distribution -- a
// systemically overflowing field shows up just as reliably in a few thousand
// recent rows as in twenty thousand, so the sample stays small deliberately.
func overflowSampleSize() int {
	if v := os.Getenv("BIFRACT_SCHEMA_OVERFLOW_SAMPLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 2000
}

// overflowMaxMemoryBytes bounds the detect query to a small, explicit budget (see
// storage.QueryLowPriorityBounded) so it can only ever fail itself cleanly -- it must never be
// able to grow large enough to threaten unrelated background work (merges, mutations) under the
// server's global memory ceiling.
func overflowMaxMemoryBytes() int64 {
	if v := os.Getenv("BIFRACT_SCHEMA_OVERFLOW_MAX_MEMORY_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 536870912 // 512MB
}

func overflowInterval() time.Duration {
	if v := os.Getenv("BIFRACT_SCHEMA_OVERFLOW_INTERVAL_MINUTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Minute
		}
	}
	return 30 * time.Minute
}

// notifier is the health-notification sink, kept as a narrow interface so this
// package does not import pkg/notifications.
type notifier interface {
	Write(notifType, severity, title, message string) error
}

// OverflowMonitor periodically looks for JSON paths that have spilled out of
// their own sub-column, records them, and raises a health notification.
//
// This is the one schema signal that earns an interrupt. It is a live
// degradation rather than advice: the affected fields are silently scanning
// every row right now, and the condition clears as soon as they are reserved.
// Everything else the schema tab reports is standing configuration guidance and
// stays passive, so the bell keeps meaning "something is wrong".
type OverflowMonitor struct {
	ch     *storage.ClickHouseClient
	pg     *storage.PostgresClient
	notify notifier
}

func NewOverflowMonitor(ch *storage.ClickHouseClient, pg *storage.PostgresClient, n notifier) *OverflowMonitor {
	return &OverflowMonitor{ch: ch, pg: pg, notify: n}
}

// Start runs the monitor until ctx is cancelled. The first sweep is delayed so
// it never competes with startup work (migrations, schema reconcile, MV setup).
func (m *OverflowMonitor) Start(ctx context.Context) {
	if m.ch == nil || m.pg == nil {
		return
	}
	go func() {
		interval := overflowInterval()
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Minute):
		}
		for {
			m.runOnce(ctx)
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
}

// runOnce performs one sweep, guarded by the advisory lock.
func (m *OverflowMonitor) runOnce(ctx context.Context) {
	unlock, ok := m.pg.TryAdvisoryLock(ctx, overflowLockID)
	if !ok {
		return // another replica owns this sweep
	}
	defer unlock()

	// The sweep is expensive by nature; bound it so a slow cluster cannot leave
	// the advisory lock held indefinitely and starve every later sweep.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	found, err := m.detect(ctx)
	if err != nil {
		log.Printf("[SchemaOverflow] detect: %v", err)
		return
	}
	if err := m.persist(ctx, found); err != nil {
		log.Printf("[SchemaOverflow] persist: %v", err)
		return
	}
	_ = m.pg.SetSetting(ctx, overflowCheckedAtKey, time.Now().UTC().Format(time.RFC3339))

	if len(found) > 0 {
		m.raise(ctx, found)
	}
}

type overflowField struct {
	Name string
	Rows uint64
}

// overflowWindowDays are the sample windows tried in order. Recency comes from a
// time predicate rather than ORDER BY (see detect), so a window that is too narrow
// simply yields a thin sample; widen until the sample is usable. An install whose
// newest data is older than the widest window has no current schema to assess, so
// the sweep reports nothing rather than scanning history.
var overflowWindowDays = []int{1, 7, 30}

// overflowMinSample is the row count below which a window is considered too thin
// and the next wider one is tried.
const overflowMinSample = 200

// detect samples the JSON column for paths held in shared storage.
//
// Two properties of this query are load-bearing and easy to undo by accident:
//
//  1. JSONSharedDataPaths(fields) is evaluated directly against the table scan.
//     Materializing `fields` through an intermediate step (notably ORDER BY inside
//     the subquery) re-serializes the JSON into a new block, and shared-data
//     allocation is then recomputed for that block rather than read from the part.
//     The paths reported become an artifact of the query plan: measured against a
//     fixture whose parts truly overflowed `extra2`/`extra3`, the ordered form
//     reported five unrelated names.
//
//  2. Recency comes from a timestamp predicate, which prunes partitions. The
//     previous `ORDER BY timestamp DESC LIMIT n` forced a reverse-order merge over
//     every part, opening the wide JSON column in each: 248k rows read for a 2k
//     limit, 1.14 GiB peak, over the 512 MiB budget, so the sweep never completed.
//     The predicate form measured 794 KiB and 7 ms on the same data.
func (m *OverflowMonitor) detect(ctx context.Context) ([]overflowField, error) {
	for i, days := range overflowWindowDays {
		last := i == len(overflowWindowDays)-1
		// Widen on a thin sample, never on a clean one: "sampled plenty of recent
		// rows, found no overflow" is the healthy answer and must terminate here,
		// or a healthy install would escalate every sweep and stale overflow from
		// weeks ago would resurface as if it were current.
		sampled, err := m.sampleSize(ctx, days)
		if err != nil {
			return nil, err
		}
		if sampled < overflowMinSample && !last {
			continue
		}
		if sampled == 0 {
			return nil, nil // no recent data: nothing to assess
		}
		return m.detectWindow(ctx, days)
	}
	return nil, nil
}

// windowPredicate bounds a sample to the trailing `days` of data. The window is
// anchored at the newest log so an install that stopped ingesting still assesses
// its own most recent data, but clamped to now() so a single future-dated
// timestamp (parsed timestamps are untrusted user data) cannot push the window
// past every real row.
func (m *OverflowMonitor) windowPredicate(days int) string {
	return fmt.Sprintf("timestamp >= least((SELECT max(timestamp) FROM %s), now64(3)) - INTERVAL %d DAY",
		m.ch.ReadTable(), days)
}

// sampleSize reports how many rows the window would contribute, without touching
// the JSON column, so a thin window is cheap to detect.
func (m *OverflowMonitor) sampleSize(ctx context.Context, days int) (uint64, error) {
	sql := fmt.Sprintf("SELECT count() AS n FROM (SELECT 1 FROM %s WHERE %s LIMIT %d)",
		m.ch.ReadTable(), m.windowPredicate(days), overflowSampleSize())
	rows, err := m.ch.QueryLowPriorityBounded(ctx, sql, overflowMaxMemoryBytes())
	if err != nil {
		return 0, fmt.Errorf("sample size (%dd window): %w", days, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return asUint64(rows[0]["n"]), nil
}

// detectWindow runs the shared-data path sample over the trailing `days` of data.
func (m *OverflowMonitor) detectWindow(ctx context.Context, days int) ([]overflowField, error) {
	sql := fmt.Sprintf(`SELECT p AS field_name, count() AS rows_seen
FROM (SELECT JSONSharedDataPaths(fields) AS paths FROM %s WHERE %s LIMIT %d)
ARRAY JOIN paths AS p
GROUP BY p ORDER BY rows_seen DESC LIMIT 200`,
		m.ch.ReadTable(), m.windowPredicate(days), overflowSampleSize())

	rows, err := m.ch.QueryLowPriorityBounded(ctx, sql, overflowMaxMemoryBytes())
	if err != nil {
		return nil, fmt.Errorf("shared-data path query (%dd window): %w", days, err)
	}
	out := make([]overflowField, 0, len(rows))
	for _, r := range rows {
		name, _ := r["field_name"].(string)
		if name == "" {
			continue
		}
		out = append(out, overflowField{Name: name, Rows: asUint64(r["rows_seen"])})
	}
	return out, nil
}

// persist replaces the recorded overflow set. Fields that no longer spill are
// deleted so the condition genuinely clears once resolved, which is what makes
// the notification self-resolving rather than something to be dismissed.
func (m *OverflowMonitor) persist(ctx context.Context, found []overflowField) error {
	tx, err := m.pg.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM schema_field_overflow`); err != nil {
		return fmt.Errorf("clear: %w", err)
	}
	for _, f := range found {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_field_overflow (field_name, rows_seen) VALUES ($1, $2)
			 ON CONFLICT (field_name) DO UPDATE SET rows_seen = $2, detected_at = NOW()`,
			f.Name, int64(f.Rows)); err != nil {
			return fmt.Errorf("insert %q: %w", f.Name, err)
		}
	}
	return tx.Commit()
}

// raise emits the health notification, skipping fields an admin has already
// dismissed. Without that filter, deciding some junk field is not worth a column
// would leave the warning firing forever, and the notification would train
// people to ignore the bell.
func (m *OverflowMonitor) raise(ctx context.Context, found []overflowField) {
	if m.notify == nil {
		return
	}
	ignored, err := (&Manager{pg: m.pg}).ListIgnored(ctx)
	if err != nil {
		log.Printf("[SchemaOverflow] read ignore list: %v", err)
		ignored = map[string]bool{}
	}
	names := make([]string, 0, len(found))
	for _, f := range found {
		if !ignored[f.Name] {
			names = append(names, f.Name)
		}
	}
	if len(names) == 0 {
		return
	}
	sort.Strings(names)

	shown := names
	if len(shown) > 5 {
		shown = shown[:5]
	}
	msg := fmt.Sprintf("%s no longer have a column of their own, so queries on them scan every row. "+
		"Reserve them on the Schema tab to restore pruning.", strings.Join(shown, ", "))
	if len(names) > len(shown) {
		msg = fmt.Sprintf("%s and %d more %s", strings.Join(shown, ", "), len(names)-len(shown), msg[strings.Index(msg, "no longer"):])
	}
	title := fmt.Sprintf("%d field%s out of schema capacity", len(names), plural(len(names)))
	if err := m.notify.Write("schema.capacity_exhausted", "warning", title, msg); err != nil {
		log.Printf("[SchemaOverflow] notify: %v", err)
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// OverflowField is a field that has lost its own column. Addable reports whether
// reserving it is actually possible: real logs contain keys like
// "resp_headers_cache-_control" that ClickHouse can hint but validFieldName
// rejects, and offering a fix button that always errors is worse than saying so.
type OverflowField struct {
	Name    string `json:"name"`
	Addable bool   `json:"addable"`
}

// listOverflow reads the cached overflow set for the insights payload.
func listOverflow(ctx context.Context, pg *storage.PostgresClient) ([]OverflowField, time.Time, error) {
	rows, err := pg.Query(ctx, `SELECT field_name FROM schema_field_overflow ORDER BY rows_seen DESC`)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("query overflow: %w", err)
	}
	defer rows.Close()

	// Non-nil so the field marshals as [] rather than null: the UI treats the two
	// the same, but null reads as "unknown" in an API and this is genuinely "none".
	out := []OverflowField{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, time.Time{}, err
		}
		out = append(out, OverflowField{Name: name, Addable: suggestable(name)})
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, err
	}

	var checked time.Time
	if v, err := pg.GetSetting(ctx, overflowCheckedAtKey); err == nil && v != "" {
		checked, _ = time.Parse(time.RFC3339, v)
	}
	return out, checked, nil
}

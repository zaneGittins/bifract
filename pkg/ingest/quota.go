package ingest

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"bifract/pkg/storage"
)

const (
	quotaRefreshInterval = 5 * time.Minute
	// After rollover, trim to this fraction of quota to avoid immediate re-trigger.
	rolloverTargetFraction = 0.80
)

// fractalQuotaState holds cached quota config and estimated usage for one fractal.
type fractalQuotaState struct {
	quotaBytes   int64  // 0 = no limit
	action       string // "reject" or "rollover"
	baseBytes    int64  // size_bytes from last Postgres refresh
	baseLogCount int64  // log_count from last Postgres refresh

	// in-memory deltas since last cache refresh (not yet in Postgres)
	deltaBytes atomic.Int64
	deltaCount atomic.Int64

	// CAS guard: 1 when a rollover goroutine is already running
	rollingOver atomic.Int32
}

// QuotaManager tracks per-fractal disk usage and enforces configured quotas.
// It reads size_bytes / log_count from Postgres (already maintained by the
// RefreshFractalStats background job) as a base, and layers in-memory deltas
// on top for accuracy between the 10-minute stats refreshes.
type QuotaManager struct {
	pg *storage.PostgresClient
	ch *storage.ClickHouseClient

	mu    sync.RWMutex
	state map[string]*fractalQuotaState

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewQuotaManager creates and starts a QuotaManager.
func NewQuotaManager(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *QuotaManager {
	qm := &QuotaManager{
		pg:    pg,
		ch:    ch,
		state: make(map[string]*fractalQuotaState),
		stop:  make(chan struct{}),
	}
	if err := qm.loadFromPostgres(); err != nil {
		log.Printf("[Quota] Initial load failed (will retry): %v", err)
	}
	qm.wg.Add(1)
	go qm.refreshLoop()
	return qm
}

// Stop halts the background refresh goroutine.
func (qm *QuotaManager) Stop() {
	close(qm.stop)
	qm.wg.Wait()
}

// CheckQuota returns false when the batch should be rejected due to quota.
// Always returns true if the fractal has no quota or if the action is "rollover".
func (qm *QuotaManager) CheckQuota(fractalID string, batchBytes int64) bool {
	qm.mu.RLock()
	st := qm.state[fractalID]
	qm.mu.RUnlock()

	if st == nil || st.quotaBytes == 0 {
		return true
	}
	if st.action != "reject" {
		return true
	}
	estimated := st.baseBytes + st.deltaBytes.Load()
	return estimated+batchBytes <= st.quotaBytes
}

// RecordInsert updates in-memory deltas after a successful ClickHouse insert.
// For rollover fractals it triggers async cleanup if the quota is now exceeded.
func (qm *QuotaManager) RecordInsert(fractalID string, batchBytes, batchCount int64) {
	qm.mu.RLock()
	st := qm.state[fractalID]
	qm.mu.RUnlock()

	if st == nil {
		return
	}

	st.deltaBytes.Add(batchBytes)
	st.deltaCount.Add(batchCount)

	if st.quotaBytes == 0 || st.action != "rollover" {
		return
	}

	estimated := st.baseBytes + st.deltaBytes.Load()
	if estimated > st.quotaBytes {
		if st.rollingOver.CompareAndSwap(0, 1) {
			go func() {
				defer st.rollingOver.Store(0)
				if err := qm.triggerRollover(fractalID, st); err != nil {
					log.Printf("[Quota] Rollover failed for fractal %s: %v", fractalID, err)
				}
			}()
		}
	}
}

// NotifyCleared resets in-memory deltas for a fractal whose logs were just cleared.
func (qm *QuotaManager) NotifyCleared(fractalID string) {
	qm.mu.RLock()
	st := qm.state[fractalID]
	qm.mu.RUnlock()

	if st == nil {
		return
	}
	st.deltaBytes.Store(0)
	st.deltaCount.Store(0)
}

// triggerRollover deletes the oldest logs for fractalID until usage is back
// within rolloverTargetFraction of the quota.
func (qm *QuotaManager) triggerRollover(fractalID string, st *fractalQuotaState) error {
	estimated := st.baseBytes + st.deltaBytes.Load()
	totalCount := st.baseLogCount + st.deltaCount.Load()

	target := int64(float64(st.quotaBytes) * rolloverTargetFraction)
	excess := estimated - target
	if excess <= 0 || totalCount == 0 {
		return nil
	}

	avgBytesPerLog := estimated / totalCount
	if avgBytesPerLog < 1 {
		avgBytesPerLog = 1
	}
	logsToDelete := (excess + avgBytesPerLog - 1) / avgBytesPerLog

	log.Printf("[Quota] Rolling over fractal %s: estimated %d bytes, quota %d, deleting ~%d oldest logs",
		fractalID, estimated, st.quotaBytes, logsToDelete)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Find the timestamp of the logsToDelete-th oldest log. The primary key
	// is (fractal_id, timestamp, log_id) so this scan is index-efficient.
	cutoffQuery := fmt.Sprintf(
		`SELECT max(timestamp) FROM (
			SELECT timestamp FROM logs WHERE fractal_id = '%s'
			ORDER BY fractal_id ASC, timestamp ASC
			LIMIT %d
		)`,
		storage.EscCHStr(fractalID), logsToDelete,
	)

	rows, err := qm.ch.Query(ctx, cutoffQuery)
	if err != nil {
		return fmt.Errorf("failed to find rollover cutoff: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	cutoffRaw, ok := rows[0]["max(timestamp)"]
	if !ok || cutoffRaw == nil {
		return nil
	}
	cutoff, ok := cutoffRaw.(string)
	if !ok || cutoff == "" {
		return nil
	}

	deleteQuery := fmt.Sprintf(
		"ALTER TABLE logs DELETE WHERE fractal_id = '%s' AND timestamp <= '%s'",
		storage.EscCHStr(fractalID), cutoff,
	)
	if err := qm.ch.Exec(ctx, deleteQuery); err != nil {
		return fmt.Errorf("rollover delete failed: %w", err)
	}

	// Update in-memory estimate; the next stats refresh will resync exact values.
	freed := logsToDelete * avgBytesPerLog
	st.deltaBytes.Add(-freed)
	if st.deltaBytes.Load() < -st.baseBytes {
		st.deltaBytes.Store(-st.baseBytes)
	}
	st.deltaCount.Add(-logsToDelete)
	if st.deltaCount.Load() < -st.baseLogCount {
		st.deltaCount.Store(-st.baseLogCount)
	}

	log.Printf("[Quota] Rollover complete for fractal %s, freed ~%d bytes", fractalID, freed)
	return nil
}

// refreshLoop periodically reloads quota config and current size_bytes from Postgres.
// Resetting the base values also resets the deltas to zero since the stats already
// reflect all inserted data.
func (qm *QuotaManager) refreshLoop() {
	defer qm.wg.Done()
	ticker := time.NewTicker(quotaRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-qm.stop:
			return
		case <-ticker.C:
			if err := qm.loadFromPostgres(); err != nil {
				log.Printf("[Quota] Refresh failed: %v", err)
			}
		}
	}
}

// loadFromPostgres reads quota config + current stats for all fractals.
func (qm *QuotaManager) loadFromPostgres() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := qm.pg.Query(ctx,
		`SELECT id, COALESCE(disk_quota_bytes, 0), COALESCE(disk_quota_action, 'reject'),
		        COALESCE(size_bytes, 0), COALESCE(log_count, 0)
		 FROM fractals`)
	if err != nil {
		return fmt.Errorf("query fractals: %w", err)
	}
	defer rows.Close()

	qm.mu.Lock()
	defer qm.mu.Unlock()

	for rows.Next() {
		var id string
		var quotaBytes, sizeBytes, logCount int64
		var action string
		if err := rows.Scan(&id, &quotaBytes, &action, &sizeBytes, &logCount); err != nil {
			return fmt.Errorf("scan fractal row: %w", err)
		}
		if existing, ok := qm.state[id]; ok {
			// Re-seat base values; deltas reset to 0 because Postgres now reflects them.
			existing.quotaBytes = quotaBytes
			existing.action = action
			existing.baseBytes = sizeBytes
			existing.baseLogCount = logCount
			existing.deltaBytes.Store(0)
			existing.deltaCount.Store(0)
		} else {
			st := &fractalQuotaState{
				quotaBytes:   quotaBytes,
				action:       action,
				baseBytes:    sizeBytes,
				baseLogCount: logCount,
			}
			qm.state[id] = st
		}
	}
	return rows.Err()
}


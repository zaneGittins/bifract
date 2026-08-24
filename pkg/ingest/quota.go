package ingest

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"bifract/pkg/storage"
)

const (
	quotaRefreshInterval = 5 * time.Minute
	// rolloverSweepInterval is how often the app tier scans for over-quota
	// rollover fractals and trims them by dropping whole partitions.
	rolloverSweepInterval = time.Minute
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
}

// QuotaManager tracks per-fractal disk usage and enforces configured quotas.
// It reads size_bytes / log_count from Postgres (already maintained by the
// RefreshFractalStats background job) as a base, and layers in-memory deltas
// on top for accuracy between the 10-minute stats refreshes.
type QuotaManager struct {
	pg *storage.PostgresClient
	ch *storage.ClickHouseClient

	// admin is an admin-level ClickHouse client (all shard addresses) used only by
	// the rollover sweep to DROP PARTITION. Set via StartRolloverSweep and only in
	// the app tier; the ingest tier's ch is an INSERT-only user that cannot drop
	// partitions, so it never runs the sweep.
	admin *storage.ClickHouseClient

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

// RecordInsert updates in-memory deltas after a successful ClickHouse insert, so
// the reject path (CheckQuota) has an accurate estimate between Postgres refreshes.
// Rollover enforcement is handled out-of-band by the app tier's sweep (see
// StartRolloverSweep); the ingest tier's ClickHouse user cannot DROP PARTITION.
func (qm *QuotaManager) RecordInsert(fractalID string, batchBytes, batchCount int64) {
	qm.mu.RLock()
	st := qm.state[fractalID]
	qm.mu.RUnlock()

	if st == nil {
		return
	}

	st.deltaBytes.Add(batchBytes)
	st.deltaCount.Add(batchCount)
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

// rolloverAdvisoryLockID is a Postgres session advisory-lock id ensuring only one
// app-tier replica runs the rollover sweep at a time. "bifract\x02".
const rolloverAdvisoryLockID int64 = 0x6269667261637402

// StartRolloverSweep launches the background loop that enforces rollover-action
// quotas by dropping whole (fractal, oldest ingest day) partitions. Only the app tier
// should call this, passing an admin ClickHouse client (all shard addresses); the
// ingest tier's INSERT-only user cannot DROP PARTITION. Under multiple app-tier
// replicas a Postgres advisory lock guarantees a single active runner per tick, so
// pods never over-trim by racing on the same fractal.
func (qm *QuotaManager) StartRolloverSweep(admin *storage.ClickHouseClient) {
	if admin == nil {
		log.Printf("[Quota] Rollover sweep not started: nil admin client")
		return
	}
	qm.admin = admin
	qm.wg.Add(1)
	go qm.rolloverLoop()
}

func (qm *QuotaManager) rolloverLoop() {
	defer qm.wg.Done()
	ticker := time.NewTicker(rolloverSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-qm.stop:
			return
		case <-ticker.C:
			qm.sweepRollovers()
		}
	}
}

// sweepRollovers finds every rollover-action fractal that is over quota and drops
// its oldest partitions until it is back within rolloverTargetFraction. Whole
// partitions (one fractal ingest-day each) are dropped, which is near-instant
// metadata, not a mutation. Oldest is oldest-ingested, so this is true FIFO: it
// evicts what has been held longest rather than whichever day happens to carry the
// oldest event timestamps. system.parts is the authoritative fresh usage source, so the
// sweep is self-correcting and needs no delta bookkeeping.
func (qm *QuotaManager) sweepRollovers() {
	quotas := make(map[string]int64)
	qm.mu.RLock()
	for id, st := range qm.state {
		if st.quotaBytes > 0 && st.action == "rollover" {
			quotas[id] = st.quotaBytes
		}
	}
	qm.mu.RUnlock()
	if len(quotas) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Single active runner across app-tier replicas. Non-blocking: if another pod
	// holds the lock, skip this tick and try again next interval. The lock is
	// session-scoped and pinned to a dedicated connection, so if this pod crashes
	// mid-sweep Postgres drops the session and auto-releases the lock -- it cannot
	// get stuck. The sweep is also idempotent (DROP PARTITION no-ops if repeated),
	// so a re-run after a crash is safe.
	unlock, acquired := qm.pg.TryAdvisoryLock(ctx, rolloverAdvisoryLockID)
	if !acquired {
		return
	}
	defer unlock()

	parts, err := qm.admin.FractalPartitionUsage(ctx)
	if err != nil {
		log.Printf("[Quota] Rollover sweep: partition usage query failed: %v", err)
		return
	}

	byFractal := make(map[string][]storage.FractalPartition)
	usage := make(map[string]int64)
	for _, p := range parts {
		if _, ok := quotas[p.FractalID]; !ok {
			continue
		}
		byFractal[p.FractalID] = append(byFractal[p.FractalID], p)
		usage[p.FractalID] += p.Bytes
	}

	for fid, quota := range quotas {
		total := usage[fid]
		if total <= quota {
			continue
		}
		target := int64(float64(quota) * rolloverTargetFraction)
		ps := byFractal[fid]
		sort.Slice(ps, func(i, j int) bool { return ps[i].MinTime.Before(ps[j].MinTime) })

		var freed int64
		dropped := 0
		// Keep at least the newest partition; never drop it all. On the ingest axis
		// that is reliably the partition currently receiving writes.
		for i := 0; i < len(ps)-1 && total-freed > target; i++ {
			if err := qm.admin.DropLogPartition(ctx, ps[i].Partition); err != nil {
				log.Printf("[Quota] Rollover: drop partition %s for fractal %s: %v", ps[i].Partition, fid, err)
				continue
			}
			freed += ps[i].Bytes
			dropped++
		}
		if dropped > 0 {
			log.Printf("[Quota] Rolled over fractal %s: dropped %d partition(s), freed ~%d bytes (was %d, quota %d)",
				fid, dropped, freed, total, quota)
		}
	}
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

package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"bifract/pkg/storage"
)

const (
	// maxEnqueueBatch caps how many logs a single Enqueue call can place
	// into one queue slot. Larger batches are split into multiple slots.
	// This bounds per-slot memory so total queue memory is predictable:
	//   max queue memory ~ bufSize * maxEnqueueBatch * avgLogSize
	maxEnqueueBatch = 5000

	// Workers coalesce multiple queue items into a single ClickHouse insert
	// to reduce the number of data parts created.
	defaultMaxBatchSize  = 10000
	defaultFlushInterval = 500 * time.Millisecond

	// Retry settings for failed batch inserts.
	maxInsertRetries  = 3
	initialRetryDelay = 500 * time.Millisecond
	maxRetryDelay     = 5 * time.Second

	// consecutiveFailures above this threshold signal the queue is unhealthy.
	unhealthyThreshold = 3
	// unhealthyCooldown is how long after the last failure before the queue
	// automatically resets to healthy, allowing traffic to resume even if no
	// worker has had a chance to succeed (e.g. because Enqueue was rejecting).
	unhealthyCooldown = 30 * time.Second

	// cpuPressureTrigger is the CPU% above which backpressure activates.
	// cpuPressureRelease is the CPU% below which it deactivates.
	// The gap prevents oscillation.
	cpuPressureTrigger = 80.0
	cpuPressureRelease = 60.0
	cpuPollInterval    = 5 * time.Second
	// cpuPressureSustainSamples consecutive polls above cpuPressureTrigger
	// are required before backpressure activates. Prevents a single heavy
	// user query from interrupting ingestion. 6 × 5s = 30 seconds.
	cpuPressureSustainSamples int64 = 6

	// diskPressureTrigger is the disk usage% above which backpressure activates.
	// diskPressureRelease is the disk usage% below which it deactivates.
	// At 90% ClickHouse starts struggling; at 95%+ data corruption risk.
	diskPressureTrigger = 90.0
	diskPressureRelease = 80.0
)

// QueueMetrics tracks ingestion queue statistics
type QueueMetrics struct {
	Accepted     atomic.Int64
	Inserted     atomic.Int64
	InsertErrors atomic.Int64
	QueueDrops   atomic.Int64
	Retries      atomic.Int64
}

// IngestQueue provides buffered log ingestion with a worker pool.
// Handlers push log batches onto a bounded channel; worker goroutines
// drain the channel, coalesce multiple batches, and batch-insert into
// ClickHouse. This reduces the number of parts created on the server.
// If the channel is full, Enqueue returns false so the handler can
// respond with 429 Too Many Requests (backpressure).
type IngestQueue struct {
	ch           chan []storage.LogEntry
	db           *storage.ClickHouseClient
	workers      int
	bufSize      int // total channel capacity, cached for depth-based backpressure
	quotaManager *QuotaManager
	wg           sync.WaitGroup
	Metrics      QueueMetrics

	// consecutiveFailures tracks how many sequential flush attempts have
	// failed across all workers. Used for adaptive backpressure: when this
	// is high, handlers reject new batches early instead of buffering logs
	// that will likely fail to insert.
	consecutiveFailures atomic.Int64
	// lastFailureUnix stores the unix timestamp (seconds) of the most recent
	// worker insert failure. Used to auto-recover: if enough time passes
	// with no new failures (ClickHouse recovered but no batches to prove it),
	// the queue resets to healthy so Enqueue accepts traffic again.
	lastFailureUnix atomic.Int64

	// cpuPressure is 1 when ClickHouse CPU backpressure is active, 0 otherwise.
	// Set by the background CPU monitor based on system.asynchronous_metrics.
	cpuPressure  atomic.Int64
	cpuHighStreak atomic.Int64 // consecutive polls above cpuPressureTrigger
	// diskPressure is 1 when ClickHouse disk usage exceeds the high watermark.
	// External ingestion is rejected while active; system fractals (audit,
	// alerts, system) bypass this since they write directly via InsertLogs.
	diskPressure atomic.Int64
	stop         chan struct{} // signals CPU/disk monitor to exit

	// lastIngested tracks the most recent successful insert time per fractal.
	// Used by the alert engine to skip evaluation when no new data has arrived.
	lastIngestedMu sync.RWMutex
	lastIngested   map[string]time.Time

	// systemFractalID is set after startup to enable internal monitoring events.
	systemFractalID atomic.Value // stores string

	// Pending drop counts per reason, flushed as system events every 30s.
	pendingDropsCPU   atomic.Int64
	pendingDropsDisk  atomic.Int64
	pendingDropsQueue atomic.Int64
	lastDropFlushUnix atomic.Int64

	notifWriter notifWriterIface
}

type notifWriterIface interface {
	Write(notifType, severity, title, message string) error
}

// SetNotificationWriter wires in the health notification writer (called from
// main.go after both are constructed).
func (q *IngestQueue) SetNotificationWriter(w notifWriterIface) { q.notifWriter = w }

// NewIngestQueue creates and starts a buffered ingestion queue.
// bufferSize controls how many pending batches can be held in memory.
// workers is the number of goroutines draining the queue.
func NewIngestQueue(db *storage.ClickHouseClient, bufferSize, workers int) *IngestQueue {
	q := &IngestQueue{
		ch:           make(chan []storage.LogEntry, bufferSize),
		db:           db,
		workers:      workers,
		bufSize:      bufferSize,
		stop:         make(chan struct{}),
		lastIngested: make(map[string]time.Time),
	}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}
	q.wg.Add(1)
	go q.monitorCPU()
	log.Printf("[Ingest Queue] Started %d workers, buffer size %d, max enqueue batch %d, batch coalesce %d/%v, CPU backpressure %.0f%%/%.0f%%, disk backpressure %.0f%%/%.0f%%",
		workers, bufferSize, maxEnqueueBatch, defaultMaxBatchSize, defaultFlushInterval, cpuPressureTrigger, cpuPressureRelease, diskPressureTrigger, diskPressureRelease)
	return q
}

// SetQuotaManager attaches a QuotaManager for per-fractal disk quota enforcement.
func (q *IngestQueue) SetQuotaManager(qm *QuotaManager) {
	q.quotaManager = qm
}

// Enqueue adds a log batch to the queue.
// Returns true if accepted, false if the queue is full or workers are
// unhealthy (caller should 429).
// Large batches are split into chunks of maxEnqueueBatch to bound
// per-slot memory usage.
func (q *IngestQueue) Enqueue(logs []storage.LogEntry) bool {
	// Adaptive backpressure: if workers are consistently failing to insert,
	// reject early rather than buffering logs that will likely be dropped.
	// Auto-recover after unhealthyCooldown so the system isn't permanently
	// stuck when ClickHouse comes back but no worker has had a chance to
	// succeed (because this gate was blocking all new batches).
	if q.consecutiveFailures.Load() >= unhealthyThreshold {
		if time.Now().Unix()-q.lastFailureUnix.Load() > int64(unhealthyCooldown.Seconds()) {
			q.consecutiveFailures.Store(0)
		} else {
			n := int64(len(logs))
			q.Metrics.QueueDrops.Add(n)
			q.pendingDropsQueue.Add(n)
			return false
		}
	}

	// CPU backpressure: reject when ClickHouse CPU is saturated.
	if q.cpuPressure.Load() == 1 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsCPU.Add(n)
		return false
	}

	// Disk backpressure: reject when ClickHouse disk is nearly full.
	if q.diskPressure.Load() == 1 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsDisk.Add(n)
		return false
	}

	// Calculate how many queue slots this batch needs after splitting.
	slotsNeeded := (len(logs) + maxEnqueueBatch - 1) / maxEnqueueBatch

	// Depth-based backpressure: reject when accepting this batch would push
	// the queue past 50% capacity. Check against slotsNeeded so we never
	// partially enqueue (which would cause duplicates on client retry).
	if q.bufSize > 0 && len(q.ch)+slotsNeeded > q.bufSize/2 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsQueue.Add(n)
		return false
	}

	// Split large batches so each queue slot holds at most maxEnqueueBatch
	// logs. This bounds per-slot memory and makes queue depth a more
	// meaningful measure of buffered work.
	total := len(logs)
	for len(logs) > maxEnqueueBatch {
		chunk := logs[:maxEnqueueBatch]
		logs = logs[maxEnqueueBatch:]
		select {
		case q.ch <- chunk:
		default:
			// Should not happen given the capacity check above, but
			// guard against races with concurrent Enqueue calls.
			q.Metrics.QueueDrops.Add(int64(total))
			return false
		}
	}

	select {
	case q.ch <- logs:
		q.Metrics.Accepted.Add(int64(total))
		return true
	default:
		q.Metrics.QueueDrops.Add(int64(total))
		return false
	}
}

// Healthy returns false when workers are unable to insert into ClickHouse
// or when CPU or disk backpressure is active.
func (q *IngestQueue) Healthy() bool {
	if q.cpuPressure.Load() == 1 {
		return false
	}
	if q.diskPressure.Load() == 1 {
		return false
	}
	if q.consecutiveFailures.Load() < unhealthyThreshold {
		return true
	}
	return time.Now().Unix()-q.lastFailureUnix.Load() > int64(unhealthyCooldown.Seconds())
}

// monitorCPU polls ClickHouse OS CPU and disk metrics, toggling backpressure.
func (q *IngestQueue) monitorCPU() {
	defer q.wg.Done()
	ticker := time.NewTicker(cpuPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-q.stop:
			return
		case <-ticker.C:
			pct, err := q.queryClickHouseCPU()
			if err != nil {
				q.cpuHighStreak.Store(0)
			} else if pct >= cpuPressureTrigger {
				streak := q.cpuHighStreak.Add(1)
				if streak >= cpuPressureSustainSamples && q.cpuPressure.Load() == 0 {
					q.cpuPressure.Store(1)
					log.Printf("[Ingest Queue] CPU backpressure ON (%.1f%%, sustained %ds)", pct, cpuPressureSustainSamples*int64(cpuPollInterval.Seconds()))
					q.writeSystemEvent("ingest.backpressure.on", map[string]string{
						"reason":    "cpu_pressure",
						"value":     fmt.Sprintf("%.1f", pct),
						"threshold": fmt.Sprintf("%.1f", cpuPressureTrigger),
					})
					if q.notifWriter != nil {
						go q.notifWriter.Write("ingest.cpu_pressure", "warning",
							"Ingest CPU Backpressure Active",
							fmt.Sprintf("CPU at %.1f%% (threshold %.1f%%)", pct, cpuPressureTrigger))
					}
				}
			} else {
				q.cpuHighStreak.Store(0)
				if pct < cpuPressureRelease && q.cpuPressure.Load() == 1 {
					q.cpuPressure.Store(0)
					log.Printf("[Ingest Queue] CPU backpressure OFF (%.1f%%)", pct)
					q.writeSystemEvent("ingest.backpressure.off", map[string]string{
						"reason": "cpu_pressure",
						"value":  fmt.Sprintf("%.1f", pct),
					})
				}
			}

			diskPct, diskErr := q.queryClickHouseDisk()
			if diskErr == nil {
				if diskPct >= diskPressureTrigger && q.diskPressure.Load() == 0 {
					q.diskPressure.Store(1)
					log.Printf("[Ingest Queue] Disk backpressure ON (%.1f%% used)", diskPct)
					q.writeSystemEvent("ingest.backpressure.on", map[string]string{
						"reason":    "disk_pressure",
						"value":     fmt.Sprintf("%.1f", diskPct),
						"threshold": fmt.Sprintf("%.1f", diskPressureTrigger),
					})
					if q.notifWriter != nil {
						go q.notifWriter.Write("ingest.disk_pressure", "warning",
							"Ingest Disk Backpressure Active",
							fmt.Sprintf("Disk at %.1f%% used (threshold %.1f%%)", diskPct, diskPressureTrigger))
					}
				} else if diskPct < diskPressureRelease && q.diskPressure.Load() == 1 {
					q.diskPressure.Store(0)
					log.Printf("[Ingest Queue] Disk backpressure OFF (%.1f%% used)", diskPct)
					q.writeSystemEvent("ingest.backpressure.off", map[string]string{
						"reason": "disk_pressure",
						"value":  fmt.Sprintf("%.1f", diskPct),
					})
				}
			}

			q.flushDropEvents()
		}
	}
}

// queryClickHouseCPU returns the highest CPU utilization (0-100) across
// all ClickHouse nodes. In single-node mode this queries via the shared
// connection pool. In cluster mode it queries each node individually and
// returns the max, so backpressure triggers when any node is overloaded.
func (q *IngestQueue) queryClickHouseCPU() (float64, error) {
	addrs := q.db.Addrs()
	if len(addrs) <= 1 {
		return q.queryNodeCPU(nil)
	}
	var maxPct float64
	var lastErr error
	for _, addr := range addrs {
		pct, err := q.queryNodeCPU(&addr)
		if err != nil {
			lastErr = err
			continue
		}
		if pct > maxPct {
			maxPct = pct
		}
	}
	if maxPct > 0 || lastErr == nil {
		return maxPct, nil
	}
	return 0, lastErr
}

// queryNodeCPU queries CPU metrics from a single ClickHouse node.
// If addr is nil, uses the shared connection pool.
func (q *IngestQueue) queryNodeCPU(addr *string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rows []map[string]interface{}
	var err error

	if addr != nil {
		conn, openErr := storage.OpenClickHouseAddr(*addr, q.db.User, q.db.Password)
		if openErr != nil {
			return 0, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, `SELECT metric, value FROM system.asynchronous_metrics
			WHERE metric IN (
				'OSUserTime', 'OSNiceTime', 'OSSystemTime',
				'OSIdleTime', 'OSIOWaitTime',
				'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime'
			)`)
	} else {
		rows, err = q.db.Query(ctx, `SELECT metric, value FROM system.asynchronous_metrics
			WHERE metric IN (
				'OSUserTime', 'OSNiceTime', 'OSSystemTime',
				'OSIdleTime', 'OSIOWaitTime',
				'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime'
			)`)
	}
	if err != nil {
		return 0, err
	}

	var user, nice, system, idle, iowait, irq, softirq, steal float64
	for _, row := range rows {
		name, _ := row["metric"].(string)
		val := asFloat64(row["value"])
		switch name {
		case "OSUserTime":
			user = val
		case "OSNiceTime":
			nice = val
		case "OSSystemTime":
			system = val
		case "OSIdleTime":
			idle = val
		case "OSIOWaitTime":
			iowait = val
		case "OSIrqTime":
			irq = val
		case "OSSoftIrqTime":
			softirq = val
		case "OSStealTime":
			steal = val
		}
	}
	busy := user + nice + system + irq + softirq + steal
	total := busy + idle + iowait
	if total <= 0 {
		return 0, nil
	}
	return busy / total * 100, nil
}

// queryClickHouseDisk returns the highest disk usage percentage (0-100) across
// all ClickHouse nodes. Queries the system.disks table for the default disk.
func (q *IngestQueue) queryClickHouseDisk() (float64, error) {
	addrs := q.db.Addrs()
	if len(addrs) <= 1 {
		return q.queryNodeDisk(nil)
	}
	var maxPct float64
	var lastErr error
	for _, addr := range addrs {
		pct, err := q.queryNodeDisk(&addr)
		if err != nil {
			lastErr = err
			continue
		}
		if pct > maxPct {
			maxPct = pct
		}
	}
	if maxPct > 0 || lastErr == nil {
		return maxPct, nil
	}
	return 0, lastErr
}

// queryNodeDisk queries disk usage from a single ClickHouse node.
func (q *IngestQueue) queryNodeDisk(addr *string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const diskQuery = `SELECT total_space, free_space FROM system.disks WHERE name = 'default' LIMIT 1`

	var rows []map[string]interface{}
	var err error

	if addr != nil {
		conn, openErr := storage.OpenClickHouseAddr(*addr, q.db.User, q.db.Password)
		if openErr != nil {
			return 0, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, diskQuery)
	} else {
		rows, err = q.db.Query(ctx, diskQuery)
	}
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	total := asFloat64(rows[0]["total_space"])
	free := asFloat64(rows[0]["free_space"])
	if total <= 0 {
		return 0, nil
	}
	return (total - free) / total * 100, nil
}

func asFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

// SetSystemFractalID enables internal monitoring events written to the system fractal.
func (q *IngestQueue) SetSystemFractalID(id string) {
	q.systemFractalID.Store(id)
}

// writeSystemEvent inserts a monitoring event into the system fractal (fire-and-forget).
func (q *IngestQueue) writeSystemEvent(event string, fields map[string]string) {
	fractalID, _ := q.systemFractalID.Load().(string)
	if fractalID == "" {
		return
	}
	fields["event"] = event
	rawBytes, err := json.Marshal(fields)
	if err != nil {
		return
	}
	rawLog := string(rawBytes)
	now := time.Now()
	entry := storage.LogEntry{
		Timestamp:       now,
		IngestTimestamp: now,
		RawLog:          rawLog,
		LogID:           storage.GenerateLogID(now, rawLog),
		Fields:          fields,
		FractalID:       fractalID,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := q.db.InsertLogs(ctx, []storage.LogEntry{entry}); err != nil {
			log.Printf("[Ingest Queue] failed to write system event %s: %v", event, err)
		}
	}()
}

// WriteSystemEvent is the public entry point for writing a system-fractal event,
// allowing external components (e.g. DistributionMonitor) to use the same path.
func (q *IngestQueue) WriteSystemEvent(event string, fields map[string]string) {
	q.writeSystemEvent(event, fields)
}

// flushDropEvents writes accumulated drop counts as system events if enough time has passed.
func (q *IngestQueue) flushDropEvents() {
	if time.Now().Unix()-q.lastDropFlushUnix.Load() < 30 {
		return
	}
	q.lastDropFlushUnix.Store(time.Now().Unix())
	for _, rc := range []struct {
		reason string
		count  *atomic.Int64
	}{
		{"cpu_pressure", &q.pendingDropsCPU},
		{"disk_pressure", &q.pendingDropsDisk},
		{"queue_full", &q.pendingDropsQueue},
	} {
		if n := rc.count.Swap(0); n > 0 {
			q.writeSystemEvent("ingest.drops", map[string]string{
				"reason": rc.reason,
				"count":  fmt.Sprintf("%d", n),
			})
		}
	}
}

// Shutdown closes the queue and waits for all workers to finish
// draining remaining batches.
func (q *IngestQueue) Shutdown() {
	log.Println("[Ingest Queue] Shutting down, draining remaining batches...")
	close(q.stop)
	close(q.ch)
	q.wg.Wait()
	log.Printf("[Ingest Queue] Shutdown complete. Inserted: %d, Errors: %d, Drops: %d, Retries: %d",
		q.Metrics.Inserted.Load(), q.Metrics.InsertErrors.Load(),
		q.Metrics.QueueDrops.Load(), q.Metrics.Retries.Load())
}

// Depth returns the current number of pending batches in the queue.
func (q *IngestQueue) Depth() int {
	return len(q.ch)
}

// LastIngested returns the most recent successful insert time for a fractal.
// Returns zero time if no data has been ingested for this fractal since startup.
func (q *IngestQueue) LastIngested(fractalID string) time.Time {
	q.lastIngestedMu.RLock()
	defer q.lastIngestedMu.RUnlock()
	return q.lastIngested[fractalID]
}

// Metrics source methods (satisfy metrics.IngestSource interface).

func (q *IngestQueue) AcceptedTotal() int64         { return q.Metrics.Accepted.Load() }
func (q *IngestQueue) InsertedTotal() int64         { return q.Metrics.Inserted.Load() }
func (q *IngestQueue) InsertErrorsTotal() int64     { return q.Metrics.InsertErrors.Load() }
func (q *IngestQueue) QueueDropsTotal() int64       { return q.Metrics.QueueDrops.Load() }
func (q *IngestQueue) RetriesTotal() int64          { return q.Metrics.Retries.Load() }
func (q *IngestQueue) CPUPressure() bool            { return q.cpuPressure.Load() == 1 }
func (q *IngestQueue) DiskPressure() bool           { return q.diskPressure.Load() == 1 }
func (q *IngestQueue) ConsecutiveFailures() int64   { return q.consecutiveFailures.Load() }

// worker drains the channel and coalesces multiple small batches into larger
// ClickHouse inserts. It flushes when the coalesced batch reaches
// defaultMaxBatchSize or after defaultFlushInterval, whichever comes first.
// Failed inserts are retried with exponential backoff before being dropped.
func (q *IngestQueue) worker(id int) {
	defer q.wg.Done()

	buf := make([]storage.LogEntry, 0, defaultMaxBatchSize)
	timer := time.NewTimer(defaultFlushInterval)
	defer timer.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}

		inserted := false
		delay := initialRetryDelay

		for attempt := 0; attempt < maxInsertRetries; attempt++ {
			if attempt > 0 {
				q.Metrics.Retries.Add(1)
				log.Printf("[Ingest Queue] Worker %d: retrying %d logs (attempt %d/%d) after %v",
					id, len(buf), attempt+1, maxInsertRetries, delay)
				time.Sleep(delay)
				delay *= 2
				if delay > maxRetryDelay {
					delay = maxRetryDelay
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			err := q.db.InsertLogs(ctx, buf)
			cancel()

			if err == nil {
				q.Metrics.Inserted.Add(int64(len(buf)))
				q.consecutiveFailures.Store(0)
				inserted = true

				// Collect per-fractal stats for quota tracking and ingestion timestamps.
				fractalIDs := make(map[string]bool, 4)
				for i := range buf {
					fractalIDs[buf[i].FractalID] = true
				}
				if q.quotaManager != nil {
					fractalBytes := make(map[string]int64, 4)
					fractalCounts := make(map[string]int64, 4)
					for i := range buf {
						fractalBytes[buf[i].FractalID] += int64(len(buf[i].RawLog))
						fractalCounts[buf[i].FractalID]++
					}
					for fid, b := range fractalBytes {
						q.quotaManager.RecordInsert(fid, b, fractalCounts[fid])
					}
				}

				// Record per-fractal insert time for alert skip optimization.
				now := time.Now()
				q.lastIngestedMu.Lock()
				for fid := range fractalIDs {
					q.lastIngested[fid] = now
				}
				q.lastIngestedMu.Unlock()

				break
			}

			log.Printf("[Ingest Queue] Worker %d: failed to insert %d logs (attempt %d/%d): %v",
				id, len(buf), attempt+1, maxInsertRetries, err)
		}

		if !inserted {
			q.Metrics.InsertErrors.Add(int64(len(buf)))
			failures := q.consecutiveFailures.Add(1)
			q.lastFailureUnix.Store(time.Now().Unix())
			q.writeSystemEvent("ingest.insert_error", map[string]string{
				"worker":     fmt.Sprintf("%d", id),
				"batch_size": fmt.Sprintf("%d", len(buf)),
			})
			if failures == unhealthyThreshold && q.notifWriter != nil {
				go q.notifWriter.Write("ingest.insert_errors", "critical",
					"Ingest Insert Errors",
					fmt.Sprintf("Worker %d: %d consecutive insert failures", id, unhealthyThreshold))
			}
		}

		// Shrink backing array if it grew beyond 2x the target to avoid
		// holding memory from transient spikes indefinitely.
		if cap(buf) > defaultMaxBatchSize*2 {
			buf = make([]storage.LogEntry, 0, defaultMaxBatchSize)
		} else {
			buf = buf[:0]
		}
	}

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(defaultFlushInterval)
	}

	for {
		select {
		case batch, ok := <-q.ch:
			if !ok {
				flush()
				return
			}
			buf = append(buf, batch...)
			if len(buf) >= defaultMaxBatchSize {
				flush()
				resetTimer()
			}
		case <-timer.C:
			flush()
			timer.Reset(defaultFlushInterval)
		}
	}
}

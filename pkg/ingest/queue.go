package ingest

import (
	"context"
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
	cpuPressure atomic.Int64
	stop        chan struct{} // signals CPU monitor to exit

	// lastIngested tracks the most recent successful insert time per fractal.
	// Used by the alert engine to skip evaluation when no new data has arrived.
	lastIngestedMu sync.RWMutex
	lastIngested   map[string]time.Time
}

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
	log.Printf("[Ingest Queue] Started %d workers, buffer size %d, max enqueue batch %d, batch coalesce %d/%v, CPU backpressure %.0f%%/%.0f%%",
		workers, bufferSize, maxEnqueueBatch, defaultMaxBatchSize, defaultFlushInterval, cpuPressureTrigger, cpuPressureRelease)
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
			q.Metrics.QueueDrops.Add(int64(len(logs)))
			return false
		}
	}

	// CPU backpressure: reject when ClickHouse CPU is saturated.
	if q.cpuPressure.Load() == 1 {
		q.Metrics.QueueDrops.Add(int64(len(logs)))
		return false
	}

	// Calculate how many queue slots this batch needs after splitting.
	slotsNeeded := (len(logs) + maxEnqueueBatch - 1) / maxEnqueueBatch

	// Depth-based backpressure: reject when accepting this batch would push
	// the queue past 50% capacity. Check against slotsNeeded so we never
	// partially enqueue (which would cause duplicates on client retry).
	if q.bufSize > 0 && len(q.ch)+slotsNeeded > q.bufSize/2 {
		q.Metrics.QueueDrops.Add(int64(len(logs)))
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
// or when CPU backpressure is active.
func (q *IngestQueue) Healthy() bool {
	if q.cpuPressure.Load() == 1 {
		return false
	}
	if q.consecutiveFailures.Load() < unhealthyThreshold {
		return true
	}
	return time.Now().Unix()-q.lastFailureUnix.Load() > int64(unhealthyCooldown.Seconds())
}

// monitorCPU polls ClickHouse OS CPU metrics and toggles cpuPressure.
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
				continue
			}
			if pct >= cpuPressureTrigger && q.cpuPressure.Load() == 0 {
				q.cpuPressure.Store(1)
				log.Printf("[Ingest Queue] CPU backpressure ON (%.1f%%)", pct)
			} else if pct < cpuPressureRelease && q.cpuPressure.Load() == 1 {
				q.cpuPressure.Store(0)
				log.Printf("[Ingest Queue] CPU backpressure OFF (%.1f%%)", pct)
			}
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
			q.consecutiveFailures.Add(1)
			q.lastFailureUnix.Store(time.Now().Unix())
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

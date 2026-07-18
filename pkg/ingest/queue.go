package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"bifract/pkg/spool"
	"bifract/pkg/storage"
)

const (
	// maxEnqueueBatch caps how many logs a single Enqueue call can place
	// into one queue slot. Larger batches are split into multiple slots.
	// This bounds per-slot memory so total queue memory is predictable:
	//   max queue memory ~ bufSize * maxEnqueueBatch * avgLogSize
	maxEnqueueBatch = 5000

	// Partition-aware batching. ClickHouse splits an insert block by partition
	// and writes one part per partition, so a batch spanning many
	// (fractal_id, day) pairs creates many small parts and multiplies merge
	// work. The accumulator groups entries by partition key and hands whole
	// keys to workers, so each insert maps to exactly one partition and
	// therefore one part.
	//
	// A key flushes on whichever comes first: batchRows, batchBytes, or
	// flushInterval since its first entry. Busy fractals hit a size bound
	// almost immediately, so the interval only governs quiet keys and
	// backfill. Returns diminish sharply as it grows: most of the coalescing
	// win is in leaving sub-second flushing at all, so it is kept short
	// enough that a single test log stays visible promptly.
	defaultBatchRows     = 20000
	defaultBatchBytes    = 16 << 20 // 16MB
	defaultFlushInterval = 30 * time.Second

	// Accumulator memory bounds. Exceeding either force-flushes keys
	// (largest first for bytes, oldest first for key count) so buffering
	// can never grow without limit.
	defaultBufferBytes = 256 << 20 // 256MB
	defaultMaxKeys     = 2048

	// accumTick is how often the accumulator evaluates age and cap triggers.
	accumTick = time.Second

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

	// memPressureTrigger is the memory% (cgroup-aware) above which backpressure
	// activates; memPressureRelease is where it deactivates. Near the cgroup memory
	// limit the kernel OOM-killer is imminent and inserts/merges are the first to be
	// killed, so shed ingest before that happens. On cgroup-limited (k8s/container)
	// deployments this reads the pod's own limit, not the node's.
	memPressureTrigger = 90.0
	memPressureRelease = 80.0
	// memPressureSustainSamples consecutive high polls before activating (3 x 5s = 15s),
	// a shorter debounce than CPU since sustained high memory is closer to an OOM-kill
	// and worth reacting to sooner, while still filtering a transient cache fill.
	memPressureSustainSamples int64 = 3

	// spoolPressureTrigger/Release are fractions of the archive spool's max
	// bytes at which spool backpressure activates/deactivates. The gap prevents
	// oscillation. Only evaluated when the archive spool is configured.
	spoolPressureTrigger = 0.90
	spoolPressureRelease = 0.70
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
	ch chan []storage.LogEntry
	db *storage.ClickHouseClient
	// metricsDB, when set, is the client the backpressure monitors poll for
	// ClickHouse CPU/memory/disk metrics. Inserts always go through db (the ingest
	// pool, which may be pinned to a single write LB), but health should reflect
	// every shard, so callers point this at the all-shards query client. Falls back
	// to db when unset. Atomic because SetMetricsClient runs on the main goroutine
	// after the monitor goroutine has already started in NewIngestQueue.
	metricsDB    atomic.Pointer[storage.ClickHouseClient]
	workers      int
	bufSize      int // total channel capacity, cached for depth-based backpressure
	quotaManager *QuotaManager
	wg           sync.WaitGroup
	Metrics      QueueMetrics

	// flushCh carries ready partition buckets from the accumulator to the
	// workers. Grouping is centralized in a single accumulator goroutine so
	// one partition key produces one part; per-worker maps would let the same
	// key accumulate in every worker and emit one part each.
	flushCh chan *partBucket

	// Accumulator tuning, resolved from env in NewIngestQueue.
	batchRows     int
	batchBytes    int64
	flushInterval time.Duration
	bufferBytes   int64
	maxKeys       int

	// accumRows/accumBytes track logs held in the accumulator, which have left
	// q.ch but are not yet handed to a worker. Without these, queue-depth
	// backpressure and alert deferral would read a near-empty q.ch while
	// hundreds of MB sit buffered.
	accumRows  atomic.Int64
	accumBytes atomic.Int64
	// accumFlushes counts partition buckets flushed, one per part written.
	accumFlushes atomic.Int64

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
	cpuPressure   atomic.Int64
	cpuHighStreak atomic.Int64 // consecutive polls above cpuPressureTrigger
	// diskPressure is 1 when ClickHouse disk usage exceeds the high watermark.
	// External ingestion is rejected while active; system fractals (audit,
	// alerts, system) bypass this since they write directly via InsertLogs.
	diskPressure atomic.Int64
	// memPressure is 1 when ClickHouse memory (cgroup-aware) is near its limit.
	// Set by the background monitor to shed ingest before an OOM-kill.
	memPressure   atomic.Int64
	memHighStreak atomic.Int64  // consecutive polls above memPressureTrigger
	stop          chan struct{} // signals CPU/disk/memory monitor to exit

	// spool is the durable archive spool (nil unless the archive feature is
	// provisioned). archiveEnabled gates the tee at runtime so a provisioned-
	// but-disabled archive adds zero ingest overhead. spoolPressure is 1 when
	// spool disk usage is near spoolMaxBytes (backpressure, like diskPressure).
	spool          *spool.Writer
	archiveEnabled atomic.Bool
	spoolPressure  atomic.Int64
	spoolMaxBytes  int64

	// lastIngested tracks the most recent successful insert time per fractal.
	// Used by the alert engine to skip evaluation when no new data has arrived.
	lastIngestedMu sync.RWMutex
	lastIngested   map[string]time.Time

	// systemFractalID is set after startup to enable internal monitoring events.
	systemFractalID atomic.Value // stores string

	// Pending drop counts per reason, flushed as system events every 30s.
	pendingDropsCPU   atomic.Int64
	pendingDropsDisk  atomic.Int64
	pendingDropsMem   atomic.Int64
	pendingDropsQueue atomic.Int64
	pendingDropsSpool atomic.Int64
	lastDropFlushUnix atomic.Int64

	notifWriter notifWriterIface
}

type notifWriterIface interface {
	Write(notifType, severity, title, message string) error
}

// partKey identifies a ClickHouse partition of the logs table, whose
// partition expression is (fractal_id, toDate(timestamp)). day is unix
// seconds at UTC midnight; the ClickHouse server runs UTC, so this matches
// toDate() exactly and a bucket never straddles two partitions.
type partKey struct {
	fractalID string
	day       int64
}

// partBucket accumulates entries destined for a single partition.
type partBucket struct {
	key       partKey
	entries   []storage.LogEntry
	bytes     int64
	firstSeen time.Time
}

// partKeyOf maps a log entry to its destination partition.
func partKeyOf(e *storage.LogEntry) partKey {
	return partKey{
		fractalID: e.FractalID,
		day:       e.Timestamp.UTC().Truncate(24 * time.Hour).Unix(),
	}
}

// entrySize estimates an entry's heap cost for the memory cap. RawLog
// dominates; the constant covers the fixed struct plus normalized fields.
func entrySize(e *storage.LogEntry) int64 {
	return int64(len(e.RawLog)) + 512
}

// accumulator drains the ingest channel, groups entries by partition, and
// emits whole buckets to the workers. It is the only goroutine that touches
// the bucket map, so no locking is needed.
func (q *IngestQueue) accumulator() {
	defer q.wg.Done()
	// Closing flushCh lets workers finish their current insert and exit.
	defer close(q.flushCh)

	buckets := make(map[partKey]*partBucket)
	ticker := time.NewTicker(accumTick)
	defer ticker.Stop()

	// emit hands a bucket to a worker and drops it from the map. It blocks if
	// every worker is busy, which is the intended backpressure path: the
	// accumulator stops draining q.ch, depth rises, and Enqueue starts
	// rejecting.
	emit := func(b *partBucket) {
		delete(buckets, b.key)
		q.accumRows.Add(-int64(len(b.entries)))
		q.accumBytes.Add(-b.bytes)
		q.accumFlushes.Add(1)
		q.flushCh <- b
	}

	// emitReady flushes keys that have reached a size or age bound, then
	// enforces the global caps.
	emitReady := func(now time.Time) {
		for _, b := range buckets {
			if len(b.entries) >= q.batchRows || b.bytes >= q.batchBytes ||
				now.Sub(b.firstSeen) >= q.flushInterval {
				emit(b)
			}
		}

		// Byte cap: flush the largest keys first, since they yield the most
		// headroom per part written.
		for q.accumBytes.Load() > q.bufferBytes && len(buckets) > 0 {
			var biggest *partBucket
			for _, b := range buckets {
				if biggest == nil || b.bytes > biggest.bytes {
					biggest = b
				}
			}
			emit(biggest)
		}

		// Key cap: flush the oldest keys first, bounding worst-case latency.
		for len(buckets) > q.maxKeys {
			var oldest *partBucket
			for _, b := range buckets {
				if oldest == nil || b.firstSeen.Before(oldest.firstSeen) {
					oldest = b
				}
			}
			emit(oldest)
		}
	}

	for {
		select {
		case batch, ok := <-q.ch:
			if !ok {
				// Shutdown: drain every buffered key before workers exit.
				for _, b := range buckets {
					emit(b)
				}
				return
			}
			now := time.Now()
			for i := range batch {
				k := partKeyOf(&batch[i])
				b := buckets[k]
				if b == nil {
					b = &partBucket{
						key:       k,
						entries:   make([]storage.LogEntry, 0, 256),
						firstSeen: now,
					}
					buckets[k] = b
				}
				sz := entrySize(&batch[i])
				b.entries = append(b.entries, batch[i])
				b.bytes += sz
				q.accumRows.Add(1)
				q.accumBytes.Add(sz)
			}
			emitReady(now)

		case now := <-ticker.C:
			emitReady(now)
		}
	}
}

// SetNotificationWriter wires in the health notification writer (called from
// main.go after both are constructed).
func (q *IngestQueue) SetNotificationWriter(w notifWriterIface) { q.notifWriter = w }

// SetMetricsClient points the backpressure monitors at a client whose Addrs() cover
// every ClickHouse shard, so CPU/memory/disk pressure trips when any shard is hot even
// though inserts route through a single write LB. Optional; falls back to the insert
// client. Call before Start()/monitors run.
func (q *IngestQueue) SetMetricsClient(c *storage.ClickHouseClient) { q.metricsDB.Store(c) }

// mdb returns the client the health monitors should poll (all-shards when set).
func (q *IngestQueue) mdb() *storage.ClickHouseClient {
	if c := q.metricsDB.Load(); c != nil {
		return c
	}
	return q.db
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
		// Buffered by worker count so a ready bucket does not block the
		// accumulator while every worker happens to be mid-insert.
		flushCh:       make(chan *partBucket, workers),
		batchRows:     envInt("BIFRACT_INGEST_BATCH_ROWS", defaultBatchRows),
		batchBytes:    int64(envInt("BIFRACT_INGEST_BATCH_BYTES", defaultBatchBytes)),
		flushInterval: time.Duration(envInt("BIFRACT_INGEST_FLUSH_SECONDS", int(defaultFlushInterval/time.Second))) * time.Second,
		bufferBytes:   int64(envInt("BIFRACT_INGEST_BUFFER_BYTES", defaultBufferBytes)),
		maxKeys:       envInt("BIFRACT_INGEST_MAX_KEYS", defaultMaxKeys),
	}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}
	q.wg.Add(1)
	go q.accumulator()
	q.wg.Add(1)
	go q.monitorCPU()
	log.Printf("[Ingest Queue] Started %d workers, buffer size %d, max enqueue batch %d, partition batching %d rows/%dMB/%v, buffer cap %dMB/%d keys, CPU backpressure %.0f%%/%.0f%%, memory backpressure %.0f%%/%.0f%%, disk backpressure %.0f%%/%.0f%%",
		workers, bufferSize, maxEnqueueBatch, q.batchRows, q.batchBytes>>20, q.flushInterval, q.bufferBytes>>20, q.maxKeys,
		cpuPressureTrigger, cpuPressureRelease, memPressureTrigger, memPressureRelease, diskPressureTrigger, diskPressureRelease)
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

	// Memory backpressure: reject when ClickHouse is near its (cgroup) memory limit,
	// so we stop feeding inserts before the kernel OOM-kills them.
	if q.memPressure.Load() == 1 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsMem.Add(n)
		return false
	}

	// Disk backpressure: reject when ClickHouse disk is nearly full.
	if q.diskPressure.Load() == 1 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsDisk.Add(n)
		return false
	}

	// Spool backpressure: when archiving is enabled and the durable spool is
	// near capacity, reject early. Combined with the fail-closed tee below this
	// guarantees we never ack a log we could not durably archive.
	if q.spoolPressure.Load() == 1 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsSpool.Add(n)
		return false
	}

	// Accumulator memory backpressure: reject once buffered bytes exceed the
	// cap. The accumulator force-flushes above this too, but rejecting here
	// stops us accepting faster than ClickHouse can drain.
	if q.accumBytes.Load() > q.bufferBytes {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsQueue.Add(n)
		return false
	}

	// Calculate how many queue slots this batch needs after splitting.
	slotsNeeded := (len(logs) + maxEnqueueBatch - 1) / maxEnqueueBatch

	// Depth-based backpressure: reject when accepting this batch would push
	// the queue past 50% capacity. Depth() includes accumulator-held rows so
	// this still trips when the channel is drained but buffering is deep.
	// Check against slotsNeeded so we never partially enqueue (which would
	// cause duplicates on client retry).
	if q.bufSize > 0 && q.Depth()+slotsNeeded > q.bufSize/2 {
		n := int64(len(logs))
		q.Metrics.QueueDrops.Add(n)
		q.pendingDropsQueue.Add(n)
		return false
	}

	// Archive tee (fail-closed, spool-before-ack): when archiving is enabled,
	// durably append the batch to the spool BEFORE it is queued for ClickHouse.
	// A spool write failure rejects the batch (429) rather than acking data we
	// could not archive. spool.Append fsyncs before returning, so a successful
	// Enqueue means "durably spooled AND queued". A rare duplicate (retry after
	// a post-spool failure) is deduped on restore via log_id.
	if q.archiveEnabled.Load() && q.spool != nil {
		if err := q.spool.Append(logs); err != nil {
			log.Printf("[Ingest Queue] spool append failed, rejecting batch: %v", err)
			n := int64(len(logs))
			q.Metrics.QueueDrops.Add(n)
			q.pendingDropsSpool.Add(n)
			return false
		}
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
	if q.memPressure.Load() == 1 {
		return false
	}
	if q.diskPressure.Load() == 1 {
		return false
	}
	if q.spoolPressure.Load() == 1 {
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

			q.monitorMemory()

			q.monitorSpool()

			q.flushDropEvents()
		}
	}
}

// monitorSpool sets/clears spoolPressure from the archive spool's disk usage
// against spoolMaxBytes, with trigger/release hysteresis. No-op when the archive
// spool is not configured.
func (q *IngestQueue) monitorSpool() {
	if q.spool == nil || q.spoolMaxBytes <= 0 {
		return
	}
	used, err := q.spool.DiskUsage()
	if err != nil {
		return
	}
	frac := float64(used) / float64(q.spoolMaxBytes)
	if frac >= spoolPressureTrigger && q.spoolPressure.Load() == 0 {
		q.spoolPressure.Store(1)
		log.Printf("[Ingest Queue] Spool backpressure ON (%.1f%% of %d bytes)", frac*100, q.spoolMaxBytes)
		q.writeSystemEvent("ingest.backpressure.on", map[string]string{
			"reason":    "spool_pressure",
			"value":     fmt.Sprintf("%.1f", frac*100),
			"threshold": fmt.Sprintf("%.1f", spoolPressureTrigger*100),
		})
		if q.notifWriter != nil {
			go q.notifWriter.Write("ingest.spool_pressure", "warning",
				"Ingest Archive Spool Backpressure Active",
				fmt.Sprintf("Spool at %.1f%% of capacity", frac*100))
		}
	} else if frac < spoolPressureRelease && q.spoolPressure.Load() == 1 {
		q.spoolPressure.Store(0)
		log.Printf("[Ingest Queue] Spool backpressure OFF (%.1f%%)", frac*100)
		q.writeSystemEvent("ingest.backpressure.off", map[string]string{
			"reason": "spool_pressure",
			"value":  fmt.Sprintf("%.1f", frac*100),
		})
	}
}

// monitorMemory sets/clears memPressure from ClickHouse's (cgroup-aware) memory
// usage with trigger/release hysteresis and a short sustain, mirroring the CPU
// monitor. Guards against the OOM class of incident where inserts get killed and the
// distribution queue backs up.
func (q *IngestQueue) monitorMemory() {
	pct, err := q.queryClickHouseMemory()
	if err != nil {
		q.memHighStreak.Store(0)
		return
	}
	if pct >= memPressureTrigger {
		streak := q.memHighStreak.Add(1)
		if streak >= memPressureSustainSamples && q.memPressure.Load() == 0 {
			q.memPressure.Store(1)
			log.Printf("[Ingest Queue] Memory backpressure ON (%.1f%%, sustained %ds)", pct, memPressureSustainSamples*int64(cpuPollInterval.Seconds()))
			q.writeSystemEvent("ingest.backpressure.on", map[string]string{
				"reason":    "memory_pressure",
				"value":     fmt.Sprintf("%.1f", pct),
				"threshold": fmt.Sprintf("%.1f", memPressureTrigger),
			})
			if q.notifWriter != nil {
				go q.notifWriter.Write("ingest.memory_pressure", "warning",
					"Ingest Memory Backpressure Active",
					fmt.Sprintf("Memory at %.1f%% (threshold %.1f%%)", pct, memPressureTrigger))
			}
		}
	} else {
		q.memHighStreak.Store(0)
		if pct < memPressureRelease && q.memPressure.Load() == 1 {
			q.memPressure.Store(0)
			log.Printf("[Ingest Queue] Memory backpressure OFF (%.1f%%)", pct)
			q.writeSystemEvent("ingest.backpressure.off", map[string]string{
				"reason": "memory_pressure",
				"value":  fmt.Sprintf("%.1f", pct),
			})
		}
	}
}

// queryClickHouseMemory returns the highest memory utilization (0-100) across all
// ClickHouse nodes, preferring the cgroup limit over node RAM. In cluster mode it
// takes the max so backpressure triggers when any node is near its limit.
func (q *IngestQueue) queryClickHouseMemory() (float64, error) {
	addrs := q.mdb().Addrs()
	if len(addrs) <= 1 {
		return q.queryNodeMemory(nil)
	}
	var maxPct float64
	var lastErr error
	for _, addr := range addrs {
		pct, err := q.queryNodeMemory(&addr)
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

// queryNodeMemory queries memory metrics from a single ClickHouse node.
// If addr is nil, uses the shared connection pool.
func (q *IngestQueue) queryNodeMemory(addr *string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rows []map[string]interface{}
	var err error
	if addr != nil {
		conn, openErr := storage.OpenClickHouseAddr(*addr, q.mdb().User, q.mdb().Password)
		if openErr != nil {
			return 0, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, storage.SystemMemoryMetricsSQL)
	} else {
		rows, err = q.mdb().Query(ctx, storage.SystemMemoryMetricsSQL)
	}
	if err != nil {
		return 0, err
	}
	pct, _ := storage.MemoryPercentFromMetrics(storage.MetricRowsToMap(rows))
	return pct, nil
}

// queryClickHouseCPU returns the highest CPU utilization (0-100) across
// all ClickHouse nodes. In single-node mode this queries via the shared
// connection pool. In cluster mode it queries each node individually and
// returns the max, so backpressure triggers when any node is overloaded.
func (q *IngestQueue) queryClickHouseCPU() (float64, error) {
	addrs := q.mdb().Addrs()
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
		conn, openErr := storage.OpenClickHouseAddr(*addr, q.mdb().User, q.mdb().Password)
		if openErr != nil {
			return 0, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, storage.SystemCPUMetricsSQL)
	} else {
		rows, err = q.mdb().Query(ctx, storage.SystemCPUMetricsSQL)
	}
	if err != nil {
		return 0, err
	}
	pct, _ := storage.CPUPercentFromMetrics(storage.MetricRowsToMap(rows))
	return pct, nil
}

// queryClickHouseDisk returns the highest disk usage percentage (0-100) across
// all ClickHouse nodes. Queries the system.disks table for the default disk.
func (q *IngestQueue) queryClickHouseDisk() (float64, error) {
	addrs := q.mdb().Addrs()
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
		conn, openErr := storage.OpenClickHouseAddr(*addr, q.mdb().User, q.mdb().Password)
		if openErr != nil {
			return 0, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, diskQuery)
	} else {
		rows, err = q.mdb().Query(ctx, diskQuery)
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
		{"memory_pressure", &q.pendingDropsMem},
		{"disk_pressure", &q.pendingDropsDisk},
		{"queue_full", &q.pendingDropsQueue},
		{"spool_pressure", &q.pendingDropsSpool},
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

// Depth returns pending work in queue-slot units. It counts both the channel
// and the accumulator, because the accumulator drains q.ch quickly and would
// otherwise make a deeply buffered queue look empty to depth-based
// backpressure, the Prometheus gauge, and alert deferral.
func (q *IngestQueue) Depth() int {
	return len(q.ch) + int(q.accumRows.Load())/maxEnqueueBatch
}

// BufferedBytes returns the bytes currently held in the accumulator.
func (q *IngestQueue) BufferedBytes() int64 { return q.accumBytes.Load() }

// envInt reads a positive integer env var, falling back to def when unset,
// unparseable, or non-positive.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
		log.Printf("[Ingest Queue] invalid %s=%q, using default %d", key, v, def)
	}
	return def
}

// LastIngested returns the most recent successful insert time for a fractal.
// Returns zero time if no data has been ingested for this fractal since startup.
func (q *IngestQueue) LastIngested(fractalID string) time.Time {
	q.lastIngestedMu.RLock()
	defer q.lastIngestedMu.RUnlock()
	return q.lastIngested[fractalID]
}

// Metrics source methods (satisfy metrics.IngestSource interface).

func (q *IngestQueue) AcceptedTotal() int64       { return q.Metrics.Accepted.Load() }
func (q *IngestQueue) InsertedTotal() int64       { return q.Metrics.Inserted.Load() }
func (q *IngestQueue) InsertErrorsTotal() int64   { return q.Metrics.InsertErrors.Load() }
func (q *IngestQueue) QueueDropsTotal() int64     { return q.Metrics.QueueDrops.Load() }
func (q *IngestQueue) RetriesTotal() int64        { return q.Metrics.Retries.Load() }
func (q *IngestQueue) CPUPressure() bool          { return q.cpuPressure.Load() == 1 }
func (q *IngestQueue) MemPressure() bool          { return q.memPressure.Load() == 1 }
func (q *IngestQueue) DiskPressure() bool         { return q.diskPressure.Load() == 1 }
func (q *IngestQueue) SpoolPressure() bool        { return q.spoolPressure.Load() == 1 }
func (q *IngestQueue) ConsecutiveFailures() int64 { return q.consecutiveFailures.Load() }

// worker inserts ready partition buckets produced by the accumulator. Each
// bucket holds entries for exactly one (fractal_id, day) partition, so each
// insert lands in one partition and creates one part.
// Failed inserts are retried with exponential backoff before being dropped.
func (q *IngestQueue) worker(id int) {
	defer q.wg.Done()

	for bucket := range q.flushCh {
		buf := bucket.entries
		if len(buf) == 0 {
			continue
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

				// A bucket is single-fractal by construction, so quota and
				// last-ingested bookkeeping need no per-entry grouping.
				fid := bucket.key.fractalID
				if q.quotaManager != nil {
					var rawBytes int64
					for i := range buf {
						rawBytes += int64(len(buf[i].RawLog))
					}
					q.quotaManager.RecordInsert(fid, rawBytes, int64(len(buf)))
				}

				// Record per-fractal insert time for alert skip optimization.
				q.lastIngestedMu.Lock()
				q.lastIngested[fid] = time.Now()
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

		// Release the bucket's backing array before waiting on the next one.
		bucket.entries = nil
	}
}

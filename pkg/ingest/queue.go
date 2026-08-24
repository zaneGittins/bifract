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

	// pressureReassert is how often an active backpressure condition rewrites
	// its health notification. Notifications are pruned after 24h, so without
	// this a multi-day outage would silently vanish from the notification list
	// while it is still shedding logs.
	pressureReassert = 4 * time.Hour
	// criticalReassert is the reassert interval once a condition has escalated:
	// data loss in progress deserves a more insistent reminder.
	criticalReassert = time.Hour
	// pressureEscalateAfter is how long a condition must stay active, while
	// actually rejecting logs, before its notification escalates to critical.
	// Rejecting logs is data loss, not a warning; an idle system under pressure
	// drops nothing and stays at warning.
	pressureEscalateAfter = 5 * time.Minute
)

// pressureState tracks one backpressure condition end to end: whether it is
// active, since when, how many logs it has rejected, and the health
// notification it owns.
type pressureState struct {
	label  string // log prefix, e.g. "Disk"
	reason string // system-event reason, e.g. "disk_pressure"
	notif  string // notification type, e.g. "ingest.disk_pressure"
	title  string // notification title

	active         atomic.Int64
	highStreak     atomic.Int64 // consecutive polls above the trigger (CPU/memory)
	sinceUnix      atomic.Int64
	dropsActive    atomic.Int64 // logs rejected during the current activation
	pendingDrops   atomic.Int64 // logs rejected since the last system-event flush
	lastNotifyUnix atomic.Int64
	critical       atomic.Bool
	detail         atomic.Value // string: current human-readable value
}

func (p *pressureState) Active() bool { return p.active.Load() == 1 }

// reject records n logs rejected because this condition is active.
func (p *pressureState) reject(n int64) {
	p.pendingDrops.Add(n)
	p.dropsActive.Add(n)
}

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

	// Backpressure conditions, set by the background monitors. While any is
	// active external ingestion is rejected; system fractals (audit, alerts,
	// system) bypass this since they write directly via InsertLogs.
	cpuState   pressureState // ClickHouse CPU, from system.asynchronous_metrics
	memState   pressureState // ClickHouse memory, cgroup-aware
	diskState  pressureState // ClickHouse disk usage
	spoolState pressureState // archive spool disk usage

	stop chan struct{} // signals CPU/disk/memory monitor to exit

	// spool is the durable archive spool (nil unless the archive feature is
	// provisioned). archiveEnabled gates the tee at runtime so a provisioned-
	// but-disabled archive adds zero ingest overhead.
	spool          *spool.Writer
	archiveEnabled atomic.Bool
	spoolMaxBytes  int64

	// systemFractalID is set after startup to enable internal monitoring events.
	systemFractalID atomic.Value // stores string

	// Drops from queue-depth backpressure (no monitor owns this one), flushed
	// as system events every 30s alongside the per-condition counters.
	pendingDropsQueue atomic.Int64
	lastDropFlushUnix atomic.Int64

	// lastInsertErrNotify rate-limits the insert-failure notification so a
	// failing ClickHouse cannot produce one Postgres write per failed batch.
	lastInsertErrNotify atomic.Int64

	notifWriter notifWriterIface
}

type notifWriterIface interface {
	Write(notifType, severity, title, message string) error
	WriteSustained(notifType, severity, title, message string, minInterval time.Duration) error
}

// partKey identifies a ClickHouse partition of the logs table, whose
// partition expression is (fractal_id, toDate(ingest_timestamp)). day is unix
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

// partKeyOf maps a log entry to its destination partition. Enqueue settles
// IngestTimestamp before anything reaches here, so the key always matches the
// partition the row lands in.
func partKeyOf(e *storage.LogEntry) partKey {
	return partKey{
		fractalID: e.FractalID,
		day:       e.IngestTimestamp.UTC().Truncate(24 * time.Hour).Unix(),
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
		ch:      make(chan []storage.LogEntry, bufferSize),
		db:      db,
		workers: workers,
		bufSize: bufferSize,
		stop:    make(chan struct{}),
		// Buffered by worker count so a ready bucket does not block the
		// accumulator while every worker happens to be mid-insert.
		flushCh:       make(chan *partBucket, workers),
		batchRows:     envInt("BIFRACT_INGEST_BATCH_ROWS", defaultBatchRows),
		batchBytes:    int64(envInt("BIFRACT_INGEST_BATCH_BYTES", defaultBatchBytes)),
		flushInterval: time.Duration(envInt("BIFRACT_INGEST_FLUSH_SECONDS", int(defaultFlushInterval/time.Second))) * time.Second,
		bufferBytes:   int64(envInt("BIFRACT_INGEST_BUFFER_BYTES", defaultBufferBytes)),
		maxKeys:       envInt("BIFRACT_INGEST_MAX_KEYS", defaultMaxKeys),
	}
	q.cpuState = pressureState{label: "CPU", reason: "cpu_pressure",
		notif: "ingest.cpu_pressure", title: "Ingest CPU Backpressure Active"}
	q.memState = pressureState{label: "Memory", reason: "memory_pressure",
		notif: "ingest.memory_pressure", title: "Ingest Memory Backpressure Active"}
	q.diskState = pressureState{label: "Disk", reason: "disk_pressure",
		notif: "ingest.disk_pressure", title: "Ingest Disk Backpressure Active"}
	q.spoolState = pressureState{label: "Spool", reason: "spool_pressure",
		notif: "ingest.spool_pressure", title: "Ingest Archive Spool Backpressure Active"}
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
	if q.cpuState.Active() {
		return q.rejectFor(&q.cpuState, logs)
	}

	// Memory backpressure: reject when ClickHouse is near its (cgroup) memory limit,
	// so we stop feeding inserts before the kernel OOM-kills them.
	if q.memState.Active() {
		return q.rejectFor(&q.memState, logs)
	}

	// Disk backpressure: reject when ClickHouse disk is nearly full.
	if q.diskState.Active() {
		return q.rejectFor(&q.diskState, logs)
	}

	// Spool backpressure: when archiving is enabled and the durable spool is
	// near capacity, reject early. Combined with the fail-closed tee below this
	// guarantees we never ack a log we could not durably archive.
	if q.spoolState.Active() {
		return q.rejectFor(&q.spoolState, logs)
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

	// Settle ingest_timestamp before anything consumes the batch. It is the logs
	// and logs_raw partition key and the archive's ingest_date, so the spool tee
	// below and the ClickHouse insert must agree on one value; stamping later would
	// let them disagree across a UTC midnight. Every real ingest path already sets
	// it at parse time, so this is the fallback for one that does not.
	for i := range logs {
		logs[i].IngestTimestamp = logs[i].IngestTime()
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
			// Counted against the spool condition for reporting only: this is a
			// write failure, not the spool-capacity backpressure state.
			q.spoolState.pendingDrops.Add(n)
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

// rejectFor records a rejected batch against the condition that caused it and
// returns false so the caller responds 429.
func (q *IngestQueue) rejectFor(p *pressureState, logs []storage.LogEntry) bool {
	n := int64(len(logs))
	q.Metrics.QueueDrops.Add(n)
	p.reject(n)
	return false
}

// Healthy returns false when workers are unable to insert into ClickHouse
// or when CPU or disk backpressure is active.
func (q *IngestQueue) Healthy() bool {
	if q.cpuState.Active() || q.memState.Active() || q.diskState.Active() || q.spoolState.Active() {
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
			pct, cpuOK, err := q.queryClickHouseCPU()
			if err != nil || !cpuOK {
				// Unmeasurable is not "idle": leave the streak reset and do not
				// let a missing metric look like healthy headroom.
				q.cpuState.highStreak.Store(0)
			} else if pct >= cpuPressureTrigger {
				if q.cpuState.highStreak.Add(1) >= cpuPressureSustainSamples {
					q.raisePressure(&q.cpuState, fmt.Sprintf("%.1f", pct), fmt.Sprintf("%.1f", cpuPressureTrigger),
						fmt.Sprintf("ClickHouse CPU at %.1f%% (threshold %.1f%%)", pct, cpuPressureTrigger))
				}
			} else {
				q.cpuState.highStreak.Store(0)
				if pct < cpuPressureRelease {
					q.clearPressure(&q.cpuState, fmt.Sprintf("%.1f", pct))
				}
			}

			if diskPct, diskOK, diskErr := q.queryClickHouseDisk(); diskErr == nil && diskOK {
				if diskPct >= diskPressureTrigger {
					q.raisePressure(&q.diskState, fmt.Sprintf("%.1f", diskPct), fmt.Sprintf("%.1f", diskPressureTrigger),
						fmt.Sprintf("ClickHouse disk at %.1f%% used (threshold %.1f%%)", diskPct, diskPressureTrigger))
				} else if diskPct < diskPressureRelease {
					q.clearPressure(&q.diskState, fmt.Sprintf("%.1f", diskPct))
				}
			}

			q.monitorMemory()

			q.monitorSpool()

			q.reassertPressure()

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
	if frac >= spoolPressureTrigger {
		q.raisePressure(&q.spoolState, fmt.Sprintf("%.1f", frac*100), fmt.Sprintf("%.1f", spoolPressureTrigger*100),
			fmt.Sprintf("Archive spool at %.1f%% of its %dMB capacity", frac*100, q.spoolMaxBytes>>20))
	} else if frac < spoolPressureRelease {
		q.clearPressure(&q.spoolState, fmt.Sprintf("%.1f", frac*100))
	}
}

// raisePressure activates a condition, or refreshes its detail text when it is
// already active so reassertions report the current value. The system event and
// the initial notification fire once, on the transition.
func (q *IngestQueue) raisePressure(p *pressureState, value, threshold, detail string) {
	p.detail.Store(detail)
	if !p.active.CompareAndSwap(0, 1) {
		return
	}
	p.sinceUnix.Store(time.Now().Unix())
	p.dropsActive.Store(0)
	p.critical.Store(false)
	log.Printf("[Ingest Queue] %s backpressure ON (%s)", p.label, detail)
	q.writeSystemEvent("ingest.backpressure.on", map[string]string{
		"reason":    p.reason,
		"value":     value,
		"threshold": threshold,
	})
	q.notifyPressure(p, "warning")
}

// clearPressure deactivates a condition. One that escalated to critical (it was
// actually rejecting logs) closes its notification out as an info-level
// recovery, so the bell does not keep showing an outage that has ended.
func (q *IngestQueue) clearPressure(p *pressureState, value string) {
	if !p.active.CompareAndSwap(1, 0) {
		return
	}
	log.Printf("[Ingest Queue] %s backpressure OFF (%s)", p.label, value)
	q.writeSystemEvent("ingest.backpressure.off", map[string]string{
		"reason": p.reason,
		"value":  value,
	})
	if p.critical.Swap(false) && q.notifWriter != nil {
		dropped := p.dropsActive.Load()
		held := time.Since(time.Unix(p.sinceUnix.Load(), 0)).Round(time.Second)
		go q.notifWriter.WriteSustained(p.notif, "info", p.title+": Recovered",
			fmt.Sprintf("Ingest recovered after %s. %d log(s) were rejected while it was active.", held, dropped), 0)
	}
}

// reassertPressure keeps notifications for still-active conditions current. It
// escalates to critical once a condition has been rejecting logs for
// pressureEscalateAfter, and rewrites the notification periodically so an
// outage outliving the 24h retention window never leaves the list.
func (q *IngestQueue) reassertPressure() {
	if q.notifWriter == nil {
		return
	}
	now := time.Now().Unix()
	for _, p := range []*pressureState{&q.cpuState, &q.memState, &q.diskState, &q.spoolState} {
		if !p.Active() {
			continue
		}
		severity, interval := "warning", int64(pressureReassert.Seconds())
		escalating := false
		if p.dropsActive.Load() > 0 && now-p.sinceUnix.Load() >= int64(pressureEscalateAfter.Seconds()) {
			severity, interval = "critical", int64(criticalReassert.Seconds())
			escalating = !p.critical.Swap(true)
		}
		if escalating || now-p.lastNotifyUnix.Load() >= interval {
			q.notifyPressure(p, severity)
		}
	}
}

// notifyPressure writes or refreshes the health notification for an active
// condition at the given severity.
func (q *IngestQueue) notifyPressure(p *pressureState, severity string) {
	if q.notifWriter == nil {
		return
	}
	detail, _ := p.detail.Load().(string)
	title, interval := p.title, pressureReassert
	if severity == "critical" {
		title = p.title + ": Rejecting Logs"
		interval = criticalReassert
		held := time.Since(time.Unix(p.sinceUnix.Load(), 0)).Round(time.Minute)
		detail = fmt.Sprintf("%s. %d log(s) rejected over the last %s; ingestion is returning 429.",
			detail, p.dropsActive.Load(), held)
	}
	p.lastNotifyUnix.Store(time.Now().Unix())
	go q.notifWriter.WriteSustained(p.notif, severity, title, detail, interval)
}

// monitorMemory sets/clears memPressure from ClickHouse's (cgroup-aware) memory
// usage with trigger/release hysteresis and a short sustain, mirroring the CPU
// monitor. Guards against the OOM class of incident where inserts get killed and the
// distribution queue backs up.
func (q *IngestQueue) monitorMemory() {
	pct, ok, err := q.queryClickHouseMemory()
	if err != nil || !ok {
		q.memState.highStreak.Store(0)
		return
	}
	if pct >= memPressureTrigger {
		if q.memState.highStreak.Add(1) >= memPressureSustainSamples {
			q.raisePressure(&q.memState, fmt.Sprintf("%.1f", pct), fmt.Sprintf("%.1f", memPressureTrigger),
				fmt.Sprintf("ClickHouse memory at %.1f%% (threshold %.1f%%)", pct, memPressureTrigger))
		}
	} else {
		q.memState.highStreak.Store(0)
		if pct < memPressureRelease {
			q.clearPressure(&q.memState, fmt.Sprintf("%.1f", pct))
		}
	}
}

// queryClickHouseMemory returns the highest memory utilization (0-100) across all
// ClickHouse nodes, preferring the cgroup limit over node RAM. In cluster mode it
// takes the max so backpressure triggers when any node is near its limit.
func (q *IngestQueue) queryClickHouseMemory() (float64, bool, error) {
	addrs := q.mdb().Addrs()
	if len(addrs) <= 1 {
		return q.queryNodeMemory(nil)
	}
	var maxPct float64
	var measured bool
	var lastErr error
	for _, addr := range addrs {
		pct, ok, err := q.queryNodeMemory(&addr)
		if err != nil {
			lastErr = err
			continue
		}
		if !ok {
			continue
		}
		measured = true
		if pct > maxPct {
			maxPct = pct
		}
	}
	if measured || lastErr == nil {
		return maxPct, measured, nil
	}
	return 0, false, lastErr
}

// queryNodeMemory queries memory metrics from a single ClickHouse node.
// If addr is nil, uses the shared connection pool.
func (q *IngestQueue) queryNodeMemory(addr *string) (float64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rows []map[string]interface{}
	var err error
	if addr != nil {
		conn, openErr := q.mdb().OpenNodeConn(*addr)
		if openErr != nil {
			return 0, false, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, storage.SystemMemoryMetricsSQL)
	} else {
		rows, err = q.mdb().Query(ctx, storage.SystemMemoryMetricsSQL)
	}
	if err != nil {
		return 0, false, err
	}
	pct, ok := storage.MemoryPercentFromMetrics(storage.MetricRowsToMap(rows))
	return pct, ok, nil
}

// queryClickHouseCPU returns the highest CPU utilization (0-100) across
// all ClickHouse nodes. In single-node mode this queries via the shared
// connection pool. In cluster mode it queries each node individually and
// returns the max, so backpressure triggers when any node is overloaded.
func (q *IngestQueue) queryClickHouseCPU() (float64, bool, error) {
	addrs := q.mdb().Addrs()
	if len(addrs) <= 1 {
		return q.queryNodeCPU(nil)
	}
	var maxPct float64
	var measured bool
	var lastErr error
	for _, addr := range addrs {
		pct, ok, err := q.queryNodeCPU(&addr)
		if err != nil {
			lastErr = err
			continue
		}
		if !ok {
			continue
		}
		measured = true
		if pct > maxPct {
			maxPct = pct
		}
	}
	if measured || lastErr == nil {
		return maxPct, measured, nil
	}
	return 0, false, lastErr
}

// queryNodeCPU queries CPU metrics from a single ClickHouse node.
// If addr is nil, uses the shared connection pool.
func (q *IngestQueue) queryNodeCPU(addr *string) (float64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rows []map[string]interface{}
	var err error

	if addr != nil {
		conn, openErr := q.mdb().OpenNodeConn(*addr)
		if openErr != nil {
			return 0, false, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, storage.SystemCPUMetricsSQL)
	} else {
		rows, err = q.mdb().Query(ctx, storage.SystemCPUMetricsSQL)
	}
	if err != nil {
		return 0, false, err
	}
	pct, ok := storage.CPUPercentFromMetrics(storage.MetricRowsToMap(rows))
	return pct, ok, nil
}

// queryClickHouseDisk returns the highest disk usage percentage (0-100) across
// all ClickHouse nodes. Queries the system.disks table for the default disk.
func (q *IngestQueue) queryClickHouseDisk() (float64, bool, error) {
	addrs := q.mdb().Addrs()
	if len(addrs) <= 1 {
		return q.queryNodeDisk(nil)
	}
	var maxPct float64
	var measured bool
	var lastErr error
	for _, addr := range addrs {
		pct, ok, err := q.queryNodeDisk(&addr)
		if err != nil {
			lastErr = err
			continue
		}
		if !ok {
			continue
		}
		measured = true
		if pct > maxPct {
			maxPct = pct
		}
	}
	if measured || lastErr == nil {
		return maxPct, measured, nil
	}
	return 0, false, lastErr
}

// queryNodeDisk queries disk usage from a single ClickHouse node.
// A server that manages storage for us reports no local disk. That is not 0%
// used: it is not measurable, and the caller must skip the trigger rather than
// read an empty answer as "plenty of room".
func (q *IngestQueue) queryNodeDisk(addr *string) (float64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const diskQuery = `SELECT total_space, free_space FROM system.disks WHERE name = 'default' LIMIT 1`

	var rows []map[string]interface{}
	var err error

	if addr != nil {
		conn, openErr := q.mdb().OpenNodeConn(*addr)
		if openErr != nil {
			return 0, false, openErr
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, diskQuery)
	} else {
		rows, err = q.mdb().Query(ctx, diskQuery)
	}
	if err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, nil
	}

	total := asFloat64(rows[0]["total_space"])
	free := asFloat64(rows[0]["free_space"])
	if total <= 0 {
		return 0, false, nil
	}
	return (total - free) / total * 100, true, nil
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
	emit := func(reason string, n int64) {
		if n > 0 {
			q.writeSystemEvent("ingest.drops", map[string]string{
				"reason": reason,
				"count":  fmt.Sprintf("%d", n),
			})
		}
	}
	for _, p := range []*pressureState{&q.cpuState, &q.memState, &q.diskState, &q.spoolState} {
		emit(p.reason, p.pendingDrops.Swap(0))
	}
	emit("queue_full", q.pendingDropsQueue.Swap(0))
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

// Metrics source methods (satisfy metrics.IngestSource interface).

func (q *IngestQueue) AcceptedTotal() int64       { return q.Metrics.Accepted.Load() }
func (q *IngestQueue) InsertedTotal() int64       { return q.Metrics.Inserted.Load() }
func (q *IngestQueue) InsertErrorsTotal() int64   { return q.Metrics.InsertErrors.Load() }
func (q *IngestQueue) QueueDropsTotal() int64     { return q.Metrics.QueueDrops.Load() }
func (q *IngestQueue) RetriesTotal() int64        { return q.Metrics.Retries.Load() }
func (q *IngestQueue) CPUPressure() bool          { return q.cpuState.Active() }
func (q *IngestQueue) MemPressure() bool          { return q.memState.Active() }
func (q *IngestQueue) DiskPressure() bool         { return q.diskState.Active() }
func (q *IngestQueue) SpoolPressure() bool        { return q.spoolState.Active() }
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
			// Reassert while inserts keep failing so the notification cannot age
			// out mid-outage, rate-limited so a failing ClickHouse does not cost
			// one Postgres write per failed batch.
			if failures >= unhealthyThreshold && q.notifWriter != nil {
				now := time.Now().Unix()
				if last := q.lastInsertErrNotify.Load(); now-last >= 60 &&
					q.lastInsertErrNotify.CompareAndSwap(last, now) {
					go q.notifWriter.WriteSustained("ingest.insert_errors", "critical",
						"Ingest Insert Errors",
						fmt.Sprintf("%d consecutive insert failures; ingestion is returning 429 and logs are being rejected", failures),
						criticalReassert)
				}
			}
		}

		// Release the bucket's backing array before waiting on the next one.
		bucket.entries = nil
	}
}

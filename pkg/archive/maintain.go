package archive

import (
	"context"
	"errors"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

// MaintainOptions tunes the maintenance pass.
type MaintainOptions struct {
	// ExpireOlderThan expires snapshots older than this age (0 disables expiry).
	ExpireOlderThan time.Duration
	// RetainLast keeps at least this many recent snapshots regardless of age.
	RetainLast int
	// ByteBudget caps the total bytes compaction will rewrite in this single
	// Maintain() pass, across all tables. Bounds the pass's memory footprint
	// regardless of backlog size; leftover work carries over to the next
	// scheduled run since compaction.Analyze always replans from the table's
	// current file layout. 0 disables compaction entirely (snapshot expiry
	// still runs).
	ByteBudget int64
	// CommitRetries bounds how many times compactGroup reloads and retries a
	// single group's commit after losing an optimistic-concurrency race.
	CommitRetries int
	// Retention maps archive table name to its retention in days. Tables absent
	// from the map are kept forever, which is every fractal's default.
	Retention RetentionPolicy
	// OrphanOlderThan bounds orphan-file cleanup: files under the table location
	// that no live snapshot references AND that are older than this are deleted
	// (0 disables). The age floor is the safety margin -- an in-flight commit has
	// written its data files but not yet its metadata, so a small value would race
	// a concurrent writer and delete live data.
	OrphanOlderThan time.Duration
	// ScanConcurrency caps how many data files compaction decodes at once. This
	// is the pass's dominant memory term (see MaintainScanConcurrency), so it is
	// resolved once per pass and logged rather than left to a library default.
	ScanConcurrency int
	// MaxBatchBytes caps how many compressed bytes of rewritten groups a single
	// commit may carry. Zero derives it from the process memory limit (see
	// maxBatchBytesFor); it exists because batch size, not group size, is what a
	// pass's peak memory scales with.
	MaxBatchBytes int64
}

// DefaultMaintainOptions returns sensible defaults: keep ~7 days of snapshots,
// never fewer than 10, compact at most ~4GiB per pass, and retry a lost
// commit race up to 10 times. The byte budget was raised from an earlier 2GiB
// after observing the sealed-partition backlog outpacing compaction
// throughput; see MaintainOptionsFromEnv to tune further without a redeploy.
//
// The retry count was 3 back when a retry re-ran the whole rewrite and each
// attempt cost minutes. compactGroup now retries only the commit, so an attempt
// is one metadata write and a generous count is the cheap way to outlast a busy
// archiver's append cadence.
func DefaultMaintainOptions() MaintainOptions {
	return MaintainOptions{
		ExpireOlderThan: 7 * 24 * time.Hour,
		RetainLast:      10,
		ByteBudget:      4 << 30,
		CommitRetries:   10,
		OrphanOlderThan: 72 * time.Hour,
		ScanConcurrency: MaintainScanConcurrency(),
	}
}

// MaintainOptionsFromEnv returns DefaultMaintainOptions with ByteBudget and
// CommitRetries overridable via BIFRACT_ARCHIVE_MAINTAIN_BYTE_BUDGET (bytes)
// and BIFRACT_ARCHIVE_MAINTAIN_COMMIT_RETRIES, so an operator can raise them
// to keep pace with a growing backlog without a code change/redeploy --
// mirrors the BIFRACT_ARCHIVE_MAINTAIN_INTERVAL pattern in cmd/bifract-archiver.
func MaintainOptionsFromEnv() MaintainOptions {
	opts := DefaultMaintainOptions()
	opts.ByteBudget = getInt64("BIFRACT_ARCHIVE_MAINTAIN_BYTE_BUDGET", opts.ByteBudget)
	opts.CommitRetries = getIntEnv("BIFRACT_ARCHIVE_MAINTAIN_COMMIT_RETRIES", opts.CommitRetries)
	opts.OrphanOlderThan = getDuration("BIFRACT_ARCHIVE_ORPHAN_OLDER_THAN", opts.OrphanOlderThan)
	opts.MaxBatchBytes = getInt64("BIFRACT_ARCHIVE_MAINTAIN_MAX_BATCH_BYTES", opts.MaxBatchBytes)
	return opts
}

// defaultOrphanSweepInterval DISABLES orphan cleanup.
//
// iceberg-go v0.6.0's Table.DeleteOrphanFiles livelocks on the gocloud S3/MinIO
// backend: measured against a live MinIO archive it burned ~1.25 cores for 7+
// minutes while issuing ~200 bytes of network traffic, and never finished. That
// is not merely slow -- the sweep runs inside the maintain pass, which holds the
// singleton advisory lock, so a wedged sweep blocks compaction, retention, and
// snapshot expiry indefinitely. The failure mode is far worse than the leak it
// cleans up.
//
// The plumbing below is kept because it is correct and cheap, and orphans are
// already-unreachable bytes (a leak, never a correctness problem). Set
// BIFRACT_ARCHIVE_ORPHAN_INTERVAL to enable it once upstream is fixed, and
// validate against your own backend first.
const defaultOrphanSweepInterval = 0

// OrphanSweepIntervalFromEnv returns the orphan-cleanup cadence, overridable via
// BIFRACT_ARCHIVE_ORPHAN_INTERVAL. Zero (the default) disables the sweep.
func OrphanSweepIntervalFromEnv() time.Duration {
	return getDuration("BIFRACT_ARCHIVE_ORPHAN_INTERVAL", defaultOrphanSweepInterval)
}

// scanWorkerMemoryBudget is the all-in memory a compaction pass needs per
// concurrent file reader: the reader's own decoded Parquet row group plus its
// share of the rolling writer's buffer, catalog metadata, and runtime overhead.
//
// MEASURED, not estimated: a single-worker pass compacting 3.8GB across 512MB
// groups peaked at 2.34GB RSS. Groups are bin-packed to TargetFileSizeBytes
// before compaction ever sees them, so this does not grow with backlog or
// partition size -- which is why one number works across every deployment size.
// Rounded up, because underestimating costs an OOMKill and overestimating costs
// a slower pass, and compaction has multiples of headroom over ingest.
const scanWorkerMemoryBudget = 2560 << 20

// fallbackScanConcurrency is used only when the process memory limit is
// unknown (no cgroup limit and no GOMEMLIMIT), where there is no budget to
// divide. Bounded rather than GOMAXPROCS-wide because an unknown limit is not
// evidence of a large one.
const fallbackScanConcurrency = 2

// MaintainScanConcurrency resolves how many data files compaction may decode
// concurrently. This is compaction's dominant memory term and the direct
// analogue of Iceberg's max-concurrent-file-group-rewrites, which upstream
// guidance says to lower when memory pressure appears.
//
// It is derived from the process memory limit rather than from CPU count: a
// fixed constant (this was 4) is unrelated to how much memory the container
// actually has, and on a 2GB limit it reliably OOMKilled the maintainer mid
// pass, every pass, forever -- compaction.Analyze replans identical work each
// time, so nothing about the retry ever differed. GOMAXPROCS is still an upper
// bound (more decoders than cores buys nothing), and
// BIFRACT_ARCHIVE_MAINTAIN_SCAN_CONCURRENCY overrides the whole calculation.
//
// automemlimit sets GOMEMLIMIT from the cgroup at startup on both Docker and
// k8s, so reading the runtime's limit here works identically on both without
// any platform-specific probing.
func MaintainScanConcurrency() int {
	if n := getIntEnv("BIFRACT_ARCHIVE_MAINTAIN_SCAN_CONCURRENCY", 0); n > 0 {
		return n
	}
	return scanConcurrencyFor(processMemoryLimit(), runtime.GOMAXPROCS(0))
}

// scanConcurrencyFor is MaintainScanConcurrency's arithmetic, split out so the
// clamps are testable without a real cgroup. memLimit of 0 means unknown.
//
// The floor of 1 is a deliberate choice to attempt the pass rather than refuse
// it: a limit too small for even one worker is a deployment problem, reported by
// MaintainMemoryUndersized so it reads as a sizing error instead of a silent
// hourly OOMKill.
func scanConcurrencyFor(memLimit int64, cpus int) int {
	cpus = max(1, cpus)
	if memLimit <= 0 {
		return min(cpus, fallbackScanConcurrency)
	}
	return max(1, min(int(memLimit/scanWorkerMemoryBudget), cpus))
}

// MaintainMemoryUndersized reports whether the process memory limit is below
// what one compaction worker needs, i.e. the pass is expected to be OOMKilled.
// Returns the limit and the requirement so the caller can say so precisely.
// False when the limit is unknown: absence of a limit is not evidence of a
// small one.
func MaintainMemoryUndersized() (undersized bool, limit, need int64) {
	limit = processMemoryLimit()
	return limit > 0 && limit < scanWorkerMemoryBudget, limit, scanWorkerMemoryBudget
}

// processMemoryLimit reports the Go runtime's memory limit in bytes, or 0 when
// none is set. math.MaxInt64 is the runtime's "no limit" sentinel.
func processMemoryLimit() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return 0
	}
	return limit
}

// peakRSSBytes reports the process's peak resident set size from
// /proc/self/status (VmHWM), or 0 where unavailable. This is the number the
// cgroup OOM killer acts on, so it is what a pass should report; it is a
// high-water mark since process start, not per-pass. Linux-only by
// construction, which is every environment Bifract ships in.
func peakRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmHWM:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

// maintainCommitConflictBackoff is the delay before compactGroup reloads and
// retries after losing a commit race.
//
// Deliberately short. It was 2s, which made sense when a retry re-ran the whole
// rewrite: pausing before minutes of work costs nothing. Now that only the
// commit is retried, the race is won by reloading and committing inside the gap
// between two of the archiver's commits, and a long pause is the one thing
// guaranteed to lose it -- a busy fractal moves the branch head many times over
// during a 2s sleep, so every attempt woke into a stale table and burned a retry
// for nothing. Measured against a writer committing every 100ms, 2s failed every
// group; this succeeds.
const maintainCommitConflictBackoff = 100 * time.Millisecond

// MaintainOutcome records how a maintain invocation concluded, persisted
// alongside its stats (see WriteMaintainStatus/WriteMaintainOutcome) so the
// admin UI can distinguish a healthy pass from a crash or a skip, instead of
// an invocation that wrote nothing looking identical to "ran, nothing to do".
type MaintainOutcome string

const (
	MaintainOutcomeOK              MaintainOutcome = "ok"
	MaintainOutcomeError           MaintainOutcome = "error"
	MaintainOutcomeSkippedLocked   MaintainOutcome = "skipped_locked"
	MaintainOutcomeSkippedDisabled MaintainOutcome = "skipped_disabled"
	// MaintainOutcomeTimeout marks a pass abandoned at MaintainPassTimeout. It is
	// distinct from a plain error so a wedged object-store call is visibly not
	// the same failure as a pass that ran and returned one.
	MaintainOutcomeTimeout MaintainOutcome = "timeout"
	// MaintainOutcomeInterrupted marks a pass that died without writing its own
	// outcome -- OOMKill, node eviction, SIGKILL. It is recorded retroactively by
	// ReconcileInterruptedMaintain, since by definition the pass itself cannot.
	MaintainOutcomeInterrupted MaintainOutcome = "interrupted"
	// MaintainOutcomeRunning is a transient marker written when a pass starts, so
	// the admin UI can show a live in-progress state. It is never appended to the
	// history table (that records only terminal outcomes).
	MaintainOutcomeRunning MaintainOutcome = "running"
)

// MaintainStats summarizes one Maintain() pass, for logging and for
// persisting to the archive_maintain_status row the admin UI's System ->
// Archive panel reads (see WriteMaintainStatus). CandidateBytes is the total
// backlog compaction.Analyze found across every table this pass reached --
// it under-reports total system-wide backlog when the pass-wide budget runs
// out before every table gets a turn (Analyze is only run while there's still
// budget to spend), when a table's LoadTable or Analyze call itself errors
// (that table's backlog silently isn't measured this pass either), and
// because today's still-open ingest_date partition is excluded from
// candidacy entirely (see isOpenPartitionGroup) so it never contributes
// bytes here regardless of how much small-file backlog it's accumulating;
// treat it as "sealed backlog found among tables this pass could inspect,"
// not a precise system-wide total.
type MaintainStats struct {
	Tables         int
	Compacted      int
	GroupsFailed   int
	Expired        int
	CandidateBytes int64
	CompactedBytes int64
	Duration       time.Duration

	// FootprintBytes/FootprintRecords are the archive's total size, summed from
	// each table's current-snapshot summary. Collected here because the pass
	// already holds every table loaded, which makes it free; the drain loop used
	// to gather it on a 30s heartbeat at the cost of a metadata.json read per
	// fractal per replica.
	FootprintBytes   int64
	FootprintRecords int64

	// RetentionTables/RetentionFiles count tables that had expired ingest_date
	// partitions dropped this pass, and the data files dropped across them.
	RetentionTables int
	RetentionFiles  int
	// OrphansDeleted counts unreferenced files reclaimed this pass.
	OrphansDeleted int
}

// Maintain runs compaction + snapshot expiry across every fractal's Iceberg
// table. It is intended to run as a SINGLETON (k8s CronJob with concurrencyPolicy
// Forbid, or leader-elected via a Postgres advisory lock) so concurrent passes
// never fight over table metadata. Errors on individual tables are logged and
// skipped so one bad table does not abort the whole pass.
//
// Per table the pass runs compaction, then archive retention (dropping expired
// ingest_date partitions), then snapshot expiry. Orphan-file cleanup is wired in
// but disabled by default; see defaultOrphanSweepInterval.
func (c *Catalog) Maintain(ctx context.Context, opts MaintainOptions) (MaintainStats, error) {
	start := time.Now()
	ns := catalog.ToIdentifier(Namespace)

	if opts.ScanConcurrency < 1 {
		opts.ScanConcurrency = MaintainScanConcurrency()
	}
	// Resolved after ScanConcurrency because the derivation divides by it: the
	// two share one memory pool, so more workers means smaller batches.
	if opts.MaxBatchBytes <= 0 {
		opts.MaxBatchBytes = maxBatchBytesFor(processMemoryLimit(), opts.ScanConcurrency)
	}
	// Logged before any work: a pass killed by the OOM killer writes no outcome
	// of its own, so this line is the only record that it started and of the
	// settings it started with.
	log.Printf("[Maintain] starting: scan concurrency %d, byte budget %d, batch cap %d, memory limit %d, GOMAXPROCS %d",
		opts.ScanConcurrency, opts.ByteBudget, opts.MaxBatchBytes, processMemoryLimit(), runtime.GOMAXPROCS(0))
	if undersized, limit, need := MaintainMemoryUndersized(); undersized {
		log.Printf("[Maintain] WARNING: memory limit %d bytes is below the %d bytes a compaction pass needs; "+
			"this pass will likely be killed before it finishes. Raise the maintainer's memory limit "+
			"(BIFRACT_ARCHIVE_MAINTAIN_MEM_LIMIT on Docker, the archive-maintain Deployment's limit on k8s).",
			limit, need)
	}

	var idents []icetable.Identifier
	for ident, err := range c.cat.ListTables(ctx, ns) {
		if err != nil {
			log.Printf("[Maintain] list tables: %v", err)
			continue
		}
		idents = append(idents, ident)
	}

	// The pass iterates catalog tables, so re-key the fractal-scoped policy once.
	retention := opts.Retention.ByTable()

	stats := MaintainStats{Tables: len(idents)}
	budget := opts.ByteBudget
	for i, ident := range idents {
		name := ident[len(ident)-1]

		tbl, err := c.cat.LoadTable(ctx, ident)
		if err != nil {
			log.Printf("[Maintain] load %s: %v", name, err)
		} else {
			addFootprint(&stats, tbl)

			// Split the remaining budget evenly across the tables not yet
			// visited this pass, so one early, backlog-heavy table can't
			// permanently starve tables later in iteration order. A table's
			// unused share simply isn't debited (budget is only reduced by bytes
			// actually compacted), so it grows later tables' shares in turn.
			if budget > 0 {
				share := budget / int64(len(idents)-i)
				res, err := compactTable(ctx, c, ident, tbl, share, tableDeadline(ctx, len(idents)-i), opts)
				if err != nil {
					log.Printf("[Maintain] compact %s: %v", name, err)
				} else {
					stats.CandidateBytes += res.candidateBytes
					if res.compactedBytes > 0 {
						stats.Compacted++
						budget -= res.compactedBytes
						stats.CompactedBytes += res.compactedBytes
					}
					if res.failedGroups > 0 {
						stats.GroupsFailed += res.failedGroups
						log.Printf("[Maintain] compact %s: %d group(s) failed after retries, skipped", name, res.failedGroups)
					}
				}
			}
		}

		// Retention before expiry: the delete only unlinks files from the current
		// snapshot, and it is expireSnapshots that actually reclaims them once no
		// retained snapshot references them. Running it first lets a single pass
		// drop and (for already-aged snapshots) free in one go.
		if days := retention[name]; days > 0 {
			res, err := applyRetention(ctx, c, ident, days, opts.ScanConcurrency)
			if err != nil {
				log.Printf("[Maintain] retention %s: %v", name, err)
			} else if res.Deleted {
				stats.RetentionTables++
				stats.RetentionFiles += res.Files
				log.Printf("[Maintain] retention %s: dropped %d file(s) before %s (keep %dd)",
					name, res.Files, res.Cutoff.Format("2006-01-02"), days)
			}
		}

		if opts.ExpireOlderThan > 0 {
			if err := expireSnapshots(ctx, c, ident, opts); err != nil {
				log.Printf("[Maintain] expire %s: %v", name, err)
			} else {
				stats.Expired++
			}
		}

		// Orphan cleanup last, so it sees the files the two steps above just
		// unlinked. Reloads the table: it must not act on pre-expiry state.
		if opts.OrphanOlderThan > 0 {
			deleted, err := cleanOrphans(ctx, c, ident, opts.OrphanOlderThan, opts.ScanConcurrency)
			if err != nil {
				log.Printf("[Maintain] orphans %s: %v", name, err)
			} else if deleted > 0 {
				stats.OrphansDeleted += deleted
				log.Printf("[Maintain] orphans %s: deleted %d unreferenced file(s)", name, deleted)
			}
		}
	}
	stats.Duration = time.Since(start)
	log.Printf("[Maintain] done: %d tables, %d compacted, %d groups failed, %d expired, %d/%d backlog bytes compacted, retention dropped %d file(s) across %d table(s), %d orphan(s) reclaimed, footprint %d bytes / %d records, peak RSS %d bytes (limit %d), took %s",
		stats.Tables, stats.Compacted, stats.GroupsFailed, stats.Expired, stats.CompactedBytes, stats.CandidateBytes,
		stats.RetentionFiles, stats.RetentionTables, stats.OrphansDeleted,
		stats.FootprintBytes, stats.FootprintRecords, peakRSSBytes(), processMemoryLimit(), stats.Duration)
	// Per-table errors are logged and skipped so one bad table cannot abort the
	// pass, which meant a pass abandoned at its deadline returned nil and was
	// recorded as a healthy 'ok'. Report the deadline explicitly: the caller pairs
	// it with the partial stats above so a truncated pass is visibly truncated
	// rather than looking like a clean run that found nothing to do.
	if err := ctx.Err(); err != nil {
		log.Printf("[Maintain] pass abandoned before visiting every table: %v", err)
		return stats, err
	}
	return stats, nil
}

// maintainMaxConsecutiveCommitFailures bounds how many batch commits in a row
// may fail before compactTable gives up on the table for this pass.
const maintainMaxConsecutiveCommitFailures = 3

// maintainMaxGroupsPerCommit bounds a single rewrite commit's blast radius. The
// commit's manifest scan is amortised across the batch, so bigger is cheaper per
// group, but a lost race discards the whole batch's commit attempt and its output
// files become orphans if the retries then run out. This caps that exposure while
// still collapsing the common budget-sized batch into one scan.
const maintainMaxGroupsPerCommit = 16

// maintainBatchMemoryNum/Den encode the measured relationship between a batch
// and the pass's peak memory: RSS ~= 2x the batch's compressed bytes times the
// scan concurrency. Three production data points, one Docker deployment,
// 512MiB-target groups:
//
//	concurrency 1, 2.07GB batched -> 4.04 GiB peak RSS (OOMKilled at a 4g limit)
//	concurrency 2, 2.07GB batched -> 8.03 GiB peak RSS
//	concurrency 1, 1.05GB batched -> 1.52 GiB peak RSS
//
// The mechanism is partly empirical -- groups are rewritten sequentially, so
// something (GC headroom under GOMEMLIMIT, buffers reachable from retained
// results) keeps memory from being reclaimed between groups -- but the fit is
// consistent across all three points and the cap below only needs the ceiling,
// not the mechanism. Budgeted at 2.5x rather than the fitted 2x so ~20% of the
// limit is headroom: cap = limit/(2.5*concurrency) means predicted peak
// 2*cap*concurrency = 0.8*limit.
const (
	maintainBatchMemoryNum = 2
	maintainBatchMemoryDen = 5
)

// maxBatchBytesFor derives the per-commit batch cap from the process memory
// limit, the same way MaintainScanConcurrency derives its worker count. Peak
// memory scales with batch bytes x concurrency (see maintainBatchMemoryNum), so
// a fixed group-count cap re-OOMs at every limit: more memory buys more workers,
// which multiplies right back. An unknown limit falls back to one target-sized
// group, which is the pre-batching memory profile scanWorkerMemoryBudget was
// measured against -- absence of a limit is not evidence of a large one.
//
// The floor of one group is deliberate and load-bearing: compactTable always
// attempts the first group of a pass regardless of any cap, so the smallest
// batch is one group and the worst case under a tiny limit is exactly the
// pre-batching behavior, not a stall.
func maxBatchBytesFor(memLimit int64, scanConcurrency int) int64 {
	scanConcurrency = max(1, scanConcurrency)
	if memLimit <= 0 {
		return targetFileSizeBytes
	}
	return memLimit * maintainBatchMemoryNum / (maintainBatchMemoryDen * int64(scanConcurrency))
}

// tableDeadline returns when the current table must stop compacting so the
// tables after it still get a turn. Splits the pass's remaining time evenly
// across the tables not yet visited, mirroring how ByteBudget is shared. With no
// pass deadline set (an operator running an unbounded catch-up by hand) it
// returns a far-future time, i.e. no per-table bound.
func tableDeadline(ctx context.Context, remainingTables int) time.Time {
	passDeadline, ok := ctx.Deadline()
	if !ok {
		return time.Now().Add(math.MaxInt32 * time.Second)
	}
	if remainingTables < 1 {
		remainingTables = 1
	}
	return time.Now().Add(time.Until(passDeadline) / time.Duration(remainingTables))
}

// addFootprint accumulates a table's size from its current-snapshot summary. A
// table with no snapshot yet (created, never appended to) contributes nothing.
func addFootprint(stats *MaintainStats, tbl *icetable.Table) {
	snap := tbl.CurrentSnapshot()
	if snap == nil || snap.Summary == nil || snap.Summary.Properties == nil {
		return
	}
	stats.FootprintBytes += int64(snap.Summary.Properties.GetInt("total-files-size", 0))
	stats.FootprintRecords += int64(snap.Summary.Properties.GetInt("total-records", 0))
}

// compactResult summarizes one table's compaction attempt within a Maintain
// pass: how much of its backlog Analyze found (candidateBytes, regardless of
// budget), how much actually got compacted (compactedBytes > 0 doubles as
// "something in this table was compacted" -- no separate bool, since nothing
// ever sets one without the other), and how many selected groups exhausted
// their retries and were skipped rather than committed.
type compactResult struct {
	compactedBytes int64
	failedGroups   int
	candidateBytes int64
}

// compactTable merges small data files within each partition into larger ones,
// committing one group at a time (see compactGroup) so one group's lost commit
// race costs only that group. Bounded by three things: budget bytes, the
// per-table deadline (see tableDeadline), and a run of consecutive failures.
//
// Today's ingest_date partition is skipped (see isOpenPartitionGroup) because it
// is still accumulating files, so compacting it is work that immediately needs
// redoing; it becomes an ordinary candidate once sealed at day rollover. Note
// this does NOT avoid commit conflicts, which are asserted on the branch head
// and so are moved by an append to any partition -- an earlier version of this
// comment claimed otherwise.
//
// No-op when the plan is empty, which is the healthy steady state once the
// archiver's roll thresholds are large enough that written files clear
// compaction's optimal-file floor.
func compactTable(ctx context.Context, c *Catalog, ident icetable.Identifier, tbl *icetable.Table, budget int64, deadline time.Time, opts MaintainOptions) (compactResult, error) {
	// Normally resolved by Maintain; re-derived here so a direct caller can never
	// run with an unbounded (or zero, which would mean one-group) batch.
	if opts.MaxBatchBytes <= 0 {
		opts.MaxBatchBytes = maxBatchBytesFor(processMemoryLimit(), opts.ScanConcurrency)
	}
	plan, err := compaction.Analyze(ctx, tbl, compaction.DefaultConfig())
	if err != nil {
		return compactResult{}, err
	}

	open := iceberg.Date(epochDay(time.Now()))
	sealed := plan.Groups[:0]
	for _, g := range plan.Groups {
		if isOpenPartitionGroup(g, open) {
			continue
		}
		sealed = append(sealed, g)
	}
	plan.Groups = sealed

	if len(plan.Groups) == 0 {
		return compactResult{}, nil
	}

	var candidateBytes, largestGroup int64
	for _, g := range plan.Groups {
		candidateBytes += g.TotalSizeBytes
		largestGroup = max(largestGroup, g.TotalSizeBytes)
	}

	name := ident[len(ident)-1]
	// Logged before the first rewrite so a pass that dies mid-compaction leaves
	// behind what it was attempting. Without it an OOMKill is indistinguishable
	// from a hang: nothing at all is written between pass start and pass end.
	log.Printf("[Maintain] compact %s: plan has %d group(s), %d candidate bytes, largest group %d bytes; %d byte budget at concurrency %d, deadline %s",
		name, len(plan.Groups), candidateBytes, largestGroup, budget, opts.ScanConcurrency,
		time.Until(deadline).Round(time.Second))

	// Groups are rewritten into a BATCH and committed together, because the
	// commit -- not the rewrite -- is what compaction costs. iceberg-go's rewrite
	// commit walks every manifest entry in the table to find the ones referencing
	// the replaced files (overwriteFiles.existingManifests), so its cost is
	// O(files in table) and is paid in full per commit no matter how little the
	// commit changes. Measured on a 96GB/184-group table that is ~5m30s a commit
	// against ~10s to rewrite a 512MB group: committing per group spent 97% of
	// every attempt re-scanning manifests. Batching amortises one scan across
	// many groups, and just as importantly means the race against the archiver's
	// concurrent appends has to be won once per batch instead of once per group.
	//
	// A failed rewrite skips its group without debiting the budget so the batch
	// moves on. The first group of the pass is always attempted even if it alone
	// exceeds budget, so a budget smaller than one group cannot stall the table
	// forever; that is safe because compaction.Analyze bin-packs to
	// TargetFileSizeBytes, so a group is bounded by file size, never by partition
	// size.
	res := compactResult{candidateBytes: candidateBytes}
	var spent int64
	var consecutiveFailures int
	for i := 0; i < len(plan.Groups); {
		// Per-table deadline: without it one backlog-heavy table consumes the whole
		// pass and every table after it in iteration order is never even loaded,
		// which is why timed-out passes reported a fraction of the real footprint.
		if !time.Now().Before(deadline) {
			log.Printf("[Maintain] compact %s: table time budget spent, %d bytes committed; leaving the rest for the next pass", name, spent)
			break
		}
		// A table failing commit after commit is systematically broken (bad
		// credentials, unreadable files), not unlucky. Give up on it rather than
		// burning the table's whole turn.
		if consecutiveFailures >= maintainMaxConsecutiveCommitFailures {
			log.Printf("[Maintain] compact %s: %d consecutive commit failures; skipping the rest of this table", name, consecutiveFailures)
			break
		}
		if spent > 0 && spent >= budget {
			break
		}

		var batch []icetable.CompactionGroupResult
		var batchBytes int64
		for ; i < len(plan.Groups); i++ {
			g := plan.Groups[i]
			// Budget exempts only the very first group of the PASS, so a budget
			// smaller than one group cannot stall the table forever.
			if !(spent == 0 && len(batch) == 0) && spent+batchBytes+g.TotalSizeBytes > budget {
				break
			}
			if len(batch) > 0 {
				// The memory bound. Peak RSS tracks batch bytes x scan concurrency
				// (see maintainBatchMemoryNum), so this -- not the group count -- is
				// what keeps the pass inside its cgroup. It bounds a BATCH, so only a
				// non-empty batch may refuse a group: a group alone in its own batch
				// is exactly the pre-batching memory profile, which always fits. An
				// exemption keyed on the pass instead (like budget's) deadlocks --
				// after the first commit, a group over the cap could never start a
				// new batch, and everything behind it would silently never compact.
				if batchBytes+g.TotalSizeBytes > opts.MaxBatchBytes {
					break
				}
				if len(batch) >= maintainMaxGroupsPerCommit {
					break
				}
				// Stop growing the batch once out of time, but still commit what has
				// already been rewritten: abandoning it would waste the rewrites and
				// leave their output as orphans. An empty batch past the deadline
				// cannot happen: the outer loop checks it before building one.
				if !time.Now().Before(deadline) {
					break
				}
			}

			gr, err := icetable.ExecuteCompactionGroup(ctx, tbl, icetable.CompactionTaskGroup{
				PartitionKey:   g.PartitionKey,
				Tasks:          g.Tasks,
				TotalSizeBytes: g.TotalSizeBytes,
			}, icetable.WithCompactionScanConcurrency(opts.ScanConcurrency))
			if err != nil {
				log.Printf("[Maintain] compact %s: rewrite group %s: %v", name, g.PartitionKey, err)
				res.failedGroups++
				continue
			}
			if len(gr.NewDataFiles) == 0 {
				continue
			}
			batch = append(batch, gr)
			batchBytes += g.TotalSizeBytes
		}

		if len(batch) == 0 {
			break
		}

		log.Printf("[Maintain] compact %s: committing %d group(s)/%d bytes in one rewrite", name, len(batch), batchBytes)
		updated, err := commitCompaction(ctx, c, ident, tbl, batch, opts)
		tbl = updated
		if err != nil {
			log.Printf("[Maintain] compact %s: commit of %d group(s): %v", name, len(batch), err)
			res.failedGroups += len(batch)
			consecutiveFailures++
			continue
		}
		consecutiveFailures = 0
		res.compactedBytes += batchBytes
		spent += batchBytes
	}
	return res, nil
}

// isOpenPartitionGroup reports whether g belongs to today's (or, by clock
// skew, a future) ingest_date partition -- the only partition the archiver
// may still append to. Every task in a Group shares one partition (that's
// the grouping contract), so the first task's value stands in for the whole
// group. Returns false (i.e. treats as sealed/compactable) if the partition
// value is missing or an unexpected type, so a scan/schema surprise degrades
// to the old behavior rather than silently excluding everything.
func isOpenPartitionGroup(g compaction.Group, today iceberg.Date) bool {
	if len(g.Tasks) == 0 {
		return false
	}
	v, ok := g.Tasks[0].File.Partition()[partitionFieldID]
	if !ok {
		return false
	}
	d, ok := v.(iceberg.Date)
	return ok && d >= today
}

// commitCompaction stages every already-rewritten group in ONE rewrite snapshot
// and commits it, retrying only the commit.
//
// Batching is the point. iceberg-go's rewrite commit walks every manifest entry
// in the table to find those referencing the replaced files
// (overwriteFiles.existingManifests), so a commit costs O(files in table)
// regardless of how few files it changes. Committing per group therefore paid
// that scan once per group; measured on a 96GB table it was ~5m30s per commit
// against ~10s to rewrite a group. One scan per batch is the difference between
// compaction keeping up and not.
//
// Retrying only the commit matters for a second reason: iceberg-go's own
// Table.doCommit retry loop is gated on errors.Is(err, icetable.ErrCommitFailed),
// and the conflict this hits in production is a bare, unwrapped error from the
// pre-commit Requirement.Validate() check in catalog/internal ("requirement
// failed: branch \"main\" has changed: ..."). That never satisfies the gate, so
// doCommit returns on the first conflict without ever retrying.
//
// Each attempt re-stages the same output files against a freshly loaded table.
// That is safe because the output Parquet is immutable and already durable in
// object storage, and re-staging goes through NewRewrite/ApplyResult so the
// rewrite conflict validator still runs against the fresh state.
//
// Always returns the freshest table handle available -- even on final failure --
// so the caller carries forward whatever this call last reloaded rather than
// starting from an already-known-stale table.
func commitCompaction(ctx context.Context, c *Catalog, ident icetable.Identifier, tbl *icetable.Table, results []icetable.CompactionGroupResult, opts MaintainOptions) (*icetable.Table, error) {
	if len(results) == 0 {
		return tbl, nil
	}
	name := ident[len(ident)-1]

	retries := opts.CommitRetries
	if retries < 1 {
		retries = 1
	}
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		// A fresh transaction and builder per attempt: RewriteFiles.Commit is
		// single-shot, and the requirements have to be rebuilt from the reloaded
		// table or the retry re-submits the same stale branch assertion.
		tx := tbl.NewTransaction()
		rw := tx.NewRewrite(nil)
		for _, r := range results {
			rw = rw.ApplyResult(r)
		}
		err := rw.Commit(ctx)
		if err == nil {
			var updated *icetable.Table
			if updated, err = tx.Commit(ctx); err == nil {
				writeVersionHint(ctx, updated)
				return updated, nil
			}
		}

		if !isCommitConflict(err) {
			return tbl, err
		}
		lastErr = err
		if attempt == retries-1 {
			break
		}
		log.Printf("[Maintain] compact %s: lost commit race with concurrent append, re-staging %d group(s) (%d/%d)",
			name, len(results), attempt+1, retries)
		if !sleep(ctx, maintainCommitConflictBackoff) {
			return tbl, ctx.Err()
		}
		reloaded, err2 := c.cat.LoadTable(ctx, ident)
		if err2 != nil {
			return tbl, err2
		}
		tbl = reloaded
	}
	return tbl, lastErr
}

// isCommitConflict reports whether err is a retryable optimistic-concurrency
// conflict rather than a permanent failure: either iceberg-go's sentinel for
// a catalog-level compare-and-swap loss (ErrCommitFailed -- e.g. the SQL
// catalog's "metadata-location moved underneath us"), or the bare
// "requirement failed: ..." error the shared pre-commit Requirement.Validate()
// check returns unwrapped when a branch/tag moved since this transaction's
// table was loaded. iceberg-go's own commit-retry loop only covers the first
// shape (it's gated on errors.Is(err, ErrCommitFailed)); the second is not a
// stable/versioned API contract, just iceberg-go's current error text, so a
// future upstream wording change could silently stop this matching.
func isCommitConflict(err error) bool {
	return errors.Is(err, icetable.ErrCommitFailed) || strings.Contains(err.Error(), "requirement failed")
}

// cleanOrphans deletes files under the table's location that no live snapshot
// references. These accumulate from commits that wrote data files and then
// failed, and from compaction rewrites whose commit lost a race -- neither is
// reachable, so nothing else ever reclaims them.
//
// olderThan is a safety margin, not a policy: a writer that has uploaded its
// Parquet but not yet committed its metadata looks exactly like an orphan, so
// only files older than any plausible in-flight commit are eligible. Reloads the
// table so it sees the post-expiry snapshot set.
// Reports a file count only, deliberately: OrphanCleanupResult.TotalSizeBytes is
// the size of every file the scan considered, not of the orphans it deleted, so
// surfacing it as reclaimed bytes would overstate by roughly the table size on
// every pass.
func cleanOrphans(ctx context.Context, c *Catalog, ident icetable.Identifier, olderThan time.Duration, concurrency int) (int, error) {
	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return 0, err
	}
	res, err := tbl.DeleteOrphanFiles(ctx,
		icetable.WithFilesOlderThan(olderThan),
		icetable.WithMaxConcurrency(concurrency),
	)
	if err != nil {
		return 0, err
	}
	return len(res.DeletedFiles), nil
}

// expireSnapshots drops snapshots older than the retention window (keeping at
// least RetainLast) to bound Iceberg metadata growth. Reloads the table so it
// operates on the post-compaction state.
func expireSnapshots(ctx context.Context, c *Catalog, ident icetable.Identifier, opts MaintainOptions) error {
	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return err
	}
	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(
		icetable.WithOlderThan(opts.ExpireOlderThan),
		icetable.WithRetainLast(opts.RetainLast),
	); err != nil {
		return err
	}
	updated, err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	// Runs on every maintenance pass, so this also backfills the hint for dormant
	// fractals that no longer receive archiver commits.
	writeVersionHint(ctx, updated)
	return nil
}

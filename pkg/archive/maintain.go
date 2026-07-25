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
}

// DefaultMaintainOptions returns sensible defaults: keep ~7 days of snapshots,
// never fewer than 10, compact at most ~4GiB per pass, and retry a lost
// commit race up to 3 times. The byte budget was raised from an earlier 2GiB
// after observing the sealed-partition backlog outpacing compaction
// throughput; see MaintainOptionsFromEnv to tune further without a redeploy.
func DefaultMaintainOptions() MaintainOptions {
	return MaintainOptions{
		ExpireOlderThan: 7 * 24 * time.Hour,
		RetainLast:      10,
		ByteBudget:      4 << 30,
		CommitRetries:   3,
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

// maintainCommitConflictBackoff is the fixed delay before compactGroup
// reloads and retries after losing a commit race, so the retry doesn't
// immediately re-collide with a writer on a regular commit cadence.
const maintainCommitConflictBackoff = 2 * time.Second

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
	// Logged before any work: a pass killed by the OOM killer writes no outcome
	// of its own, so this line is the only record that it started and of the
	// settings it started with.
	log.Printf("[Maintain] starting: scan concurrency %d, byte budget %d, memory limit %d, GOMAXPROCS %d",
		opts.ScanConcurrency, opts.ByteBudget, processMemoryLimit(), runtime.GOMAXPROCS(0))
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
				res, err := compactTable(ctx, c, ident, tbl, share, opts)
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
	return stats, nil
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

// compactTable merges small data files within each partition into larger
// ones, committing one file group at a time (see compactGroup) so a lost
// race with a concurrent append only costs a retry of that one group's
// rewrite, not the whole pass's budget -- the "partial progress" pattern
// Iceberg operators use for actively-written tables, since a single big
// commit racing a busy writer can lose every attempt indefinitely. Today's
// ingest_date partition is dropped from the plan before selection (see
// isOpenPartitionGroup): it is the only partition the archiver still appends
// to (schema.go's monotonic roll-on-ingest_date), so a compaction commit
// racing it can never be won by retrying harder -- it becomes an ordinary,
// uncontested compaction candidate once sealed at day rollover. Of what's
// left, groups are selected up to budget bytes; the first selected group is
// always included even if it alone exceeds budget, so a budget smaller than
// one group cannot stall the table forever. That valve is safe because
// compaction.Analyze bin-packs groups to TargetFileSizeBytes before we ever
// see them: a group is bounded by file size (a single file larger than the
// target simply gets its own bin, so the ceiling is MaxFileSizeBytes), never
// by partition size. Group size is therefore not what makes a pass expensive
// -- ScanConcurrency is. No-op when the plan is empty (files already large
// enough - the common case given roll-on-size at write time).
func compactTable(ctx context.Context, c *Catalog, ident icetable.Identifier, tbl *icetable.Table, budget int64, opts MaintainOptions) (compactResult, error) {
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

	var selected []compaction.Group
	var groupBytes int64
	for _, g := range plan.Groups {
		if len(selected) > 0 && groupBytes+g.TotalSizeBytes > budget {
			break
		}
		selected = append(selected, g)
		groupBytes += g.TotalSizeBytes
	}
	// Logged before the first rewrite so a pass that dies mid-compaction leaves
	// behind what it was attempting. Without it an OOMKill is indistinguishable
	// from a hang: nothing at all is written between pass start and pass end.
	log.Printf("[Maintain] compact %s: plan has %d group(s), %d candidate bytes, largest group %d bytes; rewriting %d group(s)/%d bytes against a %d byte budget at concurrency %d",
		ident[len(ident)-1], len(plan.Groups), candidateBytes, largestGroup,
		len(selected), groupBytes, budget, opts.ScanConcurrency)
	if groupBytes > budget {
		log.Printf("[Maintain] compact %s: lead group is %d bytes, over this pass's %d byte budget for the table; compacting it anyway rather than stalling on it forever",
			ident[len(ident)-1], groupBytes, budget)
	}

	res := compactResult{candidateBytes: candidateBytes}
	for _, g := range selected {
		group := icetable.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
		updated, err := compactGroup(ctx, c, ident, tbl, group, opts)
		tbl = updated
		if err != nil {
			log.Printf("[Maintain] compact %s: group %s: %v", ident[len(ident)-1], g.PartitionKey, err)
			res.failedGroups++
			continue
		}
		res.compactedBytes += g.TotalSizeBytes
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

// compactGroup rewrites and commits a single compaction group, retrying
// against a freshly reloaded table if it loses a commit race against the
// live archiver appending to the same table. iceberg-go's own Table.doCommit
// has a built-in retry-with-backoff loop, but it is gated on
// errors.Is(err, icetable.ErrCommitFailed) -- the requirement-validation
// conflict this actually hits in production (a bare, unwrapped error from the
// shared pre-commit Requirement.Validate() check, e.g. "requirement failed:
// branch \"main\" has changed: ...") never satisfies that check, so
// iceberg-go's internal retry never engages for it; hence the retry here.
// RewriteDataFiles errors are treated the same as Commit errors (both
// checked via isCommitConflict) since a concurrently-modified source file
// can surface the same conflict shape from either call.
//
// Always returns the freshest table handle available -- even on final
// failure -- so the caller carries forward whatever this call last reloaded
// rather than starting the next group from an already-known-stale table.
func compactGroup(ctx context.Context, c *Catalog, ident icetable.Identifier, tbl *icetable.Table, group icetable.CompactionTaskGroup, opts MaintainOptions) (*icetable.Table, error) {
	retries := opts.CommitRetries
	if retries < 1 {
		retries = 1
	}
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		tx := tbl.NewTransaction()
		var err error
		if _, err = tx.RewriteDataFiles(ctx, []icetable.CompactionTaskGroup{group}, icetable.RewriteDataFilesOptions{
			GroupOptions: []icetable.CompactionGroupOption{
				icetable.WithCompactionScanConcurrency(opts.ScanConcurrency),
			},
		}); err == nil {
			var updated *icetable.Table
			if updated, err = tx.Commit(ctx); err == nil {
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
		log.Printf("[Maintain] compact %s: lost commit race with concurrent append, retrying (%d/%d)",
			ident[len(ident)-1], attempt+1, retries)
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
	_, err = tx.Commit(ctx)
	return err
}

package archive

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

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
}

// DefaultMaintainOptions returns sensible defaults: keep ~7 days of snapshots,
// never fewer than 10, compact at most ~2GiB per pass, and retry a lost
// commit race up to 3 times.
func DefaultMaintainOptions() MaintainOptions {
	return MaintainOptions{
		ExpireOlderThan: 7 * 24 * time.Hour,
		RetainLast:      10,
		ByteBudget:      2 << 30,
		CommitRetries:   3,
	}
}

// maintainScanConcurrency caps compaction's concurrent file-decode workers.
// iceberg-go defaults this to runtime.GOMAXPROCS(0), which reads the node's
// total CPU count rather than this container's cgroup limit -- on a
// many-core node that lets compaction fan out far beyond what the pod is
// actually allowed, which is what caused repeated OOMKills on an abnormal
// backlog. Matches the maintain CronJob's own CPU limit; bump alongside it.
const maintainScanConcurrency = 4

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
	// MaintainOutcomeRunning is a transient marker written when a pass starts, so
	// the admin UI can show a live in-progress state. It is never appended to the
	// history table (that records only terminal outcomes).
	MaintainOutcomeRunning MaintainOutcome = "running"
)

// MaintainStats summarizes one Maintain() pass, for logging and for
// persisting to the archive_maintain_status row the admin UI's System ->
// Archive panel reads (see WriteMaintainStatus). CandidateBytes is the total
// backlog compaction.Analyze found across every table this pass reached --
// it under-reports total system-wide backlog both when the pass-wide budget
// runs out before every table gets a turn (Analyze is only run while there's
// still budget to spend) and when a table's LoadTable or Analyze call itself
// errors (that table's backlog silently isn't measured this pass either);
// treat it as "backlog found among tables this pass could inspect," not a
// precise system-wide total.
type MaintainStats struct {
	Tables         int
	Compacted      int
	GroupsFailed   int
	Expired        int
	CandidateBytes int64
	CompactedBytes int64
	Duration       time.Duration
}

// Maintain runs compaction + snapshot expiry across every fractal's Iceberg
// table. It is intended to run as a SINGLETON (k8s CronJob with concurrencyPolicy
// Forbid, or leader-elected via a Postgres advisory lock) so concurrent passes
// never fight over table metadata. Errors on individual tables are logged and
// skipped so one bad table does not abort the whole pass.
//
// Orphan-file cleanup is not yet included (iceberg-go v0.6.0 exposes no stable
// standalone entrypoint); roll-on-size + snapshot expiry keep growth bounded.
func (c *Catalog) Maintain(ctx context.Context, opts MaintainOptions) (MaintainStats, error) {
	start := time.Now()
	ns := catalog.ToIdentifier(Namespace)

	var idents []icetable.Identifier
	for ident, err := range c.cat.ListTables(ctx, ns) {
		if err != nil {
			log.Printf("[Maintain] list tables: %v", err)
			continue
		}
		idents = append(idents, ident)
	}

	stats := MaintainStats{Tables: len(idents)}
	budget := opts.ByteBudget
	for i, ident := range idents {
		name := ident[len(ident)-1]

		// Split the remaining budget evenly across the tables not yet
		// visited this pass, so one early, backlog-heavy table can't
		// permanently starve tables later in iteration order. A table's
		// unused share simply isn't debited (budget is only reduced by bytes
		// actually compacted), so it grows later tables' shares in turn.
		if budget > 0 {
			share := budget / int64(len(idents)-i)
			tbl, err := c.cat.LoadTable(ctx, ident)
			if err != nil {
				log.Printf("[Maintain] load %s: %v", name, err)
			} else {
				res, err := compactTable(ctx, c, ident, tbl, share, opts.CommitRetries)
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

		if opts.ExpireOlderThan > 0 {
			if err := expireSnapshots(ctx, c, ident, opts); err != nil {
				log.Printf("[Maintain] expire %s: %v", name, err)
			} else {
				stats.Expired++
			}
		}
	}
	stats.Duration = time.Since(start)
	log.Printf("[Maintain] done: %d tables, %d compacted, %d groups failed, %d expired, %d/%d backlog bytes compacted, took %s",
		stats.Tables, stats.Compacted, stats.GroupsFailed, stats.Expired, stats.CompactedBytes, stats.CandidateBytes, stats.Duration)
	return stats, nil
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
// commit racing a busy writer can lose every attempt indefinitely. Groups are
// selected up to budget bytes; the first selected group is always included
// even if it alone exceeds budget (a single oversized group -- e.g. one
// flagged purely by delete-file count, independent of size -- must not stall
// forever), which is logged so that safety valve is visible rather than
// silent. No-op when the plan is empty (files already large enough - the
// common case given roll-on-size at write time).
func compactTable(ctx context.Context, c *Catalog, ident icetable.Identifier, tbl *icetable.Table, budget int64, retries int) (compactResult, error) {
	plan, err := compaction.Analyze(ctx, tbl, compaction.DefaultConfig())
	if err != nil {
		return compactResult{}, err
	}
	if len(plan.Groups) == 0 {
		return compactResult{}, nil
	}

	var candidateBytes int64
	for _, g := range plan.Groups {
		candidateBytes += g.TotalSizeBytes
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
		updated, err := compactGroup(ctx, c, ident, tbl, group, retries)
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
func compactGroup(ctx context.Context, c *Catalog, ident icetable.Identifier, tbl *icetable.Table, group icetable.CompactionTaskGroup, retries int) (*icetable.Table, error) {
	if retries < 1 {
		retries = 1
	}
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		tx := tbl.NewTransaction()
		var err error
		if _, err = tx.RewriteDataFiles(ctx, []icetable.CompactionTaskGroup{group}, icetable.RewriteDataFilesOptions{
			GroupOptions: []icetable.CompactionGroupOption{
				icetable.WithCompactionScanConcurrency(maintainScanConcurrency),
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

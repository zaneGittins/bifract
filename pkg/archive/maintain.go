package archive

import (
	"context"
	"log"
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
}

// DefaultMaintainOptions returns sensible defaults: keep ~7 days of snapshots,
// never fewer than 10.
func DefaultMaintainOptions() MaintainOptions {
	return MaintainOptions{ExpireOlderThan: 7 * 24 * time.Hour, RetainLast: 10}
}

// maintainScanConcurrency caps compaction's concurrent file-decode workers.
// iceberg-go defaults this to runtime.GOMAXPROCS(0), which reads the node's
// total CPU count rather than this container's cgroup limit -- on a
// many-core node that lets compaction fan out far beyond what the pod is
// actually allowed, which is what caused repeated OOMKills on an abnormal
// backlog. Matches the maintain CronJob's own CPU limit; bump alongside it.
const maintainScanConcurrency = 4

// Maintain runs compaction + snapshot expiry across every fractal's Iceberg
// table. It is intended to run as a SINGLETON (k8s CronJob with concurrencyPolicy
// Forbid, or leader-elected via a Postgres advisory lock) so concurrent passes
// never fight over table metadata. Errors on individual tables are logged and
// skipped so one bad table does not abort the whole pass.
//
// Orphan-file cleanup is not yet included (iceberg-go v0.6.0 exposes no stable
// standalone entrypoint); roll-on-size + snapshot expiry keep growth bounded.
func (c *Catalog) Maintain(ctx context.Context, opts MaintainOptions) error {
	ns := catalog.ToIdentifier(Namespace)
	var tables, compacted, expired int
	for ident, err := range c.cat.ListTables(ctx, ns) {
		if err != nil {
			log.Printf("[Maintain] list tables: %v", err)
			continue
		}
		tables++
		name := ident[len(ident)-1]

		tbl, err := c.cat.LoadTable(ctx, ident)
		if err != nil {
			log.Printf("[Maintain] load %s: %v", name, err)
			continue
		}

		if did, err := compactTable(ctx, tbl); err != nil {
			log.Printf("[Maintain] compact %s: %v", name, err)
		} else if did {
			compacted++
		}

		if opts.ExpireOlderThan > 0 {
			if err := expireSnapshots(ctx, c, ident, opts); err != nil {
				log.Printf("[Maintain] expire %s: %v", name, err)
			} else {
				expired++
			}
		}
	}
	log.Printf("[Maintain] done: %d tables, %d compacted, %d expired", tables, compacted, expired)
	return nil
}

// compactTable merges small data files within each partition into larger ones.
// Returns true if any group was rewritten. No-op when the plan is empty (files
// already large enough - the common case given roll-on-size at write time).
func compactTable(ctx context.Context, tbl *icetable.Table) (bool, error) {
	plan, err := compaction.Analyze(ctx, tbl, compaction.DefaultConfig())
	if err != nil {
		return false, err
	}
	if len(plan.Groups) == 0 {
		return false, nil
	}
	groups := make([]icetable.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = icetable.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
	}
	tx := tbl.NewTransaction()
	if _, err := tx.RewriteDataFiles(ctx, groups, icetable.RewriteDataFilesOptions{
		GroupOptions: []icetable.CompactionGroupOption{
			icetable.WithCompactionScanConcurrency(maintainScanConcurrency),
		},
	}); err != nil {
		return false, err
	}
	if _, err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
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

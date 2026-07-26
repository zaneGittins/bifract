package archive

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
)

// RetentionPolicy maps a fractal ID to its archive retention in days. Absent
// (or <= 0) means keep forever, which is the default for every fractal.
type RetentionPolicy map[string]int

// ByTable re-keys the policy by archive table name, which is how the maintain
// pass identifies tables (it iterates the catalog, not the fractal list).
func (p RetentionPolicy) ByTable() map[string]int {
	out := make(map[string]int, len(p))
	for fractalID, days := range p {
		out[tableName(fractalID)] = days
	}
	return out
}

// Expired reports whether day falls outside the fractal's archive retention, so
// callers can tell "the archive dropped this on purpose" from "data is missing".
func (p RetentionPolicy) Expired(fractalID string, day time.Time) bool {
	days, ok := p[fractalID]
	if !ok || days <= 0 {
		return false
	}
	cutoff := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -days)
	return day.Before(cutoff)
}

// LoadRetentionPolicy reads per-fractal archive retention. Only fractals with a
// policy are returned; everything else keeps its archive forever.
func LoadRetentionPolicy(ctx context.Context, db *sql.DB) (RetentionPolicy, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id::text, archive_retention_days FROM fractals WHERE archive_retention_days IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	policy := RetentionPolicy{}
	for rows.Next() {
		var id string
		var days int
		if err := rows.Scan(&id, &days); err != nil {
			return nil, err
		}
		if days > 0 {
			policy[id] = days
		}
	}
	return policy, rows.Err()
}

// retentionResult reports what one table's retention pass removed.
type retentionResult struct {
	// Cutoff is the first ingest_date kept.
	Cutoff time.Time
	// Deleted is true when a delete snapshot was actually committed.
	Deleted bool
	// Files is how many data files the delete dropped.
	Files int
}

// applyRetention drops every ingest_date partition older than days from a
// fractal's archive table.
//
// The filter is on the partition column itself, so classification matches whole
// data files and the commit is metadata-only: no Parquet is rewritten and no
// delete files are produced, which is what makes this affordable at TB scale.
// The bytes are not freed here. The delete leaves the files referenced by older
// snapshots; ExpireSnapshots reclaims them once those snapshots age out, so the
// effective storage lag is roughly the retention window plus ExpireOlderThan.
func applyRetention(ctx context.Context, c *Catalog, ident icetable.Identifier, days int, concurrency int) (retentionResult, error) {
	var res retentionResult
	if days <= 0 {
		return res, nil
	}
	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return res, err
	}
	if tbl.CurrentSnapshot() == nil {
		return res, nil // created, never written to
	}

	// Whole UTC days: ingest_date is a Date32, so the cutoff is a day boundary
	// and "older than N days" keeps exactly the last N dates.
	cutoff := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -days)
	res.Cutoff = cutoff

	filter := iceberg.LessThan(
		iceberg.Reference(partitionFieldName),
		iceberg.Date(cutoff.Unix()/int64(24*time.Hour/time.Second)),
	)

	// Delete commits a snapshot unconditionally, even when nothing matches, so a
	// table already inside its window would gain an empty snapshot on every pass
	// (hourly, per fractal) -- exactly the metadata growth retention exists to
	// bound. Plan first and skip when there is nothing to drop; planning prunes on
	// the same expression, so it agrees with what the delete would classify.
	tasks, err := tbl.Scan(icetable.WithRowFilter(filter)).PlanFiles(ctx)
	if err != nil {
		return res, fmt.Errorf("plan expired partitions: %w", err)
	}
	if len(tasks) == 0 {
		return res, nil
	}

	tx := tbl.NewTransaction()
	if err := tx.Delete(ctx, filter, nil, icetable.WithDeleteConcurrency(concurrency)); err != nil {
		return res, fmt.Errorf("delete partitions older than %s: %w", cutoff.Format("2006-01-02"), err)
	}
	updated, err := tx.Commit(ctx)
	if err != nil {
		return res, err
	}
	writeVersionHint(ctx, updated)
	res.Deleted, res.Files = true, len(tasks)
	return res, nil
}

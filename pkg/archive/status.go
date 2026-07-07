package archive

import (
	"context"
	"database/sql"

	"github.com/apache/iceberg-go/catalog"
)

// Stats aggregates the current archive footprint across all fractal tables by
// reading each table's current-snapshot summary (total-files-size / total-records).
// Cheap: reads catalog metadata, not object listings.
func (c *Catalog) Stats(ctx context.Context) (fractalCount int, totalBytes int64, totalRecords int64, err error) {
	ns := catalog.ToIdentifier(Namespace)
	// The namespace is created lazily on the first archive commit. Before any
	// data is archived it does not exist yet; report an empty footprint rather
	// than an error so the heartbeat still records liveness (an error here would
	// abort maybeHeartbeat before writeHeartbeat, making the admin UI show the
	// archiver as "not responding" until the first commit).
	if ok, e := c.cat.CheckNamespaceExists(ctx, ns); e != nil {
		return 0, 0, 0, e
	} else if !ok {
		return 0, 0, 0, nil
	}
	for ident, e := range c.cat.ListTables(ctx, ns) {
		if e != nil {
			return fractalCount, totalBytes, totalRecords, e
		}
		fractalCount++
		tbl, e := c.cat.LoadTable(ctx, ident)
		if e != nil {
			continue // count the table but skip unreadable stats
		}
		snap := tbl.CurrentSnapshot()
		if snap == nil || snap.Summary == nil || snap.Summary.Properties == nil {
			continue
		}
		totalBytes += int64(snap.Summary.Properties.GetInt("total-files-size", 0))
		totalRecords += int64(snap.Summary.Properties.GetInt("total-records", 0))
	}
	return fractalCount, totalBytes, totalRecords, nil
}

// writeHeartbeat updates the archive_status row with the latest footprint and a
// fresh updated_at (liveness). Best-effort; the archive works without it.
func writeHeartbeat(ctx context.Context, db *sql.DB, fractalCount int, totalBytes, totalRecords int64) error {
	if db == nil {
		return nil
	}
	_, err := db.ExecContext(ctx,
		`UPDATE archive_status SET updated_at = NOW(), fractal_count = $1, total_bytes = $2, total_records = $3 WHERE id = 1`,
		fractalCount, totalBytes, totalRecords)
	return err
}

// markCommit stamps the last successful Iceberg commit time (and liveness).
func markCommit(ctx context.Context, db *sql.DB) {
	if db == nil {
		return
	}
	_, _ = db.ExecContext(ctx, `UPDATE archive_status SET updated_at = NOW(), last_commit_at = NOW() WHERE id = 1`)
}

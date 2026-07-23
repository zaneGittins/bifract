package archive

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// defaultCompletenessDays is how many sealed ingest days each pass re-checks per
// fractal. Small on purpose: the check costs two ClickHouse counts per
// (fractal, day), so the cost is fractals x days x 2 per pass. Recent days are
// what matter -- a spool loss shows up immediately, and once a sealed day has
// been confirmed archived it cannot regress.
const defaultCompletenessDays = 3

// CompletenessOptions tunes the completeness sweep.
type CompletenessOptions struct {
	// Days is how many sealed ingest days back to check. Today is always
	// excluded: it is still being written on both sides, so any count difference
	// is just archiver lag rather than a gap.
	Days int
}

// CompletenessOptionsFromEnv reads the sweep configuration.
// BIFRACT_ARCHIVE_COMPLETENESS_DAYS=0 disables the sweep entirely.
func CompletenessOptionsFromEnv() CompletenessOptions {
	return CompletenessOptions{Days: getIntEnv("BIFRACT_ARCHIVE_COMPLETENESS_DAYS", defaultCompletenessDays)}
}

// CompletenessRow is one (fractal, ingest day) comparison.
type CompletenessRow struct {
	FractalID string
	Day       time.Time
	CHCount   int64
	IceCount  int64
}

// Gap reports whether the hot store holds rows the archive does not. The reverse
// (ice > ch) is healthy: it just means retention has since dropped those rows
// from ClickHouse, which is the archive doing its job.
func (r CompletenessRow) Gap() int64 {
	if r.CHCount > r.IceCount {
		return r.CHCount - r.IceCount
	}
	return 0
}

// CheckCompleteness compares hot-store and archive row counts for the last
// opts.Days sealed ingest days of every fractal, and records the result.
//
// This is the only thing in the system that can notice an archive hole. The
// spool that feeds the archive is pod-local and ephemeral, so an ingest pod
// replaced while it still holds un-archived batches drops them: the data reached
// ClickHouse but never reached Iceberg. Reconcile cannot repair that, because it
// only ever restores ClickHouse FROM Iceberg. Detection is therefore the whole
// point -- an operator who sees a gap can widen hot-store retention before the
// only remaining copy ages out.
//
// Errors on a single fractal are logged and skipped so one unreadable table does
// not abort the sweep.
func CheckCompleteness(ctx context.Context, db *sql.DB, cat *Catalog, ch *storage.ClickHouseClient, obj objstore.Config, opts CompletenessOptions) ([]CompletenessRow, error) {
	if db == nil || opts.Days <= 0 {
		return nil, nil
	}
	fractalIDs, err := archivedFractals(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("archive: list fractals for completeness: %w", err)
	}

	// Yesterday backwards: today is still being written on both sides.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	var out []CompletenessRow
	var gaps int

	for _, fractalID := range fractalIDs {
		for i := 1; i <= opts.Days; i++ {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			day := today.AddDate(0, 0, -i)
			next := day.AddDate(0, 0, 1)

			chCount, err := countLogs(ctx, ch, fractalID, day, next)
			if err != nil {
				log.Printf("[Completeness] %s %s: hot count failed: %v", fractalID, chDate(day), err)
				continue
			}
			iceCount, err := cat.countIceberg(ctx, ch, obj, fractalID, day, next)
			if err != nil {
				log.Printf("[Completeness] %s %s: archive count failed: %v", fractalID, chDate(day), err)
				continue
			}

			row := CompletenessRow{FractalID: fractalID, Day: day, CHCount: chCount, IceCount: iceCount}
			if err := writeCompleteness(ctx, db, row); err != nil {
				log.Printf("[Completeness] %s %s: persist failed: %v", fractalID, chDate(day), err)
				continue
			}
			out = append(out, row)
			if g := row.Gap(); g > 0 {
				gaps++
				log.Printf("[Completeness] GAP: fractal %s ingest day %s has %d row(s) in ClickHouse missing from the archive",
					fractalID, chDate(day), g)
			}
		}
	}
	log.Printf("[Completeness] checked %d fractal-day(s) across %d fractal(s), %d gap(s)", len(out), len(fractalIDs), gaps)
	return out, nil
}

// archivedFractals lists live fractals that have an Iceberg table, joining
// forward through the same id -> table-name mapping the catalog uses (see
// tableName) rather than trying to invert it. Fractals with no archive table
// have no baseline to compare against; deleted fractals have had their hot-store
// partitions dropped, so there is nothing left to be missing.
func archivedFractals(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT f.id::text
		FROM fractals f
		JOIN iceberg_tables t
		  ON t.table_namespace = $1
		 AND t.table_name = 'f_' || replace(f.id::text, '-', '_')
		ORDER BY f.id`, Namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func writeCompleteness(ctx context.Context, db *sql.DB, r CompletenessRow) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO archive_completeness (fractal_id, ingest_day, ch_count, ice_count, checked_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (fractal_id, ingest_day) DO UPDATE
		SET ch_count = EXCLUDED.ch_count, ice_count = EXCLUDED.ice_count, checked_at = NOW()`,
		r.FractalID, r.Day, r.CHCount, r.IceCount)
	return err
}

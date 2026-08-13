package schemafields

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"bifract/pkg/storage"
)

// Persistence for the sweep's results. Everything the schema tab renders is read
// from these tables, so a page load costs one Postgres round trip and can never
// depend on ClickHouse being responsive.

// fractalResult is one fractal's complete measurement, written atomically so the
// tab never sees a half-updated fractal.
type fractalResult struct {
	Meta        *fractalMeta
	SampledRows uint64
	MaxPaths    int
	Fields      map[string]*fieldSample
	Bytes       map[string]*fieldMeta
}

// saveStats replaces the whole measured set in one transaction. Replacing rather
// than merging is what makes a removed fractal or a field that stopped appearing
// actually disappear from the tab instead of lingering forever.
func saveStats(ctx context.Context, pg *storage.PostgresClient, results []fractalResult) error {
	tx, err := pg.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM schema_field_stats`); err != nil {
		return fmt.Errorf("clear field stats: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM schema_fractal_stats`); err != nil {
		return fmt.Errorf("clear fractal stats: %w", err)
	}

	for _, r := range results {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_fractal_stats (fractal_id, sampled_rows, max_paths, total_bytes, sampled_at)
			 VALUES ($1, $2, $3, $4, NOW())`,
			r.Meta.ID, int64(r.SampledRows), r.MaxPaths, int64(r.Meta.TotalBytes)); err != nil {
			return fmt.Errorf("insert fractal stats %q: %w", r.Meta.ID, err)
		}

		// Union of what the sample saw and what the parts hold: a reserved field
		// with no recent values still costs storage, and it needs a row or the
		// "unused, reclaim it" case could never surface.
		names := make(map[string]struct{}, len(r.Fields)+len(r.Bytes))
		for n := range r.Fields {
			names[n] = struct{}{}
		}
		for n := range r.Bytes {
			names[n] = struct{}{}
		}

		for name := range names {
			var present, cardinality uint64
			top := []TopValue{}
			if fs := r.Fields[name]; fs != nil {
				present, cardinality = fs.Present, fs.Cardinality
				if fs.Top != nil {
					top = fs.Top
				}
			}
			var bytes uint64
			if fm := r.Bytes[name]; fm != nil {
				bytes = fm.Bytes
			}
			topJSON, err := json.Marshal(top)
			if err != nil {
				return fmt.Errorf("encode top values for %q: %w", name, err)
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO schema_field_stats
				   (fractal_id, field_name, present, cardinality, top_values, bytes_on_disk)
				 VALUES ($1, $2, $3, $4, $5, $6)`,
				r.Meta.ID, name, int64(present), int64(cardinality), string(topJSON), int64(bytes)); err != nil {
				return fmt.Errorf("insert field stat %q: %w", name, err)
			}
		}
	}
	return tx.Commit()
}

// clearStats discards every measurement, for the one case where they describe
// data that no longer exists: a schema reset truncates the logs table.
func clearStats(ctx context.Context, pg *storage.PostgresClient) error {
	if _, err := pg.Exec(ctx, `DELETE FROM schema_field_stats`); err != nil {
		return fmt.Errorf("clear field stats: %w", err)
	}
	if _, err := pg.Exec(ctx, `DELETE FROM schema_fractal_stats`); err != nil {
		return fmt.Errorf("clear fractal stats: %w", err)
	}
	return nil
}

// aggregateStats is the cross-fractal view the schema tab renders. The schema is
// table-wide, so the tab is too; the per-fractal rows exist because sampling has
// to be stratified to be accurate, not because the configuration is per fractal.
type aggregateStats struct {
	Fields      map[string]*Field
	SampledRows uint64
	MaxPaths    int
	TotalBytes  uint64
	Fractals    int
	ComputedAt  time.Time
}

// readSweptAt reports when the sweep last completed, which is what the tab shows
// as its freshness. A zero time means it has never finished a pass.
func readSweptAt(ctx context.Context, pg *storage.PostgresClient) time.Time {
	v, err := pg.GetSetting(ctx, sweptAtKey)
	if err != nil || v == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}
	}
	return t
}

// loadStats aggregates the persisted per-fractal rows.
//
// present sums, because coverage is (rows with the field) / (rows sampled) across
// every fractal. cardinality takes the per-fractal maximum: distinct counts from
// separate samples cannot be added without double counting shared values, so the
// largest single observation is reported, which is a true lower bound. Only the
// set-versus-bloom threshold reads it.
func loadStats(ctx context.Context, pg *storage.PostgresClient) (*aggregateStats, error) {
	out := &aggregateStats{Fields: map[string]*Field{}}

	rows, err := pg.Query(ctx, `
		SELECT field_name,
		       SUM(present)::bigint       AS present,
		       MAX(cardinality)::bigint   AS cardinality,
		       SUM(bytes_on_disk)::bigint AS bytes_on_disk,
		       (ARRAY_AGG(top_values ORDER BY present DESC))[1] AS top_values
		FROM schema_field_stats
		GROUP BY field_name`)
	if err != nil {
		return nil, fmt.Errorf("load field stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name    string
			present int64
			card    int64
			bytes   int64
			topJSON sql.NullString
		)
		if err := rows.Scan(&name, &present, &card, &bytes, &topJSON); err != nil {
			return nil, fmt.Errorf("scan field stat: %w", err)
		}
		f := &Field{
			FieldInsight: FieldInsight{
				Name:        name,
				Present:     uint64(present),
				Cardinality: uint64(card),
			},
			BytesOnDisk: uint64(bytes),
			Top:         []TopValue{},
		}
		if topJSON.Valid && topJSON.String != "" {
			if err := json.Unmarshal([]byte(topJSON.String), &f.Top); err != nil {
				f.Top = []TopValue{}
			}
		}
		out.Fields[name] = f
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// max_paths is a per-part budget, so the worst fractal is the capacity
	// figure; summing would describe a limit that does not exist.
	if err := pg.QueryRow(ctx, `
		SELECT COALESCE(SUM(sampled_rows), 0)::bigint,
		       COALESCE(MAX(max_paths), 0),
		       COALESCE(SUM(total_bytes), 0)::bigint,
		       COUNT(*)
		FROM schema_fractal_stats`).
		Scan(&out.SampledRows, &out.MaxPaths, &out.TotalBytes, &out.Fractals); err != nil {
		return nil, fmt.Errorf("load fractal stats: %w", err)
	}
	out.ComputedAt = readSweptAt(ctx, pg)
	return out, nil
}

// saveUsage replaces the persisted usage ranking.
func saveUsage(ctx context.Context, pg *storage.PostgresClient, usage map[string]*fieldUsage) error {
	tx, err := pg.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM schema_field_usage`); err != nil {
		return fmt.Errorf("clear usage: %w", err)
	}
	for name, u := range usage {
		refs := u.Refs
		if refs == nil {
			refs = []FieldRef{}
		}
		refsJSON, err := json.Marshal(refs)
		if err != nil {
			return fmt.Errorf("encode refs for %q: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_field_usage (field_name, weight, refs, computed_at)
			 VALUES ($1, $2, $3, NOW())`,
			name, u.Weight, string(refsJSON)); err != nil {
			return fmt.Errorf("insert usage %q: %w", name, err)
		}
	}
	return tx.Commit()
}

// loadUsage reads the persisted usage ranking.
func loadUsage(ctx context.Context, pg *storage.PostgresClient) (map[string]*fieldUsage, error) {
	rows, err := pg.Query(ctx, `SELECT field_name, weight, refs FROM schema_field_usage`)
	if err != nil {
		return nil, fmt.Errorf("load usage: %w", err)
	}
	defer rows.Close()

	out := map[string]*fieldUsage{}
	for rows.Next() {
		var (
			name    string
			weight  int
			refsRaw sql.NullString
		)
		if err := rows.Scan(&name, &weight, &refsRaw); err != nil {
			return nil, fmt.Errorf("scan usage: %w", err)
		}
		u := &fieldUsage{Weight: weight}
		if refsRaw.Valid && refsRaw.String != "" {
			if err := json.Unmarshal([]byte(refsRaw.String), &u.Refs); err != nil {
				u.Refs = nil
			}
		}
		out[name] = u
	}
	return out, rows.Err()
}

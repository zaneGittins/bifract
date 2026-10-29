package schemafields

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

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
	Paths       *pathCounts // exact presence; nil when the rollup has none
	Bytes       map[string]*fieldMeta
	SampledAt   time.Time
}

// maxFieldNameLen is schema_field_stats.field_name's VARCHAR length.
const maxFieldNameLen = 255

// saveableName reports whether Postgres can store a field name: valid UTF-8, no NUL,
// within the column's length.
func saveableName(name string) bool {
	return utf8.ValidString(name) && !strings.ContainsRune(name, 0) && utf8.RuneCountInString(name) <= maxFieldNameLen
}

// saveableTop drops values jsonb cannot hold: a NUL marshals to \u0000, which
// Postgres rejects. Invalid UTF-8 is replaced by json.Marshal, so it is safe.
func saveableTop(top []TopValue) []TopValue {
	out := make([]TopValue, 0, len(top))
	for _, v := range top {
		if !strings.ContainsRune(v.Value, 0) {
			out = append(out, v)
		}
	}
	return out
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
			`INSERT INTO schema_fractal_stats (fractal_id, sampled_rows, max_paths, total_bytes, sampled_at, total_rows)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			r.Meta.ID, int64(r.SampledRows), r.MaxPaths, int64(r.Meta.TotalBytes), r.SampledAt, int64(r.totalRows())); err != nil {
			return fmt.Errorf("insert fractal stats %q: %w", r.Meta.ID, err)
		}

		// Union of what was counted, what the sample saw and what the parts hold:
		// a reserved field with no values still costs storage, and it needs a row
		// or the "unused, reclaim it" case could never surface.
		names := make(map[string]struct{}, len(r.Fields)+len(r.Bytes))
		for n := range r.Fields {
			names[n] = struct{}{}
		}
		if r.Paths != nil {
			for n := range r.Paths.Present {
				names[n] = struct{}{}
			}
		}
		for n := range r.Bytes {
			names[n] = struct{}{}
		}

		for name := range names {
			if !saveableName(name) {
				continue // sender controlled; one would fail the whole transaction
			}
			var sampled, cardinality uint64
			top := []TopValue{}
			if fs := r.Fields[name]; fs != nil {
				sampled, cardinality = fs.Present, fs.Cardinality
				if fs.Top != nil {
					top = saveableTop(fs.Top)
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
				   (fractal_id, field_name, present, cardinality, top_values, bytes_on_disk, sample_present)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				r.Meta.ID, name, int64(r.present(name)), int64(cardinality), string(topJSON), int64(bytes), int64(sampled)); err != nil {
				return fmt.Errorf("insert field stat %q: %w", name, err)
			}
		}
	}
	return tx.Commit()
}

// loadPreviousSample reads a fractal's last persisted distribution, so a pass
// whose sample fails can carry it forward instead of writing it as unmeasured.
// A fractal never measured before returns a zero sample. Presence is the sample's
// own count: exact counts are read fresh each pass.
func loadPreviousSample(ctx context.Context, pg *storage.PostgresClient, fractalID string) (fractalResult, error) {
	out := fractalResult{Fields: map[string]*fieldSample{}}
	var sampled int64
	err := pg.QueryRow(ctx, `SELECT sampled_rows, max_paths, sampled_at
		FROM schema_fractal_stats WHERE fractal_id = $1`, fractalID).
		Scan(&sampled, &out.MaxPaths, &out.SampledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return fractalResult{}, nil
	}
	if err != nil {
		return fractalResult{}, fmt.Errorf("load previous fractal stats: %w", err)
	}
	out.SampledRows = uint64(sampled)

	rows, err := pg.Query(ctx, `SELECT field_name, sample_present, cardinality, top_values
		FROM schema_field_stats WHERE fractal_id = $1 AND sample_present > 0`, fractalID)
	if err != nil {
		return fractalResult{}, fmt.Errorf("load previous field stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name          string
			present, card int64
			topJSON       sql.NullString
		)
		if err := rows.Scan(&name, &present, &card, &topJSON); err != nil {
			return fractalResult{}, fmt.Errorf("scan previous field stat: %w", err)
		}
		fs := &fieldSample{Present: uint64(present), Cardinality: uint64(card)}
		if topJSON.Valid && topJSON.String != "" {
			_ = json.Unmarshal([]byte(topJSON.String), &fs.Top)
		}
		out.Fields[name] = fs
	}
	return out, rows.Err()
}

// aggregateStats is the cross-fractal view the schema tab renders. The schema is
// table-wide, so the tab is too; the per-fractal rows exist because sampling has
// to be stratified to be accurate, not because the configuration is per fractal.
type aggregateStats struct {
	Fields      map[string]*Field
	SampledRows uint64
	TotalRows   uint64
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
// present sums, because coverage is (rows with the field) / (total rows) across
// every fractal. cardinality takes the per-fractal maximum: distinct counts from
// separate samples cannot be added without double counting shared values, so the
// largest single observation is reported, which is a true lower bound. Only the
// set-versus-bloom threshold reads it. Top values come from the fractal whose sample
// saw the field most, with that sample's count as the denominator for their shares.
func loadStats(ctx context.Context, pg *storage.PostgresClient) (*aggregateStats, error) {
	out := &aggregateStats{Fields: map[string]*Field{}}

	rows, err := pg.Query(ctx, `
		SELECT field_name,
		       SUM(present)::bigint       AS present,
		       MAX(cardinality)::bigint   AS cardinality,
		       SUM(bytes_on_disk)::bigint AS bytes_on_disk,
		       (ARRAY_AGG(top_values ORDER BY sample_present DESC, fractal_id))[1] AS top_values,
		       (ARRAY_AGG(sample_present ORDER BY sample_present DESC, fractal_id))[1] AS sample_present
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
			sampled int64
		)
		if err := rows.Scan(&name, &present, &card, &bytes, &topJSON, &sampled); err != nil {
			return nil, fmt.Errorf("scan field stat: %w", err)
		}
		f := &Field{
			FieldInsight: FieldInsight{
				Name:          name,
				Present:       uint64(present),
				SamplePresent: uint64(sampled),
				Cardinality:   uint64(card),
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
		       COUNT(*),
		       COALESCE(SUM(total_rows), 0)::bigint
		FROM schema_fractal_stats`).
		Scan(&out.SampledRows, &out.MaxPaths, &out.TotalBytes, &out.Fractals, &out.TotalRows); err != nil {
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

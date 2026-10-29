package schemafields

import (
	"context"
	"fmt"
	"log"

	"bifract/pkg/storage"
)

// Exact presence over all retention, from the logs_field_paths rollup. Values and
// cardinality still come from the sample.

// pathCounts is one fractal's rollup: rows in total and rows carrying each field.
type pathCounts struct {
	Rows    uint64
	Present map[string]uint64
}

// readPathCounts reads each fractal's counts in its own query, so a fractal whose
// read fails (a flood of random keys on a day not yet trimmed) falls back to its
// sample alone. Fractals come from the rollup's part metadata, which also finds
// those with nothing recent in logs. Rows with an empty fractal id are skipped, as
// the part inventory skips them.
func (s *Sweeper) readPathCounts(ctx context.Context) (map[string]*pathCounts, error) {
	ids, err := s.rollupFractals(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*pathCounts, len(ids))
	for _, id := range ids {
		pc, err := s.readFractalPaths(ctx, id)
		if err != nil {
			log.Printf("[SchemaSweep] field path counts for %s: %v", id, err)
			continue
		}
		if pc != nil {
			out[id] = pc
		}
	}
	return out, nil
}

func (s *Sweeper) rollupFractals(ctx context.Context) ([]string, error) {
	sql := fmt.Sprintf(`SELECT DISTINCT %s AS fractal
FROM %s
WHERE database = currentDatabase() AND table = 'logs_field_paths' AND active`,
		fractalFromPartition, partsSource(s.ch, "parts"))
	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return nil, fmt.Errorf("field path fractals: %w", err)
	}
	var out []string
	for _, r := range rows {
		if id, _ := r["fractal"].(string); id != "" {
			out = append(out, id)
		}
	}
	return out, nil
}

// readFractalPaths reads one fractal's row count and as many of its fields as the
// sample may list. A plain LIMIT is a bounded top-N sort, and the spill settings
// let a large GROUP BY finish under the memory budget.
func (s *Sweeper) readFractalPaths(ctx context.Context, id string) (*pathCounts, error) {
	sql := fmt.Sprintf(`SELECT path, sum(cnt) AS n
FROM %s WHERE fractal_id = %s
GROUP BY path
ORDER BY path = '' DESC, n DESC
LIMIT %d
SETTINGS %s`, s.ch.FieldPathsReadTable(), quoteCH(id), sampleMaxFields+1, storage.SpillSettingsClause(sweepMaxMemoryBytes()))
	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return nil, err
	}
	pc := &pathCounts{Present: map[string]uint64{}}
	for _, r := range rows {
		path, _ := r["path"].(string)
		if path == "" {
			pc.Rows = asUint64(r["n"])
		} else {
			pc.Present[path] = asUint64(r["n"])
		}
	}
	if pc.Rows == 0 {
		return nil, nil
	}
	return pc, nil
}

// totalRows is the coverage denominator: every row the rollup counted, or the
// sample's size when there are no counts.
func (r fractalResult) totalRows() uint64 {
	if r.Paths != nil {
		return r.Paths.Rows
	}
	return r.SampledRows
}

// present is how many rows carry a field. A field the sample saw but the rollup did
// not list (past its per-fractal limit) is estimated from the sample's share.
func (r fractalResult) present(name string) uint64 {
	var sampled uint64
	if fs := r.Fields[name]; fs != nil {
		sampled = fs.Present
	}
	if r.Paths == nil {
		return sampled
	}
	if n, ok := r.Paths.Present[name]; ok {
		return n
	}
	if r.SampledRows == 0 {
		return 0
	}
	return uint64(float64(sampled) / float64(r.SampledRows) * float64(r.Paths.Rows))
}

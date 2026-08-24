package schemafields

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// Tier A: what the schema costs, read from ClickHouse part metadata.
//
// system.parts_columns exposes, per part, the sub-columns a column allocated and
// their size on disk, at metadata cost: measured at 1,281 rows and 250 KiB for a
// table whose sampled equivalent read 209 MiB.
//
// It covers TYPE-HINTED paths only. A JSON column's dynamic paths are visible in
// `substreams` but carry no size there and are absent from `subcolumns.names`
// entirely, so a dynamic path's footprint cannot be attributed from metadata and
// stays 0 until the field is reserved. The dynamic-path COUNT, which is what
// max_dynamic_paths governs, comes from JSONDynamicPaths instead (see
// probeDynamicPaths).

// inventoryPartLimit bounds the metadata scan to the newest parts. Cost here is
// linear in parts, and a cluster can hold tens of thousands, each carrying an
// array of up to max_dynamic_paths names. The newest parts are also the only ones
// that describe the schema as it stands now.
const inventoryPartLimit = 4000

// fieldMeta is one field's on-disk footprint within one fractal.
type fieldMeta struct {
	Bytes uint64
}

// fractalMeta is what part metadata says about one fractal, including the two
// numbers the sampler needs to bound itself: where the data ends and how fast it
// arrives.
type fractalMeta struct {
	ID         string
	MaxTime    time.Time
	TotalBytes uint64
	RowsPerSec float64
}

// partsSource is the metadata table to read. ShardSystemTable is one replica per
// shard, not every replica: each replica of a shard holds the same parts, so
// counting them all would multiply every byte figure by the replication factor.
func partsSource(ch *storage.ClickHouseClient, table string) string {
	return ch.Topology().ShardSystemTable("system." + table)
}

// fractalFromPartition extracts the fractal_id from a `('<fractal>','<date>')`
// partition expression. PARTITION BY (fractal_id, toDate(ingest_timestamp)) makes
// every part attributable to exactly one fractal, so per-fractal accounting needs
// no data scan. Only the fractal half is read here, so the date axis is irrelevant.
const fractalFromPartition = `extract(partition, '^\\(''([^'']*)''')`

// normalizedPaths rewrites a part's sub-column list into real JSON paths.
//
// The array also carries serialization substreams: a String path `user` reports a
// `user.size` entry of type UInt64 for its offsets. Those are not fields, but
// their bytes are the parent's, so they are folded into the parent rather than
// dropped. The sibling check means a genuine nested path that happens to be named
// `x.size` is only merged when `x` is itself a path, which is when it is in fact
// the substream.
const normalizedPaths = "arrayMap(z -> (" +
	"if(z.2 = 'UInt64' AND endsWith(z.1, '.size') AND has(`subcolumns.names`, substring(z.1, 1, length(z.1) - 5)), " +
	"substring(z.1, 1, length(z.1) - 5), z.1), z.3), " +
	"arrayZip(`subcolumns.names`, `subcolumns.types`, `subcolumns.bytes_on_disk`))"

// readInventory returns the per-fractal per-field footprint and the per-fractal
// part summary, from metadata only.
func (s *Sweeper) readInventory(ctx context.Context) (map[string]map[string]*fieldMeta, map[string]*fractalMeta, error) {
	fractals, err := s.readFractalMeta(ctx)
	if err != nil {
		return nil, nil, err
	}

	src := partsSource(s.ch, "parts_columns")
	sql := fmt.Sprintf(`WITH recent AS (
    SELECT %s AS fractal, %s AS paths
    FROM %s
    WHERE database = currentDatabase() AND table = 'logs' AND active AND column = 'fields'
    ORDER BY modification_time DESC
    LIMIT %d
)
SELECT fractal, p.1 AS field, sum(p.2) AS bytes
FROM recent ARRAY JOIN paths AS p
WHERE field != ''
GROUP BY fractal, field`,
		fractalFromPartition, normalizedPaths, src, inventoryPartLimit)

	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return nil, nil, fmt.Errorf("field inventory: %w", err)
	}

	out := make(map[string]map[string]*fieldMeta, len(fractals))
	for _, r := range rows {
		fractal, _ := r["fractal"].(string)
		field, _ := r["field"].(string)
		if field == "" {
			continue
		}
		if out[fractal] == nil {
			out[fractal] = map[string]*fieldMeta{}
		}
		out[fractal][field] = &fieldMeta{Bytes: asUint64(r["bytes"])}
	}
	return out, fractals, nil
}

// readFractalMeta reads the per-fractal part summary: which fractals hold data,
// where that data ends, how fast it arrives, and what it occupies. The first
// three are what let the sampler bound itself to a recent, thin window.
func (s *Sweeper) readFractalMeta(ctx context.Context) (map[string]*fractalMeta, error) {
	src := partsSource(s.ch, "parts_columns")
	// recent_rows is the trailing day's ingest, which only sizes the sample window.
	sql := fmt.Sprintf(`SELECT
    fractal,
    max(max_time) AS newest,
    sum(part_bytes) AS total_bytes,
    sumIf(rows, max_time >= now() - INTERVAL 1 DAY) AS recent_rows
FROM (
    SELECT %s AS fractal,
           max_time,
           rows,
           bytes_on_disk AS part_bytes
    FROM %s
    WHERE database = currentDatabase() AND table = 'logs' AND active AND column = 'fields'
    ORDER BY modification_time DESC
    LIMIT %d
)
WHERE fractal != ''
GROUP BY fractal`,
		fractalFromPartition, src, inventoryPartLimit)

	rows, err := s.ch.QueryLowPriorityBounded(ctx, sql, sweepMaxMemoryBytes())
	if err != nil {
		return nil, fmt.Errorf("fractal part summary: %w", err)
	}

	out := make(map[string]*fractalMeta, len(rows))
	for _, r := range rows {
		id, _ := r["fractal"].(string)
		if id == "" {
			continue
		}
		m := &fractalMeta{ID: id, TotalBytes: asUint64(r["total_bytes"])}
		if t, ok := r["newest"].(time.Time); ok {
			m.MaxTime = t
		}
		// Ingest rate over the trailing day, used only to size the sample window.
		// A fractal that stopped ingesting yields 0 and falls back to the widest
		// window, which is the right answer for dormant data.
		m.RowsPerSec = float64(asUint64(r["recent_rows"])) / (24 * 60 * 60)
		out[id] = m
	}
	return out, nil
}

// quoteCH escapes a value for embedding in generated SQL. Fractal ids come from
// ClickHouse metadata rather than user input, but they reach a query as literals
// and are quoted like any other.
func quoteCH(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`) + "'"
}

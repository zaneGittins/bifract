package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"bifract/pkg/sqlquote"
)

// procAncestryDepth is the deepest ancestor proc_ancestry records, the filters' maximum depth.
const procAncestryDepth = 8

// ProcAncestryBuilder keeps proc_ancestry in step with proc_lineage. Each slice of ingest time
// is reflected in full before the watermark passes it: the new processes' ancestors, then any
// earlier process whose chain stopped at one of them, then each one's status row. Every step is
// an INSERT ... SELECT inside ClickHouse, and rows replace each other on (process, depth), so a
// slice cut short by a crash is simply redone. Readers fall back to proc_lineage for whatever
// is not yet reflected, so the builder's lag never changes a result.
type ProcAncestryBuilder struct {
	pg        *PostgresClient
	ch        *ClickHouseClient
	watermark func(ctx context.Context) (time.Time, error)
	pressure  func() bool
	stop      chan struct{}
	slice     time.Duration
}

// SetIngestPressureFunc registers a callback checked before each cycle, so the builder never
// competes with heavy ingestion.
func (b *ProcAncestryBuilder) SetIngestPressureFunc(f func() bool) { b.pressure = f }

// procAncestrySliceRows is the lineage rows one slice aims for: enough to keep a backfill of
// existing history moving, small enough for every join to fit in memory on a capped pod.
const procAncestrySliceRows = 200_000

// procAncestryCycleBudget bounds one cycle, so the lock is released and progress recorded often.
const procAncestryCycleBudget = 30 * time.Second

// NewProcAncestryBuilder returns a builder that never reads past watermark, the time before
// which every stamped log is visible.
func NewProcAncestryBuilder(pg *PostgresClient, ch *ClickHouseClient, watermark func(ctx context.Context) (time.Time, error)) *ProcAncestryBuilder {
	return &ProcAncestryBuilder{pg: pg, ch: ch, watermark: watermark, slice: 5 * time.Minute}
}

// Start runs a cycle every interval until Stop.
func (b *ProcAncestryBuilder) Start(interval time.Duration) {
	b.stop = make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-b.stop:
				return
			case <-t.C:
				if b.pressure != nil && b.pressure() {
					continue
				}
				if err := b.Cycle(context.Background()); err != nil {
					log.Printf("[Ancestry] cycle: %v", err)
				}
			}
		}
	}()
}

// Stop ends the cycles.
func (b *ProcAncestryBuilder) Stop() {
	if b.stop != nil {
		close(b.stop)
	}
}

// Cycle reflects lineage up to the ingest watermark, or as far as the cycle's budget allows,
// on the replica that holds the lock.
func (b *ProcAncestryBuilder) Cycle(ctx context.Context) error {
	lockCtx, unlock, ok := b.pg.HoldAdvisoryLock(ctx, LockProcAncestry)
	if !ok {
		return nil
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(lockCtx, procAncestryCycleBudget)
	defer cancel()
	// Rows a layout conversion has yet to copy back would be passed over for good.
	if pending, err := b.ch.ConversionInProgress(ctx); err != nil || pending {
		return err
	}
	hi, err := b.watermark(ctx)
	if err != nil {
		return err
	}
	return b.ch.reflectProcAncestry(ctx, hi, &b.slice)
}

// reflectProcAncestry reflects lineage ingested before hi into proc_ancestry, one slice at a
// time, advancing the watermark after each. slice adapts so each holds about
// procAncestrySliceRows lineage rows.
func (c *ClickHouseClient) reflectProcAncestry(ctx context.Context, hi time.Time, slice *time.Duration) error {
	from, err := c.procAncestryWatermark(ctx)
	if err != nil {
		return err
	}
	if from.IsZero() {
		if from, err = c.firstLineageIngest(ctx); err != nil || from.IsZero() {
			return err
		}
	}
	for from.Before(hi) && ctx.Err() == nil {
		to := from.Add(*slice)
		if to.After(hi) {
			to = hi
		}
		n, err := c.lineageRowsBetween(ctx, from, to)
		if err != nil {
			return err
		}
		if n > procAncestrySliceRows && to.Sub(from) > time.Millisecond {
			*slice = max(time.Millisecond, *slice/2)
			continue
		}
		if n > 0 {
			if err := c.buildProcAncestry(ctx, from, to); err != nil {
				// A slice too large to finish in one cycle is halved for the next.
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					*slice = max(time.Millisecond, *slice/2)
					log.Printf("[Ancestry] slice from %s did not finish in a cycle; trying %s", from.Format(time.RFC3339), *slice)
				}
				return err
			}
		}
		if err := c.setProcAncestryWatermark(ctx, to); err != nil {
			return err
		}
		if n < procAncestrySliceRows/4 {
			*slice = min(24*time.Hour, *slice*2)
		}
		from = to
	}
	return ctx.Err()
}

func (c *ClickHouseClient) procAncestryWatermark(ctx context.Context) (time.Time, error) {
	var at time.Time
	err := c.conn.QueryRow(ctx, "SELECT argMax(at, set_at) FROM "+c.topo.Table(procAncestryWatermarkTable)).Scan(&at)
	if err != nil {
		return time.Time{}, fmt.Errorf("read ancestry watermark: %w", err)
	}
	if at.Unix() <= 0 {
		return time.Time{}, nil
	}
	return at.UTC(), nil
}

// setProcAncestryWatermark records at. Writes are ordered after the last one rather than by any
// node's clock, so the latest write is the one readers see.
func (c *ClickHouseClient) setProcAncestryWatermark(ctx context.Context, at time.Time) error {
	table := c.topo.Table(procAncestryWatermarkTable)
	return c.conn.Exec(ctx, "INSERT INTO "+table+" (id, at, set_at) SELECT 1, ?, greatest(now64(3), "+
		"(SELECT max(set_at) FROM "+table+") + toIntervalMillisecond(1))"+procAncestryInsertSettings, at)
}

func (c *ClickHouseClient) firstLineageIngest(ctx context.Context) (time.Time, error) {
	var at time.Time
	if err := c.conn.QueryRow(ctx, "SELECT min(ingest_timestamp) FROM "+c.topo.Table("proc_lineage")).Scan(&at); err != nil {
		return time.Time{}, fmt.Errorf("read first lineage ingest: %w", err)
	}
	if at.Unix() <= 0 {
		return time.Time{}, nil
	}
	return at.UTC(), nil
}

func (c *ClickHouseClient) lineageRowsBetween(ctx context.Context, from, to time.Time) (uint64, error) {
	var n uint64
	err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.topo.Table("proc_lineage")+" WHERE "+ingestBetween(from, to)).Scan(&n)
	return n, err
}

// procAncestryInsertSettings make each insert visible before the next step reads it, which on a
// cluster means waiting for the Distributed table to reach the shard.
const procAncestryInsertSettings = " SETTINGS distributed_foreground_insert = 1, max_threads = 2, max_insert_threads = 1"

func ingestBetween(from, to time.Time) string {
	return fmt.Sprintf("ingest_timestamp >= %s AND ingest_timestamp < %s", chTime(from), chTime(to))
}

func chTime(t time.Time) string {
	return "toDateTime64(" + sqlquote.String(t.UTC().Format("2006-01-02 15:04:05.000")) + ", 3, 'UTC')"
}

// buildProcAncestry reflects the lineage rows ingested in [from, to).
func (c *ClickHouseClient) buildProcAncestry(ctx context.Context, from, to time.Time) error {
	for _, stmt := range procAncestrySQL(c.topo.Table("proc_lineage"), c.topo.Table(procAncestryTable), c.topo.DistributedTables, from, to) {
		if err := c.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("build ancestry: %w", err)
		}
	}
	return nil
}

// procAncestrySQL is one slice's statements, in order.
func procAncestrySQL(lineage, anc string, distributed bool, from, to time.Time) []string {
	in, join := "IN", "INNER JOIN"
	if distributed {
		in, join = "GLOBAL IN", "GLOBAL INNER JOIN"
	}
	cols := "(fractal_id, proc_key, depth, process_guid, image, anc_key, anc_image, open_key, ingest_timestamp, version)"
	insert := "INSERT INTO " + anc + " " + cols + " "
	// batch is the slice's processes, one row each however often they were ingested.
	batch := fmt.Sprintf("SELECT fractal_id, proc_key, argMax(parent_key, timestamp) AS parent_key, argMax(process_guid, timestamp) AS process_guid, "+
		"argMax(image, timestamp) AS image, argMax(parent_image, timestamp) AS parent_image, max(ingest_timestamp) AS ingest_timestamp "+
		"FROM (SELECT fractal_id, proc_key, parent_key, process_guid, image, parent_image, timestamp, ingest_timestamp FROM %s WHERE %s) "+
		"GROUP BY fractal_id, proc_key", lineage, ingestBetween(from, to))
	batchKeys := fmt.Sprintf("SELECT proc_key FROM %s WHERE %s", lineage, ingestBetween(from, to))
	// rows reads one row per (process, depth) of the processes keyed by where, newest first.
	rows := func(where string) string {
		return fmt.Sprintf("SELECT fractal_id, proc_key, depth, argMax(process_guid, version) AS process_guid, argMax(image, version) AS image, "+
			"argMax(anc_key, version) AS anc_key, argMax(anc_image, version) AS anc_image, argMax(ingest_timestamp, version) AS ingest_timestamp "+
			"FROM (SELECT * FROM %s WHERE depth >= 1 AND %s) GROUP BY fractal_id, proc_key, depth", anc, where)
	}
	var out []string
	// Depth 1 is each process's own parent. A parent logged by image alone still counts, as it
	// does for the walk, though nothing can be found above it.
	out = append(out, insert+fmt.Sprintf("SELECT fractal_id, proc_key, 1, process_guid, image, parent_key, parent_image, '', ingest_timestamp, now64(3) "+
		"FROM (%s) WHERE parent_key != '' OR parent_image != ''", batch)+procAncestryInsertSettings)
	// Each further depth is the parent's row one level up, so a parent in the same slice is
	// complete to that depth by the time its children read it.
	for d := 1; d < procAncestryDepth; d++ {
		out = append(out, insert+fmt.Sprintf("SELECT b.fractal_id, b.proc_key, %d, b.process_guid, b.image, a.anc_key, a.anc_image, '', b.ingest_timestamp, now64(3) "+
			"FROM (%s) AS b %s (%s) AS a ON a.fractal_id = b.fractal_id AND a.proc_key = b.parent_key WHERE b.parent_key != ''",
			d+1, batch, join, rows(fmt.Sprintf("depth = %d AND proc_key %s (SELECT parent_key FROM (%s))", d, in, batch)))+procAncestryInsertSettings)
	}
	// A process whose chain passes through one that just changed continues through it. Each pass
	// reads the rows the one before wrote, so a chain extended two levels up is complete after
	// two passes; chains are at most procAncestryDepth long.
	changed, touched := batchKeys, []string{batchKeys}
	for pass := 0; pass < procAncestryDepth; pass++ {
		out = append(out, insert+fmt.Sprintf("SELECT d.fractal_id, d.proc_key, d.depth + x.depth, d.process_guid, d.image, x.anc_key, x.anc_image, '', d.ingest_timestamp, now64(3) "+
			"FROM (%s) AS d %s (%s) AS x ON x.fractal_id = d.fractal_id AND x.proc_key = d.anc_key WHERE d.anc_key != '' AND d.depth + x.depth <= %d",
			rows(fmt.Sprintf("anc_key %s (%s)", in, changed)), join, rows(fmt.Sprintf("proc_key %s (%s)", in, changed)), procAncestryDepth)+procAncestryInsertSettings)
		changed = fmt.Sprintf("SELECT proc_key FROM %s WHERE depth >= 1 AND anc_key %s (%s)", anc, in, changed)
		touched = append(touched, changed)
	}
	// Status rows, last. A chain is complete at the maximum depth, at a parent logged by image
	// alone, or at a root whose lineage is in by the end of the slice; anything else is open at
	// its deepest ancestor, so readers walk for it.
	chains := fmt.Sprintf("SELECT fractal_id, proc_key, max(depth) AS deepest, argMax(anc_key, depth) AS last, any(process_guid) AS process_guid, "+
		"any(image) AS image, any(ingest_timestamp) AS ingest_timestamp FROM (%s) GROUP BY fractal_id, proc_key",
		rows(fmt.Sprintf("proc_key %s (%s)", in, strings.Join(touched, " UNION ALL "))))
	roots := fmt.Sprintf("SELECT fractal_id, proc_key FROM %s WHERE ingest_timestamp < %s AND parent_key = '' AND parent_image = '' AND proc_key %s (SELECT last FROM (%s))",
		lineage, chTime(to), in, chains)
	out = append(out, insert+fmt.Sprintf("SELECT fractal_id, proc_key, 0, process_guid, image, '', '', "+
		"if(deepest >= %d OR last = '' OR (fractal_id, last) %s (%s), '', last), ingest_timestamp, now64(3) FROM (%s)",
		procAncestryDepth, in, roots, chains)+procAncestryInsertSettings)
	// Processes with no parent at all have no ancestor rows, only a status row.
	out = append(out, insert+fmt.Sprintf("SELECT fractal_id, proc_key, 0, process_guid, image, '', '', '', ingest_timestamp, now64(3) "+
		"FROM (%s) WHERE parent_key = '' AND parent_image = ''", batch)+procAncestryInsertSettings)
	return out
}

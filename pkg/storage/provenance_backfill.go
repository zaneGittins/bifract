package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// provenanceProjections are the lookup projections migration 019 adds, by table.
var provenanceProjections = []struct {
	table string
	names []string
}{
	{"proc_lineage", []string{"by_parent", "by_image"}},
	{"process_edges", []string{"by_target"}},
	{"proc_freq", []string{"by_target"}},
}

// provenanceSummaryViews feed provenance_summary; the backfill replays their SELECTs.
var provenanceSummaryViews = []string{"provenance_summary_lineage_mv", "provenance_summary_freq_mv"}

// Working tables the summary fill keeps on each shard until it is recorded filled: the
// fractals it has filled, and the fractals cleared while it ran (markProvenanceFillClear).
const (
	provenanceFillDone    = provenanceSummaryTable + "__done"
	provenanceFillCleared = provenanceSummaryTable + "__cleared"
)

// provenanceFillAll in provenanceFillCleared stands for every fractal: a whole-table clear.
const provenanceFillAll = "*"

var provenanceFillTableDDL = []string{
	"CREATE TABLE IF NOT EXISTS " + provenanceFillDone + " (fractal_id String) ENGINE = MergeTree ORDER BY fractal_id",
	"CREATE TABLE IF NOT EXISTS " + provenanceFillCleared + " (fractal_id String, at DateTime64(3)) ENGINE = MergeTree ORDER BY fractal_id",
}

// provenanceFillAttempts bounds how often one fractal is refilled after clears.
const provenanceFillAttempts = 3

// provenanceProgressLogEvery spaces the log lines of a long-running projection mutation.
const provenanceProgressLogEvery = 10 * time.Minute

// The projection backfill checks on a mutation it just started after the shorter interval,
// since a small partition's is done in moments, and backs off to the longer one while a large
// partition's runs.
var provenanceBackfillMinPoll, provenanceBackfillPoll = time.Second, 30 * time.Second

// provenanceMutationFailLimit is how long a projection mutation may keep failing, which
// ClickHouse retries on its own, before the backfill kills it and moves on.
var provenanceMutationFailLimit = 30 * time.Minute

// provenanceFillSettings throttle the summary fill like a repartition copy and let its
// GROUP BY spill, so it cannot monopolise a node beside live ingest and queries.
var provenanceFillSettings = "max_threads = 2, max_insert_threads = 1, max_execution_time = 0, " + SpillSettingsClause(1<<30)

// ProvenanceSummaryState records whether the backfill has filled provenance_summary.
type ProvenanceSummaryState interface {
	ProvenanceSummaryFilled(context.Context) (bool, error)
	MarkProvenanceSummaryFilled(context.Context) error
}

// BackfillProvenancePaths completes migration 019 for rows written before it, on every shard:
// it fills provenance_summary (then records it, so pgr stops scanning), then materializes the
// lookup projections one partition per node at a time. Each step resumes where it stopped.
// Run it once no partition conversion is copying rows, which bypass the summary views, and
// retry until it returns nil.
func (c *ClickHouseClient) BackfillProvenancePaths(ctx context.Context, state ProvenanceSummaryState) error {
	filled, err := state.ProvenanceSummaryFilled(ctx)
	if err != nil {
		return fmt.Errorf("read provenance summary state: %w", err)
	}
	if !filled {
		if err := c.onEveryShardPinned(ctx, "fill provenance summary", func(conn driver.Conn) error {
			return fillProvenanceSummary(ctx, conn)
		}); err != nil {
			return fmt.Errorf("fill provenance summary: %w", err)
		}
		if err := state.MarkProvenanceSummaryFilled(ctx); err != nil {
			return fmt.Errorf("record provenance summary filled: %w", err)
		}
		log.Printf("[ClickHouse] provenance summary filled")
	}
	if err := c.onEveryShardPinned(ctx, "drop provenance fill tables", func(conn driver.Conn) error {
		return dropTables(ctx, conn, provenanceFillDone, provenanceFillCleared)
	}); err != nil {
		return err
	}
	return c.materializeProvenanceProjections(ctx)
}

// fillProvenanceSummary replays the summary views over the rows already in this shard's
// tables, one fractal at a time. The views write the same rows for anything inserted
// meanwhile, and last_day keeps the max, so a repeat is harmless.
func fillProvenanceSummary(ctx context.Context, conn driver.Conn) error {
	// Without the views, rows ingested before they existed would never be summarized.
	var views uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND has(?, name)",
		provenanceSummaryViews).Scan(&views); err != nil {
		return err
	}
	if views != uint64(len(provenanceSummaryViews)) {
		return fmt.Errorf("provenance summary views missing; migration 019 has not finished")
	}
	selects := make([]string, len(provenanceSummaryViews))
	for i, view := range provenanceSummaryViews {
		sel, err := initViewSelect(view)
		if err != nil {
			return err
		}
		selects[i] = sel
	}
	for _, ddl := range provenanceFillTableDDL {
		if err := conn.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	done, err := stringSet(ctx, conn, "SELECT fractal_id FROM "+provenanceFillDone)
	if err != nil {
		return err
	}
	fractals, err := stringColumn(ctx, conn, "SELECT DISTINCT fractal_id FROM (SELECT fractal_id FROM proc_lineage UNION ALL SELECT fractal_id FROM proc_freq)")
	if err != nil {
		return err
	}
	for _, f := range fractals {
		if done[f] {
			continue
		}
		if err := fillFractalSummary(ctx, conn, f, selects); err != nil {
			return fmt.Errorf("fractal %q: %w", f, err)
		}
		if err := conn.Exec(ctx, "INSERT INTO "+provenanceFillDone+" VALUES (?)", f); err != nil {
			return err
		}
	}
	return nil
}

// fillFractalSummary fills one fractal's summary. A fractal cleared while its rows were read
// may have had stale rows written back, so its summary is dropped and filled again. A clear
// records itself after its drop, so any recorded at or after the read's start may have
// raced it.
func fillFractalSummary(ctx context.Context, conn driver.Conn, f string, selects []string) error {
	// The views' SELECTs stay verbatim; the filter scopes their reads to one partition.
	cond := "'" + escCHLiteral("fractal_id = '"+escCHLiteral(f)+"'") + "'"
	filters := "{'proc_lineage': " + cond + ", 'proc_freq': " + cond + "}"
	for range provenanceFillAttempts {
		var start time.Time
		if err := conn.QueryRow(ctx, "SELECT now64(3)").Scan(&start); err != nil {
			return err
		}
		for _, sel := range selects {
			// The newline keeps a trailing comment in a view's SELECT from swallowing the settings.
			if err := conn.Exec(ctx, "INSERT INTO "+provenanceSummaryTable+" "+sel+"\nSETTINGS additional_table_filters = "+filters+", "+provenanceFillSettings); err != nil {
				return err
			}
		}
		var cleared uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+provenanceFillCleared+" WHERE fractal_id IN (?, ?) AND at >= ?",
			f, provenanceFillAll, start).Scan(&cleared); err != nil {
			return err
		}
		if cleared == 0 {
			return nil
		}
		if err := dropFractalPartitions(ctx, conn, provenanceSummaryTable, []string{f}); err != nil {
			return err
		}
	}
	return fmt.Errorf("cleared during every attempt to fill it")
}

// markProvenanceFillClear records fractals just cleared on this node, after their rows are
// dropped, so a summary fill in progress fills them again. A no-op when no fill is running.
func markProvenanceFillClear(ctx context.Context, conn driver.Conn, ids []string) error {
	cleared, err := describeTable(ctx, conn, provenanceFillCleared)
	if err != nil || !cleared.exists {
		return err
	}
	for _, id := range ids {
		err := conn.Exec(ctx, "INSERT INTO "+provenanceFillCleared+" SELECT ?, now64(3)", id)
		var ex *clickhouse.Exception
		if errors.As(err, &ex) && ex.Code == 60 { // UNKNOWN_TABLE: the fill finished meanwhile
			return nil
		}
		if err != nil {
			return fmt.Errorf("record cleared fractal for the provenance fill: %w", err)
		}
	}
	return nil
}

// initViewSelect returns a materialized view's SELECT from init-clickhouse.sql.
func initViewSelect(view string) (string, error) {
	stmt, err := initStatement("CREATE MATERIALIZED VIEW IF NOT EXISTS " + view + " ")
	if err != nil {
		return "", err
	}
	_, sel, ok := strings.Cut(stmt, " AS\n")
	if !ok {
		return "", fmt.Errorf("view %s has no SELECT in init-clickhouse.sql", view)
	}
	return strings.TrimSpace(sel), nil
}

// projectionRun is the projection backfill's state across rounds, per node.
type projectionRun struct {
	last    map[string]string    // node -> partition its last mutation covered
	failing map[string]time.Time // node/table/mutation -> when it was first seen failing
	skipped map[string]bool      // node/table partition left for a later run
	logged  map[string]time.Time // node -> last progress log of a running mutation
}

// materializeProvenanceProjections runs rounds until no node has a partition left: each
// round, a node whose last mutation finished starts the next. A partition that cannot be
// materialized is skipped so the rest proceed, and left for the next start.
func (c *ClickHouseClient) materializeProvenanceProjections(ctx context.Context) error {
	run := &projectionRun{last: map[string]string{}, failing: map[string]time.Time{}, skipped: map[string]bool{}, logged: map[string]time.Time{}}
	wait := provenanceBackfillMinPoll
	for {
		pending, started := 0, false
		err := c.onEveryShardPinned(ctx, "materialize provenance projections", func(conn driver.Conn) error {
			var node string
			if err := conn.QueryRow(ctx, "SELECT hostName()").Scan(&node); err != nil {
				return err
			}
			before := run.last[node]
			n, err := run.next(ctx, conn, node)
			started = started || run.last[node] != before
			pending += n
			return err
		})
		if err != nil {
			return err
		}
		if pending == 0 {
			if len(run.skipped) > 0 {
				log.Printf("[ClickHouse] provenance projections complete except %d partitions, which pgr reads without them; the next start retries them", len(run.skipped))
				return nil
			}
			log.Printf("[ClickHouse] provenance projections complete")
			return nil
		}
		if started {
			wait = provenanceBackfillMinPoll
		} else {
			wait = min(max(2*wait, provenanceBackfillMinPoll), provenanceBackfillPoll)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

var mutationPartitionRe = regexp.MustCompile(`IN PARTITION ID '([^']*)'`)

// next starts this node's next projection mutation unless one is running, and reports how
// many partitions and running mutations are left, skipped partitions aside.
func (r *projectionRun) next(ctx context.Context, conn driver.Conn, node string) (int, error) {
	running, err := r.watchRunning(ctx, conn, node)
	if err != nil || running > 0 {
		return running, err
	}
	type target struct {
		table, part string
		missing     []string
	}
	var todo []target
	for _, p := range provenanceProjections {
		rows, err := conn.Query(ctx, "SELECT partition_id, groupUniqArrayArray(arrayFilter(p -> NOT has(projections, p), ?)) AS missing "+
			"FROM system.parts WHERE database = currentDatabase() AND table = ? AND active GROUP BY partition_id HAVING length(missing) > 0 ORDER BY partition_id", p.names, p.table)
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			t := target{table: p.table}
			if err := rows.Scan(&t.part, &t.missing); err != nil {
				rows.Close()
				return 0, err
			}
			todo = append(todo, t)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return 0, err
		}
	}
	pending := 0
	for _, t := range todo {
		key := t.table + " " + t.part
		if r.skipped[node+"/"+key] {
			continue
		}
		if key == r.last[node] {
			log.Printf("[ClickHouse] %s partition %s on %s still lacks %v after its mutation; skipping it this run", t.table, t.part, node, t.missing)
			r.skipped[node+"/"+key] = true
			continue
		}
		if pending++; pending > 1 {
			continue
		}
		cmds := make([]string, len(t.missing))
		for i, name := range t.missing {
			cmds[i] = "MATERIALIZE PROJECTION " + name + " IN PARTITION ID '" + escCHLiteral(t.part) + "'"
		}
		if err := conn.Exec(ctx, "ALTER TABLE "+t.table+" "+strings.Join(cmds, ", ")); err != nil {
			return 0, fmt.Errorf("materialize %v on %s partition %s on %s: %w", t.missing, t.table, t.part, node, err)
		}
		r.last[node] = key
		log.Printf("[ClickHouse] materializing %v on %s partition %s on %s", t.missing, t.table, t.part, node)
	}
	return pending, nil
}

// watchRunning reports this node's projection mutations still running. One failing past
// provenanceMutationFailLimit is killed and its partition skipped for this run; until then
// ClickHouse's own retries may still succeed.
func (r *projectionRun) watchRunning(ctx context.Context, conn driver.Conn, node string) (int, error) {
	tables := make([]string, len(provenanceProjections))
	for i, p := range provenanceProjections {
		tables[i] = p.table
	}
	// One row per command, so a mutation materializing two projections has two.
	rows, err := conn.Query(ctx, "SELECT mutation_id, table, any(command), any(latest_fail_reason), max(parts_to_do) FROM system.mutations "+
		"WHERE database = currentDatabase() AND NOT is_done AND has(?, table) AND command LIKE '%MATERIALIZE PROJECTION%' "+
		"GROUP BY mutation_id, table", tables)
	if err != nil {
		return 0, err
	}
	type mutation struct {
		id, table, command, fail string
		partsToDo                int64
	}
	var running []mutation
	for rows.Next() {
		var m mutation
		if err := rows.Scan(&m.id, &m.table, &m.command, &m.fail, &m.partsToDo); err != nil {
			rows.Close()
			return 0, err
		}
		running = append(running, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, m := range running {
		key := node + "/" + m.table + "/" + m.id
		if m.fail == "" {
			delete(r.failing, key)
			if time.Since(r.logged[node]) >= provenanceProgressLogEvery {
				log.Printf("[ClickHouse] still materializing projections on %s on %s (%d parts to do)", m.table, node, m.partsToDo)
				r.logged[node] = time.Now()
			}
			continue
		}
		since, seen := r.failing[key]
		if !seen {
			r.failing[key] = time.Now()
			log.Printf("[ClickHouse] projection mutation %s on %s on %s is failing, ClickHouse will retry it: %s", m.id, m.table, node, m.fail)
			continue
		}
		if time.Since(since) < provenanceMutationFailLimit {
			continue
		}
		if err := conn.Exec(ctx, "KILL MUTATION WHERE database = currentDatabase() AND table = '"+escCHLiteral(m.table)+"' AND mutation_id = '"+escCHLiteral(m.id)+"'"); err != nil {
			return 0, fmt.Errorf("kill failing mutation %s on %s on %s: %w", m.id, m.table, node, err)
		}
		delete(r.failing, key)
		if p := mutationPartitionRe.FindStringSubmatch(m.command); p != nil {
			r.skipped[node+"/"+m.table+" "+p[1]] = true
		}
		log.Printf("[ClickHouse] killed projection mutation %s on %s on %s after failing for %s: %s", m.id, m.table, node, provenanceMutationFailLimit, m.fail)
	}
	return len(running), nil
}

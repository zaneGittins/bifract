package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

const (
	describeQuery  = "SELECT partition_key, engine_full FROM system.tables"
	partsQuery     = "SELECT DISTINCT partition_id FROM system.parts"
	positionsQuery = "ORDER BY position"
	colTypesQuery  = "SELECT name, type FROM system.columns"
)

var lineage = repartitionTarget{table: "proc_lineage", key: "fractal_id"}

func describes(key, engine string) fakedb.CHResult {
	return fakedb.CHResult{Cols: []fakedb.Col{{Column: "partition_key", Type: "String"}, {Column: "engine_full", Type: "String"}}, Rows: [][]any{{key, engine}}}
}

// stdColumns scripts identical column lists for every table, which is what
// sameColumns and columnList read.
func stdColumns(f *fakedb.CH) {
	f.On(colTypesQuery, fakedb.CHResult{Cols: []fakedb.Col{{Column: "name", Type: "String"}, {Column: "type", Type: "String"}}, Rows: [][]any{{"fractal_id", "String"}, {"guid", "String"}}})
	f.On(positionsQuery, fakedb.Strs("name", "fractal_id", "guid"))
}

func indexOf(stmts []string, sub string) int {
	for i, s := range stmts {
		if strings.Contains(s, sub) {
			return i
		}
	}
	return -1
}

func TestConvertTargetAbsentTableIsNoop(t *testing.T) {
	c, f := newFakeClient()
	if err := c.convertTarget(context.Background(), f, lineage, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.Executed()) != 0 {
		t.Errorf("executed %v for a table that does not exist", f.Executed())
	}
}

// Converted, with no copy in progress: only bookkeeping a crash may have left is
// dropped. Nothing is copied or exchanged.
func TestConvertTargetAlreadyConverted(t *testing.T) {
	c, f := newFakeClient()
	f.OnArg(describeQuery, "proc_lineage", describes("fractal_id", "MergeTree ORDER BY x"))
	if err := c.convertTarget(context.Background(), f, lineage, nil); err != nil {
		t.Fatal(err)
	}
	got := f.Executed()
	if len(got) != 3 || indexOf(got, "`proc_lineage__stage`") < 0 || indexOf(got, "`proc_lineage__done`") < 0 || indexOf(got, "`proc_lineage__skip`") < 0 {
		t.Errorf("executed %v, want only the stage/done/skip drops", got)
	}
	if indexOf(got, "__old") >= 0 || indexOf(got, "EXCHANGE") >= 0 {
		t.Errorf("executed %v", got)
	}
}

// A leftover __old on an unexpected key means something other than this converter
// made it; copying from it would be guessing.
func TestConvertTargetRefusesForeignOldTable(t *testing.T) {
	c, f := newFakeClient()
	f.OnArg(describeQuery, "proc_lineage", describes("tuple()", ""))
	f.OnArg(describeQuery, "proc_lineage__old", describes("toYYYYMM(day)", ""))
	err := c.convertTarget(context.Background(), f, lineage, nil)
	if err == nil || !strings.Contains(err.Error(), "proc_lineage__old exists with partition key") {
		t.Fatalf("err = %v", err)
	}
	if len(f.Executed()) != 0 {
		t.Errorf("executed %v before refusing", f.Executed())
	}
}

// The full conversion: build and prove the new table, swap atomically, then copy each
// kept fractal through the stage exactly once. A fractal already recorded done is
// not copied again (these tables sum, so a second copy inflates counts), and rows of
// deleted fractals are left behind.
func TestConvertTargetSwapsThenCopiesEachKeptFractalOnce(t *testing.T) {
	c, f := newFakeClient()
	f.OnArg(describeQuery, "proc_lineage", describes("tuple()", "MergeTree ORDER BY x TTL day + toIntervalDay(99) SETTINGS index_granularity = 8192"))
	stdColumns(f)
	f.On("SELECT DISTINCT fractal_id FROM `proc_lineage__old`", fakedb.Strs("fractal_id", "f-copy", "f-done", "f-deleted"))
	f.On("SELECT fractal_id FROM `proc_lineage__done`", fakedb.Strs("fractal_id", "f-done"))
	f.OnArg(partsQuery, []string{"f-copy"}, fakedb.Strs("partition_id", "pid-copy"))
	f.On("SELECT count() FROM `proc_lineage__skip`", fakedb.Count(0))

	keep := map[string]bool{"f-copy": true, "f-done": true}
	if err := c.convertTarget(context.Background(), f, lineage, keep); err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()

	// Built from init SQL under the working names, carrying the live TTL.
	if indexOf(execs, "CREATE TABLE IF NOT EXISTS proc_lineage__old (") < 0 || indexOf(execs, "CREATE TABLE IF NOT EXISTS proc_lineage__stage (") < 0 {
		t.Errorf("working tables not created from init SQL: %v", execs)
	}
	if indexOf(execs, "ALTER TABLE `proc_lineage__old` MODIFY TTL day + toIntervalDay(99) SETTINGS materialize_ttl_after_modify = 0") < 0 {
		t.Error("the operator's TTL must carry over to the new table")
	}

	// The preflight move happens before the swap, and the copy after it.
	probe, swap := indexOf(execs, "MOVE PARTITION ID '00000000000000000000000000000000'"), indexOf(execs, "EXCHANGE TABLES `proc_lineage` AND `proc_lineage__old`")
	copyStmt := indexOf(execs, "INSERT INTO `proc_lineage__stage` (`fractal_id`, `guid`) SELECT `fractal_id`, `guid` FROM `proc_lineage__old` WHERE fractal_id = ?"+repartitionCopySettings)
	if probe < 0 || swap < 0 || copyStmt < 0 || !(probe < swap && swap < copyStmt) {
		t.Fatalf("order probe=%d swap=%d copy=%d:\n%s", probe, swap, copyStmt, strings.Join(execs, "\n"))
	}

	copies := 0
	for i, s := range execs {
		if strings.HasPrefix(s, "INSERT INTO `proc_lineage__stage`") && strings.Contains(s, "FROM `proc_lineage__old`") {
			copies++
			if fmt.Sprint(f.ExecArgs()[i]) != "[f-copy]" {
				t.Errorf("copied %v, want only f-copy", f.ExecArgs()[i])
			}
		}
	}
	if copies != 1 {
		t.Errorf("%d copies, want 1", copies)
	}
	done := indexOf(execs, "INSERT INTO `proc_lineage__done` VALUES (?)")
	move := indexOf(execs, "ALTER TABLE `proc_lineage__stage` MOVE PARTITION ID 'pid-copy' TO TABLE `proc_lineage`")
	if done < copyStmt || move < done {
		t.Errorf("the copy must be recorded done before its partitions move (done=%d move=%d)", done, move)
	}

	// Working tables go last.
	last := execs[len(execs)-4:]
	for i, name := range []string{"__old", "__stage", "__done", "__skip"} {
		if !strings.HasPrefix(last[i], "DROP TABLE IF EXISTS `proc_lineage"+name+"`") {
			t.Errorf("final drops = %v", last)
		}
	}
}

// A fractal cleared while its copy ran must not have the copy moved in.
func TestCopyOldHonoursAClearThatLandsMidCopy(t *testing.T) {
	c, f := newFakeClient()
	stdColumns(f)
	f.On("SELECT DISTINCT fractal_id FROM `proc_lineage__old`", fakedb.Strs("fractal_id", "f1"))
	f.OnArgOnce("SELECT count() FROM `proc_lineage__skip`", "f1", fakedb.Count(0))
	f.OnArg("SELECT count() FROM `proc_lineage__skip`", "f1", fakedb.Count(1))
	f.OnArg(partsQuery, []string{"f1"}, fakedb.Strs("partition_id", "staged"))

	if err := c.copyOld(context.Background(), f, lineage, map[string]bool{"f1": true}); err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()
	if indexOf(execs, "MOVE PARTITION") >= 0 {
		t.Errorf("a cleared fractal's copy was moved in: %v", execs)
	}
	// Dropped from the stage both before the copy (stale attempt) and after (the clear).
	drops := 0
	for _, s := range execs {
		if strings.HasPrefix(s, "ALTER TABLE `proc_lineage__stage` DROP PARTITION ID 'staged'") {
			drops++
		}
	}
	if drops != 2 {
		t.Errorf("%d stage drops, want 2: %v", drops, execs)
	}
}

// A fractal already marked skipped before the copy starts is never copied.
func TestCopyOldSkipsClearedFractal(t *testing.T) {
	c, f := newFakeClient()
	stdColumns(f)
	f.On("SELECT DISTINCT fractal_id FROM `proc_lineage__old`", fakedb.Strs("fractal_id", "f1"))
	f.On("SELECT count() FROM `proc_lineage__skip`", fakedb.Count(1))
	if err := c.copyOld(context.Background(), f, lineage, map[string]bool{"f1": true}); err != nil {
		t.Fatal(err)
	}
	if indexOf(f.Executed(), "FROM `proc_lineage__old` WHERE") >= 0 {
		t.Error("a cleared fractal was copied")
	}
}

func TestSwapInRefusesDifferingColumns(t *testing.T) {
	c, f := newFakeClient()
	f.OnArg(colTypesQuery, "proc_lineage", fakedb.CHResult{Cols: []fakedb.Col{{Column: "name", Type: "String"}, {Column: "type", Type: "String"}}, Rows: [][]any{{"a", "String"}}})
	f.OnArg(colTypesQuery, "proc_lineage__old", fakedb.CHResult{Cols: []fakedb.Col{{Column: "name", Type: "String"}, {Column: "type", Type: "String"}}, Rows: [][]any{{"a", "UInt8"}}})
	err := c.swapIn(context.Background(), f, lineage, tableInfo{exists: true}, false)
	if err == nil || !strings.Contains(err.Error(), "differ from init-clickhouse.sql") {
		t.Fatalf("err = %v", err)
	}
	if indexOf(f.Executed(), "EXCHANGE") >= 0 {
		t.Error("swapped despite differing columns")
	}
}

// With rows in the live table, a real row is moved through the new table before
// committing to the swap; if it does not arrive the swap is refused.
func TestProbeMove(t *testing.T) {
	probe := []string{repartitionProbeFractal}

	f := fakedb.NewCH()
	stdColumns(f)
	f.OnArg(partsQuery, probe, fakedb.Strs("partition_id", "probe-pid"))
	if err := probeMove(context.Background(), f, "live", "stage", "dst"); err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()
	ins := indexOf(execs, "INSERT INTO `stage` (`fractal_id`, `guid`) SELECT ? AS `fractal_id`, `guid` FROM `live` LIMIT 1")
	mv := indexOf(execs, "ALTER TABLE `stage` MOVE PARTITION ID 'probe-pid' TO TABLE `dst`")
	clean := indexOf(execs, "ALTER TABLE `dst` DROP PARTITION ID 'probe-pid'")
	if ins < 0 || mv < ins || clean < mv {
		t.Errorf("probe sequence:\n%s", strings.Join(execs, "\n"))
	}

	f = fakedb.NewCH()
	stdColumns(f)
	f.OnArgOnce(partsQuery, probe, fakedb.CHResult{})                        // stage clean before the insert
	f.OnArgOnce(partsQuery, probe, fakedb.Strs("partition_id", "probe-pid")) // staged
	if err := probeMove(context.Background(), f, "live", "stage", "dst"); err == nil || !strings.Contains(err.Error(), "did not arrive") {
		t.Errorf("err = %v, want the probe failure", err)
	}

	f = fakedb.NewCH()
	f.FailExec("00000000000000000000000000000000", errors.New("incompatible"))
	if err := probeMove(context.Background(), f, "live", "stage", "dst"); err == nil {
		t.Error("an incompatible MOVE must fail the preflight")
	}
}

// Clearing a fractal while a conversion is in progress must stop its earlier rows
// being copied back afterwards.
func TestMarkRepartitionSkip(t *testing.T) {
	f := fakedb.NewCH()
	f.OnArg(describeQuery, "proc_freq__skip", describes("", ""))
	f.OnArg(partsQuery, []string{"f1", ""}, fakedb.Strs("partition_id", "s1"))
	if err := markRepartitionSkip(context.Background(), f, []string{"f1", ""}); err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()
	if len(execs) != 3 ||
		execs[0] != "INSERT INTO `proc_freq__skip` VALUES (?)" ||
		execs[2] != "ALTER TABLE `proc_freq__stage` DROP PARTITION ID 's1'"+dropPartitionSettings {
		t.Errorf("executed %q", execs)
	}
}

// One ALTER per 200 partitions keeps a years-old fractal's drop under
// max_query_size.
func TestDropFractalPartitionsBatches(t *testing.T) {
	ids := make([]any, 0, 201)
	for i := range 201 {
		ids = append(ids, fmt.Sprintf("p%03d", i))
	}
	f := fakedb.NewCH()
	f.On(partsQuery, fakedb.One("partition_id", "String", ids...))
	if err := dropFractalPartitions(context.Background(), f, "logs", []string{"f1"}); err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()
	if len(execs) != 2 {
		t.Fatalf("%d statements, want 2", len(execs))
	}
	if strings.Count(execs[0], "DROP PARTITION ID") != 200 || strings.Count(execs[1], "DROP PARTITION ID") != 1 {
		t.Errorf("batch sizes %d, %d", strings.Count(execs[0], "DROP PARTITION ID"), strings.Count(execs[1], "DROP PARTITION ID"))
	}
	if !strings.HasSuffix(execs[0], dropPartitionSettings) {
		t.Error("each drop must lift max_partition_size_to_drop")
	}
}

func TestColumnListRequiresColumns(t *testing.T) {
	if _, err := columnList(context.Background(), fakedb.NewCH(), "t", "src"); err == nil {
		t.Error("an empty column list must be an error, not an empty INSERT")
	}
	f := fakedb.NewCH()
	f.On(positionsQuery, fakedb.Strs("name", "a`b"))
	if got, _ := columnList(context.Background(), f, "t", "src"); got != "`a``b`" {
		t.Errorf("columnList = %q", got)
	}
	// Computed columns cannot be inserted, and a column the source lacks takes its default.
	q := f.Queried()[0]
	for _, want := range []string{"default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')", "AND name IN (SELECT name FROM system.columns"} {
		if !strings.Contains(q, want) {
			t.Errorf("column query missing %q: %s", want, q)
		}
	}
}

func TestStringColumnSurfacesErrors(t *testing.T) {
	f := fakedb.NewCH()
	f.On("q1", fakedb.CHResult{Err: errors.New("down")})
	f.On("q2", fakedb.CHResult{Cols: []fakedb.Col{{Column: "n", Type: "UInt64"}}, Rows: [][]any{{uint64(1)}}})
	if _, err := stringColumn(context.Background(), f, "q1"); err == nil {
		t.Error("query error swallowed")
	}
	if _, err := stringColumn(context.Background(), f, "q2"); err == nil {
		t.Error("scan error swallowed")
	}
}

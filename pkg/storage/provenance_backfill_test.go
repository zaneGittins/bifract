package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"bifract/internal/fakedb"
)

// fillFake answers the summary fill's queries: both views present, fractals f1 and o'k, and
// done/cleared as given.
func fillFake(done []string, cleared func() uint64) (*ClickHouseClient, *fakedb.CH) {
	c, f := newFakeClient()
	f.On("SELECT hostName()", fakedb.Strs("h", "n1"))
	f.On("FROM system.tables WHERE database = currentDatabase() AND name IN", fakedb.CHResult{Cols: []fakedb.Col{{Column: "c", Type: "UInt64"}}, Rows: [][]any{{uint64(2)}}})
	f.On("SELECT DISTINCT fractal_id", fakedb.Strs("fractal_id", "f1", "o'k"))
	f.On("SELECT fractal_id FROM "+provenanceFillDone, fakedb.Strs("fractal_id", done...))
	f.On("SELECT now64(3)", fakedb.CHResult{Cols: []fakedb.Col{{Column: "t", Type: "DateTime64(3)"}}, Rows: [][]any{{time.Unix(1, 0)}}})
	f.Respond(func(q string, _ []any) (fakedb.CHResult, bool) {
		if strings.Contains(q, "FROM "+provenanceFillCleared) {
			return fakedb.CHResult{Cols: []fakedb.Col{{Column: "c", Type: "UInt64"}}, Rows: [][]any{{cleared()}}}, true
		}
		return fakedb.CHResult{}, false
	})
	return c, f
}

func never() uint64 { return 0 }

// The summary fill replays the views' own SELECTs one fractal at a time, records each
// fractal done, and records the summary filled only once every shard succeeded.
func TestBackfillProvenanceSummary(t *testing.T) {
	c, f := fillFake(nil, never)
	marked := 0
	if err := c.BackfillProvenancePaths(context.Background(), false, func(context.Context) error { marked++; return nil }); err != nil {
		t.Fatal(err)
	}
	inserts := f.ExecutedMatching("INSERT INTO provenance_summary ")
	if len(inserts) != 4 || marked != 1 {
		t.Fatalf("%d inserts, marked %d times:\n%s", len(inserts), marked, strings.Join(inserts, "\n"))
	}
	for i, view := range provenanceSummaryViews {
		sel, err := initViewSelect(view)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(inserts[i], "INSERT INTO provenance_summary "+sel+" SETTINGS ") {
			t.Errorf("%s not replayed verbatim", view)
		}
	}
	// The filter is a literal inside a literal: the fractal id is escaped for both.
	if want := `additional_table_filters = {'proc_lineage': 'fractal_id = ''o''''k''', 'proc_freq': 'fractal_id = ''o''''k'''}`; !strings.Contains(inserts[2], want) {
		t.Errorf("fractal filter: want %s in\n%s", want, inserts[2])
	}
	if !strings.Contains(inserts[0], "max_bytes_before_external_group_by") {
		t.Error("the fill must be able to spill")
	}
	if n := len(f.ExecutedMatching("INSERT INTO " + provenanceFillDone)); n != 2 {
		t.Errorf("%d fractals recorded done, want 2", n)
	}
	if drops := f.ExecutedMatching("DROP TABLE IF EXISTS `" + provenanceFillDone + "`"); len(drops) != 1 {
		t.Errorf("working tables not dropped once filled: %q", f.Executed())
	}
}

// A restarted fill skips fractals already done; a filled summary is not filled again.
func TestBackfillProvenanceSummaryResumes(t *testing.T) {
	c, f := fillFake([]string{"f1"}, never)
	if err := c.BackfillProvenancePaths(context.Background(), false, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if inserts := f.ExecutedMatching("INSERT INTO provenance_summary "); len(inserts) != 2 || !strings.Contains(inserts[0], "o''''k") {
		t.Errorf("resume refilled a done fractal:\n%s", strings.Join(inserts, "\n"))
	}

	c, f = fillFake(nil, never)
	if err := c.BackfillProvenancePaths(context.Background(), true, nil); err != nil || len(f.ExecutedMatching("INSERT")) != 0 {
		t.Errorf("a filled summary is not refilled: err %v", err)
	}
}

// A clear landing while a fractal's rows are read may leave stale rows behind, so that
// fractal's summary is dropped and filled again.
func TestBackfillProvenanceSummaryRefillsClearedFractal(t *testing.T) {
	calls := 0
	c, f := fillFake([]string{"o'k"}, func() uint64 {
		calls++
		if calls == 1 {
			return 1
		}
		return 0
	})
	if err := c.BackfillProvenancePaths(context.Background(), false, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if n := len(f.ExecutedMatching("INSERT INTO provenance_summary ")); n != 4 {
		t.Errorf("%d inserts, want the fractal filled twice", n)
	}
	if q := f.QueriedMatching("FROM " + provenanceFillCleared); len(q) == 0 || !strings.Contains(q[0], "fractal_id IN (?, ?) AND at >= ?") {
		t.Errorf("clear check %q", q)
	}
}

func TestBackfillProvenanceSummaryFailures(t *testing.T) {
	marked := 0
	mark := func(context.Context) error { marked++; return nil }

	// Missing views: rows from before them would never be summarized.
	c, f := newFakeClient()
	f.On("FROM system.tables WHERE database = currentDatabase() AND name IN", fakedb.CHResult{Cols: []fakedb.Col{{Column: "c", Type: "UInt64"}}, Rows: [][]any{{uint64(1)}}})
	if err := c.BackfillProvenancePaths(context.Background(), false, mark); err == nil || marked != 0 {
		t.Errorf("missing views: err %v, marked %d", err, marked)
	}

	// A failed insert is not recorded, and stops before the projections.
	c, f = fillFake(nil, never)
	f.FailExec("INSERT INTO provenance_summary", errors.New("memory"))
	if err := c.BackfillProvenancePaths(context.Background(), false, mark); err == nil || marked != 0 {
		t.Errorf("failed fill: err %v, marked %d", err, marked)
	}
	if len(f.QueriedMatching("system.parts")) != 0 {
		t.Error("projections must wait for the summary")
	}
}

// A clear tells a fill in progress which fractals to fill again; without one it does nothing.
func TestMarkProvenanceFillClear(t *testing.T) {
	_, f := newFakeClient()
	if err := markProvenanceFillClear(context.Background(), f, []string{"a"}); err != nil || len(f.Executed()) != 0 {
		t.Errorf("no fill running: err %v, executed %q", err, f.Executed())
	}
	_, f = newFakeClient()
	f.OnArg("FROM system.tables", provenanceFillCleared, fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "partition_key", Type: "String"}, {Column: "engine_full", Type: "String"}},
		Rows: [][]any{{"", "MergeTree ORDER BY fractal_id"}},
	})
	if err := markProvenanceFillClear(context.Background(), f, []string{"a", ""}); err != nil {
		t.Fatal(err)
	}
	if got := f.ExecutedMatching("INSERT INTO " + provenanceFillCleared + " SELECT ?, now64(3)"); len(got) != 2 {
		t.Errorf("recorded %q", f.Executed())
	}
	// The fill can finish and drop its table between the check and the insert.
	f.FailExec("INSERT INTO "+provenanceFillCleared, &clickhouse.Exception{Code: 60, Message: "unknown table"})
	if err := markProvenanceFillClear(context.Background(), f, []string{"a"}); err != nil {
		t.Errorf("a fill that just finished must not fail the clear: %v", err)
	}
}

// partState scripts system.parts and system.mutations across rounds: each partition lacks its
// projections until the mutation for it is issued, which then runs for one round.
type partState struct {
	lacking map[string][]string // "table partition" -> missing projections
	running map[string]int      // "table partition" -> rounds left
	fail    func(key string) string
	sticky  bool // a mutation completes without adding the projection
}

func (s *partState) respond(f *fakedb.CH) {
	f.Respond(func(q string, args []any) (fakedb.CHResult, bool) {
		for _, stmt := range f.Executed() {
			for key := range s.lacking {
				table, part, _ := strings.Cut(key, " ")
				if strings.HasPrefix(stmt, "ALTER TABLE "+table+" ") && strings.Contains(stmt, "PARTITION ID '"+part+"'") {
					if _, seen := s.running[key]; !seen {
						s.running[key] = 1
					}
				}
			}
			if strings.HasPrefix(stmt, "KILL MUTATION") {
				for key := range s.running {
					if strings.Contains(stmt, "'m-"+key+"'") {
						s.running[key] = 0
					}
				}
			}
		}
		switch {
		case q == "SELECT hostName()":
			return fakedb.Strs("h", "n1"), true
		case strings.Contains(q, "FROM system.mutations"):
			res := fakedb.CHResult{Cols: []fakedb.Col{{Column: "mutation_id", Type: "String"}, {Column: "table", Type: "String"},
				{Column: "command", Type: "String"}, {Column: "fail", Type: "String"}, {Column: "todo", Type: "Int64"}}}
			for key, left := range s.running {
				if left <= 0 {
					continue
				}
				table, part, _ := strings.Cut(key, " ")
				fail := ""
				if s.fail != nil {
					fail = s.fail(key)
				}
				res.Rows = append(res.Rows, []any{"m-" + key, table, "(MATERIALIZE PROJECTION p IN PARTITION ID '" + part + "')", fail, int64(1)})
				if fail == "" {
					s.running[key]--
					if s.running[key] == 0 && !s.sticky {
						delete(s.lacking, key)
					}
				}
			}
			return res, true
		case strings.Contains(q, "FROM system.parts"):
			table := args[0].(string)
			res := fakedb.CHResult{Cols: []fakedb.Col{{Column: "partition_id", Type: "String"}, {Column: "missing", Type: "Array(String)"}}}
			for key, missing := range s.lacking {
				if tbl, part, _ := strings.Cut(key, " "); tbl == table {
					res.Rows = append(res.Rows, []any{part, missing})
				}
			}
			return res, true
		}
		return fakedb.CHResult{}, false
	})
}

func materialize(t *testing.T, ctx context.Context, s *partState) (*fakedb.CH, error) {
	t.Helper()
	oldMin, oldMax := provenanceBackfillMinPoll, provenanceBackfillPoll
	provenanceBackfillMinPoll, provenanceBackfillPoll = 0, 0
	defer func() { provenanceBackfillMinPoll, provenanceBackfillPoll = oldMin, oldMax }()
	c, f := newFakeClient()
	s.respond(f)
	err := c.BackfillProvenancePaths(ctx, true, nil)
	return f, err
}

// Each node materializes one partition at a time, every projection it lacks in one
// mutation, and waits for that mutation before the next.
func TestBackfillProvenanceProjections(t *testing.T) {
	s := &partState{
		lacking: map[string][]string{"proc_lineage aa": {"by_parent", "by_image"}, "proc_freq bb": {"by_target"}},
		running: map[string]int{},
	}
	f, err := materialize(t, context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ALTER TABLE proc_lineage MATERIALIZE PROJECTION by_parent IN PARTITION ID 'aa', MATERIALIZE PROJECTION by_image IN PARTITION ID 'aa'",
		"ALTER TABLE proc_freq MATERIALIZE PROJECTION by_target IN PARTITION ID 'bb'",
	}
	if alters := f.ExecutedMatching("ALTER TABLE"); fmt.Sprint(alters) != fmt.Sprint(want) {
		t.Errorf("alters:\n%s\nwant:\n%s", strings.Join(alters, "\n"), strings.Join(want, "\n"))
	}
	if len(s.lacking) != 0 {
		t.Errorf("left %v", s.lacking)
	}
}

// A failure ClickHouse recovers from on retry is left alone.
func TestBackfillProvenanceProjectionsTransientFailure(t *testing.T) {
	seen := 0
	s := &partState{lacking: map[string][]string{"process_edges cc": {"by_target"}}, running: map[string]int{}, fail: func(string) string {
		if seen++; seen <= 2 {
			return "Memory limit exceeded"
		}
		return ""
	}}
	f, err := materialize(t, context.Background(), s)
	if err != nil || len(f.ExecutedMatching("KILL")) != 0 {
		t.Errorf("err %v, kills %q", err, f.ExecutedMatching("KILL"))
	}
}

// A mutation still failing after the limit is killed, its partition skipped while the rest
// proceed, and the run reports it so a later one retries.
func TestBackfillProvenanceProjectionsPersistentFailure(t *testing.T) {
	old := provenanceMutationFailLimit
	provenanceMutationFailLimit = 0
	defer func() { provenanceMutationFailLimit = old }()
	s := &partState{
		lacking: map[string][]string{"process_edges cc": {"by_target"}, "proc_freq dd": {"by_target"}},
		running: map[string]int{},
		fail: func(key string) string {
			if key == "process_edges cc" {
				return "Memory limit exceeded"
			}
			return ""
		},
	}
	f, err := materialize(t, context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "1 partitions could not be materialized") {
		t.Fatalf("err = %v", err)
	}
	if kills := f.ExecutedMatching("KILL MUTATION"); len(kills) != 1 || !strings.Contains(kills[0], "table = 'process_edges' AND mutation_id = 'm-process_edges cc'") {
		t.Errorf("kills %q", kills)
	}
	if _, left := s.lacking["proc_freq dd"]; left {
		t.Error("a failing partition must not hold up the others")
	}
}

// A partition still lacking a projection after its own mutation is skipped, not retried in a loop.
func TestBackfillProvenanceProjectionsNoProgress(t *testing.T) {
	s := &partState{lacking: map[string][]string{"proc_lineage aa": {"by_image"}}, running: map[string]int{}, sticky: true}
	f, err := materialize(t, context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "could not be materialized") {
		t.Fatalf("err = %v", err)
	}
	if n := len(f.ExecutedMatching("MATERIALIZE PROJECTION")); n != 1 {
		t.Errorf("issued %d mutations, want 1", n)
	}
}

// A cancelled backfill stops between rounds.
func TestBackfillProvenanceProjectionsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &partState{lacking: map[string][]string{"proc_lineage aa": {"by_image"}}, running: map[string]int{}, fail: func(string) string { cancel(); return "x" }}
	if _, err := materialize(t, ctx, s); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

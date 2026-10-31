package query

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
)

// spawnRow / leafRow build minimal edge maps in the provenanceColumns shape.
func spawnRow(parent, child string) map[string]interface{} {
	return map[string]interface{}{
		"parent": parent, "child": child, "label": "", "event_type": "spawn",
		"anomaly_score": 0.5, "edge_score": 0.5, "score_basis": "transition", "target_hosts": 2.0, "total_hosts": 4.0, "log_id": "l", "timestamp": "t", "fractal_id": "f",
		"command_line": "", "proc_user": "", "host": "h", "parent_label": "p.exe",
	}
}

func leafRow(parent, child string, score float64) map[string]interface{} {
	return map[string]interface{}{
		"parent": parent, "child": child, "label": "x", "event_type": "net_connect",
		"anomaly_score": score, "edge_score": score, "score_basis": "new_source", "target_hosts": 1.0, "total_hosts": 4.0, "log_id": "l", "timestamp": "t", "fractal_id": "f",
		"command_line": "", "proc_user": "", "host": "h", "parent_label": "",
	}
}

func TestEmitProvenanceSourceEmpty(t *testing.T) {
	if sql, n := emitProvenanceSource(nil, provenanceEmitMaxBytes); sql != provenanceEmptyScoreSQL || n != 0 {
		t.Fatalf("empty set should return the zero-row stub, got %q (%d)", sql, n)
	}
}

// One values() call, typed by provenanceColumns, one tuple per row, strings
// escaped so a hostile command line cannot break out of its literal.
func TestEmitProvenanceSourceShape(t *testing.T) {
	r := spawnRow("a", "b")
	r["command_line"] = `cmd /c "it's" \ x`
	rows := []map[string]interface{}{r, spawnRow("b", "c"), leafRow("b", "net:1.2.3.4", 0.9)}
	sql, n := emitProvenanceSource(rows, provenanceEmitMaxBytes)
	if n != len(rows) {
		t.Fatalf("emitted %d of %d", n, len(rows))
	}
	if !strings.HasPrefix(sql, "SELECT * FROM values('parent String, child String, label String, event_type String, anomaly_score Float64,") ||
		!strings.Contains(sql, "group_size Float64, group_first String, group_last String'") {
		t.Errorf("structure: %.300s", sql)
	}
	if got := strings.Count(sql, ", ('"); got != len(rows) {
		t.Errorf("%d tuples, want %d", got, len(rows))
	}
	if !strings.Contains(sql, `'cmd /c "it\'s" \\ x'`) {
		t.Errorf("command line not escaped: %s", sql)
	}
}

// Rows are emitted in priority order until the byte budget, and the count says
// how many fit; at least one row always goes out.
func TestEmitProvenanceSourceBudget(t *testing.T) {
	var rows []map[string]interface{}
	for i := 0; i < 100; i++ {
		rows = append(rows, spawnRow("root", fmt.Sprintf("c%03d", i)))
	}
	sql, n := emitProvenanceSource(rows, 2000)
	if n == 0 || n >= len(rows) || strings.Count(sql, ", ('") != n {
		t.Errorf("emitted %d rows under a 2000-byte budget", n)
	}
	if !strings.Contains(sql, "'c000'") {
		t.Error("the first rows in priority order must be the ones kept")
	}
	if _, n := emitProvenanceSource(rows[:1], 1); n != 1 {
		t.Error("one row is always emitted")
	}
}

// Under the limit everything goes; over it, structure follows walk order, a
// share is held for leaves, bridges go before leaves, and leaves of processes
// that are not drawn are skipped.
func TestBudgetProvenanceRows(t *testing.T) {
	tree := newProvenanceTree()
	for _, g := range []string{"root", "anc", "a", "b", "c", "d"} {
		tree.add([]string{g}, 1, 0, 0, parser.MaxProvenanceNodes)
	}
	bridge := leafRow("peer", "net:9.9.9.9", 0.5)
	bridge["score_basis"] = "reconnect"
	rows := []map[string]interface{}{
		spawnRow("root", "d"), spawnRow("root", "c"), spawnRow("root", "b"), spawnRow("root", "a"), spawnRow("anc", "root"),
		leafRow("d", "net:low", 0.1), leafRow("b", "net:hot", 0.99), bridge, leafRow("a", "net:mid", 0.8),
	}
	if got := budgetProvenanceRows(rows, tree, len(rows)); len(got) != len(rows) {
		t.Fatal("under the limit nothing is dropped")
	}
	keys := func(limit int) string {
		var out []string
		for _, r := range budgetProvenanceRows(rows, tree, limit) {
			out = append(out, reconString(r["parent"])+">"+reconString(r["child"]))
		}
		return fmt.Sprint(out)
	}
	// 5 rows, 1 held for leaves: the ancestor edge first, then a, b, c by walk
	// order, and the bridge takes the held slot.
	if got, want := keys(5), "[anc>root root>a root>b root>c peer>net:9.9.9.9]"; got != want {
		t.Errorf("got %v, want %s", got, want)
	}
	// 8 rows: all structure fits, then the bridge and the hottest leaves.
	if got, want := keys(8), "[anc>root root>a root>b root>c root>d peer>net:9.9.9.9 b>net:hot a>net:mid]"; got != want {
		t.Errorf("got %v, want %s", got, want)
	}
}

// A collapsed node's spawn row says how many processes it stands for and when
// they ran; single processes keep the defaults.
func TestAnnotateProvenanceGroups(t *testing.T) {
	tree := newProvenanceTree()
	tree.add([]string{"root"}, 1, 0, 0, parser.MaxProvenanceNodes)
	tree.add([]string{"cmd1", "cmd2", "cmd3"}, 47900, 1793088000000, 1793091600000, parser.MaxProvenanceNodes)
	group, single, leaf := spawnRow("root", "cmd1"), spawnRow("x", "root"), leafRow("cmd1", "net:1", 1)
	annotateProvenanceGroups([]map[string]interface{}{group, single, leaf}, tree)
	if group["group_size"] != 47900.0 || group["group_first"] != "2026-10-27 08:00:00.000" || group["group_last"] != "2026-10-27 09:00:00.000" {
		t.Errorf("group row %v", group)
	}
	if _, ok := single["group_size"]; ok {
		t.Error("a single process keeps the scoring SQL's defaults")
	}
	if _, ok := leaf["group_size"]; ok {
		t.Error("leaves are not annotated")
	}
}

// The scoring SQL's projection and provenanceColumns must stay in lockstep: emitProvenanceSource
// builds its literal rows from provenanceColumns, so a column added to one and not the other
// silently emits nulls (or a UNION type mismatch) rather than failing to compile.
func TestScoringSQLProjectsExactlyProvenanceColumns(t *testing.T) {
	opts := parser.QueryOptions{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		MaxRows:   100,
	}
	sql := parser.BuildProvenanceScoringSQL(0.7, nil, false, parser.ProvenanceBaseline{TotalHosts: 10}, opts)
	// Statement opens with a WITH block, so find the outer projection rather than slicing from 0.
	i := strings.Index(sql, "SELECT parent,")
	if i < 0 {
		t.Fatal("no outer projection found in scoring SQL")
	}
	head := sql[i+len("SELECT ") : i+strings.Index(sql[i:], " FROM (")]
	got := strings.Split(strings.ReplaceAll(head, " ", ""), ",")
	if len(got) != len(provenanceColumns) {
		t.Fatalf("scoring SQL projects %d columns, provenanceColumns has %d\n got: %v\nwant: %v",
			len(got), len(provenanceColumns), got, provenanceColumns)
	}
	for i, c := range provenanceColumns {
		if got[i] != c {
			t.Errorf("column %d: SQL projects %q, provenanceColumns says %q", i, got[i], c)
		}
	}
}

// span narrows the command-line lookup to the nodes' own creation times. A node whose earliest
// member was already taken is named by a later member, whose own time is unknown, so the span
// must give up rather than exclude it.
func TestProvenanceTreeSpan(t *testing.T) {
	tree := newProvenanceTree()
	tree.add([]string{"a"}, 1, 1000, 1000, parser.MaxProvenanceNodes)
	tree.add([]string{"b", "c"}, 2, 2000, 5000, parser.MaxProvenanceNodes)
	if s, e := tree.span(); s.UnixMilli() != 1000 || e.UnixMilli() != 2000 {
		t.Errorf("span %v..%v, want the nodes' own times 1000..2000", s, e)
	}
	tree.add([]string{"a", "d"}, 2, 900, 9000, parser.MaxProvenanceNodes) // "a" is taken: "d" names the node
	if s, e := tree.span(); !s.IsZero() || !e.IsZero() {
		t.Errorf("span %v..%v, want none once a node's own time is unknown", s, e)
	}
}

// The notice says which limit cut the rows, so its advice actually helps.
func TestProvenanceRowWarning(t *testing.T) {
	if w := provenanceRowWarning(8, 8, 8); w != "" {
		t.Errorf("nothing dropped, got %q", w)
	}
	if w := provenanceRowWarning(8, 4, 4); !strings.Contains(w, "row limit reached, showing 4 of 8 edges; raise limit=") {
		t.Errorf("limit= cut: %q", w)
	}
	if w := provenanceRowWarning(8, 8, 5); !strings.Contains(w, "showing 5 of 8 edges") || strings.Contains(w, "raise limit=") {
		t.Errorf("size cut must not advise raising limit=: %q", w)
	}
}

// Control characters and non-finite numbers from log data stay inside their literals.
func TestEmitProvenanceSourceEscapesControlAndNonFinite(t *testing.T) {
	r := spawnRow("a", "b")
	r["command_line"] = "x\x00y\nz\rw\tv"
	r["anomaly_score"] = math.NaN()
	r["edge_score"] = math.Inf(1)
	sql, _ := emitProvenanceSource([]map[string]interface{}{r}, provenanceEmitMaxBytes)
	if !strings.Contains(sql, `'x\0y\nz\rw\tv'`) || strings.ContainsAny(sql, "\x00\n\r\t") {
		t.Errorf("control characters not escaped: %q", sql)
	}
	if strings.Contains(sql, "NaN") || strings.Contains(sql, "Inf") {
		t.Errorf("non-finite numbers leaked: %s", sql)
	}
}

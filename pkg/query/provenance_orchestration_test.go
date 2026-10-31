package query

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// fakeLineage stands in for ClickHouse behind pgr()'s passes. It answers the
// tree-hop queries from an in-memory process list, grouping forward hops the way
// BuildProcessGroupHopSQL does, and every other pass from per-kind handlers, and
// records what it was asked. Passes are recognised by the shape of the SQL their
// parser builders emit. handler wires it behind a real ClickHouse client, so every
// pass runs through QueryProvenance with its external tables.
type fakeLineage struct {
	mu         sync.Mutex
	parentOf   map[string]string   // child -> parent, for processes that exist
	children   map[string][]string // parent -> children, in creation order
	image      map[string]string   // process -> normalized image (default: its guid)
	cmd        map[string]string   // process -> normalized command line
	createMs   map[string]int64    // process -> creation time
	failHop    map[string]bool     // frontier guid whose hop fails
	outOfRange map[string]bool     // process whose own row is outside the query window
	calls      []string

	reconTotals func() ([]map[string]interface{}, error)
	recon       func(sql string, tree storage.ExternalTable) ([]map[string]interface{}, error)
	totalHosts  func() ([]map[string]interface{}, error)
	edgeKeys    func(sql string) ([]map[string]interface{}, error)
	scan        func(sql string, tree storage.ExternalTable) ([]map[string]interface{}, error)
}

// newFakeLineage builds a lineage from "parent>child" pairs. A root is declared
// with ">root" (it exists as a process with no parent). Every process has its
// own image, so nothing collapses unless a test says so with same().
func newFakeLineage(edges ...string) *fakeLineage {
	f := &fakeLineage{parentOf: map[string]string{}, children: map[string][]string{}, image: map[string]string{},
		cmd: map[string]string{}, createMs: map[string]int64{}, failHop: map[string]bool{}, outOfRange: map[string]bool{}}
	for _, e := range edges {
		p, c, _ := strings.Cut(e, ">")
		f.addProc(p, c)
	}
	return f
}

func (f *fakeLineage) addProc(parent, child string) {
	f.parentOf[child] = parent
	if parent != "" {
		f.children[parent] = append(f.children[parent], child)
	}
	f.image[child], f.cmd[child] = child, child
	f.createMs[child] = int64(len(f.createMs) + 1)
}

// same gives processes one image and command line, so siblings among them collapse.
func (f *fakeLineage) same(image, cmd string, guids ...string) {
	for _, g := range guids {
		f.image[g], f.cmd[g] = image, cmd
	}
}

var backHopRE = regexp.MustCompile(`^SELECT process_guid, parent_guid, toUnixTimestamp64Milli\(timestamp\) AS ts_ms FROM \S+ WHERE .* process_guid = '([^']*)'$`)

func (f *fakeLineage) kinds() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for _, c := range f.calls {
		out[c]++
	}
	return out
}

func (f *fakeLineage) record(kind string) {
	f.mu.Lock()
	f.calls = append(f.calls, kind)
	f.mu.Unlock()
}

// handler returns a QueryHandler whose ClickHouse client answers from f.
func (f *fakeLineage) handler() *QueryHandler {
	return handlerAnswering(f.query)
}

// handlerAnswering returns a QueryHandler whose ClickHouse client answers every
// query with answer's rows, typed as ClickHouse columns so they scan back
// through the client's row mapper unchanged. tables holds the query's external
// tables by name.
func handlerAnswering(answer func(sql string, tables map[string]storage.ExternalTable) ([]map[string]interface{}, error)) *QueryHandler {
	db, ch := storagetest.ClickHouse()
	ch.Respond(func(sql string, args []any) (fakedb.CHResult, bool) {
		tables := map[string]storage.ExternalTable{}
		for _, a := range args {
			if t, ok := a.(storage.ExternalTable); ok {
				tables[t.Name] = t
			}
		}
		rows, err := answer(sql, tables)
		if err != nil {
			return fakedb.CHResult{Err: err}, true
		}
		return chResultOf(rows), true
	})
	return &QueryHandler{db: db}
}

// chResultOf turns row maps into a result set. Columns are the union of the
// rows' keys, typed by the first non-nil value; a row missing a column scans
// as that column's zero value.
func chResultOf(rows []map[string]interface{}) fakedb.CHResult {
	types := map[string]string{}
	for _, r := range rows {
		for k, v := range r {
			if _, ok := types[k]; ok || v == nil {
				continue
			}
			switch v.(type) {
			case string:
				types[k] = "String"
			case uint64:
				types[k] = "UInt64"
			case int64:
				types[k] = "Int64"
			case float64:
				types[k] = "Float64"
			case []string:
				types[k] = "Array(String)"
			case []uint64:
				types[k] = "Array(UInt64)"
			case []int64:
				types[k] = "Array(Int64)"
			default:
				panic(fmt.Sprintf("chResultOf: unsupported %T in column %s", v, k))
			}
		}
	}
	for _, r := range rows {
		for k := range r {
			if _, ok := types[k]; !ok {
				types[k] = "Nullable(String)"
			}
		}
	}
	var res fakedb.CHResult
	names := make([]string, 0, len(types))
	for k := range types {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		res.Cols = append(res.Cols, fakedb.Col{Column: k, Type: types[k]})
	}
	for _, r := range rows {
		vals := make([]any, len(names))
		for i, k := range names {
			vals[i] = r[k]
		}
		res.Rows = append(res.Rows, vals)
	}
	return res
}

// groupHop answers BuildProcessGroupHopSQL: the frontier members' children,
// grouped by (parent node, image, command line), earliest first.
func (f *fakeLineage) groupHop(frontier storage.ExternalTable) ([]map[string]interface{}, error) {
	type group struct {
		node, image, cmd string
		members          []string
	}
	var groups []*group
	byKey := map[string]*group{}
	for _, fr := range frontier.Rows {
		g, node := fr[0], fr[1]
		if f.failHop[g] {
			return nil, fmt.Errorf("hop failed at %s", g)
		}
		for _, c := range f.children[g] {
			key := node + "\x00" + f.image[c] + "\x00" + f.cmd[c]
			gr := byKey[key]
			if gr == nil {
				gr = &group{node: node, image: f.image[c], cmd: f.cmd[c]}
				byKey[key] = gr
				groups = append(groups, gr)
			}
			gr.members = append(gr.members, c)
		}
	}
	var rows []map[string]interface{}
	for _, gr := range groups {
		sort.Slice(gr.members, func(i, j int) bool { return f.createMs[gr.members[i]] < f.createMs[gr.members[j]] })
		rows = append(rows, map[string]interface{}{
			"parent_node": gr.node, "nimg": gr.image, "ncmd": gr.cmd, "rep": gr.members[0],
			"n": uint64(len(gr.members)), "members": gr.members,
			"first_ms": f.createMs[gr.members[0]], "last_ms": f.createMs[gr.members[len(gr.members)-1]],
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i]["first_ms"].(int64) < rows[j]["first_ms"].(int64) })
	return rows, nil
}

func (f *fakeLineage) query(sql string, tables map[string]storage.ExternalTable) ([]map[string]interface{}, error) {
	if m := backHopRE.FindStringSubmatch(sql); m != nil {
		f.record("hop")
		g := m[1]
		if f.failHop[g] {
			return nil, fmt.Errorf("hop failed at %s", g)
		}
		if p, ok := f.parentOf[g]; ok {
			return []map[string]interface{}{{"process_guid": g, "parent_guid": p, "ts_ms": f.createMs[g]}}, nil
		}
		return nil, nil
	}
	if strings.HasPrefix(sql, "SELECT process_guid, parent_guid, toString(timestamp) AS ts") {
		f.record("rows")
		var rows []map[string]interface{}
		for _, r := range tables[parser.ProvenanceTreeTable].Rows {
			g := r[0]
			if p, ok := f.parentOf[g]; ok && !f.outOfRange[g] {
				rows = append(rows, map[string]interface{}{"process_guid": g, "parent_guid": p, "ts": fmt.Sprintf("2026-10-01 00:00:%02d.000", f.createMs[g]%60),
					"log_id": "log-" + g, "fractal_id": "f1", "image": f.image[g], "parent_image": "", "commandline": f.cmd[g], "computer_name": "WS02"})
			}
		}
		return rows, nil
	}
	if strings.HasPrefix(sql, "SELECT f.node AS parent_node") {
		f.record("hop")
		return f.groupHop(tables[parser.ProvenanceFrontierTable])
	}
	call := func(kind string, h func() ([]map[string]interface{}, error)) ([]map[string]interface{}, error) {
		f.record(kind)
		if h == nil {
			return nil, nil
		}
		return h()
	}
	tree := tables[parser.ProvenanceTreeTable]
	switch {
	case strings.HasPrefix(sql, "SELECT length(groupUniqArrayMerge("):
		return call("recon_totals", f.reconTotals)
	case strings.HasPrefix(sql, "SELECT uniqExact(computer_name) AS total_hosts"):
		return call("total_hosts", f.totalHosts)
	case strings.HasPrefix(sql, "SELECT DISTINCT fkey_src, fkey_tgt FROM"):
		return call("edge_keys", func() ([]map[string]interface{}, error) {
			if f.edgeKeys == nil {
				return nil, nil
			}
			return f.edgeKeys(sql)
		})
	case strings.HasPrefix(sql, "SELECT * FROM (") && strings.Contains(sql, ") AS _d ORDER BY"):
		return call("scan", func() ([]map[string]interface{}, error) {
			if f.scan == nil {
				return nil, nil
			}
			return f.scan(sql, tree)
		})
	default:
		return call("recon", func() ([]map[string]interface{}, error) {
			if f.recon == nil {
				return nil, nil
			}
			return f.recon(sql, tree)
		})
	}
}

func pgrOpts() parser.QueryOptions {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	return parser.QueryOptions{FractalID: "f1", StartTime: start, EndTime: start.Add(24 * time.Hour), TableName: "logs", MaxRows: 1000}
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

// collect runs pass 1 against f with the default node cap.
func (f *fakeLineage) collect(t *testing.T, p parser.ProvenanceParams) *provenanceTree {
	t.Helper()
	tree, err := collectProvenanceTree(context.Background(), f.handler().db, p, pgrOpts(), parser.MaxProvenanceNodes)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func TestCollectProvenanceTree(t *testing.T) {
	// up -> root -> a -> b -> c, root -> d.
	lineage := func() *fakeLineage {
		return newFakeLineage(">up", "up>root", "root>a", "a>b", "b>c", "root>d")
	}
	cases := []struct {
		name      string
		start     string
		depth     int
		direction string
		want      []string
	}{
		{"forward", "a", 5, "forward", []string{"a", "b", "c"}},
		{"forward depth-limited", "root", 1, "forward", []string{"root", "a", "d"}},
		{"backward walks parents", "b", 5, "backward", []string{"b", "a", "root", "up"}},
		{"both directions, ancestors first", "a", 1, "both", []string{"a", "root", "b"}},
		{"unknown direction means both", "a", 1, "", []string{"a", "root", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := lineage().collect(t, parser.ProvenanceParams{Start: tc.start, Depth: tc.depth, Direction: tc.direction})
			if fmt.Sprint(tree.order) != fmt.Sprint(tc.want) {
				t.Errorf("got %v, want %v", tree.order, tc.want)
			}
			if tree.truncated {
				t.Error("a small tree is not truncated")
			}
		})
	}
}

// A polling loop: the seed runs `cmd /c tasklist | findstr` every few seconds,
// and each cmd starts its own tasklist. Identical siblings collapse into one
// node, a group's children are the union of its members' children, and a
// distinct command line or a unique grandchild under one member stays its own
// node, attached to the group.
func TestCollectProvenanceTreeCollapsesRepeatedChildren(t *testing.T) {
	f := newFakeLineage(">root")
	var cmds, lists []string
	for i := 0; i < 300; i++ {
		c, l := fmt.Sprintf("cmd%03d", i), fmt.Sprintf("list%03d", i)
		f.addProc("root", c)
		f.addProc(c, l)
		cmds, lists = append(cmds, c), append(lists, l)
	}
	f.same("cmd.exe", "cmd /c tasklist", cmds...)
	f.same("tasklist.exe", "tasklist", lists...)
	f.addProc("root", "netuse1")
	f.same("net.exe", "net use \\\\dc01 /user:alice", "netuse1")
	f.addProc("root", "netuse2")
	f.same("net.exe", "net use \\\\dc01 /user:bob", "netuse2")
	f.addProc("cmd137", "evil") // one member's unique child

	tree := f.collect(t, parser.ProvenanceParams{Start: "root", Depth: 5, Direction: "forward"})
	want := []string{"root", "cmd000", "netuse1", "netuse2", "list000", "evil"}
	if fmt.Sprint(tree.order) != fmt.Sprint(want) {
		t.Fatalf("nodes %v, want %v", tree.order, want)
	}
	if tree.size["cmd000"] != 300 || tree.size["list000"] != 300 || tree.size["netuse1"] != 1 {
		t.Errorf("sizes cmd=%d list=%d netuse1=%d", tree.size["cmd000"], tree.size["list000"], tree.size["netuse1"])
	}
	if tree.node("cmd137") != "cmd000" || tree.node("list299") != "list000" || tree.node("evil") != "evil" {
		t.Error("members must map onto their group's node")
	}
	if tree.nMember != 1+300+300+2+1 || tree.truncated {
		t.Errorf("%d members (truncated=%v); every process is kept behind its node", tree.nMember, tree.truncated)
	}
	if first, last := tree.firstMs["cmd000"], tree.lastMs["cmd000"]; first != f.createMs["cmd000"] || last != f.createMs["cmd299"] {
		t.Errorf("group span %d..%d", first, last)
	}

	tbl := tree.table()
	if tbl.Name != parser.ProvenanceTreeTable || len(tbl.Rows) != tree.nMember {
		t.Fatalf("tree table %s has %d rows", tbl.Name, len(tbl.Rows))
	}
	nodeOf := map[string]string{}
	for _, r := range tbl.Rows {
		nodeOf[r[0]] = r[1]
	}
	if nodeOf["cmd137"] != "cmd000" || nodeOf["evil"] != "evil" || nodeOf["root"] != "root" {
		t.Error("the tree table must map every member onto its node, and each node onto itself")
	}
}

// Ancestors get their own budget: they are walked before the descendants, so a
// subtree that fills the node cap can no longer push them out.
func TestCollectProvenanceTreeAncestorsSurviveWideSubtree(t *testing.T) {
	f := newFakeLineage(">explorer", "explorer>root")
	for i := 0; i < parser.MaxProvenanceNodes+50; i++ {
		f.addProc("root", fmt.Sprintf("c%05d", i)) // all distinct, nothing collapses
	}
	tree := f.collect(t, parser.ProvenanceParams{Start: "root", Depth: 3})
	if len(tree.order) != parser.MaxProvenanceNodes || !tree.truncated {
		t.Fatalf("collected %d nodes (truncated=%v), want the cap %d", len(tree.order), tree.truncated, parser.MaxProvenanceNodes)
	}
	if tree.order[1] != "explorer" {
		t.Errorf("the ancestor must be walked first, got %v", tree.order[:3])
	}
}

// Malformed lineage (a guid reused as its own ancestor) must terminate: the
// seen-guard visits each guid once in either direction.
func TestCollectProvenanceTreeCycle(t *testing.T) {
	for _, dir := range []string{"forward", "backward"} {
		f := newFakeLineage("x>y", "y>x")
		tree := f.collect(t, parser.ProvenanceParams{Start: "x", Depth: 50, Direction: dir})
		if fmt.Sprint(tree.order) != "[x y]" {
			t.Errorf("%s: got %v", dir, tree.order)
		}
		if n := f.kinds()["hop"]; n > 3 {
			t.Errorf("%s: %d hops; the walk kept going after the frontier emptied", dir, n)
		}
	}
}

// A guid seen only in leaf logs (never as a process_creation) is not a tree
// anchor: no traversal, no scoring on a phantom root.
func TestCollectProvenanceTreeMissingSeed(t *testing.T) {
	f := newFakeLineage(">root", "root>a")
	if tree := f.collect(t, parser.ProvenanceParams{Start: "ghost", Depth: 5}); tree != nil {
		t.Fatalf("got %v; want no tree", tree.order)
	}
	if n := f.kinds()["hop"]; n != 1 {
		t.Errorf("%d hop queries for a missing seed, want only the seed check", n)
	}
}

func TestCollectProvenanceTreeErrors(t *testing.T) {
	f := newFakeLineage(">root", "root>a", "a>b")
	f.failHop["a"] = true
	if _, err := collectProvenanceTree(context.Background(), f.handler().db, parser.ProvenanceParams{Start: "root", Depth: 5, Direction: "forward"}, pgrOpts(), parser.MaxProvenanceNodes); err == nil {
		t.Error("a failed hop must fail the traversal, not return a truncated tree")
	}

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	cancelAfterSeed := handlerAnswering(func(string, map[string]storage.ExternalTable) ([]map[string]interface{}, error) {
		calls++
		cancel()
		return []map[string]interface{}{{"process_guid": "root", "parent_guid": "up"}}, nil
	})
	if _, err := collectProvenanceTree(ctx, cancelAfterSeed.db,
		parser.ProvenanceParams{Start: "root", Depth: 5}, pgrOpts(), parser.MaxProvenanceNodes); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("%d queries after cancellation, want none past the seed", calls-1)
	}
}

func peer(guid string, anomaly float64) parser.ReconnectPeer {
	return parser.ReconnectPeer{ReconType: "net", PeerGUID: guid, SrcGUID: "root", ObjectID: "net:" + guid, Label: guid, Anomaly: anomaly}
}

// baseTree is root -> a, collected from f (which must contain it).
func baseTree(t *testing.T, f *fakeLineage) *provenanceTree {
	t.Helper()
	tree := newProvenanceTree()
	tree.add([]string{"root"}, 1, 0, 0, parser.MaxProvenanceNodes)
	tree.add([]string{"a"}, 1, 0, 0, parser.MaxProvenanceNodes)
	return tree
}

func TestExpandReconnectionPeers(t *testing.T) {
	t.Run("nothing severe enough expands", func(t *testing.T) {
		f := newFakeLineage()
		tree := baseTree(t, f)
		f.handler().expandReconnectionPeers(context.Background(), tree, []parser.ReconnectPeer{peer("p1", 0.79)}, pgrOpts())
		if fmt.Sprint(tree.order) != "[root a]" || len(f.kinds()) != 0 {
			t.Errorf("got %v with queries %v; want the tree unchanged and no queries", tree.order, f.kinds())
		}
	})

	t.Run("top five by severity then guid, merged in rank order", func(t *testing.T) {
		f := newFakeLineage(">p1", "p1>p1c", ">p2", "p2>a", ">p3", ">p4", ">p5", ">p6", ">p7", "p7>p7c")
		tree := baseTree(t, f)
		peers := []parser.ReconnectPeer{
			peer("p6", 0.85), peer("p5", 0.85), peer("p4", 0.85),
			peer("p3", 0.9), peer("p2", 0.85), peer("p1", 0.85),
			peer("p1", 1.0), // a peer's best row ranks it
			peer("p7", 0.5), // below the expansion bar
		}
		f.handler().expandReconnectionPeers(context.Background(), tree, peers, pgrOpts())
		// Rank: p1 (1.0), p3 (0.9), then 0.85 ties by guid: p2, p4, p5. p6 is sixth.
		want := []string{"root", "a", "p1", "p1c", "p3", "p2", "p4", "p5"}
		if fmt.Sprint(tree.order) != fmt.Sprint(want) {
			t.Errorf("got %v, want %v (p2's child a is already in the tree)", tree.order, want)
		}
	})

	t.Run("a failed expansion is skipped, not fatal", func(t *testing.T) {
		f := newFakeLineage(">p1", "p1>p1c", ">p2", "p2>p2c")
		f.failHop["p1"] = true
		tree := baseTree(t, f)
		f.handler().expandReconnectionPeers(context.Background(), tree, []parser.ReconnectPeer{peer("p1", 1), peer("p2", 1)}, pgrOpts())
		if fmt.Sprint(tree.order) != "[root a p2 p2c]" {
			t.Errorf("got %v", tree.order)
		}
	})

	t.Run("node cap", func(t *testing.T) {
		f := newFakeLineage(">p1", "p1>x", "p1>y")
		tree := newProvenanceTree()
		for i := 0; i < reconnectExpandMaxNodes-1; i++ {
			tree.add([]string{fmt.Sprintf("t%04d", i)}, 1, 0, 0, reconnectExpandMaxNodes)
		}
		f.handler().expandReconnectionPeers(context.Background(), tree, []parser.ReconnectPeer{peer("p1", 1)}, pgrOpts())
		if len(tree.order) != reconnectExpandMaxNodes {
			t.Errorf("expanded to %d nodes, want the cap %d", len(tree.order), reconnectExpandMaxNodes)
		}

		f = newFakeLineage(">p1", "p1>x")
		before := len(tree.order)
		f.handler().expandReconnectionPeers(context.Background(), tree, []parser.ReconnectPeer{peer("p1", 1)}, pgrOpts())
		if len(tree.order) != before || len(f.kinds()) != 0 {
			t.Errorf("a tree already at the cap must not expand: %d nodes, queries %v", len(tree.order), f.kinds())
		}
	})

	t.Run("cancelled request does not expand", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f := newFakeLineage(">p1", "p1>x")
		tree := baseTree(t, f)
		f.handler().expandReconnectionPeers(ctx, tree, []parser.ReconnectPeer{peer("p1", 1)}, pgrOpts())
		if len(tree.order) != 2 || len(f.kinds()) != 0 {
			t.Errorf("got %v with queries %v", tree.order, f.kinds())
		}
	})
}

func pgrParams(diffuse, reconnect bool) parser.ProvenanceParams {
	return parser.ProvenanceParams{Start: "root", Depth: 5, Direction: "forward", Threshold: 0.7, Diffuse: diffuse, Reconnect: reconnect, Lambda: 0.2, Limit: 500}
}

// scoreOptsFor mirrors the scoring builds' row budget: pgr()'s own limit, not the
// deployment cap.
func scoreOptsFor(p parser.ProvenanceParams) parser.QueryOptions {
	o := pgrOpts()
	o.MaxRows = p.Limit
	return o
}

// scanOf is the statement pgr runs to score: the scoring SQL, spawn first, bounded.
func scanOf(scoring string) string {
	return fmt.Sprintf("SELECT * FROM (%s) AS _d ORDER BY (event_type = 'spawn') DESC, anomaly_score DESC LIMIT %d", scoring, provenanceMaxScanRows)
}

// treeRows renders an external tree table as "guid>node" pairs.
func treeRows(tbl storage.ExternalTable) []string {
	var out []string
	for _, r := range tbl.Rows {
		out = append(out, r[0]+">"+r[1])
	}
	return out
}

func TestProvenanceScoreSQLRequiresScope(t *testing.T) {
	f := newFakeLineage(">root")
	_, _, err := f.handler().provenanceScoreSQL(context.Background(), pgrParams(false, false), parser.QueryOptions{})
	if err == nil || !strings.Contains(err.Error(), "fractal scoping is required") {
		t.Fatalf("err = %v", err)
	}
	if len(f.kinds()) != 0 {
		t.Errorf("an unscoped pgr() queried %v", f.kinds())
	}
}

func TestProvenanceScoreSQLEmptyTree(t *testing.T) {
	f := newFakeLineage(">other")
	sql, warnings, err := f.handler().provenanceScoreSQL(context.Background(), pgrParams(true, true), pgrOpts())
	if err != nil || sql != provenanceEmptyScoreSQL || warnings != nil {
		t.Fatalf("got %q, %v, %v; want the zero-row stub", sql, warnings, err)
	}
	if k := f.kinds(); k["hop"] != 1 || len(k) != 1 {
		t.Errorf("queries %v; an empty tree must stop after the seed check", k)
	}
}

// Per-edge scoring (diffuse=false): pass 2 is built from the fleet size and the
// tree's own edge keys under pgr()'s row budget, runs with the tree attached,
// and its rows come back as a literal source.
func TestProvenanceScoreSQLPerEdge(t *testing.T) {
	p := pgrParams(false, false)
	f := newFakeLineage(">root", "root>a", "a>b")
	f.totalHosts = func() ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"total_hosts": uint64(12)}}, nil
	}
	f.edgeKeys = func(string) ([]map[string]interface{}, error) {
		return []map[string]interface{}{
			{"fkey_src": "s1", "fkey_tgt": "t1"},
			{"fkey_src": "s1", "fkey_tgt": "t2"},
			{"fkey_src": "", "fkey_tgt": "t1"},
		}, nil
	}
	var scanned string
	var tree []string
	f.scan = func(sql string, tbl storage.ExternalTable) ([]map[string]interface{}, error) {
		scanned, tree = sql, treeRows(tbl)
		return []map[string]interface{}{spawnRow("root", "a"), spawnRow("a", "b"), leafRow("b", "net:203.0.113.9", 0.9)}, nil
	}
	got, warnings, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	start, end := time.UnixMilli(f.createMs["root"]).UTC(), time.UnixMilli(f.createMs["b"]).UTC()
	want := parser.BuildProvenanceScoringSQL(p.Threshold, p.EdgeTypes, false,
		parser.ProvenanceBaseline{TotalHosts: 12, SrcKeys: []string{"s1", ""}, TgtKeys: []string{"t1", "t2"}, NodeStart: start, NodeEnd: end}, scoreOptsFor(p))
	if scanned != scanOf(want) {
		t.Errorf("scoring scan differs from the expected build\n got: %.300s\nwant: %.300s", scanned, scanOf(want))
	}
	if fmt.Sprint(tree) != "[root>root a>a b>b]" {
		t.Errorf("scan ran with tree %v", tree)
	}
	if !strings.HasPrefix(got, "SELECT * FROM values('parent String,") || !strings.Contains(got, "'net:203.0.113.9'") || warnings != nil {
		t.Errorf("rows were not re-emitted as a literal source (warnings %v):\n%.300s", warnings, got)
	}
	if k := f.kinds(); k["recon"]+k["recon_totals"] != 0 || k["scan"] != 1 {
		t.Errorf("reconnect and diffuse are off, yet ran %v", k)
	}
}

// The baseline lookups are optional: a failure or an oversized key set falls
// back to the unrestricted baseline, which is slower but scores identically. A
// truncated key set would not.
func TestProvenanceScoreSQLBaselineFallbacks(t *testing.T) {
	p := pgrParams(false, false)
	cases := map[string]func() ([]map[string]interface{}, error){
		"lookup fails": func() ([]map[string]interface{}, error) { return nil, errors.New("boom") },
		"too many keys": func() ([]map[string]interface{}, error) {
			rows := make([]map[string]interface{}, parser.MaxProvenanceEdgeKeys+1)
			for i := range rows {
				rows[i] = map[string]interface{}{"fkey_src": fmt.Sprint(i), "fkey_tgt": "t"}
			}
			return rows, nil
		},
	}
	for name, keys := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeLineage(">root", "root>a")
			f.totalHosts = func() ([]map[string]interface{}, error) { return nil, errors.New("hosts down") }
			f.edgeKeys = func(string) ([]map[string]interface{}, error) { return keys() }
			var scanned string
			f.scan = func(sql string, _ storage.ExternalTable) ([]map[string]interface{}, error) {
				scanned = sql
				return nil, nil
			}
			if _, _, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts()); err != nil {
				t.Fatal(err)
			}
			bl := parser.ProvenanceBaseline{NodeStart: time.UnixMilli(1).UTC(), NodeEnd: time.UnixMilli(2).UTC()}
			if want := scanOf(parser.BuildProvenanceScoringSQL(p.Threshold, p.EdgeTypes, false, bl, scoreOptsFor(p))); scanned != want {
				t.Error("fallback did not score against the unrestricted baseline")
			}
		})
	}
}

// Diffusion scores with leaves down to a floor, propagates in Go, and re-emits
// the survivors.
func TestProvenanceScoreSQLDiffusion(t *testing.T) {
	p := pgrParams(true, false)
	var scanned string
	f := newFakeLineage(">root", "root>a")
	f.scan = func(sql string, _ storage.ExternalTable) ([]map[string]interface{}, error) {
		scanned = sql
		return []map[string]interface{}{spawnRow("root", "a"), leafRow("a", "net:203.0.113.9", 1), leafRow("a", "dns:example.test", 0)}, nil
	}
	got, _, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	bl := parser.ProvenanceBaseline{NodeStart: time.UnixMilli(1).UTC(), NodeEnd: time.UnixMilli(2).UTC()}
	if want := scanOf(parser.BuildProvenanceScoringSQL(p.Threshold, p.EdgeTypes, true, bl, scoreOptsFor(p))); scanned != want {
		t.Errorf("diffusion scan is not the wrapped, bounded diffuse scoring SQL:\n%.400s", scanned)
	}
	if !strings.Contains(got, "'net:203.0.113.9'") || strings.Contains(got, "dns:example.test") {
		t.Errorf("survivors were not re-emitted, or a sub-threshold leaf survived:\n%.400s", got)
	}
}

// Diffusion is best-effort: a failed scan falls back to per-edge scoring, so
// pgr() still returns the graph. A cancelled request is not a failure to paper
// over, and a per-edge failure is an error.
func TestProvenanceScoreSQLDiffusionFallbacks(t *testing.T) {
	p := pgrParams(true, false)
	bl := parser.ProvenanceBaseline{NodeStart: time.UnixMilli(1).UTC(), NodeEnd: time.UnixMilli(2).UTC()}
	perEdge := scanOf(parser.BuildProvenanceScoringSQL(p.Threshold, p.EdgeTypes, false, bl, scoreOptsFor(p)))

	t.Run("diffusion scan fails", func(t *testing.T) {
		f := newFakeLineage(">root", "root>a")
		f.scan = func(sql string, _ storage.ExternalTable) ([]map[string]interface{}, error) {
			if sql != perEdge {
				return nil, errors.New("code 241")
			}
			return []map[string]interface{}{spawnRow("root", "a")}, nil
		}
		got, _, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
		if err != nil || !strings.Contains(got, "'root', 'a'") || f.kinds()["scan"] != 2 {
			t.Errorf("got err=%v, scans=%d, sql=%.200s", err, f.kinds()["scan"], got)
		}
	})

	t.Run("both scans fail", func(t *testing.T) {
		f := newFakeLineage(">root", "root>a")
		f.scan = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
			return nil, errors.New("code 241")
		}
		if _, _, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts()); err == nil {
			t.Error("a failed per-edge scan must fail pgr()")
		}
	})

	t.Run("cancelled during the scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		f := newFakeLineage(">root", "root>a")
		f.scan = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
			cancel()
			return nil, context.Canceled
		}
		if _, _, err := f.handler().provenanceScoreSQL(ctx, p, pgrOpts()); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want the cancellation", err)
		}
		if f.kinds()["scan"] != 1 {
			t.Error("a cancelled request must not retry")
		}
	})
}

// Reconnection: the rarity-gate totals are read once and passed into the
// reverse lookup as literals, which runs against the tree's members. Severe
// peers are expanded into the scored tree, and bridges attach to the node of
// whichever member touched the artifact.
func TestProvenanceScoreSQLReconnection(t *testing.T) {
	p := pgrParams(false, true)
	opts := pgrOpts()
	var reconSQL string
	var reconTree, scanTree []string
	f := newFakeLineage(">root", "root>a", "root>a2", ">peer", "peer>peerkid")
	f.same("w.exe", "w.exe --poll", "a", "a2")
	f.reconTotals = func() ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"total_hosts": uint64(40), "total_images": uint64(9)}}, nil
	}
	f.recon = func(sql string, tbl storage.ExternalTable) ([]map[string]interface{}, error) {
		reconSQL, reconTree = sql, treeRows(tbl)
		return []map[string]interface{}{
			{"recon_type": "net", "peer_guid": "peer", "src_guid": "a2", "object_id": "net:198.51.100.7", "label": "198.51.100.7", "anomaly": 0.85, "peer_image": "evil.exe"},
			{"recon_type": "net", "peer_guid": "", "src_guid": "a", "object_id": "net:x"}, // no peer: dropped
		}, nil
	}
	f.scan = func(_ string, tbl storage.ExternalTable) ([]map[string]interface{}, error) {
		scanTree = treeRows(tbl)
		return []map[string]interface{}{spawnRow("root", "a"), spawnRow("peer", "peerkid")}, nil
	}
	got, _, err := f.handler().provenanceScoreSQL(context.Background(), p, opts)
	if err != nil {
		t.Fatal(err)
	}
	if reconSQL != parser.BuildReconnectionSQL(p, 40, 9, opts) {
		t.Error("reverse lookup was not built from the fetched rarity-gate totals")
	}
	if fmt.Sprint(reconTree) != "[root>root a>a a2>a]" {
		t.Errorf("reverse lookup ran against %v", reconTree)
	}
	if fmt.Sprint(scanTree) != "[root>root a>a a2>a peer>peer peerkid>peerkid]" {
		t.Errorf("scoring did not cover the expanded peer: %v", scanTree)
	}
	for _, want := range []string{"('a', 'net:198.51.100.7', '198.51.100.7', 'reconnect_net'", "('peer', 'net:198.51.100.7', '198.51.100.7', 'reconnect_net'"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing bridge %s in:\n%.600s", want, got)
		}
	}
	// The peer has no host to compare, so the bridge is context, and the role rides the row.
	if !strings.Contains(got, "'evil.exe', 1, '', '', 'context')") {
		t.Errorf("bridge rows must carry their reconnection role:\n%.900s", got)
	}
	if !strings.Contains(got, "'root', 'a', '', 'spawn', 0.5, 0.5, 'transition', '', 0, 0, 0, 0, 2, 4, 'l', 't', 'f', '', '', 'h', 'p.exe', 2, '") {
		t.Errorf("the collapsed node's spawn row must carry its size:\n%.600s", got)
	}
}

func TestProvenanceScoreSQLReconnectionFailures(t *testing.T) {
	p := pgrParams(false, true)

	t.Run("feature failure falls back to the base tree", func(t *testing.T) {
		f := newFakeLineage(">root", "root>a")
		f.reconTotals = func() ([]map[string]interface{}, error) { return nil, errors.New("proc_freq missing") }
		f.recon = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
			return nil, errors.New("code 36")
		}
		f.scan = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
			return []map[string]interface{}{spawnRow("root", "a")}, nil
		}
		got, _, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
		if err != nil || !strings.Contains(got, "'root', 'a'") || strings.Contains(got, "reconnect_") {
			t.Errorf("err=%v, sql=%.200s", err, got)
		}
		if f.kinds()["recon"] != 1 {
			t.Error("a failed totals lookup must still attempt reconnection at the floor gate")
		}
	})

	t.Run("cancellation is surfaced", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		f := newFakeLineage(">root", "root>a")
		f.recon = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
			cancel()
			return nil, context.Canceled
		}
		if _, _, err := f.handler().provenanceScoreSQL(ctx, p, pgrOpts()); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want the cancellation", err)
		}
	})
}

// The row limit and the node cap interact: a tree with more spawn edges than
// limit= used to return spawn rows only. Now leaves keep their share of the
// budget, and pgr says what it left out.
func TestProvenanceScoreSQLLimitKeepsLeaves(t *testing.T) {
	p := pgrParams(false, false)
	p.Limit = 40
	f := newFakeLineage(">root")
	var rows []map[string]interface{}
	for i := 0; i < 60; i++ {
		c := fmt.Sprintf("c%02d", i)
		f.addProc("root", c)
		rows = append(rows, spawnRow("root", c))
	}
	rows = append(rows, leafRow("c03", "net:203.0.113.9", 0.95), leafRow("c59", "file:x", 0.99))
	f.scan = func(string, storage.ExternalTable) ([]map[string]interface{}, error) { return rows, nil }
	got, warnings, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "'net:203.0.113.9'") {
		t.Error("a leaf of a drawn process must survive the row limit")
	}
	if strings.Contains(got, "'file:x'") {
		t.Error("a leaf of a process that is not drawn must not be emitted")
	}
	if n := strings.Count(got, "'spawn'"); n != 39 {
		t.Errorf("%d spawn rows; the rest of the budget goes back to structure", n)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "showing 40 of 62 edges") {
		t.Errorf("warnings %v", warnings)
	}
}

// pgrCommand parses q and returns its source command, so arguments are bound the
// way the parser binds them for a real query.
func pgrCommand(t *testing.T, q string) parser.CommandNode {
	t.Helper()
	pipeline, err := parser.ParseQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	cmd, ok := parser.FirstSourceCommand(pipeline)
	if !ok {
		t.Fatalf("%q has no source command", q)
	}
	return cmd
}

func TestResolvePgrSourceRefusals(t *testing.T) {
	ok := pgrCommand(t, `pgr(start="g1")`)
	iceberg := pgrOpts()
	iceberg.SourceMode = parser.SourceIceberg
	if _, err := resolvePgrSource(&QueryHandler{}, context.Background(), ok, iceberg); err == nil || !strings.Contains(err.Error(), "archived data") {
		t.Errorf("archive search: err = %v", err)
	}
	if _, err := resolvePgrSource(&QueryHandler{}, context.Background(), pgrCommand(t, `pgr()`), pgrOpts()); !errors.Is(err, errPgrNeedsStart) {
		t.Errorf("no start: err = %v", err)
	}
	if _, err := (&QueryHandler{}).resolveSourceCommand(context.Background(), parser.CommandNode{Name: "nope"}, pgrOpts()); err == nil {
		t.Error("an unregistered source command resolved")
	}
}

// The validation stand-in must expose exactly what the resolved source does, or
// a query valid in the editor fails at execution (or the reverse).
func TestStubPgrSourceMatchesResolvedShape(t *testing.T) {
	rs, err := stubPgrSource(pgrCommand(t, `pgr(start="g1", limit=40)`))
	if err != nil {
		t.Fatal(err)
	}
	if rs.SQL != provenanceEmptyScoreSQL || rs.Focus != "g1" || rs.Limit != 40 ||
		fmt.Sprint(rs.Columns) != fmt.Sprint(provenanceColumns) ||
		fmt.Sprint(rs.NumericColumns) != fmt.Sprint(provenanceNumericColumns) ||
		fmt.Sprint(rs.OrderBy) != fmt.Sprint(parser.ProvenanceOrderBy) {
		t.Errorf("stub shape: %+v", rs)
	}
	if _, err := stubPgrSource(pgrCommand(t, `pgr()`)); !errors.Is(err, errPgrNeedsStart) {
		t.Errorf("no start: err = %v", err)
	}
}

func TestProvenanceValueCoercion(t *testing.T) {
	for in, want := range map[interface{}]int64{uint64(7): 7, int64(-2): -2, 3: 3, 4.9: 4, "12": 12, "x": 0, true: 0} {
		if got := reconInt64(in); got != want {
			t.Errorf("reconInt64(%#v) = %d, want %d", in, got, want)
		}
	}
	for in, want := range map[interface{}]float64{0.5: 0.5, float32(0.25): 0.25, "0.75": 0.75, "x": 0, 1: 0} {
		if got := reconFloat(in); got != want {
			t.Errorf("reconFloat(%#v) = %v, want %v", in, got, want)
		}
	}
	if reconString(5) != "" || reconString("g") != "g" {
		t.Error("reconString")
	}
}

func TestParseReconnectPeers(t *testing.T) {
	rows := []map[string]interface{}{
		{"recon_type": "file", "peer_guid": "p1", "src_guid": "w", "label": `C:\x.exe`, "anomaly": 1.0, "peer_host": "h2"},
		{"recon_type": "net", "peer_guid": "", "object_id": "net:1"},
		{"recon_type": "dns", "peer_guid": "p2", "object_id": "dns:x", "anomaly": "0.85"},
	}
	got := parseReconnectPeers(rows)
	if len(got) != 2 || got[0].PeerGUID != "p1" || got[0].PeerHost != "h2" || got[0].Anomaly != 1 || got[1].Anomaly != 0.85 {
		t.Errorf("got %+v", got)
	}
}

func TestProvenanceEdgeKeys(t *testing.T) {
	src, tgt := provenanceEdgeKeys([]map[string]interface{}{
		{"fkey_src": "a", "fkey_tgt": "x"},
		{"fkey_src": "a", "fkey_tgt": "y"},
		{"fkey_src": "", "fkey_tgt": "x"},
		{"fkey_tgt": "z"}, // missing src reads as the empty key
	})
	if fmt.Sprint(src) != "[a ]" || fmt.Sprint(tgt) != "[x y z]" {
		t.Errorf("src=%q tgt=%q", src, tgt)
	}
}

// A peer is ranked by how many distinct artifacts it shares with the tree, so the
// artifact key must separate net/dns objects, file paths, and keyless bridges.
func TestReconPeerArtifact(t *testing.T) {
	cases := []struct {
		peer parser.ReconnectPeer
		want string
	}{
		{parser.ReconnectPeer{ReconType: "net", ObjectID: "net:1.2.3.4", Label: "1.2.3.4", PeerGUID: "p"}, "net:1.2.3.4"},
		{parser.ReconnectPeer{ReconType: "file", Label: `C:\x.exe`, PeerGUID: "p"}, "file\x00C:\\x.exe"},
		{parser.ReconnectPeer{ReconType: "remote_thread", PeerGUID: "p"}, "remote_thread\x00p"},
	}
	for _, tc := range cases {
		if got := reconPeerArtifact(tc.peer); got != tc.want {
			t.Errorf("reconPeerArtifact(%+v) = %q, want %q", tc.peer, got, tc.want)
		}
	}
}

// Filling the node cap with the last group of a level, while that level still has children to
// walk, is a truncation too.
func TestCollectProvenanceTreeCapAtEndOfLevel(t *testing.T) {
	f := newFakeLineage(">root")
	for i := 0; i < parser.MaxProvenanceNodes-1; i++ {
		c := fmt.Sprintf("c%05d", i)
		f.addProc("root", c)
		f.addProc(c, c+"-kid")
	}
	tree := f.collect(t, parser.ProvenanceParams{Start: "root", Depth: 3, Direction: "forward"})
	if len(tree.order) != parser.MaxProvenanceNodes || !tree.truncated {
		t.Errorf("%d nodes, truncated=%v; the cap cut the next level", len(tree.order), tree.truncated)
	}
}

// The member cap keeps the walk bounded, but a group's size stays the number of processes it
// stands for; only members another node already holds are taken off.
func TestProvenanceTreeMemberCapKeepsGroupSize(t *testing.T) {
	tree := newProvenanceTree()
	tree.add([]string{"held"}, 1, 0, 0, parser.MaxProvenanceNodes)
	tree.nMember = parser.MaxProvenanceMembers - 2 // room for two more members
	tree.add([]string{"a", "held", "b", "c", "d"}, 100, 0, 0, parser.MaxProvenanceNodes)
	if tree.size["a"] != 99 || len(tree.members["a"]) != 2 || !tree.truncated {
		t.Errorf("size %d, %d members kept, truncated=%v; want 99 (100 less the one held elsewhere), 2, true",
			tree.size["a"], len(tree.members["a"]), tree.truncated)
	}
}

// A scoring scan that fills its row bound may have cut rows; that is a notice, not silence.
func TestProvenanceScoreSQLScanBoundWarns(t *testing.T) {
	p := pgrParams(false, false)
	p.Limit = 20000
	f := newFakeLineage(">root")
	f.scan = func(string, storage.ExternalTable) ([]map[string]interface{}, error) {
		rows := make([]map[string]interface{}, provenanceMaxScanRows)
		for i := range rows {
			rows[i] = spawnRow("root", fmt.Sprintf("c%05d", i))
		}
		return rows, nil
	}
	_, warnings, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
	if err != nil || len(warnings) != 1 || !strings.Contains(warnings[0], "scored more edges than") {
		t.Errorf("err=%v warnings=%v", err, warnings)
	}
}

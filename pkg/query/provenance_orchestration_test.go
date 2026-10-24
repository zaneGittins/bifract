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
	"bifract/pkg/storage/storagetest"
)

// fakeLineage stands in for ClickHouse behind pgr()'s passes. It answers the
// tree-hop queries from an in-memory parent->child edge list and every other
// pass from per-kind handlers, and records what it was asked. Passes are
// recognised by the shape of the SQL their parser builders emit. handler wires it
// behind a real ClickHouse client, so every pass runs through QueryProvenance.
type fakeLineage struct {
	mu       sync.Mutex
	parentOf map[string]string   // child -> parent, for processes that exist
	children map[string][]string // parent -> children, in insertion order
	failHop  map[string]bool     // frontier guid whose hop fails
	calls    []string

	reconTotals func() ([]map[string]interface{}, error)
	recon       func(sql string) ([]map[string]interface{}, error)
	totalHosts  func() ([]map[string]interface{}, error)
	edgeKeys    func(sql string) ([]map[string]interface{}, error)
	scan        func(sql string) ([]map[string]interface{}, error)
}

// newFakeLineage builds a lineage from "parent>child" pairs. A root is declared
// with ">root" (it exists as a process with no parent).
func newFakeLineage(edges ...string) *fakeLineage {
	f := &fakeLineage{parentOf: map[string]string{}, children: map[string][]string{}, failHop: map[string]bool{}}
	for _, e := range edges {
		p, c, _ := strings.Cut(e, ">")
		f.parentOf[c] = p
		if p != "" {
			f.children[p] = append(f.children[p], c)
		}
	}
	return f
}

var hopRE = regexp.MustCompile(`^SELECT process_guid, parent_guid FROM \S+ WHERE .* (process_guid|parent_guid) IN \(([^)]*)\)$`)

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
// through the client's row mapper unchanged.
func handlerAnswering(answer func(sql string) ([]map[string]interface{}, error)) *QueryHandler {
	db, ch := storagetest.ClickHouse()
	ch.Respond(func(sql string, _ []any) (fakedb.CHResult, bool) {
		rows, err := answer(sql)
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

func (f *fakeLineage) query(sql string) ([]map[string]interface{}, error) {
	if m := hopRE.FindStringSubmatch(sql); m != nil {
		f.record("hop")
		var frontier []string
		for _, q := range strings.Split(m[2], ", ") {
			frontier = append(frontier, strings.Trim(q, "'"))
		}
		var rows []map[string]interface{}
		for _, g := range frontier {
			if f.failHop[g] {
				return nil, fmt.Errorf("hop failed at %s", g)
			}
			if m[1] == "parent_guid" { // forward: the frontier's children
				for _, c := range f.children[g] {
					rows = append(rows, map[string]interface{}{"process_guid": c, "parent_guid": g})
				}
			} else if p, ok := f.parentOf[g]; ok { // backward: the frontier node and its parent
				rows = append(rows, map[string]interface{}{"process_guid": g, "parent_guid": p})
			}
		}
		return rows, nil
	}
	call := func(kind string, h func() ([]map[string]interface{}, error)) ([]map[string]interface{}, error) {
		f.record(kind)
		if h == nil {
			return nil, nil
		}
		return h()
	}
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
			return f.scan(sql)
		})
	default:
		return call("recon", func() ([]map[string]interface{}, error) {
			if f.recon == nil {
				return nil, nil
			}
			return f.recon(sql)
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

func TestCollectProvenanceTreeGuids(t *testing.T) {
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
		{"both directions", "a", 1, "both", []string{"a", "b", "root"}},
		{"unknown direction means both", "a", 1, "", []string{"a", "b", "root"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := lineage()
			got, err := f.handler().collectProvenanceTreeGuids(context.Background(), parser.ProvenanceParams{Start: tc.start, Depth: tc.depth, Direction: tc.direction}, pgrOpts())
			if err != nil {
				t.Fatal(err)
			}
			if got[0] != tc.start {
				t.Errorf("the seed must lead the guid set, got %v", got)
			}
			if fmt.Sprint(sortedCopy(got)) != fmt.Sprint(sortedCopy(tc.want)) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Malformed lineage (a guid reused as its own ancestor) must terminate: the
// seen-guard visits each guid once in either direction.
func TestCollectProvenanceTreeGuidsCycle(t *testing.T) {
	for _, dir := range []string{"forward", "backward"} {
		f := newFakeLineage("x>y", "y>x")
		got, err := f.handler().collectProvenanceTreeGuids(context.Background(), parser.ProvenanceParams{Start: "x", Depth: 50, Direction: dir}, pgrOpts())
		if err != nil || fmt.Sprint(got) != "[x y]" {
			t.Errorf("%s: got %v, %v", dir, got, err)
		}
		if n := f.kinds()["hop"]; n > 3 {
			t.Errorf("%s: %d hops; the walk kept going after the frontier emptied", dir, n)
		}
	}
}

// A guid seen only in leaf logs (never as a process_creation) is not a tree
// anchor: no traversal, no scoring on a phantom root.
func TestCollectProvenanceTreeGuidsMissingSeed(t *testing.T) {
	f := newFakeLineage(">root", "root>a")
	got, err := f.handler().collectProvenanceTreeGuids(context.Background(), parser.ProvenanceParams{Start: "ghost", Depth: 5}, pgrOpts())
	if err != nil || got != nil {
		t.Fatalf("got %v, %v; want no tree", got, err)
	}
	if n := f.kinds()["hop"]; n != 1 {
		t.Errorf("%d hop queries for a missing seed, want only the seed check", n)
	}
}

func TestCollectProvenanceTreeGuidsErrors(t *testing.T) {
	f := newFakeLineage(">root", "root>a", "a>b")
	f.failHop["a"] = true
	if _, err := f.handler().collectProvenanceTreeGuids(context.Background(), parser.ProvenanceParams{Start: "root", Depth: 5, Direction: "forward"}, pgrOpts()); err == nil {
		t.Error("a failed hop must fail the traversal, not return a truncated tree")
	}

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	cancelAfterSeed := handlerAnswering(func(sql string) ([]map[string]interface{}, error) {
		calls++
		cancel()
		return []map[string]interface{}{{"process_guid": "root", "parent_guid": ""}}, nil
	})
	if _, err := cancelAfterSeed.collectProvenanceTreeGuids(ctx,
		parser.ProvenanceParams{Start: "root", Depth: 5}, pgrOpts()); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("%d queries after cancellation, want none past the seed", calls-1)
	}
}

// The traversal stops at MaxProvenanceGuids however wide a level is.
func TestCollectProvenanceTreeGuidsCap(t *testing.T) {
	edges := []string{">root"}
	for i := 0; i < parser.MaxProvenanceGuids+50; i++ {
		edges = append(edges, fmt.Sprintf("root>c%05d", i))
	}
	f := newFakeLineage(edges...)
	got, err := f.handler().collectProvenanceTreeGuids(context.Background(), parser.ProvenanceParams{Start: "root", Depth: 3, Direction: "forward"}, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != parser.MaxProvenanceGuids {
		t.Errorf("collected %d guids, want the cap %d", len(got), parser.MaxProvenanceGuids)
	}
}

func peer(guid string, anomaly float64) parser.ReconnectPeer {
	return parser.ReconnectPeer{ReconType: "net", PeerGUID: guid, SrcGUID: "root", ObjectID: "net:" + guid, Label: guid, Anomaly: anomaly}
}

func TestExpandReconnectionPeers(t *testing.T) {
	tree := []string{"root", "a"}

	t.Run("nothing severe enough expands", func(t *testing.T) {
		f := newFakeLineage()
		got := f.handler().expandReconnectionPeers(context.Background(), tree, []parser.ReconnectPeer{peer("p1", 0.79)}, pgrOpts())
		if fmt.Sprint(got) != fmt.Sprint(tree) || len(f.kinds()) != 0 {
			t.Errorf("got %v with queries %v; want the tree unchanged and no queries", got, f.kinds())
		}
		got[0] = "mutated"
		if tree[0] != "root" {
			t.Error("the result aliases the caller's tree slice")
		}
	})

	t.Run("top five by severity then guid, merged in rank order", func(t *testing.T) {
		f := newFakeLineage(">p1", "p1>p1c", ">p2", "p2>a", ">p3", ">p4", ">p5", ">p6", ">p7", "p7>p7c")
		peers := []parser.ReconnectPeer{
			peer("p6", 0.85), peer("p5", 0.85), peer("p4", 0.85),
			peer("p3", 0.9), peer("p2", 0.85), peer("p1", 0.85),
			peer("p1", 1.0), // a peer's best row ranks it
			peer("p7", 0.5), // below the expansion bar
		}
		got := f.handler().expandReconnectionPeers(context.Background(), tree, peers, pgrOpts())
		// Rank: p1 (1.0), p3 (0.9), then 0.85 ties by guid: p2, p4, p5. p6 is sixth.
		want := []string{"root", "a", "p1", "p1c", "p3", "p2", "p4", "p5"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("got %v, want %v (p2's child a is already in the tree)", got, want)
		}
	})

	t.Run("a failed expansion is skipped, not fatal", func(t *testing.T) {
		f := newFakeLineage(">p1", "p1>p1c", ">p2", "p2>p2c")
		f.failHop["p1"] = true
		got := f.handler().expandReconnectionPeers(context.Background(), tree, []parser.ReconnectPeer{peer("p1", 1), peer("p2", 1)}, pgrOpts())
		if fmt.Sprint(got) != fmt.Sprint([]string{"root", "a", "p2", "p2c"}) {
			t.Errorf("got %v", got)
		}
	})

	t.Run("guid cap", func(t *testing.T) {
		big := make([]string, reconnectExpandMaxGuids-1)
		for i := range big {
			big[i] = fmt.Sprintf("t%04d", i)
		}
		f := newFakeLineage(">p1", "p1>x", "p1>y")
		got := f.handler().expandReconnectionPeers(context.Background(), big, []parser.ReconnectPeer{peer("p1", 1)}, pgrOpts())
		if len(got) != reconnectExpandMaxGuids {
			t.Errorf("expanded to %d guids, want the cap %d", len(got), reconnectExpandMaxGuids)
		}

		full := append(big, "t-last")
		f = newFakeLineage(">p1", "p1>x")
		if got := f.handler().expandReconnectionPeers(context.Background(), full, []parser.ReconnectPeer{peer("p1", 1)}, pgrOpts()); len(got) != len(full) || len(f.kinds()) != 0 {
			t.Errorf("a tree already at the cap must not expand: %d guids, queries %v", len(got), f.kinds())
		}
	})

	t.Run("cancelled request does not expand", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f := newFakeLineage(">p1", "p1>x")
		got := f.handler().expandReconnectionPeers(ctx, tree, []parser.ReconnectPeer{peer("p1", 1)}, pgrOpts())
		if len(got) != len(tree) || len(f.kinds()) != 0 {
			t.Errorf("got %v with queries %v", got, f.kinds())
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

func TestProvenanceScoreSQLRequiresScope(t *testing.T) {
	f := newFakeLineage(">root")
	_, err := f.handler().provenanceScoreSQL(context.Background(), pgrParams(false, false), parser.QueryOptions{})
	if err == nil || !strings.Contains(err.Error(), "fractal scoping is required") {
		t.Fatalf("err = %v", err)
	}
	if len(f.kinds()) != 0 {
		t.Errorf("an unscoped pgr() queried %v", f.kinds())
	}
}

func TestProvenanceScoreSQLEmptyTree(t *testing.T) {
	f := newFakeLineage(">other")
	sql, err := f.handler().provenanceScoreSQL(context.Background(), pgrParams(true, true), pgrOpts())
	if err != nil || sql != provenanceEmptyScoreSQL {
		t.Fatalf("got %q, %v; want the zero-row stub", sql, err)
	}
	if k := f.kinds(); k["hop"] != 1 || len(k) != 1 {
		t.Errorf("queries %v; an empty tree must stop after the seed check", k)
	}
}

// Per-edge scoring (diffuse=false): the pass-2 SQL is built from the tree, the
// fleet size, and the tree's own edge keys, under pgr()'s row budget.
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
	got, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	want, err := parser.BuildProvenanceScoringSQL([]string{"root", "a", "b"}, p.Threshold, p.EdgeTypes, false,
		parser.ProvenanceBaseline{TotalHosts: 12, SrcKeys: []string{"s1", ""}, TgtKeys: []string{"t1", "t2"}}, scoreOptsFor(p))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("scoring SQL differs from the expected build\n got: %.300s\nwant: %.300s", got, want)
	}
	if k := f.kinds(); k["recon"]+k["recon_totals"]+k["scan"] != 0 {
		t.Errorf("reconnect and diffuse are off, yet ran %v", k)
	}
}

// The baseline lookups are optional: a failure or an oversized key set falls
// back to the unrestricted baseline, which is slower but scores identically. A
// truncated key set would not.
func TestProvenanceScoreSQLBaselineFallbacks(t *testing.T) {
	p := pgrParams(false, false)
	want, err := parser.BuildProvenanceScoringSQL([]string{"root", "a"}, p.Threshold, p.EdgeTypes, false, parser.ProvenanceBaseline{}, scoreOptsFor(p))
	if err != nil {
		t.Fatal(err)
	}
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
			got, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Error("fallback did not score against the unrestricted baseline")
			}
		})
	}
}

// Diffusion executes the scoring SQL, propagates in Go, and re-emits the
// survivors. The scan must wrap the scoring SQL before limiting it: with
// reconnection appended it ends in UNION ALL, and ClickHouse binds a bare
// trailing LIMIT to the last member only.
func TestProvenanceScoreSQLDiffusion(t *testing.T) {
	p := pgrParams(true, false)
	var scanned string
	f := newFakeLineage(">root", "root>a")
	f.scan = func(sql string) ([]map[string]interface{}, error) {
		scanned = sql
		return []map[string]interface{}{spawnRow("root", "a"), leafRow("a", "net:203.0.113.9", 1)}, nil
	}
	got, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	scoring, err := parser.BuildProvenanceScoringSQL([]string{"root", "a"}, p.Threshold, p.EdgeTypes, true, parser.ProvenanceBaseline{}, scoreOptsFor(p))
	if err != nil {
		t.Fatal(err)
	}
	wantScan := fmt.Sprintf("SELECT * FROM (%s) AS _d ORDER BY (event_type = 'spawn') DESC, anomaly_score DESC LIMIT %d", scoring, diffuseMaxScanRows)
	if scanned != wantScan {
		t.Errorf("diffusion scan is not the wrapped, bounded scoring SQL:\n%.400s", scanned)
	}
	if !strings.HasPrefix(got, "SELECT ") || !strings.Contains(got, " UNION ALL ") || !strings.Contains(got, "'net:203.0.113.9'") {
		t.Errorf("survivors were not re-emitted as a literal source:\n%.400s", got)
	}
}

// Diffusion is best-effort: a failed scan or a tree too large to inline falls
// back to per-edge scoring, so pgr() still returns the full graph. A cancelled
// request is not a failure to paper over.
func TestProvenanceScoreSQLDiffusionFallbacks(t *testing.T) {
	p := pgrParams(true, false)
	perEdge, err := parser.BuildProvenanceScoringSQL([]string{"root", "a"}, p.Threshold, p.EdgeTypes, false, parser.ProvenanceBaseline{}, scoreOptsFor(p))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("scan fails", func(t *testing.T) {
		f := newFakeLineage(">root", "root>a")
		f.scan = func(string) ([]map[string]interface{}, error) { return nil, errors.New("code 241") }
		got, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
		if err != nil || got != perEdge {
			t.Errorf("got err=%v, per-edge fallback=%v", err, got == perEdge)
		}
	})

	t.Run("backbone too large to inline", func(t *testing.T) {
		f := newFakeLineage(">root", "root>a")
		f.scan = func(string) ([]map[string]interface{}, error) {
			var rows []map[string]interface{}
			for i := 0; i < 2000; i++ {
				r := spawnRow("root", fmt.Sprintf("c%04d", i))
				r["command_line"] = strings.Repeat("x", 200)
				rows = append(rows, r)
			}
			return rows, nil
		}
		got, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
		if err != nil || got != perEdge {
			t.Errorf("got err=%v, per-edge fallback=%v (len %d)", err, got == perEdge, len(got))
		}
	})

	t.Run("cancelled during the scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		f := newFakeLineage(">root", "root>a")
		f.scan = func(string) ([]map[string]interface{}, error) {
			cancel()
			return nil, context.Canceled
		}
		if _, err := f.handler().provenanceScoreSQL(ctx, p, pgrOpts()); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want the cancellation", err)
		}
	})
}

// Reconnection: the rarity-gate totals are read once and passed into the
// reverse lookup as literals, severe peers are expanded into the scored guid
// set, and every admitted peer's bridge edges ride the scoring SQL.
func TestProvenanceScoreSQLReconnection(t *testing.T) {
	p := pgrParams(false, true)
	opts := pgrOpts()
	var reconSQL string
	f := newFakeLineage(">root", "root>a", ">peer", "peer>peerkid")
	f.reconTotals = func() ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"total_hosts": uint64(40), "total_images": uint64(9)}}, nil
	}
	f.recon = func(sql string) ([]map[string]interface{}, error) {
		reconSQL = sql
		return []map[string]interface{}{
			{"recon_type": "net", "peer_guid": "peer", "src_guid": "a", "object_id": "net:198.51.100.7", "label": "198.51.100.7", "anomaly": 0.85, "peer_image": "evil.exe"},
			{"recon_type": "net", "peer_guid": "", "src_guid": "a", "object_id": "net:x"}, // no peer: dropped
		}, nil
	}
	got, err := f.handler().provenanceScoreSQL(context.Background(), p, opts)
	if err != nil {
		t.Fatal(err)
	}

	wantRecon, err := parser.BuildReconnectionSQL([]string{"root", "a"}, p, 40, 9, opts)
	if err != nil {
		t.Fatal(err)
	}
	if reconSQL != wantRecon {
		t.Error("reverse lookup was not built from the tree and the fetched rarity-gate totals")
	}

	peers := []parser.ReconnectPeer{{ReconType: "net", PeerGUID: "peer", SrcGUID: "a", ObjectID: "net:198.51.100.7", Label: "198.51.100.7", Anomaly: 0.85, PeerImage: "evil.exe"}}
	base, err := parser.BuildProvenanceScoringSQL([]string{"root", "a", "peer", "peerkid"}, p.Threshold, p.EdgeTypes, false, parser.ProvenanceBaseline{}, scoreOptsFor(p))
	if err != nil {
		t.Fatal(err)
	}
	if want := parser.AppendReconnectionEdges(base, peers); got != want {
		t.Errorf("scoring SQL does not cover the expanded peer and its bridge edges\n got: %.300s\nwant: %.300s", got, want)
	}
}

func TestProvenanceScoreSQLReconnectionFailures(t *testing.T) {
	p := pgrParams(false, true)
	base, err := parser.BuildProvenanceScoringSQL([]string{"root", "a"}, p.Threshold, p.EdgeTypes, false, parser.ProvenanceBaseline{}, scoreOptsFor(p))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("feature failure falls back to the base tree", func(t *testing.T) {
		f := newFakeLineage(">root", "root>a")
		f.reconTotals = func() ([]map[string]interface{}, error) { return nil, errors.New("proc_freq missing") }
		f.recon = func(string) ([]map[string]interface{}, error) { return nil, errors.New("code 36") }
		got, err := f.handler().provenanceScoreSQL(context.Background(), p, pgrOpts())
		if err != nil || got != base {
			t.Errorf("err=%v, base tree=%v", err, got == base)
		}
		if f.kinds()["recon"] != 1 {
			t.Error("a failed totals lookup must still attempt reconnection at the floor gate")
		}
	})

	t.Run("cancellation is surfaced", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		f := newFakeLineage(">root", "root>a")
		f.recon = func(string) ([]map[string]interface{}, error) {
			cancel()
			return nil, context.Canceled
		}
		if _, err := f.handler().provenanceScoreSQL(ctx, p, pgrOpts()); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want the cancellation", err)
		}
	})
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

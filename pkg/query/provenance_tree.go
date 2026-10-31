package query

import (
	"context"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// provenanceTree is pgr pass 1's result: the graph's process nodes and the member processes
// behind them. A node is one process, or a group of identical siblings collapsed into one; its
// id is its earliest member's guid, so a node always names a real process.
type provenanceTree struct {
	order   []string            // node ids in walk order: seed, ancestors, descendants, then peers
	members map[string][]string // node -> member guids, the node itself first
	nodeOf  map[string]string   // member guid -> node
	size    map[string]int64    // node -> exact member count (members may be capped)
	firstMs map[string]int64    // node -> earliest member creation (unix ms), when in range
	lastMs  map[string]int64    // node -> latest member creation
	depth   map[string]int      // node -> hops from the seed, either direction
	path    map[string]string   // node -> "a > b > c", seed-relative like ptg()'s _path
	parent  map[string]string   // descendant node -> its parent node
	nMember int
	// spanUnknown is set when a node is not its group's earliest member, so its own creation
	// time is not known and span cannot bound it.
	spanUnknown bool
	// truncated is set when a cap (MaxProvenanceNodes or MaxProvenanceMembers) cut the walk.
	truncated bool
}

func newProvenanceTree() *provenanceTree {
	return &provenanceTree{members: map[string][]string{}, nodeOf: map[string]string{}, size: map[string]int64{},
		firstMs: map[string]int64{}, lastMs: map[string]int64{}, depth: map[string]int{}, path: map[string]string{}, parent: map[string]string{}}
}

func (t *provenanceTree) has(guid string) bool { _, ok := t.nodeOf[guid]; return ok }

// add records a node and its not-yet-seen members (members[0] is the node id). It returns
// false, and adds nothing, when no member is new or a cap is reached.
func (t *provenanceTree) add(members []string, n, firstMs, lastMs int64, nodeCap int) bool {
	fresh := members[:0:0]
	for _, g := range members {
		if g != "" && !t.has(g) {
			fresh = append(fresh, g)
		}
	}
	if len(fresh) == 0 {
		return false
	}
	if len(t.order) >= nodeCap {
		t.truncated = true
		return false
	}
	// n counts every member; take off only the ones another node already holds.
	n -= int64(len(members) - len(fresh))
	if room := parser.MaxProvenanceMembers - t.nMember; len(fresh) > room {
		t.truncated = true
		if room <= 0 {
			return false
		}
		fresh = fresh[:room]
	}
	node := fresh[0]
	if node != members[0] {
		t.spanUnknown = true
	}
	n = max(n, int64(len(fresh)))
	t.order = append(t.order, node)
	t.members[node] = fresh
	for _, g := range fresh {
		t.nodeOf[g] = node
	}
	t.nMember += len(fresh)
	t.size[node] = n
	if firstMs > 0 {
		t.firstMs[node], t.lastMs[node] = firstMs, lastMs
	}
	return true
}

// groupSpan formats when a collapsed node's first and last members were created, or "" for a
// single process or an unknown span.
func (t *provenanceTree) groupSpan(node string) (first, last string) {
	const layout = "2006-01-02 15:04:05.000"
	f, ok := t.firstMs[node]
	if t.size[node] <= 1 || !ok {
		return "", ""
	}
	return time.UnixMilli(f).UTC().Format(layout), time.UnixMilli(t.lastMs[node]).UTC().Format(layout)
}

// node maps a guid onto its node, or returns it unchanged when it is not in the tree.
func (t *provenanceTree) node(guid string) string {
	if n, ok := t.nodeOf[guid]; ok {
		return n
	}
	return guid
}

// table is the tree as the ProvenanceTreeTable external table.
func (t *provenanceTree) table() storage.ExternalTable {
	rows := make([][]string, 0, t.nMember)
	for _, node := range t.order {
		for _, g := range t.members[node] {
			rows = append(rows, []string{g, node})
		}
	}
	return storage.ExternalTable{Name: parser.ProvenanceTreeTable, Columns: []string{"guid", "node"}, Rows: rows}
}

// span is the creation-time range of the nodes' own rows that are in range, or zero when one
// of them is unknown.
func (t *provenanceTree) span() (start, end time.Time) {
	if t.spanUnknown {
		return time.Time{}, time.Time{}
	}
	var lo, hi int64
	for _, node := range t.order {
		f, ok := t.firstMs[node]
		if !ok {
			continue
		}
		// pm reads the node's own row, which is its earliest member.
		if lo == 0 || f < lo {
			lo = f
		}
		if f > hi {
			hi = f
		}
	}
	if lo == 0 {
		return time.Time{}, time.Time{}
	}
	return time.UnixMilli(lo).UTC(), time.UnixMilli(hi).UTC()
}

// collectProvenanceTree runs pgr pass 1: an iterative walk over proc_lineage, one indexed hop
// per level. Ancestors are walked first, one process per level, so a wide subtree can never
// starve them of the node budget. Descendants are walked with BuildProcessGroupHopSQL, which
// collapses identical siblings, so a polling loop with tens of thousands of children costs a
// handful of nodes and the cap is spent on distinct behavior. Returns nil when the seed is not a
// process in range (a guid seen only in leaf logs is not a tree anchor).
func collectProvenanceTree(ctx context.Context, db *storage.ClickHouseClient, p parser.ProvenanceParams, opts parser.QueryOptions, nodeCap int) (*provenanceTree, error) {
	seedRows, err := db.QueryProvenance(ctx, parser.BuildProcessTreeHopSQL(p.Start, opts))
	if err != nil {
		return nil, err
	}
	var seedParent string
	var seedMs int64
	seedExists := false
	for _, r := range seedRows {
		if reconString(r["process_guid"]) == p.Start {
			seedExists = true
			seedParent, seedMs = reconString(r["parent_guid"]), reconInt64(r["ts_ms"])
			break
		}
	}
	if !seedExists {
		return nil, nil
	}
	t := newProvenanceTree()
	t.add([]string{p.Start}, 1, seedMs, seedMs, nodeCap)
	t.path[p.Start] = p.Start

	if p.Direction != "forward" {
		// The seed check returned the seed's parent; each level reads that parent's own row.
		parent, child := seedParent, p.Start
		for level := 0; level < p.Depth && parent != "" && !t.has(parent); level++ {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			rows, hErr := db.QueryProvenance(ctx, parser.BuildProcessTreeHopSQL(parent, opts))
			if hErr != nil {
				return nil, hErr
			}
			var ms int64
			next := ""
			for _, r := range rows {
				if reconString(r["process_guid"]) == parent {
					ms, next = reconInt64(r["ts_ms"]), reconString(r["parent_guid"])
					break
				}
			}
			if !t.add([]string{parent}, 1, ms, ms, nodeCap) {
				break
			}
			t.depth[parent], t.path[parent] = level+1, parent+" > "+t.path[child]
			parent, child = next, parent
		}
	}

	if p.Direction != "backward" {
		frontier := []string{p.Start}
		for level := 0; level < p.Depth && len(frontier) > 0; level++ {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if len(t.order) >= nodeCap {
				t.truncated = true // a full graph with children still to walk
				break
			}
			ft := storage.ExternalTable{Name: parser.ProvenanceFrontierTable, Columns: []string{"guid", "node"}}
			for _, node := range frontier {
				for _, g := range t.members[node] {
					ft.Rows = append(ft.Rows, []string{g, node})
				}
			}
			rows, hErr := db.QueryProvenance(ctx, parser.BuildProcessGroupHopSQL(opts), ft)
			if hErr != nil {
				return nil, hErr
			}
			var next []string
			for _, r := range rows {
				members := reconStrings(r["members"])
				// The earliest member names the node; the rest follow in any order.
				rep := reconString(r["rep"])
				ordered := make([]string, 0, len(members)+1)
				ordered = append(ordered, rep)
				for _, g := range members {
					if g != rep {
						ordered = append(ordered, g)
					}
				}
				n := reconInt64(r["n"])
				if n > int64(len(ordered)) {
					t.truncated = true // groupUniqArray capped this group's members
				}
				if t.add(ordered, n, reconInt64(r["first_ms"]), reconInt64(r["last_ms"]), nodeCap) {
					node, up := t.order[len(t.order)-1], reconString(r["parent_node"])
					t.parent[node], t.depth[node], t.path[node] = up, t.depth[up]+1, t.path[up]+" > "+node
					next = append(next, node)
				}
			}
			frontier = next
		}
	}
	return t, nil
}

// reconStrings reads an Array(String) column value.
func reconStrings(v interface{}) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []interface{}:
		out := make([]string, 0, len(s))
		for _, x := range s {
			out = append(out, reconString(x))
		}
		return out
	}
	return nil
}

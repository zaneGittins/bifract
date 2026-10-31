package query

import (
	"context"
	"fmt"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// ResolveProcessTree walks the tree of the pipeline's ptg(), if it has one, and sets
// opts.ProcessTree for translation. It uses pgr's walk (indexed hops, identical siblings
// collapsed) instead of a recursive query, and returns a notice when a cap cut the tree.
// Argument errors are left to translation, which reports them in the query's own terms.
func ResolveProcessTree(ctx context.Context, db *storage.ClickHouseClient, pipeline *parser.PipelineNode, opts *parser.QueryOptions) ([]string, error) {
	p, ok, err := parser.ProcessTreeCommand(pipeline)
	if !ok || err != nil || opts.SourceMode == parser.SourceIceberg {
		return nil, nil
	}
	if opts.ProcLineageTable == "" {
		opts.ProcLineageTable = db.ProcLineageReadTable()
	}
	tree, err := collectProvenanceTree(ctx, db, parser.ProvenanceParams{Start: p.Start, Depth: p.Depth, Direction: p.Direction}, *opts, parser.MaxProvenanceNodes)
	if err != nil {
		return nil, fmt.Errorf("ptg: tree walk: %w", err)
	}
	opts.ProcessTree = &parser.ProcessTree{}
	if tree == nil {
		return nil, nil
	}
	nodes := storage.ExternalTable{Name: parser.ProvenanceTreeTable, Columns: []string{"guid", "node"}}
	for _, n := range tree.order {
		nodes.Rows = append(nodes.Rows, []string{n, n})
	}
	rows, err := db.QueryProvenance(ctx, parser.BuildProcessTreeRowsSQL(*opts), nodes)
	if err != nil {
		return nil, fmt.Errorf("ptg: node rows: %w", err)
	}
	byGUID := make(map[string]map[string]interface{}, len(rows))
	for _, r := range rows {
		byGUID[reconString(r["process_guid"])] = r
	}
	opts.ProcessTree.Rows = processTreeRows(tree, byGUID)

	if tree.truncated {
		return []string{fmt.Sprintf("ptg: process tree truncated by its size caps at %d nodes (%d processes); narrow the time range or lower depth=", len(tree.order), tree.nMember)}, nil
	}
	return nil, nil
}

// processTreeRows renders the walked tree as ptg() rows, in walk order. A node without its own
// row in range (an ancestor created before the window) is left out, as the recursive query did.
// A descendant's parent is its parent node, so a collapsed group hangs off one node.
func processTreeRows(tree *provenanceTree, byGUID map[string]map[string]interface{}) []parser.ProcessTreeRow {
	out := make([]parser.ProcessTreeRow, 0, len(tree.order))
	for _, n := range tree.order {
		r, ok := byGUID[n]
		if !ok {
			continue
		}
		row := parser.ProcessTreeRow{
			Timestamp: reconString(r["ts"]), LogID: reconString(r["log_id"]), FractalID: reconString(r["fractal_id"]),
			ProcessGUID: n, ParentGUID: reconString(r["parent_guid"]), Image: reconString(r["image"]),
			ParentImage: reconString(r["parent_image"]), Commandline: reconString(r["commandline"]),
			ComputerName: reconString(r["computer_name"]), Depth: tree.depth[n], Path: tree.path[n], GroupSize: tree.size[n],
		}
		if up, ok := tree.parent[n]; ok {
			row.ParentGUID = up
		}
		row.GroupFirst, row.GroupLast = tree.groupSpan(n)
		out = append(out, row)
	}
	return out
}

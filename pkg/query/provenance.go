package query

import (
	"context"
	"fmt"
	"log"
	"sort"

	"bifract/pkg/parser"
)

// provenanceColumns are the flat columns the pgr() scored edge list exposes. pgr() is a source
// command (parser/source_command.go): it is resolved into a SQL subquery source and these
// resolve as bare columns for any downstream BQL (filter/aggregate/sort/table/pgraph).
var provenanceColumns = []string{"parent", "child", "label", "event_type", "anomaly_score", "log_id", "timestamp", "fractal_id", "command_line", "proc_user"}

// provenanceNumericColumns is the subset of provenanceColumns that are already numeric in the
// subquery (so downstream numeric comparisons must not string-coerce them).
var provenanceNumericColumns = []string{"anomaly_score"}

// provenanceEmptyScoreSQL yields zero rows with the pgr output shape, so a query over an empty
// tree behaves correctly (count() -> 0, etc.) without special-casing every caller.
const provenanceEmptyScoreSQL = "SELECT '' AS parent, '' AS child, '' AS label, '' AS event_type, toFloat64(0) AS anomaly_score, '' AS log_id, '' AS timestamp, '' AS fractal_id, '' AS command_line, '' AS proc_user WHERE 1 = 0"

// provenanceScoreSQL runs pass 1 (tree traversal, collect guids) and returns the pass-2
// scored-edge SQL, which becomes the query's subquery source. Returns a zero-row stub when the
// tree is empty. The pass-2 SQL already carries `ORDER BY (event_type='spawn') DESC,
// anomaly_score DESC LIMIT`, so the spawn backbone is prioritized inside the subquery and an
// outer LIMIT never re-truncates process structure.
func (h *QueryHandler) provenanceScoreSQL(ctx context.Context, p parser.ProvenanceParams, opts parser.QueryOptions) (string, error) {
	treeSQL, err := parser.BuildProcessTreeQuery(p, opts)
	if err != nil {
		return "", fmt.Errorf("pgr: build tree query: %w", err)
	}
	treeRows, err := h.db.QueryLowPriority(ctx, treeSQL)
	if err != nil {
		return "", fmt.Errorf("pgr: tree pass: %w", err)
	}
	seen := make(map[string]struct{}, len(treeRows))
	guids := make([]string, 0, len(treeRows))
	for _, r := range treeRows {
		g, _ := r["process_guid"].(string)
		if g == "" {
			continue
		}
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		guids = append(guids, g)
	}
	if len(guids) == 0 {
		return provenanceEmptyScoreSQL, nil
	}

	// Cross-tree reconnection (NoDoze object-mediated): find peer processes in OTHER trees
	// that share a leaf with this subgraph, auto-expand the most anomalous ones into real
	// subtrees, and emit bridge edges for the rest. Bounded and rarity-pruned; see
	// parser.BuildReconnectionSQL. Failure is non-fatal -- pgr still returns the base tree.
	combined := guids
	var emitPeers []parser.ReconnectPeer
	if p.Reconnect {
		reconSQL, rErr := parser.BuildReconnectionSQL(guids, p, opts)
		if rErr != nil {
			log.Printf("[pgr] build reconnection query: %v", rErr)
		} else if reconSQL != "" {
			reconRows, qErr := h.db.QueryLowPriority(ctx, reconSQL)
			if qErr != nil {
				log.Printf("[pgr] reconnection lookup failed: %v", qErr)
			} else {
				peers := parseReconnectPeers(reconRows)
				combined, _ = h.expandReconnectionPeers(ctx, guids, peers, opts)
				// Reconnection owns its bridge edges (AppendReconnectionEdges), so emit every
				// peer: the emit logic decides shape (net/dns converge on the object node,
				// file is writer->executor, injection/access are skipped -- pass-2 owns those).
				emitPeers = peers
			}
		}
	}

	scoreSQL, err := parser.BuildProvenanceScoringSQL(combined, p.Threshold, p.EdgeTypes, opts)
	if err != nil {
		return "", fmt.Errorf("pgr: build scoring query: %w", err)
	}
	if len(emitPeers) > 0 {
		scoreSQL = parser.AppendReconnectionEdges(scoreSQL, emitPeers)
	}
	return scoreSQL, nil
}

// Reconnection expansion caps (orchestration side; SQL-side caps live in pkg/parser).
const (
	reconnectMaxExpand      = 5    // high-anomaly peers auto-expanded into real subtrees
	reconnectExpandDepth    = 3    // shallow descendant depth per expanded peer
	reconnectExpandMaxGuids = 2000 // hard cap on total guids after expansion
	reconnectHighAnomaly    = 0.8  // only peers at/above this bridge severity expand
)

// parseReconnectPeers converts the reverse-lookup rows into typed peers, dropping any
// without a peer guid.
func parseReconnectPeers(rows []map[string]interface{}) []parser.ReconnectPeer {
	peers := make([]parser.ReconnectPeer, 0, len(rows))
	for _, r := range rows {
		pe := parser.ReconnectPeer{
			ReconType:   reconString(r["recon_type"]),
			PeerGUID:    reconString(r["peer_guid"]),
			SrcGUID:     reconString(r["src_guid"]),
			ObjectID:    reconString(r["object_id"]),
			Label:       reconString(r["label"]),
			Anomaly:     reconFloat(r["anomaly"]),
			PeerImage:   reconString(r["peer_image"]),
			PeerLogID:   reconString(r["peer_log_id"]),
			PeerTS:      reconString(r["peer_ts"]),
			PeerFractal: reconString(r["peer_fractal"]),
		}
		if pe.PeerGUID == "" {
			continue
		}
		peers = append(peers, pe)
	}
	return peers
}

// expandReconnectionPeers ranks peers by bridge severity and traverses a shallow descendant
// subtree for the top reconnectMaxExpand of them (>= reconnectHighAnomaly), merging those
// guids into the tree set (capped). Returns the combined guid set and the set of expanded
// peer guids. Traversal failures are logged and skipped -- expansion is best-effort.
func (h *QueryHandler) expandReconnectionPeers(ctx context.Context, treeGuids []string, peers []parser.ReconnectPeer, opts parser.QueryOptions) ([]string, map[string]bool) {
	expanded := map[string]bool{}

	// Best anomaly per distinct peer, ordered by that anomaly descending.
	best := map[string]float64{}
	order := make([]string, 0, len(peers))
	for _, pe := range peers {
		if _, ok := best[pe.PeerGUID]; !ok {
			order = append(order, pe.PeerGUID)
		}
		if pe.Anomaly > best[pe.PeerGUID] {
			best[pe.PeerGUID] = pe.Anomaly
		}
	}
	sort.SliceStable(order, func(i, j int) bool { return best[order[i]] > best[order[j]] })

	seen := make(map[string]bool, len(treeGuids))
	for _, g := range treeGuids {
		seen[g] = true
	}
	combined := append([]string(nil), treeGuids...)

	expandCount := 0
	for _, g := range order {
		if expandCount >= reconnectMaxExpand || len(combined) >= reconnectExpandMaxGuids {
			break
		}
		if best[g] < reconnectHighAnomaly {
			break // order is descending, so nothing further qualifies
		}
		pp := parser.ProvenanceParams{Start: g, Depth: reconnectExpandDepth, Direction: "forward"}
		sql, err := parser.BuildProcessTreeQuery(pp, opts)
		if err != nil {
			continue
		}
		rows, err := h.db.QueryLowPriority(ctx, sql)
		if err != nil {
			log.Printf("[pgr] expand peer %s: %v", g, err)
			continue
		}
		expanded[g] = true
		expandCount++
		for _, r := range rows {
			sg := reconString(r["process_guid"])
			if sg == "" || seen[sg] {
				continue
			}
			seen[sg] = true
			combined = append(combined, sg)
			if len(combined) >= reconnectExpandMaxGuids {
				log.Printf("[pgr] reconnection expansion hit guid cap (%d)", reconnectExpandMaxGuids)
				break
			}
		}
	}
	return combined, expanded
}

func reconString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func reconFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	}
	return 0
}

// resolvedSource is a source command's SQL subquery + the flat columns it exposes.
type resolvedSource struct {
	SQL            string
	Columns        []string // exposed flat columns, in default projection order
	NumericColumns []string // subset of Columns already numeric (no string-coercion on compare)
	Focus          string   // optional node identifier the viz should center/highlight (pgr start)
}

// sourceResolver produces a resolvedSource for a source command. Resolution runs in the query
// layer because it may need the database (e.g. pgr()'s pass-1 tree traversal). Register one per
// source-command name; the core translate/handler path stays generic.
type sourceResolver func(h *QueryHandler, ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error)

var sourceResolvers = map[string]sourceResolver{
	"pgr": resolvePgrSource,
}

// resolveSourceCommand resolves a source command into its subquery source.
func (h *QueryHandler) resolveSourceCommand(ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error) {
	r, ok := sourceResolvers[cmd.Name]
	if !ok {
		return nil, fmt.Errorf("unknown source command: %s()", cmd.Name)
	}
	return r(h, ctx, cmd, opts)
}

// resolvePgrSource resolves pgr() into its two-pass scored-edge subquery.
func resolvePgrSource(h *QueryHandler, ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error) {
	if opts.SourceMode == parser.SourceIceberg {
		return nil, fmt.Errorf("pgr() operates on live process lineage and is not available over archived data")
	}
	p, ok := parser.ParseProvenanceParams(cmd)
	if !ok {
		return nil, fmt.Errorf("pgr() requires start=\"<process_guid>\"")
	}
	sql, err := h.provenanceScoreSQL(ctx, p, opts)
	if err != nil {
		return nil, err
	}
	return &resolvedSource{SQL: sql, Columns: provenanceColumns, NumericColumns: provenanceNumericColumns, Focus: p.Start}, nil
}

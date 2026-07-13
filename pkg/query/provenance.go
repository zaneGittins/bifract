package query

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bifract/pkg/parser"
)

// provenanceColumns are the flat columns the pgr() scored edge list exposes. pgr() is a source
// command (parser/source_command.go): it is resolved into a SQL subquery source and these
// resolve as bare columns for any downstream BQL (filter/aggregate/sort/table/pgraph).
var provenanceColumns = []string{"parent", "child", "label", "event_type", "anomaly_score", "log_id", "timestamp", "fractal_id", "command_line", "proc_user", "host"}

// provenanceNumericColumns is the subset of provenanceColumns that are already numeric in the
// subquery (so downstream numeric comparisons must not string-coerce them).
var provenanceNumericColumns = []string{"anomaly_score"}

// provenanceEmptyScoreSQL yields zero rows with the pgr output shape, so a query over an empty
// tree behaves correctly (count() -> 0, etc.) without special-casing every caller.
const provenanceEmptyScoreSQL = "SELECT '' AS parent, '' AS child, '' AS label, '' AS event_type, toFloat64(0) AS anomaly_score, '' AS log_id, '' AS timestamp, '' AS fractal_id, '' AS command_line, '' AS proc_user, '' AS host WHERE 1 = 0"

// provenanceScoreSQL runs pass 1 (tree traversal, collect guids) and returns the pass-2
// scored-edge SQL, which becomes the query's subquery source. Returns a zero-row stub when the
// tree is empty. The pass-2 SQL already carries `ORDER BY (event_type='spawn') DESC,
// anomaly_score DESC LIMIT`, so the spawn backbone is prioritized inside the subquery and an
// outer LIMIT never re-truncates process structure.
func (h *QueryHandler) provenanceScoreSQL(ctx context.Context, p parser.ProvenanceParams, opts parser.QueryOptions) (string, error) {
	// pgr is per-fractal / per-prism by definition; refuse to run unscoped (which would scan
	// every tenant's data). The handler always populates fractal for user queries.
	if opts.FractalID == "" && len(opts.FractalIDs) == 0 {
		return "", fmt.Errorf("pgr: fractal scoping is required")
	}
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
	if p.Reconnect && ctx.Err() == nil {
		reconSQL, rErr := parser.BuildReconnectionSQL(guids, p, opts)
		if rErr != nil {
			log.Printf("[pgr] build reconnection query: %v", rErr)
		} else if reconSQL != "" {
			// The reconnection SQL uses explicit GLOBAL IN / GLOBAL JOIN on its cross-distributed-table
			// subqueries (see BuildReconnectionSQL), so it runs on a cluster without the fragile
			// distributed_product_mode='global' setting (which errors code 36 on this query's shape).
			reconRows, qErr := h.db.QueryLowPriority(ctx, reconSQL)
			if qErr != nil {
				// Distinguish a cancelled/timed-out request (bail so the caller sees it) from a
				// genuine reconnection-feature failure (fall back to the base tree).
				if ctx.Err() != nil {
					return "", ctx.Err()
				}
				log.Printf("[pgr] reconnection lookup failed: %v", qErr)
			} else {
				peers := parseReconnectPeers(reconRows)
				combined = h.expandReconnectionPeers(ctx, guids, peers, opts)
				// Reconnection owns its bridge edges (AppendReconnectionEdges), so emit every
				// peer: the emit logic decides shape (net/dns converge on the object node,
				// file is writer->executor, injection/access are skipped -- pass-2 owns those).
				emitPeers = peers
			}
		}
	}

	scoreSQL, err := parser.BuildProvenanceScoringSQL(combined, p.Threshold, p.EdgeTypes, p.Diffuse, opts)
	if err != nil {
		return "", fmt.Errorf("pgr: build scoring query: %w", err)
	}
	if len(emitPeers) > 0 {
		scoreSQL = parser.AppendReconnectionEdges(scoreSQL, emitPeers)
	}
	if !p.Diffuse {
		return scoreSQL, nil
	}

	// Diffusion (NoDoze network-diffusion): the scoring SQL kept leaves down to a low floor (NOT
	// the user threshold). Execute it, propagate anomaly along the tree in Go, apply the threshold
	// to the PROPAGATED score, and re-emit the survivors as a literal subquery source. Doing this
	// server-side makes the propagated anomaly_score the value EVERY consumer sees -- table, count,
	// downstream BQL, and pgraph -- so an AI agent reading pgr() and an analyst viewing
	// pgr()|pgraph() get identical scores. Bounded (per-process cap + scan LIMIT keep the
	// intermediate set small). Non-fatal: on failure fall back to per-edge scoring (diffuse=false).
	//
	// Wrap in an outer SELECT before LIMIT: with reconnection on, scoreSQL ends in
	// `... UNION ALL SELECT <lit>`, and in ClickHouse `A UNION ALL B LIMIT n` binds the LIMIT to B
	// alone -- a bare append would NOT bound the whole set. The outer ORDER BY keeps spawn first so
	// the LIMIT only ever trims low-signal leaves, never process structure.
	scanSQL := fmt.Sprintf("SELECT * FROM (%s) AS _d ORDER BY (event_type = 'spawn') DESC, anomaly_score DESC LIMIT %d", scoreSQL, diffuseMaxScanRows)
	rows, qErr := h.db.QueryLowPriority(ctx, scanSQL)
	if qErr != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		log.Printf("[pgr] diffusion score query failed, falling back to per-edge scoring: %v", qErr)
		fb, ferr := parser.BuildProvenanceScoringSQL(combined, p.Threshold, p.EdgeTypes, false, opts)
		if ferr != nil {
			return "", fmt.Errorf("pgr: build scoring query: %w", ferr)
		}
		if len(emitPeers) > 0 {
			fb = parser.AppendReconnectionEdges(fb, emitPeers)
		}
		return fb, nil
	}
	survivors := diffuseProvenanceRows(rows, p.Threshold)
	sql, dropped, overflow := emitLiteralEdgeSource(survivors, provenanceColumns, provenanceNumericColumns, diffuseMaxEmitBytes)
	if overflow {
		// The spawn backbone alone exceeds the inline-literal budget (a very large tree). Re-emitting
		// it as literals would trip ClickHouse's max_query_size (error 62), so fall back to the
		// streamed per-edge scoring SQL, which carries no literal payload and scales to any size.
		// Diffusion (propagation) is lost for this one query, but pgr returns the full graph with
		// per-edge anomaly instead of failing -- and flat/pgraph stay in parity (both use this SQL).
		log.Printf("[pgr] diffusion payload too large to inline (%d edges); falling back to per-edge scoring for this query", len(survivors))
		fb, ferr := parser.BuildProvenanceScoringSQL(combined, p.Threshold, p.EdgeTypes, false, opts)
		if ferr != nil {
			return "", fmt.Errorf("pgr: build scoring query: %w", ferr)
		}
		if len(emitPeers) > 0 {
			fb = parser.AppendReconnectionEdges(fb, emitPeers)
		}
		return fb, nil
	}
	if dropped > 0 {
		log.Printf("[pgr] diffusion emitted a bounded subset: dropped %d low-signal leaf edges to fit the query-size limit", dropped)
	}
	return sql, nil
}

// diffuseMaxScanRows bounds the intermediate (pre-threshold) edge set pulled into Go for
// propagation, so a pathological tree cannot balloon memory. Spawn is ordered first, so a LIMIT
// trims low-signal leaves before structure.
const diffuseMaxScanRows = 20000

// diffuseProvenanceRows applies NoDoze network-diffusion to the scored edge rows IN GO. Anomaly is
// propagated along the spawn spine in SURPRISAL space (s = -log(1 - anomaly)): a common edge
// contributes ~0 so deep BENIGN chains stay cold (no length bias), while a chain of individually
// -common edges (LOLBins) compounds. Per process: S(v) = s(spawn edge into v) + LAMBDA*S(parent);
// the geometric decay bounds S so infinite chains converge. Each row's anomaly_score is overwritten
// with the propagated value 1 - exp(-S), then non-spawn edges below threshold are dropped (spawn is
// always kept so the full process tree is never truncated). Returns the surviving rows (filtered in
// place). Bounded O(V+E).
func diffuseProvenanceRows(rows []map[string]interface{}, threshold float64) []map[string]interface{} {
	const lambda, floor = 0.7, 0.01
	surp := func(a float64) float64 {
		m := 1 - a
		if m < floor {
			m = floor
		} else if m > 1 {
			m = 1
		}
		return -math.Log(m)
	}
	prop := func(s float64) float64 { return 1 - math.Exp(-s) }

	// Spawn structure + the raw anomaly of the spawn edge into each child.
	spawnKids := map[string][]string{}
	spawnAnom := map[string]float64{}
	isChild := map[string]bool{}
	isParent := map[string]bool{}
	for _, r := range rows {
		if reconString(r["event_type"]) != "spawn" {
			continue
		}
		pnt, chd := reconString(r["parent"]), reconString(r["child"])
		if pnt == "" || chd == "" {
			continue
		}
		spawnKids[pnt] = append(spawnKids[pnt], chd)
		spawnAnom[chd] = reconFloat(r["anomaly_score"])
		isChild[chd] = true
		isParent[pnt] = true
	}
	// Roots = spawn parents that are never a spawn child. Sorted for deterministic accumulation.
	roots := make([]string, 0)
	for p := range isParent {
		if !isChild[p] {
			roots = append(roots, p)
		}
	}
	sort.Strings(roots)
	// Accumulate S per process, iterative DFS with a seen-guard (no infinite loop on a malformed
	// multi-parent/cyclic tree).
	S := map[string]float64{}
	seen := map[string]bool{}
	type frame struct {
		g  string
		ps float64
	}
	stack := make([]frame, 0, len(roots))
	for _, r := range roots {
		stack = append(stack, frame{r, 0})
	}
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[f.g] {
			continue
		}
		seen[f.g] = true
		sg := lambda * f.ps // root has no incoming spawn edge -> starts at 0
		if a, ok := spawnAnom[f.g]; ok {
			sg = surp(a) + lambda*f.ps
		}
		S[f.g] = sg
		for _, k := range spawnKids[f.g] {
			stack = append(stack, frame{k, sg})
		}
	}
	// Rewrite each row's score to the propagated value; drop sub-threshold non-spawn edges.
	out := rows[:0]
	for _, r := range rows {
		et := reconString(r["event_type"])
		raw := reconFloat(r["anomaly_score"])
		var pr float64
		if et == "spawn" {
			pr = prop(S[reconString(r["child"])]) // S(child) already includes this spawn edge
		} else {
			pr = prop(surp(raw) + lambda*S[reconString(r["parent"])])
		}
		// Diffusion must only ever PROMOTE. The surprisal floor (log(0) guard) would otherwise
		// nudge a raw ~1.0 (never-seen) edge down to ~0.996; clamp so a propagated score is never
		// below the edge's own rarity. Also keeps a never-seen edge exactly 1.0 so threshold=1 works.
		if pr < raw {
			pr = raw
		}
		pr = math.Round(pr*10000) / 10000
		if et != "spawn" && pr < threshold {
			continue
		}
		r["anomaly_score"] = pr
		out = append(out, r)
	}
	// Spawn backbone first, then most anomalous, so a flat pgr() table reads most-suspicious-first.
	sort.SliceStable(out, func(i, j int) bool {
		si, sj := reconString(out[i]["event_type"]) == "spawn", reconString(out[j]["event_type"]) == "spawn"
		if si != sj {
			return si
		}
		return reconFloat(out[i]["anomaly_score"]) > reconFloat(out[j]["anomaly_score"])
	})
	return out
}

// diffuseMaxEmitBytes bounds the generated literal-source SQL. ClickHouse rejects any query whose
// text exceeds max_query_size (default 262144 bytes) with error 62 before it even parses, so the
// inline UNION-ALL-of-literals must stay under it -- with headroom for the downstream wrapper
// (pgraph/table/sort) that composes over this source. See emitLiteralEdgeSource.
const diffuseMaxEmitBytes = 230000

// emitLiteralEdgeSource renders edge rows as a UNION ALL of SELECT literals so the propagated
// result can serve as a source subquery -- downstream BQL composes over it exactly as it would
// over the scoring SQL. Numeric columns are emitted typed (toFloat64) so comparisons stay numeric;
// string columns are escaped for ClickHouse literals. Empty set -> the zero-row stub.
//
// The generated text is BOUNDED to maxBytes so it can never trip ClickHouse's max_query_size
// (error 62) on a large tree. rows must be spawn-first (diffuseProvenanceRows guarantees this):
// the spawn backbone (process structure) is always emitted, and low-signal leaf edges beyond the
// budget are dropped (returned as droppedLeaves so the caller can log it). If the spawn backbone
// alone exceeds the budget the tree is too large to inline as literals -- overflow=true signals
// the caller to fall back to streamed per-edge scoring (which carries no literal payload).
//
// Column aliases (AS name) are written only on the first UNION member; ClickHouse takes the result
// column names from the first SELECT and matches the rest positionally, which roughly halves the
// per-row text so far more of the tree fits under the budget.
func emitLiteralEdgeSource(rows []map[string]interface{}, cols, numericCols []string, maxBytes int) (sql string, droppedLeaves int, overflow bool) {
	if len(rows) == 0 {
		return provenanceEmptyScoreSQL, 0, false
	}
	numeric := map[string]bool{}
	for _, c := range numericCols {
		numeric[c] = true
	}
	repl := strings.NewReplacer("\\", "\\\\", "'", "\\'", "\n", "\\n", "\r", "\\r", "\t", "\\t")
	var sb strings.Builder
	emitted := 0
	emitRow := func(r map[string]interface{}) {
		if emitted > 0 {
			sb.WriteString(" UNION ALL ")
		}
		sb.WriteString("SELECT ")
		for j, c := range cols {
			if j > 0 {
				sb.WriteString(", ")
			}
			if numeric[c] {
				sb.WriteString("toFloat64(")
				sb.WriteString(strconv.FormatFloat(reconFloat(r[c]), 'f', -1, 64))
				sb.WriteString(")")
			} else {
				sb.WriteString("'")
				sb.WriteString(repl.Replace(reconString(r[c])))
				sb.WriteString("'")
			}
			if emitted == 0 { // only the first UNION member names the columns
				sb.WriteString(" AS ")
				sb.WriteString(c)
			}
		}
		emitted++
	}

	spawnBytes := 0
	for _, r := range rows {
		if reconString(r["event_type"]) == "spawn" {
			emitRow(r) // structure is never dropped
			spawnBytes = sb.Len()
			continue
		}
		// Leaf edge: keep only while under budget. Leaves are sorted most-anomalous-first, so the
		// dropped tail is the lowest-signal noise.
		if sb.Len() >= maxBytes {
			droppedLeaves++
			continue
		}
		emitRow(r)
	}
	if spawnBytes > maxBytes {
		return "", 0, true // backbone alone won't fit; caller falls back to streamed scoring
	}
	return sb.String(), droppedLeaves, false
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
			PeerHost:    reconString(r["peer_host"]),
		}
		if pe.PeerGUID == "" {
			continue
		}
		peers = append(peers, pe)
	}
	return peers
}

// expandReconnectionPeers ranks peers by bridge severity and traverses a shallow descendant
// subtree for the top reconnectMaxExpand of them (>= reconnectHighAnomaly), merging those guids
// into the tree set (capped). Ordering is deterministic (anomaly desc, then guid) so the same
// query yields the same result. The per-peer traversals run concurrently; failures are logged
// and skipped (best-effort). Returns the combined guid set.
func (h *QueryHandler) expandReconnectionPeers(ctx context.Context, treeGuids []string, peers []parser.ReconnectPeer, opts parser.QueryOptions) []string {
	combined := append([]string(nil), treeGuids...)
	if ctx.Err() != nil || len(combined) >= reconnectExpandMaxGuids {
		return combined
	}

	// Best anomaly per distinct peer.
	best := map[string]float64{}
	order := make([]string, 0, len(peers))
	for _, pe := range peers {
		if _, ok := best[pe.PeerGUID]; !ok {
			order = append(order, pe.PeerGUID)
			best[pe.PeerGUID] = pe.Anomaly
		} else if pe.Anomaly > best[pe.PeerGUID] {
			best[pe.PeerGUID] = pe.Anomaly
		}
	}
	// Deterministic rank: highest anomaly first, guid as a stable tiebreak (anomalies are
	// coarse per-type constants, so ties are common and would otherwise be order-dependent).
	sort.Slice(order, func(i, j int) bool {
		if best[order[i]] != best[order[j]] {
			return best[order[i]] > best[order[j]]
		}
		return order[i] < order[j]
	})

	// Select the top qualifying peers to expand.
	var toExpand []string
	for _, g := range order {
		if len(toExpand) >= reconnectMaxExpand || best[g] < reconnectHighAnomaly {
			break
		}
		toExpand = append(toExpand, g)
	}
	if len(toExpand) == 0 {
		return combined
	}

	// Traverse each peer's shallow subtree concurrently (h.db is a pooled client). Results are
	// merged in toExpand order so the outcome stays deterministic regardless of finish order.
	results := make([][]map[string]interface{}, len(toExpand))
	var wg sync.WaitGroup
	for i, g := range toExpand {
		pp := parser.ProvenanceParams{Start: g, Depth: reconnectExpandDepth, Direction: "forward"}
		sql, err := parser.BuildProcessTreeQuery(pp, opts)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(idx int, guid, q string) {
			defer wg.Done()
			rows, qErr := h.db.QueryLowPriority(ctx, q)
			if qErr != nil {
				log.Printf("[pgr] expand peer %s: %v", guid, qErr)
				return
			}
			results[idx] = rows
		}(i, g, sql)
	}
	wg.Wait()

	seen := make(map[string]bool, len(treeGuids))
	for _, g := range treeGuids {
		seen[g] = true
	}
	for _, rows := range results {
		for _, r := range rows {
			sg := reconString(r["process_guid"])
			if sg == "" || seen[sg] {
				continue
			}
			seen[sg] = true
			combined = append(combined, sg)
			if len(combined) >= reconnectExpandMaxGuids {
				log.Printf("[pgr] reconnection expansion hit guid cap (%d)", reconnectExpandMaxGuids)
				return combined
			}
		}
	}
	return combined
}

func reconString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// reconFloat coerces a ClickHouse-driver value to float64. anomaly is emitted via toFloat64(...)
// so it arrives as float64 today; the string/float32 fallbacks keep ranking working if the
// driver ever returns it differently (rather than silently collapsing every peer to 0).
func reconFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
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
	// Diffusion (when enabled) is applied server-side in provenanceScoreSQL, so the propagated
	// anomaly_score is what every consumer sees -- pgraph() is a pure renderer of these rows.
	sql, err := h.provenanceScoreSQL(ctx, p, opts)
	if err != nil {
		return nil, err
	}
	return &resolvedSource{SQL: sql, Columns: provenanceColumns, NumericColumns: provenanceNumericColumns, Focus: p.Start}, nil
}

package query

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// provenanceColumns are the flat columns the pgr() scored edge list exposes. pgr() is a source
// command (parser/source_command.go): it is resolved into a SQL subquery source and these
// resolve as bare columns for any downstream BQL (filter/aggregate/sort/table/pgraph).
// parent_label is the parent node's image, emitted only where the parent may have no row of its
// own (a spawn row's out-of-range ancestor, a reconnected peer) so the viz can name it instead of
// showing a bare guid. See parser.ProvenanceColumns for the explanation columns.
var provenanceColumns = parser.ProvenanceColumns

// provenanceNumericColumns is the subset of provenanceColumns that are already numeric in the
// subquery (so downstream numeric comparisons must not string-coerce them).
var provenanceNumericColumns = parser.ProvenanceNumericColumns

// provenanceEmptyScoreSQL yields zero rows with the pgr output shape, so a query over an empty
// tree behaves correctly (count() -> 0, etc.) without special-casing every caller.
var provenanceEmptyScoreSQL = func() string {
	numeric := map[string]bool{}
	for _, c := range provenanceNumericColumns {
		numeric[c] = true
	}
	parts := make([]string, len(provenanceColumns))
	for i, c := range provenanceColumns {
		v := "''"
		if numeric[c] {
			v = "toFloat64(0)"
		}
		parts[i] = v + " AS " + c
	}
	return "SELECT " + strings.Join(parts, ", ") + " WHERE 1 = 0"
}()

// provenanceScoreSQL runs pgr end to end and returns its rows as a literal subquery source.
// Pass 1 walks the tree (collectProvenanceTree), reconnection adds peers, pass 2 scores every
// edge in ClickHouse, diffusion (when on) propagates in Go, and the result is budgeted to
// p.Limit and re-emitted. Materializing here means every consumer (table, count, downstream BQL,
// pgraph) reads the same rows, and pgr knows what it had to leave out: warnings say so.
func (h *QueryHandler) provenanceScoreSQL(ctx context.Context, p parser.ProvenanceParams, opts parser.QueryOptions) (string, []string, error) {
	// pgr is per-fractal / per-prism by definition; refuse to run unscoped (which would scan
	// every tenant's data). The handler always populates fractal for user queries.
	if opts.FractalID == "" && len(opts.FractalIDs) == 0 {
		return "", nil, fmt.Errorf("pgr: fractal scoping is required")
	}
	tree, err := collectProvenanceTree(ctx, h.db, p, opts, parser.MaxProvenanceNodes)
	if err != nil {
		return "", nil, fmt.Errorf("pgr: tree pass: %w", err)
	}
	if tree == nil {
		return provenanceEmptyScoreSQL, nil, nil
	}

	// Fleet-wide inputs: the fleet size, the reconnection rarity-gate ceilings and the baseline's
	// first day. Once the backfill has filled provenance_summary they are one lookup; until then,
	// or when that lookup fails, each is a scan of the fractal, as below.
	sum, summarized := h.readProvenanceSummary(ctx, opts)
	if ctx.Err() != nil {
		return "", nil, ctx.Err()
	}

	// Scanned, fleet size depends on nothing else, so it runs alongside reconnection and the edge
	// keys. Buffered, so an early return never leaks the goroutine.
	type fleetResult struct {
		rows []map[string]interface{}
		err  error
	}
	var fleet chan fleetResult
	if !summarized {
		fleetCtx, cancelFleet := context.WithCancel(ctx)
		defer cancelFleet() // an early return stops the scan
		fleet = make(chan fleetResult, 1)
		fleetSQL := parser.BuildProvenanceTotalHostsSQL(opts)
		go func() {
			rows, err := h.db.QueryProvenance(fleetCtx, fleetSQL)
			fleet <- fleetResult{rows, err}
		}()
	}

	// Cross-tree reconnection (NoDoze object-mediated): find peer processes in OTHER trees
	// that share a leaf with this subgraph, auto-expand the most anomalous ones into real
	// subtrees, and emit bridge edges for the rest. Bounded and rarity-pruned; see
	// parser.BuildReconnectionSQL. Failure is non-fatal -- pgr still returns the base tree.
	//
	// The rarity-gate ceilings (totalHosts/totalImages) are fetched once here and passed in as
	// literals rather than live scalar subqueries. On failure both stay 0, which only tightens
	// the gate to its absolute floor (reconnectHostPrevalenceMax/reconnectImagePrevalenceMax):
	// it fails toward under-reconnecting, never toward a wrong match.
	var bridges []map[string]interface{}
	var peers []parser.ReconnectPeer
	if p.Reconnect && ctx.Err() == nil {
		reconTotalHosts, reconTotalImages := sum.reconHosts, sum.reconImages
		if !summarized {
			totRows, tErr := h.db.QueryProvenance(ctx, parser.BuildReconnectionTotalsSQL(opts))
			switch {
			case tErr != nil && ctx.Err() != nil:
				return "", nil, ctx.Err()
			case tErr != nil:
				log.Printf("[pgr] reconnection totals lookup failed, using rarity-gate floor only: %v", tErr)
			case len(totRows) > 0:
				reconTotalHosts = reconInt64(totRows[0]["total_hosts"])
				reconTotalImages = reconInt64(totRows[0]["total_images"])
			}
		}
		if reconSQL := parser.BuildReconnectionSQL(p, reconTotalHosts, reconTotalImages, opts); reconSQL != "" {
			// The reconnection SQL uses explicit GLOBAL IN / GLOBAL JOIN on its cross-distributed-table
			// subqueries (see BuildReconnectionSQL), so it runs on a cluster without the fragile
			// distributed_product_mode='global' setting (which errors code 36 on this query's shape).
			reconRows, qErr := h.db.QueryProvenance(ctx, reconSQL, tree.table())
			if qErr != nil {
				// Distinguish a cancelled/timed-out request (bail so the caller sees it) from a
				// genuine reconnection-feature failure (fall back to the base tree).
				if ctx.Err() != nil {
					return "", nil, ctx.Err()
				}
				log.Printf("[pgr] reconnection lookup failed: %v", qErr)
			} else {
				peers = rankAndCapReconnectPeers(parseReconnectPeers(reconRows), p.MaxPeers)
				h.expandReconnectionPeers(ctx, tree, peers, opts)
				// The lookup names whichever member touched the artifact; the graph draws its node.
				for i := range peers {
					peers[i].SrcGUID = tree.node(peers[i].SrcGUID)
					peers[i].PeerGUID = tree.node(peers[i].PeerGUID)
				}
				// Reconnection owns its bridge edges, so emit every peer: the emit logic decides
				// shape (net/dns converge on the object node, file is writer->executor,
				// injection/access are skipped -- pass-2 owns those).
				bridges = parser.ReconnectionEdgeRows(peers)
			}
		}
	}
	treeTable := tree.table()

	baseline := parser.ProvenanceBaseline{TotalHosts: sum.totalHosts, FirstDay: sum.firstDay}
	baseline.NodeStart, baseline.NodeEnd = tree.span()

	// The tree's own edge keys, which scope every proc_freq CTE in the scoring SQL to the rows its
	// joins can match (see parser.ProvenanceBaseline). Without them each CTE aggregates the
	// fractal's entire proc_freq, which grows with the retention window rather than with the tree.
	// Failure or overflow is non-fatal and leaves the keys empty: scoring then runs against the
	// unrestricted baseline, which is slower but returns the same scores.
	if ctx.Err() == nil {
		if keyRows, qErr := h.db.QueryProvenance(ctx, parser.BuildProvenanceEdgeKeysSQL(p.EdgeTypes, opts), treeTable); qErr != nil {
			if ctx.Err() != nil {
				return "", nil, ctx.Err()
			}
			log.Printf("[pgr] edge-key lookup failed, scoring against the unrestricted baseline: %v", qErr)
		} else if len(keyRows) > parser.MaxProvenanceEdgeKeys {
			log.Printf("[pgr] tree has more than %d distinct edge keys; scoring against the unrestricted baseline", parser.MaxProvenanceEdgeKeys)
		} else {
			baseline.SrcKeys, baseline.TgtKeys = provenanceEdgeKeys(keyRows)
		}
	}

	// Fleet size, reported as total_hosts: passed as a literal, never cached across calls. Display
	// only, so a failed lookup leaves it 0 and changes no score.
	if fleet != nil {
		fs := <-fleet
		switch {
		case fs.err != nil && ctx.Err() != nil:
			return "", nil, ctx.Err()
		case fs.err != nil:
			log.Printf("[pgr] total-hosts lookup failed, total_hosts will read 0: %v", fs.err)
		case len(fs.rows) > 0:
			baseline.TotalHosts = reconInt64(fs.rows[0]["total_hosts"])
		}
	}

	// The scoring SQL's leaf cap reads opts.MaxRows, but pgr() carries its own row budget
	// (p.Limit) precisely because the deployment MaxQueryRows can be far lower than a tree needs.
	scoreOpts := opts
	if p.Limit > 0 {
		scoreOpts.MaxRows = p.Limit
	}

	// Diffusion (NoDoze network-diffusion): the scoring SQL keeps leaves down to a low floor (NOT
	// the user threshold) so diffuseProvenanceRows can promote them, then applies the threshold to
	// the PROPAGATED score. A failed diffusion scan falls back to per-edge scoring.
	diffuse := p.Diffuse
	rows, err := h.scanProvenance(ctx, parser.BuildProvenanceScoringSQL(p.Threshold, p.EdgeTypes, diffuse, baseline, scoreOpts), treeTable)
	if err != nil && diffuse && ctx.Err() == nil {
		log.Printf("[pgr] diffusion score query failed, falling back to per-edge scoring: %v", err)
		diffuse = false
		rows, err = h.scanProvenance(ctx, parser.BuildProvenanceScoringSQL(p.Threshold, p.EdgeTypes, false, baseline, scoreOpts), treeTable)
	}
	if err != nil {
		if ctx.Err() != nil {
			return "", nil, ctx.Err()
		}
		return "", nil, fmt.Errorf("pgr: scoring pass: %w", err)
	}
	scanned := len(rows)
	rows = append(rows, bridges...)
	if diffuse {
		rows = diffuseProvenanceRows(rows, p.Threshold, p.Lambda)
		h.recordProvenanceScores(rows)
	}
	annotateProvenanceGroups(rows, tree)
	classifyReconnections(rows, peers)

	var warnings []string
	if tree.truncated {
		warnings = append(warnings, fmt.Sprintf("pgr: process tree truncated by its size caps at %d nodes (%d processes); narrow the time range or lower depth=", len(tree.order), tree.nMember))
	}
	if scanned >= provenanceMaxScanRows {
		warnings = append(warnings, fmt.Sprintf("pgr: scored more edges than the %d it can rank; the least anomalous leaves were dropped first", provenanceMaxScanRows))
	}
	kept := budgetProvenanceRows(rows, tree, p.Limit)
	sql, emitted := emitProvenanceSource(kept, provenanceEmitMaxBytes)
	if w := provenanceRowWarning(len(rows), len(kept), emitted); w != "" {
		warnings = append(warnings, w)
	}
	return sql, warnings, nil
}

// provenanceSummary is pgr's fleet-wide input read from provenance_summary.
type provenanceSummary struct {
	totalHosts, reconHosts, reconImages int64
	firstDay                            time.Time
}

// readProvenanceSummary reads the fleet-wide inputs once the backfill has filled the summary.
// It reports false when it has not, or the read fails, and the caller scans instead.
func (h *QueryHandler) readProvenanceSummary(ctx context.Context, opts parser.QueryOptions) (provenanceSummary, bool) {
	if !h.provenanceSummaryFilled(ctx) {
		return provenanceSummary{}, false
	}
	rows, err := h.db.QueryProvenance(ctx, parser.BuildProvenanceSummarySQL(opts))
	if err == nil && len(rows) == 0 {
		err = errors.New("no rows")
	}
	var day time.Time
	if err == nil {
		day, err = time.Parse(time.DateOnly, reconString(rows[0]["first_day"]))
	}
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[pgr] provenance summary read failed, scanning instead: %v", err)
		}
		return provenanceSummary{}, false
	}
	r := rows[0]
	return provenanceSummary{totalHosts: reconInt64(r["total_hosts"]), reconHosts: reconInt64(r["recon_hosts"]),
		reconImages: reconInt64(r["recon_images"]), firstDay: day}, true
}

// provenanceSummaryCheckEvery bounds how often pgr re-reads whether the summary is filled,
// and provenanceSummaryCheckTimeout how long one read may hold up a pgr call.
const (
	provenanceSummaryCheckEvery   = time.Minute
	provenanceSummaryCheckTimeout = 2 * time.Second
)

// summaryFilledCheck is one cached answer to whether provenance_summary is filled.
type summaryFilledCheck struct {
	filled  bool
	checked time.Time
}

// provenanceSummaryFilled reports whether the backfill has filled provenance_summary. The
// answer is re-read at most once a minute; a failed read counts as unfilled, so pgr scans.
func (h *QueryHandler) provenanceSummaryFilled(ctx context.Context) bool {
	if h.pg == nil {
		return false
	}
	if c := h.summaryFilled.Load(); c != nil && time.Since(c.checked) < provenanceSummaryCheckEvery {
		return c.filled
	}
	readCtx, cancel := context.WithTimeout(ctx, provenanceSummaryCheckTimeout)
	defer cancel()
	filled, err := h.pg.ProvenanceSummaryFilled(readCtx)
	if err != nil {
		if ctx.Err() != nil {
			return false // the caller gave up; nothing was learned
		}
		log.Printf("[pgr] provenance summary state unreadable, scanning instead: %v", err)
	}
	h.summaryFilled.Store(&summaryFilledCheck{filled: err == nil && filled, checked: time.Now()})
	return err == nil && filled
}

// provenanceRowWarning says when rows were left out, and by which limit: limit= (kept < total)
// or the literal-size bound (emitted < kept), which raising limit= cannot help.
func provenanceRowWarning(total, kept, emitted int) string {
	switch {
	case emitted < kept:
		return fmt.Sprintf("pgr: result too large to return in full, showing %d of %d edges; lower limit= or narrow the time range", emitted, total)
	case kept < total:
		return fmt.Sprintf("pgr: row limit reached, showing %d of %d edges; raise limit= to see more", emitted, total)
	}
	return ""
}

// scanProvenance executes a scoring SQL with the tree attached, spawn first, bounded so a
// pathological tree cannot balloon memory. The outer SELECT wraps before LIMIT so a UNION-ending
// statement is bounded as a whole.
func (h *QueryHandler) scanProvenance(ctx context.Context, scoreSQL string, tree storage.ExternalTable) ([]map[string]interface{}, error) {
	return h.db.QueryProvenance(ctx, fmt.Sprintf("SELECT * FROM (%s) AS _d ORDER BY (event_type = 'spawn') DESC, anomaly_score DESC LIMIT %d", scoreSQL, provenanceMaxScanRows), tree)
}

// provenanceMaxScanRows bounds the scored edge set pulled into Go (pre-threshold when diffusing),
// so a pathological tree cannot balloon memory. Spawn is ordered first, so the LIMIT trims
// low-signal leaves before structure; filling it is reported as a warning.
const provenanceMaxScanRows = 20000

// diffuseProvenanceRows applies NoDoze network-diffusion to the scored edge rows IN GO. Anomaly is
// propagated along the spawn spine in SURPRISAL space (s = -log(1 - anomaly)): a common edge
// contributes ~0 so deep BENIGN chains stay cold (no length bias), while a chain of individually
// -common edges (LOLBins) compounds. Per process: S(v) = s(spawn edge into v) + lambda*S(parent);
// the geometric decay bounds S so infinite chains converge. Each row's anomaly_score is overwritten
// with the propagated value 1 - exp(-S), then non-spawn edges below threshold are dropped (spawn is
// always kept so the full process tree is never truncated). Returns the surviving rows (filtered in
// place). Bounded O(V+E).
//
// lambda (caller-supplied -- see ProvenanceParams.Lambda / lambda=, default defaultDiffuseLambda)
// governs decay: too slow (this was a hardcoded 0.7) makes S(v)'s geometric series converge to a
// fairly high steady-state for ANY sufficiently deep chain of only-modestly-anomalous edges, not
// just a genuinely concentrated one -- on a thin baseline (limited proc_freq historical volume
// means plenty of legitimately-benign relationships read as "somewhat rare"), that compounds into
// most of a deep tree painting red regardless of whether anything suspicious is actually
// happening. lambda=0 makes propagation a no-op (propagated score == raw score, i.e. diffuse=true
// collapses toward diffuse=false's output). Made tunable per-query rather than fixed, since the
// right decay is a property of this deployment's baseline, not a universal constant.
func diffuseProvenanceRows(rows []map[string]interface{}, threshold, lambda float64) []map[string]interface{} {
	const floor = 0.01
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

// provenanceEmitMaxBytes bounds the literal source provenanceScoreSQL emits. The connection's
// max_query_size is 16MB (storage.maxGeneratedQuerySize); this leaves room for the downstream
// pipeline that wraps it. See emitProvenanceSource.
const provenanceEmitMaxBytes = 4 << 20

// emitProvenanceSource renders rows as one values() table function, so the materialized result
// can serve as a source subquery that downstream BQL composes over exactly like the scoring SQL.
// One values() call is a single literal block the server parses in one pass, where a UNION ALL of
// per-row SELECTs plans every row as its own query. Numeric columns are typed Float64 so
// comparisons stay numeric. Rows are emitted in order until maxBytes; emitted says how many fit,
// so callers order by priority (budgetProvenanceRows) first. No rows yields the zero-row stub.
func emitProvenanceSource(rows []map[string]interface{}, maxBytes int) (sql string, emitted int) {
	if len(rows) == 0 {
		return provenanceEmptyScoreSQL, 0
	}
	numeric := map[string]bool{}
	for _, c := range provenanceNumericColumns {
		numeric[c] = true
	}
	structure := make([]string, len(provenanceColumns))
	for i, c := range provenanceColumns {
		typ := "String"
		if numeric[c] {
			typ = "Float64"
		}
		structure[i] = c + " " + typ
	}
	var sb strings.Builder
	sb.WriteString("SELECT * FROM values('" + strings.Join(structure, ", ") + "'")
	head := sb.Len()
	for _, r := range rows {
		if emitted > 0 && sb.Len()-head >= maxBytes {
			break
		}
		sb.WriteString(", (")
		for j, c := range provenanceColumns {
			if j > 0 {
				sb.WriteString(", ")
			}
			if numeric[c] {
				f := reconFloat(r[c])
				if math.IsNaN(f) || math.IsInf(f, 0) {
					f = 0
				}
				sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
			} else {
				sb.WriteString(parser.SQLStringLiteral(reconString(r[c])))
			}
		}
		sb.WriteString(")")
		emitted++
	}
	sb.WriteString(")")
	return sb.String(), emitted
}

// budgetProvenanceRows picks the rows that fit pgr's limit= when there are more. Structure is
// kept in walk order (seed and ancestors, then the nearest descendants, then peers), but a quarter
// of the budget is held for leaves and bridges, so a wide tree can never push every file, network
// and DNS edge out. Bridges go before leaves, leaves by anomaly, and a leaf is kept only when its
// process is drawn. Room left in either share goes to the other. The result is in emit priority.
func budgetProvenanceRows(rows []map[string]interface{}, tree *provenanceTree, limit int) []map[string]interface{} {
	if limit <= 0 || len(rows) <= limit {
		return rows
	}
	rank := make(map[string]int, len(tree.order))
	for i, n := range tree.order {
		rank[n] = i
	}
	rankOf := func(r map[string]interface{}) int {
		if i, ok := rank[reconString(r["child"])]; ok {
			return i
		}
		return len(rank)
	}
	var spawn, other []map[string]interface{}
	for _, r := range rows {
		if reconString(r["event_type"]) == "spawn" {
			spawn = append(spawn, r)
		} else {
			other = append(other, r)
		}
	}
	sort.SliceStable(spawn, func(i, j int) bool { return rankOf(spawn[i]) < rankOf(spawn[j]) })
	isBridge := func(r map[string]interface{}) bool { return reconString(r["score_basis"]) == "reconnect" }
	sort.SliceStable(other, func(i, j int) bool {
		if bi, bj := isBridge(other[i]), isBridge(other[j]); bi != bj {
			return bi
		}
		return reconFloat(other[i]["anomaly_score"]) > reconFloat(other[j]["anomaly_score"])
	})

	out := make([]map[string]interface{}, 0, limit)
	drawn := map[string]bool{}
	nextSpawn := 0
	takeSpawn := func(upTo int) {
		for ; nextSpawn < len(spawn) && len(out) < upTo; nextSpawn++ {
			r := spawn[nextSpawn]
			drawn[reconString(r["parent"])], drawn[reconString(r["child"])] = true, true
			out = append(out, r)
		}
	}
	used := make([]bool, len(other))
	takeOther := func() {
		for i, r := range other {
			if len(out) >= limit {
				return
			}
			// A parent outside the tree (a peer, an object node) has no spawn row to wait for.
			if p := reconString(r["parent"]); !used[i] && (drawn[p] || !tree.has(p)) {
				used[i] = true
				out = append(out, r)
			}
		}
	}
	takeSpawn(limit - min(limit/4, len(other)))
	takeOther()
	takeSpawn(limit)
	takeOther()
	return out
}

// annotateProvenanceGroups fills the group columns of each spawn row whose child is a collapsed
// node: how many processes it stands for and when the first and last were created.
func annotateProvenanceGroups(rows []map[string]interface{}, tree *provenanceTree) {
	for _, r := range rows {
		if reconString(r["event_type"]) != "spawn" {
			continue
		}
		child := reconString(r["child"])
		if n := tree.size[child]; n > 1 {
			r["group_size"] = float64(n)
			if first, last := tree.groupSpan(child); first != "" {
				r["group_first"], r["group_last"] = first, last
			}
		}
	}
}

// Reconnection expansion caps (orchestration side; SQL-side caps live in pkg/parser).
const (
	reconnectMaxExpand      = 5    // high-anomaly peers auto-expanded into real subtrees
	reconnectExpandDepth    = 3    // shallow descendant depth per expanded peer
	reconnectExpandMaxNodes = 2000 // hard cap on total nodes after expansion
	reconnectHighAnomaly    = 0.8  // only peers at/above this bridge severity expand
)

// provenanceEdgeKeys splits the (fkey_src, fkey_tgt) pair rows into the two deduplicated key
// sets the scoring CTEs are scoped by. Every value is kept, empty ones included: an empty key
// joins nothing in practice, but carrying it costs one IN-list entry and keeps the restricted
// baseline provably identical to the unrestricted one rather than identical-in-the-cases-checked.
func provenanceEdgeKeys(rows []map[string]interface{}) (src, tgt []string) {
	seenSrc, seenTgt := map[string]bool{}, map[string]bool{}
	for _, r := range rows {
		if v := reconString(r["fkey_src"]); !seenSrc[v] {
			seenSrc[v] = true
			src = append(src, v)
		}
		if v := reconString(r["fkey_tgt"]); !seenTgt[v] {
			seenTgt[v] = true
			tgt = append(tgt, v)
		}
	}
	return src, tgt
}

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

// reconPeerArtifact is the shared object a peer row bridges on: the node id for net/dns, the
// path for file. Injection/access rows carry neither, so they key on the peer itself (one
// bridge each).
func reconPeerArtifact(pe parser.ReconnectPeer) string {
	switch {
	case pe.ObjectID != "":
		return pe.ObjectID
	case pe.Label != "":
		return pe.ReconType + "\x00" + pe.Label
	}
	return pe.ReconType + "\x00" + pe.PeerGUID
}

// rankAndCapReconnectPeers admits at most maxPeers DISTINCT peer processes, keeping every row
// of the ones it admits. This is the structural cap: only the top reconnectMaxExpand peers get
// expanded into subtrees, so every other admitted peer lands in the graph parentless -- its own
// root, its own tree. Uncapped, the per-type SQL limit allows 200 peers x 5 recon types = 1,000
// extra trees, which is unreadable regardless of how the count is displayed.
//
// Rank is by how many DISTINCT rare artifacts a peer shares with the tree: a peer reached
// through three rare IOCs is a far stronger bridge than one reached through a single IP. That
// signal is free from the rows themselves and is the only discriminator available here --
// ReconnectPeer.Anomaly is a per-type constant (file 1.0 > access 0.9 > net/dns 0.85), so it
// only orders recon types, and on its own leaves the choice of WHICH 50 net peers to keep as
// GUID-alphabetical. Ties fall back to anomaly, then guid, so the ordering stays deterministic.
func rankAndCapReconnectPeers(peers []parser.ReconnectPeer, maxPeers int) []parser.ReconnectPeer {
	if maxPeers <= 0 {
		maxPeers = parser.DefaultReconnectPeers
	}
	artifacts := map[string]map[string]bool{}
	best := map[string]float64{}
	order := make([]string, 0, len(peers))
	for _, pe := range peers {
		if _, ok := artifacts[pe.PeerGUID]; !ok {
			order = append(order, pe.PeerGUID)
			artifacts[pe.PeerGUID] = map[string]bool{}
		}
		artifacts[pe.PeerGUID][reconPeerArtifact(pe)] = true
		if pe.Anomaly > best[pe.PeerGUID] {
			best[pe.PeerGUID] = pe.Anomaly
		}
	}
	if len(order) <= maxPeers {
		return peers
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if len(artifacts[a]) != len(artifacts[b]) {
			return len(artifacts[a]) > len(artifacts[b])
		}
		if best[a] != best[b] {
			return best[a] > best[b]
		}
		return a < b
	})
	keep := make(map[string]bool, maxPeers)
	for _, g := range order[:maxPeers] {
		keep[g] = true
	}
	log.Printf("[pgr] reconnection admitted %d of %d peer processes (peers=%d)", maxPeers, len(order), maxPeers)
	out := peers[:0:0]
	for _, pe := range peers {
		if keep[pe.PeerGUID] {
			out = append(out, pe)
		}
	}
	return out
}

// expandReconnectionPeers ranks peers by bridge severity and walks a shallow descendant subtree
// for the top reconnectMaxExpand of them (>= reconnectHighAnomaly), merging those nodes into tree
// (capped at reconnectExpandMaxNodes). Ordering is deterministic (anomaly desc, then guid) so the
// same query yields the same result. The per-peer walks run concurrently; failures are logged and
// skipped (best-effort).
func (h *QueryHandler) expandReconnectionPeers(ctx context.Context, tree *provenanceTree, peers []parser.ReconnectPeer, opts parser.QueryOptions) {
	if ctx.Err() != nil || len(tree.order) >= reconnectExpandMaxNodes {
		return
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
		return
	}

	// Walk each peer's shallow subtree concurrently (h.db is a pooled client) with the same
	// collapsing walk as pass 1. Results merge in toExpand order so the outcome stays
	// deterministic regardless of finish order.
	results := make([]*provenanceTree, len(toExpand))
	var wg sync.WaitGroup
	for i, g := range toExpand {
		pp := parser.ProvenanceParams{Start: g, Depth: reconnectExpandDepth, Direction: "forward"}
		wg.Add(1)
		go func(idx int, guid string, params parser.ProvenanceParams) {
			defer wg.Done()
			sub, qErr := collectProvenanceTree(ctx, h.db, params, opts, reconnectExpandMaxNodes)
			if qErr != nil {
				log.Printf("[pgr] expand peer %s: %v", guid, qErr)
				return
			}
			results[idx] = sub
		}(i, g, pp)
	}
	wg.Wait()

	for _, sub := range results {
		if sub == nil {
			continue
		}
		for _, node := range sub.order {
			tree.add(sub.members[node], sub.size[node], sub.firstMs[node], sub.lastMs[node], reconnectExpandMaxNodes)
			if len(tree.order) >= reconnectExpandMaxNodes {
				log.Printf("[pgr] reconnection expansion hit node cap (%d)", reconnectExpandMaxNodes)
				return
			}
		}
	}
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

// reconInt64 coerces a ClickHouse-driver value to int64. uniqExact/length(...) return UInt64
// today; the int/string fallbacks keep the total-hosts/total-images lookups working if the
// driver ever returns them differently (rather than silently zeroing the rarity-gate inputs).
func reconInt64(v interface{}) int64 {
	switch n := v.(type) {
	case uint64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}

// resolvedSource is a source command's SQL subquery + the flat columns it exposes.
type resolvedSource struct {
	SQL            string
	Columns        []string // exposed flat columns, in default projection order
	NumericColumns []string // subset of Columns already numeric (no string-coercion on compare)
	Focus          string   // optional node identifier the viz should center/highlight (pgr start)
	Limit          int      // outer-query result-row cap this source prefers, overriding opts.MaxRows (0 = no preference)
	OrderBy        []string // outer-query ORDER BY this source prefers, overriding the generic timestamp-DESC default
	QuerySettings  string   // optional top-level SETTINGS clause (no leading space) the executor appends to the final query -- e.g. pgr's scan budget
	Warnings       []string // non-fatal notices about what the source left out, e.g. pgr's truncation
}

// sourceResolver produces a resolvedSource for a source command. Resolution runs in the query
// layer because it may need the database (e.g. pgr()'s pass-1 tree traversal). Register one per
// source-command name; the core translate/handler path stays generic.
type sourceResolver func(h *QueryHandler, ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error)

var sourceResolvers = map[string]sourceResolver{
	"pgr": resolvePgrSource,
}

// sourceStubs give each source command a stand-in with the resolved source's shape but no rows,
// built from the command's arguments alone. Validation translates the rest of the pipeline over
// it, so a live editor can check a pgr() query without the database the real resolution needs.
// Every source command in sourceResolvers needs one here.
var sourceStubs = map[string]func(cmd parser.CommandNode) (*resolvedSource, error){
	"pgr": stubPgrSource,
}

var errPgrNeedsStart = errors.New("pgr() requires start=\"<process_guid>\"")

// applySourceCommand resolves the pipeline's source command, when it has one, into
// opts and returns the pipeline without it. Search, validation and ExecuteBQL all
// go through here, so a source's columns, row cap and order apply the same way in
// each. The source's cap and order replace the generic MaxQueryRows and
// timestamp-DESC defaults, since its rows are already ranked (pgr() puts spawn
// structure before leaves); an explicit limit() or sort() downstream still wins.
func applySourceCommand(pipeline *parser.PipelineNode, opts *parser.QueryOptions, resolve func(parser.CommandNode) (*resolvedSource, error)) (*parser.PipelineNode, *resolvedSource, error) {
	src, ok := parser.FirstSourceCommand(pipeline)
	if !ok {
		return pipeline, nil, nil
	}
	if err := parser.ValidateSourceCommandPlacement(pipeline); err != nil {
		return nil, nil, err
	}
	rs, err := resolve(src)
	if err != nil {
		return nil, nil, err
	}
	opts.SourceSubquery = rs.SQL
	opts.SourceColumns = rs.Columns
	opts.SourceNumericColumns = rs.NumericColumns
	if rs.Limit > 0 {
		opts.MaxRows = rs.Limit
	}
	if len(rs.OrderBy) > 0 {
		opts.SourceOrderBy = rs.OrderBy
	}
	return parser.StripCommand(pipeline, src.Name), rs, nil
}

// resolveSourceCommand resolves a source command into its subquery source.
func (h *QueryHandler) resolveSourceCommand(ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error) {
	r, ok := sourceResolvers[cmd.Name]
	if !ok {
		return nil, fmt.Errorf("unknown source command: %s()", cmd.Name)
	}
	return r(h, ctx, cmd, opts)
}

// resolvePgrSource runs pgr() and resolves it into its materialized scored-edge source.
func resolvePgrSource(h *QueryHandler, ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error) {
	if opts.SourceMode == parser.SourceIceberg {
		return nil, fmt.Errorf("pgr() operates on live process lineage and is not available over archived data")
	}
	p, ok := parser.ParseProvenanceParams(cmd)
	if !ok {
		return nil, errPgrNeedsStart
	}
	// Diffusion (when enabled) is applied server-side in provenanceScoreSQL, so the propagated
	// anomaly_score is what every consumer sees -- pgraph() is a pure renderer of these rows.
	sql, warnings, err := h.provenanceScoreSQL(ctx, p, opts)
	if err != nil {
		return nil, err
	}
	return &resolvedSource{
		SQL:            sql,
		Columns:        provenanceColumns,
		NumericColumns: provenanceNumericColumns,
		Focus:          p.Start,
		Limit:          p.Limit,
		OrderBy:        parser.ProvenanceOrderBy,
		// The scoring scans ran in the resolver; the downstream query only reads the literal
		// source, so this budget is a backstop rather than the main guard.
		QuerySettings: storage.ProvenanceQuerySettings,
		Warnings:      warnings,
	}, nil
}

// stubPgrSource checks pgr()'s arguments and stands in for its scored-edge source with the
// zero-row stub, which carries the same columns, numeric columns, and ordering.
func stubPgrSource(cmd parser.CommandNode) (*resolvedSource, error) {
	p, ok := parser.ParseProvenanceParams(cmd)
	if !ok {
		return nil, errPgrNeedsStart
	}
	return &resolvedSource{
		SQL:            provenanceEmptyScoreSQL,
		Columns:        provenanceColumns,
		NumericColumns: provenanceNumericColumns,
		Focus:          p.Start,
		Limit:          p.Limit,
		OrderBy:        parser.ProvenanceOrderBy,
	}, nil
}

// recordProvenanceScores feeds the rendered score distribution to severity calibration. These are
// post-diffusion values, which is what the analyst sees and therefore what the cutoffs must track.
func (h *QueryHandler) recordProvenanceScores(rows []map[string]interface{}) {
	if h.pgrRecorder == nil || len(rows) == 0 {
		return
	}
	scores := make([]float64, 0, len(rows))
	for _, r := range rows {
		scores = append(scores, reconFloat(r["anomaly_score"]))
	}
	h.pgrRecorder.Observe(scores)
}

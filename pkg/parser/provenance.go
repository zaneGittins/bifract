package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// maxProvenanceGuids caps the tree-guid set. Beyond the graph being unreadable at that size, a
// large guid set defeats the logs idx_process_guid bloom (too many values → every granule in the
// window matches one), forcing the pass-2 edge scan to read the whole window. Capped low so the
// IN-list stays bloom-prunable and the graph stays legible; the output edge limit (default 500)
// is well under this anyway. BFS is breadth-first, so the cap keeps the processes nearest the seed.
const maxProvenanceGuids = 1000

// MaxProvenanceGuids exposes maxProvenanceGuids to the query layer's iterative tree walk
// ((*QueryHandler).collectProvenanceTreeGuids), which bounds its BFS by the same cap.
const MaxProvenanceGuids = maxProvenanceGuids

// pgr()'s own result-row cap, applied as the OUTER query's LIMIT (see resolvePgrSource /
// resolvedSource.Limit). This exists because the generic per-deployment MaxQueryRows default
// can be set far lower than what a process tree needs (e.g. 250), and the generic pipeline's
// default LIMIT re-sorts by timestamp first -- which would truncate actual spawn/tree edges,
// not just low-signal leaves, once a tree's total edge count exceeds it. pgr() sets its own
// default here instead, paired with a spawn-first OrderBy (provenanceOrderBy) so any truncation
// that does occur drops leaves before structure. An explicit `| limit(N)` downstream still wins.
const (
	defaultPgrLimit = 500
	maxPgrLimit     = 20000
)

// defaultDiffuseLambda is the decay rate for diffuse=true's anomaly propagation (see
// diffuseProvenanceRows in pkg/query/provenance.go: S(v) = surp(edge) + lambda*S(parent)). Lower
// = faster decay = a chain of only-modestly-anomalous edges needs to be more concentrated before
// it reads as high; lambda=0 makes propagation a no-op (propagated score == raw score, i.e.
// diffuse=true collapses toward diffuse=false's output). Exposed as lambda= so recalibrating
// against real data doesn't require a code change each time.
const defaultDiffuseLambda = 0.2

// Reconnection (NoDoze object-mediated) caps. Every value bounds a step so cross-tree
// reconnection stays per-subgraph and rarity-pruned -- never an all-pairs join over the
// full table. See pkg/query/provenance.go for how the resolver orchestrates these.
const (
	// maxReconnectCandidateArtifacts caps the distinct artifacts (written paths / external
	// IPs / domains) pulled from the current subgraph before the reverse lookup.
	maxReconnectCandidateArtifacts = 500
	// maxReconnectPeers caps total peer edges the reverse lookup returns, PER recon type.
	// The cross-type total is capped separately (MaxPeers / peers=), because every peer not
	// picked for subtree expansion becomes its own root -- a whole extra tree in the graph.
	maxReconnectPeers = 200
	// reconnectHostPrevalenceMax is the ABSOLUTE floor of the net/dns rarity gate: an artifact
	// on at most this many hosts always qualifies (keeps small deployments working).
	reconnectHostPrevalenceMax = 3
	// reconnectHostFraction scales the gate with fleet size: an artifact also qualifies while
	// it is on at most this fraction of all hosts. So a widespread-but-rare C2 (e.g. 9 of 200
	// hosts) still bridges -- many hosts hitting the same rare IP is a STRONGER signal, not a
	// reason to drop it -- while true ubiquitous infrastructure (CDNs, resolvers on most hosts)
	// is still pruned. Consistent with the score's global-rarity term (1 - hosts/total).
	reconnectHostFraction = "0.5"
	// reconnectImagePrevalenceMax / reconnectImageFraction: the net/dns rarity gate ALSO prunes by
	// how many DISTINCT PROCESS IMAGES touch an artifact. Host-prevalence alone is useless on a
	// single-host (or few-host) dataset -- every artifact is on 1 host and passes -- so benign but
	// ubiquitous software infrastructure (ecs.office.com, telemetry/update/CDN endpoints, resolved by
	// dozens of distinct images) floods reconnection and crowds out real shared-IOC bridges. A rare
	// C2 is touched by very few images; common infra by many. Keep only artifacts on <= max(floor,
	// fraction * total distinct images). This is the process-diversity analogue of the host gate and
	// makes rarity work independent of host count.
	reconnectImagePrevalenceMax = 4
	reconnectImageFraction      = "0.1"
	// groupUniqArray cap on the proc_freq.hosts state -- MUST match the DDL (256) or CH
	// errors code 43 on the merge.
	procFreqHostsCap = 256
	// DefaultReconnectPeers is how many distinct PEER PROCESSES (across all recon types) are
	// admitted into the graph, ranked by bridge strength. Each admitted peer is effectively
	// another tree attached to the result, so this is a readability cap, not just a row cap.
	// Overridable per query with peers=.
	DefaultReconnectPeers = 50
	// maxReconnectPeersArg bounds peers= so a typo cannot re-open the unbounded behavior.
	maxReconnectPeersArg = 500
)

// Diffusion (NoDoze network-diffusion adaptation) caps. When diffuse is on, the FINAL anomaly
// threshold is applied by the viz over the PROPAGATED score (a leaf under an anomalous chain is
// promoted), so the scoring SQL cannot pre-filter leaves by the user threshold -- a promotable
// leaf must survive to the client. Instead it keeps leaves above a low floor, capped per process,
// so the payload stays bounded regardless of tree size (no unbounded scan -- the threshold filter
// was always post-scan, so this changes rows returned, not granules read). See the frontend
// _pgApplyDiffusion for the propagation itself.
const (
	// diffuseLeafFloor: leaves below this RAW anomaly are dropped in SQL -- they can never be
	// promoted enough to matter and returning them all would flood the payload. Promotable
	// leaves (some raw signal, under a hot chain) sit above it.
	diffuseLeafFloor = "0.05"
	// diffusePerProcLeaves caps non-spawn leaves returned PER PROCESS (top-N by raw anomaly), so
	// one noisy process cannot blow the payload. procs are bounded by maxProvenanceGuids.
	diffusePerProcLeaves = 64
)

// ProvenanceParams are the parsed pgr() arguments the handler orchestrates the two-pass
// query with. start/depth/direction match ptg(); threshold prunes non-spawn edges below it.
// EdgeTypes is the resolved set of non-spawn edge types to generate (spawn -- the tree spine
// -- is always included); include=/exclude= narrow it. Empty means "all".
type ProvenanceParams struct {
	Start     string
	Depth     int
	Direction string
	Threshold float64
	EdgeTypes map[string]bool // non-spawn event_types to include; nil/empty = all
	Reconnect bool            // enable cross-tree reconnection via shared leaves (default true)
	Diffuse   bool            // propagate anomaly along the tree (NoDoze diffusion); default true
	Lambda    float64         // diffusion decay rate, 0.0-1.0 (default defaultDiffuseLambda); only used when Diffuse
	Limit     int             // outer result-row cap (default defaultPgrLimit, max maxPgrLimit)
	MaxPeers  int             // distinct reconnected peer processes admitted (default DefaultReconnectPeers)
}

// ProvenanceOrderBy is the outer-query ORDER BY resolvePgrSource attaches alongside Limit: spawn
// edges first, then by anomaly. Matches the ordering pass-2's own SQL already applies internally,
// so if Limit ever truncates, it drops low-signal leaves before process structure.
var ProvenanceOrderBy = []string{"(event_type = 'spawn') DESC", "anomaly_score DESC"}

// provenanceDefaultLeafTypes are the branches generated when the user gives NO explicit include=.
// remote_thread/process_access are excluded by default: they key off source_process_guid, which is
// absent in current data (so they emit no edges) AND has no skip index, so including them forces an
// unindexed full-window scan of the entire logs volume per branch -- catastrophic on high-rate
// fractals for zero output. They remain available on request via include="remote_thread"/etc.,
// which is when the scan cost is knowingly opted into.
var provenanceDefaultLeafTypes = []string{"file_write", "net_connect", "dns_query"}

// normalizeEdgeType maps user-facing aliases (the raw bifract_category names) onto the pgr
// output event_type values, so exclude="network_connect" and exclude="net_connect" both work.
func normalizeEdgeType(s string) string {
	switch strings.ToLower(strings.TrimSpace(strings.Trim(s, "\"'"))) {
	case "network_connect", "net_connect":
		return "net_connect"
	case "process_creation", "spawn":
		return "spawn"
	case "file_write":
		return "file_write"
	case "dns_query", "dns":
		return "dns_query"
	case "remote_thread":
		return "remote_thread"
	case "process_access":
		return "process_access"
	}
	return ""
}

// parseEdgeTypeList splits a comma/space-separated argument value into normalized event_types.
func parseEdgeTypeList(v string) []string {
	v = strings.Trim(v, "\"'[]")
	var out []string
	for _, tok := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' }) {
		if t := normalizeEdgeType(tok); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// ParseProvenanceParams parses a pgr() command node's arguments. ok is false when start= is
// missing. pgr is a source command (see source_command.go): the query layer's source resolver
// calls this, then orchestrates the two-pass query into a subquery source.
func ParseProvenanceParams(cmd CommandNode) (ProvenanceParams, bool) {
	p := ProvenanceParams{Depth: 10, Direction: "both", Threshold: 0.7, Reconnect: true, Diffuse: true, Limit: defaultPgrLimit, Lambda: defaultDiffuseLambda, MaxPeers: DefaultReconnectPeers}
	var includeTypes, excludeTypes []string
	for _, arg := range cmd.Arguments {
		switch {
		case strings.HasPrefix(arg, "start="):
			p.Start = strings.Trim(strings.TrimPrefix(arg, "start="), "\"'")
		case strings.HasPrefix(arg, "limit="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && n > 0 {
				if n > maxPgrLimit {
					n = maxPgrLimit
				}
				p.Limit = n
			}
		case strings.HasPrefix(arg, "peers="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "peers=")); err == nil && n > 0 {
				if n > maxReconnectPeersArg {
					n = maxReconnectPeersArg
				}
				p.MaxPeers = n
			}
		case strings.HasPrefix(arg, "reconnect="):
			v := strings.ToLower(strings.Trim(strings.TrimPrefix(arg, "reconnect="), "\"'"))
			p.Reconnect = v != "false" && v != "0" && v != "no"
		case strings.HasPrefix(arg, "diffuse="):
			v := strings.ToLower(strings.Trim(strings.TrimPrefix(arg, "diffuse="), "\"'"))
			p.Diffuse = v != "false" && v != "0" && v != "no"
		case strings.HasPrefix(arg, "lambda="):
			if l, err := strconv.ParseFloat(strings.TrimPrefix(arg, "lambda="), 64); err == nil && l >= 0 && l <= 1 {
				p.Lambda = l
			}
		case strings.HasPrefix(arg, "depth="):
			if d, err := strconv.Atoi(strings.TrimPrefix(arg, "depth=")); err == nil && d > 0 {
				p.Depth = d
			}
		case strings.HasPrefix(arg, "direction="):
			p.Direction = strings.ToLower(strings.Trim(strings.TrimPrefix(arg, "direction="), "\"'"))
		case strings.HasPrefix(arg, "threshold="):
			if t, err := strconv.ParseFloat(strings.TrimPrefix(arg, "threshold="), 64); err == nil && t >= 0 && t <= 1 {
				p.Threshold = t
			}
		case strings.HasPrefix(arg, "include="):
			includeTypes = parseEdgeTypeList(strings.TrimPrefix(arg, "include="))
		case strings.HasPrefix(arg, "exclude="):
			excludeTypes = parseEdgeTypeList(strings.TrimPrefix(arg, "exclude="))
		}
	}
	// Resolve the non-spawn edge-type set: start from include= (or the default leaf types when no
	// include= is given), then drop any exclude=. spawn is the backbone and is never filtered out
	// here. The default omits remote_thread/process_access (see provenanceDefaultLeafTypes); an
	// explicit include= can still request them.
	p.EdgeTypes = map[string]bool{}
	base := includeTypes
	if len(base) == 0 {
		base = provenanceDefaultLeafTypes
	}
	for _, t := range base {
		if t != "spawn" {
			p.EdgeTypes[t] = true
		}
	}
	for _, t := range excludeTypes {
		delete(p.EdgeTypes, t)
	}
	if p.Depth > 50 {
		p.Depth = 50
	}
	if p.Direction != "forward" && p.Direction != "backward" && p.Direction != "both" {
		p.Direction = "both"
	}
	return p, p.Start != ""
}

// BuildProcessTreeHopSQL builds ONE breadth-first traversal hop over proc_lineage for the
// iterative pgr pass-1 walk (see (*QueryHandler).collectProvenanceTreeGuids). It exists because
// the WITH RECURSIVE traversal in buildProcessTreeSQL cannot push its recursive frontier to an
// index -- so every depth level self-joins and FINAL-scans the ENTIRE fractal's proc_lineage
// (EXPLAIN: ~all granules per level, ×2 directions ×depth). Here the frontier is a CONCRETE
// literal set, so the planner prunes on it (EXPLAIN confirmed a handful of granules per hop):
//   - forward (descendants): parent_guid IN (frontier) -> idx_parent_guid bloom; the returned
//     process_guids are the children (next frontier).
//   - backward (ancestors): process_guid IN (frontier) -> (fractal_id, process_guid) primary key;
//     the returned parent_guids are the parents (next frontier).
// Fractal + time-window scoping matches the recursive base/step exactly. No FINAL: the caller
// dedups guids in Go and process_guid is globally unique, so the distinct guid set is identical
// to the FINAL traversal (FINAL only picks which row per guid, not which guids exist).
func BuildProcessTreeHopSQL(frontier []string, forward bool, opts QueryOptions) string {
	tbl := opts.ProcLineageTable
	if tbl == "" {
		tbl = "proc_lineage"
	}
	quoted := make([]string, len(frontier))
	for i, g := range frontier {
		quoted[i] = "'" + escapeString(g) + "'"
	}
	conds := []string{
		fmt.Sprintf("timestamp >= '%s'", opts.StartTime.Format("2006-01-02 15:04:05")),
		fmt.Sprintf("timestamp <= '%s'", opts.EndTime.Format("2006-01-02 15:04:05")),
	}
	if fc := procLineageFractalCond(opts, ""); fc != "" {
		conds = append(conds, fc)
	}
	matchCol := "process_guid" // backward: locate the frontier nodes, read their parent_guid
	if forward {
		matchCol = "parent_guid" // forward: rows whose parent is in the frontier (the children)
	}
	conds = append(conds, fmt.Sprintf("%s IN (%s)", matchCol, strings.Join(quoted, ", ")))
	return fmt.Sprintf("SELECT process_guid, parent_guid FROM %s WHERE %s", tbl, strings.Join(conds, " AND "))
}

// buildProvenanceEdgeUnion returns the raw (non-aggregated) UNION ALL of every edge branch pgr's
// tree can touch: spawn (proc_lineage), the file/net/dns leaf edges (process_edges rollup), and
// p2p (logs), each bounded by guids + the user's time window -- never by the full fractal history.
// BuildProvenanceScoringSQL aggregates it into edgesAgg.
func buildProvenanceEdgeUnion(guids []string, edgeTypes map[string]bool, opts QueryOptions) (string, error) {
	if len(guids) == 0 {
		return "", fmt.Errorf("pgr: no process guids to score")
	}
	want := func(t string) bool { return len(edgeTypes) == 0 || edgeTypes[t] }
	if len(guids) > maxProvenanceGuids {
		guids = guids[:maxProvenanceGuids]
	}
	quoted := make([]string, len(guids))
	for i, g := range guids {
		quoted[i] = "'" + escapeString(g) + "'"
	}
	inList := strings.Join(quoted, ", ")

	procLineage := opts.ProcLineageTable
	if procLineage == "" {
		procLineage = "proc_lineage"
	}
	logs := opts.EffectiveTableName()

	tsStart := opts.StartTime.Format("2006-01-02 15:04:05")
	tsEnd := opts.EndTime.Format("2006-01-02 15:04:05")
	// The user's query window, applied to the process_guid-matched edge reads (file/net/dns rollup
	// + p2p logs scan). No probe is needed anymore: the edge/p2p reads scope by guid, so the window
	// only trims which of the tree's edges are in range, not the scan cost.
	userWin := fmt.Sprintf("timestamp >= '%s' AND timestamp <= '%s'", tsStart, tsEnd)
	fractal := procLineageFractalCond(opts, "")
	frac := func() string {
		if fractal == "" {
			return ""
		}
		return " AND " + fractal
	}

	// Abstraction shortcut (identical transform to the proc_freq MVs). Used by spawn/p2p, which
	// still read proc_lineage/logs directly; the file/net/dns leaf edges now come pre-abstracted
	// from the process_edges rollup below.
	aPath := func(col string) string { return abstractExpr(col, AbstractPath) }

	// Each edge SELECT yields: src_node, dst_node, label, event_type, fkey_src, fkey_tgt, log_id,
	// timestamp, fractal_id, host -- log_id + timestamp + fractal_id are the columns the standard
	// log-detail fetch (/logs/fields) needs, in a normal search row's shape.
	spawnEdges := fmt.Sprintf(
		"SELECT parent_guid AS src_node, process_guid AS dst_node, image AS label, 'spawn' AS event_type, "+
			"%s AS fkey_src, %s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, computer_name AS host FROM %s FINAL WHERE process_guid IN (%s)%s",
		aPath("parent_image"), aPath("image"), procLineage, inList, frac())

	// Leaf edges (file/net/dns) come from the process_edges rollup keyed by
	// (fractal_id, process_guid, event_type, dst_node): a PRIMARY-KEY lookup on the tree's guids
	// instead of a bloom-defeated raw-logs scan over the window (measured 156s -> ~76ms). dst_node
	// and fkey_* are already abstracted by the MV (byte-identical to abstractExpr / proc_freq), so
	// they join proc_freq exactly as before. event_type is filtered to the enabled leaf categories;
	// userWin bounds by the edge's latest timestamp to the query window. p2p is NOT here (it keys on
	// source_process_guid and is gated off by default). AggregatingMergeTree partials (across shards
	// / unmerged parts) are merged downstream by edgesAgg's GROUP BY.
	edgeTable := opts.ProcEdgesTable
	if edgeTable == "" {
		edgeTable = "process_edges"
	}
	var leafTypes []string
	if want("file_write") {
		leafTypes = append(leafTypes, "'file_write'")
	}
	if want("net_connect") {
		leafTypes = append(leafTypes, "'net_connect'")
	}
	if want("dns_query") {
		leafTypes = append(leafTypes, "'dns_query'")
	}
	leafEdges := ""
	if len(leafTypes) > 0 {
		edgeFrac := ""
		if fractal != "" {
			edgeFrac = fractal + " AND "
		}
		leafEdges = fmt.Sprintf(
			"SELECT process_guid AS src_node, dst_node, label, event_type, fkey_src, fkey_tgt, log_id, "+
				"toString(timestamp) AS timestamp, fractal_id, computer_name AS host "+
				"FROM %s WHERE %s%s AND process_guid IN (%s) AND event_type IN (%s)",
			edgeTable, edgeFrac, userWin, inList, strings.Join(leafTypes, ", "))
	}

	// Process->process edges (injection / handle-access): source_process_guid in the tree,
	// real target_process_guid node. The actor's image is normalized to fields.image (same as
	// spawn/file/net/dns src); only the victim keeps the target_image field.
	p2pEdges := func(category, eventType string) string {
		return fmt.Sprintf(
			"SELECT fields.source_process_guid::String AS src_node, fields.target_process_guid::String AS dst_node, "+
				"fields.target_image::String AS label, '%[7]s' AS event_type, %[1]s AS fkey_src, %[2]s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, fields.computer_name::String AS host "+
				"FROM %[3]s WHERE %[4]s%[5]s AND fields.source_process_guid::String IN (%[6]s) "+
				"AND fields.bifract_category = '%[8]s' AND fields.image::String != '' AND fields.target_image::String != '' "+
				"AND fields.target_process_guid::String != ''",
			aPath("fields.image::String"), aPath("fields.target_image::String"), logs, userWin, frac(), inList, eventType, category)
	}

	// spawn is always present (the tree spine); file/net/dns ride the process_edges rollup in one
	// read (leafEdges); p2p branches (if enabled via include=) still scan logs.
	parts := []string{spawnEdges}
	if leafEdges != "" {
		parts = append(parts, leafEdges)
	}
	if want("remote_thread") {
		parts = append(parts, p2pEdges("remote_thread", "remote_thread"))
	}
	if want("process_access") {
		parts = append(parts, p2pEdges("process_access", "process_access"))
	}
	return strings.Join(parts, " UNION ALL "), nil
}

// BuildProvenanceTotalHostsSQL returns a query for the single scalar BuildProvenanceScoringSQL's
// anomExpr uses as the global-rarity denominator (UNCAPPED true fleet size, see the comment on
// anomExpr). provenanceScoreSQL runs this ONCE per pgr() call and passes the result in as
// totalHosts -- it is not re-embedded as a live scalar subquery inside SQL that may be rebuilt
// more than once within a single call (the diffuse-fallback rebuild), so it is never scanned
// twice for the same invocation, and it is never cached ACROSS separate pgr() calls (always
// fresh per call, exactly matching what the inline subquery it replaces would have returned).
func BuildProvenanceTotalHostsSQL(opts QueryOptions) string {
	procLineage := opts.ProcLineageTable
	if procLineage == "" {
		procLineage = "proc_lineage"
	}
	where := ""
	if fractal := procLineageFractalCond(opts, ""); fractal != "" {
		where = " WHERE " + fractal
	}
	return fmt.Sprintf("SELECT uniqExact(computer_name) AS total_hosts FROM %s%s", procLineage, where)
}

// BuildReconnectionTotalsSQL returns a single query producing both proc_freq rarity-gate
// ceilings BuildReconnectionSQL needs (see hostGate/imageGate): the capped host-prevalence count
// and the distinct source-image count. Computed once per pgr() call (only when Reconnect is on)
// in ONE scan instead of two separate scalar subqueries each re-scanning proc_freq.
func BuildReconnectionTotalsSQL(opts QueryOptions) string {
	procFreq := opts.ProcFreqTable
	if procFreq == "" {
		procFreq = "proc_freq"
	}
	where := ""
	if fractal := procLineageFractalCond(opts, ""); fractal != "" {
		where = " WHERE " + fractal
	}
	return fmt.Sprintf("SELECT length(groupUniqArrayMerge(%d)(hosts)) AS total_hosts, uniqExact(src_image) AS total_images FROM %s%s",
		procFreqHostsCap, procFreq, where)
}

// BuildProvenanceScoringSQL is pass 2: given the tree's guids, assemble every edge
// (spawn from proc_lineage; file/net/injection/handle-access leaf edges from logs, bounded
// by time + guid + category), score each against proc_freq (anomaly = 1 - freq(edge)/freq(src,rel,*)),
// and keep the full spawn spine plus any non-spawn edge at/above threshold. Output columns:
// parent, child, label, event_type, anomaly_score -- an edge list graph() renders directly.
// edgeTypes selects which non-spawn edge branches to generate (nil/empty = all); spawn is
// always included as the tree backbone.
//
// totalHosts (from BuildProvenanceTotalHostsSQL, computed once per pgr() call -- see
// provenanceScoreSQL) is the global-rarity denominator, substituted as a literal instead of a
// live scalar subquery so it is not silently re-scanned if this SQL is rebuilt within the same
// call (the diffuse-fallback path calls this twice). Always fresh per call; never cached across
// separate pgr() calls, so this is byte-identical to what the inline subquery would have
// returned at call time.
func BuildProvenanceScoringSQL(guids []string, threshold float64, edgeTypes map[string]bool, diffuse bool, totalHosts int64, opts QueryOptions) (string, error) {
	if len(guids) == 0 {
		return "", fmt.Errorf("pgr: no process guids to score")
	}
	// Defensive: a NaN/out-of-range threshold would emit an invalid or degenerate WHERE clause.
	if threshold != threshold || threshold < 0 { // NaN or negative
		threshold = 0
	} else if threshold > 1 {
		threshold = 1
	}
	if len(guids) > maxProvenanceGuids {
		guids = guids[:maxProvenanceGuids]
	}

	edges, err := buildProvenanceEdgeUnion(guids, edgeTypes, opts)
	if err != nil {
		return "", err
	}

	procFreq := opts.ProcFreqTable
	if procFreq == "" {
		procFreq = "proc_freq"
	}
	logs := opts.EffectiveTableName()

	// pm reads process_creation logs by process_guid over the user's query window.
	timeWin := fmt.Sprintf("timestamp >= '%s' AND timestamp <= '%s'",
		opts.StartTime.Format("2006-01-02 15:04:05"), opts.EndTime.Format("2006-01-02 15:04:05"))
	fractal := procLineageFractalCond(opts, "")
	frac := func() string {
		if fractal == "" {
			return ""
		}
		return " AND " + fractal
	}

	// pm's process_guid IN-list needs the same quoted guids buildProvenanceEdgeUnion built.
	quoted := make([]string, len(guids))
	for i, g := range guids {
		quoted[i] = "'" + escapeString(g) + "'"
	}
	inList := strings.Join(quoted, ", ")

	freqWhere := ""
	if fractal != "" {
		freqWhere = " WHERE " + fractal
	}

	// Aggregate raw per-event leaf edges to one row per (src,dst,event_type). Without this a
	// beaconing process (e.g. 10k connections to one C2) emits 10k identical edges that all get
	// scored, sorted, and can eat the LIMIT -- pushing out other processes' edges. spawn edges
	// are already one-per-process (proc_lineage FINAL). argMax/max keep a consistent latest
	// (log_id,timestamp) pair for the detail lookup.
	edgesAgg := fmt.Sprintf("SELECT src_node, dst_node, any(ev.label) AS label, event_type, any(ev.fkey_src) AS fkey_src, "+
		"any(ev.fkey_tgt) AS fkey_tgt, argMax(ev.log_id, ev.timestamp) AS log_id, max(ev.timestamp) AS timestamp, any(ev.fractal_id) AS fractal_id, any(ev.host) AS host "+
		"FROM (%s) AS ev GROUP BY src_node, dst_node, event_type", edges)

	// Anomaly = greatest of two rarity signals (non-spawn edges):
	//   1. source-relative: 1 - freq(src,rel,target)/freq(src,rel,*) (fe/ft) -- "how unusual is
	//      this target FOR this source". Blind spot: a process that only ever does one thing
	//      (malware that only talks to its C2) scores its sole behavior 0.
	//   2. global rarity: 1 - hosts(rel,target)/total_hosts (gf/totalHosts) -- "how rare is this
	//      target across the fleet". A rare C2 scores high regardless of source frequency.
	// total_hosts uses uniqExact(computer_name) over proc_lineage (UNCAPPED true fleet size),
	// not proc_freq's groupUniqArray(256) state which saturates at 256 and would under-score
	// medium-prevalence artifacts on large fleets. When a target's host count saturates the cap
	// it is treated as common (GR=0). A never-seen target (hostct 0) scores 1. When the source
	// has no baseline (ft.tot=0) we fall back to global rarity alone rather than forcing 1.0,
	// so a brand-new-but-benign process touching common targets is not all-max noise.
	// Spawn keeps the pure source-relative score (structure is never pruned; only coloured).
	gr := fmt.Sprintf("if(coalesce(gf.hostct, 0) >= %[1]d, 0, if(%[2]d = 0, 0, 1 - coalesce(gf.hostct, 0) / %[2]d))", procFreqHostsCap, totalHosts)
	anomExpr := fmt.Sprintf("multiIf(e.event_type = 'spawn', if(coalesce(ft.tot, 0) = 0, 1.0, round(1 - coalesce(fe.cnt, 0) / ft.tot, 4)), "+
		"coalesce(ft.tot, 0) = 0, round(%[1]s, 4), "+
		"round(greatest(1 - coalesce(fe.cnt, 0) / ft.tot, %[1]s), 4)) AS anomaly_score ", gr)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("WITH fe AS (SELECT src_image, event_type, target_norm, sum(event_count) AS cnt FROM %[1]s%[2]s GROUP BY src_image, event_type, target_norm), ",
		procFreq, freqWhere))
	b.WriteString(fmt.Sprintf("ft AS (SELECT src_image, event_type, sum(event_count) AS tot FROM %[1]s%[2]s GROUP BY src_image, event_type), ",
		procFreq, freqWhere))
	b.WriteString(fmt.Sprintf("gf AS (SELECT event_type, target_norm, length(groupUniqArrayMerge(%[3]d)(hosts)) AS hostct FROM %[1]s%[2]s GROUP BY event_type, target_norm), ",
		procFreq, freqWhere, procFreqHostsCap))
	// pm = per-process command line + user, read query-only from the process_creation logs of
	// the tree's guids (the same bounded keyhole: guid IN + time window + category). Command
	// lines can be enormous, so truncate to 300 chars in SQL -- never pull the full string.
	b.WriteString(fmt.Sprintf("pm AS (SELECT fields.process_guid::String AS guid, any(substring(if(fields.commandline::String != '', fields.commandline::String, fields.command_line::String), 1, 300)) AS command_line, any(fields.user::String) AS proc_user FROM %[1]s WHERE %[2]s%[3]s AND fields.process_guid::String IN (%[4]s) AND fields.bifract_category = 'process_creation' GROUP BY guid) ",
		logs, timeWin, frac(), inList))
	// Leaf gating differs by mode. Non-diffuse: filter leaves by the user threshold in SQL and
	// keep the top MaxRows non-spawn edges globally. Diffuse: the FINAL threshold is applied by
	// the viz over the PROPAGATED score, so a promotable leaf must survive -- SQL keeps leaves
	// above a low floor, capped PER PROCESS (bounded payload), and the client re-thresholds.
	// Either way ALL spawn edges are kept so process structure can never be truncated away.
	leafFloor := strconv.FormatFloat(threshold, 'f', -1, 64)
	partitionBy := "(event_type = 'spawn')"
	capN, applyCap := opts.MaxRows, opts.MaxRows > 0
	if diffuse {
		leafFloor = diffuseLeafFloor
		partitionBy = "(event_type = 'spawn'), parent" // per-process leaf cap
		capN, applyCap = diffusePerProcLeaves, true
	}
	b.WriteString("SELECT parent, child, label, event_type, anomaly_score, log_id, timestamp, fractal_id, command_line, proc_user, host FROM (")
	b.WriteString("SELECT parent, child, label, event_type, anomaly_score, log_id, timestamp, fractal_id, command_line, proc_user, host")
	if applyCap {
		b.WriteString(fmt.Sprintf(", row_number() OVER (PARTITION BY %s ORDER BY anomaly_score DESC) AS _rn", partitionBy))
	}
	b.WriteString(" FROM (")
	b.WriteString("SELECT e.src_node AS parent, e.dst_node AS child, e.label AS label, e.event_type AS event_type, e.log_id AS log_id, e.timestamp AS timestamp, e.fractal_id AS fractal_id, e.host AS host, coalesce(pm.command_line, '') AS command_line, coalesce(pm.proc_user, '') AS proc_user, ")
	b.WriteString(anomExpr)
	b.WriteString(fmt.Sprintf("FROM (%s) AS e ", edgesAgg))
	b.WriteString("LEFT JOIN fe ON fe.src_image = e.fkey_src AND fe.event_type = e.event_type AND fe.target_norm = e.fkey_tgt ")
	b.WriteString("LEFT JOIN ft ON ft.src_image = e.fkey_src AND ft.event_type = e.event_type ")
	b.WriteString("LEFT JOIN gf ON gf.event_type = e.event_type AND gf.target_norm = e.fkey_tgt ")
	b.WriteString("LEFT JOIN pm ON pm.guid = e.dst_node) AS scored ")
	b.WriteString(fmt.Sprintf("WHERE event_type = 'spawn' OR anomaly_score >= %s) AS ranked ", leafFloor))
	if applyCap {
		b.WriteString(fmt.Sprintf("WHERE event_type = 'spawn' OR _rn <= %d ", capN))
	}
	b.WriteString("ORDER BY (event_type = 'spawn') DESC, anomaly_score DESC")

	// Not run through validateGeneratedSQL: this statement is fully machine-composed and
	// legitimately multi-source UNION ALLs the five edge types (which the shared validator
	// only whitelists for WITH RECURSIVE queries). The only external input is the guid set,
	// escaped via escapeString above, so it cannot break out of its string literals.
	return b.String(), nil
}

// ReconnectPeer is one candidate cross-tree reconnection returned by the reverse lookup
// (BuildReconnectionSQL). The emitted bridge depends on ReconType:
//   - net/dns: ObjectID is the shared leaf-node id (e.g. "net:1.2.3.4"). Both the tree-side
//     toucher (SrcGUID) and the external peer (PeerGUID) are wired to it, so it converges
//     into one shared-object node regardless of pass-2's anomaly pruning.
//   - file: a direct SrcGUID(writer) -> PeerGUID(executor) edge (dropped-then-executed).
//     ObjectID is empty; the file path rides in Label. This matches "one process writes a
//     file, another launches with that image" -- a connection between the two processes.
//   - remote_thread/process_access: pass-2 already emits source->target, so these are only
//     expansion candidates (ObjectID and SrcGUID empty, not emitted as a literal edge).
type ReconnectPeer struct {
	ReconType   string  // file | net | dns | remote_thread | process_access
	PeerGUID    string  // the reconnected process (outside the current tree); expansion seed
	SrcGUID     string  // tree-side endpoint: file writer, or a tree toucher of the artifact
	ObjectID    string  // shared leaf-node id for net/dns (empty for file/injection/access)
	Label       string  // raw artifact (IP/domain/path)
	Anomaly     float64 // bridge severity, drives ranking + edge color
	PeerImage   string  // peer process image (node label)
	PeerLogID   string  // source log of the peer's touch (detail lookup)
	PeerTS      string  // peer log timestamp (string)
	PeerFractal string  // peer log fractal_id
	PeerHost    string  // peer computer_name (for cross-host link notation)
}

// reconnectEdgeType maps a reconnection recon_type onto the p.EdgeTypes gate key (the
// bifract_category-derived event_type), so include=/exclude= filters reconnection the same
// way they filter in-tree leaf edges.
var reconnectEdgeType = map[string]string{
	"file":           "file_write",
	"net":            "net_connect",
	"dns":            "dns_query",
	"remote_thread":  "remote_thread",
	"process_access": "process_access",
}

// internalDomainExpr returns a ClickHouse boolean expr that is TRUE for a benign INTERNAL name
// lookup that must not form a reconnection dns bridge. Reconnection peers are hard-capped, so noisy
// internal name resolution (NetBIOS, AD service discovery) would crowd out real shared-IOC bridges.
// It is applied to the ALREADY-ABSTRACTED domain (lowercased, trailing root dot stripped -- see
// AbstractDomain), so only the structural checks are needed:
//   - single-label name (no dot) -- NetBIOS / LLMNR / mDNS
//   - AD service-discovery record -- starts with '_' (_ldap._tcp...) or contains '._msdcs.'
// The former logs-based filter also excluded names resolving ONLY to internal IPs (via
// query_results); that signal is not carried in the process_edges rollup, so it is not applied here
// -- the proc_freq host/image rarity gate prunes widely-resolved internal names instead.
func internalDomainExpr(col string) string {
	return "(position(" + col + ", '.') = 0 OR startsWith(" + col + ", '_') OR position(" + col + ", '._msdcs.') > 0)"
}

// BuildReconnectionSQL is the cross-tree reverse lookup. Given the current subgraph's guids
// it returns peer-candidate rows (schema: recon_type, peer_guid, object_id, label, anomaly,
// peer_image, peer_log_id, peer_ts, peer_fractal). It is deliberately per-subgraph and
// rarity-pruned so it never becomes an all-pairs join over the full table:
//   - file: proc_lineage processes launched with image == a path THIS tree wrote (dropped
//     then executed). proc_lineage is the small process skeleton; image is LowCardinality.
//   - net/dns: logs rows touching an external IP / domain THIS tree touched, but only for
//     artifacts whose proc_freq host-prevalence is <= reconnectHostPrevalenceMax (rare), so
//     shared CDNs/resolvers/update servers do not reconnect everything. The net lookup rides
//     the existing idx_dst_ip bloom.
//   - remote_thread/process_access: target guids this tree injected into / opened -- emitted
//     already by pass-2, so returned only as expansion candidates (object_id empty).
// Returns "" (no error) when reconnection is disabled or no edge type is selected.
//
// totalHosts/totalImages (from BuildReconnectionTotalsSQL, computed once per pgr() call -- see
// provenanceScoreSQL) are the rarity-gate ceiling inputs, substituted as literals instead of
// live scalar subqueries. Always fresh per call; never cached across separate pgr() calls.
func BuildReconnectionSQL(guids []string, p ProvenanceParams, totalHosts, totalImages int64, opts QueryOptions) (string, error) {
	if !p.Reconnect || len(guids) == 0 {
		return "", nil
	}
	want := func(reconType string) bool {
		et := reconnectEdgeType[reconType]
		return len(p.EdgeTypes) == 0 || p.EdgeTypes[et]
	}
	if len(guids) > maxProvenanceGuids {
		guids = guids[:maxProvenanceGuids]
	}
	quoted := make([]string, len(guids))
	for i, g := range guids {
		quoted[i] = "'" + escapeString(g) + "'"
	}
	inList := strings.Join(quoted, ", ")

	logs := opts.EffectiveTableName()
	procLineage := opts.ProcLineageTable
	if procLineage == "" {
		procLineage = "proc_lineage"
	}
	procFreq := opts.ProcFreqTable
	if procFreq == "" {
		procFreq = "proc_freq"
	}

	tsStart := opts.StartTime.Format("2006-01-02 15:04:05")
	tsEnd := opts.EndTime.Format("2006-01-02 15:04:05")
	timeWin := fmt.Sprintf("timestamp >= '%s' AND timestamp <= '%s'", tsStart, tsEnd)
	fractal := procLineageFractalCond(opts, "")
	fracAnd := ""
	if fractal != "" {
		fracAnd = " AND " + fractal
	}
	// Leading-WHERE fractal fragment for proc_freq (event_type filter always present).
	freqFrac := ""
	if fractal != "" {
		freqFrac = " AND " + fractal
	}

	// Rarity gate ceiling (net/dns): an artifact bridges while it is on <= max(absolute floor,
	// fraction * total_hosts) hosts. Scales with the fleet so a rare C2 on many hosts still
	// reconnects; only near-ubiquitous infrastructure is pruned. totalHosts/totalImages are the
	// literal scalars the caller computed once for this call (see func doc above).
	hostGate := fmt.Sprintf("greatest(toUInt64(%d), toUInt64(%s * %d))", reconnectHostPrevalenceMax, reconnectHostFraction, totalHosts)
	// Process-diversity gate (see reconnectImagePrevalenceMax): prune artifacts touched by many
	// distinct process images, so ubiquitous software infra is dropped even on single-host data.
	imageGate := fmt.Sprintf("greatest(toUInt64(%d), toUInt64(%s * %d))", reconnectImagePrevalenceMax, reconnectImageFraction, totalImages)

	// Common column order for every UNION branch:
	// recon_type, peer_guid, src_guid, object_id, label, anomaly, peer_image, peer_log_id, peer_ts, peer_fractal
	var parts []string

	// net/dns now read the process_edges rollup (small, PK-scoped by process_guid) instead of
	// scanning raw logs -- the same acceleration as pgr's pass-2 (measured 156s -> ms). The tree
	// side is a PK lookup (process_guid IN tree); the peer side scans process_edges by artifact,
	// cheap because the rollup is ~10,000x smaller than logs. fkey_tgt is the abstracted artifact
	// (matches proc_freq.target_norm), dst_node is the 'net:'/'dns:' node id, fkey_src the actor
	// image, label the raw artifact.
	edgeTable := opts.ProcEdgesTable
	if edgeTable == "" {
		edgeTable = "process_edges"
	}
	edgeFrac := ""
	if fractal != "" {
		edgeFrac = fractal + " AND "
	}
	// rareGate is the proc_freq rarity subquery (host- + image-prevalence ceilings) an artifact must
	// pass to bridge; et is the proc_freq event_type. Shared by net/dns.
	rareGate := func(et string) string {
		return fmt.Sprintf("SELECT target_norm FROM %s WHERE event_type = '%s'%s GROUP BY target_norm "+
			"HAVING length(groupUniqArrayMerge(%d)(hosts)) <= %s AND uniqExact(src_image) <= %s",
			procFreq, et, freqFrac, procFreqHostsCap, hostGate, imageGate)
	}

	// file: write -> execute. The tree's written paths come from the edge rollup (label = raw
	// target_file); peer = a proc_lineage process launched with image == that path. The executor
	// side stays on proc_lineage (the process skeleton carries image); it is far smaller than logs.
	if want("file") {
		writtenPaths := fmt.Sprintf(
			"SELECT lower(label) AS p, any(process_guid) AS writer FROM %[1]s WHERE %[2]sevent_type = 'file_write' "+
				"AND process_guid IN (%[3]s) AND label != '' GROUP BY p LIMIT %[4]d",
			edgeTable, edgeFrac, inList, maxReconnectCandidateArtifacts)
		plFrac := procLineageFractalCond(opts, "pl.")
		plWhere := ""
		if plFrac != "" {
			plWhere = plFrac + " AND "
		}
		parts = append(parts, fmt.Sprintf(
			"SELECT 'file' AS recon_type, pl.process_guid AS peer_guid, wp.writer AS src_guid, '' AS object_id, "+
				"pl.image AS label, toFloat64(1.0) AS anomaly, pl.image AS peer_image, pl.log_id AS peer_log_id, "+
				"toString(pl.timestamp) AS peer_ts, pl.fractal_id AS peer_fractal, pl.computer_name AS peer_host "+
				"FROM %[1]s AS pl FINAL GLOBAL INNER JOIN (%[2]s) AS wp ON lower(pl.image) = wp.p "+
				"WHERE %[3]spl.process_guid NOT IN (%[4]s) ORDER BY pl.process_guid LIMIT %[5]d",
			procLineage, writtenPaths, plWhere, inList, maxReconnectPeers))
	}

	// net: two processes connect to the same rare EXTERNAL IP. fkey_tgt is the abstracted IP
	// (external kept raw, internal collapsed to '/24' or 'internal', which we exclude to keep the
	// external-C2 focus of the original). NOTE: the previous logs branch also bridged dns-RESOLVED
	// IPs (query_results); the rollup does not carry resolved IPs, so that cross-type
	// resolve->connect bridge is not covered here (connect<->connect and dns domain<->domain remain).
	if want("net") {
		extOnly := "fkey_tgt NOT LIKE '%/24' AND fkey_tgt != 'internal'"
		rareIP := fmt.Sprintf(
			"SELECT fkey_tgt AS ip, any(process_guid) AS toucher FROM %[1]s WHERE %[2]sevent_type = 'net_connect' "+
				"AND process_guid IN (%[3]s) AND %[4]s AND fkey_tgt GLOBAL IN (%[5]s) GROUP BY ip LIMIT %[6]d",
			edgeTable, edgeFrac, inList, extOnly, rareGate("net_connect"), maxReconnectCandidateArtifacts)
		peerScan := fmt.Sprintf(
			"SELECT process_guid AS peer_guid, fkey_tgt AS ip, dst_node, fkey_src AS img, log_id, "+
				"toString(timestamp) AS ts, fractal_id, computer_name AS host FROM %[1]s WHERE %[2]sevent_type = 'net_connect' "+
				"AND process_guid NOT IN (%[3]s) AND fkey_tgt GLOBAL IN (SELECT ip FROM (%[4]s))",
			edgeTable, edgeFrac, inList, rareIP)
		parts = append(parts, fmt.Sprintf(
			"SELECT 'net' AS recon_type, l.peer_guid AS peer_guid, any(ri.toucher) AS src_guid, l.dst_node AS object_id, "+
				"any(l.ip) AS label, toFloat64(0.85) AS anomaly, any(l.img) AS peer_image, any(l.log_id) AS peer_log_id, "+
				"any(l.ts) AS peer_ts, any(l.fractal_id) AS peer_fractal, any(l.host) AS peer_host "+
				"FROM (%[1]s) AS l GLOBAL INNER JOIN (%[2]s) AS ri ON l.ip = ri.ip GROUP BY peer_guid, object_id ORDER BY peer_guid LIMIT %[3]d",
			peerScan, rareIP, maxReconnectPeers))
	}

	// dns: two processes -> same rare DOMAIN. The rollup's dns edges already exclude IP-form queries
	// (the MV's NOT match(query, ipv4) filter), so an IP is never both a dns: and net: node.
	if want("dns") {
		rareDom := fmt.Sprintf(
			"SELECT fkey_tgt AS q, any(process_guid) AS toucher FROM %[1]s WHERE %[2]sevent_type = 'dns_query' "+
				"AND process_guid IN (%[3]s) AND NOT %[6]s AND fkey_tgt GLOBAL IN (%[4]s) GROUP BY q LIMIT %[5]d",
			edgeTable, edgeFrac, inList, rareGate("dns_query"), maxReconnectCandidateArtifacts, internalDomainExpr("fkey_tgt"))
		peerScan := fmt.Sprintf(
			"SELECT process_guid AS peer_guid, fkey_tgt AS q, dst_node, fkey_src AS img, log_id, "+
				"toString(timestamp) AS ts, fractal_id, computer_name AS host FROM %[1]s WHERE %[2]sevent_type = 'dns_query' "+
				"AND process_guid NOT IN (%[3]s) AND fkey_tgt GLOBAL IN (SELECT q FROM (%[4]s))",
			edgeTable, edgeFrac, inList, rareDom)
		parts = append(parts, fmt.Sprintf(
			"SELECT 'dns' AS recon_type, l.peer_guid AS peer_guid, any(ri.toucher) AS src_guid, l.dst_node AS object_id, "+
				"any(l.q) AS label, toFloat64(0.85) AS anomaly, any(l.img) AS peer_image, any(l.log_id) AS peer_log_id, "+
				"any(l.ts) AS peer_ts, any(l.fractal_id) AS peer_fractal, any(l.host) AS peer_host "+
				"FROM (%[1]s) AS l GLOBAL INNER JOIN (%[2]s) AS ri ON l.q = ri.q GROUP BY peer_guid, object_id ORDER BY peer_guid LIMIT %[3]d",
			peerScan, rareDom, maxReconnectPeers))
	}

	// remote_thread / process_access: target guids this tree acted on. pass-2 already emits
	// the source->target edge, so these are expansion candidates only (src_guid/object_id empty).
	p2p := func(reconType, category string, anomaly string) string {
		return fmt.Sprintf(
			"SELECT '%[1]s' AS recon_type, fields.target_process_guid::String AS peer_guid, '' AS src_guid, '' AS object_id, "+
				"any(fields.target_image::String) AS label, toFloat64(%[2]s) AS anomaly, any(fields.target_image::String) AS peer_image, "+
				"'' AS peer_log_id, '' AS peer_ts, '' AS peer_fractal, any(fields.computer_name::String) AS peer_host "+
				"FROM %[3]s WHERE %[4]s%[5]s AND fields.bifract_category = '%[6]s' "+
				"AND fields.source_process_guid::String IN (%[7]s) AND fields.target_process_guid::String != '' "+
				"AND fields.target_process_guid::String NOT IN (%[7]s) GROUP BY peer_guid ORDER BY peer_guid LIMIT %[8]d",
			reconType, anomaly, logs, timeWin, fracAnd, category, inList, maxReconnectPeers)
	}
	if want("remote_thread") {
		parts = append(parts, p2p("remote_thread", "remote_thread", "0.95"))
	}
	if want("process_access") {
		parts = append(parts, p2p("process_access", "process_access", "0.9"))
	}

	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " UNION ALL "), nil
}

// AppendReconnectionEdges wraps the pass-2 scored-edge SQL and UNION ALLs the reconnection
// bridge edges. Reconnection owns these edges explicitly (as literal rows) so a shared
// object node always converges even when pass-2 prunes the underlying low-anomaly leaf edge:
//   - net/dns: two edges -- tree-toucher -> object node AND peer -> object node.
//   - file: one edge -- writer -> executor (the file path in Label; no object node).
//   - injection/access: none (pass-2 owns source->target); peer is expansion-only.
// Edges are deduped by (parent, child, event_type). Every value is escaped.
func AppendReconnectionEdges(pass2SQL string, peers []ReconnectPeer) string {
	const cols = "parent, child, label, event_type, anomaly_score, log_id, timestamp, fractal_id, command_line, proc_user, host"
	base := "SELECT " + cols + " FROM (" + pass2SQL + ")"

	seen := map[string]bool{}
	var lits []string
	emit := func(parent, child, label, eventType string, anomaly float64, logID, ts, fractal, host string) {
		if parent == "" || child == "" {
			return
		}
		key := parent + "\x00" + child + "\x00" + eventType
		if seen[key] {
			return
		}
		seen[key] = true
		lits = append(lits, fmt.Sprintf(
			"SELECT '%s' AS parent, '%s' AS child, '%s' AS label, '%s' AS event_type, toFloat64(%s) AS anomaly_score, "+
				"'%s' AS log_id, '%s' AS timestamp, '%s' AS fractal_id, '' AS command_line, '' AS proc_user, '%s' AS host",
			escapeString(parent), escapeString(child), escapeString(label), escapeString(eventType),
			strconv.FormatFloat(anomaly, 'f', 4, 64), escapeString(logID), escapeString(ts), escapeString(fractal), escapeString(host)))
	}

	for _, pe := range peers {
		et := "reconnect_" + pe.ReconType
		switch {
		case pe.ObjectID != "": // net/dns: converge both endpoints on the shared object node
			emit(pe.SrcGUID, pe.ObjectID, pe.Label, et, pe.Anomaly, "", "", "", "")
			emit(pe.PeerGUID, pe.ObjectID, pe.Label, et, pe.Anomaly, pe.PeerLogID, pe.PeerTS, pe.PeerFractal, pe.PeerHost)
		case pe.ReconType == "file" && pe.SrcGUID != "": // file: writer -> executor
			emit(pe.SrcGUID, pe.PeerGUID, pe.Label, et, pe.Anomaly, pe.PeerLogID, pe.PeerTS, pe.PeerFractal, pe.PeerHost)
		}
		// injection/access: skip (pass-2 owns the source->target edge)
	}
	if len(lits) == 0 {
		return base
	}
	return base + " UNION ALL " + strings.Join(lits, " UNION ALL ")
}

// Provenance (pgr) abstraction helpers. proc_freq stores ABSTRACTED behavioral keys so
// "the same behavior" collapses to one row (a per-user temp path, a churning DHCP host,
// etc. become one key). The abstraction MUST be byte-identical on the MV write side and
// the pgr() read side, so both sides call abstractExpr -- the single source of truth.
//
// The regex escaping here is validated against ClickHouse 26.6: raw-string literals hold
// the exact SQL text CH receives, so CH's string-literal unescape (\\ -> \) leaves the
// intended RE2 pattern. Do not "simplify" the backslashes without re-testing in CH.

const (
	// AbstractPath: lowercase; mask user home dirs (users/home segment -> *); collapse
	// GUIDs and long digit runs (random temp names). Keeps the full path otherwise, so a
	// masqueraded c:\temp\powershell.exe stays distinct from c:\windows\system32\powershell.exe.
	AbstractPath = "path"
	// AbstractIP: external addresses kept as-is (where C2 lives); internal v4 -> its /24
	// subnet (absorbs per-host DHCP churn, keeps segment distinctions for lateral movement);
	// internal v6 (loopback/link-local/ULA) -> 'internal'.
	AbstractIP = "ip"
	// AbstractDomain: DNS query targets. Lowercase and strip the trailing root dot so
	// "WWW.Evil-C2.com." and "www.evil-c2.com" collapse to one key. Kept otherwise-verbatim
	// (no PSL folding) so a rare DGA host stays distinct and scores anomalous.
	AbstractDomain = "domain"
)

// abstractExpr returns a ClickHouse SQL expression that abstracts colSQL per kind
// (AbstractPath or AbstractIP). colSQL is any SQL expression yielding the raw String
// (e.g. "fields.image::String" or a proc_lineage column name).
func abstractExpr(colSQL, kind string) string {
	switch kind {
	case AbstractIP:
		return fmt.Sprintf(ipAbstractTmpl, colSQL)
	case AbstractDomain:
		return fmt.Sprintf(domainAbstractTmpl, colSQL)
	default: // AbstractPath
		return fmt.Sprintf(pathAbstractTmpl, colSQL)
	}
}

// pathAbstractTmpl: %s is substituted once with the column expression.
const pathAbstractTmpl = `lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(%s, ` +
	`'(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), ` +
	`'\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), ` +
	`'[0-9]{6,}', '*'))`

// ipAbstractTmpl: %[1]s is the column expression, referenced multiple times.
const ipAbstractTmpl = `multiIf(` +
	`match(%[1]s, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), ` +
	`concat(replaceRegexpOne(%[1]s, '\\.[0-9]{1,3}$', ''), '.0/24'), ` +
	`match(%[1]s, '^(::1$|fe80:|fc|fd)'), 'internal', ` +
	`%[1]s)`

// domainAbstractTmpl: %s is substituted once. Lowercase + strip the trailing FQDN root dot.
const domainAbstractTmpl = `lower(replaceRegexpOne(%s, '\\.$', ''))`

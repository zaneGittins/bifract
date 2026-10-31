package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// MaxProvenanceNodes caps the graph's process nodes. A node is one process, or a group of
// identical siblings collapsed into one (see BuildProcessGroupHopSQL), so the cap bounds what
// the graph has to draw, not how many processes the tree holds. Breadth-first, so the cap keeps
// the nodes nearest the seed.
const MaxProvenanceNodes = 1000

// MaxProvenanceMembers caps the processes behind those nodes. A polling loop can put tens of
// thousands of identical children under one parent; they cost one node but every member's
// guid is still carried (as the ProvenanceTreeTable external table) so no member's children or
// leaves are lost to collapsing.
const MaxProvenanceMembers = 500_000

// ProvenanceTreeTable is the external table (guid String, node String) holding every member
// process of the tree and the node it collapsed into. The query layer sends it with each pass
// that reads the tree, so membership never travels as a literal IN-list.
const ProvenanceTreeTable = "_pgr_tree"

// ProvenanceFrontierTable is the same shape, holding one hop's frontier (see
// BuildProcessGroupHopSQL).
const ProvenanceFrontierTable = "_pgr_frontier"

// inTree is the membership condition for a column read from a possibly Distributed table.
// GLOBAL builds the set once on the initiator and ships it to every shard.
func inTree(col string) string {
	return col + " GLOBAL IN (SELECT guid FROM " + ProvenanceTreeTable + ")"
}

// pgr()'s own result-row cap. The query layer budgets its rows to it (structure in walk order,
// a share held for leaves and bridges) and also applies it as the OUTER query's LIMIT (see
// resolvePgrSource / resolvedSource.Limit). It exists because the generic per-deployment
// MaxQueryRows default can be set far lower than what a process tree needs (e.g. 250), and the
// generic pipeline's default LIMIT re-sorts by timestamp first. An explicit `| limit(N)`
// downstream still wins.
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
	// maxReconnectPeers caps total peer edges the reverse lookup returns, PER recon type,
	// keeping those of the least-shared artifacts first.
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
	// is still pruned. Kept tight because the bridge budget is small: admitting artifacts on a
	// large share of the fleet crowds out real shared-IOC bridges.
	reconnectHostFraction = "0.05"
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
// threshold is applied over the PROPAGATED score (a leaf under an anomalous chain is promoted),
// so the scoring SQL cannot pre-filter leaves by the user threshold -- a promotable leaf must
// survive. Instead it keeps leaves above a low floor, capped per process, so the payload stays
// bounded regardless of tree size (no unbounded scan -- the threshold filter was always
// post-scan, so this changes rows returned, not granules read). See diffuseProvenanceRows in
// pkg/query/provenance.go for the propagation itself.
const (
	// diffuseLeafFloor: leaves below this RAW anomaly are dropped in SQL -- they can never be
	// promoted enough to matter and returning them all would flood the payload. Promotable
	// leaves (some raw signal, under a hot chain) sit above it.
	diffuseLeafFloor = "0.05"
	// diffusePerProcLeaves caps non-spawn leaves returned PER PROCESS (top-N by raw anomaly), so
	// one noisy process cannot blow the payload. procs are bounded by MaxProvenanceNodes.
	diffusePerProcLeaves = 64
)

// nodeStabilityFloor is the minimum NoDoze IN/OUT score (Eq.4/Eq.5). A node first observed today
// has one observed day and one new-edge day, giving a raw stability of 0, which would zero the
// whole path product in Eq.2 and paint every edge through it max-anomalous on the strength of a
// thin baseline alone. The floor keeps a young node damping rather than annihilating the score.
const nodeStabilityFloor = "0.05"

// hostDayPrecision is the uniqCombined64 precision host-days are counted with: exact up to 128
// distinct (measured on CH 26.6), about 1.6% error above, and a few KB of state per group at most.
// Leave-one-out needs exactness only where few host-days remain; an exact count would hold every
// pair of a ubiquitous relationship in memory.
const hostDayPrecision = 12

// minStabilityDays is how many days of baseline the IN/OUT terms need before they carry any
// information. Stability is the share of days a node gained no new edge, so its resolution is 1/d:
// on a 2-day baseline nearly every node scores exactly 0.5 and the term is a constant that only
// scales the score without ordering anything (measured directly -- IN was 0.5 for every edge, in
// both classes). Below this span the terms are held at 1.0, which reduces Eq.2 to the transition
// term alone rather than silently multiplying in noise. 14 days gives ~7% resolution.
const minStabilityDays = 14

// MinStabilityDays exposes the gate to the admin readout, so an operator can see why the
// node-stability terms are inactive rather than wondering.
const MinStabilityDays = minStabilityDays

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

// ProvenanceColumns is pgr()'s output shape, shared by the scoring SQL, the reconnection bridge
// rows and the query layer's re-emission so they cannot drift. The explanation columns say why an edge
// scored what it did:
//   - anomaly_score: the final score, after diffusion; edge_score: the edge's own score before it.
//   - score_basis: transition (the source's own history), new_source (a binary that never ran
//     elsewhere, scored by the target's rarity), no_source (ghost root), or reconnect (a bridge).
//   - edge_host_days / source_host_days: host-days on which ANYONE ELSE made this relationship /
//     made any relationship of this type from this source. All *_host_days are leave-one-out.
//   - source_exec_host_days: other host-days the source binary was executed.
//   - target_host_days: other host-days the target was touched from any source.
//   - target_hosts / total_hosts: distinct hosts that touched the target (state-capped at 256),
//     including this one, out of the fleet.
//   - first_seen: the first day the relationship was ever observed.
//
// group_size is how many identical sibling processes a spawn row's child node stands for (1 for
// a single process); group_first / group_last span their creation times when it is more than one.
// The scoring SQL emits the single-process defaults; the query layer fills in collapsed nodes.
// recon_role is set on reconnection bridges: lead, shared or context (see the query layer).
var ProvenanceColumns = []string{
	"parent", "child", "label", "event_type", "anomaly_score", "edge_score", "score_basis", "first_seen",
	"edge_host_days", "source_host_days", "source_exec_host_days", "target_host_days", "target_hosts", "total_hosts",
	"log_id", "timestamp", "fractal_id", "command_line", "proc_user", "host", "parent_label",
	"group_size", "group_first", "group_last", "recon_role",
}

// ProvenanceNumericColumns is the subset of ProvenanceColumns that is Float64, so downstream
// comparisons stay numeric and literal re-emission types them.
var ProvenanceNumericColumns = []string{
	"anomaly_score", "edge_score", "edge_host_days", "source_host_days", "source_exec_host_days", "target_host_days", "target_hosts", "total_hosts",
	"group_size",
}

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

// edgeTypeList normalizes a list argument's values into event_types, dropping
// anything unrecognised. A single value may still hold several types separated
// by commas or spaces: include="spawn,file_write" is the documented spelling.
func edgeTypeList(values []string) []string {
	var out []string
	for _, v := range values {
		for _, tok := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' }) {
			if t := normalizeEdgeType(tok); t != "" {
				out = append(out, t)
			}
		}
	}
	return out
}

// ParseProvenanceParams parses a pgr() command node's arguments. ok is false when start= is
// missing. pgr is a source command (see source_command.go): the query layer's source resolver
// calls this, then orchestrates the two-pass query into a subquery source.
func ParseProvenanceParams(cmd CommandNode) (ProvenanceParams, bool) {
	p := ProvenanceParams{Depth: 10, Direction: "both", Threshold: 0.7, Reconnect: true, Diffuse: true, Limit: defaultPgrLimit, Lambda: defaultDiffuseLambda, MaxPeers: DefaultReconnectPeers}
	b, err := BindCommand(cmd)
	if err != nil {
		return p, false
	}
	p.Start = b.Str("start", "")
	if n := b.Int("limit", p.Limit); n > 0 {
		p.Limit = min(n, maxPgrLimit)
	}
	if n := b.Int("peers", p.MaxPeers); n > 0 {
		p.MaxPeers = min(n, maxReconnectPeersArg)
	}
	p.Reconnect = b.Flag("reconnect", p.Reconnect)
	p.Diffuse = b.Flag("diffuse", p.Diffuse)
	if l := b.Float("lambda", p.Lambda); l >= 0 && l <= 1 {
		p.Lambda = l
	}
	if d := b.Int("depth", p.Depth); d > 0 {
		p.Depth = d
	}
	if v := b.Str("direction", ""); v != "" {
		p.Direction = strings.ToLower(v)
	}
	if t := b.Float("threshold", p.Threshold); t >= 0 && t <= 1 {
		p.Threshold = t
	}
	includeTypes := edgeTypeList(b.Strings("include"))
	excludeTypes := edgeTypeList(b.Strings("exclude"))

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

// BuildProcessTreeHopSQL builds one backward hop (ancestors) of the iterative pgr pass-1 walk,
// and the seed check: a (fractal_id, process_guid) primary-key lookup of guid's own row, whose
// parent_guid is the next hop. ts_ms is its creation time. No FINAL: the caller dedups guids and
// process_guid is globally unique.
func BuildProcessTreeHopSQL(guid string, opts QueryOptions) string {
	conds := append(procLineageWindow(opts, ""), fmt.Sprintf("process_guid = '%s'", escapeString(guid)))
	return fmt.Sprintf("SELECT process_guid, parent_guid, toUnixTimestamp64Milli(timestamp) AS ts_ms FROM %s WHERE %s",
		procLineageTable(opts), strings.Join(conds, " AND "))
}

// BuildProcessGroupHopSQL builds one forward hop of the walk, collapsing repeated identical
// children. The frontier arrives as ProvenanceFrontierTable (member guid -> node). Children of
// every member are grouped by (parent node, child_image, child_cmd_key), the image and command
// line abstracted as proc_freq abstracts them (user dirs, GUIDs and 6+ digit runs masked) and
// stored on the row, so a polling loop's 47,900 `cmd /c tasklist` children become one row while
// distinct command lines stay apart. Recursing on the groups makes a group's children the union
// of its members' children, so a unique grandchild under one member still surfaces as its own node.
//
// Each row is one group: rep (earliest member, the node id), n (exact member count), members
// (capped at MaxProvenanceMembers), and the creation time span. It reads only the by_parent
// projection's columns, so the children are a lookup keyed by parent, and on a cluster each
// shard aggregates its own before the initiator merges them.
func BuildProcessGroupHopSQL(opts QueryOptions) string {
	conds := append(procLineageWindow(opts, "c."),
		"c.parent_guid GLOBAL IN (SELECT guid FROM "+ProvenanceFrontierTable+")")
	return fmt.Sprintf("SELECT f.node AS parent_node, "+
		"argMin(c.process_guid, (c.timestamp, c.process_guid)) AS rep, uniqExact(c.process_guid) AS n, "+
		"groupUniqArray(%[1]d)(c.process_guid) AS members, "+
		"toUnixTimestamp64Milli(min(c.timestamp)) AS first_ms, toUnixTimestamp64Milli(max(c.timestamp)) AS last_ms "+
		"FROM %[2]s AS c GLOBAL INNER JOIN %[3]s AS f ON c.parent_guid = f.guid WHERE %[4]s "+
		"GROUP BY parent_node, c.child_image, c.child_cmd_key ORDER BY first_ms, rep",
		MaxProvenanceMembers, procLineageTable(opts), ProvenanceFrontierTable, strings.Join(conds, " AND "))
}

func procLineageTable(opts QueryOptions) string {
	if opts.ProcLineageTable != "" {
		return opts.ProcLineageTable
	}
	return "proc_lineage"
}

func procFreqTable(opts QueryOptions) string {
	if opts.ProcFreqTable != "" {
		return opts.ProcFreqTable
	}
	return "proc_freq"
}

// fractalWhere is a WHERE clause scoping a whole-table read to the query's fractals, or "".
func fractalWhere(opts QueryOptions) string {
	if fractal := procLineageFractalCond(opts, ""); fractal != "" {
		return " WHERE " + fractal
	}
	return ""
}

func provenanceSummaryTable(opts QueryOptions) string {
	if opts.ProvenanceSummaryTable != "" {
		return opts.ProvenanceSummaryTable
	}
	return "provenance_summary"
}

func procEdgesTable(opts QueryOptions) string {
	if opts.ProcEdgesTable != "" {
		return opts.ProcEdgesTable
	}
	return "process_edges"
}

// procLineageWindow is the fractal and query-window scope of a proc_lineage read.
func procLineageWindow(opts QueryOptions, prefix string) []string {
	conds := []string{
		fmt.Sprintf("%stimestamp >= '%s'", prefix, chTimeLiteral(opts.StartTime)),
		fmt.Sprintf("%stimestamp <= '%s'", prefix, chTimeLiteral(opts.EndTime)),
	}
	if fc := procLineageFractalCond(opts, prefix); fc != "" {
		conds = append(conds, fc)
	}
	return conds
}

// buildProvenanceEdgeUnion returns the raw (non-aggregated) UNION ALL of every edge branch pgr's
// tree can touch: spawn (proc_lineage), the file/net/dns leaf edges (process_edges rollup), and
// p2p (logs), each bounded by the tree's members (ProvenanceTreeTable) + the user's time window --
// never by the full fractal history. Endpoints are raw guids; BuildProvenanceScoringSQL maps them
// onto nodes and aggregates.
func buildProvenanceEdgeUnion(edgeTypes map[string]bool, opts QueryOptions) string {
	want := func(t string) bool { return len(edgeTypes) == 0 || edgeTypes[t] }
	procLineage := procLineageTable(opts)
	logs := opts.EffectiveTableName()

	tsStart := chTimeLiteral(opts.StartTime)
	tsEnd := chTimeLiteral(opts.EndTime)
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

	// Abstraction shortcut (identical transform to the proc_freq MVs) for the columns that are not
	// stored abstracted: a spawn's parent image and the p2p branches' logs fields. A spawn's own
	// image is child_image, and the file/net/dns leaf edges come pre-abstracted from the rollup.
	aPath := func(col string) string { return abstractExpr(col, AbstractPath) }

	// Each edge SELECT yields: src_node, dst_node, label, event_type, fkey_src, fkey_tgt, log_id,
	// timestamp, fractal_id, host, parent_label, iday -- log_id + timestamp + fractal_id are the
	// columns the standard log-detail fetch (/logs/fields) needs, in a normal search row's shape.
	// iday is the ingest day, the day proc_freq bucketed the event under, so (host, iday) is the
	// edge's own host-day in the baseline (the leaf rollup keeps only its latest ingest).
	//
	// parent_label carries the PARENT node's image for rows whose parent may have no row of its own
	// in the result. A spawn row's parent_guid is only a tree node when its OWN process_creation is
	// in range; when it is not, the child's event still names it (proc_lineage.parent_image), so the
	// ancestor renders as its image instead of a bare guid. Empty on the other branches, whose
	// parents are always in-tree processes that carry their own spawn row.
	spawnEdges := fmt.Sprintf(
		"SELECT parent_guid AS src_node, process_guid AS dst_node, image AS label, 'spawn' AS event_type, "+
			"%s AS fkey_src, %s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, computer_name AS host, "+
			"parent_image AS parent_label, toDate(ingest_timestamp) AS iday FROM %s FINAL WHERE %s%s",
		aPath("parent_image"), "child_image", procLineage, inTree("process_guid"), frac())

	// Leaf edges (file/net/dns) come from the process_edges rollup keyed by
	// (fractal_id, process_guid, event_type, dst_node): a PRIMARY-KEY lookup on the tree's guids
	// instead of a bloom-defeated raw-logs scan over the window (measured 156s -> ~76ms). dst_node
	// and fkey_* are already abstracted by the MV (byte-identical to abstractExpr / proc_freq), so
	// they join proc_freq exactly as before. event_type is filtered to the enabled leaf categories;
	// userWin bounds by the edge's latest timestamp to the query window. p2p is NOT here (it keys on
	// source_process_guid and is gated off by default). AggregatingMergeTree partials (across shards
	// / unmerged parts) are merged downstream by edgesAgg's GROUP BY.
	edgeTable := procEdgesTable(opts)
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
				"toString(timestamp) AS timestamp, fractal_id, computer_name AS host, '' AS parent_label, toDate(ingest_timestamp) AS iday "+
				"FROM %s WHERE %s%s AND %s AND event_type IN (%s)",
			edgeTable, edgeFrac, userWin, inTree("process_guid"), strings.Join(leafTypes, ", "))
	}

	// Process->process edges (injection / handle-access): source_process_guid in the tree,
	// real target_process_guid node. The actor's image is normalized to fields.image (same as
	// spawn/file/net/dns src); only the victim keeps the target_image field.
	p2pEdges := func(category, eventType string) string {
		return fmt.Sprintf(
			"SELECT fields.source_process_guid::String AS src_node, fields.target_process_guid::String AS dst_node, "+
				"fields.target_image::String AS label, '%[7]s' AS event_type, %[1]s AS fkey_src, %[2]s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, fields.computer_name::String AS host, '' AS parent_label, toDate(ingest_timestamp) AS iday "+
				"FROM %[3]s WHERE %[4]s%[5]s AND %[6]s "+
				"AND fields.bifract_category = '%[8]s' AND fields.image::String != '' AND fields.target_image::String != '' "+
				"AND fields.target_process_guid::String != ''",
			aPath("fields.image::String"), aPath("fields.target_image::String"), logs, userWin, frac(), inTree("fields.source_process_guid::String"), eventType, category)
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
	return strings.Join(parts, " UNION ALL ")
}

// BuildProvenanceTotalHostsSQL returns a query for the fleet size BuildProvenanceScoringSQL
// reports as total_hosts (uncapped, from proc_lineage). Until provenance_summary is filled,
// provenanceScoreSQL runs it once per pgr() call and passes the result in as a literal, so the
// diffuse-fallback rebuild never rescans it; it is never cached across calls.
func BuildProvenanceTotalHostsSQL(opts QueryOptions) string {
	procLineage := procLineageTable(opts)
	where := fractalWhere(opts)
	return fmt.Sprintf("SELECT uniqExact(computer_name) AS total_hosts FROM %s%s", procLineage, where)
}

// BuildReconnectionTotalsSQL returns a single query producing both proc_freq rarity-gate
// ceilings BuildReconnectionSQL needs (see hostGate/imageGate): the capped host-prevalence count
// and the distinct source-image count. Until provenance_summary is filled, computed once per
// pgr() call (only when Reconnect is on) in one scan.
func BuildReconnectionTotalsSQL(opts QueryOptions) string {
	procFreq := procFreqTable(opts)
	where := fractalWhere(opts)
	return fmt.Sprintf("SELECT length(groupUniqArrayMerge(%d)(hosts)) AS total_hosts, uniqExact(src_image) AS total_images FROM %s%s",
		procFreqHostsCap, procFreq, where)
}

// BuildProvenanceSummarySQL reads every fleet-wide input from provenance_summary in one
// lookup: what BuildProvenanceTotalHostsSQL, BuildReconnectionTotalsSQL and the scoring SQL's
// baseline span compute by scanning the fractal. Each shard holds its own copy of a row, so
// counts are of distinct values; recon_hosts keeps the 256 cap the baseline's host sets have.
// first_day is 1970-01-01 for an empty baseline, which is what the scan yields too.
func BuildProvenanceSummarySQL(opts QueryOptions) string {
	where := fractalWhere(opts)
	return fmt.Sprintf("SELECT uniqExactIf(value, kind = 'lineage_host') AS total_hosts, "+
		"least(%[1]d, uniqExactIf(value, kind = 'freq_host')) AS recon_hosts, uniqExactIf(value, kind = 'freq_image') AS recon_images, "+
		"toString(minIf(toDateOrZero(value), kind = 'freq_day')) AS first_day FROM %[2]s%[3]s",
		procFreqHostsCap, provenanceSummaryTable(opts), where)
}

// ProvenanceBaseline holds the per-call inputs BuildProvenanceScoringSQL scores against. All of
// it is fetched once per pgr() call and substituted as literals rather than left as live scalar
// subqueries, so nothing is re-scanned when the scoring SQL is rebuilt within the same call (the
// diffuse-fallback path builds it twice). Never cached across calls.
type ProvenanceBaseline struct {
	// TotalHosts is the fleet size reported as total_hosts (BuildProvenanceSummarySQL, or
	// BuildProvenanceTotalHostsSQL until the summary is filled).
	TotalHosts int64
	// SrcKeys/TgtKeys are the DISTINCT fkey_src / fkey_tgt of the tree's own edges
	// (BuildProvenanceEdgeKeysSQL), scoping every proc_freq CTE to rows its join can match.
	// Unscoped, each CTE aggregates the fractal's whole proc_freq, which holds one row per
	// relationship per day and so grows with the retention window, not with the tree.
	//
	// Empty leaves the CTEs that depend on it unrestricted: slower, same scores. That is the
	// fallback when the lookup fails or overflows MaxProvenanceEdgeKeys, because a TRUNCATED
	// key set would turn real baseline rows into missed joins and change scores.
	SrcKeys []string
	TgtKeys []string
	// NodeStart/NodeEnd span the creation times of the graph's nodes. When set they narrow the
	// command-line and user lookups from the whole query window to the rows the nodes can be in.
	NodeStart, NodeEnd time.Time
	// FirstDay is the baseline's earliest day (BuildProvenanceSummarySQL). Zero has the scoring
	// SQL compute it from proc_freq.
	FirstDay time.Time
}

// MaxProvenanceEdgeKeys caps each edge key set. Past it the query layer drops both sets and
// scores against the unrestricted baseline rather than risk a truncated one. Sized so the
// literals stay well inside the raised max_query_size (storage.maxGeneratedQuerySize, 16MB):
// each set is substituted into at most two CTEs.
const MaxProvenanceEdgeKeys = 20000

// BuildProvenanceEdgeKeysSQL returns the DISTINCT (fkey_src, fkey_tgt) pairs of the tree's edges,
// which is what ProvenanceBaseline's key sets are collected from. It reads the same bounded edge
// union pass 2 scores (guid-keyed rollup + proc_lineage lookups), so it adds an indexed keyhole
// read, not a scan. One row over the cap is returned so the caller can detect overflow.
func BuildProvenanceEdgeKeysSQL(edgeTypes map[string]bool, opts QueryOptions) string {
	return fmt.Sprintf("SELECT DISTINCT fkey_src, fkey_tgt FROM (%s) AS ev LIMIT %d", buildProvenanceEdgeUnion(edgeTypes, opts), MaxProvenanceEdgeKeys+1)
}

// keyInCond renders `col IN ('a', 'b')` for a non-empty key set, or "" to leave the caller's
// CTE unrestricted (see ProvenanceBaseline).
func keyInCond(col string, keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = "'" + escapeString(k) + "'"
	}
	return fmt.Sprintf("%s IN (%s)", col, strings.Join(quoted, ", "))
}

// BuildProvenanceScoringSQL is pass 2: for the tree in ProvenanceTreeTable, assemble every edge
// (spawn from proc_lineage, file/net/dns from the process_edges rollup, injection/handle-access
// from logs, bounded by time + member guid + category), score each against proc_freq in
// leave-one-out host-days, and keep the full spawn spine plus any non-spawn edge at/above
// threshold. The output is
// ProvenanceColumns. edgeTypes selects which non-spawn edge branches to generate (nil/empty =
// all); spawn is always included as the tree backbone.
//
// bl carries the per-call baseline inputs: the fleet size and the tree's edge key sets. See
// ProvenanceBaseline.
func BuildProvenanceScoringSQL(threshold float64, edgeTypes map[string]bool, diffuse bool, bl ProvenanceBaseline, opts QueryOptions) string {
	// Defensive: a NaN/out-of-range threshold would emit an invalid or degenerate WHERE clause.
	if threshold != threshold || threshold < 0 { // NaN or negative
		threshold = 0
	} else if threshold > 1 {
		threshold = 1
	}
	edges := buildProvenanceEdgeUnion(edgeTypes, opts)

	procFreq := procFreqTable(opts)
	logs := opts.EffectiveTableName()

	// pl/pu read the nodes' own process_creation rows, so they only need the span those fall in.
	pmStart, pmEnd := opts.StartTime, opts.EndTime
	if !bl.NodeStart.IsZero() && bl.NodeStart.After(pmStart) {
		pmStart = bl.NodeStart
	}
	if !bl.NodeEnd.IsZero() && bl.NodeEnd.Before(pmEnd) {
		pmEnd = bl.NodeEnd
	}
	timeWin := fmt.Sprintf("timestamp >= '%s' AND timestamp <= '%s'", chTimeLiteral(pmStart), chTimeLiteral(pmEnd))
	fractal := procLineageFractalCond(opts, "")
	frac := func() string {
		if fractal == "" {
			return ""
		}
		return " AND " + fractal
	}

	// freqWhere composes a proc_freq WHERE from the fractal scope plus the caller's key
	// restrictions (empty conditions drop out).
	freqWhere := func(extra ...string) string {
		var conds []string
		if fractal != "" {
			conds = append(conds, fractal)
		}
		for _, c := range extra {
			if c != "" {
				conds = append(conds, c)
			}
		}
		if len(conds) == 0 {
			return ""
		}
		return " WHERE " + strings.Join(conds, " AND ")
	}
	// Which COLUMN a key set applies to depends on the CTE. fe/ft and ins are keyed by the edge's
	// source (fe/ft join src_image, ins is grouped by the target_norm that IS that source node); fe,
	// gf and outs by its target. Crossing them silently changes scores.
	srcOnSrc := keyInCond("src_image", bl.SrcKeys)
	tgtOnTgt := keyInCond("target_norm", bl.TgtKeys)
	srcOnTgt := keyInCond("target_norm", bl.SrcKeys)
	tgtOnSrc := keyInCond("src_image", bl.TgtKeys)

	// Map each endpoint onto its node (a guid outside the tree, an object node or an out-of-range
	// parent, keeps its own id), then aggregate to one row per (src,dst,event_type). That merges a
	// collapsed group's members: their spawn edges become one, and their leaves union. It also
	// folds a beaconing process's 10k identical connections into one edge that cannot eat the
	// LIMIT. The detail lookup gets a consistent (log_id,timestamp) pair: a leaf's latest event,
	// and for a spawn edge the node's own creation row (raw_dst = node), even when members tie.
	// hd_pairs are the edge's own host-days across every member, which keeps leave-one-out exact.
	mapped := fmt.Sprintf("SELECT if(ms.node = '', ev.src_node, ms.node) AS src_node, if(md.node = '', ev.dst_node, md.node) AS dst_node, "+
		"ev.label AS label, ev.event_type AS event_type, ev.fkey_src AS fkey_src, ev.fkey_tgt AS fkey_tgt, ev.log_id AS log_id, ev.timestamp AS timestamp, "+
		"ev.fractal_id AS fractal_id, ev.host AS host, ev.parent_label AS parent_label, ev.iday AS iday, ev.dst_node AS raw_dst "+
		"FROM (%[1]s) AS ev LEFT JOIN %[2]s AS ms ON ms.guid = ev.src_node LEFT JOIN %[2]s AS md ON md.guid = ev.dst_node", edges, ProvenanceTreeTable)
	edgesAgg := fmt.Sprintf("SELECT src_node, dst_node, any(ev.label) AS label, event_type, any(ev.fkey_src) AS fkey_src, "+
		"any(ev.fkey_tgt) AS fkey_tgt, if(event_type = 'spawn', argMin(ev.log_id, (ev.raw_dst != dst_node, ev.timestamp)), argMax(ev.log_id, ev.timestamp)) AS log_id, "+
		"if(event_type = 'spawn', argMin(ev.timestamp, (ev.raw_dst != dst_node, ev.timestamp)), max(ev.timestamp)) AS timestamp, any(ev.fractal_id) AS fractal_id, any(ev.host) AS host, "+
		"any(ev.parent_label) AS parent_label, groupUniqArray((ev.host, ev.iday)) AS hd_pairs "+
		"FROM (%s) AS ev GROUP BY src_node, dst_node, event_type", mapped)

	// Frequencies are HOST-DAYS, NoDoze's unit: an event counts once per host per day (Eq.7/8), so
	// a process beaconing 10k times to one IP is one host-day, not 10k observations of normality.
	// proc_freq keeps a host set per (relationship, day), so host-days are the distinct
	// (day, host) pairs across its rows (unmerged parts and shards repeat them).
	hostDayPairs := "arrayMap(h -> (day, h), finalizeAggregation(hosts))"
	hostDays := fmt.Sprintf("uniqCombined64Array(%d)(%s)", hostDayPrecision, hostDayPairs)

	var b strings.Builder
	// ClickHouse INLINES a CTE at each reference, so each proc_freq CTE below is its own
	// ReadFromMergeTree in the plan, and each carries its own key scope (see ProvenanceBaseline).
	// fe, ft and outs restrict src_image, a PRIMARY-KEY prefix, so they prune granules; gf and ins
	// restrict target_norm, which the by_target projection indexes. rel is inlined per CTE
	// rather than shared because ins and outs need different restrictions, and span reads
	// first_day directly instead of through it.
	//
	// first_seen is the earliest day a relationship was ever observed. Rows written before
	// proc_freq gained first_day carry the column default (1970-01-01); day is what a backfill
	// would have set, so substitute it and let the value self-correct as new rows arrive.
	firstSeen := "min(if(first_day = toDate(0), day, first_day))"
	// fe: host-days per relationship. Only the tree's own (source, target) keys can join it.
	b.WriteString(fmt.Sprintf("WITH fe AS (SELECT src_image, event_type, target_norm, %[3]s AS hd, %[4]s AS first_seen FROM %[1]s%[2]s GROUP BY src_image, event_type, target_norm), ",
		procFreq, freqWhere(srcOnSrc, tgtOnTgt), hostDays, firstSeen))
	// ft: Freq(src,rel,*) per Eq.8, the host-days on which the source made this relationship with
	// ANY target (the wildcard event counted once per host-day). So M is the share of the source's
	// active host-days that included this target, and a target it always reaches scores near 0.
	// Never restricted by target.
	b.WriteString(fmt.Sprintf("ft AS (SELECT src_image, event_type, %[3]s AS hd FROM %[1]s%[2]s GROUP BY src_image, event_type), ",
		procFreq, freqWhere(srcOnSrc), hostDays))
	// gf: the target's fleet footprint from any source -- distinct hosts (capped at the state's
	// 256) for display, host-days for scoring a source with no history of its own.
	b.WriteString(fmt.Sprintf("gf AS (SELECT event_type, target_norm, length(groupUniqArrayArray(%[3]d)(finalizeAggregation(hosts))) AS hosts_n, %[4]s AS hd FROM %[1]s%[2]s GROUP BY event_type, target_norm), ",
		procFreq, freqWhere(tgtOnTgt), procFreqHostsCap, hostDays))
	// The observed window ends at the QUERY's end, not today(): a historical investigation must not
	// be credited with stable days that postdate what it is looking at.
	asOf := fmt.Sprintf("toDate('%s')", opts.EndTime.UTC().Format("2006-01-02"))
	// span = days the baseline covers, gating the stability terms (see minStabilityDays). It is the
	// age of the WHOLE baseline, so it is never key-restricted.
	if bl.FirstDay.IsZero() {
		b.WriteString(fmt.Sprintf("span AS (SELECT dateDiff('day', %[3]s, %[4]s) + 1 AS days FROM %[1]s%[2]s), ",
			procFreq, freqWhere(), firstSeen, asOf))
	} else {
		b.WriteString(fmt.Sprintf("span AS (SELECT dateDiff('day', toDate('%s'), %s) + 1 AS days), ", bl.FirstDay.UTC().Format("2006-01-02"), asOf))
	}
	// NoDoze Eq.4/Eq.5 node stability: a day is STABLE for a node when it gained no new edge, and
	// stability is the share of stable days over the node's observed lifetime. rel holds one
	// first-seen day per relationship, so counting distinct dates per node counts the days that
	// node gained an edge. nodeStabilityFloor stops a node observed on a single day from scoring a
	// hard 0 and zeroing the whole Eq.2 product.
	//
	// The CTEs carry INSTABILITY (1 - stability) because an unmatched LEFT JOIN yields the column
	// default, 0, not NULL: a node absent here never gained an edge in that direction, which is
	// perfectly stable, and 1 - 0 says exactly that. The restriction is on the grouping column
	// itself, so every surviving group keeps all of its rows.
	stab := func(key, restrict, relExtra, nodeExtra string) string {
		rel := fmt.Sprintf("SELECT %[1]s AS node, %[4]s AS fd%[5]s FROM %[2]s%[3]s GROUP BY src_image, target_norm",
			key, procFreq, freqWhere(restrict), firstSeen, relExtra)
		return fmt.Sprintf("SELECT node, if((SELECT days FROM span) < %[4]d, 0.0, least(1 - %[2]s, uniqExact(fd) / greatest(1, dateDiff('day', min(fd), %[3]s) + 1))) AS instab%[5]s FROM (%[1]s) GROUP BY node",
			rel, nodeStabilityFloor, asOf, minStabilityDays, nodeExtra)
	}
	b.WriteString("outs AS (" + stab("src_image", tgtOnSrc, "", "") + "), ")
	// ins already reads every row where the source node is a target, so it also counts the
	// host-days the source binary was EXECUTED (spawn target): whether it is an established binary
	// at all, as opposed to whether it ever made this relationship. The per-parent states merge
	// into one distinct count per binary; summing per-parent counts would double a host-day on
	// which two parents ran it.
	b.WriteString("ins AS (" + stab("target_norm", srcOnTgt,
		fmt.Sprintf(", uniqCombined64ArrayIfState(%d)(%s, event_type = 'spawn') AS xs", hostDayPrecision, hostDayPairs),
		fmt.Sprintf(", uniqCombined64ArrayIfMerge(%d)(xs) AS exec_hd", hostDayPrecision),
	) + "), ")
	// sx: the tree's own executions of each binary, the host-days its processes added to the
	// exec_hd above. Same keyhole lookup and filter as proc_freq's spawn MV.
	procLineage := procLineageTable(opts)
	b.WriteString(fmt.Sprintf("sx AS (SELECT child_image AS node, length(groupUniqArray((computer_name, toDate(ingest_timestamp)))) AS self_exec FROM %[1]s FINAL WHERE %[2]s%[3]s AND parent_image != '' AND image != '' GROUP BY node), ",
		procLineage, inTree("process_guid"), frac()))
	// pm = per-node command line + user from the node's own lineage row (pl), a primary-key lookup.
	// Only nodes need one (a node is its own member), not every member of a collapsed group.
	// A row written before proc_lineage carried the user reads '\0' there and falls back to its
	// logs row (pu), found by the logs primary key the lineage row holds; a guid filter defeats the
	// bloom on large trees. Truncated: command lines can be enormous.
	nodes := fmt.Sprintf("process_guid GLOBAL IN (SELECT guid FROM %s WHERE guid = node)", ProvenanceTreeTable)
	b.WriteString(fmt.Sprintf("pl AS (SELECT process_guid AS guid, any(substring(commandline, 1, 300)) AS command_line, max(proc_user) AS lineage_user FROM %[1]s WHERE %[2]s%[3]s AND %[4]s GROUP BY guid), ",
		procLineage, timeWin, frac(), nodes))
	b.WriteString(fmt.Sprintf("pu AS (SELECT fields.process_guid::String AS guid, any(fields.user::String) AS proc_user FROM %[1]s WHERE %[2]s%[3]s "+
		"AND (timestamp, log_id) GLOBAL IN (SELECT timestamp, log_id FROM %[4]s WHERE %[5]s%[3]s AND proc_user = '\\0') AND fields.bifract_category = 'process_creation' GROUP BY guid), ",
		logs, timeWin, frac(), procLineage, nodes))
	b.WriteString("pm AS (SELECT pl.guid AS guid, pl.command_line AS command_line, if(pl.lineage_user = '\\0', pu.proc_user, pl.lineage_user) AS proc_user FROM pl LEFT JOIN pu ON pu.guid = pl.guid) ")

	// joined: one row per edge with its baseline, plus the edge's OWN share of that baseline. The
	// baseline includes the activity being scored, so each count is taken leave-one-out: the
	// distinct host-days among every edge in this tree with the same relationship (self_edge), the
	// same source and relationship to any target (self_src), and the same target from any
	// source (self_tgt), which is exactly what those edges added to fe, ft and gf (sx does the same
	// for exec_hd). A time cutoff cannot do this: proc_freq buckets by ingest day, and a replayed
	// dataset lands on one.
	joined := "SELECT e.src_node AS parent, e.dst_node AS child, e.label AS label, e.event_type AS event_type, e.fkey_src AS fkey_src, " +
		"e.log_id AS log_id, e.timestamp AS timestamp, e.fractal_id AS fractal_id, e.host AS host, e.parent_label AS parent_label, " +
		"coalesce(pm.command_line, '') AS command_line, coalesce(pm.proc_user, '') AS proc_user, " +
		"fe.hd AS fe_hd, fe.first_seen AS fe_first, ft.hd AS ft_hd, gf.hd AS gf_hd, gf.hosts_n AS gf_hosts, ins.exec_hd AS exec_hd, sx.self_exec AS self_exec, " +
		"(1 - ins.instab) * (1 - outs.instab) AS stability, " +
		"length(groupUniqArrayArray(e.hd_pairs) OVER (PARTITION BY e.fkey_src, e.event_type, e.fkey_tgt)) AS self_edge, " +
		"length(groupUniqArrayArray(e.hd_pairs) OVER (PARTITION BY e.fkey_src, e.event_type)) AS self_src, " +
		"length(groupUniqArrayArray(e.hd_pairs) OVER (PARTITION BY e.event_type, e.fkey_tgt)) AS self_tgt " +
		fmt.Sprintf("FROM (%s) AS e ", edgesAgg) +
		"LEFT JOIN fe ON fe.src_image = e.fkey_src AND fe.event_type = e.event_type AND fe.target_norm = e.fkey_tgt " +
		"LEFT JOIN ft ON ft.src_image = e.fkey_src AND ft.event_type = e.event_type " +
		"LEFT JOIN gf ON gf.event_type = e.event_type AND gf.target_norm = e.fkey_tgt " +
		"LEFT JOIN ins ON ins.node = e.fkey_src " +
		"LEFT JOIN outs ON outs.node = e.fkey_tgt " +
		"LEFT JOIN sx ON sx.node = e.fkey_src " +
		"LEFT JOIN pm ON pm.guid = e.dst_node"

	// Per-edge anomaly is NoDoze Eq.1 inverted, 1 - IN(SRC) * M * OUT(DST), with M the transition
	// probability Freq(src,rel,target) / Freq(src,rel,*) over everyone else's host-days. It is an
	// INTERMEDIATE only: NoDoze scores a PATH (Eq.2/Eq.3), which diffuseProvenanceRows computes by
	// propagating these values. Three cases, named in score_basis:
	//   - transition: the source is an established binary (it made this kind of relationship, or
	//     simply ran, on someone else's host-day). A target nobody else reached from it is M = 0,
	//     the paper's never-seen case, and scores 1: Word spawning its first child is red.
	//   - new_source: the binary never ran anywhere else. M is undefined, and calling it 0 would
	//     paint every child of a new binary red. Its edges are scored by how rarely anyone else
	//     touched the target, 1 / (1 + other host-days): a never-seen C2 scores 1, a shared resolver
	//     near 0. The binary's own novelty is the edge INTO it, which the transition case scores.
	//   - no_source: ghost roots (no parent_image) can never match proc_freq, whose MVs all require
	//     parent_image != '', so there is nothing for the child to be unusual FOR. Scores 0.
	// Global rarity is never folded into the transition score: greatest() of the two measured
	// worse than either alone. The target's footprint ships as its own columns instead.
	scored := "SELECT parent, child, label, event_type, log_id, timestamp, fractal_id, host, parent_label, command_line, proc_user, " +
		"greatest(0, toFloat64(fe_hd) - self_edge) AS edge_host_days, " +
		"greatest(0, toFloat64(ft_hd) - self_src) AS source_host_days, " +
		"greatest(0, toFloat64(exec_hd) - self_exec) AS source_exec_host_days, " +
		"greatest(0, toFloat64(gf_hd) - self_tgt) AS target_host_days, " +
		"toFloat64(gf_hosts) AS target_hosts, " +
		fmt.Sprintf("toFloat64(%d) AS total_hosts, ", bl.TotalHosts) +
		"multiIf(fkey_src = '', 'no_source', source_host_days > 0 OR source_exec_host_days > 0, 'transition', 'new_source') AS score_basis, " +
		"round(multiIf(score_basis = 'no_source', 0, score_basis = 'transition', " +
		"1 - stability * if(source_host_days > 0, least(1, edge_host_days / source_host_days), 0), 1 / (1 + target_host_days)), 4) AS anomaly_score, " +
		"anomaly_score AS edge_score, " +
		"if(fe_first = toDate(0), '', toString(fe_first)) AS first_seen, " +
		"toFloat64(1) AS group_size, '' AS group_first, '' AS group_last, '' AS recon_role " +
		"FROM (" + joined + ") AS joined"

	// Leaf gating differs by mode. Non-diffuse: filter leaves by the user threshold in SQL and
	// keep the top MaxRows non-spawn edges globally. Diffuse: the FINAL threshold is applied over
	// the PROPAGATED score, so a promotable leaf must survive -- SQL keeps leaves above a low
	// floor, capped PER PROCESS (bounded payload), and diffuseProvenanceRows re-thresholds.
	// Either way ALL spawn edges are kept so process structure can never be truncated away.
	leafFloor := strconv.FormatFloat(threshold, 'f', -1, 64)
	partitionBy := "(event_type = 'spawn')"
	capN, applyCap := opts.MaxRows, opts.MaxRows > 0
	if diffuse {
		leafFloor = diffuseLeafFloor
		partitionBy = "(event_type = 'spawn'), parent" // per-process leaf cap
		capN, applyCap = diffusePerProcLeaves, true
	}
	cols := strings.Join(ProvenanceColumns, ", ")
	b.WriteString("SELECT " + cols + " FROM (")
	b.WriteString("SELECT " + cols)
	if applyCap {
		b.WriteString(fmt.Sprintf(", row_number() OVER (PARTITION BY %s ORDER BY anomaly_score DESC) AS _rn", partitionBy))
	}
	b.WriteString(" FROM (" + scored + ") AS scored ")
	b.WriteString(fmt.Sprintf("WHERE event_type = 'spawn' OR anomaly_score >= %s) AS ranked ", leafFloor))
	if applyCap {
		b.WriteString(fmt.Sprintf("WHERE event_type = 'spawn' OR _rn <= %d ", capN))
	}
	b.WriteString("ORDER BY (event_type = 'spawn') DESC, anomaly_score DESC")

	// Not run through validateGeneratedSQL: this statement is fully machine-composed and
	// legitimately multi-source UNION ALLs the five edge types (which the shared validator
	// only whitelists for WITH RECURSIVE queries). The tree arrives as external data, and the
	// edge keys are escaped via escapeString, so nothing can break out of a string literal.
	return b.String()
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
//
// The former logs-based filter also excluded names resolving ONLY to internal IPs (via
// query_results); that signal is not carried in the process_edges rollup, so it is not applied here
// -- the proc_freq host/image rarity gate prunes widely-resolved internal names instead.
func internalDomainExpr(col string) string {
	return "(position(" + col + ", '.') = 0 OR startsWith(" + col + ", '_') OR position(" + col + ", '._msdcs.') > 0)"
}

// BuildReconnectionSQL is the cross-tree reverse lookup. Given the subgraph in
// ProvenanceTreeTable it returns peer-candidate rows (schema: recon_type, peer_guid, object_id, label, anomaly,
// peer_image, peer_log_id, peer_ts, peer_fractal). It is deliberately per-subgraph and
// rarity-pruned so it never becomes an all-pairs join over the full table:
//   - file: proc_lineage processes launched with image == a path THIS tree wrote (dropped
//     then executed), looked up by image_lc.
//   - net/dns: process_edges rows touching an external IP / domain THIS tree touched, but only
//     for artifacts under the proc_freq host and image prevalence ceilings (rareGate), so
//     shared CDNs/resolvers/update servers do not reconnect everything.
//   - remote_thread/process_access: target guids this tree injected into / opened -- emitted
//     already by pass-2, so returned only as expansion candidates (object_id empty).
//
// Returns "" when reconnection is disabled or no edge type is selected.
//
// totalHosts/totalImages (BuildProvenanceSummarySQL, or BuildReconnectionTotalsSQL until the
// summary is filled; read once per pgr() call, see provenanceScoreSQL) are the rarity-gate
// ceiling inputs, substituted as literals instead of live scalar subqueries.
func BuildReconnectionSQL(p ProvenanceParams, totalHosts, totalImages int64, opts QueryOptions) string {
	if !p.Reconnect {
		return ""
	}
	want := func(reconType string) bool {
		et := reconnectEdgeType[reconType]
		return len(p.EdgeTypes) == 0 || p.EdgeTypes[et]
	}
	notInTree := func(col string) string {
		return col + " GLOBAL NOT IN (SELECT guid FROM " + ProvenanceTreeTable + ")"
	}

	logs := opts.EffectiveTableName()
	procLineage := procLineageTable(opts)
	procFreq := procFreqTable(opts)

	tsStart := chTimeLiteral(opts.StartTime)
	tsEnd := chTimeLiteral(opts.EndTime)
	timeWin := fmt.Sprintf("timestamp >= '%s' AND timestamp <= '%s'", tsStart, tsEnd)
	fractal := procLineageFractalCond(opts, "")
	fracAnd := ""
	if fractal != "" {
		fracAnd = " AND " + fractal
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

	// net/dns read the process_edges rollup. The tree side is a primary-key lookup (process_guid
	// IN tree); the peer side and the rarity gate look artifacts up by (event_type, target), the
	// by_target projections of process_edges and proc_freq. fkey_tgt is the abstracted artifact
	// (matches proc_freq.target_norm), dst_node is the 'net:'/'dns:' node id, fkey_src the actor
	// image, label the raw artifact.
	edgeTable := procEdgesTable(opts)
	edgeFrac := ""
	if fractal != "" {
		edgeFrac = fractal + " AND "
	}
	// rareGate is the proc_freq rarity subquery (host- + image-prevalence ceilings) an artifact must
	// pass to bridge; et is the proc_freq event_type. Shared by net/dns. It aggregates only the
	// tree's own targets, which are all the callers test, instead of every target in the fractal.
	rareGate := func(et string) string {
		treeTargets := fmt.Sprintf("SELECT fkey_tgt FROM %s WHERE %sevent_type = '%s' AND %s", edgeTable, edgeFrac, et, inTree("process_guid"))
		return fmt.Sprintf("SELECT target_norm FROM %s WHERE event_type = '%s'%s AND target_norm GLOBAL IN (%s) GROUP BY target_norm "+
			"HAVING length(groupUniqArrayMerge(%d)(hosts)) <= %s AND uniqExact(src_image) <= %s",
			procFreq, et, fracAnd, treeTargets, procFreqHostsCap, hostGate, imageGate)
	}

	// file: write -> execute. The tree's written paths come from the edge rollup (label = raw
	// target_file); peer = a proc_lineage process launched with image == that path.
	if want("file") {
		writtenPaths := fmt.Sprintf(
			"SELECT lower(label) AS p, any(process_guid) AS writer FROM %[1]s WHERE %[2]sevent_type = 'file_write' "+
				"AND %[3]s AND label != '' GROUP BY p ORDER BY p LIMIT %[4]d",
			edgeTable, edgeFrac, inTree("process_guid"), maxReconnectCandidateArtifacts)
		// Latest row per process running a written path, looked up by image_lc (the by_image
		// projection). Deduped with LIMIT BY rather than FINAL, which on a Distributed table only
		// dedupes within each shard.
		executors := fmt.Sprintf(
			"SELECT process_guid, image, log_id, timestamp, fractal_id, computer_name FROM %[1]s WHERE %[2]simage_lc GLOBAL IN (SELECT p FROM (%[3]s)) "+
				"AND %[4]s ORDER BY process_guid, timestamp DESC, log_id DESC LIMIT 1 BY process_guid",
			procLineage, edgeFrac, writtenPaths, notInTree("process_guid"))
		parts = append(parts, fmt.Sprintf(
			"SELECT 'file' AS recon_type, pl.process_guid AS peer_guid, wp.writer AS src_guid, '' AS object_id, "+
				"pl.image AS label, toFloat64(1.0) AS anomaly, pl.image AS peer_image, pl.log_id AS peer_log_id, "+
				"toString(pl.timestamp) AS peer_ts, pl.fractal_id AS peer_fractal, pl.computer_name AS peer_host "+
				"FROM (%[1]s) AS pl GLOBAL INNER JOIN (%[2]s) AS wp ON lower(pl.image) = wp.p "+
				"ORDER BY count() OVER (PARTITION BY wp.p), pl.process_guid LIMIT %[3]d",
			executors, writtenPaths, maxReconnectPeers))
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
				"AND %[3]s AND %[4]s AND fkey_tgt GLOBAL IN (%[5]s) GROUP BY ip ORDER BY ip LIMIT %[6]d",
			edgeTable, edgeFrac, inTree("process_guid"), extOnly, rareGate("net_connect"), maxReconnectCandidateArtifacts)
		peerScan := fmt.Sprintf(
			"SELECT process_guid AS peer_guid, fkey_tgt AS ip, dst_node, fkey_src AS img, log_id, "+
				"toString(timestamp) AS ts, fractal_id, computer_name AS host FROM %[1]s WHERE %[2]sevent_type = 'net_connect' "+
				"AND %[3]s AND fkey_tgt GLOBAL IN (SELECT ip FROM (%[4]s))",
			edgeTable, edgeFrac, notInTree("process_guid"), rareIP)
		parts = append(parts, fmt.Sprintf(
			"SELECT 'net' AS recon_type, l.peer_guid AS peer_guid, any(ri.toucher) AS src_guid, l.dst_node AS object_id, "+
				"any(l.ip) AS label, toFloat64(0.85) AS anomaly, any(l.img) AS peer_image, any(l.log_id) AS peer_log_id, "+
				"any(l.ts) AS peer_ts, any(l.fractal_id) AS peer_fractal, any(l.host) AS peer_host "+
				"FROM (%[1]s) AS l GLOBAL INNER JOIN (%[2]s) AS ri ON l.ip = ri.ip GROUP BY peer_guid, object_id ORDER BY count() OVER (PARTITION BY object_id), peer_guid LIMIT %[3]d",
			peerScan, rareIP, maxReconnectPeers))
	}

	// dns: two processes -> same rare DOMAIN. The rollup's dns edges already exclude IP-form queries
	// (the MV's NOT match(query, ipv4) filter), so an IP is never both a dns: and net: node.
	if want("dns") {
		rareDom := fmt.Sprintf(
			"SELECT fkey_tgt AS q, any(process_guid) AS toucher FROM %[1]s WHERE %[2]sevent_type = 'dns_query' "+
				"AND %[3]s AND NOT %[6]s AND fkey_tgt GLOBAL IN (%[4]s) GROUP BY q ORDER BY q LIMIT %[5]d",
			edgeTable, edgeFrac, inTree("process_guid"), rareGate("dns_query"), maxReconnectCandidateArtifacts, internalDomainExpr("fkey_tgt"))
		peerScan := fmt.Sprintf(
			"SELECT process_guid AS peer_guid, fkey_tgt AS q, dst_node, fkey_src AS img, log_id, "+
				"toString(timestamp) AS ts, fractal_id, computer_name AS host FROM %[1]s WHERE %[2]sevent_type = 'dns_query' "+
				"AND %[3]s AND fkey_tgt GLOBAL IN (SELECT q FROM (%[4]s))",
			edgeTable, edgeFrac, notInTree("process_guid"), rareDom)
		parts = append(parts, fmt.Sprintf(
			"SELECT 'dns' AS recon_type, l.peer_guid AS peer_guid, any(ri.toucher) AS src_guid, l.dst_node AS object_id, "+
				"any(l.q) AS label, toFloat64(0.85) AS anomaly, any(l.img) AS peer_image, any(l.log_id) AS peer_log_id, "+
				"any(l.ts) AS peer_ts, any(l.fractal_id) AS peer_fractal, any(l.host) AS peer_host "+
				"FROM (%[1]s) AS l GLOBAL INNER JOIN (%[2]s) AS ri ON l.q = ri.q GROUP BY peer_guid, object_id ORDER BY count() OVER (PARTITION BY object_id), peer_guid LIMIT %[3]d",
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
				"AND %[7]s AND fields.target_process_guid::String != '' "+
				"AND %[9]s GROUP BY peer_guid ORDER BY peer_guid LIMIT %[8]d",
			reconType, anomaly, logs, timeWin, fracAnd, category, inTree("fields.source_process_guid::String"), maxReconnectPeers,
			notInTree("fields.target_process_guid::String"))
	}
	if want("remote_thread") {
		parts = append(parts, p2p("remote_thread", "remote_thread", "0.95"))
	}
	if want("process_access") {
		parts = append(parts, p2p("process_access", "process_access", "0.9"))
	}

	return strings.Join(parts, " UNION ALL ")
}

// ReconnectionEdgeRows renders the reconnection bridges as pgr rows (ProvenanceColumns).
// Reconnection owns these edges explicitly so a shared object node always converges even when
// pass-2 prunes the underlying low-anomaly leaf edge:
//   - net/dns: two edges -- tree-toucher -> object node AND peer -> object node.
//   - file: one edge -- writer -> executor (the file path in Label; no object node).
//   - injection/access: none (pass-2 owns source->target); peer is expansion-only.
//
// Edges are deduped by (parent, child, event_type).
//
// The peer -> object edge carries the peer's image in parent_label. Only the top few peers are
// expanded into real subtrees, so most peers reach the graph through this edge alone and have no
// process_creation row of their own to name them -- but the very event that formed the bridge (the
// peer's connection / lookup) does name the image, so the node renders as its image rather than a
// bare guid.
func ReconnectionEdgeRows(peers []ReconnectPeer) []map[string]interface{} {
	seen := map[string]bool{}
	var rows []map[string]interface{}
	emit := func(parent, child, label, eventType string, anomaly float64, logID, ts, fractal, host, parentLabel string) {
		if parent == "" || child == "" {
			return
		}
		key := parent + "\x00" + child + "\x00" + eventType
		if seen[key] {
			return
		}
		seen[key] = true
		// A bridge's severity is a fixed per-type value, not a baseline measurement, so its counts
		// are zero and score_basis says so.
		r := map[string]interface{}{}
		for _, c := range ProvenanceNumericColumns {
			r[c] = float64(0)
		}
		for k, v := range map[string]interface{}{
			"parent": parent, "child": child, "label": label, "event_type": eventType,
			"anomaly_score": anomaly, "edge_score": anomaly, "score_basis": "reconnect", "first_seen": "",
			"log_id": logID, "timestamp": ts, "fractal_id": fractal, "command_line": "", "proc_user": "",
			"host": host, "parent_label": parentLabel, "group_size": float64(1), "group_first": "", "group_last": "", "recon_role": "",
		} {
			r[k] = v
		}
		rows = append(rows, r)
	}

	for _, pe := range peers {
		et := "reconnect_" + pe.ReconType
		switch {
		case pe.ObjectID != "": // net/dns: converge both endpoints on the shared object node
			emit(pe.SrcGUID, pe.ObjectID, pe.Label, et, pe.Anomaly, "", "", "", "", "")
			emit(pe.PeerGUID, pe.ObjectID, pe.Label, et, pe.Anomaly, pe.PeerLogID, pe.PeerTS, pe.PeerFractal, pe.PeerHost, pe.PeerImage)
		case pe.ReconType == "file" && pe.SrcGUID != "": // file: writer -> executor
			emit(pe.SrcGUID, pe.PeerGUID, pe.Label, et, pe.Anomaly, pe.PeerLogID, pe.PeerTS, pe.PeerFractal, pe.PeerHost, "")
		}
		// injection/access: skip (pass-2 owns the source->target edge)
	}
	return rows
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

func init() {
	// pgr() generates the pipeline source; its options are all named.
	registerSpec(&CommandSpec{Name: "pgr", Params: []ParamSpec{
		namedLit("start"), namedLit("depth"), namedLit("direction"), namedLit("limit"),
		namedLit("threshold"), namedLit("lambda"), namedLit("diffuse"),
		namedLit("reconnect"), namedLit("peers"),
		namedList("include"), namedList("exclude"),
	}})
}

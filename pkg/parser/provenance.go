package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// maxProvenanceGuids caps the tree-guid set fed into the pass-2 leaf-fetch IN-list, so a
// pathological tree can't produce an unbounded query.
const maxProvenanceGuids = 10000

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

// Reconnection (NoDoze object-mediated) caps. Every value bounds a step so cross-tree
// reconnection stays per-subgraph and rarity-pruned -- never an all-pairs join over the
// full table. See pkg/query/provenance.go for how the resolver orchestrates these.
const (
	// maxReconnectCandidateArtifacts caps the distinct artifacts (written paths / external
	// IPs / domains) pulled from the current subgraph before the reverse lookup.
	maxReconnectCandidateArtifacts = 500
	// maxReconnectPeers caps total peer edges the reverse lookup returns.
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
	Limit     int             // outer result-row cap (default defaultPgrLimit, max maxPgrLimit)
}

// ProvenanceOrderBy is the outer-query ORDER BY resolvePgrSource attaches alongside Limit: spawn
// edges first, then by anomaly. Matches the ordering pass-2's own SQL already applies internally,
// so if Limit ever truncates, it drops low-signal leaves before process structure.
var ProvenanceOrderBy = []string{"(event_type = 'spawn') DESC", "anomaly_score DESC"}

// provenanceLeafTypes are the non-spawn edge event_types pgr can emit. spawn is the tree
// backbone and is always present, so it is not listed here.
var provenanceLeafTypes = []string{"file_write", "net_connect", "dns_query", "remote_thread", "process_access"}

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
	p := ProvenanceParams{Depth: 10, Direction: "both", Threshold: 0.7, Reconnect: true, Diffuse: true, Limit: defaultPgrLimit}
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
		case strings.HasPrefix(arg, "reconnect="):
			v := strings.ToLower(strings.Trim(strings.TrimPrefix(arg, "reconnect="), "\"'"))
			p.Reconnect = v != "false" && v != "0" && v != "no"
		case strings.HasPrefix(arg, "diffuse="):
			v := strings.ToLower(strings.Trim(strings.TrimPrefix(arg, "diffuse="), "\"'"))
			p.Diffuse = v != "false" && v != "0" && v != "no"
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
	// Resolve the non-spawn edge-type set: start from include= (or all leaf types), then drop
	// any exclude=. spawn is the backbone and is never filtered out here.
	p.EdgeTypes = map[string]bool{}
	base := includeTypes
	if len(base) == 0 {
		base = provenanceLeafTypes
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

// BuildProcessTreeQuery is pass 1: the ptg() spawn-tree SQL. The handler runs it and
// collects the process_guid set (tree membership) to bound the pass-2 leaf-fetch.
//
// The tree's completeness must NOT depend on the display row limit: buildProcessTreeSQL
// caps its output at opts.MaxRows, and if that (the configured MaxQueryRows) is smaller than
// the tree, it drops whole right-side/deep branches -- so a child only a few levels down
// silently disappears. Override the cap to the guid budget (the same bound pass 2 uses), so
// the tree is only ever limited by maxProvenanceGuids, not by the smaller display limit.
func BuildProcessTreeQuery(p ProvenanceParams, opts QueryOptions) (string, error) {
	topts := opts
	topts.MaxRows = maxProvenanceGuids
	res, err := buildProcessTreeSQL(p.Start, p.Depth, p.Direction, nil, "", nil, topts)
	if err != nil {
		return "", err
	}
	return res.SQL, nil
}

// BuildProvenanceScoringSQL is pass 2: given the tree's guids, assemble every edge
// (spawn from proc_lineage; file/net/injection/handle-access leaf edges from logs, bounded
// by time + guid + category), score each against proc_freq (anomaly = 1 - freq(edge)/freq(src,rel,*)),
// and keep the full spawn spine plus any non-spawn edge at/above threshold. Output columns:
// parent, child, label, event_type, anomaly_score -- an edge list graph() renders directly.
// edgeTypes selects which non-spawn edge branches to generate (nil/empty = all); spawn is
// always included as the tree backbone. Skipping a branch avoids scanning logs for that
// bifract_category entirely -- a real cost saving at scale, not just a post-filter.
func BuildProvenanceScoringSQL(guids []string, threshold float64, edgeTypes map[string]bool, diffuse bool, opts QueryOptions) (string, error) {
	if len(guids) == 0 {
		return "", fmt.Errorf("pgr: no process guids to score")
	}
	// Defensive: a NaN/out-of-range threshold would emit an invalid or degenerate WHERE clause.
	if threshold != threshold || threshold < 0 { // NaN or negative
		threshold = 0
	} else if threshold > 1 {
		threshold = 1
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
	procFreq := opts.ProcFreqTable
	if procFreq == "" {
		procFreq = "proc_freq"
	}
	logs := opts.EffectiveTableName()

	tsStart := opts.StartTime.Format("2006-01-02 15:04:05")
	tsEnd := opts.EndTime.Format("2006-01-02 15:04:05")
	timeWin := fmt.Sprintf("timestamp >= '%s' AND timestamp <= '%s'", tsStart, tsEnd)
	fractal := procLineageFractalCond(opts, "")
	frac := func() string {
		if fractal == "" {
			return ""
		}
		return " AND " + fractal
	}

	// Abstraction shortcuts (identical transform to the proc_freq MVs -> matching join keys).
	aPath := func(col string) string { return abstractExpr(col, AbstractPath) }
	aIP := func(col string) string { return abstractExpr(col, AbstractIP) }

	// Each edge SELECT yields: src_node, dst_node, label, event_type, fkey_src, fkey_tgt,
	// log_id, timestamp, fractal_id. log_id + timestamp + fractal_id are exactly the columns
	// the standard log-detail fetch (/logs/fields) needs, projected in the same shape a normal
	// search row uses (toString(timestamp)) -- so clicking a pgr table row or a pgraph node
	// goes through the identical, proven detail-load path instead of a bespoke lookup.
	spawnEdges := fmt.Sprintf(
		"SELECT parent_guid AS src_node, process_guid AS dst_node, image AS label, 'spawn' AS event_type, "+
			"%s AS fkey_src, %s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, computer_name AS host FROM %s FINAL WHERE process_guid IN (%s)%s",
		aPath("parent_image"), aPath("image"), procLineage, inList, frac())

	fileEdges := fmt.Sprintf(
		"SELECT fields.process_guid::String AS src_node, concat('file:', %[1]s) AS dst_node, "+
			"fields.target_file::String AS label, 'file_write' AS event_type, %[2]s AS fkey_src, %[1]s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, fields.computer_name::String AS host "+
			"FROM %[3]s WHERE %[4]s%[5]s AND fields.process_guid::String IN (%[6]s) "+
			"AND fields.bifract_category = 'file_write' AND fields.image::String != '' AND fields.target_file::String != ''",
		aPath("fields.target_file::String"), aPath("fields.image::String"), logs, timeWin, frac(), inList)

	netEdges := fmt.Sprintf(
		"SELECT fields.process_guid::String AS src_node, concat('net:', %[1]s) AS dst_node, "+
			"fields.dst_ip::String AS label, 'net_connect' AS event_type, %[2]s AS fkey_src, %[1]s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, fields.computer_name::String AS host "+
			"FROM %[3]s WHERE %[4]s%[5]s AND fields.process_guid::String IN (%[6]s) "+
			"AND fields.bifract_category = 'network_connect' AND fields.image::String != '' AND fields.dst_ip::String != ''",
		aIP("fields.dst_ip::String"), aPath("fields.image::String"), logs, timeWin, frac(), inList)

	// DNS edges: process -> resolved domain node. src is the querying image; target is the
	// abstracted (lowercased, root-dot-stripped) query name.
	dnsEdges := fmt.Sprintf(
		"SELECT fields.process_guid::String AS src_node, concat('dns:', %[1]s) AS dst_node, "+
			"fields.query::String AS label, 'dns_query' AS event_type, %[2]s AS fkey_src, %[1]s AS fkey_tgt, log_id, toString(timestamp) AS timestamp, fractal_id, fields.computer_name::String AS host "+
			"FROM %[3]s WHERE %[4]s%[5]s AND fields.process_guid::String IN (%[6]s) "+
			"AND fields.bifract_category = 'dns_query' AND fields.image::String != '' AND fields.query::String != '' AND NOT match(fields.query::String, %[7]s)",
		abstractExpr("fields.query::String", AbstractDomain), aPath("fields.image::String"), logs, timeWin, frac(), inList, ipv4Re)

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
			aPath("fields.image::String"), aPath("fields.target_image::String"), logs, timeWin, frac(), inList, eventType, category)
	}

	// spawn is always present (the tree spine); leaf branches are included per edgeTypes.
	parts := []string{spawnEdges}
	if want("file_write") {
		parts = append(parts, fileEdges)
	}
	if want("net_connect") {
		parts = append(parts, netEdges)
	}
	if want("dns_query") {
		parts = append(parts, dnsEdges)
	}
	if want("remote_thread") {
		parts = append(parts, p2pEdges("remote_thread", "remote_thread"))
	}
	if want("process_access") {
		parts = append(parts, p2pEdges("process_access", "process_access"))
	}
	edges := strings.Join(parts, " UNION ALL ")

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
	totalHosts := fmt.Sprintf("(SELECT uniqExact(computer_name) FROM %s%s)", procLineage, freqWhere)
	gr := fmt.Sprintf("if(coalesce(gf.hostct, 0) >= %[1]d, 0, if(%[2]s = 0, 0, 1 - coalesce(gf.hostct, 0) / %[2]s))", procFreqHostsCap, totalHosts)
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

// privateV4Re / privateAddrRe: a ClickHouse regex ALTERNATION (no anchors) for the leading form of
// a PRIVATE/internal address. Covers RFC1918 + loopback + link-local IPv4, IPv6 loopback (::1),
// link-local (fe80:), ULA (fc00::/7 as fcXX:/fdXX:), and IPv4-MAPPED IPv6 (::ffff:<private-v4>,
// which plain prefix matching missed and would leak internal endpoints through as "external").
// internalIPMatch anchors it. Shared by the net endpoint filter and the DNS internal-lookup gate so
// the two never disagree on what "internal" means.
const privateV4Re = `10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.`
const privateAddrRe = privateV4Re + `|::1$|fe80:|f[cd][0-9a-f]{2}:|::ffff:(` + privateV4Re + `)`

// internalIPMatch returns a ClickHouse boolean expr: true when the address expr `col` is internal.
func internalIPMatch(col string) string {
	return "match(" + col + ", '^(" + privateAddrRe + ")')"
}

// ipv4Re is a quoted RE2 literal (for match()) that recognises a bare IPv4 address -- used to
// pull IP-form endpoints out of dns_query query/query_results.
const ipv4Re = `'^([0-9]{1,3}\\.){3}[0-9]{1,3}$'`

// internalDNSExpr returns a ClickHouse boolean expr that is TRUE for a BENIGN INTERNAL name lookup
// that must not form a reconnection bridge. Reconnection peer edges are hard-capped, so noisy
// internal name resolution (NetBIOS, AD service discovery, internal hosts) would crowd out real
// shared-IOC bridges. A name is treated as internal when ANY holds:
//   - single-label name (no dot after trimming a trailing root dot) -- NetBIOS / LLMNR / mDNS
//   - AD service-discovery record -- starts with '_' (_ldap._tcp...) or contains '._msdcs.'
//   - it resolves ONLY to internal addresses -- there is at least one IP-form result and none of the
//     IP-form results are external (hostname/SRV-data results are ignored, so a mixed or non-IP
//     result set never falsely excludes a real external lookup)
// qCol/rCol are the query and query_results column expressions.
func internalDNSExpr(qCol, rCol string) string {
	resultsArr := "arrayFilter(x -> x != '', splitByChar(';', " + rCol + "))"
	isIP := "(match(x, " + ipv4Re + ") OR position(x, ':') > 0)"
	hasIP := "arrayExists(x -> " + isIP + ", " + resultsArr + ")"
	hasExternalIP := "arrayExists(x -> " + isIP + " AND NOT " + internalIPMatch("x") + ", " + resultsArr + ")"
	noTLD := "position(replaceRegexpOne(" + qCol + ", '\\\\.+$', ''), '.') = 0"
	service := "startsWith(lower(" + qCol + "), '_') OR position(lower(" + qCol + "), '._msdcs.') > 0"
	return "(" + noTLD + " OR " + service + " OR (" + hasIP + " AND NOT " + hasExternalIP + "))"
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
func BuildReconnectionSQL(guids []string, p ProvenanceParams, opts QueryOptions) (string, error) {
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
	freqWhereClause := ""
	if fractal != "" {
		freqFrac = " AND " + fractal
		freqWhereClause = " WHERE " + fractal
	}

	// Rarity gate ceiling (net/dns): an artifact bridges while it is on <= max(absolute floor,
	// fraction * total_hosts) hosts. Scales with the fleet so a rare C2 on many hosts still
	// reconnects; only near-ubiquitous infrastructure is pruned.
	totalHosts := fmt.Sprintf("(SELECT length(groupUniqArrayMerge(%d)(hosts)) FROM %s%s)", procFreqHostsCap, procFreq, freqWhereClause)
	hostGate := fmt.Sprintf("greatest(toUInt64(%d), toUInt64(%s * %s))", reconnectHostPrevalenceMax, reconnectHostFraction, totalHosts)
	// Process-diversity gate (see reconnectImagePrevalenceMax): prune artifacts touched by many
	// distinct process images, so ubiquitous software infra is dropped even on single-host data.
	totalImages := fmt.Sprintf("(SELECT uniqExact(src_image) FROM %s%s)", procFreq, freqWhereClause)
	imageGate := fmt.Sprintf("greatest(toUInt64(%d), toUInt64(%s * %s))", reconnectImagePrevalenceMax, reconnectImageFraction, totalImages)

	// Common column order for every UNION branch:
	// recon_type, peer_guid, src_guid, object_id, label, anomaly, peer_image, peer_log_id, peer_ts, peer_fractal
	var parts []string

	// file: write -> execute. peer = process launched with image == a path this tree wrote;
	// src = the tree writer of that path. Emitted later as a direct writer -> executor edge.
	if want("file") {
		writtenPaths := fmt.Sprintf(
			"SELECT lower(fields.target_file::String) AS p, fields.process_guid::String AS writer FROM %[1]s WHERE %[2]s%[3]s "+
				"AND fields.process_guid::String IN (%[4]s) AND fields.bifract_category = 'file_write' "+
				"AND fields.target_file::String != '' GROUP BY p, writer LIMIT %[5]d",
			logs, timeWin, fracAnd, inList, maxReconnectCandidateArtifacts)
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

	// net (endpoint IP): two processes touch the same rare external IP, where "touch" means
	// CONNECT to it (network_connect.dst_ip) OR RESOLVE it (dns_query: an IP-form query name,
	// or any IP in the ';'-separated query_results). This bridges the DNS-resolve -> connect
	// chain -- one process resolves a domain/IP, others connect to that IP -- which pure
	// connect<->connect matching misses. src = a tree toucher of the IP, so the shared IP node
	// converges (tree + peer) independent of pass-2 pruning.
	if want("net") {
		// endpointIPs(guidCond, ipFilter): (guid, ip, img, log_id, ts, fractal, host) for external
		// IPv4/IPv6 endpoints a process connected to or resolved. When ipFilter (a subquery of
		// candidate IPs) is set, it is pushed INTO the network_connect scan so the idx_dst_ip
		// bloom can prune granules -- otherwise the peer side would scan the whole fleet's net/dns
		// volume and only filter via the join. dns IPs are filtered on the outer ip.
		endpointIPs := func(guidCond, ipFilter string) string {
			netExtra, dnsOuter := "", ""
			if ipFilter != "" {
				// GLOBAL IN: ipFilter is a subquery over the DISTRIBUTED logs/proc_freq tables. On a
				// cluster a plain IN(subquery-over-distributed) is denied (code 288); GLOBAL broadcasts
				// the small candidate-IP set to every shard. (No-op on a single node.)
				netExtra = " AND fields.dst_ip::String GLOBAL IN (" + ipFilter + ")"
				dnsOuter = " AND ip GLOBAL IN (" + ipFilter + ")"
			}
			return fmt.Sprintf(
				"SELECT guid, ip, img, log_id, ts, fractal, host FROM ("+
					"SELECT fields.process_guid::String AS guid, fields.dst_ip::String AS ip, fields.image::String AS img, log_id, toString(timestamp) AS ts, fractal_id AS fractal, fields.computer_name::String AS host "+
					"FROM %[1]s WHERE %[2]s%[3]s AND fields.bifract_category = 'network_connect' AND fields.process_guid::String %[4]s AND fields.dst_ip::String != ''%[7]s "+
					"UNION ALL "+
					"SELECT fields.process_guid::String AS guid, ip, fields.image::String AS img, log_id, toString(timestamp) AS ts, fractal_id AS fractal, fields.computer_name::String AS host "+
					"FROM %[1]s ARRAY JOIN arrayFilter(x -> x != '', arrayConcat(splitByChar(';', fields.query_results::String), array(fields.query::String))) AS ip "+
					"WHERE %[2]s%[3]s AND fields.bifract_category = 'dns_query' AND fields.process_guid::String %[4]s"+
					") WHERE (match(ip, %[5]s) OR position(ip, ':') > 0) AND %[6]s%[8]s",
				logs, timeWin, fracAnd, guidCond, ipv4Re, "NOT "+internalIPMatch("ip"), netExtra, dnsOuter)
		}
		rareIP := fmt.Sprintf(
			"SELECT ip, any(guid) AS toucher FROM (SELECT DISTINCT ip, guid FROM (%[1]s) LIMIT %[2]d) AS c WHERE ip GLOBAL IN ("+
				"SELECT target_norm FROM %[3]s WHERE event_type = 'net_connect'%[4]s GROUP BY target_norm "+
				"HAVING length(groupUniqArrayMerge(%[5]d)(hosts)) <= %[6]s AND uniqExact(src_image) <= %[7]s) GROUP BY ip",
			endpointIPs(fmt.Sprintf("IN (%s)", inList), ""), maxReconnectCandidateArtifacts,
			procFreq, freqFrac, procFreqHostsCap, hostGate, imageGate)
		parts = append(parts, fmt.Sprintf(
			"SELECT 'net' AS recon_type, l.guid AS peer_guid, any(ri.toucher) AS src_guid, concat('net:', %[1]s) AS object_id, "+
				"any(l.ip) AS label, toFloat64(0.85) AS anomaly, any(l.img) AS peer_image, any(l.log_id) AS peer_log_id, "+
				"any(l.ts) AS peer_ts, any(l.fractal) AS peer_fractal, any(l.host) AS peer_host "+
				"FROM (%[2]s) AS l GLOBAL INNER JOIN (%[3]s) AS ri ON l.ip = ri.ip GROUP BY peer_guid, object_id ORDER BY peer_guid LIMIT %[4]d",
			abstractExpr("l.ip", AbstractIP), endpointIPs(fmt.Sprintf("NOT IN (%s)", inList), "SELECT ip FROM ("+rareIP+")"), rareIP, maxReconnectPeers))
	}

	// dns: two processes -> same rare DOMAIN (IP-form queries are excluded here -- they route
	// through the net endpoint branch instead, so an IP isn't shown as both a dns: and net: node).
	if want("dns") {
		aDom := func(col string) string { return abstractExpr(col, AbstractDomain) }
		notIP := fmt.Sprintf(" AND NOT match(fields.query::String, %s)", ipv4Re)
		// Drop benign internal name lookups (NetBIOS, AD service records, internal-only resolutions)
		// so they don't consume the capped reconnection budget and crowd out real shared-IOC bridges.
		notInternal := " AND NOT " + internalDNSExpr("fields.query::String", "fields.query_results::String")
		rareDom := fmt.Sprintf(
			"SELECT q, any(t) AS toucher FROM (SELECT DISTINCT %[1]s AS q, fields.process_guid::String AS t "+
				"FROM %[2]s WHERE %[3]s%[4]s AND fields.process_guid::String IN (%[5]s) AND fields.bifract_category = 'dns_query' "+
				"AND fields.query::String != ''%[11]s%[12]s LIMIT %[6]d) AS c WHERE q GLOBAL IN ("+
				"SELECT target_norm FROM %[7]s WHERE event_type = 'dns_query'%[8]s GROUP BY target_norm "+
				"HAVING length(groupUniqArrayMerge(%[9]d)(hosts)) <= %[10]s AND uniqExact(src_image) <= %[13]s) GROUP BY q",
			aDom("fields.query::String"), logs, timeWin, fracAnd, inList, maxReconnectCandidateArtifacts,
			procFreq, freqFrac, procFreqHostsCap, hostGate, notIP, notInternal, imageGate)
		peerScan := fmt.Sprintf(
			"SELECT fields.process_guid::String AS peer_guid, %[1]s AS q, fields.image::String AS img, "+
				"log_id, toString(timestamp) AS ts, fractal_id, fields.computer_name::String AS host FROM %[2]s WHERE %[3]s%[4]s "+
				"AND fields.bifract_category = 'dns_query' AND fields.process_guid::String NOT IN (%[5]s) AND fields.query::String != ''%[6]s%[8]s "+
				"AND %[1]s GLOBAL IN (%[7]s)",
			aDom("fields.query::String"), logs, timeWin, fracAnd, inList, notIP, "SELECT q FROM ("+rareDom+")", notInternal)
		parts = append(parts, fmt.Sprintf(
			"SELECT 'dns' AS recon_type, l.peer_guid AS peer_guid, any(ri.toucher) AS src_guid, concat('dns:', l.q) AS object_id, "+
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

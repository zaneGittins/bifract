package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// maxProvenanceGuids caps the tree-guid set fed into the pass-2 leaf-fetch IN-list, so a
// pathological tree can't produce an unbounded query.
const maxProvenanceGuids = 10000

// ProvenanceParams are the parsed pgr() arguments the handler orchestrates the two-pass
// query with. start/depth/direction match ptg(); threshold prunes non-spawn edges below it.
type ProvenanceParams struct {
	Start     string
	Depth     int
	Direction string
	Threshold float64
}

// ExtractProvenanceParams scans a parsed pipeline for a pgr() command and returns its
// arguments. ok is false when there is no pgr() (or start= is missing). The query handler
// calls this before translation and, on a match, orchestrates the two-pass provenance query
// instead of the normal single-statement path (mirrors ExtractCommentParams).
func ExtractProvenanceParams(pipeline *PipelineNode) (ProvenanceParams, bool) {
	if pipeline == nil {
		return ProvenanceParams{}, false
	}
	for _, cmd := range pipeline.Commands {
		if cmd.Name != "pgr" {
			continue
		}
		p := ProvenanceParams{Depth: 10, Direction: "both", Threshold: 0.7}
		for _, arg := range cmd.Arguments {
			switch {
			case strings.HasPrefix(arg, "start="):
				p.Start = strings.Trim(strings.TrimPrefix(arg, "start="), "\"'")
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
			}
		}
		if p.Depth > 50 {
			p.Depth = 50
		}
		if p.Direction != "forward" && p.Direction != "backward" && p.Direction != "both" {
			p.Direction = "both"
		}
		return p, p.Start != ""
	}
	return ProvenanceParams{}, false
}

// ExtractPGraphConfig returns the render config when a pgr() pipeline pipes to pgraph() --
// the provenance-native visualization (process/file/socket nodes shaped by type, edges
// colored by anomaly_score). So `pgr(...)` alone yields the scored edge table (export / LLM /
// further piping) and `pgr(...) | pgraph()` renders the graph. pgraph() reads pgr()'s fixed
// output columns, so the only arg is an optional limit=.
func ExtractPGraphConfig(pipeline *PipelineNode) (map[string]interface{}, bool) {
	if pipeline == nil {
		return nil, false
	}
	for _, cmd := range pipeline.Commands {
		if cmd.Name != "pgraph" {
			continue
		}
		cfg := map[string]interface{}{"limit": 500}
		for _, arg := range cmd.Arguments {
			if strings.HasPrefix(arg, "limit=") {
				if n, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && n > 0 {
					cfg["limit"] = n
				}
			}
		}
		return cfg, true
	}
	return nil, false
}

// BuildProcessTreeQuery is pass 1: the ptg() spawn-tree SQL. The handler runs it and
// collects the process_guid set (tree membership) to bound the pass-2 leaf-fetch.
func BuildProcessTreeQuery(p ProvenanceParams, opts QueryOptions) (string, error) {
	res, err := buildProcessTreeSQL(p.Start, p.Depth, p.Direction, nil, "", nil, opts)
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
func BuildProvenanceScoringSQL(guids []string, threshold float64, opts QueryOptions) (string, error) {
	if len(guids) == 0 {
		return "", fmt.Errorf("pgr: no process guids to score")
	}
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
	// log_id, event_time. log_id + event_time identify the originating log so a clicked graph
	// node can fetch its source event via the fast timestamp-pruned lookup. Server TZ is UTC,
	// so toString(timestamp) is a UTC wall-clock the frontend turns into RFC3339 with a Z.
	spawnEdges := fmt.Sprintf(
		"SELECT parent_guid AS src_node, process_guid AS dst_node, image AS label, 'spawn' AS event_type, "+
			"%s AS fkey_src, %s AS fkey_tgt, log_id, toString(timestamp) AS event_time FROM %s FINAL WHERE process_guid IN (%s)%s",
		aPath("parent_image"), aPath("image"), procLineage, inList, frac())

	fileEdges := fmt.Sprintf(
		"SELECT fields.process_guid::String AS src_node, concat('file:', %[1]s) AS dst_node, "+
			"fields.target_file::String AS label, 'file_write' AS event_type, %[2]s AS fkey_src, %[1]s AS fkey_tgt, log_id, toString(timestamp) AS event_time "+
			"FROM %[3]s WHERE %[4]s%[5]s AND fields.process_guid::String IN (%[6]s) "+
			"AND fields.bifract_category = 'file_write' AND fields.image::String != '' AND fields.target_file::String != ''",
		aPath("fields.target_file::String"), aPath("fields.image::String"), logs, timeWin, frac(), inList)

	netEdges := fmt.Sprintf(
		"SELECT fields.process_guid::String AS src_node, concat('net:', %[1]s) AS dst_node, "+
			"fields.dst_ip::String AS label, 'net_connect' AS event_type, %[2]s AS fkey_src, %[1]s AS fkey_tgt, log_id, toString(timestamp) AS event_time "+
			"FROM %[3]s WHERE %[4]s%[5]s AND fields.process_guid::String IN (%[6]s) "+
			"AND fields.bifract_category = 'network_connect' AND fields.image::String != '' AND fields.dst_ip::String != ''",
		aIP("fields.dst_ip::String"), aPath("fields.image::String"), logs, timeWin, frac(), inList)

	// DNS edges: process -> resolved domain node. src is the querying image; target is the
	// abstracted (lowercased, root-dot-stripped) query name.
	dnsEdges := fmt.Sprintf(
		"SELECT fields.process_guid::String AS src_node, concat('dns:', %[1]s) AS dst_node, "+
			"fields.query::String AS label, 'dns_query' AS event_type, %[2]s AS fkey_src, %[1]s AS fkey_tgt, log_id, toString(timestamp) AS event_time "+
			"FROM %[3]s WHERE %[4]s%[5]s AND fields.process_guid::String IN (%[6]s) "+
			"AND fields.bifract_category = 'dns_query' AND fields.image::String != '' AND fields.query::String != ''",
		abstractExpr("fields.query::String", AbstractDomain), aPath("fields.image::String"), logs, timeWin, frac(), inList)

	// Process->process edges (injection / handle-access): source_process_guid in the tree,
	// real target_process_guid node.
	p2pEdges := func(category, eventType string) string {
		return fmt.Sprintf(
			"SELECT fields.source_process_guid::String AS src_node, fields.target_process_guid::String AS dst_node, "+
				"fields.target_image::String AS label, '%[7]s' AS event_type, %[1]s AS fkey_src, %[2]s AS fkey_tgt, log_id, toString(timestamp) AS event_time "+
				"FROM %[3]s WHERE %[4]s%[5]s AND fields.source_process_guid::String IN (%[6]s) "+
				"AND fields.bifract_category = '%[8]s' AND fields.source_image::String != '' AND fields.target_image::String != ''",
			aPath("fields.source_image::String"), aPath("fields.target_image::String"), logs, timeWin, frac(), inList, eventType, category)
	}

	edges := strings.Join([]string{
		spawnEdges, fileEdges, netEdges, dnsEdges,
		p2pEdges("remote_thread", "remote_thread"),
		p2pEdges("process_access", "process_access"),
	}, " UNION ALL ")

	freqWhere := ""
	if fractal != "" {
		freqWhere = " WHERE " + fractal
	}

	// fe = per-edge count; ft = denominator freq(src,rel,*). Unseen (src,rel) with no baseline
	// -> anomaly 1.0. Keep the whole spawn spine; prune only non-spawn edges below threshold.
	var b strings.Builder
	b.WriteString(fmt.Sprintf("WITH fe AS (SELECT src_image, event_type, target_norm, sum(event_count) AS cnt FROM %[1]s%[2]s GROUP BY src_image, event_type, target_norm), ",
		procFreq, freqWhere))
	b.WriteString(fmt.Sprintf("ft AS (SELECT src_image, event_type, sum(event_count) AS tot FROM %[1]s%[2]s GROUP BY src_image, event_type) ",
		procFreq, freqWhere))
	b.WriteString("SELECT parent, child, label, event_type, anomaly_score, log_id, event_time FROM (")
	b.WriteString("SELECT e.src_node AS parent, e.dst_node AS child, e.label AS label, e.event_type AS event_type, e.log_id AS log_id, e.event_time AS event_time, ")
	b.WriteString("if(coalesce(ft.tot, 0) = 0, 1.0, round(1 - coalesce(fe.cnt, 0) / ft.tot, 4)) AS anomaly_score ")
	b.WriteString(fmt.Sprintf("FROM (%s) AS e ", edges))
	b.WriteString("LEFT JOIN fe ON fe.src_image = e.fkey_src AND fe.event_type = e.event_type AND fe.target_norm = e.fkey_tgt ")
	b.WriteString("LEFT JOIN ft ON ft.src_image = e.fkey_src AND ft.event_type = e.event_type) AS scored ")
	b.WriteString(fmt.Sprintf("WHERE event_type = 'spawn' OR anomaly_score >= %s ", strconv.FormatFloat(threshold, 'f', -1, 64)))
	b.WriteString("ORDER BY anomaly_score DESC")
	if opts.MaxRows > 0 {
		b.WriteString(fmt.Sprintf(" LIMIT %d", opts.MaxRows))
	}

	// Not run through validateGeneratedSQL: this statement is fully machine-composed and
	// legitimately multi-source UNION ALLs the five edge types (which the shared validator
	// only whitelists for WITH RECURSIVE queries). The only external input is the guid set,
	// escaped via escapeString above, so it cannot break out of its string literals.
	return b.String(), nil
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

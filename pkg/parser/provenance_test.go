package parser

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestAbstractExprMatchesMVs guards write/read parity: the abstraction inlined into the
// static proc_freq MVs must be byte-identical to what abstractExpr() emits for the pgr()
// read side, or the freq-join keys will silently mismatch. If this fails, one side was
// edited without the other.
//
// init-clickhouse.sql is the canonical full set (fresh installs) and must carry every use.
// The numbered migrations carry only their delta: 009 adds the file (target_file) fix plus
// the remote_thread/process_access/dns MVs, so it must carry those columns. 008 is released
// and immutable (its file MV keys off the now-superseded fields.artifact), so it is not
// re-checked here beyond its own frozen contents.
func TestAbstractExprMatchesMVs(t *testing.T) {
	type use struct{ col, kind string }
	all := []use{
		{"fields.parent_image::String", AbstractPath}, // spawn src
		{"fields.image::String", AbstractPath},        // spawn target / file+net+dns src / p2p actor src
		{"fields.target_file::String", AbstractPath},  // file target
		{"fields.dst_ip::String", AbstractIP},         // net target
		{"fields.target_image::String", AbstractPath}, // remote_thread/process_access target
		{"fields.query::String", AbstractDomain},      // dns target
		{"image", AbstractPath},                       // proc_lineage.child_image (sibling collapse)
		{"commandline", AbstractPath},                 // proc_lineage.child_cmd_key
	}
	// migration 019 adds the proc_lineage collapse keys pgr's forward hop groups by.
	delta019 := []use{
		{"image", AbstractPath},
		{"commandline", AbstractPath},
	}
	// migration 009 introduces/repairs these MVs (image src, file target, p2p target, dns query).
	delta009 := []use{
		{"fields.image::String", AbstractPath},
		{"fields.target_file::String", AbstractPath},
		{"fields.target_image::String", AbstractPath},
		{"fields.query::String", AbstractDomain},
	}
	// migration 011 (process_edges_mv) abstracts the file/net/dns leaf edges the same way, so pgr's
	// edge-table fkey_tgt matches proc_freq.target_norm for the scoring join. No p2p/spawn here.
	delta011 := []use{
		{"fields.image::String", AbstractPath},       // edge src (fkey_src)
		{"fields.target_file::String", AbstractPath}, // file target
		{"fields.dst_ip::String", AbstractIP},        // net target
		{"fields.query::String", AbstractDomain},     // dns target
	}
	checks := []struct {
		file string
		uses []use
	}{
		{"../../db/init-clickhouse.sql", all},
		{"../../db/migrations/clickhouse/009_proc_freq_events.sql", delta009},
		{"../../db/migrations/clickhouse/011_process_edges.sql", delta011},
		{"../../db/migrations/clickhouse/019_provenance_access_paths.sql", delta019},
	}
	// Compare whitespace-insensitively: ClickHouse ignores SQL whitespace, so the abstracted VALUE
	// (what must match proc_freq) is identical regardless of spacing between args. The abstraction
	// regex literals contain no internal spaces, so stripping spaces is safe and guards the logic
	// (function nesting / patterns) without being brittle to formatting across the SQL files.
	noSpace := func(s string) string { return strings.ReplaceAll(s, " ", "") }
	for _, c := range checks {
		b, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("read %s: %v", c.file, err)
		}
		sql := noSpace(string(b))
		for _, u := range c.uses {
			if want := abstractExpr(u.col, u.kind); !strings.Contains(sql, noSpace(want)) {
				t.Errorf("%s missing/drifted abstraction for %s (%s):\n  want substring: %s", c.file, u.col, u.kind, want)
			}
		}
	}
}

// TestAbstractExpr pins the exact SQL these helpers emit. The strings below were validated
// directly against ClickHouse 26.6 (see the abstraction review): paths collapse user dirs
// /GUIDs/temp-numbers, internal v4 -> /24, internal v6 -> 'internal', external kept, domains
// lowercased with the FQDN root dot stripped. If a change alters this SQL, re-validate in CH
// AND update the static MV DDL to match (the MV write side and this read side must stay
// byte-identical for the freq join keys to line up).
func TestAbstractExpr(t *testing.T) {
	wantPath := `lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, ` +
		`'(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), ` +
		`'\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), ` +
		`'[0-9]{6,}', '*'))`
	if got := abstractExpr("fields.image::String", AbstractPath); got != wantPath {
		t.Errorf("path abstraction drifted:\n got:  %s\n want: %s", got, wantPath)
	}

	wantIP := `multiIf(` +
		`match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), ` +
		`concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), ` +
		`match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', ` +
		`fields.dst_ip::String)`
	if got := abstractExpr("fields.dst_ip::String", AbstractIP); got != wantIP {
		t.Errorf("ip abstraction drifted:\n got:  %s\n want: %s", got, wantIP)
	}

	wantDomain := `lower(replaceRegexpOne(fields.query::String, '\\.$', ''))`
	if got := abstractExpr("fields.query::String", AbstractDomain); got != wantDomain {
		t.Errorf("domain abstraction drifted:\n got:  %s\n want: %s", got, wantDomain)
	}
}

// pgrCmd parses a real pgr() query and returns its command node, so these tests
// exercise the same typed arguments the translator sees.
func pgrCmd(args ...string) CommandNode {
	pipeline, err := ParseQuery("* | pgr(" + strings.Join(args, ", ") + ")")
	if err != nil {
		panic("pgrCmd: " + err.Error())
	}
	var found CommandNode
	ForEachCommand(pipeline, func(cmd CommandNode) {
		if strings.EqualFold(cmd.Name, "pgr") {
			found = cmd
		}
	})
	return found
}

func TestParseProvenanceReconnect(t *testing.T) {
	if p, ok := ParseProvenanceParams(pgrCmd(`start="W1"`)); !ok || !p.Reconnect {
		t.Fatalf("reconnect should default true, got ok=%v reconnect=%v", ok, p.Reconnect)
	}
	for _, off := range []string{"reconnect=false", "reconnect=0", `reconnect="no"`} {
		if p, _ := ParseProvenanceParams(pgrCmd(`start="W1"`, off)); p.Reconnect {
			t.Errorf("%q should disable reconnect", off)
		}
	}
	if p, _ := ParseProvenanceParams(pgrCmd(`start="W1"`, "reconnect=true")); !p.Reconnect {
		t.Error("reconnect=true should enable reconnect")
	}
}

func TestParseProvenancePeers(t *testing.T) {
	if p, _ := ParseProvenanceParams(pgrCmd(`start="W1"`)); p.MaxPeers != DefaultReconnectPeers {
		t.Errorf("peers should default to %d, got %d", DefaultReconnectPeers, p.MaxPeers)
	}
	if p, _ := ParseProvenanceParams(pgrCmd(`start="W1"`, "peers=200")); p.MaxPeers != 200 {
		t.Errorf("peers=200 should parse, got %d", p.MaxPeers)
	}
	if p, _ := ParseProvenanceParams(pgrCmd(`start="W1"`, "peers=99999")); p.MaxPeers != maxReconnectPeersArg {
		t.Errorf("peers should clamp to %d, got %d", maxReconnectPeersArg, p.MaxPeers)
	}
	// Junk, zero and a negative must fall back to the default, never to
	// "unbounded".
	for _, bad := range []string{"peers=0", "peers=-5", `peers="abc"`, "peers=abc"} {
		if p, _ := ParseProvenanceParams(pgrCmd(`start="W1"`, bad)); p.MaxPeers != DefaultReconnectPeers {
			t.Errorf("%q should keep the default, got %d", bad, p.MaxPeers)
		}
	}
}

func reconOpts() QueryOptions {
	o := QueryOptions{ProcLineageTable: "proc_lineage", ProcFreqTable: "proc_freq", FractalID: "f1"}
	return o
}

// An ancestor whose own process_creation is outside the window has no row of its own, but every
// child's creation event names it. The spawn branch must carry that image out as parent_label, and
// every UNION branch must project the column so the shapes still line up.
func TestScoringSQLCarriesParentLabel(t *testing.T) {
	opts := reconOpts()
	opts.MaxRows = 100
	sql := BuildProvenanceScoringSQL(0.7, map[string]bool{"file_write": true, "remote_thread": true}, false, ProvenanceBaseline{TotalHosts: 10}, opts)
	if !strings.Contains(sql, "parent_image AS parent_label") {
		t.Error("spawn branch must emit the parent image as parent_label")
	}
	if n := strings.Count(sql, "AS parent_label"); n < 3 { // spawn + leaf + p2p branch
		t.Errorf("every edge branch must project parent_label, got %d", n)
	}
	if !strings.Contains(sql, "any(ev.parent_label) AS parent_label") {
		t.Error("edge aggregation must keep parent_label")
	}
	if !strings.Contains(sql, "proc_user, host, parent_label, group_size, group_first, group_last, recon_role FROM (") {
		t.Error("parent_label must reach the final projection")
	}
}

func TestBuildReconnectionSQL(t *testing.T) {
	p := ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{}}
	sql := BuildReconnectionSQL(p, 100, 50, reconOpts())
	for _, want := range []string{
		"'file' AS recon_type", "'net' AS recon_type", "'dns' AS recon_type",
		"'remote_thread' AS recon_type", "'process_access' AS recon_type",
		"src_guid", "object_id",
		"event_type = 'net_connect'",      // rarity gate
		"groupUniqArrayMerge(256)(hosts)", // host-prevalence gate
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("reconnection SQL missing %q", want)
		}
	}

	// reconnect=false yields no SQL.
	if s := BuildReconnectionSQL(ProvenanceParams{Reconnect: false}, 100, 50, reconOpts()); s != "" {
		t.Error("reconnect=false should yield empty SQL")
	}
	// include= narrows the generated branches.
	only := ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{"net_connect": true}}
	s := BuildReconnectionSQL(only, 100, 50, reconOpts())
	if !strings.Contains(s, "'net' AS recon_type") || strings.Contains(s, "'dns' AS recon_type") {
		t.Error("include=net_connect should generate only the net branch")
	}
}

// edgeRow finds the bridge row parent -> child, or nil.
func edgeRow(rows []map[string]interface{}, parent, child string) map[string]interface{} {
	for _, r := range rows {
		if r["parent"] == parent && r["child"] == child {
			return r
		}
	}
	return nil
}

func TestReconnectionEdgeRows(t *testing.T) {
	peers := []ReconnectPeer{
		{ReconType: "file", PeerGUID: "exec", SrcGUID: "writer", Label: "c:\\a.exe", Anomaly: 1.0},
		{ReconType: "net", PeerGUID: "peer", SrcGUID: "tree", ObjectID: "net:1.2.3.4", Label: "1.2.3.4", Anomaly: 0.85},
		{ReconType: "remote_thread", PeerGUID: "victim", Anomaly: 0.95}, // pass-2 owns; must NOT be emitted
	}
	rows := ReconnectionEdgeRows(peers)
	if r := edgeRow(rows, "writer", "exec"); r == nil || r["event_type"] != "reconnect_file" {
		t.Error("file bridge should be writer->executor")
	}
	if edgeRow(rows, "tree", "net:1.2.3.4") == nil || edgeRow(rows, "peer", "net:1.2.3.4") == nil {
		t.Error("net bridge should wire both tree toucher and peer to the object node")
	}
	if len(rows) != 3 {
		t.Errorf("injection/access peer must not be emitted (pass-2 owns it): %v", rows)
	}
}

// A reconnected peer is usually not expanded into a subtree, so its bridge edge is the only place
// its identity can come from: the peer's own connection/lookup event names the image, and that
// must ride out as parent_label or the node renders as a bare guid.
func TestReconnectionEdgeRowsCarryPeerImage(t *testing.T) {
	rows := ReconnectionEdgeRows([]ReconnectPeer{
		{ReconType: "net", PeerGUID: "peer", SrcGUID: "tree", ObjectID: "net:1.2.3.4", Label: "1.2.3.4", Anomaly: 0.85, PeerImage: "c:\\windows\\evil.exe"},
	})
	if r := edgeRow(rows, "peer", "net:1.2.3.4"); r == nil || r["parent_label"] != "c:\\windows\\evil.exe" {
		t.Errorf("peer edge must carry the peer image as parent_label, got: %v", r)
	}
	// The tree-side edge's parent is an in-tree process with its own row; it must not be relabeled.
	if r := edgeRow(rows, "tree", "net:1.2.3.4"); r == nil || r["parent_label"] != "" {
		t.Errorf("tree-side edge should leave parent_label empty, got: %v", r)
	}
}

func TestReconnectionEdgeRowsDedup(t *testing.T) {
	// Two net peers sharing one object node must emit the tree-side edge only once.
	rows := ReconnectionEdgeRows([]ReconnectPeer{
		{ReconType: "net", PeerGUID: "p1", SrcGUID: "tree", ObjectID: "net:9.9.9.9", Label: "9.9.9.9", Anomaly: 0.85},
		{ReconType: "net", PeerGUID: "p2", SrcGUID: "tree", ObjectID: "net:9.9.9.9", Label: "9.9.9.9", Anomaly: 0.85},
	})
	if len(rows) != 3 || edgeRow(rows, "p1", "net:9.9.9.9") == nil || edgeRow(rows, "p2", "net:9.9.9.9") == nil {
		t.Errorf("want one tree-side edge and one per peer, got %v", rows)
	}
}

// TestInternalDomainExpr asserts the reconnection dns internal-name filter carries both structural
// signal branches (single-label / NetBIOS, and AD service-discovery records) over the abstracted
// domain column. The resolves-only-internal branch is intentionally gone (not carried by the
// process_edges rollup; the proc_freq rarity gate covers it) -- see internalDomainExpr.
func TestInternalDomainExpr(t *testing.T) {
	e := internalDomainExpr("fkey_tgt")
	for _, sub := range []string{"position(fkey_tgt, '.') = 0", "startsWith(fkey_tgt, '_')", "._msdcs."} {
		if !strings.Contains(e, sub) {
			t.Errorf("internalDomainExpr missing expected fragment %q\n  got: %s", sub, e)
		}
	}
}

// cteBody returns the body of the named top-level CTE in a scoring statement, matching
// parentheses outside string literals.
func cteBody(t *testing.T, sql, name string) string {
	t.Helper()
	marker := name + " AS ("
	start := strings.Index(sql, "WITH "+marker)
	if start < 0 {
		start = strings.Index(sql, ", "+marker)
	}
	if start < 0 {
		t.Fatalf("no CTE %q in scoring SQL", name)
	}
	i := strings.Index(sql[start:], marker) + start + len(marker)
	depth, inStr := 1, false
	for j := i; j < len(sql); j++ {
		switch c := sql[j]; {
		case inStr && c == '\\':
			j++
		case c == '\'':
			inStr = !inStr
		case inStr:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return sql[i:j]
			}
		}
	}
	t.Fatalf("unterminated CTE %q", name)
	return ""
}

func scoringSQL(t *testing.T, edgeTypes map[string]bool, bl ProvenanceBaseline) string {
	t.Helper()
	opts := reconOpts()
	opts.MaxRows = 100
	sql := BuildProvenanceScoringSQL(0.7, edgeTypes, false, bl, opts)
	return sql
}

// Command lines and users come from the nodes' own lineage rows, a primary-key lookup. Only a
// node whose row predates the user column ('\0') reads logs, by the (timestamp, log_id) keys its
// lineage row holds: a guid filter leaves only the bloom index, which a large tree defeats.
// Every read carries the fractal scope, so a prism never pairs one fractal's keys with another's logs.
func TestScoringSQLReadsCommandLinesFromLineage(t *testing.T) {
	prism := reconOpts()
	prism.FractalID, prism.FractalIDs, prism.IncludeEmptyFractalID = "", []string{"f1", "f2"}, true
	for _, c := range []struct {
		name  string
		opts  QueryOptions
		scope string
	}{
		{"fractal", reconOpts(), "fractal_id = 'f1'"},
		{"prism", prism, "fractal_id IN ('f1', 'f2', '')"},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.opts.MaxRows = 100
			sql := BuildProvenanceScoringSQL(0.7, nil, false, ProvenanceBaseline{TotalHosts: 10}, c.opts)
			pl := cteBody(t, sql, "pl")
			for _, want := range []string{
				"any(substring(commandline, 1, 300)) AS command_line, max(proc_user) AS lineage_user FROM proc_lineage WHERE ",
				"' AND " + c.scope + " AND process_guid GLOBAL IN (SELECT guid FROM _pgr_tree WHERE guid = node) GROUP BY guid",
			} {
				if !strings.Contains(pl, want) {
					t.Errorf("want %q in pl:\n%s", want, pl)
				}
			}
			pu := cteBody(t, sql, "pu")
			for _, want := range []string{
				"(timestamp, log_id) GLOBAL IN (SELECT timestamp, log_id FROM proc_lineage WHERE process_guid GLOBAL IN (SELECT guid FROM _pgr_tree WHERE guid = node) AND " + c.scope + " AND proc_user = '\\0')",
				"' AND " + c.scope + " AND (timestamp, log_id)",
			} {
				if !strings.Contains(pu, want) {
					t.Errorf("want %q in pu:\n%s", want, pu)
				}
			}
			if strings.Contains(pu, "fields.process_guid::String IN") || strings.Contains(pu, "commandline") {
				t.Errorf("pu reads logs for the user only, by log key:\n%s", pu)
			}
			// '\0' is a row from before the column; an empty user is a known empty one.
			if pm := cteBody(t, sql, "pm"); !strings.Contains(pm, "if(pl.lineage_user = '\\0', pu.proc_user, pl.lineage_user) AS proc_user FROM pl LEFT JOIN pu ON pu.guid = pl.guid") {
				t.Errorf("pm must prefer the lineage user:\n%s", pm)
			}
			// The tree's own spawn targets read the stored abstraction.
			if sx := cteBody(t, sql, "sx"); !strings.HasPrefix(sx, "SELECT child_image AS node,") || !strings.Contains(sql, "child_image AS fkey_tgt") {
				t.Errorf("sx and spawn edges must use child_image:\n%s", sx)
			}
		})
	}
}

// A ghost root (proc_lineage row with no parent_image) yields an empty fkey_src, which can never
// match a proc_freq row because every proc_freq_* MV requires a parent_image. Without the guard
// it scores max-anomalous, painting the root of most trees red.
func TestScoringSQLGhostRootScoresZero(t *testing.T) {
	sql := scoringSQL(t, nil, ProvenanceBaseline{TotalHosts: 10})
	for _, want := range []string{"multiIf(fkey_src = '', 'no_source'", "multiIf(score_basis = 'no_source', 0,"} {
		if !strings.Contains(sql, want) {
			t.Errorf("empty fkey_src must score 0 before any baseline term: missing %q", want)
		}
	}
}

// Per-edge anomaly is NoDoze Eq.2/Eq.3: 1 - IN(SRC)*M*OUT(DST). Global rarity is never a factor
// of the transition score -- combining it measured worse than either input alone.
func TestScoringSQLUsesNodeStabilityFactors(t *testing.T) {
	sql := scoringSQL(t, nil, ProvenanceBaseline{TotalHosts: 10})
	for _, want := range []string{
		"(1 - ins.instab) * (1 - outs.instab) AS stability",
		"1 - stability * if(source_host_days > 0, least(1, edge_host_days / source_host_days), 0)",
		"LEFT JOIN ins ON ins.node = e.fkey_src",   // IN keys on the source node
		"LEFT JOIN outs ON outs.node = e.fkey_tgt", // OUT keys on the destination node
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("scoring SQL missing %q", want)
		}
	}
	// An unmatched LEFT JOIN yields the column default (0), not NULL, so coalescing a stability
	// column to 1 never fires and a node absent from the CTE silently scores maximally unstable.
	if strings.Contains(sql, "coalesce(ins.") || strings.Contains(sql, "coalesce(outs.") {
		t.Error("stability must not rely on coalesce over a non-Nullable LEFT JOIN column")
	}
	transition := sql[strings.Index(sql, "score_basis = 'transition', "):strings.Index(sql, "AS anomaly_score")]
	if strings.Contains(transition, "target_hosts") || strings.Contains(transition, "gf_hosts") {
		t.Error("the transition score must not fold in the target's global footprint")
	}
}

// Frequencies are host-days (NoDoze Eq.7/8): every proc_freq count merges the per-row host sets
// into distinct (day, host) pairs. Raw event_count would let one beaconing process saturate its
// own baseline.
func TestScoringSQLCountsHostDays(t *testing.T) {
	sql := scoringSQL(t, nil, ProvenanceBaseline{TotalHosts: 10})
	pairs := "uniqCombined64Array(12)(arrayMap(h -> (day, h), finalizeAggregation(hosts)))"
	for _, cte := range []string{"fe", "ft", "gf"} {
		body := cteBody(t, sql, cte)
		if !strings.Contains(body, pairs) {
			t.Errorf("%s must count host-days, got: %s", cte, body)
		}
		if strings.Contains(body, "event_count") {
			t.Errorf("%s must not count raw events", cte)
		}
	}
	// Eq.8's Freq(src,rel,*) counts the wildcard event once per host-day, so ft groups by the
	// source and relationship only, never per target.
	if ft := cteBody(t, sql, "ft"); !strings.HasSuffix(ft, "GROUP BY src_image, event_type") {
		t.Errorf("ft must aggregate per (source, relationship): %s", ft)
	}
	// Whether the source binary ran elsewhere merges per-parent states, so a host-day on which two
	// parents launched it counts once.
	ins := cteBody(t, sql, "ins")
	for _, want := range []string{"uniqCombined64ArrayIfState(12)(", "event_type = 'spawn') AS xs", "uniqCombined64ArrayIfMerge(12)(xs) AS exec_hd"} {
		if !strings.Contains(ins, want) {
			t.Errorf("ins missing %q", want)
		}
	}
}

// The baseline includes the activity being scored, so every count is taken leave-one-out: the
// tree's own distinct host-days per key are subtracted, clamped at 0. Without it a never-seen
// process's only connection is its source's entire history (M = 1) and scores 0.
func TestScoringSQLLeavesOneOut(t *testing.T) {
	sql := scoringSQL(t, map[string]bool{"file_write": true, "remote_thread": true}, ProvenanceBaseline{TotalHosts: 10})
	if n := strings.Count(sql, "toDate(ingest_timestamp) AS iday"); n < 3 { // spawn + leaf + p2p
		t.Errorf("every edge branch must carry its ingest day, got %d", n)
	}
	for _, want := range []string{
		"groupUniqArray((ev.host, ev.iday)) AS hd_pairs",
		"length(groupUniqArrayArray(e.hd_pairs) OVER (PARTITION BY e.fkey_src, e.event_type, e.fkey_tgt)) AS self_edge",
		"length(groupUniqArrayArray(e.hd_pairs) OVER (PARTITION BY e.fkey_src, e.event_type)) AS self_src",
		"length(groupUniqArrayArray(e.hd_pairs) OVER (PARTITION BY e.event_type, e.fkey_tgt)) AS self_tgt",
		"greatest(0, toFloat64(fe_hd) - self_edge) AS edge_host_days",
		"greatest(0, toFloat64(ft_hd) - self_src) AS source_host_days",
		"greatest(0, toFloat64(gf_hd) - self_tgt) AS target_host_days",
		"greatest(0, toFloat64(exec_hd) - self_exec) AS source_exec_host_days",
		"LEFT JOIN sx ON sx.node = e.fkey_src",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("scoring SQL missing %q", want)
		}
	}
	// sx must count executions exactly as the spawn MV does, or self_exec drifts from exec_hd.
	sx := cteBody(t, sql, "sx")
	for _, want := range []string{"FROM proc_lineage FINAL", "parent_image != '' AND image != ''", "process_guid GLOBAL IN (SELECT guid FROM _pgr_tree)", "toDate(ingest_timestamp)"} {
		if !strings.Contains(sx, want) {
			t.Errorf("sx missing %q: %s", want, sx)
		}
	}
}

// A binary that never ran anywhere else has no transition probability. Calling it 0 would paint
// every child of a new binary red, so its edges are scored by the target's own rarity instead,
// while an established binary doing something new keeps the never-seen score of 1.
func TestScoringSQLNewSourceScoresTargetRarity(t *testing.T) {
	sql := scoringSQL(t, nil, ProvenanceBaseline{TotalHosts: 10})
	for _, want := range []string{
		"source_host_days > 0 OR source_exec_host_days > 0, 'transition', 'new_source') AS score_basis",
		"1 / (1 + target_host_days)), 4) AS anomaly_score",
		"anomaly_score AS edge_score",
		"toFloat64(10) AS total_hosts",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("scoring SQL missing %q", want)
		}
	}
}

// ClickHouse inlines CTEs, so each proc_freq-reading CTE in the scoring SQL is its own table
// scan. Every one must therefore be scoped to the keys its join can match, or pgr() aggregates
// the fractal's whole proc_freq once per CTE -- work that grows with the retention window rather
// than with the tree, and what exhausts the query memory cap on an aged fractal.
func TestScoringSQLScopesBaselineToEdgeKeys(t *testing.T) {
	bl := ProvenanceBaseline{TotalHosts: 10, SrcKeys: []string{"c:\\a.exe"}, TgtKeys: []string{"1.2.3.4", "d.example"}}
	sql := scoringSQL(t, nil, bl)
	// Which COLUMN each key set applies to depends on the CTE, and crossing them changes scores
	// without erroring: fe/ft join src_image to the edge source (fe also its target), ins is
	// grouped by the target_norm that IS that source node, gf joins target_norm to the edge
	// target, and outs is grouped by the src_image that IS that target node.
	src := "src_image IN ('c:\\\\a.exe')"
	tgt := "target_norm IN ('1.2.3.4', 'd.example')"
	srcAsTgt := "target_norm IN ('c:\\\\a.exe')"
	tgtAsSrc := "src_image IN ('1.2.3.4', 'd.example')"
	for _, c := range []struct {
		cte       string
		want, not []string
	}{
		{"fe", []string{src, tgt}, []string{srcAsTgt, tgtAsSrc}},
		{"ft", []string{src}, []string{tgt, srcAsTgt, tgtAsSrc}},
		{"gf", []string{tgt}, []string{src, srcAsTgt, tgtAsSrc}},
		{"ins", []string{srcAsTgt}, []string{src, tgt, tgtAsSrc}},
		{"outs", []string{tgtAsSrc}, []string{src, tgt, srcAsTgt}},
	} {
		body := cteBody(t, sql, c.cte)
		for _, w := range c.want {
			if !strings.Contains(body, w) {
				t.Errorf("%s must be scoped by %q", c.cte, w)
			}
		}
		for _, n := range c.not {
			if strings.Contains(body, n) {
				t.Errorf("%s must not be scoped by %q", c.cte, n)
			}
		}
	}
	// span measures the age of the whole baseline, so it alone stays unrestricted.
	if span := cteBody(t, sql, "span"); strings.Contains(span, "IN (") {
		t.Errorf("span must not be key-restricted: %s", span)
	}
}

// An empty key set means the lookup failed or overflowed. Scoring must then fall back to the
// unrestricted baseline: a partial key set would turn real baseline rows into missed joins and
// silently change scores.
func TestScoringSQLUnrestrictedWithoutEdgeKeys(t *testing.T) {
	sql := scoringSQL(t, nil, ProvenanceBaseline{TotalHosts: 10})
	if strings.Contains(sql, "src_image IN (") || strings.Contains(sql, "target_norm IN (") {
		t.Error("no edge keys must leave every proc_freq CTE unrestricted")
	}
}

// Bridge rows must carry every ProvenanceColumn, typed like the scoring SQL's, so they render
// and re-emit exactly like scored edges.
func TestReconnectionEdgeRowsCarryProvenanceColumns(t *testing.T) {
	rows := ReconnectionEdgeRows([]ReconnectPeer{
		{ReconType: "file", PeerGUID: "exec", SrcGUID: "writer", Label: "c:\\a.exe", Anomaly: 1.0},
	})
	numeric := map[string]bool{}
	for _, c := range ProvenanceNumericColumns {
		numeric[c] = true
	}
	r := rows[0]
	for _, c := range ProvenanceColumns {
		v, ok := r[c]
		if !ok {
			t.Fatalf("bridge row is missing %q", c)
		}
		if _, isF := v.(float64); isF != numeric[c] {
			t.Errorf("%s has type %T", c, v)
		}
	}
	if r["score_basis"] != "reconnect" || r["edge_score"] != 1.0 || r["group_size"] != 1.0 {
		t.Errorf("a bridge must say it is a bridge and carry its own score: %v", r)
	}
}

// The hop groups by the collapse keys proc_lineage stores, so it runs no regex per child,
// keyed on the parent's node so a group's children are grouped across all of its members, and
// reads the frontier from external data rather than a literal list.
func TestProcessGroupHopSQL(t *testing.T) {
	sql := BuildProcessGroupHopSQL(reconOpts())
	for _, want := range []string{
		"f.node AS parent_node",
		"GROUP BY parent_node, c.child_image, c.child_cmd_key",
		"uniqExact(c.process_guid) AS n",
		"FROM proc_lineage AS c GLOBAL INNER JOIN _pgr_frontier AS f ON c.parent_guid = f.guid WHERE ",
		"c.fractal_id = 'f1' AND c.parent_guid GLOBAL IN (SELECT guid FROM _pgr_frontier)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("hop SQL missing %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "replaceRegexpAll") {
		t.Errorf("hop SQL abstracts per row:\n%s", sql)
	}
	// Only the by_parent projection's columns, or ClickHouse reads the table instead.
	byParent := map[string]bool{"fractal_id": true, "parent_guid": true, "process_guid": true, "timestamp": true, "child_image": true, "child_cmd_key": true}
	for _, m := range regexp.MustCompile(`\bc\.(\w+)`).FindAllStringSubmatch(sql, -1) {
		if !byParent[m[1]] {
			t.Errorf("hop reads c.%s, which by_parent does not hold", m[1])
		}
	}
}

// Members of a collapsed group are merged onto their node before aggregation, so their spawn
// edges become one and their leaves union; nothing in pass 2 reads a literal guid list.
func TestScoringSQLMapsMembersOntoNodes(t *testing.T) {
	sql := scoringSQL(t, map[string]bool{"file_write": true}, ProvenanceBaseline{TotalHosts: 10})
	for _, want := range []string{
		"LEFT JOIN _pgr_tree AS ms ON ms.guid = ev.src_node LEFT JOIN _pgr_tree AS md ON md.guid = ev.dst_node",
		"if(ms.node = '', ev.src_node, ms.node) AS src_node",
		"process_guid GLOBAL IN (SELECT guid FROM _pgr_tree) AND event_type IN ('file_write')",
		// A collapsed node's spawn edge carries the node's own creation log (whichever member is
		// the node, however timestamps tie), a leaf its latest event.
		"ev.dst_node AS raw_dst",
		"if(event_type = 'spawn', argMin(ev.log_id, (ev.raw_dst != dst_node, ev.timestamp)), argMax(ev.log_id, ev.timestamp)) AS log_id",
		"if(event_type = 'spawn', argMin(ev.timestamp, (ev.raw_dst != dst_node, ev.timestamp)), max(ev.timestamp)) AS timestamp",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("scoring SQL missing %q", want)
		}
	}
}

// Command lines and users are looked up only inside the span the nodes were created in.
func TestScoringSQLNarrowsCommandLineWindow(t *testing.T) {
	opts := reconOpts()
	opts.StartTime = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	opts.EndTime = time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	bl := ProvenanceBaseline{NodeStart: time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC), NodeEnd: time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)}
	sql := BuildProvenanceScoringSQL(0.7, nil, false, bl, opts)
	for _, cte := range []string{"pl", "pu"} {
		if !strings.Contains(cteBody(t, sql, cte), "timestamp >= '2026-10-27 08:00:00.000' AND timestamp <= '2026-10-27 09:00:00.000'") {
			t.Errorf("%s not narrowed to the node span:\n%s", cte, cteBody(t, sql, cte))
		}
	}
	sql = BuildProvenanceScoringSQL(0.7, nil, false, ProvenanceBaseline{}, opts)
	for _, cte := range []string{"pl", "pu"} {
		if !strings.Contains(cteBody(t, sql, cte), "timestamp >= '2026-10-01 00:00:00.000' AND timestamp <= '2026-10-31 00:00:00.000'") {
			t.Errorf("without a span %s reads the query window:\n%s", cte, cteBody(t, sql, cte))
		}
	}
}

// The baseline's age comes from the summary when the caller has it, instead of a pass over
// the fractal's whole baseline.
func TestScoringSQLBaselineSpan(t *testing.T) {
	opts := reconOpts()
	opts.EndTime = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	span := cteBody(t, BuildProvenanceScoringSQL(0.7, nil, false, ProvenanceBaseline{FirstDay: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)}, opts), "span")
	if want := "SELECT dateDiff('day', toDate('2026-09-02'), toDate('2026-10-31')) + 1 AS days"; span != want {
		t.Errorf("span = %q, want %q", span, want)
	}
	if span = cteBody(t, BuildProvenanceScoringSQL(0.7, nil, false, ProvenanceBaseline{}, opts), "span"); !strings.Contains(span, "FROM proc_freq WHERE fractal_id = 'f1'") {
		t.Errorf("without a first day span reads the baseline:\n%s", span)
	}
}

// One read of the summary yields every fleet-wide input. Shards each hold their own copy of a
// row, so counts are distinct values. The baseline's host ceiling keeps proc_freq's 256 cap.
func TestProvenanceSummarySQL(t *testing.T) {
	opts := reconOpts()
	opts.ProvenanceSummaryTable = "provenance_summary_distributed"
	want := "SELECT uniqExactIf(value, kind = 'lineage_host') AS total_hosts, " +
		"least(256, uniqExactIf(value, kind = 'freq_host')) AS recon_hosts, uniqExactIf(value, kind = 'freq_image') AS recon_images, " +
		"toString(minIf(toDateOrZero(value), kind = 'freq_day')) AS first_day FROM provenance_summary_distributed WHERE fractal_id = 'f1'"
	if got := BuildProvenanceSummarySQL(opts); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// The rarity gate is evaluated only for the tree's own artifacts of that type, never by
// grouping every target in the fractal's baseline; the result for the tree is the same.
func TestReconnectionRarityGateScopedToTree(t *testing.T) {
	sql := BuildReconnectionSQL(ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{}}, 100, 50, reconOpts())
	for _, et := range []string{"net_connect", "dns_query"} {
		want := "FROM proc_freq WHERE event_type = '" + et + "' AND fractal_id = 'f1' AND target_norm GLOBAL IN (SELECT fkey_tgt FROM process_edges WHERE fractal_id = 'f1' AND event_type = '" + et + "' AND process_guid GLOBAL IN (SELECT guid FROM _pgr_tree)) GROUP BY target_norm"
		if !strings.Contains(sql, want) {
			t.Errorf("%s gate not scoped to the tree:\n%s", et, sql)
		}
	}
}

// Each candidate list is read twice (as a filter and as a join), so a capped list must be the
// same list both times: ordered before its LIMIT.
func TestReconnectionCandidateCapsAreDeterministic(t *testing.T) {
	sql := BuildReconnectionSQL(ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{}}, 100, 50, reconOpts())
	for _, k := range []string{"p", "ip", "q"} {
		if want := fmt.Sprintf("GROUP BY %s ORDER BY %s LIMIT %d", k, k, maxReconnectCandidateArtifacts); !strings.Contains(sql, want) {
			t.Errorf("missing %q", want)
		}
	}
}

// Executors of a dropped file are found among the processes running a written path, scoped to
// the query's fractals and outside the tree, latest row per process, without FINAL over the
// whole lineage table.
func TestReconnectionFileExecutorsWithoutFinal(t *testing.T) {
	prism := reconOpts()
	prism.FractalID, prism.FractalIDs = "", []string{"f1", "f2"}
	sql := BuildReconnectionSQL(ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{"file_write": true}}, 100, 50, prism)
	if strings.Contains(sql, "FINAL") {
		t.Errorf("file branch must not FINAL the lineage table:\n%s", sql)
	}
	for _, want := range []string{
		"FROM proc_lineage WHERE fractal_id IN ('f1', 'f2') AND image_lc GLOBAL IN (SELECT p FROM (",
		"AND process_guid GLOBAL NOT IN (SELECT guid FROM _pgr_tree) ORDER BY process_guid, timestamp DESC, log_id DESC LIMIT 1 BY process_guid",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
}

// Each branch's peer limit keeps the rows of the least-shared artifacts first, so a C2 one
// other process touched is never cut for a telemetry domain dozens share.
func TestReconnectionPeerLimitPrefersRareArtifacts(t *testing.T) {
	sql := BuildReconnectionSQL(ProvenanceParams{Reconnect: true}, 100, 50, reconOpts())
	for want, n := range map[string]int{
		"GROUP BY peer_guid, object_id ORDER BY count() OVER (PARTITION BY object_id), peer_guid LIMIT": 2, // net, dns
		"ORDER BY count() OVER (PARTITION BY wp.p), pl.process_guid LIMIT":                              1, // file
	} {
		if got := strings.Count(sql, want); got != n {
			t.Errorf("%q appears %d times, want %d", want, got, n)
		}
	}
}

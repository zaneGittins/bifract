package parser

import (
	"os"
	"strings"
	"testing"
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
		{"fields.target_file::String", AbstractPath},  // file target
		{"fields.dst_ip::String", AbstractIP},         // net target
		{"fields.query::String", AbstractDomain},      // dns target
	}
	checks := []struct {
		file string
		uses []use
	}{
		{"../../db/init-clickhouse.sql", all},
		{"../../db/migrations/clickhouse/009_proc_freq_events.sql", delta009},
		{"../../db/migrations/clickhouse/011_process_edges.sql", delta011},
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

// pgrCmd builds a CommandNode for pgr() with the given args (helper for parse tests).
func pgrCmd(args ...string) CommandNode { return CommandNode{Name: "pgr", Arguments: args} }

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
	// Junk and zero must fall back to the default, never to "unbounded".
	for _, bad := range []string{"peers=0", "peers=-5", "peers=abc"} {
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
	sql, err := BuildProvenanceScoringSQL([]string{"g1"}, 0.7, map[string]bool{"file_write": true, "remote_thread": true}, false, 10, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "parent_image AS parent_label") {
		t.Error("spawn branch must emit the parent image as parent_label")
	}
	if n := strings.Count(sql, "AS parent_label"); n < 3 { // spawn + leaf + p2p branch
		t.Errorf("every edge branch must project parent_label, got %d", n)
	}
	if !strings.Contains(sql, "any(ev.parent_label) AS parent_label") {
		t.Error("edge aggregation must keep parent_label")
	}
	if !strings.Contains(sql, "proc_user, host, parent_label FROM (") {
		t.Error("parent_label must reach the final projection")
	}
}

func TestBuildReconnectionSQL(t *testing.T) {
	p := ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{}}
	sql, err := BuildReconnectionSQL([]string{"g1", "g2"}, p, 100, 50, reconOpts())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"'file' AS recon_type", "'net' AS recon_type", "'dns' AS recon_type",
		"'remote_thread' AS recon_type", "'process_access' AS recon_type",
		"src_guid", "object_id",
		"event_type = 'net_connect'",       // rarity gate
		"groupUniqArrayMerge(256)(hosts)",  // host-prevalence gate
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("reconnection SQL missing %q", want)
		}
	}

	// reconnect=false yields no SQL.
	if s, _ := BuildReconnectionSQL([]string{"g1"}, ProvenanceParams{Reconnect: false}, 100, 50, reconOpts()); s != "" {
		t.Error("reconnect=false should yield empty SQL")
	}
	// include= narrows the generated branches.
	only := ProvenanceParams{Reconnect: true, EdgeTypes: map[string]bool{"net_connect": true}}
	s, _ := BuildReconnectionSQL([]string{"g1"}, only, 100, 50, reconOpts())
	if !strings.Contains(s, "'net' AS recon_type") || strings.Contains(s, "'dns' AS recon_type") {
		t.Error("include=net_connect should generate only the net branch")
	}
}

func TestAppendReconnectionEdges(t *testing.T) {
	peers := []ReconnectPeer{
		{ReconType: "file", PeerGUID: "exec", SrcGUID: "writer", Label: "c:\\a.exe", Anomaly: 1.0},
		{ReconType: "net", PeerGUID: "peer", SrcGUID: "tree", ObjectID: "net:1.2.3.4", Label: "1.2.3.4", Anomaly: 0.85},
		{ReconType: "remote_thread", PeerGUID: "victim", Anomaly: 0.95}, // pass-2 owns; must NOT be emitted
	}
	out := AppendReconnectionEdges("SELECT 1", peers)
	// file: writer -> executor
	if !strings.Contains(out, "'writer' AS parent, 'exec' AS child") || !strings.Contains(out, "'reconnect_file'") {
		t.Error("file bridge should be writer->executor")
	}
	// net: both endpoints converge on the object node
	if !strings.Contains(out, "'tree' AS parent, 'net:1.2.3.4' AS child") ||
		!strings.Contains(out, "'peer' AS parent, 'net:1.2.3.4' AS child") {
		t.Error("net bridge should wire both tree toucher and peer to the object node")
	}
	// injection target is not emitted as a literal edge
	if strings.Contains(out, "'victim'") {
		t.Error("injection/access peer must not be emitted (pass-2 owns it)")
	}
}

// A reconnected peer is usually not expanded into a subtree, so its bridge edge is the only place
// its identity can come from: the peer's own connection/lookup event names the image, and that
// must ride out as parent_label or the node renders as a bare guid.
func TestAppendReconnectionEdgesCarriesPeerImage(t *testing.T) {
	peers := []ReconnectPeer{
		{ReconType: "net", PeerGUID: "peer", SrcGUID: "tree", ObjectID: "net:1.2.3.4", Label: "1.2.3.4", Anomaly: 0.85, PeerImage: "c:\\windows\\evil.exe"},
	}
	out := AppendReconnectionEdges("SELECT 1", peers)
	if !strings.Contains(out, "'c:\\\\windows\\\\evil.exe' AS parent_label") {
		t.Errorf("peer edge must carry the peer image as parent_label, got: %s", out)
	}
	// The tree-side edge's parent is an in-tree process with its own row; it must not be relabeled.
	treeEdge := out[strings.Index(out, "'tree' AS parent"):]
	if end := strings.Index(treeEdge, " UNION ALL "); end > 0 {
		treeEdge = treeEdge[:end]
	}
	if !strings.Contains(treeEdge, "'' AS parent_label") {
		t.Errorf("tree-side edge should leave parent_label empty, got: %s", treeEdge)
	}
}

func TestAppendReconnectionEdgesDedup(t *testing.T) {
	// Two net peers sharing one object node must emit the tree-side edge only once.
	peers := []ReconnectPeer{
		{ReconType: "net", PeerGUID: "p1", SrcGUID: "tree", ObjectID: "net:9.9.9.9", Label: "9.9.9.9", Anomaly: 0.85},
		{ReconType: "net", PeerGUID: "p2", SrcGUID: "tree", ObjectID: "net:9.9.9.9", Label: "9.9.9.9", Anomaly: 0.85},
	}
	out := AppendReconnectionEdges("SELECT 1", peers)
	if n := strings.Count(out, "'tree' AS parent, 'net:9.9.9.9' AS child"); n != 1 {
		t.Errorf("tree-side convergence edge should be deduped to 1, got %d", n)
	}
	if strings.Count(out, "'p1' AS parent, 'net:9.9.9.9' AS child") != 1 ||
		strings.Count(out, "'p2' AS parent, 'net:9.9.9.9' AS child") != 1 {
		t.Error("each peer's convergence edge should appear once")
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

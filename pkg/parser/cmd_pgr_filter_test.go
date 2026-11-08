package parser

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func pgrOpts() QueryOptions {
	return QueryOptions{
		FractalID: "f",
		StartTime: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
	}
}

func translateNoFatal(q string, o QueryOptions) (*TranslationResult, error) {
	pl, err := ParseQuery(q)
	if err != nil {
		return nil, err
	}
	return TranslateToSQLWithOrder(pl, o)
}

const pgrRowRef = "fields.`process_guid`::String"

// The window of the log scan, which also bounds the candidates.
const pgrWindow = "timestamp >= '2026-11-01 00:00:00.000' AND timestamp <= '2026-11-02 00:00:00.000' AND fractal_id = 'f'"

func TestPgrHasAncestorSQL(t *testing.T) {
	cand := "(SELECT " + procKeyExpr(pgrRowRef) + " FROM logs WHERE " + pgrWindow + ")"
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"depth 1 answers closed processes from one ancestry row", `pgrHasAncestor(/a/, depth=1)`, []string{
			pgrRowRef + " IN (SELECT process_guid FROM proc_ancestry WHERE fractal_id = 'f' AND depth BETWEEN 1 AND 1 AND match(anc_image, '(?i)a') AND proc_key IN (SELECT proc_key FROM (" + pgrStatusSQL(cand) + ")",
			" OR " + pgrRowRef + " IN (SELECT process_guid FROM proc_lineage WHERE fractal_id = 'f' AND match(parent_image, '(?i)a') AND proc_key IN (SELECT proc_key FROM proc_lineage WHERE fractal_id = 'f' AND proc_key IN " + cand + " AND proc_key NOT IN (SELECT proc_key FROM (",
		}},
		{"bare name matches the file name", `pgrHasAncestor("RunDLL32.exe")`, []string{
			`(lower(parent_image) = lower('RunDLL32.exe') OR endsWith(lower(parent_image), lower('\\RunDLL32.exe')) OR endsWith(lower(parent_image), lower('/RunDLL32.exe')))`,
		}},
		{"a path is one equality", `pgrHasAncestor("C:\\Windows\\cmd.exe", depth=1)`, []string{`lower(parent_image) = lower('C:\\Windows\\cmd.exe') AND`}},
		{"regex ignores case", `pgrHasAncestor(/Temp.+\.exe$/)`, []string{`match(parent_image, '(?i)Temp.+\\.exe$')`}},
		{"negated", `NOT pgrHasAncestor("cmd.exe")`, []string{pgrRowRef + " NOT IN (SELECT process_guid"}},
		{"operand", `pgrHasAncestor(/a/) OR user=x`, []string{") OR "}},
		{"regex binding stays a regex", `let &v := /Temp.+exe/; pgrHasAncestor(&v)`, []string{"match(parent_image, '(?i)Temp.+exe')"}},
		{"bare word", `pgrHasAncestor(cmd.exe)`, []string{"lower('cmd.exe')"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := mustTranslate(t, tc.query, pgrOpts())
			for _, w := range tc.want {
				if !strings.Contains(sql, w) {
					t.Errorf("missing %q in:\n%s", w, sql)
				}
			}
		})
	}
}

func TestPgrHasNetworkSQL(t *testing.T) {
	cand := "(SELECT " + procKeyExpr(pgrRowRef) + " FROM logs WHERE " + pgrWindow + ")"
	for q, want := range map[string]string{
		`pgrHasNetwork()`:               pgrRowRef + " IN (SELECT process_guid FROM process_edges WHERE fractal_id = 'f' AND event_type = 'net_connect' AND proc_key IN " + cand + ")",
		`pgrHasNetwork("2A00:1450::1")`: "fkey_tgt = '2A00:1450::1'",
		`pgrHasNetwork(/^203/)`:         `match(fkey_tgt, '^203')`,
		`NOT pgrHasNetwork()`:           pgrRowRef + " NOT IN (SELECT process_guid FROM process_edges",
		`pgrHasNetwork(external=true)`:  pgrExternalTarget,
		`pgrHasNetwork("a'b")`:          `fkey_tgt = 'a\'b'`,
	} {
		if sql := mustTranslate(t, q, pgrOpts()); !strings.Contains(sql, want) {
			t.Errorf("%s: missing %q in:\n%s", q, want, sql)
		}
	}
	if sql := mustTranslate(t, `pgrHasNetwork(external=false)`, pgrOpts()); strings.Contains(sql, pgrExternalTarget) {
		t.Errorf("external=false must not narrow:\n%s", sql)
	}
	if sql := mustTranslate(t, `pgrHasNetwork(/^203/, external=true)`, pgrOpts()); !strings.Contains(sql, "match(fkey_tgt, '^203')") || !strings.Contains(sql, pgrExternalTarget) {
		t.Errorf("a value and external= must both apply:\n%s", sql)
	}
}

func TestPgrNetworkInternalValues(t *testing.T) {
	for value, want := range map[string]string{
		"10.1.2.5": "10.1.2.0/24", "172.16.9.1": "172.16.9.0/24", "192.168.1.1": "192.168.1.0/24", "127.0.0.1": "127.0.0.0/24",
		"169.254.9.9": "169.254.9.0/24", "fe80::1": "internal", "fd00::1": "internal", "::1": "internal",
	} {
		_, err := translateNoFatal(`pgrHasNetwork("`+value+`")`, pgrOpts())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v", value, err)
		}
	}
	// Stored as they arrived, so these can match.
	for _, value := range []string{"FE80::1", "FD00::1", "::ffff:10.1.2.3", "203.0.113.5", "10.1.2.0/24", "internal", "172.32.0.1"} {
		if _, err := translateNoFatal(`pgrHasNetwork("`+value+`")`, pgrOpts()); err != nil {
			t.Errorf("%s: %v", value, err)
		}
	}
}

// pgrStatusSQL is the status lookup of the candidates' ancestry.
func pgrStatusSQL(cand string) string {
	return "SELECT proc_key, argMax(open_key, version) AS open_key FROM proc_ancestry WHERE fractal_id = 'f' AND depth = 0 AND proc_key IN " + cand + " GROUP BY proc_key"
}

// Processes the ancestry cannot answer yet walk up proc_lineage, as before the closure existed.
func TestPgrHasAncestorWalksUp(t *testing.T) {
	cand := "(SELECT " + procKeyExpr(pgrRowRef) + " FROM logs WHERE " + pgrWindow + ")"
	status := pgrStatusSQL(cand)
	closed := "SELECT proc_key FROM (" + status + ") WHERE open_key = '' OR open_key NOT IN (SELECT proc_key FROM proc_lineage WHERE fractal_id = 'f' AND proc_key IN (SELECT open_key FROM (" + status + ") WHERE open_key != ''))"
	fallback := "(SELECT proc_key FROM proc_lineage WHERE fractal_id = 'f' AND proc_key IN " + cand + " AND proc_key NOT IN (" + closed + "))"
	parents := "(SELECT parent_key FROM proc_lineage WHERE fractal_id = 'f' AND proc_key IN " + fallback + ")"
	m := "match(parent_image, '(?i)a')"
	want := "SELECT process_guid FROM proc_lineage WHERE fractal_id = 'f' AND (" + m + " OR parent_key IN " +
		"(SELECT proc_key FROM proc_lineage WHERE fractal_id = 'f' AND " + m + " AND proc_key IN " + parents + ")) AND proc_key IN " + fallback
	if sql := mustTranslate(t, `pgrHasAncestor(/a/, depth=2)`, pgrOpts()); !strings.Contains(sql, want) {
		t.Errorf("missing %q in:\n%s", want, sql)
	}
	if one := mustTranslate(t, `pgrHasAncestor("a.exe", depth=1)`, pgrOpts()); strings.Contains(one, "SELECT parent_key FROM proc_lineage") {
		t.Errorf("depth=1 reads only the row's own lineage:\n%s", one)
	}
	if def := mustTranslate(t, `pgrHasAncestor("a.exe")`, pgrOpts()); def != mustTranslate(t, `pgrHasAncestor("a.exe", depth=5)`, pgrOpts()) {
		t.Errorf("the default depth should be 5")
	}
}

// Lookups are by key, so only log scans carry the time range: what happened before the range
// still counts.
func TestPgrFilterHasNoTimeBoundOnProvenance(t *testing.T) {
	for _, q := range []string{`pgrHasAncestor("a.exe")`, `pgrHasNetwork()`} {
		sql := mustTranslate(t, q, pgrOpts())
		if bounds, scans := strings.Count(sql, "timestamp >= "), strings.Count(sql, "FROM logs WHERE"); bounds != scans {
			t.Errorf("%s: %d time bounds for %d log scans:\n%s", q, bounds, scans, sql)
		}
	}
}

// pgrNarrowedBy reports whether every candidate scan in sql is narrowed by exactly cond.
func pgrNarrowedBy(sql, cond string) bool {
	scan := "SELECT " + procKeyExpr(pgrRowRef) + " FROM logs WHERE " + pgrWindow
	n := strings.Count(sql, scan)
	return n > 0 && strings.Count(sql, scan+" AND "+cond+")") == n
}

func TestPgrCandidatesAreNarrowedByOtherFilters(t *testing.T) {
	bob := "fields.`user`::String = 'bob'"
	for _, q := range []string{
		`user=bob | pgrHasAncestor("a.exe", depth=1)`,
		`pgrHasAncestor("a.exe", depth=1) | pgrHasAncestor("b.exe", depth=1) | user=bob`,
		`user=bob | pgrHasDescendant("a.exe")`,
		// An operand that is a conjunct of its own is narrowed as a command is.
		`user=bob pgrHasAncestor("a.exe", depth=1)`,
		`user=bob AND pgrHasAncestor("a.exe", depth=1)`,
		`(user=bob AND pgrHasAncestor("a.exe", depth=1))`,
	} {
		if sql := mustTranslate(t, q, pgrOpts()); !pgrNarrowedBy(sql, bob) {
			t.Errorf("%s: every candidate scan should be narrowed by the plain filter only:\n%s", q, sql)
		}
	}
}

// Narrowing a filter inside an OR would drop rows its other branch keeps.
func TestPgrCandidatesIgnoreOROperands(t *testing.T) {
	sql := mustTranslate(t, `user=bob OR pgrHasAncestor("a.exe")`, pgrOpts())
	if strings.Count(sql, "bob") != 1 {
		t.Errorf("an OR operand was narrowed:\n%s", sql)
	}
}

// One filter's own subquery is not a scan filter, so it must not narrow another.
func TestPgrCandidatesDoNotEmbedOtherFilters(t *testing.T) {
	sql := mustTranslate(t, `user=bob | pgrHasNetwork() | pgrHasAncestor("a", depth=1)`, pgrOpts())
	if strings.Count(sql, "FROM process_edges") != 1 || !pgrNarrowedBy(sql, "fields.`user`::String = 'bob'") {
		t.Errorf("a filter reached another's candidates:\n%s", sql)
	}
}

// Candidate text from another query must stay as it is: narrowing it by this query's filters
// changed results when a binding was inlined.
func TestPgrCandidatesNarrowOnlyOwnEntries(t *testing.T) {
	ctx := &CommandContext{Plan: NewQueryPlan(), Opts: pgrOpts(), Registry: NewFieldRegistry(SourceHot, nil)}
	candidates := pgrCandidates(ctx)
	own, foreign := "a IN ("+candidates+")", "b IN ("+candidates+")"
	ctx.Plan.SourceStage().Layer.Where = []string{"fields.`user`::String = 'bob'", own, foreign}
	ctx.Plan.pgrWhere = []string{own}
	narrowPgrCandidates(ctx)
	where := ctx.Plan.SourceStage().Layer.Where
	if !strings.Contains(where[1], "AND fields.`user`::String = 'bob'") {
		t.Errorf("own entry not narrowed: %s", where[1])
	}
	if where[2] != foreign {
		t.Errorf("foreign entry changed: %s", where[2])
	}
}

func TestPgrHasAncestorFollowsTheQueryTimeBasis(t *testing.T) {
	o := pgrOpts()
	o.UseIngestTimestamp, o.EndExclusive = true, true
	sql := mustTranslate(t, `pgrHasAncestor("a.exe", depth=1)`, o)
	if sub := sql[strings.Index(sql, "FROM proc_lineage"):]; !strings.Contains(sub, "ingest_timestamp >= '2026-11-01") || !strings.Contains(sub, "ingest_timestamp < '2026-11-02") {
		t.Errorf("candidates are not on the alert's ingest axis: %s", sub)
	}
}

func TestPgrHasAncestorOnDistributedLogs(t *testing.T) {
	o := pgrOpts()
	o.TableName, o.ProcLineageTable, o.ProcEdgesTable = "logs_distributed", "proc_lineage_distributed", "process_edges_distributed"
	for q, want := range map[string]string{
		`pgrHasAncestor("a.exe")`:     "GLOBAL IN (SELECT process_guid FROM proc_lineage_distributed",
		`NOT pgrHasAncestor("a.exe")`: "GLOBAL NOT IN (SELECT process_guid FROM proc_lineage_distributed",
		`pgrHasNetwork()`:             "GLOBAL IN (SELECT process_guid FROM process_edges_distributed",
	} {
		sql := mustTranslate(t, q, o)
		if !strings.Contains(sql, want) {
			t.Errorf("%s: missing %q in:\n%s", q, want, sql)
		}
		if strings.Contains(sql, "FROM logs WHERE") || !strings.Contains(sql, "SELECT "+procKeyExpr(pgrRowRef)+" FROM logs_distributed WHERE") {
			t.Errorf("%s: candidates must read the same table as the rows:\n%s", q, sql)
		}
		if plain := strings.NewReplacer("GLOBAL IN (", "", "GLOBAL NOT IN (", "").Replace(sql); strings.Contains(plain, " IN (SELECT") {
			t.Errorf("%s: a plain IN over Distributed tables:\n%s", q, sql)
		}
	}
}

func TestPgrHasAncestorEscapesItsArgument(t *testing.T) {
	if sql := mustTranslate(t, `pgrHasAncestor("a'b.exe")`, pgrOpts()); !strings.Contains(sql, `lower('a\'b.exe')`) {
		t.Errorf("argument not escaped:\n%s", sql)
	}
}

func TestPgrHasAncestorInChainStep(t *testing.T) {
	sql := mustTranslate(t, `chain(user){image=a; pgrHasAncestor("b.exe")}`, pgrOpts())
	if !strings.Contains(sql, pgrRowRef+" IN (SELECT process_guid FROM proc_lineage") {
		t.Errorf("chain step lost the filter:\n%s", sql)
	}
}

func TestPgrHasAncestorRejectedOnArchive(t *testing.T) {
	o := pgrOpts()
	o.SourceMode = SourceIceberg
	if _, err := translateNoFatal(`pgrHasAncestor("a.exe")`, o); err == nil || !strings.Contains(err.Error(), "not supported in archive search") {
		t.Errorf("err = %v", err)
	}
}

func TestPgrHasAncestorRejects(t *testing.T) {
	cases := map[string]string{
		`pgrHasAncestor()`:                           "needs a name",
		`pgrHasAncestor(["a.exe"])`:                  "one name",
		`pgrHasAncestor("")`:                         "no value",
		`NOT pgrHasAncestor("")`:                     "no value",
		`pgrHasAncestor(*)`:                          "no value",
		`let &e := lower(image); pgrHasAncestor(&e)`: "one name",
		`let &v := image; pgrHasAncestor(&v)`:        "one name",
		`pgrHasAncestor("a", depth=0)`:               "depth=",
		`pgrHasNetwork(*)`:                           "no value",
		`pgrHasNetwork("10.0.0.0/8")`:                "use a /regex/",
		`pgrHasNetwork("8.8.8.0/24")`:                "use a /regex/",
		`pgrHasNetwork("8.8.8.*")`:                   "use a /regex/",
		`groupBy(user) | case { pgrHasAncestor("a") | l:="x"; * | l:="y" }`:    "before groupby",
		`process_guid := "E" | pgrHasNetwork()`:                                "cannot be assigned",
		`pgrHasNetwork() | process_guid := "E"`:                                "cannot be assigned",
		`process_guid := "E" | case { pgrHasNetwork() | l:="1"; * | l:="2" }`:  "cannot be assigned",
		`let &s := pgrHasNetwork() | groupby(user); in(user, &s)`:              "let binding",
		`let &s := pgrHasAncestor("a") | groupby(user); * | join(user) { &s }`: "let binding",
		`pgrHasAncestor("a", depth=9)`:                                         "between 1 and 8",
		`groupBy(user) | pgrHasAncestor("a.exe")`:                              "before groupby",
		`groupBy(user) | NOT pgrHasAncestor("a")`:                              "before groupby",
		`groupBy(user) | user=y OR pgrHasAncestor("a")`:                        "before groupby",
		`groupBy(process_guid) | _count>1 OR pgrHasAncestor("a")`:              "before groupby",
		`chain(user){a=1; b=2} | pgrHasAncestor("a")`:                          "before chain",
		`chain(user){a=1; b=2} | user=y OR pgrHasAncestor("a")`:                "before chain",
		`pgrHasNetwork(["1.1.1.1"])`:                                           "one address",
		`pgrHasNetwork("")`:                                                    "no value",
		`pgrHasNetwork(external=maybe)`:                                        "external=",
		`pgrHasAncestor("a", external=true)`:                                   "external",
		`pgrHasNetwork(depth=2)`:                                               "depth",
		`groupBy(user) | pgrHasNetwork()`:                                      "before groupby",
		`chain(user){a=1; b=2} | user=y OR pgrHasNetwork()`:                    "before chain",
		`pgrHasAncestor("C:\Windows\cmd.exe")`:                                 "double",
	}
	for q, want := range cases {
		_, err := translateNoFatal(q, pgrOpts())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", q, err, want)
		}
	}
}

func TestPgrFilterAcceptedBeforeAggregation(t *testing.T) {
	for _, q := range []string{`pgrHasAncestor("a.exe") | groupBy(user)`, `user=y OR pgrHasAncestor("a") | groupBy(user)`} {
		if sql := mustTranslate(t, q, pgrOpts()); !strings.Contains(sql, "FROM proc_lineage") {
			t.Errorf("%s: filter lost:\n%s", q, sql)
		}
	}
}

// A generated source only blocks what reads it: a join block still reads logs.
func TestPgrFilterAllowedInJoinOverSubquerySource(t *testing.T) {
	o := pgrOpts()
	o.SourceSubquery = "(SELECT 1 AS a)"
	o.SourceColumns = []string{"process_guid"}
	if _, err := translateNoFatal(`* | join(process_guid) { pgrHasNetwork() | groupBy(process_guid) }`, o); err != nil {
		t.Error(err)
	}
}

// pgr() rows have no fields column, so the filter has nothing to test.
func TestPgrHasAncestorRejectedOverSubquerySource(t *testing.T) {
	o := pgrOpts()
	o.SourceSubquery = "(SELECT 1 AS a)"
	o.SourceColumns = []string{"a"}
	if _, err := translateNoFatal(`pgrHasAncestor("a.exe")`, o); err == nil || !strings.Contains(err.Error(), "cannot follow") {
		t.Errorf("err = %v", err)
	}
}

// Each row's answer stands alone, so slicing the range must not change results.
func TestPgrHasAncestorStaysChunkable(t *testing.T) {
	res, err := translateNoFatal(`pgrHasAncestor("a.exe")`, pgrOpts())
	if err != nil {
		t.Fatal(err)
	}
	if res.TimeScopedSubquery {
		t.Error("marked time scoped")
	}
}

func TestPgrLookupBudget(t *testing.T) {
	repeat := func(q string, n int) string { return strings.TrimSuffix(strings.Repeat(q+" | ", n), " | ") }
	deep := `pgrHasAncestor("a", depth=8)`
	for q, ok := range map[string]bool{
		deep: true,
		`pgrHasAncestor("a") | pgrHasAncestor("b")`: true,
		repeat(`pgrHasNetwork()`, 8):                true,
		repeat(`pgrHasNetwork()`, 9):                false,
		repeat(`pgrHasAncestor("a", depth=1)`, 9):   false,
		repeat(deep, 2):                             false,
		`user=x OR ` + repeat(deep, 2):              false,
		`* | join(process_guid) { ` + deep + ` | groupBy(process_guid) } | ` + deep: false,
	} {
		if _, err := translateNoFatal(q, pgrOpts()); (err == nil) != ok {
			t.Errorf("%.60s: err = %v, want ok=%v", q, err, ok)
		}
	}
}

func TestFirstPgrFilter(t *testing.T) {
	for q, want := range map[string]string{
		`user=x`:                    "",
		`pgrHasAncestor("a")`:       "pgrHasAncestor",
		`user=x OR PGRHASNETWORK()`: "pgrHasNetwork",
		`user=x | groupby(process_guid) | pgrContext(commandline)`: "pgrContext",
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		if got := FirstPgrFilter(pl); got != want {
			t.Errorf("%s: got %q, want %q", q, got, want)
		}
	}
}

// Tenant isolation: every provenance read carries the scope, at every level of the walk.
func TestPgrFilterScopesEveryRead(t *testing.T) {
	o := pgrOpts()
	o.FractalID, o.FractalIDs = "", []string{"a", "b"}
	for _, q := range []string{`pgrHasAncestor("x.exe", depth=8)`, `NOT pgrHasAncestor("x.exe")`, `pgrHasNetwork()`} {
		sql := mustTranslate(t, q, o)
		reads := strings.Count(sql, "FROM proc_lineage") + strings.Count(sql, "FROM process_edges")
		scoped := strings.Count(sql, "FROM proc_lineage WHERE fractal_id IN ('a', 'b')") + strings.Count(sql, "FROM process_edges WHERE fractal_id IN ('a', 'b')")
		if reads == 0 || reads != scoped {
			t.Errorf("%s: %d of %d reads scoped:\n%s", q, scoped, reads, sql)
		}
	}
}

// RE2 on both sides, so Go checks the boundaries of what ClickHouse will run.
func TestPgrExternalBoundaries(t *testing.T) {
	v6, v4, not := regexp.MustCompile(pgrGlobalIPv6), regexp.MustCompile(pgrPlainIPv4), regexp.MustCompile(pgrNonRoutableIPv4)
	external := func(s string) bool { return v6.MatchString(s) || (v4.MatchString(s) && !not.MatchString(s)) }
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "1.0.0.1": true, "100.63.255.1": true, "100.128.0.1": true, "223.255.255.1": true, "172.32.0.1": true,
		"2606:4700::1111": true, "2A00:1450::1": true, "3fff::1": true,
		"0.1.2.3": false, "100.64.0.1": false, "100.127.0.1": false, "224.0.0.1": false, "239.1.1.1": false,
		"240.0.0.1": false, "255.255.255.255": false, "10.1.2.0/24": false, "internal": false,
		"1fff::1": false, "4000::1": false, "ff02::1": false, "::ffff:8.8.8.8": false, "evil.example.com": false,
	} {
		if got := external(addr); got != want {
			t.Errorf("%s: external = %v", addr, got)
		}
	}
}

// The filter's error for internal addresses must follow what ingest does.
func TestInternalRangesMatchIngest(t *testing.T) {
	for _, prefix := range []string{internalIPv4Prefix, internalIPv6Prefix} {
		if !strings.Contains(ipAbstractTmpl, strings.ReplaceAll(prefix, `\`, `\\`)) {
			t.Errorf("%s is not in ipAbstractTmpl", prefix)
		}
	}
}

// Descendants named in the ancestry match by the descendant's own image; processes added since the
// builder's watermark, and those below them, walk up proc_lineage to find their ancestors.
func TestPgrHasDescendantSQL(t *testing.T) {
	cand := "(SELECT " + procKeyExpr(pgrRowRef) + " FROM logs WHERE " + pgrWindow + ")"
	m := "match(image, '(?i)who')"
	since := "ingest_timestamp >= (SELECT argMax(at, set_at) FROM proc_ancestry_watermark)"
	ready := "(SELECT throwIf(count() > 1000000, 'pgrHasDescendant() is not available yet: process ancestry is still being recorded. Try again later.') FROM proc_lineage WHERE fractal_id = 'f' AND " + since + ") = 0"
	recent := "SELECT proc_key FROM proc_lineage WHERE fractal_id = 'f' AND " + ready + " AND " + since
	below := "SELECT proc_key FROM proc_ancestry WHERE fractal_id = 'f' AND anc_key IN (" + recent + ")"
	l0 := "SELECT proc_key FROM proc_lineage WHERE fractal_id = 'f' AND " + m + " AND (proc_key IN (" + recent + ") OR proc_key IN (" + below + "))"
	l1 := "SELECT parent_key FROM proc_lineage WHERE fractal_id = 'f' AND parent_key != '' AND proc_key IN (" + l0 + ")"
	l2 := "SELECT parent_key FROM proc_lineage WHERE fractal_id = 'f' AND parent_key != '' AND proc_key IN (" + l1 + ")"
	row := procKeyExpr(pgrRowRef)
	want := "(" + row + " IN (SELECT anc_key FROM proc_ancestry WHERE fractal_id = 'f' AND depth BETWEEN 1 AND 2 AND " + m + " AND anc_key != '' AND anc_key IN " + cand + ")" +
		" OR " + row + " IN (" + l1 + ") OR " + row + " IN (" + l2 + "))"
	if sql := mustTranslate(t, `pgrHasDescendant(/who/, depth=2)`, pgrOpts()); !strings.Contains(sql, want) {
		t.Errorf("missing %q in:\n%s", want, sql)
	}
	for q, want := range map[string]string{
		`NOT pgrHasDescendant("whoami.exe")`: procKeyExpr(pgrRowRef) + " NOT IN (SELECT anc_key FROM proc_ancestry",
		`pgrHasDescendant("whoami.exe")`:     "depth BETWEEN 1 AND 5",
		`pgrHasDescendant("C:\\x\\w.exe")`:   `lower(image) = lower('C:\\x\\w.exe')`,
	} {
		if sql := mustTranslate(t, q, pgrOpts()); !strings.Contains(sql, want) {
			t.Errorf("%s: missing %q in:\n%s", q, want, sql)
		}
	}
	o := pgrOpts()
	o.TableName, o.ProcLineageTable, o.ProcAncestryTable, o.ProcAncestryMarkTable = "logs_distributed", "proc_lineage_distributed", "proc_ancestry_distributed", "proc_ancestry_watermark_distributed"
	sql := mustTranslate(t, `pgrHasDescendant("whoami.exe")`, o)
	if plain := strings.NewReplacer("GLOBAL IN (", "", "GLOBAL NOT IN (", "").Replace(sql); strings.Contains(plain, " IN (SELECT") {
		t.Errorf("a plain IN over Distributed tables:\n%s", sql)
	}
	for _, q := range []string{`pgrHasDescendant()`, `pgrHasDescendant("a", depth=9)`, `groupby(user) | pgrHasDescendant("a")`} {
		if _, err := translateNoFatal(q, pgrOpts()); err == nil {
			t.Errorf("%s: want an error", q)
		}
	}
}

// Ancestry is built per fractal, so a scope over several fractals walks lineage as before, where a
// parent in another member fractal still counts.
func TestPgrHasAncestorAcrossFractalsWalks(t *testing.T) {
	o := pgrOpts()
	o.FractalID, o.FractalIDs = "", []string{"a", "b"}
	if sql := mustTranslate(t, `pgrHasAncestor("x.exe")`, o); strings.Contains(sql, "proc_ancestry") {
		t.Errorf("a multi-fractal scope read the per-fractal ancestry:\n%s", sql)
	}
	o.FractalIDs = []string{"a"}
	if sql := mustTranslate(t, `pgrHasAncestor("x.exe")`, o); !strings.Contains(sql, "proc_ancestry") {
		t.Errorf("one fractal should read the ancestry:\n%s", sql)
	}
}

// Until the ancestry has caught up, the descendant filter would walk all lineage, so it says so.
func TestPgrHasDescendantRefusesWhileAncestryCatchesUp(t *testing.T) {
	sql := mustTranslate(t, `pgrHasDescendant("x.exe")`, pgrOpts())
	if !strings.Contains(sql, "throwIf(count() > 1000000, 'pgrHasDescendant() is not available yet: process ancestry is still being recorded. Try again later.')") {
		t.Errorf("no readiness check:\n%s", sql)
	}
}

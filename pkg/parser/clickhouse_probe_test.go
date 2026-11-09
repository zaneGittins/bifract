package parser_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/db"
	"bifract/pkg/models"
	"bifract/pkg/parser"
)

// Every query the translator accepts must produce SQL ClickHouse accepts. Tests that
// compare SQL text cannot see an unknown identifier or an illegal argument type, and
// both reached users that way: a model column filtered on the raw scan, lower() given
// a number. This runs every golden query and a generated grid of command shapes
// through ClickHouse's analyzer (EXPLAIN QUERY TREE, which resolves every name and
// type without reading data) against the real schema.
//
// It needs a clickhouse binary (clickhouse local); set BIFRACT_TEST_CLICKHOUSE_BIN to
// one not on PATH. Without one it skips, unless BIFRACT_REQUIRE_CLICKHOUSE is set, as
// CI sets it. BIFRACT_PROBE_REPORT=path writes every rejection there, not only the
// first few.
func TestTranslationsResolveInClickHouse(t *testing.T) {
	bin := clickhouseBin(t)
	var probes []probe
	for _, q := range goldenQueries(t) {
		// hints only adds index hints to plain, which do not change what resolves.
		for _, v := range []string{"plain", "alert"} {
			probes = append(probes, probe{query: q, variant: v})
		}
	}
	for _, q := range probeGrid() {
		for _, v := range []string{"plain", "alert"} {
			probes = append(probes, probe{query: q, variant: v, grid: true})
		}
	}
	for i := range probes {
		probes[i].translate()
	}
	failures := resolveAll(t, bin, probes)

	var unexpected []string
	failing := map[string]bool{}
	for _, p := range probes {
		key := p.query + " [" + p.variant + "]"
		msg, failed := failures[key]
		if !failed {
			continue
		}
		failing[p.query] = true
		if _, known := knownUnresolvable[p.query]; !known {
			unexpected = append(unexpected, fmt.Sprintf("%s\n    %s\n    SQL: %s", key, msg, oneLine(p.sql)))
		}
	}
	for _, q := range slices.Sorted(maps.Keys(knownUnresolvable)) {
		if !failing[q] {
			t.Errorf("%s now resolves: remove it from knownUnresolvable", q)
		}
	}
	if path := os.Getenv("BIFRACT_PROBE_REPORT"); path != "" {
		if err := os.WriteFile(path, []byte(strings.Join(unexpected, "\n")+"\n"), 0o644); err != nil {
			t.Error(err)
		}
	}
	if len(unexpected) > 0 {
		t.Errorf("%d translations ClickHouse rejects:\n%s", len(unexpected), strings.Join(unexpected[:min(len(unexpected), 15)], "\n"))
	}
}

// knownUnresolvable are queries the translator accepts but whose SQL ClickHouse
// rejects, each a bug to fix, with the cause. A fix fails the test until its
// entry is removed.
var knownUnresolvable = map[string]string{
	`* | head(50) | k := host | chain(user) { k="x"; b="y" }`: "a copy of a field is projected into the aggregating SELECT",
	`* | head(50) | k := host | count(k)`:                     "a copy of a field is projected into the aggregating SELECT",
	`* | head(50) | k := host | top(k)`:                       "a copy of a field is projected into the aggregating SELECT",
	`* | k := host | chain(user) { k="x"; b="y" }`:            "a copy of a field is projected into the aggregating SELECT",
	`* | k := host | count(k)`:                                "a copy of a field is projected into the aggregating SELECT",
	`* | k := host | top(k)`:                                  "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user]) | k := host | chain(user) { k="x"; b="y" }`:                     "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user]) | k := host | count(k)`:                                         "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user]) | k := host | top(k)`:                                           "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user]) | k := is_new | chain(user) { k="x"; b="y" }`:                   "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user]) | k := is_new | count(k)`:                                       "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user]) | k := is_new | top(k)`:                                         "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := host | chain(user) { k="x"; b="y" }`:       "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := host | count(k)`:                           "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := host | top(k)`:                             "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := is_new | chain(user) { k="x"; b="y" }`:     "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := is_new | count(k)`:                         "a copy of a field is projected into the aggregating SELECT",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := is_new | top(k)`:                           "a copy of a field is projected into the aggregating SELECT",
	`pgrHasAncestor("cmd.exe") | k := host | chain(user) { k="x"; b="y" }`:                                           "a copy of a field is projected into the aggregating SELECT",
	`pgrHasAncestor("cmd.exe") | k := host | count(k)`:                                                               "a copy of a field is projected into the aggregating SELECT",
	`pgrHasAncestor("cmd.exe") | k := host | top(k)`:                                                                 "a copy of a field is projected into the aggregating SELECT",
	`* | head(50) | k := len(host) | cidr("k", "10.0.0.0/8")`:                                                        "a text match or cidr() reads a number without converting it",
	`* | head(50) | k := len(host) | cidr(k, "10.0.0.0/8")`:                                                          "a text match or cidr() reads a number without converting it",
	`* | head(50) | k := len(host) | k =~ "x"`:                                                                       "a text match or cidr() reads a number without converting it",
	`* | k := len(host) | cidr("k", "10.0.0.0/8")`:                                                                   "a text match or cidr() reads a number without converting it",
	`* | k := len(host) | cidr(k, "10.0.0.0/8")`:                                                                     "a text match or cidr() reads a number without converting it",
	`* | k := len(host) | k =~ "x"`:                                                                                  "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user]) | k := event_count * 2 | cidr("k", "10.0.0.0/8")`:               "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user]) | k := event_count * 2 | cidr(k, "10.0.0.0/8")`:                 "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user]) | k := event_count * 2 | k =~ "x"`:                              "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user]) | k := len(host) | cidr("k", "10.0.0.0/8")`:                     "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user]) | k := len(host) | cidr(k, "10.0.0.0/8")`:                       "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user]) | k := len(host) | k =~ "x"`:                                    "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := event_count * 2 | cidr("k", "10.0.0.0/8")`: "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := event_count * 2 | cidr(k, "10.0.0.0/8")`:   "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := event_count * 2 | k =~ "x"`:                "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := len(host) | cidr("k", "10.0.0.0/8")`:       "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := len(host) | cidr(k, "10.0.0.0/8")`:         "a text match or cidr() reads a number without converting it",
	`* | modelLookup(model="probe_seen", key=[user], strict=false) | k := len(host) | k =~ "x"`:                      "a text match or cidr() reads a number without converting it",
	`pgrHasAncestor("cmd.exe") | k := len(host) | cidr("k", "10.0.0.0/8")`:                                           "a text match or cidr() reads a number without converting it",
	`pgrHasAncestor("cmd.exe") | k := len(host) | cidr(k, "10.0.0.0/8")`:                                             "a text match or cidr() reads a number without converting it",
	`pgrHasAncestor("cmd.exe") | k := len(host) | k =~ "x"`:                                                          "a text match or cidr() reads a number without converting it",
	`* | frequency(user) | (_percentage > 3 OR lower(_value)="x")`:                                                   "Window function sum(_count) OVER () is found in HAVING in query",
	`* | groupby(a) | groupby(b) | _count > 1`:                                                                       "Unknown expression identifier `b`. Maybe you meant: ['a'].",
	`* | groupby(country, city, function=count()) | groupby(country) | sum(_count)`:                                  "Illegal type UInt64 of first argument of function toFloat64OrNull. Conversion functions with postfix 'OrZero' ",
	`* | groupby(lower(user)) | table(lower_user) | groupby(lower(user))`:                                            "Unknown expression or function identifier `fields.user`.",
	`* | groupby(src_ip, function=count()) | groupby(_count, function=count())`:                                      "Aggregate function COUNT(*) AS _count is found in GROUP BY in query",
	`* | groupby(upper(host)) | table(upper_host) | groupby(upper(host), function=count())`:                          "Unknown expression or function identifier `fields.host`.",
	`* | groupby(user) | avg(_count) | _avg > 1`:                                                                     "Aggregate function COUNT(*) AS _count is found inside another aggregate function in query",
	`* | groupby(user) | groupby(user) | chain(user) { a="x"; b="y" }`:                                               "Unknown expression or function identifier `fields.user`.",
	`* | groupby(user) | modelLookup(model="fs", key=[user]) | groupby(is_new)`:                                      "Unknown expression identifier `is_new`.",
	`* | groupby(user) | model_lookup(model="fs", key=[user]) | groupby(is_new)`:                                     "Unknown expression identifier `is_new`.",
	`* | groupby(user, function=count()) | groupby(_count, function=count())`:                                        "Aggregate function COUNT(*) AS _count is found in GROUP BY in query",
	`* | model_lookup(model="fs", key=[user]) | groupby(user) | groupby(is_new)`:                                     "Unknown expression identifier `is_new`.",
	`image=/powershell/i | groupBy(user) | count() | sort(count, desc) | limit(10)`:                                  "Unknown expression or function identifier `fields.count`.",
	`status=500 | groupBy(host) | count() | sort(count, desc) | limit(5)`:                                            "Unknown expression or function identifier `fields.count`.",
	`status>=500 | replace(message, "\n", " ") | groupBy(host) | count() | sort(count, desc) | head(10)`:             "Unknown expression or function identifier `fields.count`.",
}

// probe is one query translated under one option set.
type probe struct {
	query, variant string
	grid           bool
	sql            string
}

func (p *probe) translate() {
	pl, err := parser.ParseQuery(p.query)
	if err != nil {
		return
	}
	opts := probeOptions(p.query, p.variant, p.grid)
	r, err := parser.TranslateToSQLWithOrder(pl, opts)
	if err != nil {
		return
	}
	p.sql = strings.TrimRight(r.SQL, "; \n")
}

// probeOptions is a search ("plain") or an alert on a cluster ("alert"), with every
// model a golden query names. Grid queries name the probe model and dictionary,
// which exist in the probe schema.
func probeOptions(q, variant string, grid bool) parser.QueryOptions {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := parser.QueryOptions{StartTime: start, EndTime: start.Add(24 * time.Hour), MaxRows: 100, FractalID: "f1"}
	o.Models = parser.GoldenModels(q)
	if grid {
		o.Models = map[string]parser.AnalyticsModelInfo{
			"probe_seen": {ID: "probe_seen", TableName: "model_probe_seen", ModelType: "first_seen", Keys: []string{"user"}, FractalID: "f1"},
		}
		o.DictionaryDatabase, o.Dictionaries = "logs", map[string]map[string]string{"d": {"c": "d_names"}}
	}
	if variant == "alert" {
		o.TableName, o.NormLogIndexHints, o.UseIngestTimestamp, o.MinimalProjection = "logs_distributed", true, true, true
		for k, m := range o.Models {
			m.Distributed, m.TableName = true, m.TableName+"_distributed"
			o.Models[k] = m
		}
		o.HasCommentFilter, o.CommentLogIDs = true, []string{"a", "b"}
	}
	return o
}

// probeGrid crosses each place a field can come from with each way a command reads
// it, in each context that changes where it is read: the raw scan, the scan-level
// model join, a provenance filter that narrows by the scan's filters, and a row cap.
func probeGrid() []string {
	contexts := []string{
		"* | ",
		`* | modelLookup(model="probe_seen", key=[user]) | `,
		`* | modelLookup(model="probe_seen", key=[user], strict=false) | `,
		`pgrHasAncestor("cmd.exe") | `,
		"* | head(50) | ",
	}
	producers := []struct{ src, needsModel string }{
		{"k := host | ", ""},
		{"k := lower(host) | ", ""},
		{"k := len(host) | ", ""},
		{"eval(k = lower(host)) | ", ""},
		{"eval(k = host) | ", ""},
		{`lowercase("host", output=k) | `, ""},
		{`case { a="1" | k := "x"; * | k := "y" } | `, ""},
		{"k := is_new | ", "model"},
		{"eval(k = lower(is_new)) | ", "model"},
		{"k := event_count * 2 | ", "model"},
	}
	consumers := []string{
		`k = "x"`, `k != "x"`, `k = *`, `k > 1`, `k =~ "x"`, `lower(k) = "x"`, `NOT k = "x" OR a = "1"`,
		`in(k, values=["x"])`, `in("k", values=["x"])`, `cidr(k, "10.0.0.0/8")`, `cidr("k", "10.0.0.0/8")`,
		`match(dict="d", field=k, column="c", include=[c], strict=true)`,
		"sort(k)", "dedup(k)", "dedup(k) | head(5)", "groupby(k)", "groupby(k) | _count > 1",
		"table(k)", "top(k)", "count(k)", `k2 := concat(k, "a") | table(k2)`, `strftime("%Y", field=k, as=y)`,
		`chain(user) { k="x"; b="y" }`,
	}
	var out []string
	for _, c := range contexts {
		model := strings.Contains(c, "modelLookup")
		for _, p := range producers {
			if p.needsModel != "" && !model {
				continue
			}
			for _, k := range consumers {
				out = append(out, c+p.src+k)
			}
		}
	}
	return out
}

func goldenQueries(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("testdata/golden.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var qs []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var rec struct{ Q string }
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		qs = append(qs, rec.Q)
	}
	return qs
}

func clickhouseBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("BIFRACT_TEST_CLICKHOUSE_BIN")
	if bin == "" {
		bin = "clickhouse"
	}
	if path, err := exec.LookPath(bin); err == nil {
		return path
	}
	if os.Getenv("BIFRACT_REQUIRE_CLICKHOUSE") != "" {
		t.Fatalf("no clickhouse binary %q, and BIFRACT_REQUIRE_CLICKHOUSE is set", bin)
	}
	t.Skipf("no clickhouse binary %q; set BIFRACT_TEST_CLICKHOUSE_BIN", bin)
	return ""
}

// probeSchema is the real ClickHouse schema, the probe models' tables and dictionary,
// and stand-ins for the Distributed tables a cluster reads, which resolve the same.
func probeSchema(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	b.WriteString(strings.TrimRight(strings.TrimSpace(db.ClickHouseSQL), ";"))
	b.WriteString(";\nUSE logs;\n")
	for _, table := range []string{"logs", "proc_lineage", "proc_ancestry", "process_edges", "proc_ancestry_watermark"} {
		fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %[1]s_distributed AS %[1]s;\n", table)
	}
	// Golden queries model every model as rarity; the grid's has a name of its own.
	tables := map[string]models.ModelType{"model_probe_seen": models.ModelTypeFirstSeen}
	for _, q := range goldenQueries(t) {
		for _, m := range parser.GoldenModels(q) {
			tables[m.TableName] = models.ModelTypeRarity
		}
	}
	for _, name := range slices.Sorted(maps.Keys(tables)) {
		for _, table := range []string{name, name + "_distributed"} {
			ddl, err := models.GenerateDDL(models.ModelDefinition{}, tables[name], table)
			if err != nil {
				t.Fatal(err)
			}
			b.WriteString(ddl + ";\n")
		}
	}
	b.WriteString("CREATE DICTIONARY IF NOT EXISTS d_names (key String, c String) PRIMARY KEY key SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0);\n")
	return b.String()
}

var probeMarkerRe = regexp.MustCompile(`(?m)^PROBE (\d+)$`)
var probeErrorRe = regexp.MustCompile(`Code: \d+\. DB::Exception: [^\n]*`)

// resolveAll analyzes every translated probe, split across clickhouse local
// processes, and returns the error for each that ClickHouse rejects, keyed by query
// and variant.
func resolveAll(t *testing.T, bin string, probes []probe) map[string]string {
	t.Helper()
	schema := probeSchema(t)
	dir := t.TempDir()
	workers := min(runtime.NumCPU(), 8)
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		failures = map[string]string{}
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var batch strings.Builder
			batch.WriteString(schema)
			batch.WriteString("SELECT 'SCHEMA READY';\n")
			for i := w; i < len(probes); i += workers {
				if probes[i].sql != "" {
					fmt.Fprintf(&batch, "SELECT 'PROBE %d';\nEXPLAIN QUERY TREE %s;\n", i, probes[i].sql)
				}
			}
			path := filepath.Join(dir, fmt.Sprintf("batch%d.sql", w))
			if err := os.WriteFile(path, []byte(batch.String()), 0o644); err != nil {
				t.Error(err)
				return
			}
			cmd := exec.Command(bin, "local", "--path", filepath.Join(dir, fmt.Sprint("data", w)), "--multiquery", "--ignore-error", "--queries-file", path)
			out, _ := cmd.CombinedOutput()
			text := string(out)
			ready := strings.Index(text, "SCHEMA READY")
			if ready < 0 {
				t.Errorf("the probe schema did not load: %s", oneLine(text))
				return
			}
			if e := probeErrorRe.FindString(text[:ready]); e != "" {
				t.Errorf("the probe schema did not load: %s", e)
				return
			}
			locs := probeMarkerRe.FindAllStringSubmatchIndex(text, -1)
			mu.Lock()
			defer mu.Unlock()
			for k, loc := range locs {
				end := len(text)
				if k+1 < len(locs) {
					end = locs[k+1][0]
				}
				if e := probeErrorRe.FindString(text[loc[1]:end]); e != "" {
					var i int
					fmt.Sscan(text[loc[2]:loc[3]], &i)
					failures[probes[i].query+" ["+probes[i].variant+"]"] = e
				}
			}
		}()
	}
	wg.Wait()
	return failures
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

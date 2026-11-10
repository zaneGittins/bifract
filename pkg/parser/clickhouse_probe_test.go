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
	// The canary proves the harness sees a rejection at all: a clickhouse local that
	// reported none would otherwise pass every query.
	canary := probe{query: "canary", variant: "plain", sql: "SELECT no_such_column FROM logs"}
	probes = append(probes, canary)
	failures := resolveAll(t, bin, probes, func(p probe) bool {
		_, known := knownUnresolvable[p.query]
		return !known && p.query != canary.query
	})

	if _, rejected := failures["canary [plain]"]; !rejected {
		t.Fatalf("ClickHouse accepted %q: the harness cannot see rejections", canary.sql)
	}
	delete(failures, "canary [plain]")
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
var knownUnresolvable = map[string]string{}

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
		{"k := bin(timestamp, 5m) | ", ""},
		{"k := timestamp - 1h | ", ""},
		{"k := now() - timestamp | ", ""},
		{"k := hourOfDay(timestamp) | ", ""},
	}
	consumers := []string{
		`k = "x"`, `k != "x"`, `k = *`, `k > 1`, `k =~ "x"`, `lower(k) = "x"`, `NOT k = "x" OR a = "1"`,
		`in(k, values=["x"])`, `in("k", values=["x"])`, `cidr(k, "10.0.0.0/8")`, `cidr("k", "10.0.0.0/8")`,
		`match(dict="d", field=k, column="c", include=[c], strict=true)`,
		"sort(k)", "dedup(k)", "dedup(k) | head(5)", "groupby(k)", "groupby(k) | _count > 1", "groupby(user)",
		"groupby(k) | max(_count) | table(_max)", "groupby(k) | mad(_count) | _mad > 1", "groupby(k) | count() | sort(count)",
		"groupby(k) | head(3) | max(_count)", "groupby(k) | n := _count * 2 | max(n)",
		"table(k)", "top(k)", "count(k)", `k2 := concat(k, "a") | table(k2)`, `strftime("%Y", field=k, as=y)`,
		`chain(user) { k="x"; b="y" }`,
		`x := dateDiff("second", k, timestamp) | table(x)`, `hourOfDay(k) < 6`, `(now() - k) < 1h`, `k > "2026-01-01"`,
		`groupby(bin(k, 1h))`, `x := k + 1h | groupby(x)`, `groupby(user, function=max(k))`, `k > 5m`,
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

// resolveAll analyzes every translated probe and returns the error for each that
// ClickHouse rejects, keyed by query and variant.
//
// The probes run in batches across clickhouse local processes with --ignore-error,
// which some releases (26.8) honour by printing nothing for a failed statement. So a
// probe counts as resolved only on positive evidence, its query tree in the batch's
// output. One that needsReason is run again on its own, without --ignore-error, for
// the error; any other is reported as rejected without one.
func resolveAll(t *testing.T, bin string, probes []probe, needsReason func(probe) bool) map[string]string {
	t.Helper()
	schema := probeSchema(t)
	dir := t.TempDir()
	if _, msg := analyze(bin, dir, "schema", schema); msg != "" {
		t.Fatalf("the probe schema did not load: %s", msg)
	}
	workers := min(runtime.NumCPU(), 8)
	resolved := make([]bool, len(probes))
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var batch strings.Builder
			batch.WriteString(schema)
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
			out, _ := cmd.Output() // stdout alone, so markers and trees stay in order
			text := string(out)
			locs := probeMarkerRe.FindAllStringSubmatchIndex(text, -1)
			for k, loc := range locs {
				end := len(text)
				if k+1 < len(locs) {
					end = locs[k+1][0]
				}
				if strings.Contains(text[loc[1]:end], "QUERY id:") {
					var i int
					fmt.Sscan(text[loc[2]:loc[3]], &i)
					resolved[i] = true // each index belongs to one worker
				}
			}
		}()
	}
	wg.Wait()

	var (
		mu       sync.Mutex
		failures = map[string]string{}
		next     = make(chan int)
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				msg := "rejected"
				if needsReason(probes[i]) {
					ok, reason := analyze(bin, dir, fmt.Sprint("probe", i), schema+"EXPLAIN QUERY TREE "+probes[i].sql+";\n")
					msg = reason
					if ok {
						msg = "resolved alone but not in its batch"
					}
				}
				mu.Lock()
				failures[probes[i].query+" ["+probes[i].variant+"]"] = msg
				mu.Unlock()
			}
		}()
	}
	for i, p := range probes {
		if p.sql != "" && !resolved[i] {
			next <- i
		}
	}
	close(next)
	wg.Wait()
	return failures
}

// analyze runs statements in a clickhouse local process of their own, stopping at the
// first error, and reports whether the last printed a query tree, or the error.
func analyze(bin, dir, name, statements string) (bool, string) {
	path := filepath.Join(dir, name+".sql")
	if err := os.WriteFile(path, []byte(statements), 0o644); err != nil {
		return false, err.Error()
	}
	out, err := exec.Command(bin, "local", "--path", filepath.Join(dir, name+"-data"), "--multiquery", "--queries-file", path).CombinedOutput()
	if e := probeErrorRe.FindString(string(out)); e != "" {
		return false, e
	}
	if err != nil {
		return false, fmt.Sprintf("%v: %s", err, oneLine(string(out)))
	}
	return strings.Contains(string(out), "QUERY id:"), ""
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

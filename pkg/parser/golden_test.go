package parser

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The golden corpus is every query the tests and docs exercised when it was
// captured, each translated under the option sets below. testdata/golden.jsonl holds
// a hash of each translation, so any change to generated SQL shows up here, query by
// query. To see whether changed SQL keeps its meaning, compare it with another ref:
//
//	go run ./cmd/bifract-bqlgolden -base origin/main
//
// and once the change is understood, record it:
//
//	go test ./pkg/parser -run TestGolden -update
//
// A new query is added as a line holding only "q"; -update fills in the rest.

var (
	updateGolden = flag.Bool("update", false, "rewrite testdata/golden.jsonl from the current translator")
	goldenDump   = flag.String("golden.dump", "", "write every golden translation in full to this file")
)

const goldenPath = "testdata/golden.jsonl"

// goldenVariants are the option sets each golden query is translated under: what a
// search, an alert on a cluster, a search with index hints and an archive search use.
var goldenVariants = []struct {
	name string
	opts func(q string) QueryOptions
}{
	{"plain", func(string) QueryOptions { return goldenBase() }},
	{"alert", func(q string) QueryOptions {
		o := goldenBase()
		o.TableName, o.NormLogIndexHints, o.UseIngestTimestamp, o.MinimalProjection = "logs_distributed", true, true, true
		o.Models = GoldenModels(q)
		for k, m := range o.Models {
			m.Distributed, m.TableName = true, m.TableName+"_distributed"
			o.Models[k] = m
		}
		o.HasCommentFilter, o.CommentLogIDs = true, []string{"a", "b"}
		return o
	}},
	{"hints", func(q string) QueryOptions {
		o := goldenBase()
		o.NormLogIndexHints, o.Models = true, GoldenModels(q)
		return o
	}},
	{"iceberg", func(string) QueryOptions {
		o := goldenBase()
		o.SourceMode = SourceIceberg
		return o
	}},
}

func goldenBase() QueryOptions {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return QueryOptions{StartTime: start, EndTime: start.Add(24 * time.Hour), MaxRows: 100, FractalID: "f1"}
}

var (
	goldenModelRe = regexp.MustCompile(`model\s*=\s*"?([A-Za-z0-9_\-]+)"?`)
	goldenKeyRe   = regexp.MustCompile(`key\s*=\s*\[([^\]]*)\]`)
)

// GoldenModels gives every model a query names a rarity model keyed as the query
// asks, so a lookup translates without a model store.
func GoldenModels(q string) map[string]AnalyticsModelInfo {
	keys := []string{"user"}
	if m := goldenKeyRe.FindStringSubmatch(q); m != nil {
		keys = nil
		for _, k := range strings.Split(m[1], ",") {
			keys = append(keys, strings.Trim(strings.TrimSpace(k), `"`))
		}
	}
	out := map[string]AnalyticsModelInfo{}
	for _, m := range goldenModelRe.FindAllStringSubmatch(q, -1) {
		out[m[1]] = AnalyticsModelInfo{ID: "id-" + m[1], TableName: "model_" + m[1], ModelType: "rarity", Keys: keys, MinSample: 5, FractalID: "f1"}
	}
	return out
}

// goldenTranslations is each variant's output for q, in full: the SQL and what the
// result reports about it, or the error. Parse errors are recorded under "parse".
func goldenTranslations(q string) map[string]string {
	pl, err := ParseQuery(q)
	if err != nil {
		return map[string]string{"parse": "error: " + err.Error()}
	}
	out := map[string]string{}
	for _, v := range goldenVariants {
		r, err := TranslateToSQLWithOrder(pl, v.opts(q))
		if err != nil {
			out[v.name] = "error: " + err.Error()
			continue
		}
		// SourceWhere is SQL too, so it gets a line of its own the comparison tool
		// can parse.
		sw, _ := json.Marshal(r.SourceWhere)
		out[v.name] = fmt.Sprintf("%s\nfields=%v agg=%v chart=%s cfg=%v dto=%v swc=%v tss=%v rc=%v rdr=%v chain=%v\nsw=%s",
			r.SQL, r.FieldOrder, r.IsAggregated, r.ChartType, r.ChartConfig, r.DefaultTimeOrder,
			r.SourceWhereComplete, r.TimeScopedSubquery, r.RowCapped, r.RangeDependentRows, r.Chain != nil, sw)
	}
	if h, err := BuildHistogramSQL(pl, goldenBase(), 60); err == nil {
		out["hist"] = h
	} else {
		out["hist"] = "error: " + err.Error()
	}
	return out
}

// goldenDigest keeps an error as written and reduces SQL to a short hash.
func goldenDigest(out string) string {
	if strings.HasPrefix(out, "error: ") {
		return out
	}
	sum := sha256.Sum256([]byte(out))
	return hex.EncodeToString(sum[:8])
}

func readGolden(t *testing.T) []map[string]string {
	t.Helper()
	f, err := os.Open(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var recs []map[string]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		rec := map[string]string{}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("%s: %v", goldenPath, err)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return recs
}

func writeGolden(t *testing.T, path string, recs []map[string]string) {
	t.Helper()
	var b strings.Builder
	for _, rec := range recs {
		line, err := json.Marshal(rec) // keys sorted, so a record diffs field by field
		if err != nil {
			t.Fatal(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGolden(t *testing.T) {
	recs := readGolden(t)
	if len(recs) == 0 {
		t.Fatalf("%s holds no queries", goldenPath)
	}
	var full []map[string]string
	var changed []string
	for i, rec := range recs {
		q := rec["q"]
		out := goldenTranslations(q)
		want := map[string]string{"q": q}
		for k, v := range out {
			want[k] = goldenDigest(v)
		}
		if *goldenDump != "" {
			out["q"] = q
			full = append(full, out)
		}
		for k, v := range want {
			if rec[k] != v {
				changed = append(changed, fmt.Sprintf("%s [%s]:\n    %s", q, k, out[k]))
			}
		}
		for k := range rec {
			if _, ok := want[k]; !ok {
				changed = append(changed, fmt.Sprintf("%s [%s]: no longer produced", q, k))
			}
		}
		recs[i] = want
	}
	if *goldenDump != "" {
		writeGolden(t, *goldenDump, full)
	}
	if *updateGolden {
		writeGolden(t, goldenPath, recs)
		return
	}
	if len(changed) > 0 {
		shown := changed[:min(len(changed), 20)]
		t.Errorf("%d golden translations changed. Compare them with go run ./cmd/bifract-bqlgolden -base origin/main,\n"+
			"then record them with go test ./pkg/parser -run TestGolden -update. The first %d:\n%s",
			len(changed), len(shown), strings.Join(shown, "\n"))
	}
}

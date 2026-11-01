//go:build normlogesc

// Randomized check that norm_log index hints never change which rows a field
// filter returns. Rows go in through the production insert path into a clone of
// the logs table with small granules, so hints prune for real, and every
// generated filter must return the same rows with hints on and off.
//
//	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d clickhouse
//	go test -tags normlogesc ./pkg/parser/ -run TestNormLogHintsKeepEveryRow -v
//
// Override the DSN with BIFRACT_TEST_CH_DSN, and the seed with BIFRACT_TEST_SEED.
package parser

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"bifract/db"
	"bifract/pkg/storage"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	hintTestRows    = 20000
	hintTestFilters = 2500
)

// hintPieces builds field values: characters JSON escapes, letters whose case
// folding leaves ASCII (Kelvin sign, long s), number-like and address-like tokens.
var hintPieces = []string{
	"a", "b", "c", "k", "s", "K", "S", "x", "Q", "Z", "mimi", "katz", "Kit", "user", "svc", "tool",
	"0", "7", "42", "443", "1.5e3", "inf", "NaN", "10.1.2.3", "209.146.1.1", "2026-10-20", "true",
	".", "-", "_", ":", "@", " ", "/", "\\", "\"", "'", "\t", "\n", "<", "&", ",",
	"é", "É", "ë", "K", "ſ", "İ", "日本", " ",
}

func hintValue(r *rand.Rand) string {
	var b strings.Builder
	for n := 1 + r.Intn(10); n > 0; n-- {
		b.WriteString(hintPieces[r.Intn(len(hintPieces))])
	}
	return b.String()
}

var hintFields = []string{"user", "commandline", "note", "answers"}

func hintSubstring(r *rand.Rand, v string) string {
	rs := []rune(v)
	i := r.Intn(len(rs))
	j := i + 1 + r.Intn(len(rs)-i)
	return string(rs[i:j])
}

// bqlString quotes v as a BQL string literal.
func bqlString(v string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(v) + `"`
}

// hintRegex builds a /regex/ matching sub: literals, quoted metacharacters, and
// constructs that make parts optional or alternate, which must not be hinted.
func hintRegex(r *rand.Rand, sub string, fold bool) string {
	var b strings.Builder
	if r.Intn(3) == 0 {
		b.WriteString(".*")
	}
	for _, c := range sub {
		lit := regexp.QuoteMeta(string(c))
		if fold && (c == 'K' || c == 'ſ') {
			lit = map[rune]string{'K': "k", 'ſ': "s"}[c] // folds back under (?i)
		}
		switch r.Intn(12) {
		case 0:
			b.WriteString("(" + lit + ")+")
		case 1:
			b.WriteString("(?:" + lit + "|zzq)")
		case 2:
			b.WriteString(lit + "?")
		case 3:
			b.WriteString(lit + `\s*`)
		default:
			b.WriteString(lit)
		}
	}
	// Parts a match may skip, spelled so no row contains them: hinting one would
	// skip every block.
	switch r.Intn(4) {
	case 0:
		b.WriteString("(?:zzqoptional)?")
	case 1:
		b.WriteString("(zzqstarred)*")
	case 2:
		b.WriteString("(?:zzqalternate|" + regexp.QuoteMeta(sub) + ")")
	}
	p := strings.ReplaceAll(b.String(), "/", `\/`)
	if fold {
		return "/" + p + "/i"
	}
	return "/" + p + "/"
}

// hintFilter combines leaf filters the ways a hint must survive: alone, under
// NOT, in an OR, and beside a negated one in the same blocks.
func hintFilter(r *rand.Rand, rows []map[string]string) string {
	switch r.Intn(8) {
	case 0:
		return "!(" + hintLeaf(r, rows) + ")"
	case 1:
		return "(" + hintLeaf(r, rows) + " OR " + hintLeaf(r, rows) + ")"
	case 2:
		return hintLeaf(r, rows) + " AND !(" + hintLeaf(r, rows) + ")"
	}
	return hintLeaf(r, rows)
}

// hintList is v, sometimes followed by a second value, quoted as a BQL list.
func hintList(r *rand.Rand, v string) string {
	vals := []string{bqlString(v)}
	if r.Intn(3) == 0 {
		vals = append(vals, bqlString(hintValue(r)))
	}
	return strings.Join(vals, ",")
}

func hintLeaf(r *rand.Rand, rows []map[string]string) string {
	row := rows[r.Intn(len(rows))]
	field := hintFields[r.Intn(len(hintFields))]
	v := row[field]
	if v == "" {
		v = hintValue(r)
	}
	switch r.Intn(6) {
	case 0:
		return field + "=" + hintList(r, v)
	case 1, 2:
		sub := hintSubstring(r, v)
		if r.Intn(2) == 0 {
			sub = strings.NewReplacer("\u212A", "k", "\u017F", "s").Replace(sub)
		}
		return field + "=~" + hintList(r, sub)
	case 3:
		rs := []rune(v)
		return field + "=^" + hintList(r, string(rs[:1+r.Intn(len(rs))]))
	case 4:
		rs := []rune(v)
		return field + "=$" + hintList(r, string(rs[r.Intn(len(rs)):]))
	default:
		return field + "=" + hintRegex(r, hintSubstring(r, v), r.Intn(2) == 0)
	}
}

func TestNormLogHintsKeepEveryRowAgainstClickHouse(t *testing.T) {
	ctx := context.Background()
	conn := chTestConn(t)
	defer conn.Close()
	opts, err := clickhouse.ParseDSN(chTestDSN())
	if err != nil {
		t.Fatal(err)
	}

	dbName := fmt.Sprintf("normlog_hint_%d", time.Now().UnixNano())
	if err := conn.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName) })

	// The production logs DDL, with small granules so a hint has blocks to skip.
	const head, tail = "CREATE TABLE IF NOT EXISTS logs (", "SETTINGS index_granularity = 8192;"
	from := strings.Index(db.ClickHouseSQL, head)
	to := strings.Index(db.ClickHouseSQL[max(from, 0):], tail)
	if from < 0 || to < 0 {
		t.Fatal("logs DDL not found in db/init-clickhouse.sql")
	}
	ddl := "CREATE TABLE " + dbName + ".logs (" + db.ClickHouseSQL[from+len(head):from+to] + "SETTINGS index_granularity = 64"
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create logs clone: %v", err)
	}

	client, err := storage.NewClickHouseClient(storage.ClientOptions{Conn: storage.ConnOptions{
		Addrs: storage.HostAddrs(opts.Addr, 9000), Database: dbName, User: opts.Auth.Username, Password: opts.Auth.Password,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	seed := time.Now().UnixNano()
	if s := os.Getenv("BIFRACT_TEST_SEED"); s != "" {
		if seed, err = strconv.ParseInt(s, 10, 64); err != nil {
			t.Fatalf("BIFRACT_TEST_SEED: %v", err)
		}
	}
	t.Logf("seed %d", seed)
	r := rand.New(rand.NewSource(seed))

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]map[string]string, hintTestRows)
	entries := make([]storage.LogEntry, hintTestRows)
	for i := range rows {
		f := map[string]string{}
		for _, name := range hintFields {
			if r.Intn(3) > 0 {
				f[name] = hintValue(r)
			}
		}
		rows[i] = f
		ts := start.Add(time.Duration(i) * time.Second)
		entries[i] = storage.LogEntry{Timestamp: ts, IngestTimestamp: ts, LogID: fmt.Sprintf("id%06d", i), Fields: f, FractalID: "f1"}
	}
	for i := 0; i < len(entries); i += 5000 {
		if err := client.InsertLogsInto(ctx, "logs", entries[i:min(i+5000, len(entries))]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	ids := func(sql string) []string {
		res, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"use_query_condition_cache": 0})),
			"SELECT log_id FROM ("+sql+")")
		if err != nil {
			t.Fatalf("query: %v\n%s", err, sql)
		}
		defer res.Close()
		var out []string
		for res.Next() {
			var id string
			if err := res.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		slices.Sort(out)
		return out
	}
	translate := func(q string, hints bool) (string, bool) {
		p, err := ParseQuery(q)
		if err != nil {
			return "", false
		}
		sql, err := TranslateToSQL(p, QueryOptions{
			StartTime: start, EndTime: start.Add(hintTestRows * time.Second), FractalID: "f1",
			TableName: dbName + ".logs", MaxRows: hintTestRows * 2, NormLogIndexHints: hints,
		})
		return sql, err == nil
	}

	var hinted, matched, failed int
	for n := 0; n < hintTestFilters; n++ {
		q := hintFilter(r, rows)
		with, ok1 := translate(q, true)
		without, ok2 := translate(q, false)
		if !ok1 || !ok2 {
			failed++
			continue
		}
		if !strings.Contains(with, "indexHint(") {
			continue
		}
		hinted++
		got, want := ids(with), ids(without)
		if len(want) > 0 {
			matched++
		}
		if !slices.Equal(got, want) {
			t.Errorf("hints changed the rows of %s: %d with, %d without\n%s", q, len(got), len(want), with)
		}
	}
	t.Logf("%d filters hinted (%d with matches), %d failed to translate", hinted, matched, failed)
	if hinted < hintTestFilters/4 || matched < hinted/2 {
		t.Fatalf("too few hinted filters with matches to mean anything: %d hinted, %d matched", hinted, matched)
	}

	// The hint must actually reach the index, or the comparison above proves nothing.
	sql, _ := translate(`user=~"zzqnotthere"`, true)
	res, err := conn.Query(ctx, "EXPLAIN indexes = 1 SELECT log_id FROM ("+sql+")")
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for res.Next() {
		var line string
		if err := res.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	res.Close()
	// The granule count printed right after the norm_log index's own name.
	if !regexp.MustCompile(`Name: norm_log_ngram_lc\n(?:.*\n){0,4}?\s*Granules: 0/\d+`).MatchString(plan.String()) {
		t.Errorf("an absent value's hint did not prune every granule:\n%s", plan.String())
	}
}

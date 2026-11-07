//go:build chancestry || chcluster

// Builds proc_ancestry from a random forest against a real ClickHouse and compares every
// process's ancestors and status with a plain walk of the same lineage, after each step.
//
//	go test -tags chancestry ./pkg/storage/ -run TestProcAncestry -v   (ClickHouse on localhost:19011)
package storage

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
)

const ancestryAddr = "localhost:19011"

type lineageRow struct {
	fractal, guid, parent, image, parentImage string
	ingest                                    time.Time
}

// ancestryForest is the fixture: random trees, plus the shapes a walk can trip on.
func ancestryForest(base time.Time) []lineageRow {
	r := rand.New(rand.NewSource(7))
	var rows []lineageRow
	guid := func(fractal string, n int) string {
		if n%3 == 0 {
			return fmt.Sprintf("plain-%d", n)
		}
		return fmt.Sprintf("{6304%04x-%04x-%04x-0000-%012x}", n%7, n%65536, 0x6860+n/500, n)
	}
	img := func(n int) string { return fmt.Sprintf(`C:\bin\p%d.exe`, n%23) }
	for _, fractal := range []string{"fa", "fb"} {
		const n = 1500
		for i := 0; i < n; i++ {
			parent := ""
			switch {
			case i < 40: // roots
			case i%97 == 0: // a parent that never arrives
				parent = guid(fractal, 100000+i)
			default:
				parent = guid(fractal, r.Intn(i))
			}
			// Mostly in order, sometimes a parent lands well after its children.
			ingest := base.Add(time.Duration(i)*2*time.Second + time.Duration(r.Intn(3))*time.Second)
			if i%53 == 0 {
				ingest = base.Add(time.Duration(n)*2*time.Second + time.Duration(r.Intn(600))*time.Second)
			}
			rows = append(rows, lineageRow{fractal, guid(fractal, i), parent, img(i), img(i / 2), ingest})
		}
		// Two parents arriving together late, one above the other: D waits on K, P waits on Q,
		// then K (child of P) and Q (child of root R) land in one slice.
		late := base.Add(30 * time.Minute)
		rows = append(rows,
			lineageRow{fractal, guid(fractal, 400000), guid(fractal, 400001), img(4), img(5), base.Add(time.Second)},
			lineageRow{fractal, guid(fractal, 400002), guid(fractal, 400003), img(6), img(7), base.Add(time.Second)},
			lineageRow{fractal, guid(fractal, 400004), "", img(8), "", base.Add(time.Second)},
			lineageRow{fractal, guid(fractal, 400001), guid(fractal, 400002), img(5), img(6), late},
			lineageRow{fractal, guid(fractal, 400003), guid(fractal, 400004), img(7), img(8), late})
		// A chain deeper than the maximum, a self-parent and a two-process cycle.
		for d := 0; d < 12; d++ {
			parent := ""
			if d > 0 {
				parent = guid(fractal, 200000+d-1)
			}
			rows = append(rows, lineageRow{fractal, guid(fractal, 200000+d), parent, img(d), img(d + 1), base.Add(time.Duration(d) * time.Second)})
		}
		rows = append(rows,
			lineageRow{fractal, guid(fractal, 300000), guid(fractal, 300000), img(1), img(1), base},
			lineageRow{fractal, guid(fractal, 300001), guid(fractal, 300002), img(2), img(3), base},
			lineageRow{fractal, guid(fractal, 300002), guid(fractal, 300001), img(3), img(2), base.Add(time.Second)})
	}
	return rows
}

type ancRow struct {
	depth      int
	key, image string
}

// walkAncestry is the reference: from lineage ingested before hi, each process's ancestors as
// the filters' walk sees them, and the ancestor its chain stops at when that one has no lineage.
func walkAncestry(rows []lineageRow, hi time.Time, key map[string]string) (map[string][]ancRow, map[string]string) {
	known := map[string]lineageRow{}
	for _, r := range rows {
		if r.ingest.Before(hi) {
			known[r.fractal+"|"+r.guid] = r
		}
	}
	chains, open := map[string][]ancRow{}, map[string]string{}
	for id, r := range known {
		var chain []ancRow
		cur, stop := r, ""
		for d := 1; d <= procAncestryDepth && (cur.parent != "" || cur.parentImage != ""); d++ {
			chain = append(chain, ancRow{d, key[cur.fractal+"|"+cur.parent], cur.parentImage})
			if cur.parent == "" {
				break
			}
			next, ok := known[cur.fractal+"|"+cur.parent]
			if !ok {
				if d < procAncestryDepth {
					stop = key[cur.fractal+"|"+cur.parent]
				}
				break
			}
			cur = next
		}
		chains[id], open[id] = chain, stop
	}
	return chains, open
}

func TestProcAncestry(t *testing.T) {
	ctx := context.Background()
	root, err := openClickHouseConn(ConnOptions{Addrs: []string{ancestryAddr}, Database: "default", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{"DROP DATABASE IF EXISTS logs SYNC", "CREATE DATABASE logs"} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	root.Close()
	conn, err := openClickHouseConn(ConnOptions{Addrs: []string{ancestryAddr}, Database: "logs", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 2, MaxIdleConns: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, table := range []string{"logs", "proc_lineage", procAncestryTable, procAncestryWatermarkTable} {
		ddl, err := initTableDDL(table)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	rows := ancestryForest(base)
	seedAncestry(t, conn, "", rows)
	c := &ClickHouseClient{conn: conn}
	compareAncestry(t, c, rows, base, "proc_ancestry", func(fractal string) parser.QueryOptions { return parser.QueryOptions{FractalID: fractal} })
}

// seedAncestry inserts the forest's lineage and one log per process through tables named with
// suffix.
func seedAncestry(t *testing.T, conn interface {
	Exec(context.Context, string, ...any) error
}, suffix string, rows []lineageRow) {
	t.Helper()
	ctx := context.Background()
	var values []string
	for _, r := range rows {
		values = append(values, fmt.Sprintf("('%s', %s, %s, '%s', '%s', '%s', '%s')", r.fractal, chTime(r.ingest), chTime(r.ingest),
			r.guid, r.parent, strings.ReplaceAll(r.image, `\`, `\\`), strings.ReplaceAll(r.parentImage, `\`, `\\`)))
	}
	if err := conn.Exec(ctx, "INSERT INTO proc_lineage"+suffix+" (fractal_id, timestamp, ingest_timestamp, process_guid, parent_guid, image, parent_image) SETTINGS distributed_foreground_insert = 1 VALUES "+strings.Join(values, ", ")); err != nil {
		t.Fatal(err)
	}
	// One log per process, inside the query window, for the filters to test.
	values = values[:0]
	for _, r := range rows {
		values = append(values, fmt.Sprintf(`('%s', toDateTime64('2026-11-05 00:00:00', 3, 'UTC'), '%s', '{"process_guid": "%s"}')`, r.fractal, r.guid, r.guid))
	}
	// A log with no process must never match through an empty key.
	values = append(values, `('fa', toDateTime64('2026-11-05 00:00:00', 3, 'UTC'), 'none', '{"message": "x"}')`, `('fb', toDateTime64('2026-11-05 00:00:00', 3, 'UTC'), 'none', '{"message": "x"}')`)
	if err := conn.Exec(ctx, "INSERT INTO logs"+suffix+" (fractal_id, timestamp, log_id, fields) SETTINGS distributed_foreground_insert = 1 VALUES "+strings.Join(values, ", ")); err != nil {
		t.Fatal(err)
	}
}

// compareAncestry builds in steps and compares the ancestry and the filters with the reference.
func compareAncestry(t *testing.T, c *ClickHouseClient, rows []lineageRow, base time.Time, ancTable string, opts func(string) parser.QueryOptions) {
	t.Helper()
	ctx := context.Background()
	key := map[string]string{}
	krows, err := c.conn.Query(ctx, "SELECT fractal_id, process_guid, proc_key, parent_guid, parent_key FROM "+strings.Replace(ancTable, "proc_ancestry", "proc_lineage", 1))
	if err != nil {
		t.Fatal(err)
	}
	for krows.Next() {
		var f, g, k, pg, pk string
		if err := krows.Scan(&f, &g, &k, &pg, &pk); err != nil {
			t.Fatal(err)
		}
		key[f+"|"+g], key[f+"|"+pg] = k, pk
	}
	krows.Close()
	slice := time.Minute
	for _, step := range []time.Duration{20 * time.Minute, 50 * time.Minute, 2 * time.Hour} {
		hi := base.Add(step)
		if err := c.reflectProcAncestry(ctx, hi, &slice); err != nil {
			t.Fatalf("build to %s: %v", hi, err)
		}
		wantChains, wantOpen := walkAncestry(rows, hi, key)
		gotChains, gotOpen := readAncestry(t, c, ancTable, key)
		bad := 0
		for id, want := range wantChains {
			gotStop, hasStatus := gotOpen[id]
			if got := gotChains[id]; !hasStatus || fmt.Sprint(got) != fmt.Sprint(want) || gotStop != wantOpen[id] {
				if bad++; bad <= 5 {
					t.Errorf("step %s, %s:\n got  %v open %q\n want %v open %q", step, id, got, gotOpen[id], want, wantOpen[id])
				}
			}
		}
		for id := range gotOpen {
			if _, ok := wantChains[id]; !ok {
				if bad++; bad <= 5 {
					t.Errorf("step %s: %s built before its lineage was in", step, id)
				}
			}
		}
		t.Logf("step %s: %d processes, %d wrong, slice now %s", step, len(wantChains), bad, slice)
		checkPgrFilters(t, c, rows, step, opts)
	}
}

// checkPgrFilters runs the ancestor and descendant filters as translated for a query and
// compares the processes they keep with a walk of all lineage, built or not.
func checkPgrFilters(t *testing.T, c *ClickHouseClient, rows []lineageRow, step time.Duration, opts func(fractal string) parser.QueryOptions) {
	t.Helper()
	all := map[string]lineageRow{}
	for _, r := range rows {
		all[r.fractal+"|"+r.guid] = r
	}
	// chain lists a process's ancestors' guids and the image each was logged with, nearest first.
	chain := func(r lineageRow) (guids, images []string) {
		for d := 1; d <= procAncestryDepth && (r.parent != "" || r.parentImage != ""); d++ {
			guids, images = append(guids, r.parent), append(images, r.parentImage)
			if r.parent == "" {
				break
			}
			next, ok := all[r.fractal+"|"+r.parent]
			if !ok {
				break
			}
			r = next
		}
		return guids, images
	}
	matches := func(pattern, image string) bool {
		if strings.HasPrefix(pattern, "/") {
			return regexp.MustCompile("(?i)" + strings.Trim(pattern, "/")).MatchString(image)
		}
		return strings.HasSuffix(strings.ToLower(image), `\`+strings.ToLower(pattern))
	}
	for _, fractal := range []string{"fa", "fb"} {
		for _, tc := range []struct {
			descendant bool
			value      string
			depth      int
		}{{false, "p3.exe", 1}, {false, "p3.exe", 3}, {false, "/p1[0-9]/", 5}, {false, "p7.exe", 8}, {true, "p5.exe", 1}, {true, "p5.exe", 4}, {true, "/p2[0-2]/", 8}} {
			want := map[string]bool{}
			for _, r := range rows {
				if r.fractal != fractal {
					continue
				}
				guids, images := chain(r)
				for d := 0; d < len(guids) && d < tc.depth; d++ {
					if !tc.descendant && matches(tc.value, images[d]) {
						want[r.guid] = true
					}
					if tc.descendant && guids[d] != "" && matches(tc.value, r.image) {
						want[guids[d]] = true
					}
				}
			}
			name := "pgrHasAncestor"
			if tc.descendant {
				name = "pgrHasDescendant"
			}
			value := tc.value
			if !strings.HasPrefix(value, "/") {
				value = `"` + value + `"`
			}
			for _, negate := range []bool{false, true} {
				q := fmt.Sprintf("%s(%s, depth=%d)", name, value, tc.depth)
				if negate {
					q = "NOT " + q
				}
				got := runFilter(t, c, opts(fractal), q+" | table(process_guid)")
				if got[""] != negate {
					t.Errorf("step %s, %s, %s: a log with no process kept=%v", step, fractal, q, got[""])
				}
				for _, r := range rows {
					if r.fractal == fractal && got[r.guid] != (want[r.guid] != negate) {
						t.Errorf("step %s, %s, %s: %s kept=%v", step, fractal, q, r.guid, got[r.guid])
						break
					}
				}
			}
		}
	}
}

func runFilter(t *testing.T, c *ClickHouseClient, o parser.QueryOptions, q string) map[string]bool {
	t.Helper()
	pl, err := parser.ParseQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	o.MaxRows, o.StartTime, o.EndTime = 100000, time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	res, err := parser.TranslateToSQLWithOrder(pl, o)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	out := map[string]bool{}
	rs, err := c.conn.Query(context.Background(), res.SQL)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rs.Close()
	cols := rs.Columns()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for rs.Next() {
		for i := range vals {
			var s string
			vals[i] = &s
			ptrs[i] = vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for i, col := range cols {
			if col == "process_guid" {
				out[*vals[i].(*string)] = true
			}
		}
	}
	return out
}

// readAncestry returns proc_ancestry as built, by fractal and GUID.
func readAncestry(t *testing.T, c *ClickHouseClient, table string, key map[string]string) (map[string][]ancRow, map[string]string) {
	t.Helper()
	byKey := map[string]string{}
	for id, k := range key {
		byKey[strings.SplitN(id, "|", 2)[0]+"|"+k] = id
	}
	rows, err := c.conn.Query(context.Background(), "SELECT fractal_id, proc_key, depth, argMax(anc_key, version), argMax(anc_image, version), argMax(open_key, version) "+
		"FROM "+table+" GROUP BY fractal_id, proc_key, depth ORDER BY fractal_id, proc_key, depth")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	chains, open := map[string][]ancRow{}, map[string]string{}
	for rows.Next() {
		var f, k, ak, ai, ok string
		var d uint8
		if err := rows.Scan(&f, &k, &d, &ak, &ai, &ok); err != nil {
			t.Fatal(err)
		}
		id := byKey[f+"|"+k]
		if d == 0 {
			open[id] = ok
			if _, seen := chains[id]; !seen {
				chains[id] = nil // a process with no ancestors still has a chain, an empty one
			}
			continue
		}
		chains[id] = append(chains[id], ancRow{int(d), ak, ai})
	}
	for id := range chains {
		sort.Slice(chains[id], func(i, j int) bool { return chains[id][i].depth < chains[id][j].depth })
	}
	return chains, open
}

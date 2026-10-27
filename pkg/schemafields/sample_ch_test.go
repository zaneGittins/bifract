//go:build normlogesc

// Proves the schema sample spans its window at bounded cost, against a real
// server, on a table laid out like logs.
//
//	go test -tags normlogesc ./pkg/schemafields/ -run TestSample -v
//
// Override the DSN with BIFRACT_TEST_CH_DSN.
package schemafields

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"bifract/pkg/parser"
)

const sampleTestTable = "schema_sample_e2e"

const (
	sampleTestParts   = 16
	sampleTestPartLen = 20000
	sampleTestWant    = 16000
)

// seedSample writes one part per half hour of an 8 hour window: source a for the
// first half, source b for the second, in ingest order as real inserts arrive.
func seedSample(t *testing.T, conn clickhouse.Conn, anchor time.Time, window time.Duration) {
	t.Helper()
	ctx := context.Background()
	exec := func(q string) {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("exec: %v\n%s", err, q)
		}
	}
	exec("DROP TABLE IF EXISTS " + sampleTestTable)
	exec(fmt.Sprintf(`CREATE TABLE %s (
		fractal_id String,
		timestamp DateTime64(3),
		ingest_timestamp DateTime64(3),
		log_id String,
		norm_log String
	) ENGINE = MergeTree() PARTITION BY (fractal_id, toDate(ingest_timestamp)) ORDER BY (timestamp, log_id)`, sampleTestTable))
	exec("SYSTEM STOP MERGES " + sampleTestTable)

	from := anchor.Add(-window)
	step := window / sampleTestParts
	for i := 0; i < sampleTestParts; i++ {
		src := "a"
		if i >= sampleTestParts/2 {
			src = "b"
		}
		start := from.Add(step * time.Duration(i))
		exec(fmt.Sprintf(`INSERT INTO %s SELECT
			'f',
			toDateTime64('%s', 3, 'UTC') + number / 100,
			toDateTime64('%s', 3, 'UTC') + number / 100,
			toString(%d + number),
			'{"src":"%s","%s_field":"v' || toString(number %% 7) || '"}'
			FROM numbers(%d)`,
			sampleTestTable, start.Format("2006-01-02 15:04:05"), start.Format("2006-01-02 15:04:05"),
			i*sampleTestPartLen, src, src, sampleTestPartLen))
	}
}

type sampleRun struct {
	fields   map[string]*fieldSample
	readRows uint64
	memory   uint64
}

// runSample executes a sample under the sweep's own thread setting and reports
// what it returned and what it cost.
func runSample(t *testing.T, conn clickhouse.Conn, sql string) sampleRun {
	t.Helper()
	tag := fmt.Sprintf("schema-sample-%d", time.Now().UnixNano())
	rows, err := conn.Query(context.Background(),
		sql+fmt.Sprintf("\nSETTINGS max_threads = 1, log_comment = '%s'", tag))
	if err != nil {
		t.Fatalf("sample: %v\n%s", err, sql)
	}
	out := sampleRun{fields: map[string]*fieldSample{}}
	for rows.Next() {
		var (
			key                  string
			present, card        uint64
			vals                 []string
			counts, errs, hashes []uint64
		)
		if err := rows.Scan(&key, &present, &card, &vals, &counts, &errs, &hashes); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out.fields[key] = &fieldSample{Present: present, Cardinality: card, distinct: hashes}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("sample rows: %v", err)
	}
	rows.Close()

	// FLUSH LOGS can return before the row is readable, so poll briefly.
	var err2 error
	for i := 0; i < 50; i++ {
		if err := conn.Exec(context.Background(), "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatalf("flush logs: %v", err)
		}
		err2 = conn.QueryRow(context.Background(), fmt.Sprintf(
			"SELECT read_rows, memory_usage FROM system.query_log WHERE log_comment = '%s' AND type = 'QueryFinish'", tag)).
			Scan(&out.readRows, &out.memory)
		if err2 == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err2 != nil {
		var types []string
		r, _ := conn.Query(context.Background(), fmt.Sprintf("SELECT toString(type) FROM system.query_log WHERE log_comment = '%s'", tag))
		for r != nil && r.Next() {
			var ty string
			_ = r.Scan(&ty)
			types = append(types, ty)
		}
		t.Fatalf("query_log %s: %v (entries %v)\n%s", tag, err2, types, sql[len(sql)-200:])
	}
	return out
}

// TestSampleSpansTheWindow is the property the strata exist for. A single
// unsorted LIMIT under max_threads=1 fills from the oldest parts of the window,
// so a source ingested later in the window is missed. The strata must see both
// sources, and since they run one at a time, no slice may cost more memory than
// the single scan did.
func TestSampleSpansTheWindow(t *testing.T) {
	conn := ovfConn(t)
	defer conn.Close()
	anchor := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	window := 8 * time.Hour
	seedSample(t, conn, anchor, window)
	defer conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+sampleTestTable)

	single := runSample(t, conn, parser.BuildFieldSampleSQL(parser.FieldSampleParams{
		Table: sampleTestTable, Where: windowPredicate("f", anchor, window), SampleSize: sampleTestWant,
	}))

	var (
		slices         []map[string]*fieldSample
		readRows, peak uint64
	)
	for _, sql := range sliceSampleSQL(sampleTestTable, "f", anchor, window, sampleTestWant) {
		r := runSample(t, conn, sql)
		slices = append(slices, r.fields)
		readRows += r.readRows
		peak = max(peak, r.memory)
	}
	merged := mergeSlices(slices, sampleSliceTopN, sampleTopN, sampleMaxFields)
	t.Logf("single: read_rows=%d memory=%d b_field=%v", single.readRows, single.memory, single.fields["b_field"] != nil)
	t.Logf("strata: read_rows=%d peak memory=%d", readRows, peak)

	if merged["a_field"] == nil || merged["b_field"] == nil {
		t.Errorf("strata must see both sources: %v", merged)
	}
	if peak > single.memory {
		t.Errorf("a slice peaked at %d bytes, above the single scan's %d", peak, single.memory)
	}
	if readRows > single.readRows*sampleStrata {
		t.Errorf("strata read %d rows, want at most %d (single scan %d, window %d)",
			readRows, single.readRows*sampleStrata, single.readRows, sampleTestParts*sampleTestPartLen)
	}
	if single.fields["b_field"] != nil {
		t.Logf("single LIMIT now reaches the later source; the bias this guards against may be gone")
	}

	// The probe is the coverage denominator, so it must count what the strata read.
	var n uint64
	if err := conn.QueryRow(context.Background(), probeSQL(sampleTestTable, "f", anchor, window, sampleTestWant)).Scan(&n); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got := merged["src"].Present; got != n {
		t.Errorf("probe counted %d rows, strata read %d", n, got)
	}
	if c := merged["a_field"].Cardinality; c != 7 {
		t.Errorf("a_field has 7 distinct values across slices, merged %d", c)
	}
}

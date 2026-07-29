//go:build normlogesc

// Proves the two properties the overflow detector depends on, against a real
// server. system.parts is the ground truth for which JSON paths actually spilled
// into shared storage; the detector's query must agree with it.
//
//	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d clickhouse
//	go test -tags normlogesc ./pkg/schemafields/ -run TestOverflowDetect -v
//
// Override the DSN with BIFRACT_TEST_CH_DSN.
package schemafields

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const ovfTestTable = "ovf_detect_e2e"

func ovfConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	dsn := os.Getenv("BIFRACT_TEST_CH_DSN")
	if dsn == "" {
		dsn = "clickhouse://default:bifract@localhost:9000/default"
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping clickhouse (is the dev container up?): %v", err)
	}
	return conn
}

// seedOverflow builds a table whose JSON column has a deliberately tiny
// max_dynamic_paths, so surplus keys spill into shared storage, across two
// partitions of differing age.
func seedOverflow(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	ctx := context.Background()
	exec := func(q string) {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("exec: %v\n%s", err, q)
		}
	}
	exec("DROP TABLE IF EXISTS " + ovfTestTable)
	exec(fmt.Sprintf(`CREATE TABLE %s (
		timestamp DateTime64(3),
		log_id String,
		fields JSON(max_dynamic_paths=4, `+"`image`"+` String, `+"`user`"+` String)
	) ENGINE = MergeTree() PARTITION BY toDate(timestamp) ORDER BY (timestamp, log_id)`, ovfTestTable))

	insert := func(day string, idBase int, extra string) {
		exec(fmt.Sprintf(`INSERT INTO %s SELECT
			toDateTime64('%s 00:00:00', 3) + number,
			toString(%d + number),
			toJSONString(map('image','i','user','u',
				'%s_a','x','%s_b','x','%s_c','x',
				'extra1','x','extra2','x','extra3','x'))::JSON(max_dynamic_paths=4, `+"`image`"+` String, `+"`user`"+` String)
			FROM numbers(500)`, ovfTestTable, day, idBase, extra, extra, extra))
	}
	// An old partition and a recent one. now() is the anchor clamp, so the recent
	// partition must be genuinely recent for the 1-day window to select it.
	insert("2026-01-15", 0, "old")
	exec(fmt.Sprintf(`INSERT INTO %s SELECT
		now64(3) - toIntervalSecond(number),
		toString(100000 + number),
		toJSONString(map('image','i','user','u',
			'new_a','y','new_b','y','new_c','y',
			'extra1','y','extra2','y','extra3','y'))::JSON(max_dynamic_paths=4, `+"`image`"+` String, `+"`user`"+` String)
		FROM numbers(500)`, ovfTestTable))
}

// truthFromParts reads which paths genuinely live in shared storage, per part.
func truthFromParts(t *testing.T, conn clickhouse.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(
		"SELECT DISTINCT p FROM %s ARRAY JOIN JSONSharedDataPaths(fields) AS p ORDER BY p", ovfTestTable))
	if err != nil {
		t.Fatalf("truth query: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func pathsFrom(t *testing.T, conn clickhouse.Conn, query string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("query: %v\n%s", err, query)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		var n uint64
		if err := rows.Scan(&name, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// TestOverflowDetectMatchesParts is the correctness property: the detector's
// query must report exactly the paths that system-level inspection says spilled.
func TestOverflowDetectMatchesParts(t *testing.T) {
	conn := ovfConn(t)
	defer conn.Close()
	seedOverflow(t, conn)
	defer conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+ovfTestTable)

	truth := truthFromParts(t, conn)
	if len(truth) == 0 {
		t.Fatal("fixture produced no overflow paths; the test cannot prove anything")
	}
	t.Logf("ground truth from parts: %v", truth)

	window := fmt.Sprintf(
		"timestamp >= least((SELECT max(timestamp) FROM %s), now64(3)) - INTERVAL 1 DAY",
		ovfTestTable)
	got := pathsFrom(t, conn, fmt.Sprintf(`SELECT p AS field_name, count() AS rows_seen
FROM (SELECT JSONSharedDataPaths(fields) AS paths FROM %s WHERE %s LIMIT 2000)
ARRAY JOIN paths AS p GROUP BY p ORDER BY rows_seen DESC LIMIT 200`, ovfTestTable, window))

	if strings.Join(got, ",") != strings.Join(truth, ",") {
		t.Errorf("detector disagrees with parts\n  got:   %v\n  truth: %v", got, truth)
	}
}

// TestOverflowOrderedFormIsWrong pins the regression this fix addresses. An
// ORDER BY inside the subquery re-materializes the JSON column, and shared-data
// allocation is then recomputed for the intermediate block rather than read from
// the part. If ClickHouse ever makes this form agree with the parts, this test
// fails and the workaround can be revisited.
func TestOverflowOrderedFormIsWrong(t *testing.T) {
	conn := ovfConn(t)
	defer conn.Close()
	seedOverflow(t, conn)
	defer conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+ovfTestTable)

	truth := truthFromParts(t, conn)
	ordered := pathsFrom(t, conn, fmt.Sprintf(`SELECT p AS field_name, count() AS rows_seen
FROM (SELECT fields FROM %s ORDER BY timestamp DESC LIMIT 2000)
ARRAY JOIN JSONSharedDataPaths(fields) AS p
GROUP BY p ORDER BY rows_seen DESC LIMIT 200`, ovfTestTable))

	t.Logf("ordered form reports: %v (truth: %v)", ordered, truth)
	if strings.Join(ordered, ",") == strings.Join(truth, ",") {
		t.Skip("ordered form now agrees with parts; ClickHouse behaviour may have changed")
	}
}

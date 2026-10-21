//go:build chmodels

// Runs the model SQL builders against a real ClickHouse, each test in its own
// scratch database:
//
//	go test -tags chmodels ./pkg/models/ -run CH -v
//
// BIFRACT_TEST_CLICKHOUSE (default localhost:9000) and
// BIFRACT_TEST_CLICKHOUSE_PASSWORD (default bifract) point at the server.
package models

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"bifract/pkg/storage"
)

type chDB struct {
	t *testing.T
	c *storage.ClickHouseClient
}

func newCHDB(t *testing.T) *chDB {
	t.Helper()
	addr := os.Getenv("BIFRACT_TEST_CLICKHOUSE")
	if addr == "" {
		addr = "localhost:9000"
	}
	pass, ok := os.LookupEnv("BIFRACT_TEST_CLICKHOUSE_PASSWORD")
	if !ok {
		pass = "bifract"
	}
	connect := func(db string) *storage.ClickHouseClient {
		c, err := storage.NewClickHouseClient(storage.ClientOptions{Conn: storage.ConnOptions{
			Addrs: storage.HostAddrs([]string{addr}, 9000), Database: db, User: "default", Password: pass,
		}})
		if err != nil {
			t.Fatalf("connect %s: %v", addr, err)
		}
		return c
	}
	admin := connect("default")
	db := fmt.Sprintf("models_sqltest_%d", time.Now().UnixNano())
	if err := admin.Conn().Exec(context.Background(), "CREATE DATABASE "+db); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.Conn().Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
		admin.Close()
	})
	d := &chDB{t: t, c: connect(db)}
	t.Cleanup(func() { _ = d.c.Close() })
	d.exec(`CREATE TABLE logs (
    fractal_id LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    ingest_timestamp DateTime64(3, 'UTC'),
    fields JSON
) ENGINE = MergeTree ORDER BY timestamp`)
	return d
}

func (d *chDB) exec(stmts ...string) {
	d.t.Helper()
	for _, s := range stmts {
		if err := d.c.Conn().Exec(context.Background(), s); err != nil {
			d.t.Fatalf("exec: %v\n%s", err, s)
		}
	}
}

func (d *chDB) rows(q string) []map[string]interface{} {
	d.t.Helper()
	rows, err := d.c.Query(context.Background(), q)
	if err != nil {
		d.t.Fatalf("query: %v\n%s", err, q)
	}
	return rows
}

// connLogs inserts one connection log per (offset back from now, src, dst).
func (d *chDB) connLogs(src, dst string, every time.Duration, n int) {
	d.t.Helper()
	d.exec(fmt.Sprintf(`INSERT INTO logs SELECT 'f1', now64(3, 'UTC') - toIntervalSecond(number * %d + 30), now64(3, 'UTC'),
    '{"src_ip":"%s","dst_ip":"%s","dst_port":"443","orig_bytes":"' || toString(number %% 997) || '","duration":"1"}'
FROM numbers(%d)`, int(every.Seconds()), src, dst, n))
}

// A busy pair keeps the daily cap on every day of the window, timestamps and
// sizes stay paired, and pairs outside the gate never reach the scorer.
func TestCHBeaconReadKeepsEveryDay(t *testing.T) {
	d := newCHDB(t)
	d.connLogs("10.0.0.1", "198.51.100.1", 30*time.Second, 7*2880) // 2880 a day, over the cap
	d.connLogs("10.0.0.2", "198.51.100.2", 2*time.Second, 7*43200) // a strobe
	d.connLogs("10.0.0.3", "198.51.100.3", time.Hour, 2)           // too few
	def := ModelDefinition{Beacon: &BeaconParams{StrobeLimit: 100000}}
	ins, err := BuildNetStateInsert(def, "s", "logs", "", "f1")
	if err != nil {
		t.Fatal(err)
	}
	d.exec(BuildNetStateTableDDL("s", 7), ins)

	rows := d.rows(BuildNetScoreReadQuery(def, ModelTypeBeacon, "s", "f1", 7) + scorerReadSettings())
	if len(rows) != 1 || getString(rows[0], "src") != "10.0.0.1" {
		t.Fatalf("gated pairs: %v", rows)
	}
	p := pairFromRow(rows[0], time.Now().Add(-8*24*time.Hour).Unix())
	if len(p.TsList) < 7*netStateArrayCap || len(p.TsList) > 8*netStateArrayCap {
		t.Errorf("%d points, want the daily cap on each of 7-8 days", len(p.TsList))
	}
	if span := p.LastTs - p.FirstTs; span < 6*86400 {
		t.Errorf("points span %v, want the whole window", time.Duration(span)*time.Second)
	}
	if len(p.SizeList) != len(p.TsList) {
		t.Errorf("%d sizes for %d timestamps", len(p.SizeList), len(p.TsList))
	}
}

// A preview longer than the longest window keeps each pair inside the point
// budget, counting the extra calendar day a range off midnight touches.
func TestCHBeaconPreviewStaysInBudget(t *testing.T) {
	d := newCHDB(t)
	d.connLogs("10.0.0.1", "198.51.100.1", 30*time.Second, 31*2880)
	end := time.Now().UTC()
	b := NetPreviewBounds{Start: end.Add(-30 * 24 * time.Hour), End: end, Days: 30}
	q, err := BuildNetPreviewAgg(ModelDefinition{}, ModelTypeBeacon, "logs", "f1", b)
	if err != nil {
		t.Fatal(err)
	}
	rows := d.rows(q)
	if len(rows) != 1 {
		t.Fatalf("pairs: %v", rows)
	}
	if n := len(getInt64Slice(rows[0], "ts_list")); n > netPairPointBudget || n < netPairPointBudget*9/10 {
		t.Errorf("%d points, want close to and within the %d budget", n, netPairPointBudget)
	}
	if !strings.Contains(q, "HAVING") {
		t.Error("preview is not gated")
	}
}

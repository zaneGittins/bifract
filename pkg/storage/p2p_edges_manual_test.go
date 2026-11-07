//go:build chancestry

package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
)

// Injection and handle-access edges logged before the rollup began are read from logs, and later
// ones from the rollup, so pgr keeps both across the upgrade that moved them.
func TestP2PEdgesKeepHistory(t *testing.T) {
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
	for _, table := range []string{"logs", "proc_lineage", "process_edges", provenanceMarksTable} {
		ddl, err := initTableDDL(table)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	event := func(n int, category, target string) string {
		ingest := "2026-11-05 00:30:00"
		if n > 2 {
			ingest = "2026-11-05 02:00:00"
		}
		return fmt.Sprintf(`('f1', toDateTime64('2026-11-05 00:00:%02d', 3, 'UTC'), toDateTime64('`+ingest+`', 3, 'UTC'), 'l%d', '{"bifract_category": "%s", "source_process_guid": "src", "target_process_guid": "%s", "image": "C:\\\\a.exe", "target_image": "C:\\\\%s.exe", "computer_name": "WS1"}')`, n, n, category, target, target)
	}
	insert := func(rows ...string) {
		if err := conn.Exec(ctx, "INSERT INTO logs (fractal_id, timestamp, ingest_timestamp, log_id, fields) VALUES "+strings.Join(rows, ", ")); err != nil {
			t.Fatal(err)
		}
	}
	insert(event(1, "remote_thread", "old1"), event(2, "process_access", "old2"))
	mv, err := initStatement("CREATE MATERIALIZED VIEW IF NOT EXISTS process_edges_p2p_mv")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, mv); err != nil {
		t.Fatal(err)
	}
	// The latest mark wins, as a toggle off and back on leaves it.
	for _, at := range []string{p2pMarkNever, "toDateTime64('2026-11-05 01:00:00', 3, 'UTC')"} {
		if err := setP2PMark(ctx, conn, at); err != nil {
			t.Fatal(err)
		}
	}
	insert(event(3, "remote_thread", "new1"), event(4, "process_access", "new2"), event(5, "remote_thread", "new1"))

	c := &ClickHouseClient{conn: conn}
	start := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	opts := parser.QueryOptions{FractalID: "f1", StartTime: start, EndTime: start.Add(time.Hour)}
	tree := ExternalTable{Name: parser.ProvenanceTreeTable, Columns: []string{"guid", "node"}, Rows: [][]string{{"src", "src"}}}
	types := map[string]bool{"remote_thread": true, "process_access": true}
	keys, err := c.QueryProvenance(ctx, parser.BuildProvenanceEdgeKeysSQL(types, opts), tree)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, k := range keys {
		got = append(got, fmt.Sprint(k["fkey_tgt"]))
	}
	sort.Strings(got)
	if want := `[c:\new1.exe c:\new2.exe c:\old1.exe c:\old2.exe]`; fmt.Sprint(got) != want {
		t.Errorf("edge targets %v, want %s", got, want)
	}
	var rolled uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM process_edges WHERE event_type IN ('remote_thread', 'process_access')").Scan(&rolled); err != nil || rolled != 2 {
		t.Errorf("rollup rows %d (%v), want one per new edge: only events after the mark, merged by edge", rolled, err)
	}
	peers, err := c.QueryProvenance(ctx, parser.BuildReconnectionSQL(parser.ProvenanceParams{Reconnect: true, EdgeTypes: types}, 1, 1, opts), tree)
	if err != nil {
		t.Fatal(err)
	}
	var peerIDs []string
	for _, p := range peers {
		peerIDs = append(peerIDs, fmt.Sprint(p["recon_type"], ":", p["peer_guid"]))
	}
	sort.Strings(peerIDs)
	if want := "[process_access:new2 process_access:old2 remote_thread:new1 remote_thread:old1]"; fmt.Sprint(peerIDs) != want {
		t.Errorf("reconnection peers %v, want %s", peerIDs, want)
	}
}

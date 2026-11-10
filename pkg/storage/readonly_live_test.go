//go:build chlive

// A user read that has no query identity runs on the read-only fallback. It needs a
// reachable ClickHouse:
//
//	BIFRACT_CH_ADDR=localhost:9000 BIFRACT_CH_PASSWORD= go test -tags chlive ./pkg/storage -run ReadOnly -v
package storage

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestLiveReadOnlyFallbackCannotWrite(t *testing.T) {
	addr := os.Getenv("BIFRACT_CH_ADDR")
	if addr == "" {
		t.Skip("BIFRACT_CH_ADDR not set")
	}
	c, err := NewClickHouseClient(ClientOptions{
		Conn: ConnOptions{Addrs: []string{addr}, Database: "default", User: "default", Password: os.Getenv("BIFRACT_CH_PASSWORD")},
		Role: RoleIngest,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()
	// No identities are provisioned, so every class falls back.
	ctx := UncappedReadContext(context.Background())
	if _, err := c.Query(ctx, "SELECT 1 SETTINGS max_threads = 2"); err != nil {
		t.Fatalf("a read with its own settings must run: %v", err)
	}
	defer c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS default.bifract_readonly_probe")
	for _, stmt := range []string{
		"CREATE TABLE default.bifract_readonly_probe (a UInt8) ENGINE = Memory",
		"CREATE TABLE default.bifract_readonly_probe (a UInt8) ENGINE = Memory SETTINGS readonly = 0",
		"SELECT 1 SETTINGS readonly = 0",
	} {
		_, err := c.Query(ctx, stmt)
		if err == nil || !strings.Contains(err.Error(), "readonly") {
			t.Errorf("%s: want a readonly refusal, got %v", stmt, err)
		}
	}
	// A write on the connection a user read is routed to is refused too.
	if err := c.conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS default.bifract_readonly_probe (a UInt8) ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	conn, err := c.routeRead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "INSERT INTO default.bifract_readonly_probe VALUES (1)"); err == nil || !strings.Contains(err.Error(), "readonly") {
		t.Errorf("insert: want a readonly refusal, got %v", err)
	}
	// An admin read still runs on the default connection.
	if _, err := c.Query(AdminReadContext(context.Background()), "SELECT 1"); err != nil {
		t.Fatalf("admin read: %v", err)
	}
}

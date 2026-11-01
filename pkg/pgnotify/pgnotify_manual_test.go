//go:build pgnotify

// Sends a notice through a real Postgres from one bus to another, as between
// two Bifract processes.
//
//	docker run -d --name bfnotify -e POSTGRES_PASSWORD=bifract -p 15432:5432 postgres:16
//	go test -tags pgnotify ./pkg/pgnotify/ -v
package pgnotify

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestNoticeReachesAnotherProcess(t *testing.T) {
	dsn := os.Getenv("BIFRACT_TEST_PG_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:bifract@localhost:15432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	receiver := New(db)
	got := make(chan string, 4)
	receiver.Subscribe(TopicIngestTokens, func() { got <- TopicIngestTokens })
	receiver.Subscribe(TopicSettings, func() { got <- TopicSettings })
	receiver.Listen(dsn)
	defer receiver.Close()
	waitForRefresh(t, got)

	New(db).Publish(TopicIngestTokens)
	select {
	case topic := <-got:
		if topic != TopicIngestTokens {
			t.Errorf("received %s, want %s", topic, TopicIngestTokens)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notice not received")
	}
	select {
	case topic := <-got:
		t.Errorf("unexpected extra notice %s", topic)
	case <-time.After(300 * time.Millisecond):
	}
}

// When the listening connection drops, notices may be lost, so every
// subscriber runs once it is back.
func TestReconnectRefreshesEverySubscriber(t *testing.T) {
	dsn := os.Getenv("BIFRACT_TEST_PG_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:bifract@localhost:15432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := New(db)
	got := make(chan string, 4)
	b.Subscribe(TopicIngestTokens, func() { got <- TopicIngestTokens })
	b.Subscribe(TopicSettings, func() { got <- TopicSettings })
	b.Listen(dsn)
	defer b.Close()
	waitForRefresh(t, got)
	if _, err := db.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	waitForRefresh(t, got)
}

// waitForRefresh waits for both subscribers to run, as they do once the
// listener connects or reconnects.
func waitForRefresh(t *testing.T, got chan string) {
	t.Helper()
	seen := map[string]bool{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 2 {
		select {
		case topic := <-got:
			seen[topic] = true
		case <-deadline:
			t.Fatalf("only %v refreshed", seen)
		}
	}
}

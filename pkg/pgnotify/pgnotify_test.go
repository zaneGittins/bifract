package pgnotify

import (
	"testing"
	"time"

	"bifract/internal/fakedb"

	"github.com/lib/pq"
)

func TestDispatchRoutesByTopic(t *testing.T) {
	b := New(nil)
	got := map[string]int{}
	b.Subscribe(TopicIngestTokens, func() { got[TopicIngestTokens]++ })
	b.Subscribe(TopicSettings, func() { got[TopicSettings]++ })

	b.dispatch(&pq.Notification{Channel: channel, Extra: TopicIngestTokens})
	b.dispatch(&pq.Notification{Channel: channel, Extra: "something_else"})
	if got[TopicIngestTokens] != 1 || got[TopicSettings] != 0 {
		t.Errorf("after an ingest token notice: %v", got)
	}
}

// A reconnect may have missed notices, so every subscriber refreshes.
func TestDispatchReconnectRunsEverySubscriber(t *testing.T) {
	b := New(nil)
	got := map[string]int{}
	b.Subscribe(TopicIngestTokens, func() { got[TopicIngestTokens]++ })
	b.Subscribe(TopicSettings, func() { got[TopicSettings]++ })

	b.dispatch(nil)
	if got[TopicIngestTokens] != 1 || got[TopicSettings] != 1 {
		t.Errorf("after a reconnect: %v", got)
	}
}

func TestPublishNotifiesTheChannel(t *testing.T) {
	pg := fakedb.NewPG()
	pg.On("pg_notify", fakedb.Row(""))
	New(pg.DB()).Publish(TopicSettings)

	calls := pg.Matching("pg_notify")
	if len(calls) != 1 {
		t.Fatalf("%d pg_notify calls, want 1", len(calls))
	}
	if args := calls[0].Args; len(args) != 2 || args[0] != channel || args[1] != TopicSettings {
		t.Errorf("pg_notify args = %v", args)
	}
}

// A bus with no database, as in single-process tests, publishes nothing and
// does not panic.
func TestPublishWithoutDatabase(t *testing.T) {
	var b *Bus
	b.Publish(TopicSettings)
	New(nil).Publish(TopicSettings)
}

// Startup must not wait on the listener: with Postgres unreachable, Listen
// returns at once and pq keeps retrying in the background.
func TestListenDoesNotBlockWhenUnreachable(t *testing.T) {
	b := New(nil)
	done := make(chan struct{})
	go func() {
		b.Listen("postgres://bifract@127.0.0.1:1/bifract?sslmode=disable&connect_timeout=1")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Listen blocked on an unreachable database")
	}
	b.Close()
}

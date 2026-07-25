package sse

import (
	"sync"
	"testing"
	"time"
)

type fakePublisher struct {
	mu       sync.Mutex
	rooms    []string
	payloads [][]byte
	excludes []string
}

func (f *fakePublisher) Publish(room string, payload []byte, excludeClientID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rooms = append(f.rooms, room)
	f.payloads = append(f.payloads, payload)
	f.excludes = append(f.excludes, excludeClientID)
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rooms)
}

func recv(t *testing.T, c *Client) []byte {
	t.Helper()
	select {
	case data := <-c.Send:
		return data
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

func TestBroadcastDeliversLocallyAndPublishes(t *testing.T) {
	hub := NewHub()
	pub := &fakePublisher{}
	hub.SetRelay(pub)

	c := hub.Register("notebook:1", ClientInfo{Username: "a"})
	hub.Broadcast("notebook:1", Event{Type: SectionUpdated, Data: "x"}, "")

	if got := string(recv(t, c)); got == "" {
		t.Fatal("local client received no data")
	}
	if pub.count() != 1 {
		t.Fatalf("relay publish count = %d, want 1", pub.count())
	}
	if pub.rooms[0] != "notebook:1" {
		t.Fatalf("relay room = %q, want notebook:1", pub.rooms[0])
	}
}

// A replica with no local clients in a room must still publish: the room's
// members may all be connected to a different replica.
func TestBroadcastPublishesWithNoLocalClients(t *testing.T) {
	hub := NewHub()
	pub := &fakePublisher{}
	hub.SetRelay(pub)

	hub.Broadcast("dashboard:99", Event{Type: WidgetUpdated, Data: "x"}, "")

	if pub.count() != 1 {
		t.Fatalf("relay publish count = %d, want 1 (no local clients must not skip the relay)", pub.count())
	}
}

// Events arriving from another replica must not be re-published, or two replicas
// would bounce the same event back and forth forever.
func TestDeliverLocalDoesNotRepublish(t *testing.T) {
	hub := NewHub()
	pub := &fakePublisher{}
	hub.SetRelay(pub)

	c := hub.Register("notebook:1", ClientInfo{Username: "a"})
	hub.deliverLocal("notebook:1", FormatSSE(Event{Type: SectionUpdated, Data: "x"}), "")

	if got := string(recv(t, c)); got == "" {
		t.Fatal("local client received no data")
	}
	if pub.count() != 0 {
		t.Fatalf("relay publish count = %d, want 0", pub.count())
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	hub := NewHub()
	sender := hub.Register("notebook:1", ClientInfo{Username: "a"})
	other := hub.Register("notebook:1", ClientInfo{Username: "b"})

	hub.Broadcast("notebook:1", Event{Type: SectionUpdated, Data: "x"}, sender.ID)

	if got := string(recv(t, other)); got == "" {
		t.Fatal("other client received no data")
	}
	select {
	case data := <-sender.Send:
		t.Fatalf("excluded sender received data: %q", data)
	default:
	}
}

// No relay attached is the single-replica case and must keep working.
func TestBroadcastWithoutRelay(t *testing.T) {
	hub := NewHub()
	c := hub.Register("notebook:1", ClientInfo{Username: "a"})
	hub.Broadcast("notebook:1", Event{Type: SectionUpdated, Data: "x"}, "")
	if got := string(recv(t, c)); got == "" {
		t.Fatal("local client received no data")
	}
}

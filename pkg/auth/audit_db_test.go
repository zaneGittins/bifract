package auth

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/fractals"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// auditEvent is one auth event as written to the system fractal.
type auditEvent struct {
	fractal string
	fields  map[string]string
}

// newAuditedHandler wires a ClickHouse fake so auth events can be read back.
// Events are inserted from a goroutine: the logs batch is sent, then the
// logs_raw batch is prepared on the same goroutine, so the OnBatch hook for the
// latter can read the former without racing and hand it over on a channel.
func newAuditedHandler(t *testing.T) (*AuthHandler, *authDB, <-chan auditEvent) {
	t.Helper()
	db := newAuthDB(t)
	ch, chf := storagetest.ClickHouse()
	events := make(chan auditEvent, 32)
	chf.OnBatch = func(b *fakedb.Batch) {
		if !strings.Contains(b.Query, "logs_raw") {
			return
		}
		all := chf.Batches()
		for i := len(all) - 1; i >= 0; i-- {
			if strings.HasPrefix(all[i].Query, "INSERT INTO logs ") {
				for _, r := range all[i].Appended {
					events <- auditEvent{fractal: r[3].(string), fields: r[2].(map[string]string)}
				}
				return
			}
		}
	}
	h := NewAuthHandlerWithAPIKeys(db.client, ch, fractals.NewManager(db.client, ch), nil)
	return h, db, events
}

func collect(t *testing.T, events <-chan auditEvent, n int) []auditEvent {
	t.Helper()
	var got []auditEvent
	deadline := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case e := <-events:
			got = append(got, e)
		case <-deadline:
			t.Fatalf("got %d auth events, want %d: %v", len(got), n, got)
		}
	}
	return got
}

// Sign-in outcomes and admin actions on accounts land in the system fractal
// with who, from where, and why.
func TestAuthEventsAreAudited(t *testing.T) {
	t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
	h, db, events := newAuditedHandler(t)
	db.addUser("alice")

	// One action at a time, each waiting for its event, so a single insert
	// goroutine is in flight whenever the hook reads a batch.
	var got []auditEvent
	login(h, "alice", "wrong", "198.51.100.20")
	got = append(got, collect(t, events, 1)...)
	login(h, "alice", userPassword, "198.51.100.20")
	got = append(got, collect(t, events, 1)...)
	post(t, h.HandleAdminResetPassword, "/", `{"username":"alice"}`, func(r *http.Request) *http.Request {
		r.RemoteAddr = "192.0.2.1:5000"
		return withUser(r, &storage.User{Username: "root", IsAdmin: true})
	})
	got = append(got, collect(t, events, 1)...)
	sort.Slice(got, func(i, j int) bool { return fmt.Sprint(got[i].fields) < fmt.Sprint(got[j].fields) })
	want := []map[string]string{
		{"event": "login_failed", "user": "alice", "src_ip": "198.51.100.20", "detail": "invalid username or password"},
		{"event": "login_success", "user": "alice", "src_ip": "198.51.100.20"},
		{"event": "password_reset", "user": "alice", "src_ip": "192.0.2.1", "detail": "reset by admin root"},
	}
	sort.Slice(want, func(i, j int) bool { return fmt.Sprint(want[i]) < fmt.Sprint(want[j]) })
	for i := range want {
		if got[i].fractal != systemFractalID {
			t.Errorf("event %v written to fractal %q, want the system fractal", got[i].fields, got[i].fractal)
		}
		if fmt.Sprint(got[i].fields) != fmt.Sprint(want[i]) {
			t.Errorf("event %d = %v, want %v", i, got[i].fields, want[i])
		}
	}
}

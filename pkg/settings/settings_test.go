package settings

import (
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/clientip"
	"bifract/pkg/pgnotify"
	"bifract/pkg/storage/storagetest"
)

func TestClampRecallConcurrency(t *testing.T) {
	cases := []struct{ in, want int }{
		{-5, 1},
		{0, 1},
		{1, 1},
		{5, 5},
		{maxRecallConcurrency, maxRecallConcurrency},
		{maxRecallConcurrency + 1, maxRecallConcurrency},
		{1000, maxRecallConcurrency},
	}
	for _, c := range cases {
		if got := clampRecallConcurrency(c.in); got != c.want {
			t.Errorf("clampRecallConcurrency(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// readErr, when set, fails kvSettings' whole-table read.
var readErr error

// kvSettings backs settings.Init and Update with an in-memory settings table.
func kvSettings(t *testing.T, initial map[string]string) map[string]string {
	t.Helper()
	var mu sync.Mutex
	rows := map[string]string{}
	for k, v := range initial {
		rows[k] = v
	}
	client, pg := storagetest.Postgres()
	pg.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.Contains(q, "SELECT key, value FROM settings"):
			if readErr != nil {
				return fakedb.PGResult{Err: readErr}, true
			}
			var all [][]driver.Value
			for k, v := range rows {
				all = append(all, []driver.Value{k, v})
			}
			return fakedb.RowsOf(all...), true
		case strings.Contains(q, "SELECT value FROM settings"):
			if v, ok := rows[args[0].(string)]; ok {
				return fakedb.Row(v), true
			}
			return fakedb.PGResult{}, true
		case strings.Contains(q, "INSERT INTO settings"):
			rows[args[0].(string)] = args[1].(string)
			return fakedb.Affected(1), true
		}
		return fakedb.PGResult{}, false
	})
	if err := Init(client); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		globalSettings = nil
		if err := clientip.SetTrustedProxies(clientip.DefaultTrustedProxies); err != nil {
			t.Error(err)
		}
	})
	return rows
}

func peerWithXFF(peer, xff string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = peer + ":1"
	r.Header.Set("X-Forwarded-For", xff)
	return r
}

func TestTrustedProxiesSetting(t *testing.T) {
	t.Run("defaults when unset", func(t *testing.T) {
		kvSettings(t, nil)
		if got := Get().TrustedProxyCIDRs; strings.Join(got, ",") != strings.Join(clientip.DefaultTrustedProxies, ",") {
			t.Errorf("TrustedProxyCIDRs = %v, want the defaults", got)
		}
		if got := clientip.FromRequest(peerWithXFF("172.18.0.5", "198.51.100.4")); got != "198.51.100.4" {
			t.Errorf("bundled proxy not trusted by default: %q", got)
		}
	})

	t.Run("stored list is applied at startup", func(t *testing.T) {
		kvSettings(t, map[string]string{"trusted_proxy_cidrs": `["203.0.113.10/32"]`})
		if got := clientip.FromRequest(peerWithXFF("172.18.0.5", "198.51.100.4")); got != "172.18.0.5" {
			t.Errorf("peer outside the stored list trusted: %q", got)
		}
		if got := clientip.FromRequest(peerWithXFF("203.0.113.10", "198.51.100.4")); got != "198.51.100.4" {
			t.Errorf("stored proxy not trusted: %q", got)
		}
	})

	t.Run("stored empty list trusts nobody", func(t *testing.T) {
		kvSettings(t, map[string]string{"trusted_proxy_cidrs": `[]`})
		if got := clientip.FromRequest(peerWithXFF("127.0.0.1", "198.51.100.4")); got != "127.0.0.1" {
			t.Errorf("empty list honored a header: %q", got)
		}
	})

	t.Run("corrupt row keeps the defaults", func(t *testing.T) {
		kvSettings(t, map[string]string{"trusted_proxy_cidrs": `["not a range"]`})
		if got := Get().TrustedProxyCIDRs; strings.Join(got, ",") != strings.Join(clientip.DefaultTrustedProxies, ",") {
			t.Errorf("TrustedProxyCIDRs = %v, want the defaults", got)
		}
	})

	t.Run("admin update validates, persists and applies live", func(t *testing.T) {
		rows := kvSettings(t, nil)
		h := NewHandler(nil)
		post := func(body string) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			h.HandleUpdate(rec, httptest.NewRequest(http.MethodPost, "/api/v1/settings", strings.NewReader(body)))
			return rec
		}

		if rec := post(`{"trusted_proxy_cidrs":["10.0.0.0/8","bogus"]}`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "bogus") {
			t.Fatalf("invalid range: %d %s", rec.Code, rec.Body.String())
		}
		if _, saved := rows["trusted_proxy_cidrs"]; saved {
			t.Error("an invalid list was persisted")
		}

		if rec := post(`{"trusted_proxy_cidrs":[" 203.0.113.10 ","2001:db8::/32"]}`); rec.Code != http.StatusOK {
			t.Fatalf("valid list: %d %s", rec.Code, rec.Body.String())
		}
		if got := rows["trusted_proxy_cidrs"]; got != `["203.0.113.10/32","2001:db8::/32"]` {
			t.Errorf("persisted %s", got)
		}
		if got := clientip.FromRequest(peerWithXFF("172.18.0.5", "198.51.100.4")); got != "172.18.0.5" {
			t.Errorf("update not applied live: %q", got)
		}

		// A save from a control that does not send the field leaves it alone.
		if rec := post(`{"alert_eval_interval_seconds":120}`); rec.Code != http.StatusOK {
			t.Fatalf("unrelated save: %d %s", rec.Code, rec.Body.String())
		}
		if got := strings.Join(Get().TrustedProxyCIDRs, ","); got != "203.0.113.10/32,2001:db8::/32" {
			t.Errorf("unrelated save changed trusted proxies to %s", got)
		}
	})
}

// Settings saved by another process take effect here when it announces them,
// trusted proxies included, and a failed read keeps what is in force.
func TestReloadPicksUpAnotherProcessesSave(t *testing.T) {
	rows := kvSettings(t, map[string]string{"require_mfa": "false"})
	notices := fakedb.NewPG()
	notices.On("pg_notify", fakedb.Row(""))
	b := pgnotify.New(notices.DB())
	Watch(b)
	t.Cleanup(func() { bus = nil })

	rows["require_mfa"] = "true"
	rows["trusted_proxy_cidrs"] = `["203.0.113.10/32"]`
	b.Deliver(pgnotify.TopicSettings)
	if !Get().RequireMFA {
		t.Error("require_mfa saved elsewhere not picked up")
	}
	if got := clientip.FromRequest(peerWithXFF("203.0.113.10", "198.51.100.4")); got != "198.51.100.4" {
		t.Errorf("reloaded proxy not trusted: %q", got)
	}

	readErr = errors.New("pq: connection reset")
	t.Cleanup(func() { readErr = nil })
	rows["require_mfa"] = "false"
	b.Deliver(pgnotify.TopicSettings)
	if !Get().RequireMFA {
		t.Error("a failed reload changed the settings in force")
	}
}

// Saving announces the change so every other process reloads.
func TestUpdateAnnouncesTheSave(t *testing.T) {
	kvSettings(t, nil)
	notices := fakedb.NewPG()
	notices.On("pg_notify", fakedb.Row(""))
	Watch(pgnotify.New(notices.DB()))
	t.Cleanup(func() { bus = nil })

	s := Get()
	s.RequireMFA = false
	if err := Update(&s); err != nil {
		t.Fatal(err)
	}
	if n := len(notices.Matching("pg_notify")); n != 1 {
		t.Errorf("%d notices after a save, want 1", n)
	}
}

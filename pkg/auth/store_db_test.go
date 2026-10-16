package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bifract/internal/setup"
	"bifract/pkg/storage"

	"golang.org/x/crypto/bcrypt"
)

func TestPgSessionStoreRoundTrip(t *testing.T) {
	db := newAuthDB(t)
	store := newPgSessionStore(db.client.DB())
	created := time.Now().Add(-time.Minute).Truncate(time.Second)

	in := &Session{Username: "alice", CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour), SelectedFractal: opsFractalID, MFAPending: true}
	if err := store.Set("sid", in); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Get("sid")
	if err != nil || !ok {
		t.Fatalf("Get = (%v, %v, %v)", got, ok, err)
	}
	if got.Username != "alice" || !got.CreatedAt.Equal(created) || got.SelectedFractal != opsFractalID || got.SelectedPrism != "" || !got.MFAPending {
		t.Errorf("Get = %+v, want what was stored", got)
	}
	// An empty scope is stored as NULL, not as an empty id.
	if r := db.rows("sessions", row{"session_id": "sid"})[0]; r["selected_prism"] != nil {
		t.Errorf("empty prism stored as %#v", r["selected_prism"])
	}

	// Set on an existing id replaces the session but keeps when it began.
	in2 := &Session{Username: "alice", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(2 * time.Hour), SelectedPrism: opsPrismID}
	if err := store.Set("sid", in2); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.Get("sid")
	if got.MFAPending || got.SelectedFractal != "" || got.SelectedPrism != opsPrismID || !got.CreatedAt.Equal(created) {
		t.Errorf("after upsert = %+v", got)
	}
	if n := len(db.rows("sessions", nil)); n != 1 {
		t.Errorf("%d rows after an upsert", n)
	}
}

func TestPgSessionStoreGet(t *testing.T) {
	db := newAuthDB(t)
	store := newPgSessionStore(db.client.DB())
	db.newSession("expired", "alice", func(r row) { r["expires_at"] = time.Now().Add(-time.Second) })
	db.newSession("stale-fractal", "alice", onFractal(otherScopeID))
	db.newSession("stale-prism", "alice", onPrism(otherScopeID))

	if s, ok, err := store.Get("expired"); s != nil || ok || err != nil {
		t.Errorf("expired session: (%v, %v, %v), want not found", s, ok, err)
	}
	if s, ok, err := store.Get("unknown"); s != nil || ok || err != nil {
		t.Errorf("unknown session: (%v, %v, %v), want not found", s, ok, err)
	}
	// A scope deleted since it was selected reads as no scope, so callers fall
	// back to a default instead of pinning the session to a dangling id.
	if s, ok, _ := store.Get("stale-fractal"); !ok || s.SelectedFractal != "" {
		t.Errorf("stale fractal: %+v", s)
	}
	if s, ok, _ := store.Get("stale-prism"); !ok || s.SelectedPrism != "" {
		t.Errorf("stale prism: %+v", s)
	}

	db.failOn("FROM sessions s", errors.New("pq: too many clients"))
	if s, ok, err := store.Get("stale-fractal"); s != nil || ok || err == nil {
		t.Errorf("store failure: (%v, %v, %v), want an error", s, ok, err)
	}
}

func TestPgSessionStoreScopeUpdates(t *testing.T) {
	db := newAuthDB(t)
	store := newPgSessionStore(db.client.DB())
	db.newSession("sid", "alice", onPrism(opsPrismID))
	db.newSession("expired", "alice", func(r row) { r["expires_at"] = time.Now().Add(-time.Second) })

	// Selecting one kind of scope clears the other.
	if err := store.UpdateFractal("sid", labFractalID); err != nil {
		t.Fatal(err)
	}
	if r := db.rows("sessions", row{"session_id": "sid"})[0]; r["selected_fractal"] != labFractalID || r["selected_prism"] != nil {
		t.Errorf("after UpdateFractal: %v", r)
	}
	if err := store.UpdatePrism("sid", opsPrismID); err != nil {
		t.Fatal(err)
	}
	if r := db.rows("sessions", row{"session_id": "sid"})[0]; r["selected_prism"] != opsPrismID || r["selected_fractal"] != nil {
		t.Errorf("after UpdatePrism: %v", r)
	}

	for _, id := range []string{"expired", "unknown"} {
		if err := store.UpdateFractal(id, labFractalID); !errors.Is(err, errSessionNotFound) {
			t.Errorf("UpdateFractal(%s) = %v, want errSessionNotFound", id, err)
		}
		if err := store.UpdatePrism(id, opsPrismID); !errors.Is(err, errSessionNotFound) {
			t.Errorf("UpdatePrism(%s) = %v, want errSessionNotFound", id, err)
		}
	}
	if r := db.rows("sessions", row{"session_id": "expired"})[0]; r["selected_fractal"] != nil {
		t.Error("an expired session was repointed")
	}

	db.failOn("UPDATE sessions", errors.New("pq: connection reset"))
	if err := store.UpdateFractal("sid", labFractalID); err == nil || errors.Is(err, errSessionNotFound) {
		t.Errorf("UpdateFractal on a failing store = %v", err)
	}
	if err := store.UpdatePrism("sid", opsPrismID); err == nil || errors.Is(err, errSessionNotFound) {
		t.Errorf("UpdatePrism on a failing store = %v", err)
	}
}

func TestPgSessionStoreDeletes(t *testing.T) {
	db := newAuthDB(t)
	store := newPgSessionStore(db.client.DB())
	db.newSession("a1", "alice")
	db.newSession("a2", "alice")
	db.newSession("b1", "bob")
	db.newSession("b-old", "bob", func(r row) { r["expires_at"] = time.Now().Add(-time.Hour) })

	store.Delete("a1")
	if len(db.rows("sessions", row{"session_id": "a1"})) != 0 || len(db.rows("sessions", nil)) != 3 {
		t.Errorf("Delete removed the wrong rows: %v", db.rows("sessions", nil))
	}
	store.Cleanup()
	if len(db.rows("sessions", row{"session_id": "b-old"})) != 0 || len(db.rows("sessions", nil)) != 2 {
		t.Errorf("Cleanup removed the wrong rows: %v", db.rows("sessions", nil))
	}
	store.DeleteByUsername("alice")
	if len(db.sessionsOf("alice")) != 0 || len(db.sessionsOf("bob")) != 1 {
		t.Errorf("DeleteByUsername removed the wrong rows: %v", db.rows("sessions", nil))
	}
}

// The stock schema ships admin with the well-known password "bifract", hashed
// without a pepper, and a forced change on first login.
func TestMigrateDefaultAdminHash(t *testing.T) {
	defaultHash, err := bcrypt.GenerateFromPassword([]byte(defaultAdminPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	stockAdmin := func(r row) {
		r["password_hash"] = string(defaultHash)
		r["is_admin"] = true
		r["force_password_change"] = true
	}

	t.Run("nothing configured", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "")
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
		h, db := newDBHandler(t)
		db.addUser("admin", stockAdmin)
		h.migrateDefaultAdminHash()
		if db.user("admin")["password_hash"] != string(defaultHash) || len(db.pg.Matching("WHERE username = 'admin'")) != 0 {
			t.Error("migration ran with nothing to migrate to")
		}
	})

	// A deployment-generated hash is a new random password, so the forced
	// change is dropped with the default.
	t.Run("deployment hash", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "$2a$12$deploymentgeneratedhashvalue")
		t.Setenv("BIFRACT_PASSWORD_PEPPER", "")
		h, db := newDBHandler(t)
		db.addUser("admin", stockAdmin)
		h.migrateDefaultAdminHash()
		admin := db.user("admin")
		if admin["password_hash"] != "$2a$12$deploymentgeneratedhashvalue" || admin["force_password_change"] != false {
			t.Errorf("admin = %v", admin)
		}
	})

	// Re-peppering keeps the password "bifract", so the change on first login
	// must still be forced.
	t.Run("pepper only", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "")
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		// The re-hash runs at full cost inside the migration's 5s deadline,
		// which an instrumented build (-race) can overrun.
		start := time.Now()
		if _, err := hashPassword(defaultAdminPassword); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 2*time.Second {
			t.Skipf("bcrypt at cost %d takes %v here, too close to the migration deadline", bcryptCost, time.Since(start))
		}
		h, db := newDBHandler(t)
		db.addUser("admin", stockAdmin)
		h.migrateDefaultAdminHash()
		admin := db.user("admin")
		if err := verifyPassword(admin["password_hash"].(string), defaultAdminPassword); err != nil {
			t.Errorf("default password does not verify under the pepper: %v", err)
		}
		if admin["force_password_change"] != true {
			t.Error("re-peppering the default password dropped the forced change")
		}
		rec := login(h, "admin", defaultAdminPassword, "203.0.113.1")
		if rec.Code != http.StatusOK {
			t.Fatalf("login as the default admin: %d", rec.Code)
		}
		if user := decodeBody(t, rec)["user"].(map[string]any); user["force_password_change"] != true {
			t.Errorf("login as the default admin = %v, want force_password_change", user)
		}
	})

	// An older release re-peppered the default password and cleared the forced
	// change with it. While the password is still the default, startup puts the
	// flag back; a password the admin chose is left alone.
	t.Run("already re-peppered without the flag", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "")
		t.Setenv("BIFRACT_PASSWORD_PEPPER", testPepper)
		peppered, err := hashPassword(defaultAdminPassword)
		if err != nil {
			t.Fatal(err)
		}
		h, db := newDBHandler(t)
		db.addUser("admin", func(r row) {
			r["password_hash"] = peppered
			r["is_admin"] = true
			r["force_password_change"] = false
		})
		db.addUser("other", asAdmin)
		h.migrateDefaultAdminHash()
		if db.user("admin")["force_password_change"] != true {
			t.Error("default password left without a forced change")
		}
		if db.user("admin")["password_hash"] != peppered {
			t.Error("the default hash was rewritten")
		}

		h, db = newDBHandler(t)
		db.addUser("admin", asAdmin)
		h.migrateDefaultAdminHash()
		if db.user("admin")["force_password_change"] == true || len(db.pg.Matching("UPDATE users")) != 0 {
			t.Error("a chosen admin password was flagged for change")
		}
	})

	t.Run("password already changed", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "$2a$12$deploymentgeneratedhashvalue")
		h, db := newDBHandler(t)
		db.addUser("admin", asAdmin)
		before := db.user("admin")["password_hash"]
		h.migrateDefaultAdminHash()
		if db.user("admin")["password_hash"] != before || len(db.pg.Matching("UPDATE users")) != 0 {
			t.Error("a changed admin password was overwritten")
		}
	})

	t.Run("no admin account", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "$2a$12$deploymentgeneratedhashvalue")
		h, db := newDBHandler(t)
		h.migrateDefaultAdminHash()
		if len(db.pg.Matching("UPDATE users")) != 0 {
			t.Error("update issued with no admin row")
		}
	})

	// The constructor runs it, so a fresh deployment is migrated before any login.
	t.Run("at startup", func(t *testing.T) {
		t.Setenv("BIFRACT_ADMIN_PASSWORD_HASH", "$2a$12$deploymentgeneratedhashvalue")
		db := newAuthDB(t)
		db.addUser("admin", stockAdmin)
		newHandlerOver(db)
		if db.user("admin")["password_hash"] != "$2a$12$deploymentgeneratedhashvalue" {
			t.Error("not migrated at startup")
		}
	})
}

func TestHandleGenerateClientCert(t *testing.T) {
	caDir := t.TempDir()
	if err := setup.GenerateClientCA(caDir); err != nil {
		t.Fatal(err)
	}
	h, db := newDBHandler(t)
	db.addUser("alice")
	admin := &storage.User{Username: "root", IsAdmin: true}
	gen := func(user, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		r = withURLParam(withUser(r, admin), "username", user)
		rec := httptest.NewRecorder()
		h.HandleGenerateClientCert(rec, r)
		return rec
	}

	if rec := gen("alice", `{"password":"p12-pass"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("mTLS not configured: %d", rec.Code)
	}
	h.clientCADir = caDir
	if rec := gen("nobody", `{"password":"p12-pass"}`); rec.Code != http.StatusNotFound {
		t.Errorf("unknown user: %d", rec.Code)
	}
	if rec := gen("alice", `{}`); rec.Code != http.StatusBadRequest {
		t.Errorf("no bundle password: %d", rec.Code)
	}
	rec := gen("alice", `{"password":"p12-pass"}`)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/x-pkcs12" || rec.Body.Len() == 0 {
		t.Fatalf("status %d, type %q, %d bytes", rec.Code, rec.Header().Get("Content-Type"), rec.Body.Len())
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename="alice.p12"` {
		t.Errorf("Content-Disposition = %q", got)
	}

	if err := os.Remove(filepath.Join(caDir, "ca-key.pem")); err != nil {
		t.Fatal(err)
	}
	if rec := gen("alice", `{"password":"p12-pass"}`); rec.Code != http.StatusInternalServerError {
		t.Errorf("CA key missing: %d", rec.Code)
	}
	h.clientCADir = t.TempDir()
	if rec := gen("alice", `{"password":"p12-pass"}`); rec.Code != http.StatusInternalServerError {
		t.Errorf("CA missing: %d", rec.Code)
	}
}

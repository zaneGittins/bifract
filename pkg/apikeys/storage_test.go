package apikeys

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"bifract/pkg/auth"
)

func TestGenerateAPIKeyFormat(t *testing.T) {
	s := &Storage{}
	tests := []struct {
		scope, wantPrefix string
	}{
		{scope: "alpha", wantPrefix: "bifract_alpha_"},
		// Hyphens and spaces would break the underscore-delimited format.
		{scope: "prod-web logs", wantPrefix: "bifract_prod_web_logs_"},
		{scope: TenantKeyPrefix, wantPrefix: "bifract_admin_"},
	}
	for _, tt := range tests {
		t.Run(tt.scope, func(t *testing.T) {
			full, keyID, err := s.GenerateAPIKey(context.Background(), tt.scope)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(full, tt.wantPrefix) {
				t.Fatalf("key %q lacks prefix %q", full, tt.wantPrefix)
			}
			random := strings.TrimPrefix(full, tt.wantPrefix)
			// 32 random bytes, hex encoded: 256 bits of entropy.
			if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(random) {
				t.Fatalf("random part %q is not 64 hex chars", random)
			}
			// The public identifier is a prefix of the secret part, never the whole of it.
			if keyID != random[:8] {
				t.Errorf("keyID = %q, want first 8 chars of %q", keyID, random)
			}
		})
	}
}

func TestGenerateAPIKeyUnique(t *testing.T) {
	s := &Storage{}
	seen := map[string]bool{}
	ids := map[string]bool{}
	for i := 0; i < 200; i++ {
		full, keyID, err := s.GenerateAPIKey(context.Background(), "f")
		if err != nil {
			t.Fatal(err)
		}
		if seen[full] {
			t.Fatalf("duplicate key after %d draws", i)
		}
		seen[full] = true
		ids[keyID] = true
	}
	// 32-bit identifiers: a collision in 200 draws would mean the randomness is broken.
	if len(ids) != 200 {
		t.Errorf("got %d distinct key IDs from 200 keys", len(ids))
	}
}

func TestHashKey(t *testing.T) {
	s := &Storage{}
	sum := sha256.Sum256([]byte("bifract_x_abc"))
	if got, want := s.HashKey("bifract_x_abc"), hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("HashKey = %q, want SHA-256 hex %q", got, want)
	}
	if s.HashKey("a") == s.HashKey("b") {
		t.Fatal("distinct keys hash the same")
	}
}

// Only the hash reaches the database: the plaintext is never bound to any
// statement, so it cannot be read back from the table or the query log.
func TestCreateStoresOnlyHash(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour).UTC()
	tests := []struct {
		name                  string
		create                func(*Storage, string, string) (*APIKey, error)
		wantFractal, wantPrsm any
		wantTenant            bool
	}{
		{
			name: "fractal",
			create: func(s *Storage, full, id string) (*APIKey, error) {
				return s.CreateFractalAPIKey(context.Background(), CreateAPIKeyRequest{Name: "ci", Role: "analyst"}, fractalA, "alice", full, id)
			},
			wantFractal: fractalA,
		},
		{
			name: "prism",
			create: func(s *Storage, full, id string) (*APIKey, error) {
				return s.CreatePrismAPIKey(context.Background(), CreateAPIKeyRequest{Name: "ci", Role: "viewer"}, prismA, "alice", full, id)
			},
			wantPrsm: prismA,
		},
		{
			name: "tenant",
			create: func(s *Storage, full, id string) (*APIKey, error) {
				return s.CreateTenantAPIKey(context.Background(), CreateAPIKeyRequest{Name: "ops", TenantAdmin: true, ExpiresAt: &expiry}, "alice", full, id)
			},
			wantTenant: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newKeyDB()
			s := NewStorage(db.client)
			full, keyID, _ := s.GenerateAPIKey(context.Background(), "scope")

			key, err := tt.create(s, full, keyID)
			if err != nil {
				t.Fatal(err)
			}
			inserts := db.pg.Matching("INSERT INTO api_keys")
			if len(inserts) != 1 {
				t.Fatalf("got %d inserts, want 1", len(inserts))
			}
			args := inserts[0].Args
			if args[3] != s.HashKey(full) {
				t.Errorf("key_hash = %v, want hash of the key", args[3])
			}
			if args[2] != keyID {
				t.Errorf("key_id = %v, want %q", args[2], keyID)
			}
			if args[4] != tt.wantFractal || args[5] != tt.wantPrsm {
				t.Errorf("scope = (%v, %v), want (%v, %v)", args[4], args[5], tt.wantFractal, tt.wantPrsm)
			}
			if args[9] != tt.wantTenant {
				t.Errorf("tenant_admin = %v, want %v", args[9], tt.wantTenant)
			}
			for _, c := range db.pg.Calls() {
				for _, a := range c.Args {
					if s, ok := a.(string); ok && strings.Contains(s, full) {
						t.Fatalf("plaintext key bound to %q", c.Query)
					}
				}
			}
			if key.ID == "" || key.KeyID != keyID {
				t.Errorf("returned key = %+v, want it read back by id", key)
			}
		})
	}
}

// created_by is a foreign key into users: an empty attribution must bind NULL.
func TestCreateAttribution(t *testing.T) {
	for _, tt := range []struct {
		user string
		want any
	}{{"alice", "alice"}, {"", nil}} {
		db := newKeyDB()
		s := NewStorage(db.client)
		if _, err := s.CreateFractalAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k"}, fractalA, tt.user, "bifract_a_x", "x"); err != nil {
			t.Fatal(err)
		}
		if got := db.pg.Matching("INSERT INTO api_keys")[0].Args[6]; got != tt.want {
			t.Errorf("user %q: created_by bound as %#v, want %#v", tt.user, got, tt.want)
		}
	}
}

// A key is either bound to exactly one scope or is a tenant admin key bound to
// none. Every other shape is refused before anything is written.
func TestCreateRejectsInvalidShapes(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	tests := []struct {
		name string
		run  func(*Storage) error
	}{
		{name: "both scopes", run: func(s *Storage) error {
			_, err := s.createAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k"}, "u", "full", "id", fractalA, prismA)
			return err
		}},
		{name: "scoped key claiming tenant admin", run: func(s *Storage) error {
			_, err := s.CreateFractalAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k", TenantAdmin: true, ExpiresAt: &expiry}, fractalA, "u", "full", "id")
			return err
		}},
		{name: "unscoped key that is not tenant admin", run: func(s *Storage) error {
			_, err := s.CreateTenantAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k"}, "u", "full", "id")
			return err
		}},
		{name: "unscoped key bypassing the tenant guard", run: func(s *Storage) error {
			_, err := s.createAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k"}, "u", "full", "id", "", "")
			return err
		}},
		{name: "tenant admin without expiry", run: func(s *Storage) error {
			_, err := s.CreateTenantAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k", TenantAdmin: true}, "u", "full", "id")
			return err
		}},
		{name: "unknown role", run: func(s *Storage) error {
			_, err := s.CreatePrismAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k", Role: "superuser"}, prismA, "u", "full", "id")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newKeyDB()
			if err := tt.run(NewStorage(db.client)); err == nil {
				t.Fatal("want error")
			}
			if w := db.writes(); len(w) != 0 {
				t.Fatalf("wrote %d statements, want none", len(w))
			}
		})
	}
}

func TestValidateAPIKey(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	s := &Storage{}

	db := newKeyDB()
	db.add(keyRow{id: "k1", keyID: "aaaa1111", keyHash: s.HashKey("bifract_alpha_good"), fractalID: fractalA, role: "analyst", createdBy: "alice", active: true, expires: &future})
	db.add(keyRow{id: "k2", keyID: "bbbb2222", keyHash: s.HashKey("bifract_alpha_expired"), fractalID: fractalA, role: "admin", active: true, expires: &past})
	db.add(keyRow{id: "k3", keyID: "cccc3333", keyHash: s.HashKey("bifract_alpha_disabled"), fractalID: fractalA, role: "admin", active: false})
	db.add(keyRow{id: "k4", keyID: "dddd4444", keyHash: s.HashKey("bifract_prism_a_key"), prismID: prismA, role: "viewer", createdBy: "alice", active: true})
	db.add(keyRow{id: "k5", keyHash: s.HashKey("bifract_alpha_bob"), fractalID: fractalA, role: "viewer", createdBy: "bob", active: true})
	db.add(keyRow{id: "k6", keyHash: s.HashKey("bifract_alpha_orphan"), fractalID: fractalA, role: "viewer", active: true})
	db.add(keyRow{id: "k7", keyHash: s.HashKey("bifract_admin_root"), tenant: true, createdBy: "root", active: true, expires: &future})
	db.add(keyRow{id: "k8", keyHash: s.HashKey("bifract_admin_demoted"), tenant: true, createdBy: "alice", active: true, expires: &future})
	db.users["alice"] = userRow{enabled: true}
	db.users["bob"] = userRow{enabled: false}
	db.users["root"] = userRow{enabled: true, admin: true}
	st := NewStorage(db.client)

	t.Run("valid key carries its scope and role", func(t *testing.T) {
		v, err := st.ValidateAPIKey(context.Background(), "bifract_alpha_good")
		if err != nil {
			t.Fatal(err)
		}
		if v.ID != "k1" || v.FractalID != fractalA || v.FractalName != "alpha-logs" || v.Role != "analyst" || v.CreatedBy != "alice" || v.TenantAdmin {
			t.Fatalf("validated key = %+v", v.APIKey)
		}
		// The lookup is by hash; the presented secret is not sent to the database.
		call := db.pg.Matching("ak.key_hash = $1")[0]
		if call.Args[0] != s.HashKey("bifract_alpha_good") {
			t.Errorf("looked up by %v, want the key's hash", call.Args[0])
		}
		if !strings.Contains(call.Query, "ak.is_active = true") {
			t.Error("lookup does not exclude deactivated keys")
		}
	})
	t.Run("tenant key of a tenant admin", func(t *testing.T) {
		if v, err := st.ValidateAPIKey(context.Background(), "bifract_admin_root"); err != nil || v.ID != "k7" {
			t.Fatalf("ValidateAPIKey = (%v, %v)", v, err)
		}
	})
	t.Run("prism key", func(t *testing.T) {
		v, err := st.ValidateAPIKey(context.Background(), "bifract_prism_a_key")
		if err != nil {
			t.Fatal(err)
		}
		if v.PrismID != prismA || v.PrismName != "Prism A" || v.FractalID != "" {
			t.Fatalf("validated key = %+v", v.APIKey)
		}
	})
	for _, tt := range []struct{ name, key string }{
		{"unknown key", "bifract_alpha_nope"},
		{"expired key", "bifract_alpha_expired"},
		{"deactivated key", "bifract_alpha_disabled"},
		{"empty key", ""},
		// A key acts for the person who issued it, so it stops when they
		// are disabled or deleted, or for a tenant key, lose tenant admin.
		{"creator disabled", "bifract_alpha_bob"},
		{"creator deleted", "bifract_alpha_orphan"},
		{"tenant key of a demoted creator", "bifract_admin_demoted"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, err := st.ValidateAPIKey(context.Background(), tt.key)
			// ErrAPIKeyRejected is what makes the middleware answer 401 rather than 503.
			if !errors.Is(err, auth.ErrAPIKeyRejected) || v != nil {
				t.Fatalf("ValidateAPIKey = (%v, %v), want ErrAPIKeyRejected", v, err)
			}
		})
	}
	t.Run("database failure is not a rejection", func(t *testing.T) {
		db.failAll = fmt.Errorf("connection refused")
		defer func() { db.failAll = nil }()
		_, err := st.ValidateAPIKey(context.Background(), "bifract_alpha_good")
		if err == nil || errors.Is(err, auth.ErrAPIKeyRejected) {
			t.Fatalf("err = %v, want a non-rejection error so the caller can retry", err)
		}
	})
}

// Every scoped read and write is confined to the scope the caller addressed the
// key through. A key that exists in another scope must read as not found, and
// a write that matched nothing must not report success.
func TestScopedOperationsStayInScope(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	newDB := func() *keyDB {
		db := newKeyDB()
		db.add(keyRow{id: "fk", fractalID: fractalA, role: "viewer", active: true})
		db.add(keyRow{id: "pk", prismID: prismA, role: "viewer", active: true})
		db.add(keyRow{id: "tk", tenant: true, active: true, expires: &expiry})
		return db
	}
	type op struct {
		name string
		run  func(s *Storage, keyID string) error
	}
	ops := func(scope string) []op {
		return []op{
			{"get", func(s *Storage, id string) error {
				var err error
				switch scope {
				case "fractal":
					_, err = s.GetFractalAPIKey(context.Background(), id, fractalA)
				case "prism":
					_, err = s.GetPrismAPIKey(context.Background(), id, prismA)
				default:
					_, err = s.GetTenantAPIKey(context.Background(), id)
				}
				return err
			}},
			{"update", func(s *Storage, id string) error {
				req := UpdateAPIKeyRequest{Name: ptr("renamed"), IsActive: ptr(false)}
				var err error
				switch scope {
				case "fractal":
					_, err = s.UpdateFractalAPIKey(context.Background(), id, fractalA, req)
				case "prism":
					_, err = s.UpdatePrismAPIKey(context.Background(), id, prismA, req)
				default:
					_, err = s.UpdateTenantAPIKey(context.Background(), id, req)
				}
				return err
			}},
			{"toggle", func(s *Storage, id string) error {
				var err error
				switch scope {
				case "fractal":
					_, err = s.ToggleFractalAPIKey(context.Background(), id, fractalA)
				case "prism":
					_, err = s.TogglePrismAPIKey(context.Background(), id, prismA)
				default:
					_, err = s.ToggleTenantAPIKey(context.Background(), id)
				}
				return err
			}},
			{"delete", func(s *Storage, id string) error {
				switch scope {
				case "fractal":
					return s.DeleteFractalAPIKey(context.Background(), id, fractalA)
				case "prism":
					return s.DeletePrismAPIKey(context.Background(), id, prismA)
				default:
					return s.DeleteTenantAPIKey(context.Background(), id)
				}
			}},
		}
	}
	owner := map[string]string{"fractal": "fk", "prism": "pk", "tenant": "tk"}
	for _, scope := range []string{"fractal", "prism", "tenant"} {
		for _, o := range ops(scope) {
			for _, keyID := range []string{"fk", "pk", "tk", "missing"} {
				name := fmt.Sprintf("%s %s via %s scope", o.name, keyID, scope)
				t.Run(name, func(t *testing.T) {
					db := newDB()
					before, _ := db.get(keyID)
					err := o.run(NewStorage(db.client), keyID)
					if keyID == owner[scope] {
						if err != nil {
							t.Fatalf("own key: %v", err)
						}
						return
					}
					if err == nil {
						t.Fatal("reached a key outside the addressed scope")
					}
					if o.name != "get" && !errors.Is(err, ErrKeyNotFound) {
						t.Errorf("err = %v, want ErrKeyNotFound", err)
					}
					if keyID != "missing" {
						if after, ok := db.get(keyID); !ok || after != before {
							t.Errorf("key %s changed through another scope", keyID)
						}
					}
				})
			}
		}
	}
}

// The SET list and the WHERE clause share one placeholder sequence; the id and
// scope must bind after every SET value.
func TestUpdateBindsInOrder(t *testing.T) {
	expiry := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	db := newKeyDB()
	db.add(keyRow{id: "fk", fractalID: fractalA, role: "viewer", active: true})
	req := UpdateAPIKeyRequest{
		Name:        ptr("n"),
		Description: ptr("d"),
		ExpiresAt:   &expiry,
		IsActive:    ptr(false),
		Role:        ptr("admin"),
		TenantAdmin: ptr(false),
	}
	if _, err := NewStorage(db.client).UpdateFractalAPIKey(context.Background(), "fk", fractalA, req); err != nil {
		t.Fatal(err)
	}
	c := db.pg.Matching("UPDATE api_keys")[0]
	for i, col := range []string{"name", "description", "expires_at", "is_active", "role", "tenant_admin"} {
		if want := fmt.Sprintf("%s = $%d", col, i+1); !strings.Contains(c.Query, want) {
			t.Errorf("query lacks %q:\n%s", want, c.Query)
		}
	}
	if !strings.Contains(c.Query, "WHERE id = $7 AND fractal_id = $8") {
		t.Errorf("WHERE binds wrong placeholders:\n%s", c.Query)
	}
	want := []any{"n", "d", expiry, false, "admin", false, "fk", fractalA}
	if fmt.Sprint(c.Args) != fmt.Sprint(want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

// expires_at is a TIMESTAMP without time zone, which keeps the wall clock of
// whatever it is given and drops the offset. An expiry sent with an offset
// must be stored as the same instant in UTC, or it lands hours off and slips
// past the bound a calling key's own expiry sets.
func TestExpiryIsStoredInUTC(t *testing.T) {
	offset, _ := time.Parse(time.RFC3339, "2027-01-01T11:30:00+02:00")
	want := time.Date(2027, 1, 1, 9, 30, 0, 0, time.UTC)
	stored := func(v driver.Value) {
		t.Helper()
		got, ok := v.(time.Time)
		if !ok || got.Location() != time.UTC || !got.Equal(want) {
			t.Errorf("expires_at bound as %v, want %v", v, want)
		}
	}

	db := newKeyDB()
	s := NewStorage(db.client)
	if _, err := s.CreateTenantAPIKey(context.Background(), CreateAPIKeyRequest{Name: "ops", TenantAdmin: true, ExpiresAt: &offset}, "alice", "bifract_admin_x", "x"); err != nil {
		t.Fatal(err)
	}
	stored(db.pg.Matching("INSERT INTO api_keys")[0].Args[7])

	db.add(keyRow{id: "fk", fractalID: fractalA, active: true})
	if _, err := s.UpdateFractalAPIKey(context.Background(), "fk", fractalA, UpdateAPIKeyRequest{ExpiresAt: &offset}); err != nil {
		t.Fatal(err)
	}
	stored(db.pg.Matching("UPDATE api_keys")[0].Args[0])
}

func TestUpdateWithNoFields(t *testing.T) {
	db := newKeyDB()
	db.add(keyRow{id: "fk", fractalID: fractalA, active: true})
	if _, err := NewStorage(db.client).UpdateFractalAPIKey(context.Background(), "fk", fractalA, UpdateAPIKeyRequest{}); err == nil {
		t.Fatal("empty update accepted")
	}
	if len(db.writes()) != 0 {
		t.Fatal("empty update reached the database")
	}
}

func TestToggleFlipsActive(t *testing.T) {
	db := newKeyDB()
	db.add(keyRow{id: "fk", fractalID: fractalA, active: true})
	s := NewStorage(db.client)
	k, err := s.ToggleFractalAPIKey(context.Background(), "fk", fractalA)
	if err != nil || k.IsActive {
		t.Fatalf("first toggle = (%+v, %v), want inactive", k, err)
	}
	k, err = s.ToggleFractalAPIKey(context.Background(), "fk", fractalA)
	if err != nil || !k.IsActive {
		t.Fatalf("second toggle = (%+v, %v), want active", k, err)
	}
}

func TestListByScope(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	db := newKeyDB()
	db.add(keyRow{id: "a1", fractalID: fractalA, active: true})
	db.add(keyRow{id: "a2", fractalID: fractalA, active: false})
	db.add(keyRow{id: "b1", fractalID: fractalB, active: true})
	db.add(keyRow{id: "p1", prismID: prismA, active: true})
	db.add(keyRow{id: "t1", tenant: true, active: true, expires: &expiry})
	s := NewStorage(db.client)

	ids := func(keys []APIKey, err error) string {
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, k := range keys {
			out = append(out, k.ID)
		}
		return strings.Join(out, ",")
	}
	if got := ids(s.ListAPIKeysByFractal(context.Background(), fractalA)); got != "a1,a2" {
		t.Errorf("fractal A lists %q", got)
	}
	if got := ids(s.ListAPIKeysByPrism(context.Background(), prismA)); got != "p1" {
		t.Errorf("prism A lists %q", got)
	}
	if got := ids(s.ListAllAPIKeys(context.Background())); got != "a1,a2,b1,p1,t1" {
		t.Errorf("all lists %q", got)
	}
	if got := ids(s.ListAPIKeysByFractal(context.Background(), fractalB)); got != "b1" {
		t.Errorf("fractal B lists %q", got)
	}

	db.failAll = errors.New("boom")
	if _, err := s.ListAllAPIKeys(context.Background()); err == nil {
		t.Error("list swallowed a database error")
	}
}

func TestUpdateLastUsed(t *testing.T) {
	db := newKeyDB()
	if err := NewStorage(db.client).UpdateLastUsed(context.Background(), "aaaa1111"); err != nil {
		t.Fatal(err)
	}
	c := db.pg.Matching("UPDATE api_keys")
	if len(c) != 1 || c[0].Args[0] != "aaaa1111" || !strings.Contains(c[0].Query, "usage_count = usage_count + 1") {
		t.Fatalf("calls = %+v", c)
	}
}

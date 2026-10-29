package apikeys

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/storage"
)

var (
	tenantAdmin  = &storage.User{Username: "root", IsAdmin: true}
	fractalAdmin = &storage.User{Username: "fadmin"}
	analyst      = &storage.User{Username: "ana"}
	prismAdmin   = &storage.User{Username: "padmin"}
	stranger     = &storage.User{Username: "nobody"}
)

// grantRoles gives the fixture users their permission rows: fadmin is admin of
// fractal A only, ana is analyst there, padmin is admin of prism A only.
func grantRoles(db *keyDB) {
	db.fractalRoles[fractalA] = map[string]string{"fadmin": "admin", "ana": "analyst"}
	db.fractalRoles[fractalB] = map[string]string{"ana": "admin"}
	db.prismRoles[prismA] = map[string]string{"padmin": "admin", "ana": "analyst"}
}

type route struct {
	name   string
	fn     func(*Handler) http.HandlerFunc
	method string
	body   string
	keyed  bool // takes a {keyId}
}

var fractalRoutes = []route{
	{"list", func(h *Handler) http.HandlerFunc { return h.HandleListAPIKeys }, http.MethodGet, "", false},
	{"create", func(h *Handler) http.HandlerFunc { return h.HandleCreateAPIKey }, http.MethodPost, `{"name":"ci","role":"viewer"}`, false},
	{"get", func(h *Handler) http.HandlerFunc { return h.HandleGetAPIKey }, http.MethodGet, "", true},
	{"update", func(h *Handler) http.HandlerFunc { return h.HandleUpdateAPIKey }, http.MethodPut, `{"name":"renamed"}`, true},
	{"toggle", func(h *Handler) http.HandlerFunc { return h.HandleToggleAPIKey }, http.MethodPost, "", true},
	{"delete", func(h *Handler) http.HandlerFunc { return h.HandleDeleteAPIKey }, http.MethodDelete, "", true},
}

var prismRoutes = []route{
	{"list", func(h *Handler) http.HandlerFunc { return h.HandleListPrismAPIKeys }, http.MethodGet, "", false},
	{"create", func(h *Handler) http.HandlerFunc { return h.HandleCreatePrismAPIKey }, http.MethodPost, `{"name":"ci","role":"viewer"}`, false},
	{"get", func(h *Handler) http.HandlerFunc { return h.HandleGetPrismAPIKey }, http.MethodGet, "", true},
	{"update", func(h *Handler) http.HandlerFunc { return h.HandleUpdatePrismAPIKey }, http.MethodPut, `{"name":"renamed"}`, true},
	{"toggle", func(h *Handler) http.HandlerFunc { return h.HandleTogglePrismAPIKey }, http.MethodPost, "", true},
	{"delete", func(h *Handler) http.HandlerFunc { return h.HandleDeletePrismAPIKey }, http.MethodDelete, "", true},
}

// Managing a scope's keys takes admin on that scope. A refused caller must not
// reach the api_keys table at all.
func TestScopedRoutesRequireScopeAdmin(t *testing.T) {
	fractalKey := &auth.ValidatedAPIKey{KeyID: "fk000001", FractalID: fractalA, Role: "admin", CreatedBy: "fadmin"}
	prismKey := &auth.ValidatedAPIKey{KeyID: "pk000001", PrismID: prismA, Role: "admin", CreatedBy: "padmin"}
	tenantKey := &auth.ValidatedAPIKey{KeyID: "tk000001", TenantAdmin: true, CreatedBy: "root"}

	tests := []struct {
		name   string
		routes []route
		scope  string // the {id} the request addresses
		key    string // a key that lives in that scope
		who    principal
		want   int
	}{
		{"fractal: anonymous", fractalRoutes, fractalA, "fk", anonymous, http.StatusUnauthorized},
		{"fractal: no grant", fractalRoutes, fractalA, "fk", sessionUser(stranger), http.StatusForbidden},
		{"fractal: analyst", fractalRoutes, fractalA, "fk", sessionUser(analyst), http.StatusForbidden},
		// Admin of one fractal is nothing on another.
		{"fractal: admin of another fractal", fractalRoutes, fractalB, "fb", sessionUser(fractalAdmin), http.StatusForbidden},
		{"fractal: prism admin", fractalRoutes, fractalA, "fk", sessionUser(prismAdmin), http.StatusForbidden},
		{"fractal: fractal admin", fractalRoutes, fractalA, "fk", sessionUser(fractalAdmin), http.StatusOK},
		{"fractal: tenant admin", fractalRoutes, fractalB, "fb", sessionUser(tenantAdmin), http.StatusOK},
		// A fractal-admin API key resolves by its synthetic apikey_ name, which
		// holds no permission row, so it cannot manage keys even on its own fractal.
		{"fractal: fractal-admin API key", fractalRoutes, fractalA, "fk", apiKeyPrincipal(fractalKey), http.StatusForbidden},
		{"fractal: tenant-admin API key", fractalRoutes, fractalA, "fk", apiKeyPrincipal(tenantKey), http.StatusOK},

		{"prism: anonymous", prismRoutes, prismA, "pk", anonymous, http.StatusUnauthorized},
		{"prism: no grant", prismRoutes, prismA, "pk", sessionUser(stranger), http.StatusForbidden},
		{"prism: analyst", prismRoutes, prismA, "pk", sessionUser(analyst), http.StatusForbidden},
		{"prism: admin of another prism", prismRoutes, prismB, "pb", sessionUser(prismAdmin), http.StatusForbidden},
		{"prism: fractal admin", prismRoutes, prismA, "pk", sessionUser(fractalAdmin), http.StatusForbidden},
		{"prism: prism admin", prismRoutes, prismA, "pk", sessionUser(prismAdmin), http.StatusOK},
		{"prism: tenant admin", prismRoutes, prismB, "pb", sessionUser(tenantAdmin), http.StatusOK},
		// Unlike the fractal routes, a prism-admin key's role is honoured on the
		// prism it was issued for (rbac.ResolvePrismRoleWithAdmin reads it from
		// the request) and on no other.
		{"prism: prism-admin API key, own prism", prismRoutes, prismA, "pk", apiKeyPrincipal(prismKey), http.StatusOK},
		{"prism: prism-admin API key, other prism", prismRoutes, prismB, "pb", apiKeyPrincipal(prismKey), http.StatusForbidden},
		{"prism: tenant-admin API key", prismRoutes, prismA, "pk", apiKeyPrincipal(tenantKey), http.StatusOK},
	}
	for _, tt := range tests {
		for _, rt := range tt.routes {
			t.Run(tt.name+"/"+rt.name, func(t *testing.T) {
				db := newKeyDB()
				grantRoles(db)
				db.add(keyRow{id: "fk", fractalID: fractalA, role: "viewer", active: true})
				db.add(keyRow{id: "fb", fractalID: fractalB, role: "viewer", active: true})
				db.add(keyRow{id: "pk", prismID: prismA, role: "viewer", active: true})
				db.add(keyRow{id: "pb", prismID: prismB, role: "viewer", active: true})
				h := newTestHandler(db)

				params := []string{"id", tt.scope}
				if rt.keyed {
					params = append(params, "keyId", tt.key)
				}
				rec := call(rt.fn(h), rt.method, rt.body, tt.who, params...)
				if rec.Code != tt.want {
					t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
				}
				if tt.want != http.StatusOK && len(db.pg.Matching("api_keys")) != 0 {
					t.Fatalf("refused caller reached api_keys: %+v", db.pg.Matching("api_keys"))
				}
			})
		}
	}
}

// A key ID from another scope is not found through this one, so an admin of
// fractal A cannot read, change, toggle or delete fractal B's keys by guessing
// their IDs.
func TestScopedRoutesDoNotReachOtherScopesKeys(t *testing.T) {
	for _, tt := range []struct {
		name   string
		routes []route
		scope  string
		who    *storage.User
	}{
		{"fractal", fractalRoutes, fractalA, fractalAdmin},
		{"prism", prismRoutes, prismA, prismAdmin},
	} {
		for _, rt := range tt.routes {
			if !rt.keyed {
				continue
			}
			for _, foreign := range []string{"fb", "pb", "tk"} {
				t.Run(tt.name+"/"+rt.name+"/"+foreign, func(t *testing.T) {
					db := newKeyDB()
					grantRoles(db)
					expiry := time.Now().Add(time.Hour)
					db.add(keyRow{id: "fb", fractalID: fractalB, role: "viewer", active: true})
					db.add(keyRow{id: "pb", prismID: prismB, role: "viewer", active: true})
					db.add(keyRow{id: "tk", tenant: true, active: true, expires: &expiry})
					before, _ := db.get(foreign)

					rec := call(rt.fn(newTestHandler(db)), rt.method, rt.body, sessionUser(tt.who), "id", tt.scope, "keyId", foreign)
					if rec.Code == http.StatusOK {
						t.Fatalf("reached a foreign key: %s", rec.Body.String())
					}
					if after, ok := db.get(foreign); !ok || after != before {
						t.Fatal("foreign key changed")
					}
				})
			}
		}
	}
}

func TestCreateFractalKeyShowsSecretOnce(t *testing.T) {
	db := newKeyDB()
	grantRoles(db)
	h := newTestHandler(db)

	rec := call(h.HandleCreateAPIKey, http.MethodPost, `{"name":"ci","role":"analyst"}`, sessionUser(fractalAdmin), "id", fractalA)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var created CreateAPIKeyResponse
	if err := json.Unmarshal(decode(t, rec).Data, &created); err != nil {
		t.Fatal(err)
	}
	// The key names its fractal, with the hyphen folded to keep the format parseable.
	if !strings.HasPrefix(created.Key, "bifract_alpha_logs_") {
		t.Errorf("key %q does not carry the fractal name", created.Key)
	}
	if created.KeyID == "" || created.APIKey.KeyID != created.KeyID {
		t.Errorf("key id mismatch: %q vs %q", created.KeyID, created.APIKey.KeyID)
	}
	if created.APIKey.Role != "analyst" || created.APIKey.FractalID != fractalA || created.APIKey.TenantAdmin {
		t.Errorf("created key = %+v", created.APIKey)
	}
	stored, _ := db.get(created.APIKey.ID)
	if stored.keyHash != (&Storage{}).HashKey(created.Key) {
		t.Error("stored hash is not the hash of the returned key")
	}
	if stored.createdBy != "fadmin" {
		t.Errorf("created_by = %q, want the session user", stored.createdBy)
	}

	// After creation the secret is gone: neither list nor get can return it, or its hash.
	for _, rec := range []string{
		call(h.HandleListAPIKeys, http.MethodGet, "", sessionUser(fractalAdmin), "id", fractalA).Body.String(),
		call(h.HandleGetAPIKey, http.MethodGet, "", sessionUser(fractalAdmin), "id", fractalA, "keyId", created.APIKey.ID).Body.String(),
		call(h.HandleToggleAPIKey, http.MethodPost, "", sessionUser(fractalAdmin), "id", fractalA, "keyId", created.APIKey.ID).Body.String(),
	} {
		if !strings.Contains(rec, created.APIKey.ID) {
			t.Fatalf("response does not describe the key: %s", rec)
		}
		if strings.Contains(rec, created.Key) || strings.Contains(rec, stored.keyHash) || strings.Contains(rec, "hash") {
			t.Fatalf("response leaks the secret or its hash: %s", rec)
		}
	}
}

func TestCreateFractalKeyFallsBackToID(t *testing.T) {
	db := newKeyDB()
	delete(db.fractalNames, fractalA)
	rec := call(newTestHandler(db).HandleCreateAPIKey, http.MethodPost, `{"name":"ci"}`, sessionUser(tenantAdmin), "id", fractalA)
	var created CreateAPIKeyResponse
	_ = json.Unmarshal(decode(t, rec).Data, &created)
	if want := "bifract_" + strings.ReplaceAll(fractalA, "-", "_") + "_"; !strings.HasPrefix(created.Key, want) {
		t.Fatalf("key %q, want prefix %q", created.Key, want)
	}
}

// A key-authenticated caller cannot give any key, itself included, an expiry
// later than its own, so a tenant key cannot outlive the expiry its issuer
// chose by renewing itself or minting a successor. People are not bounded.
func TestKeyCannotOutliveCallingKey(t *testing.T) {
	callerExpiry := time.Now().Add(time.Hour)
	within := time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)
	beyond := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	tenantKey := apiKeyPrincipal(&auth.ValidatedAPIKey{KeyID: "tk000001", TenantAdmin: true, CreatedBy: "root", ExpiresAt: &callerExpiry})

	create := route{fn: func(h *Handler) http.HandlerFunc { return h.HandleCreateAPIKey }, method: http.MethodPost}
	createPrism := route{fn: func(h *Handler) http.HandlerFunc { return h.HandleCreatePrismAPIKey }, method: http.MethodPost}
	createTenant := route{fn: func(h *Handler) http.HandlerFunc { return h.HandleCreateTenantAPIKey }, method: http.MethodPost}
	update := route{fn: func(h *Handler) http.HandlerFunc { return h.HandleUpdateAPIKey }, method: http.MethodPut}
	updatePrism := route{fn: func(h *Handler) http.HandlerFunc { return h.HandleUpdatePrismAPIKey }, method: http.MethodPut}
	updateTenant := route{fn: func(h *Handler) http.HandlerFunc { return h.HandleUpdateTenantAPIKey }, method: http.MethodPut}
	fractalIDs := []string{"id", fractalA}
	prismIDs := []string{"id", prismA}

	tests := []struct {
		name  string
		route route
		body  string
		ids   []string
		who   principal
		want  int
	}{
		{"tenant key minted beyond caller", createTenant, `{"name":"x","expires_at":"` + beyond + `"}`, nil, tenantKey, http.StatusBadRequest},
		{"tenant key minted within caller", createTenant, `{"name":"x","expires_at":"` + within + `"}`, nil, tenantKey, http.StatusOK},
		{"fractal key that never expires", create, `{"name":"x"}`, fractalIDs, tenantKey, http.StatusBadRequest},
		{"fractal key beyond caller", create, `{"name":"x","expires_at":"` + beyond + `"}`, fractalIDs, tenantKey, http.StatusBadRequest},
		{"fractal key within caller", create, `{"name":"x","expires_at":"` + within + `"}`, fractalIDs, tenantKey, http.StatusOK},
		{"prism key that never expires", createPrism, `{"name":"x"}`, prismIDs, tenantKey, http.StatusBadRequest},
		{"caller extends itself", updateTenant, `{"expires_at":"` + beyond + `"}`, []string{"keyId", "tk"}, tenantKey, http.StatusBadRequest},
		{"caller shortens itself", updateTenant, `{"expires_at":"` + within + `"}`, []string{"keyId", "tk"}, tenantKey, http.StatusOK},
		{"fractal key extended beyond caller", update, `{"expires_at":"` + beyond + `"}`, []string{"id", fractalA, "keyId", "fk"}, tenantKey, http.StatusBadRequest},
		{"prism key extended beyond caller", updatePrism, `{"expires_at":"` + beyond + `"}`, []string{"id", prismA, "keyId", "pk"}, tenantKey, http.StatusBadRequest},
		{"rename leaves expiry alone", update, `{"name":"renamed"}`, []string{"id", fractalA, "keyId", "fk"}, tenantKey, http.StatusOK},
		{"person sets any expiry", createTenant, `{"name":"x","expires_at":"` + beyond + `"}`, nil, sessionUser(tenantAdmin), http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newKeyDB()
			db.add(keyRow{id: "fk", fractalID: fractalA, role: "viewer", active: true})
			db.add(keyRow{id: "pk", prismID: prismA, role: "viewer", active: true})
			db.add(keyRow{id: "tk", tenant: true, active: true, expires: &callerExpiry})
			rec := call(tt.route.fn(newTestHandler(db)), tt.route.method, tt.body, tt.who, tt.ids...)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
			}
			if tt.want != http.StatusOK && len(db.writes()) != 0 {
				t.Fatal("refused request wrote")
			}
		})
	}
}

// Writes made with an API key are credited to the person who issued the key,
// never to the synthetic apikey_ principal, which has no users row.
func TestCreateAttributesAPIKeyCallsToIssuer(t *testing.T) {
	for _, tt := range []struct {
		creator string
		want    string
	}{{"root", "root"}, {"", ""}} {
		db := newKeyDB()
		tenantKey := &auth.ValidatedAPIKey{KeyID: "tk000001", TenantAdmin: true, CreatedBy: tt.creator}
		rec := call(newTestHandler(db).HandleCreateAPIKey, http.MethodPost, `{"name":"ci"}`, apiKeyPrincipal(tenantKey), "id", fractalA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
		}
		got := db.pg.Matching("INSERT INTO api_keys")[0].Args[6]
		if tt.want == "" && got != nil {
			t.Errorf("created_by = %#v, want NULL for a key with no issuer", got)
		}
		if tt.want != "" && got != tt.want {
			t.Errorf("created_by = %#v, want %q", got, tt.want)
		}
	}
}

func TestCreateRejectsBadInput(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Format(time.RFC3339)
	for _, tt := range []struct {
		name string
		body string
	}{
		{"malformed", `{"name":`},
		{"no name", `{"role":"viewer"}`},
		{"unknown role", `{"name":"x","role":"superadmin"}`},
		// The instance-wide grant has its own route; a scope never issues it.
		{"tenant admin on a scope", `{"name":"x","tenant_admin":true,"expires_at":"` + expiry + `"}`},
		{"tenant admin without expiry", `{"name":"x","tenant_admin":true}`},
	} {
		for _, rt := range []struct {
			name  string
			fn    func(*Handler) http.HandlerFunc
			scope string
		}{
			{"fractal", func(h *Handler) http.HandlerFunc { return h.HandleCreateAPIKey }, fractalA},
			{"prism", func(h *Handler) http.HandlerFunc { return h.HandleCreatePrismAPIKey }, prismA},
		} {
			t.Run(rt.name+"/"+tt.name, func(t *testing.T) {
				db := newKeyDB()
				rec := call(rt.fn(newTestHandler(db)), http.MethodPost, tt.body, sessionUser(tenantAdmin), "id", rt.scope)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
				}
				if len(db.writes()) != 0 {
					t.Fatal("rejected request wrote a key")
				}
			})
		}
	}
}

func TestCreatePrismKey(t *testing.T) {
	db := newKeyDB()
	grantRoles(db)
	rec := call(newTestHandler(db).HandleCreatePrismAPIKey, http.MethodPost, `{"name":"ci","role":"viewer"}`, sessionUser(prismAdmin), "id", prismA)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var created CreateAPIKeyResponse
	_ = json.Unmarshal(decode(t, rec).Data, &created)
	if !strings.HasPrefix(created.Key, "bifract_Prism_A_") {
		t.Errorf("key %q does not carry the prism name", created.Key)
	}
	args := db.pg.Matching("INSERT INTO api_keys")[0].Args
	if args[4] != nil || args[5] != prismA {
		t.Errorf("scope bound as (%v, %v), want prism only", args[4], args[5])
	}
}

func TestCreatePrismKeyNeedsAKnownPrism(t *testing.T) {
	db := newKeyDB()
	h := newTestHandler(db)
	unknown := "55555555-5555-5555-5555-555555555555"
	if rec := call(h.HandleCreatePrismAPIKey, http.MethodPost, `{"name":"ci"}`, sessionUser(tenantAdmin), "id", unknown); rec.Code != http.StatusNotFound {
		t.Errorf("unknown prism: status = %d, want 404", rec.Code)
	}
	h.SetPrismResolver(nil)
	if rec := call(h.HandleCreatePrismAPIKey, http.MethodPost, `{"name":"ci"}`, sessionUser(tenantAdmin), "id", prismA); rec.Code != http.StatusInternalServerError {
		t.Errorf("no resolver: status = %d, want 500", rec.Code)
	}
	if len(db.writes()) != 0 {
		t.Fatal("wrote a key for a prism that could not be resolved")
	}
}

// The tenant route issues exactly one kind of grant. It sets the grant itself
// rather than trusting the body, so a body asking for a scoped role or for no
// tenant grant still yields a tenant admin key with no scope role.
func TestCreateTenantKey(t *testing.T) {
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	db := newKeyDB()
	h := newTestHandler(db)

	rec := call(h.HandleCreateTenantAPIKey, http.MethodPost, `{"name":"ops","role":"viewer","tenant_admin":false,"expires_at":"`+expiry+`"}`, sessionUser(tenantAdmin))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var created CreateAPIKeyResponse
	_ = json.Unmarshal(decode(t, rec).Data, &created)
	if !strings.HasPrefix(created.Key, "bifract_admin_") {
		t.Errorf("key %q lacks the tenant prefix", created.Key)
	}
	args := db.pg.Matching("INSERT INTO api_keys")[0].Args
	if args[4] != nil || args[5] != nil || args[8] != "" || args[9] != true {
		t.Errorf("bound scope=(%v,%v) role=%v tenant=%v, want unscoped tenant admin with no role", args[4], args[5], args[8], args[9])
	}

	for _, body := range []string{`{"name":"ops"}`, `{"expires_at":"` + expiry + `"}`, `nope`} {
		before := len(db.writes())
		if rec := call(h.HandleCreateTenantAPIKey, http.MethodPost, body, sessionUser(tenantAdmin)); rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rec.Code)
		}
		if len(db.writes()) != before {
			t.Errorf("body %s wrote a key", body)
		}
	}
}

func TestTenantRoutesAddressOnlyTenantKeys(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	routes := []route{
		{"get", func(h *Handler) http.HandlerFunc { return h.HandleGetTenantAPIKey }, http.MethodGet, "", true},
		{"update", func(h *Handler) http.HandlerFunc { return h.HandleUpdateTenantAPIKey }, http.MethodPut, `{"name":"renamed"}`, true},
		{"toggle", func(h *Handler) http.HandlerFunc { return h.HandleToggleTenantAPIKey }, http.MethodPost, "", true},
		{"delete", func(h *Handler) http.HandlerFunc { return h.HandleDeleteTenantAPIKey }, http.MethodDelete, "", true},
	}
	for _, rt := range routes {
		for _, tt := range []struct {
			key  string
			want int
		}{{"tk", http.StatusOK}, {"fk", http.StatusNotFound}, {"missing", http.StatusNotFound}} {
			t.Run(rt.name+"/"+tt.key, func(t *testing.T) {
				db := newKeyDB()
				db.add(keyRow{id: "tk", tenant: true, active: true, expires: &expiry})
				db.add(keyRow{id: "fk", fractalID: fractalA, active: true})
				rec := call(rt.fn(newTestHandler(db)), rt.method, rt.body, sessionUser(tenantAdmin), "keyId", tt.key)
				if rec.Code != tt.want {
					t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
				}
			})
		}
		t.Run(rt.name+"/no key id", func(t *testing.T) {
			rec := call(rt.fn(newTestHandler(newKeyDB())), rt.method, rt.body, sessionUser(tenantAdmin))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}
}

// An update may not turn a key into another kind of key, nor strip a tenant
// key's expiry requirement; refusals happen before any write.
func TestUpdateRefusesKindChange(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	tests := []struct {
		name string
		fn   func(*Handler) http.HandlerFunc
		body string
		ids  []string
	}{
		{"promote fractal key", func(h *Handler) http.HandlerFunc { return h.HandleUpdateAPIKey }, `{"tenant_admin":true}`, []string{"id", fractalA, "keyId", "fk"}},
		{"promote prism key", func(h *Handler) http.HandlerFunc { return h.HandleUpdatePrismAPIKey }, `{"tenant_admin":true}`, []string{"id", prismA, "keyId", "pk"}},
		{"demote tenant key", func(h *Handler) http.HandlerFunc { return h.HandleUpdateTenantAPIKey }, `{"tenant_admin":false}`, []string{"keyId", "tk"}},
		{"scope role on tenant key", func(h *Handler) http.HandlerFunc { return h.HandleUpdateTenantAPIKey }, `{"role":"admin"}`, []string{"keyId", "tk"}},
		{"unknown role", func(h *Handler) http.HandlerFunc { return h.HandleUpdateAPIKey }, `{"role":"god"}`, []string{"id", fractalA, "keyId", "fk"}},
		{"malformed fractal", func(h *Handler) http.HandlerFunc { return h.HandleUpdateAPIKey }, `{`, []string{"id", fractalA, "keyId", "fk"}},
		{"malformed prism", func(h *Handler) http.HandlerFunc { return h.HandleUpdatePrismAPIKey }, `{`, []string{"id", prismA, "keyId", "pk"}},
		{"malformed tenant", func(h *Handler) http.HandlerFunc { return h.HandleUpdateTenantAPIKey }, `{`, []string{"keyId", "tk"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newKeyDB()
			db.add(keyRow{id: "fk", fractalID: fractalA, role: "viewer", active: true})
			db.add(keyRow{id: "pk", prismID: prismA, role: "viewer", active: true})
			db.add(keyRow{id: "tk", tenant: true, active: true, expires: &expiry})
			rec := call(tt.fn(newTestHandler(db)), http.MethodPut, tt.body, sessionUser(tenantAdmin), tt.ids...)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if len(db.writes()) != 0 {
				t.Fatal("refused update wrote")
			}
		})
	}
}

func TestToggleReportsNewState(t *testing.T) {
	db := newKeyDB()
	db.add(keyRow{id: "fk", fractalID: fractalA, active: true})
	h := newTestHandler(db)
	for _, want := range []string{"API key deactivated successfully", "API key activated successfully"} {
		rec := call(h.HandleToggleAPIKey, http.MethodPost, "", sessionUser(tenantAdmin), "id", fractalA, "keyId", "fk")
		if got := decode(t, rec).Message; got != want {
			t.Errorf("message = %q, want %q", got, want)
		}
	}
}

func TestListAllAPIKeys(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	db := newKeyDB()
	db.add(keyRow{id: "fk", fractalID: fractalA, active: true, keyHash: "deadbeef"})
	db.add(keyRow{id: "tk", tenant: true, active: true, expires: &expiry, keyHash: "cafebabe"})
	h := newTestHandler(db)

	rec := call(h.HandleListAllAPIKeys, http.MethodGet, "", sessionUser(tenantAdmin))
	var keys []APIKey
	if err := json.Unmarshal(decode(t, rec).Data, &keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("listed %d keys, want 2", len(keys))
	}
	if body := rec.Body.String(); strings.Contains(body, "deadbeef") || strings.Contains(body, "cafebabe") {
		t.Fatalf("list leaks key hashes: %s", body)
	}

	db.failAll = errErr
	if rec := call(h.HandleListAllAPIKeys, http.MethodGet, "", sessionUser(tenantAdmin)); rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

// An empty list is [] on the wire, not null.
func TestListEmptyScope(t *testing.T) {
	rec := call(newTestHandler(newKeyDB()).HandleListAPIKeys, http.MethodGet, "", sessionUser(tenantAdmin), "id", fractalB)
	if got := string(decode(t, rec).Data); got != "[]" {
		t.Fatalf("data = %s, want []", got)
	}
}

func TestStorageFailuresAreServerErrors(t *testing.T) {
	db := newKeyDB()
	db.failAll = errErr
	h := newTestHandler(db)
	for _, tt := range []struct {
		name string
		fn   http.HandlerFunc
		body string
		ids  []string
	}{
		{"list fractal", h.HandleListAPIKeys, "", []string{"id", fractalA}},
		{"list prism", h.HandleListPrismAPIKeys, "", []string{"id", prismA}},
		{"create fractal", h.HandleCreateAPIKey, `{"name":"x"}`, []string{"id", fractalA}},
		{"create prism", h.HandleCreatePrismAPIKey, `{"name":"x"}`, []string{"id", prismA}},
		{"create tenant", h.HandleCreateTenantAPIKey, `{"name":"x","expires_at":"2099-01-01T00:00:00Z"}`, nil},
	} {
		if rec := call(tt.fn, http.MethodPost, tt.body, sessionUser(tenantAdmin), tt.ids...); rec.Code != http.StatusInternalServerError {
			t.Errorf("%s: status = %d, want 500", tt.name, rec.Code)
		}
	}
}

func TestMissingPathParams(t *testing.T) {
	h := newTestHandler(newKeyDB())
	for _, rt := range append(append([]route{}, fractalRoutes...), prismRoutes...) {
		if rec := call(rt.fn(h), rt.method, rt.body, sessionUser(tenantAdmin)); rec.Code != http.StatusBadRequest {
			t.Errorf("%s with no ids: status = %d, want 400", rt.name, rec.Code)
		}
		if rt.keyed {
			if rec := call(rt.fn(h), rt.method, rt.body, sessionUser(tenantAdmin), "id", fractalA); rec.Code != http.StatusBadRequest {
				t.Errorf("%s with no key id: status = %d, want 400", rt.name, rec.Code)
			}
		}
	}
}

var errErr = errors.New("database unavailable")

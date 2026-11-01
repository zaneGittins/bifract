package ingesttokens

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"bifract/pkg/storage"
)

var (
	tenantAdmin  = &storage.User{Username: "root", IsAdmin: true}
	fractalAdmin = &storage.User{Username: "fadmin"}
	analyst      = &storage.User{Username: "ana"}
	stranger     = &storage.User{Username: "nobody"}
)

type route struct {
	name   string
	fn     func(*Handler) http.HandlerFunc
	method string
	body   string
	keyed  bool
}

var routes = []route{
	{"list", func(h *Handler) http.HandlerFunc { return h.HandleListTokens }, http.MethodGet, "", false},
	{"create", func(h *Handler) http.HandlerFunc { return h.HandleCreateToken }, http.MethodPost, `{"name":"edge"}`, false},
	{"get", func(h *Handler) http.HandlerFunc { return h.HandleGetToken }, http.MethodGet, "", true},
	{"update", func(h *Handler) http.HandlerFunc { return h.HandleUpdateToken }, http.MethodPut, `{"name":"renamed"}`, true},
	{"toggle", func(h *Handler) http.HandlerFunc { return h.HandleToggleToken }, http.MethodPost, "", true},
	{"delete", func(h *Handler) http.HandlerFunc { return h.HandleDeleteToken }, http.MethodDelete, "", true},
}

func seedTokens(db *tokenDB) {
	db.roles[fractalA] = map[string]string{"fadmin": "admin", "ana": "analyst"}
	db.add(tokenRow{id: "a1", fractalID: fractalA, active: true, isDefault: true})
	db.add(tokenRow{id: "a2", fractalID: fractalA, active: true})
	db.add(tokenRow{id: "a3", fractalID: fractalA, active: true})
	db.add(tokenRow{id: "b1", fractalID: fractalB, active: true, isDefault: true})
	db.add(tokenRow{id: "b2", fractalID: fractalB, active: true})
	db.add(tokenRow{id: "b3", fractalID: fractalB, active: true})
}

// Token management takes admin on the fractal. A refused caller never reaches
// ingest_tokens.
func TestRoutesRequireFractalAdmin(t *testing.T) {
	tests := []struct {
		name    string
		who     *storage.User
		fractal string
		token   string
		want    int
	}{
		{"anonymous", nil, fractalA, "a2", http.StatusUnauthorized},
		{"no grant", stranger, fractalA, "a2", http.StatusForbidden},
		{"analyst", analyst, fractalA, "a2", http.StatusForbidden},
		{"admin of another fractal", fractalAdmin, fractalB, "b2", http.StatusForbidden},
		{"fractal admin", fractalAdmin, fractalA, "a2", http.StatusOK},
		{"tenant admin", tenantAdmin, fractalB, "b2", http.StatusOK},
	}
	for _, tt := range tests {
		for _, rt := range routes {
			t.Run(tt.name+"/"+rt.name, func(t *testing.T) {
				db := newTokenDB()
				seedTokens(db)
				params := []string{"id", tt.fractal}
				if rt.keyed {
					params = append(params, "tokenId", tt.token)
				}
				rec := call(rt.fn(newTestHandler(db, NewTokenCache(time.Hour))), rt.method, rt.body, asUser(tt.who), params...)
				if rec.Code != tt.want {
					t.Fatalf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body.String())
				}
				if tt.want != http.StatusOK && len(db.pg.Matching("ingest_tokens")) != 0 {
					t.Fatal("refused caller reached ingest_tokens")
				}
			})
		}
	}
}

// A token id from another fractal is not reachable through this one.
func TestRoutesDoNotReachOtherFractalsTokens(t *testing.T) {
	for _, rt := range routes {
		if !rt.keyed {
			continue
		}
		t.Run(rt.name, func(t *testing.T) {
			db := newTokenDB()
			seedTokens(db)
			before, _ := db.get("b2")
			rec := call(rt.fn(newTestHandler(db, nil)), rt.method, rt.body, asUser(tenantAdmin), "id", fractalA, "tokenId", "b2")
			if rec.Code == http.StatusOK {
				t.Fatalf("reached fractal B's token through fractal A: %s", rec.Body.String())
			}
			if after, ok := db.get("b2"); !ok || after != before {
				t.Fatal("fractal B's token changed")
			}
		})
	}
}

func TestCreateTokenResponse(t *testing.T) {
	db := newTokenDB()
	seedTokens(db)
	rec := call(newTestHandler(db, nil).HandleCreateToken, http.MethodPost, `{"name":"edge","parser_type":"kv"}`, asUser(fractalAdmin), "id", fractalA)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp CreateTokenResponse
	if err := json.Unmarshal(decode(t, rec).Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.Token, "bifract_ingest_"+resp.TokenPrefix) {
		t.Errorf("token %q does not start with its prefix %q", resp.Token, resp.TokenPrefix)
	}
	if resp.IngestToken.FractalID != fractalA || resp.IngestToken.ParserType != "kv" {
		t.Errorf("token = %+v", resp.IngestToken)
	}
	args := db.pg.Matching("INSERT INTO ingest_tokens")[0].Args
	if args[3] != HashToken(resp.Token) {
		t.Error("stored hash is not the hash of the returned token")
	}
	if args[9] != "fadmin" {
		t.Errorf("created_by = %v, want the session user", args[9])
	}
	// The hash is never part of any response.
	if strings.Contains(rec.Body.String(), HashToken(resp.Token)) {
		t.Fatal("create response carries the token hash")
	}
	list := call(newTestHandler(db, nil).HandleListTokens, http.MethodGet, "", asUser(fractalAdmin), "id", fractalA).Body.String()
	if strings.Contains(list, HashToken(resp.Token)) || strings.Contains(list, "hash") {
		t.Fatalf("list carries token hashes: %s", list)
	}
}

func TestCreateTokenRejectsBadInput(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		want       int
	}{
		{"malformed", `{"name":`, http.StatusBadRequest},
		{"no name", `{"parser_type":"json"}`, http.StatusBadRequest},
		{"bad parser", `{"name":"x","parser_type":"xml"}`, http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := newTokenDB()
			rec := call(newTestHandler(db, nil).HandleCreateToken, http.MethodPost, tt.body, asUser(tenantAdmin), "id", fractalA)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if len(db.writes()) != 0 {
				t.Fatal("rejected request wrote a token")
			}
		})
	}
}

func TestUnknownFractal(t *testing.T) {
	unknown := "99999999-9999-9999-9999-999999999999"
	for _, rt := range routes[:2] { // list and create check the fractal exists
		db := newTokenDB()
		rec := call(rt.fn(newTestHandler(db, nil)), rt.method, rt.body, asUser(tenantAdmin), "id", unknown)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404", rt.name, rec.Code)
		}
		if len(db.writes()) != 0 {
			t.Errorf("%s wrote for an unknown fractal", rt.name)
		}
	}
}

// Validated tokens are cached on the ingest path. Any change that can revoke
// or re-route a token must drop the cache, or the old grant keeps working for
// the cache TTL.
func TestMutationsInvalidateCache(t *testing.T) {
	for _, rt := range routes {
		if rt.name == "list" || rt.name == "get" || rt.name == "create" {
			continue
		}
		t.Run(rt.name, func(t *testing.T) {
			db := newTokenDB()
			seedTokens(db)
			cache := NewTokenCache(time.Hour)
			cache.Set(HashToken("bifract_ingest_x"), &ValidatedToken{TokenID: "a2", FractalID: fractalA}, cache.Generation())
			rec := call(rt.fn(newTestHandler(db, cache)), rt.method, rt.body, asUser(tenantAdmin), "id", fractalA, "tokenId", "a2")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
			}
			if _, ok := cache.Get(HashToken("bifract_ingest_x")); ok {
				t.Fatal("cache still serves the token after " + rt.name)
			}
		})
	}
}

func TestDeleteAndToggleRefusals(t *testing.T) {
	db := newTokenDB()
	seedTokens(db)
	h := newTestHandler(db, NewTokenCache(time.Hour))
	if rec := call(h.HandleDeleteToken, http.MethodDelete, "", asUser(tenantAdmin), "id", fractalA, "tokenId", "a1"); rec.Code != http.StatusBadRequest {
		t.Errorf("delete default: status = %d, want 400", rec.Code)
	}
	if _, ok := db.get("a1"); !ok {
		t.Fatal("default token deleted")
	}

	// Deactivate down to one, then the last must stay on.
	if rec := call(h.HandleToggleToken, http.MethodPost, "", asUser(tenantAdmin), "id", fractalA, "tokenId", "a2"); decode(t, rec).Message != "Ingest token deactivated successfully" {
		t.Fatalf("toggle a2: %s", rec.Body.String())
	}
	if rec := call(h.HandleToggleToken, http.MethodPost, "", asUser(tenantAdmin), "id", fractalA, "tokenId", "a3"); rec.Code != http.StatusOK {
		t.Fatalf("toggle a3: %s", rec.Body.String())
	}
	if rec := call(h.HandleToggleToken, http.MethodPost, "", asUser(tenantAdmin), "id", fractalA, "tokenId", "a1"); rec.Code != http.StatusBadRequest {
		t.Fatalf("deactivating the last active token: status = %d, want 400", rec.Code)
	}
	if rec := call(h.HandleToggleToken, http.MethodPost, "", asUser(tenantAdmin), "id", fractalA, "tokenId", "a2"); decode(t, rec).Message != "Ingest token activated successfully" {
		t.Fatalf("reactivate a2: %s", rec.Body.String())
	}
}

func TestBadRequests(t *testing.T) {
	h := newTestHandler(newTokenDB(), nil)
	for _, rt := range routes {
		if rec := call(rt.fn(h), rt.method, rt.body, asUser(tenantAdmin)); rec.Code != http.StatusBadRequest {
			t.Errorf("%s with no fractal: status = %d, want 400", rt.name, rec.Code)
		}
		if rt.keyed {
			if rec := call(rt.fn(h), rt.method, rt.body, asUser(tenantAdmin), "id", fractalA); rec.Code != http.StatusBadRequest {
				t.Errorf("%s with no token: status = %d, want 400", rt.name, rec.Code)
			}
		}
	}
	if rec := call(h.HandleUpdateToken, http.MethodPut, "{", asUser(tenantAdmin), "id", fractalA, "tokenId", "a1"); rec.Code != http.StatusBadRequest {
		t.Errorf("malformed update: status = %d", rec.Code)
	}
}

package prisms

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// principal builds the context the auth middleware produces: a session credits
// its own user, an API key credits the key's creator (empty once deleted).
func principal(r *http.Request, user, attribution string) *http.Request {
	ctx := context.WithValue(r.Context(), "user", &storage.User{Username: user, IsAdmin: true})
	ctx = context.WithValue(ctx, storage.AttributionUserKey, attribution)
	return r.WithContext(ctx)
}

// granted_by is a foreign key into users, which an API key's apikey_<id>
// principal has no row in.
func TestHandleGrantPrismPermissionRecordsAttributedUser(t *testing.T) {
	for name, tc := range map[string]struct {
		user, attribution string
		want              any
	}{
		"session":               {"alice", "alice", "alice"},
		"api key":               {"apikey_k1", "bob", "bob"},
		"api key, creator gone": {"apikey_k1", "", nil},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			h := NewHandler(NewManager(pg), pg, nil)
			r := httptest.NewRequest("POST", "/prisms/p1/permissions", strings.NewReader(`{"username":"carol","role":"viewer"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "p1")
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
			h.HandleGrantPrismPermission(httptest.NewRecorder(), principal(r, tc.user, tc.attribution))

			ins := fake.Matching("INSERT INTO prism_permissions")
			if len(ins) != 1 || ins[0].Args[4] != tc.want {
				t.Fatalf("granted_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

// An API key's principal is not a user, so there is nobody to auto-grant.
func TestHandleCreatePrismGrantsOnlyAUserCreator(t *testing.T) {
	for name, tc := range map[string]struct {
		user, attribution string
		wantGrants        int
	}{
		"session": {"alice", "alice", 1},
		"api key": {"apikey_k1", "bob", 0},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			fake.On("INSERT INTO prisms", fakedb.Row("p1", "p", "", tc.attribution, time.Now(), time.Now()))
			h := NewHandler(NewManager(pg), pg, nil)
			r := httptest.NewRequest("POST", "/prisms", strings.NewReader(`{"name":"p"}`))
			w := httptest.NewRecorder()
			h.HandleCreatePrism(w, principal(r, tc.user, tc.attribution))
			if w.Code != http.StatusOK {
				t.Fatalf("code = %d", w.Code)
			}

			grants := fake.Matching("INSERT INTO prism_permissions")
			if len(grants) != tc.wantGrants {
				t.Fatalf("grants = %+v, want %d", grants, tc.wantGrants)
			}
			if tc.wantGrants == 1 && (grants[0].Args[1] != tc.user || grants[0].Args[4] != tc.user) {
				t.Fatalf("grant = %+v, want the creator as grantee and grantor", grants[0])
			}
		})
	}
}

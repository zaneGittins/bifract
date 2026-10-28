package fractals

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// granted_by is a foreign key into users, which an API key's apikey_<id>
// principal has no row in.
func TestHandleGrantPermissionRecordsAttributedUser(t *testing.T) {
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
			h := NewHandler(nil, nil, nil)
			h.SetRBAC(pg, nil)
			r := httptest.NewRequest("POST", "/fractals/f1/permissions", strings.NewReader(`{"username":"carol","role":"viewer"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "f1")
			ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
			ctx = context.WithValue(ctx, "user", &storage.User{Username: tc.user, IsAdmin: true})
			ctx = context.WithValue(ctx, storage.AttributionUserKey, tc.attribution)
			h.HandleGrantPermission(httptest.NewRecorder(), r.WithContext(ctx))

			ins := fake.Matching("INSERT INTO fractal_permissions")
			if len(ins) != 1 || ins[0].Args[4] != tc.want {
				t.Fatalf("granted_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

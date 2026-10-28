package groups

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// added_by is a foreign key into users. An API key authenticates as a
// synthetic apikey_<id> user with no row there, so the insert must credit the
// attributed human, or write NULL when there is none.
func TestHandleAddMemberRecordsAttributedUser(t *testing.T) {
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
			fake.On("FROM users", fakedb.Row("carol", nil, "Carol", "#000", "C", time.Now(), nil, false, "local", false, true, "UTC", "", false, int64(0)))
			h := NewHandler(pg)
			r := httptest.NewRequest("POST", "/groups/g1/members", strings.NewReader(`{"username":"carol"}`))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "g1")
			ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
			ctx = context.WithValue(ctx, "user", &storage.User{Username: tc.user})
			ctx = context.WithValue(ctx, storage.AttributionUserKey, tc.attribution)
			h.HandleAddMember(httptest.NewRecorder(), r.WithContext(ctx))

			ins := fake.Matching("INSERT INTO group_members")
			if len(ins) != 1 || ins[0].Args[2] != tc.want {
				t.Fatalf("added_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

package contextlinks

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// created_by is a foreign key into users, which an API key's apikey_<id>
// principal has no row in.
func TestHandleCreateRecordsAttributedUser(t *testing.T) {
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
			h := NewHandler(NewManager(pg))
			body := `{"short_name":"vt","context_link":"https://example.test/{{value}}","match_fields":["hash"]}`
			r := httptest.NewRequest("POST", "/context-links", strings.NewReader(body))
			ctx := context.WithValue(r.Context(), "user", &storage.User{Username: tc.user})
			ctx = context.WithValue(ctx, storage.AttributionUserKey, tc.attribution)
			h.HandleCreate(httptest.NewRecorder(), r.WithContext(ctx))

			ins := fake.Matching("INSERT INTO context_links")
			if len(ins) != 1 || ins[0].Args[6] != tc.want {
				t.Fatalf("created_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

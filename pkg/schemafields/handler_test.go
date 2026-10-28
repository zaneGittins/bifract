package schemafields

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// created_by credits the person behind the request, never an API key's
// apikey_<id> principal. The column is NOT NULL, so nobody is stored as "".
func TestHandleCreateRecordsAttributedUser(t *testing.T) {
	for name, tc := range map[string]struct {
		user, attribution string
		want              any
	}{
		"session":               {"alice", "alice", "alice"},
		"api key":               {"apikey_k1", "bob", "bob"},
		"api key, creator gone": {"apikey_k1", "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			pg, fake := storagetest.Postgres()
			h := NewHandler(NewManager(pg), nil, nil)
			r := httptest.NewRequest("POST", "/schema-fields", strings.NewReader(`{"field_name":"custom_x"}`))
			ctx := context.WithValue(r.Context(), "user", &storage.User{Username: tc.user})
			ctx = context.WithValue(ctx, storage.AttributionUserKey, tc.attribution)
			h.HandleCreate(httptest.NewRecorder(), r.WithContext(ctx))

			ins := fake.Matching("INSERT INTO clickhouse_schema_fields")
			if len(ins) != 1 || ins[0].Args[2] != tc.want {
				t.Fatalf("created_by = %+v, want %v", ins, tc.want)
			}
		})
	}
}

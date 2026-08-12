package auth

import (
	"context"
	"testing"

	"bifract/pkg/storage"
)

func TestAttributionUsername(t *testing.T) {
	sessionUser := &storage.User{Username: "analyst1"}
	apiKeyUser := &storage.User{Username: "apikey_b42bd5e5"}

	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{
			name: "session user attributes to itself",
			ctx:  context.WithValue(context.Background(), "user", sessionUser),
			want: "analyst1",
		},
		{
			name: "api key attributes to the key creator, never the synthetic principal",
			ctx: context.WithValue(
				context.WithValue(
					context.WithValue(context.Background(), "user", apiKeyUser),
					"auth_type", "api_key"),
				"api_key", &ValidatedAPIKey{KeyID: "b42bd5e5", CreatedBy: "admin"}),
			want: "admin",
		},
		{
			name: "api key whose creator was deleted has no attribution",
			ctx: context.WithValue(
				context.WithValue(
					context.WithValue(context.Background(), "user", apiKeyUser),
					"auth_type", "api_key"),
				"api_key", &ValidatedAPIKey{KeyID: "b42bd5e5"}),
			want: "",
		},
		{
			name: "unauthenticated context has no attribution",
			ctx:  context.Background(),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AttributionUsername(tt.ctx); got != tt.want {
				t.Errorf("AttributionUsername() = %q, want %q", got, tt.want)
			}
			// Whatever the principal, the value must never be a username that
			// only exists in memory: those columns are foreign keys.
			if got := AttributionUsername(tt.ctx); got == apiKeyUser.Username {
				t.Errorf("AttributionUsername() returned the synthetic API key principal %q", got)
			}
		})
	}
}

func TestNullableUserBindsEmptyAsNull(t *testing.T) {
	// The empty string is not a users row, so it violates the foreign key
	// exactly like an unknown username does. It must bind as NULL.
	if v := storage.NullableUser(""); v != nil {
		t.Errorf("NullableUser(\"\") = %v, want nil", v)
	}
	if v := storage.NullableUser("admin"); v != "admin" {
		t.Errorf("NullableUser(\"admin\") = %v, want \"admin\"", v)
	}
}

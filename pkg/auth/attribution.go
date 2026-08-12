package auth

import (
	"context"
	"fmt"
	"net/http"

	"bifract/pkg/storage"
)

// AttributionUsername returns the username to record in a `created_by` /
// `updated_by` column for the current principal. Those columns are foreign
// keys against users(username), so they may only ever hold a username that
// exists in the users table, or NULL.
//
// API key requests authenticate as a synthetic "apikey_<id>" principal that
// has no users row, so writes are attributed to the user who created the key
// (the same rule comments already use). An empty result means "no attributable
// user" and must be stored as NULL, not as an empty string; pass it through
// storage.NullableUser at the bind site.
// DenyAPIKey blocks API key requests from a route group that manages per-user
// state. Those rows key off users(username), which a synthetic principal cannot
// satisfy, and widening them to the key's creator would hand a machine
// credential that user's private data. Session requests pass through.
func DenyAPIKey(what string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if IsAPIKey(r.Context()) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprintf(w, `{"success":false,"error":"%s is per-user and not available for API key authentication"}`, what)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// IsAPIKey reports whether the request authenticated with an API key rather
// than a user session. Per-user state (favorites, presence, query history,
// notification reads) keys off users(username) and has no meaning for a
// machine principal, so those endpoints reject API keys instead of writing a
// username that cannot satisfy the foreign key.
func IsAPIKey(ctx context.Context) bool {
	authType, _ := ctx.Value("auth_type").(string)
	return authType == "api_key"
}

func AttributionUsername(ctx context.Context) string {
	if authType, _ := ctx.Value("auth_type").(string); authType == "api_key" {
		if keyData, ok := ctx.Value("api_key").(*ValidatedAPIKey); ok {
			return keyData.CreatedBy
		}
		return ""
	}
	if user, ok := ctx.Value("user").(*storage.User); ok && user != nil {
		return user.Username
	}
	return ""
}

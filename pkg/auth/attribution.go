package auth

import (
	"context"
	"fmt"
	"net/http"
)

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

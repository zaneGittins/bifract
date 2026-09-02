package main

import (
	"net/http"
	"testing"
)

// bodylessRoutes are the POST/PUT/PATCH operations that genuinely take no
// request body: toggles, syncs, selects, cancels. They are written out so that
// a route which does take a body cannot reach the description undeclared, which
// is how POST /query shipped for a while showing no body and no way to run a
// query from the explorer.
var bodylessRoutes = []string{
	"POST /api/v1/admin/kill-query",
	"POST /api/v1/admin/schema-fields/ignore/{name}",
	"POST /api/v1/admin/schema-fields/refresh",
	"POST /api/v1/alert-changes/{id}/discard",
	"POST /api/v1/alert-changes/{id}/merge",
	"POST /api/v1/alerts/{id}/duplicate",
	"POST /api/v1/api-keys/{keyId}/toggle",
	"POST /api/v1/auth/logout",
	"POST /api/v1/auth/mfa/enroll",
	"POST /api/v1/dashboards/{id}/execute",
	"POST /api/v1/dashboards/{id}/presence",
	"POST /api/v1/dictionaries/{id}/columns/{name}/key",
	"POST /api/v1/dictionaries/{id}/reload",
	"POST /api/v1/email-actions/{id}/test",
	"POST /api/v1/feeds/{id}/alerts/disable-all",
	"POST /api/v1/feeds/{id}/alerts/enable-all",
	"POST /api/v1/feeds/{id}/sync",
	"POST /api/v1/fractals/stats/refresh",
	"POST /api/v1/fractals/{id}/api-keys/{keyId}/toggle",
	"POST /api/v1/fractals/{id}/ingest-tokens/{tokenId}/toggle",
	"POST /api/v1/fractals/{id}/select",
	"POST /api/v1/instruction-libraries/{id}/sync",
	"POST /api/v1/models/{id}/backfill/cancel",
	"POST /api/v1/models/{id}/disable-alert",
	"POST /api/v1/models/{id}/enable-alert",
	"POST /api/v1/normalizers/{id}/duplicate",
	"POST /api/v1/normalizers/{id}/set-default",
	"POST /api/v1/notebooks/active",
	"POST /api/v1/notebooks/{id}/lock",
	"POST /api/v1/notebooks/{id}/presence",
	"POST /api/v1/notebooks/{id}/sections/{section_id}/summarize",
	"POST /api/v1/notifications/read",
	"POST /api/v1/prisms/{id}/api-keys/{keyId}/toggle",
	"POST /api/v1/prisms/{id}/select",
	"POST /api/v1/recall/{fractalID}/{id}/cancel",
	"POST /api/v1/saved-queries/{id}/favorite",
	"POST /api/v1/saved-queries/{id}/use",
	"POST /api/v1/system/archive/clear",
	"POST /api/v1/system/archive/maintain/run",
	"POST /api/v1/system/archive/restore/{id}/cancel",
	"POST /api/v1/system/archive/restore/{id}/resume",
	"POST /api/v1/system/archive/spool/clear",
	"POST /api/v1/webhooks/{id}/test",
}

// TestRequestBodiesAreDeclared requires every operation that can carry a body to
// say what that body is: a Go type in Request, a media type in Consumes for raw
// input, or membership of bodylessRoutes. Silence is a failure, because silence
// is indistinguishable from an omission.
func TestRequestBodiesAreDeclared(t *testing.T) {
	mux, registry := buildRouter(testDeps())

	bodyless := map[string]bool{}
	for _, r := range bodylessRoutes {
		bodyless[r] = true
	}

	mounted := map[string]bool{}
	for _, route := range walkRouter(t, mux, registry) {
		mounted[route.key()] = true

		switch route.method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
		default:
			continue
		}
		described, ok := registry.Lookup(route.method, route.path)
		if !ok {
			continue
		}
		if described.Request != nil || described.Consumes != "" || bodyless[route.key()] {
			continue
		}
		t.Errorf("%s declares no request body.\n"+
			"Set Request to the type it decodes, or Consumes to the media type it reads raw.\n"+
			"If it genuinely takes no body, add it to bodylessRoutes.", route.key())
	}

	for _, r := range bodylessRoutes {
		if !mounted[r] {
			t.Errorf("%s is listed in bodylessRoutes but is no longer mounted; remove it", r)
		}
	}
}

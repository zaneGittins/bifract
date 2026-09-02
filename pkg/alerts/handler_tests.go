package alerts

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/rbac"
)

// RunTestsRequest asks for a test run against an arbitrary query.
//
// The query and the tests both come from the request rather than from storage, so the
// editor can run what is on screen: an unsaved edit, or a brand new alert that has no
// row yet.
type RunTestsRequest struct {
	// SessionID identifies one editor. Reusing it across runs keeps the loaded events
	// in place, so iterating on a query does not reload the corpus each time.
	SessionID   string      `json:"session_id"`
	QueryString string      `json:"query_string"`
	Tests       []AlertTest `json:"tests"`
}

// SetTestRunner wires in the runner. Without it the tests endpoints report that test
// runs are unavailable rather than failing obscurely.
func (h *Handler) SetTestRunner(runner *TestRunner) {
	h.testRunner = runner
}

// HandleListTests returns an alert's saved tests.
func (h *Handler) HandleListTests(w http.ResponseWriter, r *http.Request) {
	_, alertID, ok := h.alertForRevisionAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	tests, err := h.manager.ListTests(r.Context(), alertID)
	if err != nil {
		log.Printf("[Alerts] Failed to list tests: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load tests")
		return
	}

	h.respondSuccess(w, tests)
}

// HandleRunTests evaluates a query against a set of tests.
func (h *Handler) HandleRunTests(w http.ResponseWriter, r *http.Request) {
	if !h.testRunner.Available() {
		h.respondError(w, http.StatusServiceUnavailable, "Test runs are unavailable: no ClickHouse connection")
		return
	}
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	var req RunTestsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if strings.TrimSpace(req.QueryString) == "" {
		h.respondError(w, http.StatusBadRequest, "Query string is required")
		return
	}
	if strings.TrimSpace(req.SessionID) == "" {
		h.respondError(w, http.StatusBadRequest, "Session ID is required")
		return
	}

	// Namespaced by principal so one user's session id cannot address another's
	// loaded events.
	sessionKey := h.attributionUser(r) + ":" + req.SessionID

	result, err := h.testRunner.Run(r.Context(), sessionKey, req.QueryString, req.Tests)
	if err != nil {
		if strings.Contains(err.Error(), "invalid query syntax") ||
			strings.Contains(err.Error(), "test ") ||
			strings.Contains(err.Error(), "at most") {
			h.respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		log.Printf("[Alerts] Test run failed: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Test run failed: "+err.Error())
		return
	}

	result.SessionID = req.SessionID
	h.respondSuccess(w, result)
}

// HandleReleaseTestSession drops an editor's loaded events when it closes. Sessions
// also expire on their own, so a client that never calls this leaks nothing for long.
func (h *Handler) HandleReleaseTestSession(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	sessionID := chi.URLParam(r, "sessionId")
	if sessionID == "" {
		h.respondError(w, http.StatusBadRequest, "Session ID is required")
		return
	}

	h.testRunner.Release(r.Context(), h.attributionUser(r)+":"+sessionID)
	h.respondSuccess(w, map[string]bool{"released": true})
}

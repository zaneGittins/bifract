package alerts

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
)

// RestoreRevisionRequest is the body of a restore.
type RestoreRevisionRequest struct {
	// DropMissingActions re-applies the definition without action references that no
	// longer resolve. Without it, such a restore is refused so the wiring is never
	// silently lost.
	DropMissingActions bool `json:"drop_missing_actions"`
}

// alertForRevisionAccess loads the alert and enforces the given role on its scope.
func (h *Handler) alertForRevisionAccess(w http.ResponseWriter, r *http.Request, required rbac.Role) (*Alert, string, bool) {
	alertID := chi.URLParam(r, "id")
	if alertID == "" {
		h.respondError(w, http.StatusBadRequest, "Alert ID is required")
		return nil, "", false
	}

	alert, err := h.manager.GetAlert(r.Context(), alertID)
	if err != nil {
		if errors.Is(err, ErrAlertNotFound) || strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			log.Printf("[Alerts] Failed to load alert for revisions: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load alert")
		}
		return nil, "", false
	}

	if alert.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, alert.FractalID, required) {
			return nil, "", false
		}
	} else if alert.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, alert.PrismID, required) {
			return nil, "", false
		}
	} else if !h.requireRole(w, r, required) {
		return nil, "", false
	}

	return alert, alertID, true
}

// HandleListRevisions returns an alert's definition history, newest first.
func (h *Handler) HandleListRevisions(w http.ResponseWriter, r *http.Request) {
	_, alertID, ok := h.alertForRevisionAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	revisions, err := h.manager.ListRevisions(r.Context(), alertID)
	if err != nil {
		log.Printf("[Alerts] Failed to list revisions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load revision history")
		return
	}

	h.respondSuccess(w, revisions)
}

// HandleGetRevision returns one stored revision.
func (h *Handler) HandleGetRevision(w http.ResponseWriter, r *http.Request) {
	_, alertID, ok := h.alertForRevisionAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	number, err := strconv.Atoi(chi.URLParam(r, "revision"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Revision must be a number")
		return
	}

	revision, err := h.manager.GetRevision(r.Context(), alertID, number)
	if err != nil {
		if errors.Is(err, ErrRevisionNotFound) {
			h.respondError(w, http.StatusNotFound, "Revision not found")
		} else {
			log.Printf("[Alerts] Failed to load revision: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load revision")
		}
		return
	}

	h.respondSuccess(w, revision)
}

// HandleRestoreRevision re-applies a stored definition as a new revision at the head.
func (h *Handler) HandleRestoreRevision(w http.ResponseWriter, r *http.Request) {
	_, alertID, ok := h.alertForRevisionAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	number, err := strconv.Atoi(chi.URLParam(r, "revision"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Revision must be a number")
		return
	}

	// The body is optional: a plain restore sends none.
	var req RestoreRevisionRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	alert, err := h.manager.RestoreRevision(r.Context(), alertID, number, h.attributionUser(r), req.DropMissingActions)
	if err != nil {
		if h.gateRequiredResponse(w, err) || h.policyBlockedResponse(w, err) {
			return
		}

		var blocked *RestoreBlockedError
		switch {
		case errors.Is(err, ErrRevisionNotFound):
			h.respondError(w, http.StatusNotFound, "Revision not found")
		case errors.Is(err, ErrAlertNotFound):
			h.respondError(w, http.StatusNotFound, "Alert not found")
		case errors.As(err, &blocked):
			// The standard envelope, with the unresolved references as data so the
			// client can offer to restore without them.
			api.WriteJSON(w, http.StatusConflict, api.Response[[]MissingActionRef]{
				Success: false,
				Error:   blocked.Error(),
				Code:    api.CodeForStatus(http.StatusConflict),
				Data:    blocked.Missing,
			})
		case strings.Contains(err.Error(), "invalid query syntax"), strings.Contains(err.Error(), "cannot use aggregate"):
			h.respondError(w, http.StatusBadRequest, err.Error())
		default:
			log.Printf("[Alerts] Failed to restore revision: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to restore revision")
		}
		return
	}

	h.respondSuccess(w, alert)
}

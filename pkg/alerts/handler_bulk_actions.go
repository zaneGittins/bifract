package alerts

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// BulkActionRequest adds and removes actions across many alerts.
type BulkActionRequest struct {
	Changes []ActionChange `json:"changes"`
	// Summary tells reviewers why, when the scope reviews changes.
	Summary string `json:"summary,omitempty"`
}

// HandlePreviewBulkActions works out a bulk action edit without applying it.
func (h *Handler) HandlePreviewBulkActions(w http.ResponseWriter, r *http.Request) {
	req, fractalID, prismID, ok := h.bulkActionRequest(w, r)
	if !ok {
		return
	}
	plan, err := h.manager.PreviewActionChanges(r.Context(), BulkActionChange{Changes: req.Changes}, fractalID, prismID)
	if err != nil {
		h.bulkActionError(w, err)
		return
	}
	h.respondSuccess(w, plan)
}

// HandleApplyBulkActions applies a bulk action edit, or opens it as one proposal when
// the scope reviews changes.
func (h *Handler) HandleApplyBulkActions(w http.ResponseWriter, r *http.Request) {
	req, fractalID, prismID, ok := h.bulkActionRequest(w, r)
	if !ok {
		return
	}
	plan, err := h.manager.ApplyActionChanges(r.Context(), BulkActionChange{Changes: req.Changes}, req.Summary,
		fractalID, prismID, storage.AttributionUser(r.Context()))
	if err != nil {
		h.bulkActionError(w, err)
		return
	}
	h.respondSuccess(w, plan)
}

func (h *Handler) bulkActionRequest(w http.ResponseWriter, r *http.Request) (BulkActionRequest, string, string, bool) {
	var req BulkActionRequest
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return req, "", "", false
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return req, "", "", false
	}
	fractalID, prismID, ok := h.requireScope(w, r, "bulk action edit")
	return req, fractalID, prismID, ok
}

func (h *Handler) bulkActionError(w http.ResponseWriter, err error) {
	var blocked *ActionChangeBlockedError
	switch {
	case errors.As(err, &blocked):
		// The plan names each alert and the rule it would break, so the client can
		// offer to leave those out.
		api.WriteJSON(w, http.StatusUnprocessableEntity, api.Response[*BulkActionPlan]{
			Success: false,
			Error:   blocked.Error(),
			Code:    api.CodeForStatus(http.StatusUnprocessableEntity),
			Data:    blocked.Plan,
		})
	case errors.Is(err, ErrInvalidActionChange):
		h.respondError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrBulkActionAlerts):
		h.respondError(w, http.StatusNotFound, err.Error())
	default:
		log.Printf("[Alerts] Bulk action edit failed: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update alert actions")
	}
}

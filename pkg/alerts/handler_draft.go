package alerts

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
)

// SaveDraftRequest is an autosave from the editor. ID is empty for the first save of a
// new alert's draft; an alert-scoped draft is found by its alert regardless.
type SaveDraftRequest struct {
	ID      string           `json:"id,omitempty"`
	AlertID string           `json:"alert_id,omitempty"`
	Title   string           `json:"title,omitempty"`
	Summary string           `json:"summary,omitempty"`
	Content *RevisionContent `json:"content"`
	Tests   []AlertTest      `json:"tests,omitempty"`
}

// HandleListDrafts returns the caller's drafts in scope.
func (h *Handler) HandleListDrafts(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	list, err := h.manager.ListDrafts(r.Context(), fractalID, prismID, h.attributionUser(r))
	if err != nil {
		log.Printf("[Alerts] Failed to list drafts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load drafts")
		return
	}
	api.WriteJSON(w, http.StatusOK, api.Response[[]*ChangeRequest]{Success: true, Data: list})
}

// HandleDraftForAlert returns the caller's draft for one alert, or null.
func (h *Handler) HandleDraftForAlert(w http.ResponseWriter, r *http.Request) {
	_, alertID, ok := h.alertForRevisionAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	draft, err := h.manager.DraftForAlert(r.Context(), alertID, h.attributionUser(r))
	if err != nil {
		log.Printf("[Alerts] Failed to find draft: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load draft")
		return
	}
	h.respondSuccess(w, draft)
}

// HandleSaveDraft creates or updates the caller's draft.
func (h *Handler) HandleSaveDraft(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	var req SaveDraftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	in := ChangeRequestInput{AlertID: req.AlertID, Title: req.Title, Summary: req.Summary, Content: req.Content, Tests: req.Tests}
	draft, err := h.manager.SaveDraft(r.Context(), req.ID, fractalID, prismID, in, h.attributionUser(r))
	if err != nil {
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, draft)
}

// HandleSubmitDraft turns the caller's draft into an open proposal.
func (h *Handler) HandleSubmitDraft(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst); !ok {
		return
	}

	cr, err := h.manager.SubmitDraft(r.Context(), chi.URLParam(r, "id"), h.attributionUser(r))
	if err != nil {
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, cr)
}

// HandleDeleteDraft removes the caller's own draft.
func (h *Handler) HandleDeleteDraft(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst); !ok {
		return
	}

	if err := h.manager.DeleteDraft(r.Context(), chi.URLParam(r, "id"), h.attributionUser(r)); err != nil {
		if errors.Is(err, ErrChangeRequestNotFound) {
			h.respondError(w, http.StatusNotFound, "Draft not found")
			return
		}
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

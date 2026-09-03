package alerts

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
)

// ReviewRequest is a reviewer's decision.
type ReviewRequest struct {
	Decision string `json:"decision"`
	Comment  string `json:"comment"`
}

// ChangeRequestDetail is a proposal with everything a reviewer needs to judge it: the
// diff against what is live, the tests, and the policy checks.
type ChangeRequestDetail struct {
	*ChangeRequest
	Current   *RevisionContent `json:"current,omitempty"`
	Readiness *MergeReadiness  `json:"readiness,omitempty"`
	CanMerge  bool             `json:"can_merge"`
	CanReview bool             `json:"can_review"`
	IsAuthor  bool             `json:"is_author"`
}

// changeRequestAccess loads a proposal and enforces the required role on the scope that
// proposal belongs to.
//
// Authorizing against the caller's own selected scope would be a tenant isolation hole:
// a proposal is addressed by UUID, so an analyst in one fractal could otherwise read,
// approve, merge or delete a proposal belonging to another. The role must be checked
// against the resource, never against whatever scope the caller happens to have selected.
func (h *Handler) changeRequestAccess(w http.ResponseWriter, r *http.Request, required rbac.Role) (*ChangeRequest, bool) {
	crID := chi.URLParam(r, "id")
	if crID == "" {
		h.respondError(w, http.StatusBadRequest, "Proposal ID is required")
		return nil, false
	}

	cr, err := h.manager.GetChangeRequest(r.Context(), crID)
	if err != nil {
		h.changeRequestError(w, err)
		return nil, false
	}
	// A draft exists only for its author. Anyone else is told it does not exist,
	// which is both the truth of the matter and the answer that leaks nothing.
	if cr.Draft() && cr.Author != h.attributionUser(r) {
		h.respondError(w, http.StatusNotFound, "Proposal not found")
		return nil, false
	}

	fractalID, prismID, err := h.manager.changeRequestScope(r.Context(), crID)
	if err != nil {
		h.changeRequestError(w, err)
		return nil, false
	}

	if prismID != "" {
		if !h.requireRoleOnPrism(w, r, prismID, required) {
			return nil, false
		}
	} else if !h.requireRoleOnFractal(w, r, fractalID, required) {
		return nil, false
	}

	return cr, true
}

// HandleGetGateConfig returns the scope's review policy.
func (h *Handler) HandleGetGateConfig(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	cfg, err := h.manager.GateConfigFor(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to load gate config: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load review settings")
		return
	}
	h.respondSuccess(w, cfg)
}

// HandleSaveGateConfig stores the scope's review policy.
func (h *Handler) HandleSaveGateConfig(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAdmin)
	if !ok {
		return
	}

	var cfg GateConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	saved, err := h.manager.SaveGateConfig(r.Context(), fractalID, prismID, cfg, h.attributionUser(r))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.respondSuccess(w, saved)
}

// HandleListChangeRequests returns the scope's proposals.
func (h *Handler) HandleListChangeRequests(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	list, err := h.manager.ListChangeRequests(r.Context(), fractalID, prismID, r.URL.Query().Get("open") == "true")
	if err != nil {
		log.Printf("[Alerts] Failed to list proposals: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load proposals")
		return
	}
	api.WriteJSON(w, http.StatusOK, api.Response[[]*ChangeRequest]{Success: true, Data: list})
}

// HandleSubmitChangeRequest opens a proposal, or resubmits a revised one.
func (h *Handler) HandleSubmitChangeRequest(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}
	// Revising an existing proposal is authorized against that proposal's scope, not
	// the scope the caller happens to have selected.
	if chi.URLParam(r, "id") != "" {
		if _, ok := h.changeRequestAccess(w, r, rbac.RoleAnalyst); !ok {
			return
		}
	}

	var in ChangeRequestInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	cr, err := h.manager.SubmitChangeRequest(r.Context(), chi.URLParam(r, "id"), fractalID, prismID, in, h.attributionUser(r))
	if err != nil {
		// Shared with the other proposal handlers, so one underlying failure cannot
		// answer 400 here and 409 there.
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, cr)
}

// HandleGetChangeRequest returns one proposal with everything needed to review it.
func (h *Handler) HandleGetChangeRequest(w http.ResponseWriter, r *http.Request) {
	cr, ok := h.changeRequestAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}
	ctx := r.Context()

	detail := ChangeRequestDetail{ChangeRequest: cr}
	username := h.attributionUser(r)
	detail.IsAuthor = cr.Author == username

	// The definition the proposal would replace, so the reviewer sees a diff rather
	// than a wall of proposed fields.
	if cr.AlertID != "" {
		if alert, err := h.manager.GetAlert(ctx, cr.AlertID); err == nil {
			current := alertRevisionContent(alert)
			detail.Current = &current
		}
	}

	// Tests run on request, so opening a proposal to read it stays cheap.
	readiness, err := h.manager.EvaluateChangeRequest(ctx, cr, r.URL.Query().Get("run_tests") == "true")
	if err != nil {
		log.Printf("[Alerts] Failed to evaluate proposal: %v", err)
	} else {
		detail.Readiness = readiness
		detail.CanMerge = readiness.OK()
	}

	user := h.getUserObj(r)
	isAdmin := user != nil && user.IsAdmin
	crFractal, crPrism, err := h.manager.changeRequestScope(ctx, cr.ID)
	if err != nil {
		h.changeRequestError(w, err)
		return
	}
	cfg, err := h.manager.GateConfigFor(ctx, crFractal, crPrism)
	if err == nil {
		detail.CanReview = cr.CanApprove(username, isAdmin, cfg) == nil
	}

	h.respondSuccess(w, detail)
}

// HandleReviewChangeRequest records an approval or a rejection.
func (h *Handler) HandleReviewChangeRequest(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.changeRequestAccess(w, r, ReviewerRole); !ok {
		return
	}

	var req ReviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	user := h.getUserObj(r)
	isAdmin := user != nil && user.IsAdmin

	cr, err := h.manager.ReviewChangeRequest(r.Context(), chi.URLParam(r, "id"),
		req.Decision, req.Comment, h.attributionUser(r), isAdmin)
	if err != nil {
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, cr)
}

// HandleMergeChangeRequest applies an approved proposal.
func (h *Handler) HandleMergeChangeRequest(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.changeRequestAccess(w, r, ReviewerRole); !ok {
		return
	}

	cr, err := h.manager.MergeChangeRequest(r.Context(), chi.URLParam(r, "id"), h.attributionUser(r))
	if err != nil {
		var blocked *MergeBlockedError
		if errors.As(err, &blocked) {
			api.WriteJSON(w, http.StatusConflict, api.Response[*MergeReadiness]{
				Success: false,
				Error:   blocked.Error(),
				Code:    api.CodeForStatus(http.StatusConflict),
				Data:    blocked.Readiness,
			})
			return
		}
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, cr)
}

// HandleDiscardChangeRequest withdraws a proposal without destroying it.
func (h *Handler) HandleDiscardChangeRequest(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.changeRequestAccess(w, r, rbac.RoleAnalyst); !ok {
		return
	}

	user := h.getUserObj(r)
	isAdmin := user != nil && user.IsAdmin

	if err := h.manager.DiscardChangeRequest(r.Context(), chi.URLParam(r, "id"), h.attributionUser(r), isAdmin); err != nil {
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, map[string]bool{"discarded": true})
}

// HandleDeleteChangeRequest removes a proposal permanently. Admin only: rejecting is
// how a reviewer sends work back, and it deliberately keeps that work.
func (h *Handler) HandleDeleteChangeRequest(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.changeRequestAccess(w, r, rbac.RoleAdmin); !ok {
		return
	}

	if err := h.manager.DeleteChangeRequest(r.Context(), chi.URLParam(r, "id")); err != nil {
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

func (h *Handler) changeRequestError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrChangeRequestNotFound):
		h.respondError(w, http.StatusNotFound, "Proposal not found")
	case errors.Is(err, ErrAlertNotFound):
		h.respondError(w, http.StatusNotFound, "Alert not found")
	case strings.Contains(err.Error(), "only the author or an admin"),
		strings.Contains(err.Error(), "only the author"),
		strings.Contains(err.Error(), "cannot be approved by its author"),
		strings.Contains(err.Error(), "self approval is turned off"):
		h.respondError(w, http.StatusForbidden, err.Error())
	case strings.Contains(err.Error(), "is merged"), strings.Contains(err.Error(), "is discarded"),
		strings.Contains(err.Error(), "no longer be edited"):
		h.respondError(w, http.StatusConflict, err.Error())
	// Bad input is the caller's mistake, not the server's. Falling through to the
	// default told a client its request had crashed something when it was simply
	// rejected, and buried the reason in a log line.
	case strings.Contains(err.Error(), "must be"), strings.Contains(err.Error(), "needs a"),
		strings.Contains(err.Error(), "needs an"), strings.Contains(err.Error(), "is required"),
		strings.Contains(err.Error(), "has no alert yet"), strings.Contains(err.Error(), "say why"),
		strings.Contains(err.Error(), "at most"), strings.Contains(err.Error(), "duplicate"):
		h.respondError(w, http.StatusBadRequest, err.Error())
	default:
		log.Printf("[Alerts] Proposal operation failed: %v", err)
		h.respondError(w, http.StatusInternalServerError, err.Error())
	}
}

// gateRequiredResponse tells a client that a direct write needs a proposal instead.
func (h *Handler) gateRequiredResponse(w http.ResponseWriter, err error) bool {
	var gated *GateRequiredError
	if !errors.As(err, &gated) {
		return false
	}
	api.WriteJSON(w, http.StatusConflict, api.Response[map[string]string]{
		Success: false,
		Error:   gated.Error(),
		Code:    api.CodeForStatus(http.StatusConflict),
		Data:    map[string]string{"gate": "required", "kind": gated.Kind},
	})
	return true
}

// ProposeFromYAMLRequest imports a document into the review queue rather than applying it.
type ProposeFromYAMLRequest struct {
	Content      string `json:"content"`
	Summary      string `json:"summary"`
	NormalizerID string `json:"normalizer_id,omitempty"`
}

// HandleProposeFromYAML opens a proposal from an alert or Sigma document.
//
// A gated scope refuses a direct import, and telling an analyst to retype the rule in
// the editor would make the gate a wall rather than a queue.
func (h *Handler) HandleProposeFromYAML(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	var req ProposeFromYAMLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		h.respondError(w, http.StatusBadRequest, "A document is required")
		return
	}
	if strings.TrimSpace(req.Summary) == "" {
		h.respondError(w, http.StatusBadRequest, "Describe the change for the reviewer")
		return
	}

	cr, err := h.manager.ProposeFromYAML(r.Context(), req.Content, req.Summary,
		h.attributionUser(r), fractalID, prismID, req.NormalizerID)
	if err != nil {
		h.changeRequestError(w, err)
		return
	}
	h.respondSuccess(w, cr)
}

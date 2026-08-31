package notebooks

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"bifract/pkg/api"
	"bifract/pkg/auth"
	"bifract/pkg/rbac"
	"bifract/pkg/sse"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// The notebook the rail captures into, per user and per scope.
//
// This lived in localStorage, so it did not survive a browser change and no
// server-side caller could know where to file evidence. Per scope because
// carrying one across a fractal switch would send every capture to a notebook
// the new scope cannot read.

type SetActiveNotebookRequest struct {
	NotebookID string `json:"notebook_id"`
}

// scopeKey identifies the fractal or prism an active notebook belongs to.
func (h *NotebookHandler) scopeKey(r *http.Request) (string, error) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		return "", err
	}
	if prismID != "" {
		return "prism:" + prismID, nil
	}
	if fractalID != "" {
		return "fractal:" + fractalID, nil
	}
	return "", nil
}

// requireActiveNotebookUser resolves the user whose capture target is being
// read or written. The row keys off users(username), which an API key's
// synthetic principal cannot satisfy, and answering with the key creator's
// choice would hand a machine credential that person's working state.
func requireActiveNotebookUser(w http.ResponseWriter, r *http.Request) (*storage.User, bool) {
	return requireHumanUser(w, r, "The active notebook is per-user and not available for API key authentication")
}

// requireHumanUser resolves the acting user, refusing API keys with message.
func requireHumanUser(w http.ResponseWriter, r *http.Request, message string) (*storage.User, bool) {
	if auth.IsAPIKey(r.Context()) {
		api.WriteError(w, http.StatusForbidden, message)
		return nil, false
	}
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		api.WriteError(w, http.StatusUnauthorized, "Not authenticated")
		return nil, false
	}
	return user, true
}

// HandleGetActiveNotebook returns this user's capture target for the current
// scope plus whether the scope has any notebooks or comments at all. The search
// page reads both in one call: the second decides whether the results table
// shows its star gutter, which stays absent until the scope uses the feature.
func (h *NotebookHandler) HandleGetActiveNotebook(w http.ResponseWriter, r *http.Request) {
	user, ok := requireActiveNotebookUser(w, r)
	if !ok {
		return
	}
	username := user.Username
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Notebooks] Failed to resolve scope: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}
	if fractalID == "" && prismID == "" {
		api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: captureState{}})
		return
	}
	scope := "fractal:" + fractalID
	if prismID != "" {
		scope = "prism:" + prismID
	}

	notebookID, err := h.pg.GetActiveNotebook(r.Context(), username, scope)
	if err != nil {
		log.Printf("[Notebooks] Failed to read active notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to read the active notebook")
		return
	}
	hasNotebooks, hasComments, err := h.pg.ScopeCaptureState(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Notebooks] Failed to read scope capture state: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to read the active notebook")
		return
	}
	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: captureState{
		NotebookID:   notebookID,
		HasNotebooks: hasNotebooks,
		HasComments:  hasComments,
	}})
}

// captureState is what the search page needs to decide whether to offer capture
// and where a capture goes.
type captureState struct {
	NotebookID   string `json:"notebook_id"`
	HasNotebooks bool   `json:"has_notebooks"`
	HasComments  bool   `json:"has_comments"`
}

// HandleSetActiveNotebook records the notebook this user captures into. An
// empty notebook_id clears it.
func (h *NotebookHandler) HandleSetActiveNotebook(w http.ResponseWriter, r *http.Request) {
	user, ok := requireActiveNotebookUser(w, r)
	if !ok {
		return
	}
	username := user.Username

	var req SetActiveNotebookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Notebooks] Failed to resolve scope: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}
	if fractalID == "" && prismID == "" {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return
	}
	scope := "fractal:" + fractalID
	if prismID != "" {
		scope = "prism:" + prismID
	}

	if req.NotebookID == "" {
		if err := h.pg.ClearActiveNotebook(r.Context(), username, scope); err != nil {
			log.Printf("[Notebooks] Failed to clear active notebook: %v", err)
			api.WriteError(w, http.StatusInternalServerError, "Failed to clear the active notebook")
			return
		}
		api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: map[string]any{"notebook_id": ""}})
		return
	}

	// The notebook has to be one this scope can read, or the rail would file
	// captures somewhere the user cannot see them.
	nb, err := h.pg.GetNotebook(r.Context(), req.NotebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if nb.FractalID != fractalID || nb.PrismID != prismID {
		api.WriteError(w, http.StatusForbidden, "Notebook is not in the current scope")
		return
	}
	// The capture target has to be somewhere this user can actually file, so it
	// takes the same gate as filing: analyst on the scope, and not locked.
	if !h.requireEditable(w, r, nb) {
		return
	}

	if err := h.pg.SetActiveNotebook(r.Context(), username, scope, nb.ID); err != nil {
		log.Printf("[Notebooks] Failed to set active notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to set the active notebook")
		return
	}
	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: map[string]any{"notebook_id": nb.ID}})
}

// HandleEnsureActiveNotebook returns this user's capture target for the current
// scope, creating a scratch notebook when there is none. The search page calls
// it on the first capture so starting to collect never stops to ask for a name.
func (h *NotebookHandler) HandleEnsureActiveNotebook(w http.ResponseWriter, r *http.Request) {
	user, ok := requireActiveNotebookUser(w, r)
	if !ok {
		return
	}
	username := user.Username

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Notebooks] Failed to resolve scope: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}
	if fractalID == "" && prismID == "" {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return
	}
	scope := "fractal:" + fractalID
	if prismID != "" {
		scope = "prism:" + prismID
	}

	// An active notebook that still exists wins: a scratch is only ever the
	// fallback for someone who has not chosen one.
	if existingID, err := h.pg.GetActiveNotebook(r.Context(), username, scope); err != nil {
		log.Printf("[Notebooks] Failed to read active notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to read the active notebook")
		return
	} else if existingID != "" {
		if nb, err := h.pg.GetNotebook(r.Context(), existingID); err == nil {
			api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: map[string]any{
				"notebook_id": nb.ID, "name": nb.Name, "created": false,
			}})
			return
		}
	}

	nb, err := h.pg.GetOrCreateScratchNotebook(r.Context(), fractalID, prismID, username)
	if err != nil {
		log.Printf("[Notebooks] Failed to create a scratch notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to create a notebook to capture into")
		return
	}
	if err := h.pg.SetActiveNotebook(r.Context(), username, scope, nb.ID); err != nil {
		log.Printf("[Notebooks] Failed to set active notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to set the active notebook")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: map[string]any{
		"notebook_id": nb.ID, "name": nb.Name, "created": true,
	}})
}

// maxFiledEvidence bounds one filing request. A notebook is read end to end, so
// a caller asking for thousands of sections is a mistake, not a workload.
const maxFiledEvidence = 500

// HandleFileEvidence files comments that already exist into a notebook. This is
// how a comment reaches a second notebook, and how duplicating a notebook keeps
// its evidence pointing at the same comments rather than copying them.
func (h *NotebookHandler) HandleFileEvidence(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if !h.requireEditable(w, r, nb) {
		return
	}

	var req FileEvidenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if len(req.Evidence) == 0 {
		api.WriteError(w, http.StatusBadRequest, "evidence is required")
		return
	}
	if len(req.Evidence) > maxFiledEvidence {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("At most %d comments can be filed at once", maxFiledEvidence))
		return
	}

	items := make([]storage.EvidenceItem, 0, len(req.Evidence))
	for _, entry := range req.Evidence {
		comment, err := h.pg.GetComment(r.Context(), entry.CommentID)
		if err != nil {
			api.WriteError(w, http.StatusNotFound, "Comment not found")
			return
		}
		// Filing a comment from another scope would surface its text and log id
		// to readers who cannot see the fractal it came from.
		if comment.FractalID != nb.FractalID || comment.PrismID != nb.PrismID {
			api.WriteError(w, http.StatusForbidden, "Comment is not in this notebook's scope")
			return
		}
		items = append(items, storage.EvidenceItem{
			CommentID:  comment.ID,
			Title:      evidenceTitle(*comment),
			EventTime:  comment.LogTimestamp,
			OrderIndex: entry.OrderIndex,
		})
	}

	sections, err := h.pg.InsertEvidenceSections(r.Context(), notebookID, items)
	if err != nil {
		log.Printf("[Notebooks] Failed to file evidence: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to file evidence")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: fmt.Sprintf("Filed %d of %d comments", len(sections), len(req.Evidence)),
		Data:    map[string]any{"added": len(sections), "sections": sections},
	})

	for i := range sections {
		h.broadcastSSE(r, notebookID, sse.Event{Type: sse.SectionAdded, Data: sections[i]})
	}
}

// HandleDeleteEvidence removes a log from a notebook: the counterpart to
// starring it. Sections referencing that log go, and so does any comment left
// behind that carried no text of its own.
func (h *NotebookHandler) HandleDeleteEvidence(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	logID := chi.URLParam(r, "log_id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if !h.requireEditable(w, r, nb) {
		return
	}
	if !evidenceLogID.MatchString(logID) {
		api.WriteError(w, http.StatusBadRequest, "log_id is not a valid log identifier")
		return
	}

	sectionIDs, err := h.pg.DeleteEvidence(r.Context(), notebookID, logID)
	if err != nil {
		log.Printf("[Notebooks] Failed to remove evidence: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to remove the evidence")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: fmt.Sprintf("Removed %d evidence sections", len(sectionIDs)),
		Data:    map[string]any{"removed": len(sectionIDs)},
	})

	for _, id := range sectionIDs {
		h.broadcastSSE(r, notebookID, sse.Event{Type: sse.SectionRemoved, Data: map[string]any{"id": id}})
	}
}

// evidenceLogID constrains the path parameter: log_id reaches storage lookups
// from several call sites and is a content hash, never free text.
var evidenceLogID = regexp.MustCompile(`^[0-9a-fA-F]{8,64}$`)

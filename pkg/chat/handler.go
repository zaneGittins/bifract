package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/fractals"
	"bifract/pkg/storage"
)

// Handler provides HTTP endpoints for the chat system.
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
	rbacResolver   RBACResolver
	prismResolver  PrismResolver
}

// RBACResolver checks whether a user has any access to a fractal.
type RBACResolver interface {
	HasFractalAccess(ctx context.Context, user *storage.User, fractalID string) bool
}

// PrismResolver resolves prism details for chat context.
type PrismResolver interface {
	GetPrismInfo(ctx context.Context, prismID string) (id, name, description string, err error)
}

// SetPrismResolver sets the prism resolver for prism-scoped chat context.
func (h *Handler) SetPrismResolver(resolver PrismResolver) {
	h.prismResolver = resolver
}

type apiResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// NewHandler creates a new chat handler.
func NewHandler(manager *Manager, fractalManager *fractals.Manager, rbacResolver RBACResolver) *Handler {
	return &Handler{manager: manager, fractalManager: fractalManager, rbacResolver: rbacResolver}
}

// SetRBACResolver sets the RBAC resolver (for deferred initialization).
func (h *Handler) SetRBACResolver(resolver RBACResolver) {
	h.rbacResolver = resolver
}

// verifyConversationOwner loads a conversation and verifies the requesting user
// is the owner and the conversation belongs to the caller's current scope.
// Returns the conversation or writes an error response.
func (h *Handler) verifyConversationOwner(w http.ResponseWriter, r *http.Request, convID string) *Conversation {
	username := h.getUsername(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return nil
	}

	conv, err := h.manager.GetConversation(r.Context(), convID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "conversation not found")
		return nil
	}

	if conv.CreatedBy != username {
		h.respondError(w, http.StatusForbidden, "access denied")
		return nil
	}

	// Verify the conversation belongs to the caller's current scope.
	fractalID, prismID, _ := h.getScope(r)
	scopeMatch := (conv.FractalID != "" && conv.FractalID == fractalID) ||
		(conv.PrismID != "" && conv.PrismID == prismID)
	if !scopeMatch {
		h.respondError(w, http.StatusNotFound, "conversation not found")
		return nil
	}

	return conv
}

func (h *Handler) HandleListConversations(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	username := h.getUsername(r)

	convs, err := h.manager.ListConversations(r.Context(), fractalID, prismID, username)
	if err != nil {
		log.Printf("[Chat] Failed to list conversations: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load conversations")
		return
	}
	if convs == nil {
		convs = []*Conversation{}
	}
	h.respondSuccess(w, map[string]interface{}{"conversations": convs, "count": len(convs)})
}

func (h *Handler) HandleCreateConversation(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	username := h.getUsername(r)

	var req struct {
		Title      string   `json:"title"`
		LibraryIDs []string `json:"library_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	conv, err := h.manager.CreateConversation(r.Context(), fractalID, prismID, req.Title, username, req.LibraryIDs)
	if err != nil {
		log.Printf("[Chat] Failed to create conversation: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to create conversation")
		return
	}
	h.respondSuccess(w, conv)
}

func (h *Handler) HandleRenameConversation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if h.verifyConversationOwner(w, r, id) == nil {
		return
	}

	var req struct {
		Title string `json:"title"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Title == "" {
		h.respondError(w, http.StatusBadRequest, "title is required")
		return
	}

	conv, err := h.manager.RenameConversation(r.Context(), id, req.Title)
	if err != nil {
		log.Printf("[Chat] Failed to rename conversation %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to rename conversation")
		return
	}
	h.respondSuccess(w, conv)
}

func (h *Handler) HandleDeleteConversation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if h.verifyConversationOwner(w, r, id) == nil {
		return
	}

	if err := h.manager.DeleteConversation(r.Context(), id); err != nil {
		log.Printf("[Chat] Failed to delete conversation %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete conversation")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

func (h *Handler) HandleGetMessages(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if h.verifyConversationOwner(w, r, id) == nil {
		return
	}

	msgs, err := h.manager.GetMessages(r.Context(), id)
	if err != nil {
		log.Printf("[Chat] Failed to get messages for %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load messages")
		return
	}
	if msgs == nil {
		msgs = []*Message{}
	}
	h.respondSuccess(w, map[string]interface{}{"messages": msgs, "count": len(msgs)})
}

func (h *Handler) HandleClearMessages(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if h.verifyConversationOwner(w, r, id) == nil {
		return
	}

	if err := h.manager.ClearMessages(r.Context(), id); err != nil {
		log.Printf("[Chat] Failed to clear messages for %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to clear messages")
		return
	}
	h.respondSuccess(w, map[string]bool{"cleared": true})
}

func (h *Handler) HandleDeleteAllConversations(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	username := h.getUsername(r)

	if err := h.manager.DeleteAllConversations(r.Context(), fractalID, prismID, username); err != nil {
		log.Printf("[Chat] Failed to delete all conversations: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete conversations")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}


func (h *Handler) HandleSetConversationLibraries(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if h.verifyConversationOwner(w, r, id) == nil {
		return
	}

	var req struct {
		LibraryIDs []string `json:"library_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := h.manager.SetConversationLibraries(r.Context(), id, req.LibraryIDs); err != nil {
		log.Printf("[Chat] Failed to set libraries for %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update conversation libraries")
		return
	}
	h.respondSuccess(w, map[string]bool{"updated": true})
}

func (h *Handler) HandleGetConversationLibraries(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if h.verifyConversationOwner(w, r, id) == nil {
		return
	}

	libs, err := h.manager.GetConversationLibraries(r.Context(), id)
	if err != nil {
		log.Printf("[Chat] Failed to get libraries for %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load conversation libraries")
		return
	}
	h.respondSuccess(w, libs)
}

// ---- Instruction Handlers ----

func (h *Handler) HandleListInstructions(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	if prismID != "" {
		// Legacy instructions are fractal-scoped; use instruction libraries for prism context
		h.respondSuccess(w, map[string]interface{}{"instructions": []*Instruction{}, "count": 0})
		return
	}

	insts, err := h.manager.ListInstructions(r.Context(), fractalID)
	if err != nil {
		log.Printf("[Chat] Failed to list instructions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load instructions")
		return
	}
	if insts == nil {
		insts = []*Instruction{}
	}
	h.respondSuccess(w, map[string]interface{}{"instructions": insts, "count": len(insts)})
}

func (h *Handler) HandleCreateInstruction(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	if prismID != "" {
		h.respondError(w, http.StatusBadRequest, "Legacy instructions are not available in prism context. Use instruction libraries instead.")
		return
	}
	username := h.getUsername(r)

	var req struct {
		Name      string `json:"name"`
		Content   string `json:"content"`
		IsDefault bool   `json:"is_default"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	inst, err := h.manager.CreateInstruction(r.Context(), fractalID, req.Name, req.Content, username, req.IsDefault)
	if err != nil {
		log.Printf("[Chat] Failed to create instruction: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to create instruction")
		return
	}
	h.respondSuccess(w, inst)
}

func (h *Handler) HandleUpdateInstruction(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "instructionId")

	// Verify instruction belongs to current scope
	inst, err := h.manager.GetInstruction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "instruction not found")
		return
	}
	fractalID, _, _ := h.getScope(r)
	if inst.FractalID != fractalID {
		h.respondError(w, http.StatusNotFound, "instruction not found")
		return
	}

	var req struct {
		Name      string `json:"name"`
		Content   string `json:"content"`
		IsDefault bool   `json:"is_default"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	updated, err := h.manager.UpdateInstruction(r.Context(), id, req.Name, req.Content, req.IsDefault)
	if err != nil {
		log.Printf("[Chat] Failed to update instruction %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update instruction")
		return
	}
	h.respondSuccess(w, updated)
}

func (h *Handler) HandleDeleteInstruction(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "instructionId")

	// Verify instruction belongs to current scope
	inst, err := h.manager.GetInstruction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "instruction not found")
		return
	}
	fractalID, _, _ := h.getScope(r)
	if inst.FractalID != fractalID {
		h.respondError(w, http.StatusNotFound, "instruction not found")
		return
	}

	if err := h.manager.DeleteInstruction(r.Context(), id); err != nil {
		log.Printf("[Chat] Failed to delete instruction %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete instruction")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

func (h *Handler) HandleStream(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var req struct {
		Content   string `json:"content"`
		TimeRange string `json:"time_range"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Content == "" {
		h.respondError(w, http.StatusBadRequest, "content is required")
		return
	}
	if req.TimeRange == "" {
		req.TimeRange = "24h"
	}

	conv := h.verifyConversationOwner(w, r, id)
	if conv == nil {
		return
	}

	// Verify the user has access to this conversation's scope
	user, _ := r.Context().Value("user").(*storage.User)
	if h.rbacResolver != nil && user != nil && conv.FractalID != "" {
		if !h.rbacResolver.HasFractalAccess(r.Context(), user, conv.FractalID) {
			h.respondError(w, http.StatusForbidden, "access denied")
			return
		}
	}

	// Get context for the AI system prompt
	var fractal *fractals.Fractal
	if conv.FractalID != "" {
		var err error
		fractal, err = h.fractalManager.GetFractal(r.Context(), conv.FractalID)
		if err != nil {
			h.respondError(w, http.StatusInternalServerError, "failed to get fractal info")
			return
		}
	} else if conv.PrismID != "" && h.prismResolver != nil {
		id, name, desc, err := h.prismResolver.GetPrismInfo(r.Context(), conv.PrismID)
		if err != nil {
			h.respondError(w, http.StatusInternalServerError, "failed to get prism info")
			return
		}
		fractal = &fractals.Fractal{ID: id, Name: name, Description: desc}
	} else {
		h.respondError(w, http.StatusBadRequest, "conversation has no scope")
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	if err := h.manager.StreamResponse(r.Context(), w, flusher, conv, fractal, req.Content, req.TimeRange); err != nil {
		// Error already sent as SSE event by StreamResponse
	}
}

// ---- Helpers ----

func (h *Handler) getScope(r *http.Request) (fractalID, prismID string, err error) {
	if pid, _ := r.Context().Value("selected_prism").(string); pid != "" {
		return "", pid, nil
	}
	if fid, _ := r.Context().Value("selected_fractal").(string); fid != "" {
		return fid, "", nil
	}
	return "", "", fmt.Errorf("no fractal or prism selected")
}

func (h *Handler) getUsername(r *http.Request) string {
	user, _ := r.Context().Value("user").(*storage.User)
	if user != nil {
		return user.Username
	}
	return ""
}

func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(apiResponse{Success: true, Data: data})
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(apiResponse{Success: false, Error: msg})
}

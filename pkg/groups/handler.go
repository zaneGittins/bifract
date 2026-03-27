package groups

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/storage"
)

type Handler struct {
	pg *storage.PostgresClient
}

func NewHandler(pg *storage.PostgresClient) *Handler {
	return &Handler{pg: pg}
}

type apiResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if user, ok := r.Context().Value("user").(*storage.User); ok {
		return user
	}
	return nil
}

func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) *storage.User {
	user := h.getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(apiResponse{Error: "Only administrators can manage groups"})
		return nil
	}
	return user
}

func (h *Handler) sendJSON(w http.ResponseWriter, status int, resp apiResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

// HandleListGroups lists all groups (tenant admin only).
func (h *Handler) HandleListGroups(w http.ResponseWriter, r *http.Request) {
	if h.requireAdmin(w, r) == nil {
		return
	}
	groups, err := h.pg.ListGroups(r.Context())
	if err != nil {
		h.sendJSON(w, http.StatusInternalServerError, apiResponse{Error: "Failed to list groups"})
		return
	}
	if groups == nil {
		groups = []storage.Group{}
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Data: groups})
}

// HandleCreateGroup creates a new group (tenant admin only).
func (h *Handler) HandleCreateGroup(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Invalid request body"})
		return
	}
	if req.Name == "" {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Group name is required"})
		return
	}

	group, err := h.pg.CreateGroup(r.Context(), req.Name, req.Description, user.Username)
	if err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Failed to create group"})
		return
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Message: "Group created", Data: group})
}

// HandleGetGroup retrieves a single group (tenant admin only).
func (h *Handler) HandleGetGroup(w http.ResponseWriter, r *http.Request) {
	if h.requireAdmin(w, r) == nil {
		return
	}
	id := chi.URLParam(r, "id")
	group, err := h.pg.GetGroup(r.Context(), id)
	if err != nil {
		h.sendJSON(w, http.StatusNotFound, apiResponse{Error: "Group not found"})
		return
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Data: group})
}

// HandleUpdateGroup updates a group (tenant admin only).
func (h *Handler) HandleUpdateGroup(w http.ResponseWriter, r *http.Request) {
	if h.requireAdmin(w, r) == nil {
		return
	}
	id := chi.URLParam(r, "id")

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Invalid request body"})
		return
	}
	if req.Name == "" {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Group name is required"})
		return
	}

	group, err := h.pg.UpdateGroup(r.Context(), id, req.Name, req.Description)
	if err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Failed to update group"})
		return
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Message: "Group updated", Data: group})
}

// HandleDeleteGroup deletes a group (tenant admin only).
func (h *Handler) HandleDeleteGroup(w http.ResponseWriter, r *http.Request) {
	if h.requireAdmin(w, r) == nil {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.pg.DeleteGroup(r.Context(), id); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Failed to delete group"})
		return
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Message: "Group deleted"})
}

// HandleListMembers lists members of a group (tenant admin only).
func (h *Handler) HandleListMembers(w http.ResponseWriter, r *http.Request) {
	if h.requireAdmin(w, r) == nil {
		return
	}
	id := chi.URLParam(r, "id")
	members, err := h.pg.ListGroupMembers(r.Context(), id)
	if err != nil {
		h.sendJSON(w, http.StatusInternalServerError, apiResponse{Error: "Failed to list members"})
		return
	}
	if members == nil {
		members = []storage.GroupMember{}
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Data: members})
}

// HandleAddMember adds a user to a group (tenant admin only).
func (h *Handler) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}
	groupID := chi.URLParam(r, "id")

	var req struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Invalid request body"})
		return
	}
	if req.Username == "" {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Username is required"})
		return
	}

	// Verify user exists
	if _, err := h.pg.GetUser(r.Context(), req.Username); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "User not found"})
		return
	}

	if err := h.pg.AddGroupMember(r.Context(), groupID, req.Username, user.Username); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Failed to add member"})
		return
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Message: "Member added"})
}

// HandleRemoveMember removes a user from a group (tenant admin only).
func (h *Handler) HandleRemoveMember(w http.ResponseWriter, r *http.Request) {
	if h.requireAdmin(w, r) == nil {
		return
	}
	groupID := chi.URLParam(r, "id")
	username := chi.URLParam(r, "username")

	if err := h.pg.RemoveGroupMember(r.Context(), groupID, username); err != nil {
		h.sendJSON(w, http.StatusBadRequest, apiResponse{Error: "Failed to remove member"})
		return
	}
	h.sendJSON(w, http.StatusOK, apiResponse{Success: true, Message: "Member removed"})
}

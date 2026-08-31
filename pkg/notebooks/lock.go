package notebooks

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"

	"bifract/pkg/api"
	"bifract/pkg/sse"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// Locking is a deliberate human act on a shared record, so it is closed to API
// keys: a machine credential sealing someone's working notebook is never what
// the person holding the key meant.
const lockAPIKeyMessage = "Locking a notebook is a per-user action and not available for API key authentication"

// HandleLockNotebook freezes a notebook.
func (h *NotebookHandler) HandleLockNotebook(w http.ResponseWriter, r *http.Request) {
	user, ok := requireHumanUser(w, r, lockAPIKeyMessage)
	if !ok {
		return
	}
	nb, ok := h.lockTarget(w, r)
	if !ok {
		return
	}
	if nb.IsLocked() {
		api.WriteError(w, http.StatusConflict, nb.LockedMessage())
		return
	}

	locked, err := h.pg.LockNotebook(r.Context(), nb.ID, user.Username)
	if err != nil {
		h.writeLockError(w, err, "lock")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Message: "Notebook locked", Data: locked})
	h.broadcastSSE(r, nb.ID, sse.Event{
		Type: sse.NotebookLocked,
		Data: map[string]any{"locked_at": locked.LockedAt, "locked_by": locked.LockedBy},
	})
}

// HandleUnlockNotebook returns a notebook to editable. Only the user who locked
// it or an admin may, so unlocking stays a visible decision rather than a way
// around someone else's seal.
func (h *NotebookHandler) HandleUnlockNotebook(w http.ResponseWriter, r *http.Request) {
	user, ok := requireHumanUser(w, r, lockAPIKeyMessage)
	if !ok {
		return
	}
	nb, ok := h.lockTarget(w, r)
	if !ok {
		return
	}
	if !nb.IsLocked() {
		api.WriteError(w, http.StatusConflict, "This notebook is not locked")
		return
	}
	if nb.LockedBy != user.Username && !user.IsAdmin {
		api.WriteError(w, http.StatusForbidden,
			fmt.Sprintf("Only %s or an administrator can unlock this notebook", nb.LockedBy))
		return
	}

	unlocked, err := h.pg.UnlockNotebook(r.Context(), nb.ID)
	if err != nil {
		h.writeLockError(w, err, "unlock")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Message: "Notebook unlocked", Data: unlocked})
	h.broadcastSSE(r, nb.ID, sse.Event{
		Type: sse.NotebookUnlocked,
		Data: map[string]any{"unlocked_by": user.Username},
	})
}

// lockTarget resolves the notebook and checks the caller may write to its scope.
// It deliberately does not use requireEditable, which refuses locked notebooks:
// these two routes are the only writes a locked notebook still accepts.
func (h *NotebookHandler) lockTarget(w http.ResponseWriter, r *http.Request) (*storage.Notebook, bool) {
	nb, err := h.pg.GetNotebook(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return nil, false
	}
	if !h.canWrite(r, nb) {
		forbidden(w)
		return nil, false
	}
	return nb, true
}

func (h *NotebookHandler) writeLockError(w http.ResponseWriter, err error, verb string) {
	var unrun *storage.UnrunQueriesError
	switch {
	case errors.As(err, &unrun):
		api.WriteError(w, http.StatusConflict, fmt.Sprintf(
			"%s never been run. Run every query before locking, or it will be sealed with no results.",
			pendingSections(unrun.Count)))
	case errors.Is(err, sql.ErrNoRows):
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
	case errors.Is(err, storage.ErrNotebookAlreadyLocked):
		api.WriteError(w, http.StatusConflict, "This notebook is already locked")
	case errors.Is(err, storage.ErrNotebookNotLocked):
		api.WriteError(w, http.StatusConflict, "This notebook is not locked")
	default:
		log.Printf("[Notebooks] Failed to %s notebook: %v", verb, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to "+verb+" the notebook")
	}
}

func pendingSections(n int) string {
	if n == 1 {
		return "1 query section has"
	}
	return fmt.Sprintf("%d query sections have", n)
}

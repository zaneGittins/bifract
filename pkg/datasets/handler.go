package datasets

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/api"
	"bifract/pkg/auth"
	"bifract/pkg/exchange"
	"bifract/pkg/fractals"
	"bifract/pkg/sse"
	"bifract/pkg/storage"
)

// maxDescription is the longest description a fractal takes.
const maxDescription = 500

// IndexSource is the Exchange index a dataset is added from.
type IndexSource interface {
	Index(ctx context.Context) (*exchange.Index, error)
}

// FractalCreator makes the fractal a dataset is imported into.
type FractalCreator interface {
	CreateFractalForDataset(ctx context.Context, name, description, createdBy string, start, end time.Time) (*fractals.Fractal, error)
	DeleteFractal(ctx context.Context, fractalID string) error
}

// Hub serves and feeds the SSE room import progress goes to.
type Hub interface {
	Broadcaster
	ServeSSE(w http.ResponseWriter, r *http.Request, room string, info sse.ClientInfo) *sse.Client
}

// Handler adds Exchange datasets and reports their imports. Every route needs a
// tenant admin: adding one creates a fractal, and the list names every fractal
// made this way.
type Handler struct {
	index    IndexSource
	fractals FractalCreator
	store    *Store
	hub      Hub
}

func NewHandler(index IndexSource, fractals FractalCreator, store *Store, hub Hub) *Handler {
	return &Handler{index: index, fractals: fractals, store: store, hub: hub}
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	imports, err := h.store.List(r.Context())
	if err != nil {
		log.Printf("[Datasets] %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to list dataset imports")
		return
	}
	api.WriteSuccess(w, imports)
}

// HandleAdd creates a fractal named after the dataset and queues its import.
func (h *Handler) HandleAdd(w http.ResponseWriter, r *http.Request) {
	idx, err := h.index.Index(r.Context())
	if errors.Is(err, exchange.ErrDisabled) {
		api.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		log.Printf("[Datasets] Failed to load the index: %v", err)
		api.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	it := idx.Find(chi.URLParam(r, "id"))
	if it == nil || it.Kind != exchange.KindDataset || it.Dataset == nil {
		api.WriteError(w, http.StatusNotFound, "no such dataset in the Exchange")
		return
	}
	if err := fractals.ValidateName(it.ID); err != nil {
		api.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	d := it.Dataset
	user := auth.AttributionUsername(r.Context())
	f, err := h.fractals.CreateFractalForDataset(r.Context(), it.ID, fractalDescription(it), user, d.Start, d.End)
	if errors.Is(err, fractals.ErrNameTaken) {
		api.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		log.Printf("[Datasets] Creating the fractal for %s: %v", it.ID, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to create the dataset's fractal")
		return
	}
	imp, err := h.store.Create(r.Context(), it.ID, f.ID, user, d.URL, d.SHA256, d.Bytes, d.Logs)
	if err != nil {
		log.Printf("[Datasets] %v", err)
		if derr := h.fractals.DeleteFractal(context.Background(), f.ID); derr != nil {
			log.Printf("[Datasets] Removing fractal %s after a failed add: %v", f.Name, derr)
		}
		api.WriteError(w, http.StatusInternalServerError, "Failed to queue the import")
		return
	}
	h.hub.Broadcast(Room, sse.Event{Type: EventProgress, Data: imp}, "")
	api.WriteSuccess(w, imp)
}

// fractalDescription is the dataset's fractal description: its name, then as much of its
// description as fits.
func fractalDescription(it *exchange.Item) string {
	desc := strings.TrimSpace(it.Name)
	if more := strings.Join(strings.Fields(it.Description), " "); more != "" {
		desc += ". " + more
	}
	if len(desc) <= maxDescription {
		return desc
	}
	cut := strings.LastIndex(desc[:maxDescription-3], " ")
	if cut < 0 {
		// No space to break at: back up to the start of a rune.
		for cut = maxDescription - 3; cut > 0 && !utf8.RuneStart(desc[cut]); cut-- {
		}
	}
	return desc[:cut] + "..."
}

// HandleRetry runs a failed import again from the start.
func (h *Handler) HandleRetry(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "importId"), 10, 64)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "invalid import id")
		return
	}
	if err := h.store.Retry(r.Context(), id); err != nil {
		if errors.Is(err, ErrNotFound) {
			api.WriteError(w, http.StatusConflict, "only a failed import can be retried")
			return
		}
		log.Printf("[Datasets] %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to retry the import")
		return
	}
	imp, err := h.store.Get(r.Context(), id)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to read the import")
		return
	}
	h.hub.Broadcast(Room, sse.Event{Type: EventProgress, Data: imp}, "")
	api.WriteSuccess(w, imp)
}

// HandleEvents streams import progress.
func (h *Handler) HandleEvents(w http.ResponseWriter, r *http.Request) {
	user, _ := r.Context().Value("user").(*storage.User)
	if user == nil {
		api.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.hub.ServeSSE(w, r, Room, sse.ClientInfo{Username: user.Username, DisplayName: user.DisplayName})
}

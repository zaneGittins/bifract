package exchange

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/api"
)

// Handler serves the exchange index to the UI. Installing goes through each
// kind's own import endpoint, so its permissions and review gates apply as usual.
type Handler struct {
	client *Client
}

func NewHandler(client *Client) *Handler {
	return &Handler{client: client}
}

// Listing is what the exchange offers.
type Listing struct {
	Enabled bool   `json:"enabled"`
	Source  string `json:"source,omitempty"`
	Items   []Item `json:"items"`
}

// ItemContent is an item with its file. A search also arrives parsed, in the
// shape a saved query is created with.
type ItemContent struct {
	Item    Item         `json:"item"`
	Content string       `json:"content"`
	Search  *SavedSearch `json:"search,omitempty"`
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	if !h.client.Enabled() {
		api.WriteSuccess(w, Listing{Items: []Item{}})
		return
	}
	idx, err := h.client.Index(r.Context())
	if err != nil {
		log.Printf("[Exchange] Failed to load the index: %v", err)
		api.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	api.WriteSuccess(w, Listing{Enabled: true, Source: h.client.Source(), Items: idx.Items})
}

func (h *Handler) HandleItem(w http.ResponseWriter, r *http.Request) {
	if !h.client.Enabled() {
		api.WriteError(w, http.StatusNotFound, ErrDisabled.Error())
		return
	}
	idx, err := h.client.Index(r.Context())
	if err != nil {
		log.Printf("[Exchange] Failed to load the index: %v", err)
		api.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	it := idx.Find(chi.URLParam(r, "id"))
	if it == nil {
		api.WriteError(w, http.StatusNotFound, "no such exchange item")
		return
	}
	out := ItemContent{Item: *it}
	if it.Kind == KindLookup || it.Kind == KindBundle {
		api.WriteSuccess(w, out)
		return
	}
	if out.Content, err = h.client.Content(r.Context(), it); err != nil {
		log.Printf("[Exchange] Failed to fetch %s: %v", it.ID, err)
		api.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if it.Kind == KindSearch {
		if out.Search, err = ParseSearch([]byte(out.Content)); err != nil {
			api.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
	}
	api.WriteSuccess(w, out)
}

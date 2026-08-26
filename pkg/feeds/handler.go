package feeds

import (
	"bifract/pkg/api"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"bifract/pkg/alerts"
	"bifract/pkg/fractals"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// Handler provides HTTP endpoints for alert feed management.
type Handler struct {
	manager        *Manager
	alertManager   *alerts.Manager
	fractalManager *fractals.Manager
	syncer         *Syncer
}

// NewHandler creates a new feed handler.
func NewHandler(manager *Manager, alertManager *alerts.Manager, fractalManager *fractals.Manager, syncer *Syncer) *Handler {
	return &Handler{
		manager:        manager,
		alertManager:   alertManager,
		fractalManager: fractalManager,
		syncer:         syncer,
	}
}

// apiResponse is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type apiResponse = api.Response[any]

func (h *Handler) respond(w http.ResponseWriter, status int, data interface{}, errMsg string) {
	api.WriteJSON(w, status, api.Response[any]{Success: errMsg == "", Data: data, Error: errMsg})
}

func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if u, ok := r.Context().Value("user").(*storage.User); ok {
		return u
	}
	return nil
}

func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) *storage.User {
	user := h.getCurrentUser(r)
	if user == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return nil
	}
	if !user.IsAdmin {
		h.respond(w, http.StatusForbidden, nil, "admin access required")
		return nil
	}
	return user
}

// getFeedScoped fetches a feed by ID and verifies it belongs to the caller's
// current scope (fractal or prism). Returns nil and writes an error response
// if the feed is not found or not in scope.
func (h *Handler) getFeedScoped(w http.ResponseWriter, r *http.Request, id string) *Feed {
	feed, err := h.manager.Get(r.Context(), id)
	if err != nil {
		h.respond(w, http.StatusNotFound, nil, "Feed not found")
		return nil
	}
	fractalID, prismID := h.getScope(r)
	if (feed.FractalID != "" && feed.FractalID == fractalID) ||
		(feed.PrismID != "" && feed.PrismID == prismID) {
		return feed
	}
	h.respond(w, http.StatusNotFound, nil, "Feed not found")
	return nil
}

func (h *Handler) getScope(r *http.Request) (string, string) {
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		return "", prismID
	}
	if fractalID, ok := r.Context().Value("selected_fractal").(string); ok && fractalID != "" {
		return fractalID, ""
	}
	f, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", ""
	}
	return f.ID, ""
}

// HandleListFeeds returns all feeds for the current fractal or prism.
func (h *Handler) HandleListFeeds(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	feeds, err := h.manager.List(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Feeds] Failed to list feeds: %v", err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to load feeds")
		return
	}
	h.respond(w, http.StatusOK, feeds, "")
}

// HandleGetFeed returns a single feed.
func (h *Handler) HandleGetFeed(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	feed := h.getFeedScoped(w, r, id)
	if feed == nil {
		return
	}
	h.respond(w, http.StatusOK, feed, "")
}

// HandleCreateFeed creates a new feed (admin only).
func (h *Handler) HandleCreateFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	feed, err := h.manager.Create(r.Context(), req, fractalID, prismID, user.Username)
	if err != nil {
		log.Printf("[Feeds] Failed to create feed: %v", err)
		h.respond(w, http.StatusBadRequest, nil, "Failed to create feed")
		return
	}
	h.respond(w, http.StatusCreated, feed, "")
}

// HandleUpdateFeed updates an existing feed (admin only).
func (h *Handler) HandleUpdateFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if h.getFeedScoped(w, r, id) == nil {
		return
	}
	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	feed, err := h.manager.Update(r.Context(), id, req)
	if err != nil {
		log.Printf("[Feeds] Failed to update feed %s: %v", id, err)
		h.respond(w, http.StatusBadRequest, nil, "Failed to update feed")
		return
	}
	h.respond(w, http.StatusOK, feed, "")
}

// HandleDeleteFeed deletes a feed and all its alerts (admin only).
func (h *Handler) HandleDeleteFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if h.getFeedScoped(w, r, id) == nil {
		return
	}
	if err := h.manager.Delete(r.Context(), id); err != nil {
		log.Printf("[Feeds] Failed to delete feed %s: %v", id, err)
		h.respond(w, http.StatusNotFound, nil, "Failed to delete feed")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// HandleSyncFeed triggers an immediate sync (admin only).
func (h *Handler) HandleSyncFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	feed := h.getFeedScoped(w, r, id)
	if feed == nil {
		return
	}

	// Detached from the request: a full re-translation of a large Sigma repo runs well past
	// the 60s HTTP timeout, and a cancelled request used to leave the sync half-applied.
	// Progress is reported through the feed's sync status.
	if !h.syncer.StartManualSync(feed) {
		h.respond(w, http.StatusConflict, nil, "A sync is already running for this feed")
		return
	}
	h.respond(w, http.StatusAccepted, nil, "")
}

// HandleGetFeedAlerts returns all alerts for a specific feed (authenticated).
func (h *Handler) HandleGetFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if h.getCurrentUser(r) == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return
	}

	id := chi.URLParam(r, "id")
	if h.getFeedScoped(w, r, id) == nil {
		return
	}
	alertsList, err := h.alertManager.ListFeedAlerts(r.Context(), id)
	if err != nil {
		log.Printf("[Feeds] Failed to list alerts for feed %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to load feed alerts")
		return
	}
	h.respond(w, http.StatusOK, alertsList, "")
}

// HandleEnableAllAlerts enables all alerts for a feed (admin only).
func (h *Handler) HandleEnableAllAlerts(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if h.getFeedScoped(w, r, id) == nil {
		return
	}
	if err := h.alertManager.EnableFeedAlerts(r.Context(), id, true, user.Username); err != nil {
		log.Printf("[Feeds] Failed to enable alerts for feed %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to enable feed alerts")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// HandleDisableAllAlerts disables all alerts for a feed (admin only).
func (h *Handler) HandleDisableAllAlerts(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if h.getFeedScoped(w, r, id) == nil {
		return
	}
	if err := h.alertManager.EnableFeedAlerts(r.Context(), id, false, user.Username); err != nil {
		log.Printf("[Feeds] Failed to disable alerts for feed %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to disable feed alerts")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// HandleListAllFeedAlerts returns one page of feed alerts for the current
// fractal or prism (authenticated). Filtering, sorting and paging all happen in
// Postgres: a feed can hold thousands of rules and the full set is far too
// large to ship to the browser on every visit.
func (h *Handler) HandleListAllFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if h.getCurrentUser(r) == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return
	}

	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	q := parseFeedAlertQuery(r, fractalID, prismID)
	page, err := h.alertManager.ListFeedAlertsPage(r.Context(), q)
	if err != nil {
		log.Printf("[Feeds] Failed to list feed alerts: %v", err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to load feed alerts")
		return
	}

	if r.URL.Query().Get("facets") == "1" {
		facets, err := h.alertManager.FeedAlertFacetsFor(r.Context(), fractalID, prismID)
		if err != nil {
			log.Printf("[Feeds] Failed to load feed alert facets: %v", err)
			h.respond(w, http.StatusInternalServerError, nil, "Failed to load feed alerts")
			return
		}
		page.Facets = facets
	}

	h.respond(w, http.StatusOK, page, "")
}

// parseFeedAlertQuery reads the table's filter/sort/page state off the URL.
// "all" is the UI's unset sentinel for every dropdown.
func parseFeedAlertQuery(r *http.Request, fractalID, prismID string) alerts.FeedAlertQuery {
	v := r.URL.Query()
	unset := func(key string) string {
		val := v.Get(key)
		if val == "all" {
			return ""
		}
		return val
	}

	limit, _ := strconv.Atoi(v.Get("limit"))
	offset, _ := strconv.Atoi(v.Get("offset"))

	return alerts.FeedAlertQuery{
		FractalID: fractalID,
		PrismID:   prismID,
		Search:    v.Get("search"),
		Status:    unset("status"),
		FeedID:    unset("feed_id"),
		Severity:  unset("severity"),
		Label:     unset("label"),
		Sort:      v.Get("sort"),
		Dir:       v.Get("dir"),
		Limit:     limit,
		Offset:    offset,
	}
}

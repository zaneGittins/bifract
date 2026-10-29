package query

import (
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// ObservedFieldsResponse lists the fields the logs in scope carry, most common
// first, and the rows they are counted over, so a share is Rows of a field / Rows.
type ObservedFieldsResponse struct {
	Success bool                    `json:"success"`
	Fields  []storage.ObservedField `json:"fields"`
	Rows    uint64                  `json:"rows"`
}

// observedFieldsTTL is how long a scope's list is reused. Fields appear over
// hours, and autocomplete asks on every page load and scope change.
const observedFieldsTTL = time.Minute

// observedFieldsMaxScopes bounds the cache; past it the cache starts over.
const observedFieldsMaxScopes = 512

type observedEntry struct {
	set storage.ObservedFieldSet
	at  time.Time
}

type observedFieldsCache struct {
	mu      sync.Mutex
	entries map[string]observedEntry
	now     func() time.Time
}

func newObservedFieldsCache() *observedFieldsCache {
	return &observedFieldsCache{entries: map[string]observedEntry{}, now: time.Now}
}

func (c *observedFieldsCache) get(key string) (storage.ObservedFieldSet, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || c.now().Sub(e.at) > observedFieldsTTL {
		return storage.ObservedFieldSet{}, false
	}
	return e.set, true
}

func (c *observedFieldsCache) put(key string, set storage.ObservedFieldSet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= observedFieldsMaxScopes {
		c.entries = map[string]observedEntry{}
	}
	c.entries[key] = observedEntry{set: set, at: c.now()}
}

// HandleObservedFields lists the fields the logs in the request's scope actually
// carry, over all retention, from the field path rollup. Unlike /query/fieldstats it
// ignores any query, which is what lets it be this cheap.
func (h *QueryHandler) HandleObservedFields(w http.ResponseWriter, r *http.Request) {
	if err := authorizeScoped(r, rbac.RoleViewer); err != nil {
		api.WriteError(w, err.status, err.msg)
		return
	}
	scope, err := h.resolveScope(r, r.URL.Query().Get("fractal_id"), rbac.RoleViewer)
	if err != nil {
		api.WriteError(w, err.status, err.msg)
		return
	}
	ids := []string{scope.fractalID}
	if scope.isPrism() {
		ids = scope.prismMembers
	}
	ids = slices.DeleteFunc(slices.Clone(ids), func(id string) bool { return id == "" })

	key := strings.Join(slices.Sorted(slices.Values(ids)), ",")
	if scope.includeEmpty {
		key += "|unscoped"
	}
	set, ok := h.observedFields.get(key)
	if !ok {
		var qerr error
		set, qerr = h.db.ObservedFields(r.Context(), ids, scope.includeEmpty)
		if qerr != nil {
			log.Printf("[QueryHandler] observed fields: %v", qerr)
			api.WriteError(w, http.StatusInternalServerError, "Failed to list fields")
			return
		}
		h.observedFields.put(key, set)
	}
	api.WriteJSON(w, http.StatusOK, ObservedFieldsResponse{Success: true, Fields: set.Fields, Rows: set.Rows})
}

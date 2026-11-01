package dictionaries

import (
	"bifract/pkg/api"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// Handler provides HTTP endpoints for dictionary management.
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
}

// NewHandler creates a new dictionary handler.
func NewHandler(manager *Manager, fractalManager *fractals.Manager) *Handler {
	return &Handler{manager: manager, fractalManager: fractalManager}
}

// ---- Dictionary CRUD ----

func (h *Handler) HandleListDictionaries(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
		return
	}

	dicts, err := h.manager.ListDictionaries(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Dictionaries] Failed to list dictionaries: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load dictionaries")
		return
	}
	if dicts == nil {
		dicts = []*Dictionary{}
	}
	for _, d := range dicts {
		d.Editable = d.changeableFrom(r, fractalID, prismID)
	}
	api.WriteList(w, dicts)
}

// CreateDictionaryRequest defines a new dictionary and its columns.
type CreateDictionaryRequest struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	KeyColumn   string             `json:"key_column"`
	Columns     []DictionaryColumn `json:"columns"`
	IsGlobal    bool               `json:"is_global"`
	// Kind is "value" (the default), "network" for a CIDR list or "pattern".
	Kind string `json:"kind"`
	// Source fills the list from a URL. Setting one needs an administrator.
	Source *Source `json:"source,omitempty"`
}

func (h *Handler) HandleCreateDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
		return
	}

	var req CreateDictionaryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Source != nil && !h.requireSourceAdmin(w, r) {
		return
	}

	username := auth.AttributionUsername(r.Context())
	dict, err := h.manager.CreateDictionary(r.Context(), CreateOptions{
		FractalID: fractalID, PrismID: prismID, Name: req.Name, Description: req.Description,
		KeyColumn: req.KeyColumn, Columns: req.Columns, CreatedBy: username,
		IsGlobal: req.IsGlobal, Kind: req.Kind, Source: req.Source,
	})
	if req.Source != nil {
		h.respondSourceResult(w, dict, err)
		return
	}
	if err != nil {
		log.Printf("[Dictionaries] Failed to create dictionary: %v", err)
		h.respondManagerError(w, "Failed to create dictionary", err)
		return
	}
	h.respondSuccess(w, dict)
}

// requireSourceAdmin restricts choosing what a source URL points at to
// administrators: the server makes the request, from inside its own network.
func (h *Handler) requireSourceAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !api.Granted(r, api.AccessTenantAdmin) {
		h.respondError(w, http.StatusForbidden, "Only an administrator can fill a lookup from a URL")
		return false
	}
	return true
}

// HandleSetSource attaches or replaces a list's source and loads it now.
func (h *Handler) HandleSetSource(w http.ResponseWriter, r *http.Request) {
	if !h.requireSourceAdmin(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	var req Source
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dict, err := h.manager.SetSource(r.Context(), existing.ID, req)
	h.respondSourceResult(w, dict, err)
}

// HandleDetachSource stops refreshing a list and makes its rows editable.
func (h *Handler) HandleDetachSource(w http.ResponseWriter, r *http.Request) {
	if !h.requireSourceAdmin(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	dict, err := h.manager.DetachSource(r.Context(), existing.ID)
	if errors.Is(err, errSourceBusy) {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		log.Printf("[Dictionaries] Failed to detach source from dictionary %s: %v", existing.ID, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to detach the source")
		return
	}
	h.respondSuccess(w, dict)
}

// HandleSyncSource refreshes a list from its source now.
func (h *Handler) HandleSyncSource(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	dict, err := h.manager.SyncSource(r.Context(), existing.ID)
	h.respondSourceResult(w, dict, err)
}

// respondSourceResult answers a call that loads a source: the list once the
// load is done, a 202 with the list while it goes on in the background, and
// otherwise why it failed. A source problem is the caller's to fix and is
// described; anything else stays in the server log.
func (h *Handler) respondSourceResult(w http.ResponseWriter, dict *Dictionary, err error) {
	switch {
	case err == nil:
		h.respondSuccess(w, dict)
	case errors.Is(err, errSourceLoading):
		api.WriteJSON(w, http.StatusAccepted, api.Response[*Dictionary]{Success: true, Data: dict,
			Message: "Still loading; the list fills in when it finishes"})
	case errors.Is(err, errSourceBusy):
		h.respondError(w, http.StatusConflict, err.Error())
	case errors.Is(err, errSourceCooldown):
		h.respondError(w, http.StatusTooManyRequests, err.Error())
	case IsInvalidInput(err):
		h.respondError(w, http.StatusBadRequest, "Could not load the source: "+err.Error())
	default:
		log.Printf("[Dictionaries] source load failed: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Could not load the source: "+sourceErrorText(err))
	}
}

func (h *Handler) HandleGetDictionary(w http.ResponseWriter, r *http.Request) {
	dict := h.getDictionaryScoped(w, r)
	if dict == nil {
		return
	}
	h.respondSuccess(w, dict)
}

// UpdateDictionaryRequest carries the dictionary metadata that may change; columns are managed separately.
type UpdateDictionaryRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	IsGlobal    bool   `json:"is_global"`
}

func (h *Handler) HandleUpdateDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID

	var req UpdateDictionaryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dict, err := h.manager.UpdateDictionary(r.Context(), id, req.Name, req.Description, req.IsGlobal)
	if err != nil {
		log.Printf("[Dictionaries] Failed to update dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update dictionary")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleDeleteDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	if err := h.manager.DeleteDictionary(r.Context(), id); errors.Is(err, errSourceBusy) {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	} else if err != nil {
		log.Printf("[Dictionaries] Failed to delete dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete dictionary")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// ---- Columns ----

// AddColumnRequest names a column to add.
type AddColumnRequest struct {
	Name string `json:"name"`
}

// respondManagerError reports a caller-fixable failure with its reason and anything
// else as a generic 500, so the editor can show why an action was refused.
func (h *Handler) respondManagerError(w http.ResponseWriter, fallback string, err error) {
	if IsInvalidInput(err) {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.respondError(w, http.StatusInternalServerError, fallback)
}

func (h *Handler) HandleAddColumn(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID

	var req AddColumnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	dict, err := h.manager.AddColumn(r.Context(), id, req.Name)
	if err != nil {
		log.Printf("[Dictionaries] Failed to add column to dictionary %s: %v", id, err)
		h.respondManagerError(w, "Failed to add column", err)
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleRemoveColumn(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	colName := chi.URLParam(r, "name")

	dict, err := h.manager.RemoveColumn(r.Context(), id, colName)
	if err != nil {
		log.Printf("[Dictionaries] Failed to remove column from dictionary %s: %v", id, err)
		h.respondManagerError(w, "Failed to remove column", err)
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleSetColumnKey(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	colName := chi.URLParam(r, "name")

	dict, err := h.manager.SetColumnKey(r.Context(), id, colName)
	if err != nil {
		log.Printf("[Dictionaries] Failed to set column key on dictionary %s: %v", id, err)
		h.respondManagerError(w, "Failed to set column key", err)
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleUnsetColumnKey(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	colName := chi.URLParam(r, "name")

	dict, err := h.manager.UnsetColumnKey(r.Context(), id, colName)
	if err != nil {
		log.Printf("[Dictionaries] Failed to unset column key on dictionary %s: %v", id, err)
		h.respondManagerError(w, "Failed to unset column key", err)
		return
	}
	h.respondSuccess(w, dict)
}

// CaseInsensitiveKeysRequest toggles case-insensitive lookups on a dictionary.
type CaseInsensitiveKeysRequest struct {
	Enabled bool `json:"enabled"`
	// ConfirmCollisions applies the change even though keys stop being distinct
	// once case is ignored. Without it such a request is refused, because the
	// colliding rows would quietly become unreachable.
	ConfirmCollisions bool `json:"confirm_collisions"`
}

func (h *Handler) HandleSetCaseInsensitiveKeys(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	var req CaseInsensitiveKeysRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	dict, err := h.manager.SetCaseInsensitiveKeys(r.Context(), existing.ID, req.Enabled, req.ConfirmCollisions)
	if err != nil {
		log.Printf("[Dictionaries] Failed to set case sensitivity on dictionary %s: %v", existing.ID, err)
		h.respondManagerError(w, "Failed to change case sensitivity", err)
		return
	}
	h.respondSuccess(w, dict)
}

// HandleKeyCollisions previews what ignoring case would cost, so the toggle can
// say so before it is flipped rather than after.
func (h *Handler) HandleKeyCollisions(w http.ResponseWriter, r *http.Request) {
	existing := h.getDictionaryScoped(w, r)
	if existing == nil {
		return
	}
	report, err := h.manager.KeyCollisions(r.Context(), existing.ID)
	if err != nil {
		log.Printf("[Dictionaries] Failed to check key collisions on dictionary %s: %v", existing.ID, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to check key collisions")
		return
	}
	h.respondSuccess(w, report)
}

// ---- Data (rows) ----

func (h *Handler) HandleGetRows(w http.ResponseWriter, r *http.Request) {
	existing := h.getDictionaryScoped(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	search := r.URL.Query().Get("search")

	limit := 100
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if n, err := strconv.Atoi(o); err == nil && n >= 0 {
			offset = n
		}
	}

	rows, total, err := h.manager.GetRows(r.Context(), id, search, limit, offset)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get rows for dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load rows")
		return
	}
	if rows == nil {
		rows = []DictionaryRow{}
	}
	api.WritePage(w, rows, api.Page{Total: int(total), Limit: limit, Offset: offset})
}

// UpsertRowsRequest carries the rows to insert or update.
type UpsertRowsRequest struct {
	Rows []DictionaryRow `json:"rows"`
}

func (h *Handler) HandleUpsertRows(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID

	var req UpsertRowsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.manager.UpsertRows(r.Context(), id, req.Rows); err != nil {
		log.Printf("[Dictionaries] Failed to upsert rows in dictionary %s: %v", id, err)
		h.respondManagerError(w, "Failed to save rows", err)
		return
	}
	h.respondSuccess(w, map[string]int{"upserted": len(req.Rows)})
}

func (h *Handler) HandleDeleteRow(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	// A key can contain anything, "/" included, which no path segment survives:
	// prefer the query string and keep the path form for older callers.
	key := r.URL.Query().Get("key")
	if key == "" {
		key = chi.URLParam(r, "key")
	}

	if err := h.manager.DeleteRow(r.Context(), id, key); err != nil {
		log.Printf("[Dictionaries] Failed to delete row from dictionary %s: %v", id, err)
		h.respondManagerError(w, "Failed to delete row", err)
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// HandleImportCSV loads dictionary rows from a CSV. Accepts multipart/form-data
// with a "file" field or a raw CSV body, gzip-encoded or not, and streams it
// rather than buffering, so the size of the file is the client's concern alone.
//
// reload=false defers the live-dictionary refresh and the row-count update, for
// a client sending one file as a series of requests; it must finish with
// POST /dictionaries/{id}/reload, which does both.
func (h *Handler) HandleImportCSV(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID

	var csvReader io.Reader = r.Body
	if strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/form-data") {
		// Spills to a temporary file past this, so a large upload is bounded by
		// disk rather than by memory.
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			h.respondError(w, http.StatusBadRequest, "failed to parse multipart form")
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			h.respondError(w, http.StatusBadRequest, "file field required")
			return
		}
		defer file.Close()
		csvReader = file
	}

	if strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "gzip") {
		gz, err := gzip.NewReader(csvReader)
		if err != nil {
			h.respondError(w, http.StatusBadRequest, "body is not gzip data")
			return
		}
		defer gz.Close()
		// Compression means a small body can expand without bound, so what it
		// expands to is capped as it is read. Nothing else here is: an
		// uncompressed body is already bounded by what the client sent.
		csvReader = &cappedReader{r: gz, max: maxDecompressedImport}
	}

	opts := ImportOptions{DeferReload: r.URL.Query().Get("reload") == "false"}
	count, err := h.manager.ImportCSV(r.Context(), id, csvReader, opts)
	if err != nil {
		log.Printf("[Dictionaries] CSV import failed for dictionary %s after %d rows: %v", id, count, err)
		// A partial load stays committed, so the count travels with the error and
		// the client can report what landed.
		respondErrorDetail(w, importStatus(err), "CSV import failed: "+err.Error(),
			map[string]int{"imported": count})
		return
	}
	h.respondSuccess(w, map[string]int{"imported": count})
}

// maxDecompressedImport is what one gzip-encoded import may expand to. Far
// larger than any dictionary CSV, and far short of a bomb.
const maxDecompressedImport = 2 << 30

// cappedReader fails once it has read more than it was allowed. io.LimitReader
// would report a clean end of file instead, which reads as a short CSV that
// imported successfully.
type cappedReader struct {
	r    io.Reader
	max  int64
	read int64
}

func (c *cappedReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += int64(n)
	if c.read > c.max {
		return n, badInput("the data expands to more than %d bytes", c.max)
	}
	return n, err
}

// importStatus separates a malformed file from a failure of ours.
func importStatus(err error) int {
	if IsInvalidInput(err) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

// HandleExportCSV streams all dictionary rows as a CSV file download.
func (h *Handler) HandleExportCSV(w http.ResponseWriter, r *http.Request) {
	existing := h.getDictionaryScoped(w, r)
	if existing == nil {
		return
	}
	filename := strings.ReplaceAll(existing.Name, `"`, "_") + ".csv"
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	if err := h.manager.ExportCSV(r.Context(), existing.ID, w); err != nil {
		log.Printf("[Dictionaries] CSV export failed for dictionary %s: %v", existing.ID, err)
	}
}

// HandleReloadDictionary forces a ClickHouse dictionary reload.
func (h *Handler) HandleReloadDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	existing := h.getDictionaryForWrite(w, r)
	if existing == nil {
		return
	}
	id := existing.ID
	if err := h.manager.ReloadDictionary(r.Context(), id); err != nil {
		log.Printf("[Dictionaries] Failed to reload dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to reload dictionary")
		return
	}
	h.respondSuccess(w, map[string]bool{"reloaded": true})
}

// ---- Dictionary Actions ----

func (h *Handler) HandleListDictionaryActions(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope for dictionary actions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine scope")
		return
	}
	actions, err := h.manager.ListDictionaryActions(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Dictionaries] Failed to list dictionary actions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load dictionary actions")
		return
	}
	if actions == nil {
		actions = []*DictionaryAction{}
	}
	api.WriteList(w, actions)
}

// DictionaryActionRequest carries a dictionary action, on create and on update.
type DictionaryActionRequest struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	DictionaryName    string `json:"dictionary_name"`
	MaxLogsPerTrigger int    `json:"max_logs_per_trigger"`
	Enabled           bool   `json:"enabled"`
}

func (h *Handler) HandleCreateDictionaryAction(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	var req DictionaryActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" || req.DictionaryName == "" {
		h.respondError(w, http.StatusBadRequest, "name and dictionary_name are required")
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope for dictionary action create: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine scope")
		return
	}

	username := auth.AttributionUsername(r.Context())
	action, err := h.manager.CreateDictionaryAction(r.Context(), req.Name, req.Description,
		req.DictionaryName, req.MaxLogsPerTrigger, req.Enabled, username, fractalID, prismID)
	if err != nil {
		log.Printf("[Dictionaries] Failed to create dictionary action: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to create dictionary action")
		return
	}
	h.respondSuccess(w, action)
}

func (h *Handler) HandleGetDictionaryAction(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	action, err := h.manager.GetDictionaryAction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Dictionary action not found")
		return
	}
	if !h.requireDictionaryActionInScope(w, r, action) {
		return
	}
	h.respondSuccess(w, action)
}

func (h *Handler) HandleUpdateDictionaryAction(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	existing, err := h.manager.GetDictionaryAction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Dictionary action not found")
		return
	}
	if !h.requireDictionaryActionInScope(w, r, existing) {
		return
	}

	var req DictionaryActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	action, err := h.manager.UpdateDictionaryAction(r.Context(), id, req.Name, req.Description,
		req.DictionaryName, req.MaxLogsPerTrigger, req.Enabled)
	if err != nil {
		log.Printf("[Dictionaries] Failed to update dictionary action %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update dictionary action")
		return
	}
	h.respondSuccess(w, action)
}

func (h *Handler) HandleDeleteDictionaryAction(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	existing, err := h.manager.GetDictionaryAction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Dictionary action not found")
		return
	}
	if !h.requireDictionaryActionInScope(w, r, existing) {
		return
	}
	if err := h.manager.DeleteDictionaryAction(r.Context(), id); err != nil {
		log.Printf("[Dictionaries] Failed to delete dictionary action %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete dictionary action")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// requireDictionaryActionInScope verifies the action belongs to the caller's current
// session scope. Writes a 404 and returns false on mismatch.
func (h *Handler) requireDictionaryActionInScope(w http.ResponseWriter, r *http.Request, action *DictionaryAction) bool {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine scope")
		return false
	}
	inScope := (prismID != "" && action.PrismID == prismID) ||
		(fractalID != "" && action.FractalID == fractalID)
	if !inScope {
		h.respondError(w, http.StatusNotFound, "Dictionary action not found")
		return false
	}
	return true
}

// ---- Helpers ----

// getDictionaryScoped fetches the {id} dictionary for reading. It must belong
// to the caller's scope or be global; otherwise it answers 404 and returns nil.
func (h *Handler) getDictionaryScoped(w http.ResponseWriter, r *http.Request) *Dictionary {
	return h.dictionaryInScope(w, r, false)
}

// getDictionaryForWrite is getDictionaryScoped for a change. A global list is
// changed only from its own scope or by a tenant admin, answering 403 otherwise.
func (h *Handler) getDictionaryForWrite(w http.ResponseWriter, r *http.Request) *Dictionary {
	return h.dictionaryInScope(w, r, true)
}

func (h *Handler) dictionaryInScope(w http.ResponseWriter, r *http.Request, write bool) *Dictionary {
	dict, err := h.manager.GetDictionary(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Dictionary not found")
		return nil
	}
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
		return nil
	}
	dict.Editable = dict.changeableFrom(r, fractalID, prismID)
	switch {
	case !dict.IsGlobal && !dict.ownedBy(fractalID, prismID):
		h.respondError(w, http.StatusNotFound, "Dictionary not found")
	case write && !dict.Editable:
		h.respondError(w, http.StatusForbidden, "A global lookup can only be changed from the fractal or prism it was created in")
	default:
		return dict
	}
	return nil
}

func (d *Dictionary) ownedBy(fractalID, prismID string) bool {
	return (d.FractalID != "" && d.FractalID == fractalID) || (d.PrismID != "" && d.PrismID == prismID)
}

// changeableFrom reports whether r may change d from the given scope: its own
// lists, and a global from elsewhere only as a tenant admin.
func (d *Dictionary) changeableFrom(r *http.Request, fractalID, prismID string) bool {
	return d.ownedBy(fractalID, prismID) || (d.IsGlobal && api.Granted(r, api.AccessTenantAdmin))
}

func (h *Handler) getScope(r *http.Request) (fractalID, prismID string, err error) {
	if pid, ok := r.Context().Value("selected_prism").(string); ok && pid != "" {
		return "", pid, nil
	}
	if fid, ok := r.Context().Value("selected_fractal").(string); ok && fid != "" {
		return fid, "", nil
	}
	if h.fractalManager == nil {
		return "", "", fmt.Errorf("no fractal context available")
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, "", nil
}

// requireAnalyst checks that the current user has at least analyst role on the session fractal or prism.
func (h *Handler) requireAnalyst(w http.ResponseWriter, r *http.Request) bool {
	user, _ := r.Context().Value("user").(*storage.User)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
}

func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	api.WriteSuccess(w, data)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	api.WriteError(w, status, msg)
}

// respondErrorDetail reports a failure that still has a result worth returning,
// which a partly committed bulk load does.
func respondErrorDetail[T any](w http.ResponseWriter, status int, msg string, data T) {
	api.WriteJSON(w, status, api.Response[T]{
		Success: false,
		Error:   msg,
		Code:    api.CodeForStatus(status),
		Data:    data,
	})
}

package schemafields

import (
	"bifract/pkg/api"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"bifract/pkg/storage"
)

// reconcileTimeout bounds a single background ClickHouse reconcile. The ALTER
// can be slow on large tables but should never run unbounded.
const reconcileTimeout = 30 * time.Minute

type Handler struct {
	manager       *Manager
	ch            *storage.ClickHouseClient
	onFieldChange func(map[string]bool) // called after create/reset so the parser can reload
	reconcileMu   sync.Mutex            // serializes background ClickHouse DDL
	// sweeper produces every measurement the insights payload reports. The
	// handler only ever reads its results and, on request, asks it to run again.
	sweeper *Sweeper
}

// apiResponse is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type apiResponse = api.Response[any]

// NewHandler creates a handler for the schema fields admin API.
// onFieldChange is called with the new complete type-hinted field map after
// any write operation so the caller (main) can update parser.SetCustomTypeHintedFields.
func NewHandler(manager *Manager, ch *storage.ClickHouseClient, onFieldChange func(map[string]bool)) *Handler {
	return &Handler{manager: manager, ch: ch, onFieldChange: onFieldChange}
}

// SetSweeper wires the background measurement the insights payload is read from.
func (h *Handler) SetSweeper(s *Sweeper) { h.sweeper = s }

// HandleList returns project defaults and user-defined custom fields.
func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	custom, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] list: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load schema fields")
		return
	}
	if custom == nil {
		custom = []SchemaField{}
	}
	h.respondSuccess(w, map[string]interface{}{
		"defaults": ProjectDefaultFields,
		"custom":   custom,
	})
}

// HandleCatalog returns the known field names (project defaults + user-defined
// custom fields) for query autocompletion. Unlike HandleList it is not gated to
// admins: field names are schema, not log content, and every authenticated user
// already queries against them. The response is a flat, deduplicated name list.
func (h *Handler) HandleCatalog(w http.ResponseWriter, r *http.Request) {
	custom, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] catalog: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load schema fields")
		return
	}
	seen := make(map[string]bool, len(ProjectDefaultFields)+len(custom))
	names := make([]string, 0, len(ProjectDefaultFields)+len(custom))
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		names = append(names, name)
	}
	for _, f := range ProjectDefaultFields {
		add(f.FieldName)
	}
	for _, f := range custom {
		add(f.FieldName)
	}
	h.respondSuccess(w, map[string]interface{}{"fields": names})
}

// HandleCreate adds a custom field, syncs ClickHouse schema, and notifies the parser.
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	f, err := h.manager.Create(r.Context(), req, h.getCurrentUser(r))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The field is persisted (status pending) and immediately visible to the
	// parser. The ClickHouse reconcile (MODIFY COLUMN + ADD INDEX) can block for
	// minutes on large tables, so it runs in the background and reports its
	// outcome via sync_status. Respond now so the UI list updates instantly.
	// Only refresh the parser when List succeeds: passing a nil set on error
	// would wipe every custom type-hint until the next successful reload.
	if custom, err := h.manager.List(r.Context()); err != nil {
		log.Printf("[SchemaFields] list after create %q: %v", f.FieldName, err)
	} else {
		h.notifyFieldChange(custom)
	}
	h.reconcileAsync([]string{f.FieldName})

	h.respondSuccess(w, f)
}

// HandleInsights returns the field distribution, storage, capacity, and ranked
// suggestions in one payload so the schema tab renders from a single request.
// It reads only the tables the background sweep writes.
func (h *Handler) HandleInsights(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	ins, err := h.BuildInsights(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] insights: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Could not read field statistics")
		return
	}
	h.respondSuccess(w, ins)
}

// HandleRefresh asks the background sweep to re-measure now. It returns
// immediately: the sweep takes as long as it takes, and the page picks up the
// result on its next load rather than holding a request open for it.
func (h *Handler) HandleRefresh(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	if h.sweeper == nil {
		h.respondError(w, http.StatusServiceUnavailable, "Schema measurement is not running")
		return
	}
	h.sweeper.Refresh()
	h.respondSuccess(w, map[string]string{"message": "Measuring schema"})
}

// HandleIgnore dismisses a suggested field.
func (h *Handler) HandleIgnore(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	name := chi.URLParam(r, "name")
	if err := h.manager.Ignore(r.Context(), name, h.getCurrentUser(r)); err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.respondSuccess(w, map[string]string{"message": fmt.Sprintf("Field %q ignored", name)})
}

// HandleUnignore restores a dismissed field to the suggestions list.
func (h *Handler) HandleUnignore(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	name := chi.URLParam(r, "name")
	if err := h.manager.Unignore(r.Context(), name); err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.respondSuccess(w, map[string]string{"message": fmt.Sprintf("Field %q restored", name)})
}

// reconcileAsync applies the current full field set to ClickHouse in the
// background and updates sync_status for the named fields. Reconciles are
// serialized so concurrent admin actions never issue overlapping ALTERs.
func (h *Handler) reconcileAsync(fieldNames []string) {
	go func() {
		h.reconcileMu.Lock()
		defer h.reconcileMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), reconcileTimeout)
		defer cancel()

		custom, err := h.manager.List(ctx)
		if err != nil {
			log.Printf("[SchemaFields] list before reconcile: %v", err)
			h.markFields(ctx, fieldNames, SyncStatusError, err.Error())
			return
		}
		all := append(append([]SchemaField{}, ProjectDefaultFields...), custom...)
		res, err := h.ch.ReconcileSchemaFields(ctx, ToSpecs(all))
		if err != nil {
			log.Printf("[SchemaFields] reconcile %v: %v", fieldNames, err)
			h.markFields(ctx, fieldNames, SyncStatusError, err.Error())
			return
		}
		// A field whose skip index failed is not fully applied: the type hint is
		// live and the field is queryable, but it will not prune. Report that
		// rather than a blanket active.
		for _, name := range fieldNames {
			if e, bad := res.IndexErrors[name]; bad {
				h.markFields(ctx, []string{name}, SyncStatusError, e)
				continue
			}
			h.markFields(ctx, []string{name}, SyncStatusActive, "")
		}
	}()
}

// dropFieldIndexAsync removes a deleted field's skip index from ClickHouse in the
// background, serialized on reconcileMu so it can't race a concurrent add/reconcile
// (e.g. an immediate delete-then-recreate). The type hint is intentionally retained.
// Dropping a skip index does not alter the column/insert schema, so it is safe on
// clusters and cannot back up the distributed insert queue.
func (h *Handler) dropFieldIndexAsync(fieldName string) {
	go func() {
		h.reconcileMu.Lock()
		defer h.reconcileMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), reconcileTimeout)
		defer cancel()

		if err := h.ch.DropSchemaFieldIndex(ctx, fieldName); err != nil {
			log.Printf("[SchemaFields] drop index for deleted field %q: %v", fieldName, err)
		}
	}()
}

// markFields updates the sync status of the given fields, ignoring those that
// no longer exist (e.g. removed mid-reconcile).
func (h *Handler) markFields(ctx context.Context, fieldNames []string, status, errMsg string) {
	for _, name := range fieldNames {
		if err := h.manager.UpdateSyncStatus(ctx, name, status, errMsg); err != nil {
			log.Printf("[SchemaFields] update status %q: %v", name, err)
		}
	}
}

// HandleDelete removes a custom field from Postgres (soft: ClickHouse schema unchanged until reset).
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	name := chi.URLParam(r, "name")
	if err := h.manager.Delete(r.Context(), name); err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Only refresh the parser when List succeeds (see HandleCreate).
	if custom, err := h.manager.List(r.Context()); err != nil {
		log.Printf("[SchemaFields] list after delete %q: %v", name, err)
	} else {
		h.notifyFieldChange(custom)
	}
	// Drop the field's ClickHouse skip index so a later recreate with a different
	// index type applies cleanly (the reconcile is additive and would otherwise keep
	// the stale index). Serialized with reconciles so it can't race a concurrent add.
	h.dropFieldIndexAsync(name)
	h.respondSuccess(w, map[string]string{"message": fmt.Sprintf("Field %q removed", name)})
}

// ResetRequest carries the confirmation phrase guarding a destructive reset.
type ResetRequest struct {
	Confirm string `json:"confirm"`
}

// HandleReset truncates all log data, rebuilds ClickHouse schema from current config,
// and reloads the parser type-hint map.
func (h *Handler) HandleReset(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	var body ResetRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Confirm != "DELETE ALL LOG DATA" {
		h.respondError(w, http.StatusBadRequest, `confirmation required: {"confirm": "DELETE ALL LOG DATA"}`)
		return
	}

	custom, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] list before reset: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load schema fields")
		return
	}
	all := append(append([]SchemaField{}, ProjectDefaultFields...), custom...)

	if err := h.ch.TruncateAndReschema(r.Context(), ToSpecs(all)); err != nil {
		log.Printf("[SchemaFields] reset: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Schema reset failed: "+err.Error())
		return
	}

	// The table is empty after truncate, so the reschema above is fast and has
	// already completed synchronously: every field is now fully indexed.
	names := make([]string, len(custom))
	for i, f := range custom {
		names[i] = f.FieldName
	}
	h.markFields(r.Context(), names, SyncStatusActive, "")

	h.notifyFieldChange(custom)

	// Every measurement described data that no longer exists. Clearing them
	// keeps deleted fields from lingering on the tab until the next sweep, and
	// the sweep is asked to run so the page refills rather than staying blank.
	if err := clearStats(r.Context(), h.manager.pg); err != nil {
		log.Printf("[SchemaFields] clear stats after reset: %v", err)
	}
	if h.sweeper != nil {
		h.sweeper.Refresh()
	}
	h.respondSuccess(w, map[string]string{"message": "Schema reset complete. All log data has been deleted."})
}

// HandleExportYAML returns the custom schema fields as a YAML document.
// Project defaults are built into the binary and are intentionally excluded.
func (h *Handler) HandleExportYAML(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	custom, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] export list: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load schema fields")
		return
	}
	export := SchemaExport{Fields: make([]SchemaFieldExport, 0, len(custom))}
	for _, f := range custom {
		export.Fields = append(export.Fields, SchemaFieldExport{
			FieldName: f.FieldName,
			IndexType: string(f.IndexType),
		})
	}
	out, err := yaml.Marshal(export)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to marshal YAML")
		return
	}
	w.Header().Set("Content-Type", "text/yaml")
	w.Header().Set("Content-Disposition", `attachment; filename="schema-fields.yaml"`)
	w.Write(out)
}

// HandleImportYAML replaces the entire custom field set with the fields in the
// uploaded YAML (replace semantics: fields absent from the file are removed).
// The full set is then reconciled into ClickHouse in the background.
func (h *Handler) HandleImportYAML(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	var export SchemaExport
	if err := yaml.Unmarshal(body, &export); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid YAML: "+err.Error())
		return
	}

	reqs := make([]CreateRequest, 0, len(export.Fields))
	for _, f := range export.Fields {
		reqs = append(reqs, CreateRequest{FieldName: f.FieldName, IndexType: IndexType(f.IndexType)})
	}

	fields, err := h.manager.ReplaceAll(r.Context(), reqs, h.getCurrentUser(r))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.FieldName
	}
	h.notifyFieldChange(fields)
	h.reconcileAsync(names)

	h.respondSuccess(w, map[string]interface{}{
		"message": fmt.Sprintf("Imported %d custom field(s). Schema is updating.", len(fields)),
		"count":   len(fields),
	})
}

func (h *Handler) notifyFieldChange(custom []SchemaField) {
	if h.onFieldChange == nil {
		return
	}
	m := make(map[string]bool, len(custom))
	for _, f := range custom {
		m[f.FieldName] = true
	}
	h.onFieldChange(m)
}

func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Admin access required")
		return false
	}
	return true
}

func (h *Handler) getCurrentUser(r *http.Request) string {
	if user, ok := r.Context().Value("user").(*storage.User); ok {
		return user.Username
	}
	return ""
}

func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	api.WriteSuccess(w, data)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	api.WriteError(w, status, msg)
}

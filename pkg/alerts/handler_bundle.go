package alerts

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
)

// origin records which scope an archive came from, as provenance in the manifest.
// It is never used to route an import: that always lands in the caller's own scope.
func origin(fractalID, prismID string) string {
	if prismID != "" {
		return "prism:" + prismID
	}
	if fractalID != "" {
		return "fractal:" + fractalID
	}
	return ""
}

// HandleExportBundle streams every manual alert in the scope, with its tests, as a
// zip an operator can carry to another instance (analyst+).
func (h *Handler) HandleExportBundle(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	data, err := h.manager.ExportBundle(r.Context(), fractalID, prismID, origin(fractalID, prismID))
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to export alerts: "+err.Error())
		return
	}

	name := fmt.Sprintf("bifract-alerts-%s.zip", time.Now().UTC().Format("2006-01-02"))
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, err := w.Write(data); err != nil {
		return
	}
}

// HandleImportBundle applies an uploaded archive to the caller's scope (analyst+).
// The body is the zip itself, so a large bundle never has to be base64'd through JSON.
func (h *Handler) HandleImportBundle(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAnalyst)
	if !ok {
		return
	}

	data, err := io.ReadAll(io.LimitReader(r.Body, MaxBundleBytes+1))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to read the archive")
		return
	}
	if len(data) == 0 {
		h.respondError(w, http.StatusBadRequest, "The archive is empty")
		return
	}
	if len(data) > MaxBundleBytes {
		h.respondError(w, http.StatusRequestEntityTooLarge, "The archive is larger than this instance accepts")
		return
	}

	// Every alert would be refused individually otherwise, which reads as a broken
	// archive rather than as the gate doing its job.
	if cfg, err := h.manager.GateConfigFor(r.Context(), fractalID, prismID); err == nil && cfg.Enabled {
		h.respondError(w, http.StatusConflict,
			"this scope reviews alert changes, so a bundle cannot be applied directly")
		return
	}

	result, err := h.manager.ImportBundle(r.Context(), data, h.attributionUser(r), fractalID, prismID,
		r.URL.Query().Get("overwrite") == "true")
	if err != nil {
		if h.gateRequiredResponse(w, err) || h.policyBlockedResponse(w, err) {
			return
		}
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	api.WriteJSON(w, http.StatusOK, api.Response[*BundleImportResult]{Success: true, Data: result})
}

package dashboards

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/attack"
	"bifract/pkg/auth"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// shareTokenPrefix identifies Bifract shared-link tokens (like the bifract_ API
// key prefix). The stored token_prefix column keeps only this plus a few chars
// for display; the full token is never persisted.
const shareTokenPrefix = "bshl_"

// sensitiveChartConfigKeys are stripped from any chart_config exposed on the
// anonymous path. Pivots/drilldowns encode navigation to OTHER dashboards or the
// search page and would leak internal structure; they are also non-functional on
// a public wallboard, which has no app shell to navigate.
var sensitiveChartConfigKeys = []string{"pivots", "pivot", "drilldowns", "drilldown"}

// ---- global toggle ----

// sharedLinksEnabled reports whether the public Shared Links feature is globally
// enabled. Default off: any error or absent setting reads as disabled.
func (h *DashboardHandler) sharedLinksEnabled(ctx context.Context) bool {
	v, err := h.pg.GetSetting(ctx, storage.SharedLinksEnabledSetting)
	return err == nil && v == "true"
}

// ---- token helpers ----

// generateShareToken returns (fullToken, sha256HexHash, displayPrefix). The full
// token carries 256 bits of entropy and is shown to the creator once; only the
// hash is stored.
func generateShareToken() (string, string, string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", "", err
	}
	full := shareTokenPrefix + base64.RawURLEncoding.EncodeToString(buf)
	sum := sha256.Sum256([]byte(full))
	hash := hex.EncodeToString(sum[:])
	prefix := full
	if len(prefix) > 13 {
		prefix = prefix[:13] // "bshl_" + 8 chars, fits token_prefix VARCHAR(16)
	}
	return full, hash, prefix, nil
}

func hashShareToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ---- public (anonymous) DTO ----

// publicWidget is the ALLOWLISTED shape returned to anonymous viewers. It carries
// only what is needed to render, and deliberately omits query_content, the
// compiled SQL, raw variables, and dashboard/fractal identifiers.
type publicWidget struct {
	ID             string          `json:"id"`
	Title          *string         `json:"title,omitempty"`
	ChartType      string          `json:"chart_type"`
	ChartConfig    json.RawMessage `json:"chart_config,omitempty"`
	PosX           int             `json:"pos_x"`
	PosY           int             `json:"pos_y"`
	Width          int             `json:"width"`
	Height         int             `json:"height"`
	Results        json.RawMessage `json:"results,omitempty"` // sanitized last_results (sql stripped)
	LastExecutedAt *time.Time      `json:"last_executed_at,omitempty"`
}

// publicDashboard is the anonymous response. No id, fractal/prism, variables,
// author, or created_by are exposed.
type publicDashboard struct {
	Name            string         `json:"name"`
	Description     string         `json:"description,omitempty"`
	TimeRangeType   string         `json:"time_range_type"`
	RefreshInterval int            `json:"refresh_interval"`
	Widgets         []publicWidget `json:"widgets"`
	// AttackMatrix is the embedded MITRE matrix, sent only when a widget renders
	// one. An anonymous viewer cannot call /attack/matrix (viewer+), and inlining
	// it here beats opening a second public endpoint for what is public MITRE
	// reference data. Absent for every dashboard without a mitre() panel.
	AttackMatrix interface{} `json:"attack_matrix,omitempty"`
}

// stripJSONKeys removes the given top-level keys from a JSON object blob. Returns
// the input unchanged if it is null/empty or not an object.
func stripJSONKeys(raw json.RawMessage, keys ...string) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil // not an object we recognize: drop rather than risk leaking
	}
	for _, k := range keys {
		delete(m, k)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return out
}

// buildPublicDashboard assembles the sanitized anonymous payload from the stored
// dashboard + widgets. It strips the compiled SQL from cached results and any
// pivot/drilldown navigation from chart configs.
func buildPublicDashboard(d *storage.Dashboard, widgets []storage.DashboardWidget) publicDashboard {
	pub := publicDashboard{
		Name:            d.Name,
		Description:     d.Description,
		TimeRangeType:   d.TimeRangeType,
		RefreshInterval: d.RefreshInterval,
		Widgets:         make([]publicWidget, 0, len(widgets)),
	}
	needsAttack := false
	for i := range widgets {
		w := widgets[i]
		results := stripJSONKeys(w.LastResults, "sql")
		if resultChartType(results) == "mitre" {
			needsAttack = true
		}
		pub.Widgets = append(pub.Widgets, publicWidget{
			ID:             w.ID,
			Title:          w.Title,
			ChartType:      w.ChartType,
			ChartConfig:    stripJSONKeys(w.ChartConfig, sensitiveChartConfigKeys...),
			PosX:           w.PosX,
			PosY:           w.PosY,
			Width:          w.Width,
			Height:         w.Height,
			Results:        results,
			LastExecutedAt: w.LastExecutedAt,
		})
	}
	if needsAttack {
		// A corrupt embedded matrix is a build defect, not a reason to 500 an
		// otherwise-fine wallboard: the panel says so itself when it is missing.
		if m, err := attack.Get(); err == nil {
			pub.AttackMatrix = m
		}
	}
	return pub
}

// resultChartType reads chart_type out of a cached widget result blob. The
// renderer keys off the cached result, not the widget's configured type, so this
// must read the same field.
func resultChartType(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var probe struct {
		ChartType string `json:"chart_type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	return probe.ChartType
}

// ---- anonymous handler ----

// sharedNotFound returns an indistinguishable 404 for every failure mode
// (feature off, unknown token, expired, revoked, missing dashboard), so the
// endpoint is not an oracle for which tokens exist.
func sharedNotFound(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(map[string]interface{}{"error": "not found"})
}

// HandleSharedDashboard serves a dashboard to an anonymous viewer via a share
// token. It NEVER executes BQL: it returns only cached widget results, and
// registers the dashboard for keep-warm so the background executor refreshes its
// own stored widgets on cadence. The token in the path is the only input.
func (h *DashboardHandler) HandleSharedDashboard(w http.ResponseWriter, r *http.Request) {
	if !h.sharedLinksEnabled(r.Context()) {
		sharedNotFound(w)
		return
	}
	token := chi.URLParam(r, "token")
	if token == "" || !strings.HasPrefix(token, shareTokenPrefix) {
		sharedNotFound(w)
		return
	}

	link, err := h.pg.GetDashboardSharedLinkByHash(r.Context(), hashShareToken(token))
	if err != nil {
		sharedNotFound(w)
		return
	}
	if link.RevokedAt != nil {
		sharedNotFound(w)
		return
	}
	if link.ExpiresAt != nil && time.Now().After(*link.ExpiresAt) {
		sharedNotFound(w)
		return
	}

	dashboard, err := h.pg.GetDashboard(r.Context(), link.DashboardID)
	if err != nil {
		sharedNotFound(w)
		return
	}
	widgets, err := h.pg.GetDashboardWidgets(r.Context(), link.DashboardID)
	if err != nil {
		sharedNotFound(w)
		return
	}

	// Keep the cache warm without any authenticated viewer, bounded/coalesced by
	// the executor. This is the only anonymous-triggered execution path and it
	// only ever re-runs the dashboard's own stored widgets.
	if h.executor != nil {
		h.executor.MarkSharedActive(dashboard)
	}
	// Best-effort audit; never block or fail the read on it.
	h.pg.TouchDashboardSharedLink(context.Background(), link.ID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Robots-Tag", "noindex, nofollow")
	json.NewEncoder(w).Encode(buildPublicDashboard(dashboard, widgets))
}

// ---- authenticated management handlers (analyst+ on the dashboard's scope) ----

type createSharedLinkRequest struct {
	Label            string `json:"label"`
	ExpiresInSeconds int64  `json:"expires_in_seconds"` // 0 or absent = never expires
}

type createSharedLinkResponse struct {
	Success bool                         `json:"success"`
	Data    *storage.DashboardSharedLink `json:"data"`
	Token   string                       `json:"token"` // shown exactly once
}

// HandleCreateSharedLink mints a new shared link for a dashboard. Requires
// analyst+ on the dashboard's scope (bypassing RBAC is a privileged act) and the
// global toggle to be enabled.
func (h *DashboardHandler) HandleCreateSharedLink(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !h.sharedLinksEnabled(r.Context()) {
		jsonForbidden(w)
		return
	}
	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req createSharedLinkRequest
	// Tolerate an empty body (all fields optional).
	_ = json.NewDecoder(r.Body).Decode(&req)
	label := strings.TrimSpace(req.Label)
	if len(label) > 200 {
		label = label[:200]
	}
	var expiresAt *time.Time
	if req.ExpiresInSeconds > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresInSeconds) * time.Second)
		expiresAt = &t
	}

	createdBy := auth.AttributionUsername(r.Context())

	full, hash, prefix, err := generateShareToken()
	if err != nil {
		jsonError(w, "Failed to generate token")
		return
	}
	link, err := h.pg.CreateDashboardSharedLink(r.Context(), id, hash, prefix, label, createdBy, expiresAt)
	if err != nil {
		jsonError(w, "Failed to create shared link")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(createSharedLinkResponse{Success: true, Data: link, Token: full})
}

// HandleListSharedLinks lists the active links for a dashboard (viewer+; the
// token itself is never returned). Available even when the global toggle is off
// so admins can audit/clean up existing links.
func (h *DashboardHandler) HandleListSharedLinks(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleViewer) {
		return
	}
	links, err := h.pg.ListDashboardSharedLinks(r.Context(), id)
	if err != nil {
		jsonError(w, "Failed to list shared links")
		return
	}
	if links == nil {
		links = []storage.DashboardSharedLink{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: links})
}

// HandleRevokeSharedLink revokes a link (analyst+). Works regardless of the
// global toggle so a link can always be killed.
func (h *DashboardHandler) HandleRevokeSharedLink(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	linkID := chi.URLParam(r, "link_id")
	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}
	if err := h.pg.RevokeDashboardSharedLink(r.Context(), id, linkID); err != nil {
		jsonError(w, "Shared link not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Shared link revoked"})
}

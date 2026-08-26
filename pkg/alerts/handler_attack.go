package alerts

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/attack"
	"bifract/pkg/rbac"
)

// ATT&CK coverage endpoints. Rules already carry Sigma's attack.* tags verbatim
// in alerts.labels; these read them back against the embedded matrix so operators
// can see coverage and gaps instead of a wall of opaque label chips.

// HandleAttackMatrix returns the embedded ATT&CK matrix (viewer+).
//
// The matrix only changes when the binary does, so it is served with a strong
// ETag: the grid is ~700 techniques and there is no reason to re-download it on
// every tab visit.
func (h *Handler) HandleAttackMatrix(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}

	matrix, err := attack.Get()
	if err != nil {
		log.Printf("[Attack] Failed to load matrix: %v", err)
		h.respondError(w, http.StatusInternalServerError, "ATT&CK matrix unavailable")
		return
	}

	etag := fmt.Sprintf(`"attack-%s-%s"`, matrix.Domain, matrix.Version)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, max-age=86400")

	h.respondSuccess(w, matrix)
}

// HandleAttackCoverage returns per-technique rule counts plus the summary strip
// for the caller's current fractal or prism (viewer+).
func (h *Handler) HandleAttackCoverage(w http.ResponseWriter, r *http.Request) {
	rows, matrix, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	filter, err := attackFilter(matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.respondSuccess(w, matrix.Compute(rows, filter))
}

// TechniqueRules is one ATT&CK technique with the rules covering it, plus the
// matrix context the detail panel shows alongside them.
type TechniqueRules struct {
	Technique  *attack.Technique `json:"technique"`
	Platforms  []string          `json:"platforms"`
	LogSources []string          `json:"log_sources"`
	URL        string            `json:"url"`
	Rules      []attack.RuleRow  `json:"rules"`
	Count      int               `json:"count"`
}

// AttackGaps ranks uncovered techniques. CatalogPopulated distinguishes "no
// candidate rules exist" from "the feed catalog was never synced", which look
// the same from an empty candidate list.
type AttackGaps struct {
	Gaps             []Gap `json:"gaps"`
	UncoveredTotal   int   `json:"uncovered_total"`
	CandidateRules   int   `json:"candidate_rules"`
	CatalogPopulated bool  `json:"catalog_populated"`
	Returned         int   `json:"returned"`
}

// HandleAttackTechniqueRules returns the rules covering one technique (viewer+).
// Kept separate from the coverage payload so the grid response stays small
// regardless of how many rules a deployment has.
func (h *Handler) HandleAttackTechniqueRules(w http.ResponseWriter, r *http.Request) {
	rows, matrix, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	id := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "id")))
	technique := matrix.Technique(id)
	if technique == nil {
		h.respondError(w, http.StatusNotFound, "Unknown ATT&CK technique")
		return
	}

	filter, err := attackFilter(matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	includeSub := r.URL.Query().Get("include_sub") == "true"
	rules := matrix.RulesFor(rows, filter, technique.ID, includeSub)
	if rules == nil {
		rules = []attack.RuleRow{}
	}

	h.respondSuccess(w, TechniqueRules{
		Technique:  technique,
		Platforms:  matrix.PlatformNames(technique),
		LogSources: matrix.LogSourceNames(technique),
		URL:        techniqueURL(technique.ID),
		Rules:      rules,
		Count:      len(rules),
	})
}

// HandleAttackGaps ranks uncovered techniques by what can be done about them
// today, cross-referencing the feed rule catalog (viewer+).
//
// Coverage shows where the holes are; this is what makes them actionable, since
// "SigmaHQ has 12 rules for T1055, all filtered out by min_level" is a decision
// and "no coverage" is not.
func (h *Handler) HandleAttackGaps(w http.ResponseWriter, r *http.Request) {
	rows, matrix, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	filter, err := attackFilter(matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Attack] Failed to get scope for gaps: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	candidates, err := h.manager.listCandidateRules(r.Context(), fractalID, prismID)
	if err != nil {
		// A feed that has not synced since the catalog shipped simply has no rows.
		// The gap list is still useful without candidates, so this is not fatal.
		log.Printf("[Attack] Failed to load candidate rules: %v", err)
		candidates = nil
	}

	limit := 25
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil && n > 0 && n <= 500 {
			limit = n
		}
	}

	coverage := matrix.Compute(rows, filter)
	gaps := computeGaps(matrix, coverage, candidates, filter, limit)
	if gaps == nil {
		gaps = []Gap{}
	}

	uncovered := coverage.Summary.TechniquesTotal - coverage.Summary.TechniquesCovered
	h.respondSuccess(w, AttackGaps{
		Gaps:             gaps,
		UncoveredTotal:   uncovered,
		CandidateRules:   len(candidates),
		CatalogPopulated: len(candidates) > 0,
		Returned:         len(gaps),
	})
}

// HandleAttackTechniqueGap returns the candidate rules for one uncovered
// technique, for the detail drawer (viewer+).
func (h *Handler) HandleAttackTechniqueGap(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}

	matrix, err := attack.Get()
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "ATT&CK matrix unavailable")
		return
	}

	id := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "id")))
	technique := matrix.Technique(id)
	if technique == nil {
		h.respondError(w, http.StatusNotFound, "Unknown ATT&CK technique")
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	candidates, err := h.manager.listCandidateRules(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Attack] Failed to load candidate rules: %v", err)
		candidates = nil
	}

	// An empty coverage set makes computeGaps treat every technique as a gap, which
	// is what we want here: the caller has already decided this one is uncovered.
	gaps := computeGaps(matrix, &attack.Coverage{Techniques: map[string]*attack.TechniqueCoverage{}},
		candidates, attack.Filter{}, 0)
	for _, g := range gaps {
		if g.TechniqueID == id {
			h.respondSuccess(w, g)
			return
		}
	}

	h.respondSuccess(w, Gap{
		TechniqueID: technique.ID,
		Name:        technique.Name,
		Tactics:     technique.Tactics,
		ByReason:    map[string]int{},
		LogSources:  matrix.LogSourceNames(technique),
	})
}

// navigatorLayer is the ATT&CK Navigator layer format (v4.5). Exporting one lets
// operators open Bifract's coverage in MITRE's own tool and diff it against other
// sources, which is the lingua franca for sharing coverage.
type navigatorLayer struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Domain      string                 `json:"domain"`
	Versions    map[string]string      `json:"versions"`
	Techniques  []navigatorTechnique   `json:"techniques"`
	Gradient    map[string]interface{} `json:"gradient"`
	Legend      []map[string]string    `json:"legendItems"`
	ShowSubs    bool                   `json:"showSubtechniques"`
	SortMode    int                    `json:"sorting"`
}

type navigatorTechnique struct {
	TechniqueID string `json:"techniqueID"`
	Score       int    `json:"score"`
	Comment     string `json:"comment,omitempty"`
	Enabled     bool   `json:"enabled"`
}

// HandleAttackLayer downloads coverage as an ATT&CK Navigator layer (viewer+).
func (h *Handler) HandleAttackLayer(w http.ResponseWriter, r *http.Request) {
	rows, matrix, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	filter, err := attackFilter(matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	coverage := matrix.Compute(rows, filter)

	scopeName := r.URL.Query().Get("scope_name")
	if scopeName == "" {
		scopeName = "Bifract"
	}

	layer := navigatorLayer{
		Name:        scopeName + " detection coverage",
		Description: fmt.Sprintf("Bifract rule coverage, %d/%d techniques", coverage.Summary.TechniquesCovered, coverage.Summary.TechniquesTotal),
		Domain:      matrix.Domain,
		Versions: map[string]string{
			"attack":    majorVersion(matrix.Version),
			"navigator": "5.1.0",
			"layer":     "4.5",
		},
		Gradient: map[string]interface{}{
			"colors":   []string{"#2d2d4a", "#9c6ade"},
			"minValue": 0,
			"maxValue": maxScore(coverage),
		},
		Legend:   []map[string]string{{"label": "Rules covering the technique", "color": "#9c6ade"}},
		ShowSubs: true,
	}

	for id, cell := range coverage.Techniques {
		if cell.Total == 0 {
			continue
		}
		comment := fmt.Sprintf("%d rule(s), %d enabled", cell.Total, cell.Enabled)
		if cell.Inherited > 0 {
			comment = fmt.Sprintf("%d direct, %d via sub-techniques, %d enabled", cell.Direct, cell.Inherited, cell.Enabled)
		}
		layer.Techniques = append(layer.Techniques, navigatorTechnique{
			TechniqueID: id,
			Score:       cell.Total,
			Comment:     comment,
			Enabled:     true,
		})
	}

	filename := strings.NewReplacer(" ", "-", "\"", "", "/", "-").Replace(strings.ToLower(scopeName))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename+"-attack-coverage.json"))
	if err := json.NewEncoder(w).Encode(layer); err != nil {
		log.Printf("[Attack] Failed to write Navigator layer: %v", err)
	}
}

// attackRows resolves scope, checks RBAC, and loads the rules for the current
// session. It writes the error response itself; ok is false when it did.
func (h *Handler) attackRows(w http.ResponseWriter, r *http.Request) ([]attack.RuleRow, *attack.Matrix, bool) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return nil, nil, false
	}

	matrix, err := attack.Get()
	if err != nil {
		log.Printf("[Attack] Failed to load matrix: %v", err)
		h.respondError(w, http.StatusInternalServerError, "ATT&CK matrix unavailable")
		return nil, nil, false
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Attack] Failed to get scope: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return nil, nil, false
	}

	rows, err := h.manager.ListCoverageRows(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Attack] Failed to load coverage rows: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load rules")
		return nil, nil, false
	}

	return rows, matrix, true
}

var validSeverities = map[string]bool{"critical": true, "high": true, "medium": true, "low": true, "info": true}

func attackFilter(matrix *attack.Matrix, r *http.Request) (attack.Filter, error) {
	q := r.URL.Query()
	f := attack.Filter{
		EnabledOnly: q.Get("enabled_only") == "true",
		Severity:    q.Get("severity"),
		FeedID:      q.Get("feed_id"),
		Platform:    q.Get("platform"),
	}

	if f.Severity != "" && !validSeverities[f.Severity] {
		return f, fmt.Errorf("unknown severity %q", f.Severity)
	}
	if f.Platform != "" {
		known := false
		for _, p := range matrix.Platforms {
			if p == f.Platform {
				known = true
				break
			}
		}
		if !known {
			return f, fmt.Errorf("unknown platform %q", f.Platform)
		}
	}
	return f, nil
}

func maxScore(c *attack.Coverage) int {
	max := 1
	for _, cell := range c.Techniques {
		if cell.Total > max {
			max = cell.Total
		}
	}
	return max
}

// majorVersion trims "19.1" to "19" for the Navigator layer, which expects the
// ATT&CK major version only.
func majorVersion(v string) string {
	if dot := strings.IndexByte(v, '.'); dot > 0 {
		return v[:dot]
	}
	return v
}

func techniqueURL(id string) string {
	return "https://attack.mitre.org/techniques/" + strings.ReplaceAll(id, ".", "/") + "/"
}

package alerts

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

// AttackCoverage is the coverage map plus, per uncovered technique, how many
// unimported feed rules could close it. The two travel together because the map
// colours those cells: a gap a feed can close is a different thing to look at
// than a gap that needs new telemetry.
type AttackCoverage struct {
	Techniques map[string]*attack.TechniqueCoverage `json:"techniques"`
	Summary    attack.Summary                       `json:"summary"`
	Candidates map[string]int                       `json:"candidates"`
}

// HandleAttackCoverage returns per-technique rule counts plus the summary strip
// for the caller's current fractal or prism (viewer+).
func (h *Handler) HandleAttackCoverage(w http.ResponseWriter, r *http.Request) {
	scope, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	filter, err := attackFilter(scope.Matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	coverage := scope.Matrix.Compute(scope.Rows, filter)

	counts := map[string]int{}
	candidates, err := h.manager.listCandidateRules(r.Context(), scope.FractalID, scope.PrismID)
	if err != nil {
		// A feed that has never synced simply has no catalog rows, and the map is
		// still correct without the amber cells, so this is not fatal.
		log.Printf("[Attack] Failed to load candidate rules: %v", err)
	} else {
		counts = candidateCounts(scope.Matrix, coverage, candidates, filter)
	}

	h.respondSuccess(w, AttackCoverage{
		Techniques: coverage.Techniques,
		Summary:    coverage.Summary,
		Candidates: counts,
	})
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

// HandleAttackTechniqueRules returns the rules covering one technique (viewer+).
// Kept separate from the coverage payload so the grid response stays small
// regardless of how many rules a deployment has.
func (h *Handler) HandleAttackTechniqueRules(w http.ResponseWriter, r *http.Request) {
	scope, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	id := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "id")))
	technique := scope.Matrix.Technique(id)
	if technique == nil {
		h.respondError(w, http.StatusNotFound, "Unknown ATT&CK technique")
		return
	}

	filter, err := attackFilter(scope.Matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	includeSub := r.URL.Query().Get("include_sub") == "true"
	rules := scope.Matrix.RulesFor(scope.Rows, filter, technique.ID, includeSub)
	if rules == nil {
		rules = []attack.RuleRow{}
	}

	h.respondSuccess(w, TechniqueRules{
		Technique:  technique,
		Platforms:  scope.Matrix.PlatformNames(technique),
		LogSources: scope.Matrix.LogSourceNames(technique),
		URL:        techniqueURL(technique.ID),
		Rules:      rules,
		Count:      len(rules),
	})
}

// HandleAttackTechniqueGap returns the candidate rules for one uncovered
// technique, for the detail drawer (viewer+).
func (h *Handler) HandleAttackTechniqueGap(w http.ResponseWriter, r *http.Request) {
	scope, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	id := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "id")))
	technique := scope.Matrix.Technique(id)
	if technique == nil {
		h.respondError(w, http.StatusNotFound, "Unknown ATT&CK technique")
		return
	}

	candidates, err := h.manager.listCandidateRules(r.Context(), scope.FractalID, scope.PrismID)
	if err != nil {
		log.Printf("[Attack] Failed to load candidate rules: %v", err)
		candidates = nil
	}

	h.respondSuccess(w, techniqueGap(scope.Matrix, candidates, technique))
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
	scope, ok := h.attackRows(w, r)
	if !ok {
		return
	}

	filter, err := attackFilter(scope.Matrix, r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	matrix := scope.Matrix
	coverage := matrix.Compute(scope.Rows, filter)

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
func (h *Handler) attackRows(w http.ResponseWriter, r *http.Request) (attackScope, bool) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return attackScope{}, false
	}

	matrix, err := attack.Get()
	if err != nil {
		log.Printf("[Attack] Failed to load matrix: %v", err)
		h.respondError(w, http.StatusInternalServerError, "ATT&CK matrix unavailable")
		return attackScope{}, false
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Attack] Failed to get scope: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return attackScope{}, false
	}

	rows, err := h.manager.ListCoverageRows(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Attack] Failed to load coverage rows: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load rules")
		return attackScope{}, false
	}

	return attackScope{Matrix: matrix, Rows: rows, FractalID: fractalID, PrismID: prismID}, true
}

// attackScope is what every coverage endpoint needs: the matrix, the rules in the
// caller's scope, and the scope itself, resolved once.
type attackScope struct {
	Matrix    *attack.Matrix
	Rows      []attack.RuleRow
	FractalID string
	PrismID   string
}

// validSeverities is the enum, not a second list: it previously accepted "info",
// which nothing writes, and rejected "informational", which everything else uses.
var validSeverities = func() map[string]bool {
	set := map[string]bool{}
	for _, s := range Severity("").EnumValues() {
		set[s] = true
	}
	return set
}()

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

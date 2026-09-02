package alerts

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
)

// SavePoliciesRequest replaces a fractal's whole rule set.
type SavePoliciesRequest struct {
	Policies []Policy `json:"policies"`
}

// EvaluatePolicyRequest asks how a definition would fare, without saving it. The editor
// calls this as the author types.
type EvaluatePolicyRequest struct {
	Name                string      `json:"name"`
	Description         string      `json:"description"`
	QueryString         string      `json:"query_string"`
	AlertType           string      `json:"alert_type"`
	Severity            string      `json:"severity"`
	ThrottleTimeSeconds int         `json:"throttle_time_seconds"`
	ThrottleField       string      `json:"throttle_field"`
	Labels              []string    `json:"labels"`
	References          []string    `json:"references"`
	WindowDuration      *int        `json:"window_duration,omitempty"`
	ScheduleCron        *string     `json:"schedule_cron,omitempty"`
	QueryWindowSeconds  *int        `json:"query_window_seconds,omitempty"`
	WebhookActionIDs    []string    `json:"webhook_action_ids"`
	FractalActionIDs    []string    `json:"fractal_action_ids"`
	DictionaryActionIDs []string    `json:"dictionary_action_ids"`
	EmailActionIDs      []string    `json:"email_action_ids"`
	Tests               []AlertTest `json:"tests"`
	// TestsRun and TestsPassing let the editor fold in the test run it already did, so
	// a rule reading test outcomes reports live rather than deferring. The save path
	// re-runs the tests itself and never trusts these.
	TestsRun     bool `json:"tests_run"`
	TestsPassing bool `json:"tests_passing"`
}

func (r EvaluatePolicyRequest) subject() PolicySubject {
	return PolicySubject{
		Content: RevisionContent{
			Name: r.Name, Description: r.Description, QueryString: r.QueryString,
			AlertType: r.AlertType, Severity: r.Severity,
			ThrottleTimeSeconds: r.ThrottleTimeSeconds, ThrottleField: r.ThrottleField,
			Labels: r.Labels, References: r.References,
			WindowDuration: r.WindowDuration, ScheduleCron: r.ScheduleCron,
			QueryWindowSeconds:  r.QueryWindowSeconds,
			WebhookActionIDs:    r.WebhookActionIDs,
			FractalActionIDs:    r.FractalActionIDs,
			DictionaryActionIDs: r.DictionaryActionIDs,
			EmailActionIDs:      r.EmailActionIDs,
		},
		Tests:        r.Tests,
		TestsRun:     r.TestsRun,
		TestsPassing: r.TestsPassing,
	}
}

// ComplianceRow is one alert measured against the current rule set.
type ComplianceRow struct {
	AlertID    string      `json:"alert_id"`
	Name       string      `json:"name"`
	Enabled    bool        `json:"enabled"`
	Blocking   int         `json:"blocking"`
	Warnings   int         `json:"warnings"`
	Violations []Violation `json:"violations"`
}

// policyScopeAccess resolves the scope in play and enforces the role that owns it.
//
// A prism holds its own rule set, so the check has to follow the scope rather than
// always asking about a fractal. Tenant admins pass either way: both resolvers
// short-circuit on the instance-wide grant.
func (h *Handler) policyScopeAccess(w http.ResponseWriter, r *http.Request, required rbac.Role) (string, string, bool) {
	fractalID, prismID, ok := h.requireScope(w, r, "alert policies")
	if !ok {
		return "", "", false
	}

	if prismID != "" {
		if !h.requireRoleOnPrism(w, r, prismID, required) {
			return "", "", false
		}
		return "", prismID, true
	}
	if !h.requireRoleOnFractal(w, r, fractalID, required) {
		return "", "", false
	}
	return fractalID, "", true
}

// HandlePolicyCatalog returns the fields and operators a rule can be built from.
func (h *Handler) HandlePolicyCatalog(w http.ResponseWriter, r *http.Request) {
	h.respondSuccess(w, Catalog())
}

// HandleListPolicies returns the current fractal's rule set.
func (h *Handler) HandleListPolicies(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	policies, err := h.manager.ListPolicies(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list policies: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load policies")
		return
	}
	h.respondSuccess(w, policies)
}

// HandleSavePolicies replaces the rule set. Admin only: a rule set decides what the
// rest of the fractal is allowed to save.
func (h *Handler) HandleSavePolicies(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAdmin)
	if !ok {
		return
	}

	var req SavePoliciesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	policies, err := h.manager.ReplacePolicies(r.Context(), fractalID, prismID, req.Policies, h.attributionUser(r))
	if err != nil {
		if strings.Contains(err.Error(), "rule ") || strings.Contains(err.Error(), "at most") {
			h.respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		log.Printf("[Alerts] Failed to save policies: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to save policies")
		return
	}
	h.respondSuccess(w, policies)
}

// HandleEvaluatePolicies reports how a definition fares without saving it.
func (h *Handler) HandleEvaluatePolicies(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	var req EvaluatePolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	policies, err := h.manager.ListPolicies(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to load policies: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load policies")
		return
	}

	result := EvaluatePolicies(policies, req.subject())
	h.respondSuccess(w, result)
}

// HandlePolicyCompliance measures every alert in the fractal against the rule set.
//
// This is the answer to "what breaks if I turn this on", and it is why blocking needs
// no grandfathering: an existing alert keeps running untouched, and its violations are
// visible here before anyone edits it.
func (h *Handler) HandlePolicyCompliance(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	ctx := r.Context()
	policies, err := h.manager.ListPolicies(ctx, fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to load policies: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load policies")
		return
	}

	alerts, err := h.manager.ListAlerts(ctx, false, fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load alerts")
		return
	}

	rows := []ComplianceRow{}
	for _, alert := range alerts {
		// Feed alerts are exempt from enforcement, so reporting them here would only
		// be noise about rules nobody can act on.
		if alert.FeedID != "" {
			continue
		}

		tests, err := h.manager.ListTests(ctx, alert.ID)
		if err != nil {
			log.Printf("[Alerts] Failed to load tests for %s: %v", alert.ID, err)
			continue
		}

		result := EvaluatePolicies(policies, NewPolicySubject(alertRevisionContent(alert), tests))
		if len(result.Violations) == 0 {
			continue
		}
		rows = append(rows, ComplianceRow{
			AlertID: alert.ID, Name: alert.Name, Enabled: alert.Enabled,
			Blocking: result.Blocking, Warnings: result.Warnings, Violations: result.Violations,
		})
	}

	api.WriteJSON(w, http.StatusOK, api.Response[[]ComplianceRow]{Success: true, Data: rows})
}

// policyBlockedResponse renders a refused save so the client can put each violation
// against the field it concerns.
func (h *Handler) policyBlockedResponse(w http.ResponseWriter, err error) bool {
	var blocked *PolicyBlockedError
	if !errors.As(err, &blocked) {
		return false
	}
	api.WriteJSON(w, http.StatusUnprocessableEntity, api.Response[[]Violation]{
		Success: false,
		Error:   blocked.Error(),
		Code:    api.CodeForStatus(http.StatusUnprocessableEntity),
		Data:    blocked.Blocking(),
	})
	return true
}

// ImportPoliciesRequest carries a policy document.
type ImportPoliciesRequest struct {
	Content string `json:"content"`
	// Replace discards the existing rule set. Without it the imported rules are
	// appended, which is what merging two baselines needs.
	Replace bool `json:"replace"`
}

// HandleImportPolicies loads a rule set from a policy document.
func (h *Handler) HandleImportPolicies(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleAdmin)
	if !ok {
		return
	}

	var req ImportPoliciesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	imported, err := ParsePolicyDocument(req.Content)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	combined := imported
	if !req.Replace {
		existing, err := h.manager.ListPolicies(r.Context(), fractalID, prismID)
		if err != nil {
			log.Printf("[Alerts] Failed to load policies: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load policies")
			return
		}
		combined = append(existing, imported...)
	}

	saved, err := h.manager.ReplacePolicies(r.Context(), fractalID, prismID, combined, h.attributionUser(r))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.respondSuccess(w, saved)
}

// HandleExportPolicies renders the rule set as a policy document.
func (h *Handler) HandleExportPolicies(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, ok := h.policyScopeAccess(w, r, rbac.RoleViewer)
	if !ok {
		return
	}

	policies, err := h.manager.ListPolicies(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to load policies: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load policies")
		return
	}

	name := "Alert policies"
	if fractalID != "" && h.fractalManager != nil {
		if fractal, err := h.fractalManager.GetFractal(r.Context(), fractalID); err == nil && fractal != nil {
			name = fractal.Name + " alert policies"
		}
	}

	content, err := RenderPolicyDocument(name, policies)
	if err != nil {
		log.Printf("[Alerts] Failed to render policies: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to render policies")
		return
	}
	h.respondSuccess(w, map[string]string{"content": content})
}

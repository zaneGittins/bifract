package aitools

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Governance tools: an alert's history, and the proposal workflow a reviewed scope
// requires instead of a direct write.
//
// Approving and merging are deliberately absent. A model that could approve its own
// proposal would make the review gate theatre, so those verbs stay with a person.
func registerGovernanceTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "get_alert_history",
		Annotations: readOnly(),
		Description: "Show how an alert's definition has changed, newest first.\n\n" +
			"Use it before editing a detection someone else wrote: the summaries say what " +
			"moved and who moved it, which is usually why a rule looks the way it does.\n\n" +
			"Returns each revision's author, age, change summary and full definition.",
	}, getAlertHistory)

	add(d, &mcp.Tool{
		Name:        "list_alert_changes",
		Annotations: readOnly(),
		Description: "List proposed alert changes awaiting review in this scope.\n\n" +
			"Check here before proposing: an alert may already have a change open, and a " +
			"second proposal against it will go stale as soon as the first merges.",
	}, listAlertChanges)

	add(d, &mcp.Tool{
		Name:        "propose_alert_change",
		Annotations: mutates(),
		Description: "Propose a change to an alert, for a scope that reviews changes before " +
			"they go live.\n\n" +
			"When create_alert, update_alert or delete_alert is refused because the scope " +
			"is reviewed, this is how the work is submitted instead. The alert keeps " +
			"running its current definition until a person approves and merges.\n\n" +
			"kind create needs the full definition; update needs alert_id and the full " +
			"definition it should become; delete needs alert_id and a summary saying why.\n\n" +
			"Approving and merging are not available to a model. Say that the proposal is " +
			"open and let a reviewer decide.",
	}, proposeAlertChange)
}

type getAlertHistoryArgs struct {
	AlertID string `json:"alert_id" jsonschema:"The alert to show history for."`
}

func getAlertHistory(ctx context.Context, c Client, in getAlertHistoryArgs) (any, error) {
	return c.Get(ctx, fmt.Sprintf("/alerts/%s/revisions", in.AlertID), nil)
}

type listAlertChangesArgs struct {
	OpenOnly bool `json:"open_only,omitempty" jsonschema:"Return only proposals still awaiting review."`
}

func listAlertChanges(ctx context.Context, c Client, in listAlertChangesArgs) (any, error) {
	var query map[string][]string
	if in.OpenOnly {
		query = map[string][]string{"open": {"true"}}
	}
	return c.Get(ctx, "/alert-changes", query)
}

type proposeAlertChangeArgs struct {
	Kind    string `json:"kind" jsonschema:"'create', 'update' or 'delete'."`
	AlertID string `json:"alert_id,omitempty" jsonschema:"The alert being changed. Required for update and delete."`
	Title   string `json:"title,omitempty" jsonschema:"Short name for the proposal."`
	Summary string `json:"summary,omitempty" jsonschema:"What changed and why, for the reviewer. Required for delete."`

	Name        string `json:"name,omitempty" jsonschema:"Alert name. Required for create and update."`
	QueryString string `json:"query_string,omitempty" jsonschema:"The BQL query. Required for create and update."`
	Description string `json:"description,omitempty" jsonschema:"What this alert detects and why it matters."`
	AlertType   string `json:"alert_type,omitempty" jsonschema:"'event', 'scheduled' or 'compound'. Default event."`
	Severity    string `json:"severity,omitempty" jsonschema:"How urgent a firing is. Default medium."`

	Labels              []string `json:"labels,omitempty" jsonschema:"Tags such as ['attack.t1059']."`
	References          []string `json:"references,omitempty" jsonschema:"Reference URLs."`
	ThrottleTimeSeconds int      `json:"throttle_time_seconds,omitempty" jsonschema:"Minimum seconds between repeat firings."`
	ThrottleField       string   `json:"throttle_field,omitempty" jsonschema:"Re-fire only when this field's value changes."`
	ScheduleCron        string   `json:"schedule_cron,omitempty" jsonschema:"Five-field cron. Required for alert_type scheduled."`
	QueryWindowSeconds  int      `json:"query_window_seconds,omitempty" jsonschema:"Scheduled lookback in seconds."`
	WindowDuration      int      `json:"window_duration,omitempty" jsonschema:"Compound correlation window in seconds."`
}

func proposeAlertChange(ctx context.Context, c Client, in proposeAlertChangeArgs) (any, error) {
	kind := strings.TrimSpace(in.Kind)
	body := map[string]any{
		"kind":     kind,
		"alert_id": in.AlertID,
		"title":    in.Title,
		"summary":  in.Summary,
	}

	if kind != "delete" {
		alertType := strings.TrimSpace(in.AlertType)
		if alertType == "" {
			alertType = "event"
		}
		severity := strings.TrimSpace(in.Severity)
		if severity == "" {
			severity = "medium"
		}

		content := map[string]any{
			"name":                  in.Name,
			"query_string":          in.QueryString,
			"description":           in.Description,
			"alert_type":            alertType,
			"severity":              severity,
			"labels":                orEmpty(in.Labels),
			"references":            orEmpty(in.References),
			"throttle_time_seconds": in.ThrottleTimeSeconds,
			"throttle_field":        in.ThrottleField,
			"webhook_action_ids":    []string{},
			"fractal_action_ids":    []string{},
			"dictionary_action_ids": []string{},
			"email_action_ids":      []string{},
		}
		if cron := strings.TrimSpace(in.ScheduleCron); cron != "" {
			content["schedule_cron"] = cron
		}
		if in.QueryWindowSeconds > 0 {
			content["query_window_seconds"] = in.QueryWindowSeconds
		}
		if in.WindowDuration > 0 {
			content["window_duration"] = in.WindowDuration
		}
		body["content"] = content
	}

	return c.Post(ctx, "/alert-changes", body)
}

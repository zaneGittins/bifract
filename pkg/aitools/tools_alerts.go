package aitools

import (
	"context"
	"net/url"
	"strings"

	"bifract/pkg/alerts"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// carried are read back and resent on update. The endpoint replaces the alert
// rather than patching it, filling an absent field with its default: an omitted
// severity became 'medium', an omitted schedule_cron made the alert unsaveable.
var carried = []string{
	"name", "query_string", "description", "alert_type", "severity", "enabled",
	"labels", "references", "throttle_time_seconds", "throttle_field",
	"window_duration", "schedule_cron", "query_window_seconds",
	// Read and written under the same name, unlike the actions below.
	"dictionary_action_ids",
}

// actionFields are read expanded and written as ids.
var actionFields = map[string]string{
	"webhook_actions": "webhook_action_ids",
	"fractal_actions": "fractal_action_ids",
	"email_actions":   "email_action_ids",
}

func registerAlertTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "list_alerts",
		Annotations: readOnly(),
		Description: "List the detection alerts configured in this fractal.\n\n" +
			"Also a good way to learn the fractal's real query patterns and which fields " +
			"existing detections rely on.\n\n" +
			"Returns alerts with their names, BQL queries, type, labels, and status.",
	}, listAlerts)

	add(d, &mcp.Tool{
		Name:        "get_alert",
		Annotations: readOnly(),
		Description: "Get one alert in full: its query, schedule, actions, and execution history.",
	}, getAlert)

	add(d, &mcp.Tool{
		Name:        "create_alert",
		Annotations: mutates(),
		Description: "Create a detection alert.\n\n" +
			"Alerts run on a background interval against newly ingested logs, tracking a " +
			"cursor so nothing is missed across restarts. Validate the query with " +
			"validate_bql first.\n\n" +
			"Each type has a requirement the others do not, and a request that misses it " +
			"is rejected rather than stored:\n" +
			"  event      the query must not aggregate. Use a plain filter.\n" +
			"  scheduled  needs schedule_cron and query_window_seconds.\n" +
			"  compound   needs window_duration.\n\n" +
			"Returns the created alert.",
	}, createAlert,
		enumFor[alerts.AlertType]("alert_type"),
		enumFor[alerts.Severity]("severity"))

	add(d, &mcp.Tool{
		Name:        "update_alert",
		Annotations: mutates(),
		Description: "Update an alert. Only the fields you supply change; the rest keep their values.\n\n" +
			"The API replaces the whole alert, so this reads it first and sends the current " +
			"values back for everything you did not name.\n\n" +
			"Returns the updated alert.",
	}, updateAlert,
		enumFor[alerts.AlertType]("alert_type"),
		enumFor[alerts.Severity]("severity"))

	add(d, &mcp.Tool{
		Name:        "delete_alert",
		Annotations: destroys(),
		Description: "Delete an alert.",
	}, deleteAlert)

	add(d, &mcp.Tool{
		Name:        "get_alert_executions",
		Annotations: readOnly(),
		Description: "Show when an alert fired and what it matched.\n\n" +
			"Use this to judge whether a detection is noisy before tuning it.\n\n" +
			"Returns recent executions with timestamps and match counts.",
	}, getAlertExecutions)
}

type listAlertsArgs struct {
	EnabledOnly bool `json:"enabled_only,omitempty" jsonschema:"Return only alerts that are currently active."`
}

func listAlerts(ctx context.Context, c Client, in listAlertsArgs) (any, error) {
	var query url.Values
	if in.EnabledOnly {
		query = url.Values{"enabled": {"true"}}
	}
	return c.Get(ctx, "/alerts", query)
}

type alertIDArgs struct {
	AlertID string `json:"alert_id" jsonschema:"The alert UUID."`
}

func getAlert(ctx context.Context, c Client, in alertIDArgs) (any, error) {
	return c.Get(ctx, "/alerts/"+url.PathEscape(in.AlertID), nil)
}

func deleteAlert(ctx context.Context, c Client, in alertIDArgs) (any, error) {
	return c.Delete(ctx, "/alerts/"+url.PathEscape(in.AlertID))
}

func getAlertExecutions(ctx context.Context, c Client, in alertIDArgs) (any, error) {
	return c.Get(ctx, "/alerts/"+url.PathEscape(in.AlertID)+"/executions", nil)
}

type createAlertArgs struct {
	Name        string `json:"name" jsonschema:"Alert name, for example 'Brute Force Detection'."`
	QueryString string `json:"query_string" jsonschema:"The BQL query that triggers the alert."`
	Description string `json:"description,omitempty" jsonschema:"What this alert detects and why it matters."`
	AlertType   string `json:"alert_type,omitempty" jsonschema:"'event' evaluates each newly ingested log as it arrives (the usual choice); 'scheduled' re-runs the query on a cron; 'compound' correlates several conditions over a window. Default event."`
	Severity    string `json:"severity,omitempty" jsonschema:"How urgent a firing is. Default medium."`
	// A pointer so an omitted argument keeps the server's default of enabled,
	// which a bare bool cannot express: its zero value is 'disabled'.
	Enabled             *bool    `json:"enabled,omitempty" jsonschema:"Whether the alert starts active. Default true."`
	Labels              []string `json:"labels,omitempty" jsonschema:"Tags such as ['T1110', 'brute-force']."`
	References          []string `json:"references,omitempty" jsonschema:"Reference URLs."`
	ThrottleTimeSeconds int      `json:"throttle_time_seconds,omitempty" jsonschema:"Minimum seconds between repeat firings. 0 means none."`
	ThrottleField       string   `json:"throttle_field,omitempty" jsonschema:"Re-fire only when this field's value changes, for example 'src_ip'."`
	ScheduleCron        string   `json:"schedule_cron,omitempty" jsonschema:"Five-field cron for a scheduled alert, for example '*/15 * * * *'. Required for alert_type scheduled."`
	QueryWindowSeconds  int      `json:"query_window_seconds,omitempty" jsonschema:"How far back a scheduled run looks, in seconds. Required for alert_type scheduled."`
	WindowDuration      int      `json:"window_duration,omitempty" jsonschema:"Correlation window for a compound alert, in seconds. Required for alert_type compound."`
}

func createAlert(ctx context.Context, c Client, in createAlertArgs) (any, error) {
	alertType := strings.TrimSpace(in.AlertType)
	if alertType == "" {
		alertType = string(alerts.AlertTypeEvent)
	}
	severity := strings.TrimSpace(in.Severity)
	if severity == "" {
		severity = string(alerts.SeverityMedium)
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}

	body := map[string]any{
		"name":                  in.Name,
		"query_string":          in.QueryString,
		"description":           in.Description,
		"alert_type":            alertType,
		"severity":              severity,
		"enabled":               enabled,
		"labels":                orEmpty(in.Labels),
		"references":            orEmpty(in.References),
		"throttle_time_seconds": in.ThrottleTimeSeconds,
		"throttle_field":        in.ThrottleField,
	}
	// Sent only when set: the API reads their absence as "not applicable to this
	// type", and a zero would fail the positive-value check instead.
	if cron := strings.TrimSpace(in.ScheduleCron); cron != "" {
		body["schedule_cron"] = cron
	}
	if in.QueryWindowSeconds > 0 {
		body["query_window_seconds"] = in.QueryWindowSeconds
	}
	if in.WindowDuration > 0 {
		body["window_duration"] = in.WindowDuration
	}
	return c.Post(ctx, "/alerts", body)
}

type updateAlertArgs struct {
	AlertID             string   `json:"alert_id" jsonschema:"The alert UUID."`
	Name                string   `json:"name,omitempty" jsonschema:"New name. Omit to keep the current one."`
	QueryString         string   `json:"query_string,omitempty" jsonschema:"New BQL query. Omit to keep the current one."`
	Description         string   `json:"description,omitempty" jsonschema:"New description. Omit to keep the current one."`
	AlertType           string   `json:"alert_type,omitempty" jsonschema:"New evaluation type. Omit to keep the current one."`
	Severity            string   `json:"severity,omitempty" jsonschema:"New severity. Omit to keep the current one."`
	Enabled             *bool    `json:"enabled,omitempty" jsonschema:"New enabled state. Omit to keep the current one."`
	Labels              []string `json:"labels,omitempty" jsonschema:"New label list, replacing the current one. Omit to keep it."`
	ThrottleTimeSeconds *int     `json:"throttle_time_seconds,omitempty" jsonschema:"New throttle window in seconds. Omit to keep the current one."`
	ThrottleField       *string  `json:"throttle_field,omitempty" jsonschema:"New throttle field. Omit to keep the current one."`
}

func updateAlert(ctx context.Context, c Client, in updateAlertArgs) (any, error) {
	path := "/alerts/" + url.PathEscape(in.AlertID)
	current, err := c.Get(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	alert := current
	if nested := Field[map[string]any](current, "alert"); nested != nil {
		alert = nested
	}

	body := carryForward(alert)
	for key, value := range map[string]any{
		"name":         nonEmpty(in.Name),
		"query_string": nonEmpty(in.QueryString),
		"description":  nonEmpty(in.Description),
		"alert_type":   nonEmpty(in.AlertType),
		"severity":     nonEmpty(in.Severity),
	} {
		if value != nil {
			body[key] = value
		}
	}
	if in.Enabled != nil {
		body["enabled"] = *in.Enabled
	}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	if in.ThrottleTimeSeconds != nil {
		body["throttle_time_seconds"] = *in.ThrottleTimeSeconds
	}
	if in.ThrottleField != nil {
		body["throttle_field"] = *in.ThrottleField
	}
	return c.Put(ctx, path, body)
}

// carryForward is the alert's current state, in the shape the update endpoint
// accepts.
func carryForward(alert any) map[string]any {
	object, ok := alert.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	body := make(map[string]any, len(carried)+len(actionFields))
	for _, name := range carried {
		if value, present := object[name]; present && value != nil {
			body[name] = value
		}
	}
	for readAs, writeAs := range actionFields {
		actions, _ := object[readAs].([]any)
		ids := make([]string, 0, len(actions))
		for _, action := range actions {
			if id := Field[string](action, "id"); id != "" {
				ids = append(ids, id)
			}
		}
		body[writeAs] = ids
	}
	return body
}

// nonEmpty reports a caller-supplied string as a change, or nil when it was left
// blank and the current value should stand.
func nonEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// orEmpty keeps a nil slice out of the request body, where it would encode as
// null rather than an empty list.
func orEmpty(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

package aitools

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/alerts"
	"bifract/pkg/api"

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

// AlertTestArg is one test case: a set of events and what the rule should say about
// them. Events are normalized field objects, the shape a query runs against.
type AlertTestArg struct {
	Name        string           `json:"name" jsonschema:"What this case proves, for example 'benign certutil must not fire'."`
	Expectation string           `json:"expectation" jsonschema:"'match' if the rule should fire on these events, 'no_match' if it must not."`
	Events      []map[string]any `json:"events" jsonschema:"The events, each a flat object of field names to values. Several events in one test are one scenario: a compound or chain rule only fires when they correlate."`
}

// testsBody converts test arguments into the API's shape. A nil slice means the
// caller said nothing about tests, which must not be read as "remove them all".
func testsBody(tests []AlertTestArg) []map[string]any {
	if tests == nil {
		return nil
	}
	out := make([]map[string]any, 0, len(tests))
	for i, t := range tests {
		events := t.Events
		if events == nil {
			events = []map[string]any{}
		}
		expectation := strings.TrimSpace(t.Expectation)
		if expectation == "" {
			expectation = "match"
		}
		out = append(out, map[string]any{
			"name":        t.Name,
			"expectation": expectation,
			"events":      events,
			"position":    i,
		})
	}
	return out
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
			"A fractal may enforce policy rules on a definition, and a save that breaks a " +
			"blocking rule is refused with the rule's message: fix what it names and retry.\n\n" +
			"A fractal may also review changes before they go live. That refusal says to " +
			"open a proposal instead, which is propose_alert_change.\n\n" +
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
			"Refused if the fractal has a blocking policy rule the result would break, or " +
			"if the fractal reviews changes, in which case use propose_alert_change.\n\n" +
			"Returns the updated alert.",
	}, updateAlert,
		enumFor[alerts.AlertType]("alert_type"),
		enumFor[alerts.Severity]("severity"))

	add(d, &mcp.Tool{
		Name:        "delete_alert",
		Annotations: destroys(),
		Description: "Delete an alert.\n\n" +
			"Refused where the fractal reviews changes: propose_alert_change with kind " +
			"delete instead, saying why it should go.",
	}, deleteAlert)

	add(d, &mcp.Tool{
		Name:        "get_alert_tests",
		Annotations: readOnly(),
		Description: "Show the test cases stored with an alert.\n\n" +
			"Read these before editing a detection: they state what the rule is meant to " +
			"catch and what it must ignore, and an edit that breaks one is a regression.",
	}, getAlertTests)

	add(d, &mcp.Tool{
		Name:        "run_alert_tests",
		Annotations: readOnly(),
		Description: "Run test cases against a query without saving anything.\n\n" +
			"The events are written to a private scratch table, the query runs against " +
			"just them, and the table is dropped. Nothing touches real logs or the alert.\n\n" +
			"Use it before create_alert or propose_alert_change: a scope may require " +
			"passing tests, and this is how to know they pass first.\n\n" +
			"Returns each case with passed, matched and the row count.",
		// Read-only in effect, but the endpoint needs analyst: it writes the events
		// to a scratch table before dropping it.
	}, runAlertTests, needsAccess(api.AccessAnalyst))

	add(d, &mcp.Tool{
		Name:        "get_alert_policies",
		Annotations: readOnly(),
		Description: "List the policy rules this scope enforces on an alert definition.\n\n" +
			"Read them before writing a detection. A rule with severity 'block' refuses " +
			"the save outright; 'warn' is advice the author is expected to answer. Each " +
			"rule names a field, a condition and the message an author sees.\n\n" +
			"An empty list means the scope checks nothing.",
	}, getAlertPolicies)

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

	Tests []AlertTestArg `json:"tests,omitempty" jsonschema:"Test cases stored with the alert, each naming events the rule should or should not fire on. A fractal may require them, and they are what proves a rule still works after an edit."`
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
	if tests := testsBody(in.Tests); tests != nil {
		body["tests"] = tests
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

	Tests []AlertTestArg `json:"tests,omitempty" jsonschema:"Replace the alert's test cases with these. Omit to keep the existing ones; send an empty list to remove them."`
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
	if tests := testsBody(in.Tests); tests != nil {
		body["tests"] = tests
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

type getAlertTestsArgs struct {
	AlertID string `json:"alert_id" jsonschema:"The alert UUID."`
}

func getAlertTests(ctx context.Context, c Client, in getAlertTestsArgs) (any, error) {
	return c.Get(ctx, "/alerts/"+url.PathEscape(in.AlertID)+"/tests", nil)
}

type runAlertTestsArgs struct {
	QueryString string         `json:"query_string" jsonschema:"The BQL query to evaluate."`
	Tests       []AlertTestArg `json:"tests" jsonschema:"The cases to run."`
}

func runAlertTests(ctx context.Context, c Client, in runAlertTestsArgs) (any, error) {
	// A fresh session per call: the scratch table is torn down with it, so a run
	// never inherits events from an earlier one.
	session := "mcp-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	result, err := c.Post(ctx, "/alerts/tests/run", map[string]any{
		"session_id":   session,
		"query_string": in.QueryString,
		"tests":        testsBody(in.Tests),
	})
	// Released whatever the outcome: a failed run still left a table behind.
	_, _ = c.Delete(ctx, "/alerts/tests/session/"+url.PathEscape(session))
	return result, err
}

type getAlertPoliciesArgs struct{}

func getAlertPolicies(ctx context.Context, c Client, in getAlertPoliciesArgs) (any, error) {
	return c.Get(ctx, "/alert-policies", nil)
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

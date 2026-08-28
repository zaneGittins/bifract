package mcpserver

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// window is embedded by every tool that scans logs, so the time range is
// described once and reads identically wherever it appears.
type window struct {
	Start string `json:"start,omitempty" jsonschema:"Start of the range, RFC3339 (2025-01-01T00:00:00Z). Defaults to 24 hours ago."`
	End   string `json:"end,omitempty" jsonschema:"End of the range, RFC3339. Defaults to now."`
}

// body returns the request fields for the range, omitting blanks so the server
// applies its own default rather than being handed an empty string.
func (w window) body() map[string]any {
	out := map[string]any{}
	if start := strings.TrimSpace(w.Start); start != "" {
		out["start"] = start
	}
	if end := strings.TrimSpace(w.End); end != "" {
		out["end"] = end
	}
	return out
}

func registerQueryTools(s *mcp.Server, c *Client) {
	register(s, c, &mcp.Tool{
		Name: "get_context",
		Description: "Report which Bifract instance and fractal this session is bound to.\n\n" +
			"Call this first in a new conversation. The API key fixes the fractal, so nothing " +
			"here is selectable; it tells you the scope your queries will run in and the role " +
			"that governs what you may write.\n\n" +
			"Returns the instance URL, server version, fractal id, and effective role.",
	}, getContext)

	register(s, c, &mcp.Tool{
		Name: "query_logs",
		Description: "Execute a BQL query against the fractal's logs.\n\n" +
			"The fractal comes from the API key. Prefer a bounded time range: the backing " +
			"store holds up to billions of rows and an unbounded scan is expensive.\n\n" +
			"Returns matching rows or aggregation results, with the field order and row count.",
	}, queryLogs)

	register(s, c, &mcp.Tool{
		Name: "validate_bql",
		Description: "Check a BQL query for syntax and translation errors without running it.\n\n" +
			"Costs no database work. Use it on a long pipeline before query_logs so a typo " +
			"does not cost a full query round trip.\n\n" +
			"Returns whether the query is valid, and the parse or translation error if not.",
	}, validateBQL)

	register(s, c, &mcp.Tool{
		Name: "get_fields",
		Description: "List the field names available to queries in this fractal.\n\n" +
			"These are the normalized fields BQL refers to by bare name (host=web-01, not " +
			"fields.host). For what the values actually look like, follow with get_field_stats.\n\n" +
			"Returns the field names, filtered if a filter was given.",
	}, getFields)

	register(s, c, &mcp.Tool{
		Name: "get_field_stats",
		Description: "Profile the fields present in the events a query matches.\n\n" +
			"For each field: how many sampled rows carry it, how many distinct values it has, " +
			"and its most frequent values. Use this to learn a field's real value shape before " +
			"writing an equality filter, and to spot which fields are worth grouping on.\n\n" +
			"Runs over a bounded sample at low database priority, so it does not slow searches. " +
			"Not supported for source-command queries such as pgr().\n\n" +
			"Returns per-field coverage, cardinality, and top values over the sample.",
	}, getFieldStats)

	register(s, c, &mcp.Tool{
		Name: "get_recent_logs",
		Description: "Fetch the most recent logs in the fractal.\n\n" +
			"Useful for seeing the real shape of ingested events, and for checking how fresh " +
			"the data is before choosing a time range.\n\n" +
			"Returns recent log entries with all their fields.",
	}, getRecentLogs)

	register(s, c, &mcp.Tool{
		Name:        "get_bql_reference",
		Description: "Get the BQL syntax reference: every supported function and operator with examples.",
	}, getBQLReference)
}

func getContext(ctx context.Context, c *Client, _ noArgs) (any, error) {
	identity, err := c.Get(ctx, "/auth/user", nil)
	if err != nil {
		return nil, err
	}
	user := identity
	if nested := field[map[string]any](identity, "user"); nested != nil {
		user = nested
	}
	version, err := c.Get(ctx, "/version", nil)
	if err != nil {
		return nil, err
	}

	role := field[string](user, "fractal_role")
	if role == "" {
		role = field[string](user, "prism_role")
	}
	if role == "" {
		role = "none"
	}
	name := field[string](user, "display_name")
	if name == "" {
		name = field[string](user, "username")
	}

	fractal, prism := field[string](user, "selected_fractal"), field[string](user, "selected_prism")

	// The server ignores the scope header for a key that already has a scope, so
	// the configured one only fills the gap an instance-wide key leaves.
	scope, conflict := c.Config().Scope, ""
	switch {
	case fractal != "" || prism != "":
		if configured := c.Config().FractalScope(); configured != "" && configured != fractal {
			conflict = "BIFRACT_FRACTAL_ID names " + configured +
				", but this key is issued for the scope above and the server ignores the setting. Unset it."
		}
	case scope != "":
		if id := c.Config().FractalScope(); id != "" {
			fractal = id
		} else if id, ok := strings.CutPrefix(scope, "prism:"); ok {
			prism = id
		}
	}

	reported := map[string]any{
		"url":            c.Config().URL,
		"server_version": orUnknown(field[string](version, "version")),
		"identity":       name,
		"fractal_id":     fractal,
		"prism_id":       prism,
		"role":           role,
		"note": "Queries default to the last 24 hours unless start/end are given. " +
			"'viewer' can read only; 'analyst' can also write comments, alerts, " +
			"notebooks, and dashboards.",
	}
	if conflict != "" {
		reported["scope_warning"] = conflict
	} else if scope != "" {
		reported["scope_source"] = "BIFRACT_FRACTAL_ID/BIFRACT_PRISM_ID: this key is instance-wide and belongs to no scope of its own"
	}
	return reported, nil
}

type queryLogsArgs struct {
	Query string `json:"query" jsonschema:"BQL query, starting with a filter expression. Examples: 'level=error | head(10)', 'image=~powershell | groupby(computer_name)', 'bifract_category=\"process_creation\" | count()'."`
	window
}

func queryLogs(ctx context.Context, c *Client, in queryLogsArgs) (any, error) {
	body := in.window.body()
	body["query"] = in.Query

	result, err := c.Post(ctx, "/query", body)
	if err != nil {
		return nil, err
	}

	summary := fmt.Sprintf("Found %d results in %dms",
		int(field[float64](result, "count")), int(field[float64](result, "execution_ms")))
	if field[bool](result, "is_aggregated") {
		summary += " (aggregated)"
	}
	if hit := int(field[float64](result, "limit_hit")); hit > 0 {
		summary += fmt.Sprintf(" [limit: %d]", hit)
	}

	return map[string]any{
		"summary":     summary,
		"field_order": field[[]any](result, "field_order"),
		"results":     field[[]any](result, "results"),
	}, nil
}

type validateBQLArgs struct {
	Query string `json:"query" jsonschema:"The BQL query string to check."`
}

func validateBQL(ctx context.Context, c *Client, in validateBQLArgs) (any, error) {
	return c.Post(ctx, "/query/validate", map[string]any{"query": in.Query})
}

type getFieldsArgs struct {
	Filter string `json:"filter,omitempty" jsonschema:"Case-insensitive substring to narrow the list (for example 'ip' or 'process')."`
}

func getFields(ctx context.Context, c *Client, in getFieldsArgs) (any, error) {
	response, err := c.Get(ctx, "/query/fields", nil)
	if err != nil {
		return nil, err
	}
	fields := field[[]any](response, "fields")
	if needle := strings.ToLower(strings.TrimSpace(in.Filter)); needle != "" {
		matched := make([]any, 0, len(fields))
		for _, name := range fields {
			if text, ok := name.(string); ok && strings.Contains(strings.ToLower(text), needle) {
				matched = append(matched, name)
			}
		}
		fields = matched
	}
	return map[string]any{"count": len(fields), "fields": fields}, nil
}

type getFieldStatsArgs struct {
	Query string `json:"query" jsonschema:"The BQL filter to profile. Only the filter portion is used."`
	window
}

func getFieldStats(ctx context.Context, c *Client, in getFieldStatsArgs) (any, error) {
	body := in.window.body()
	body["query"] = in.Query

	result, err := c.Post(ctx, "/query/fieldstats", body)
	if err != nil {
		return nil, err
	}
	if object, ok := result.(map[string]any); ok {
		if supported, present := object["supported"].(bool); present && !supported {
			return "Field stats are not available for this query shape (source commands like pgr()).", nil
		}
	}
	return map[string]any{
		"sample_size": int(field[float64](result, "sample_size")),
		"approximate": field[bool](result, "approximate"),
		"fields":      field[[]any](result, "fields"),
	}, nil
}

type getRecentLogsArgs struct {
	Count int `json:"count,omitempty" jsonschema:"How many logs to return, 1 to 100. Default 10."`
}

func getRecentLogs(ctx context.Context, c *Client, in getRecentLogsArgs) (any, error) {
	count := clamp(in.Count, 10, 1, 100)
	return c.Get(ctx, "/logs/recent", url.Values{"count": {strconv.Itoa(count)}})
}

func getBQLReference(ctx context.Context, c *Client, _ noArgs) (any, error) {
	return c.Get(ctx, "/query/reference", nil)
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

package mcpserver

import (
	"context"
	"net/url"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Read-only views of what a credential can reach: the fractals themselves, the
// queries analysts have saved, and the dashboards built on them.
func registerCatalogTools(s *mcp.Server, c *Client) {
	register(s, c, &mcp.Tool{
		Name: "list_fractals",
		Description: "List the fractals and prisms this credential can reach.\n\n" +
			"A fractal is an isolated container of logs, alerts, comments and dictionaries; " +
			"teams, environments and customers are usually kept in separate ones. A prism is " +
			"a read-only view spanning several fractals, used to query across them at once.\n\n" +
			"Most tools act on the single fractal the API key is bound to, which get_context " +
			"reports. Use this to understand what else exists on the instance, to interpret a " +
			"fractal id seen elsewhere, or to confirm the key's scope really is the data you " +
			"were asked about.\n\n" +
			"Returns the fractals with their ids, names and row counts, and the prisms with " +
			"the fractals each spans.",
	}, listFractals)

	register(s, c, &mcp.Tool{
		Name: "list_saved_queries",
		Description: "List the BQL queries users have saved in this fractal.\n\n" +
			"The best available record of how this environment is actually searched: field " +
			"names in real use, and the pipelines analysts trust.\n\n" +
			"Returns saved queries with their names, BQL, descriptions, and tags.",
	}, listSavedQueries)

	register(s, c, &mcp.Tool{
		Name: "list_dashboards",
		Description: "List the dashboards in this fractal.\n\n" +
			"Returns a summary per dashboard; use get_dashboard for a specific one's widgets " +
			"and their queries.",
	}, listDashboards)

	register(s, c, &mcp.Tool{
		Name: "get_dashboard",
		Description: "Read a dashboard with all of its widgets.\n\n" +
			"Each widget carries the BQL query behind it, which makes a dashboard a useful " +
			"source of vetted queries for this fractal.\n\n" +
			"Returns the dashboard, its variables, and every widget with its query and chart config.",
	}, getDashboard)
}

func listFractals(ctx context.Context, c *Client, _ noArgs) (any, error) {
	payload, err := c.Get(ctx, "/fractals", nil)
	if err != nil {
		return nil, err
	}
	object, ok := payload.(map[string]any)
	if !ok {
		return payload, nil
	}
	fractals := summarize(object["fractals"], "id", "name", "description", "log_count", "created_at")
	return map[string]any{
		"fractals": fractals,
		"prisms":   summarize(object["prisms"], "id", "name", "description", "fractal_ids"),
		"count":    len(fractals),
		"note": "Reachability is not the same as scope: tools run against the fractal " +
			"the API key is bound to, which get_context reports.",
	}, nil
}

func listSavedQueries(ctx context.Context, c *Client, _ noArgs) (any, error) {
	return c.Get(ctx, "/saved-queries", nil)
}

func listDashboards(ctx context.Context, c *Client, _ noArgs) (any, error) {
	payload, err := c.Get(ctx, "/dashboards", nil)
	if err != nil {
		return nil, err
	}
	if _, ok := payload.([]any); !ok {
		return payload, nil
	}
	summaries := summarize(payload,
		"id", "name", "description", "time_range_type", "refresh_interval", "created_by", "updated_at")
	return map[string]any{"count": len(summaries), "dashboards": summaries}, nil
}

type getDashboardArgs struct {
	DashboardID string `json:"dashboard_id" jsonschema:"The dashboard UUID."`
}

func getDashboard(ctx context.Context, c *Client, in getDashboardArgs) (any, error) {
	return c.Get(ctx, "/dashboards/"+url.PathEscape(in.DashboardID), nil)
}

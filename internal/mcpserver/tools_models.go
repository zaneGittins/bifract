package mcpserver

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerModelTools(s *mcp.Server, c *Client) {
	register(s, c, &mcp.Tool{
		Name: "list_models",
		Description: "List the behavioral models defined in this fractal.\n\n" +
			"Models are baselines the platform maintains continuously (beaconing intervals, " +
			"long-lived connections, first-seen values, volume norms) rather than " +
			"point-in-time queries. They answer \"is this normal for this environment\" in a " +
			"way a single query cannot.\n\n" +
			"Returns each model's id, name, type, status, and the BQL source query that feeds it.",
	}, listModels)

	register(s, c, &mcp.Tool{
		Name:        "get_model",
		Description: "Get one model's full definition, status, and backfill state.",
	}, getModel)

	register(s, c, &mcp.Tool{
		Name: "get_model_data",
		Description: "Read the rows a model has accumulated.\n\n" +
			"This is the baseline itself: what the model has observed and how often. Use it " +
			"to check whether an artifact from an investigation is normal for this " +
			"environment or genuinely new.\n\n" +
			"Returns the model's rows and the total row count.",
	}, getModelData)
}

func listModels(ctx context.Context, c *Client, _ noArgs) (any, error) {
	response, err := c.Get(ctx, "/models", nil)
	if err != nil {
		return nil, err
	}
	summaries := summarize(field[[]any](response, "models"),
		"id", "name", "description", "model_type", "status", "alert_mode",
		"source_query", "backfill_status", "error_message")
	if len(summaries) == 0 {
		return map[string]any{
			"count": 0,
			"hint":  "No models are defined in this fractal. They are created in the UI under Models.",
		}, nil
	}
	return map[string]any{"count": len(summaries), "models": summaries}, nil
}

type modelIDArgs struct {
	ModelID string `json:"model_id" jsonschema:"The model UUID."`
}

func getModel(ctx context.Context, c *Client, in modelIDArgs) (any, error) {
	return c.Get(ctx, "/models/"+url.PathEscape(in.ModelID), nil)
}

type getModelDataArgs struct {
	ModelID string `json:"model_id" jsonschema:"The model UUID."`
	Search  string `json:"search,omitempty" jsonschema:"Substring filter over the rows."`
	Sort    string `json:"sort,omitempty" jsonschema:"Column to sort by. Model-specific; see get_model."`
	Order   string `json:"order,omitempty" jsonschema:"'asc' or 'desc'."`
	Limit   int    `json:"limit,omitempty" jsonschema:"Max rows, 1 to 500. Default 50."`
	Offset  int    `json:"offset,omitempty" jsonschema:"Pagination offset."`
}

func getModelData(ctx context.Context, c *Client, in getModelDataArgs) (any, error) {
	query := url.Values{
		"limit":  {strconv.Itoa(clamp(in.Limit, 50, 1, 500))},
		"offset": {strconv.Itoa(max(0, in.Offset))},
	}
	for name, value := range map[string]string{"search": in.Search, "sort": in.Sort, "order": in.Order} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			query.Set(name, trimmed)
		}
	}
	return c.Get(ctx, "/models/"+url.PathEscape(in.ModelID)+"/data", query)
}

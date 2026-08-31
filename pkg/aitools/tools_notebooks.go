package aitools

import (
	"context"
	"net/url"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerNotebookTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "list_notebooks",
		Annotations: readOnly(),
		Description: "List the notebooks in this fractal, with their metadata.",
	}, listNotebooks)

	add(d, &mcp.Tool{
		Name:        "get_notebook",
		Annotations: readOnly(),
		Description: "Read a notebook and all of its sections, in order.",
	}, getNotebook)

	add(d, &mcp.Tool{
		Name:        "create_notebook",
		Annotations: mutates(),
		Description: "Create a notebook to collect an investigation in.\n\n" +
			"A notebook interleaves narrative, live BQL queries and the events they turned up, " +
			"so an investigation stays reproducible: a reader can re-run each step rather than " +
			"trust a pasted result.\n\n" +
			"Make one at the start of a hunt, then pass its id to add_comment as you go.\n\n" +
			"Returns the created notebook, including its id.",
	}, createNotebook)

	add(d, &mcp.Tool{
		Name:        "add_notebook_section",
		Annotations: mutates(),
		Description: "Append narrative or a runnable query step to a notebook.\n\n" +
			"Evidence does not go here: an event is filed with add_comment and a notebook_id, " +
			"which writes the comment and the notebook entry as one record.\n\n" +
			"Returns the created section, including its id.",
	}, addNotebookSection)
}

type listNotebooksArgs struct {
	Limit  int `json:"limit,omitempty" jsonschema:"Max notebooks to return, 1 to 100. Default 20."`
	Offset int `json:"offset,omitempty" jsonschema:"Pagination offset."`
}

func listNotebooks(ctx context.Context, c Client, in listNotebooksArgs) (any, error) {
	return c.Get(ctx, "/notebooks", url.Values{
		"limit":  {strconv.Itoa(clamp(in.Limit, 20, 1, 100))},
		"offset": {strconv.Itoa(max(0, in.Offset))},
	})
}

type notebookIDArgs struct {
	NotebookID string `json:"notebook_id" jsonschema:"The notebook UUID."`
}

func getNotebook(ctx context.Context, c Client, in notebookIDArgs) (any, error) {
	return c.Get(ctx, "/notebooks/"+url.PathEscape(in.NotebookID), nil)
}

type createNotebookArgs struct {
	Name                 string `json:"name" jsonschema:"Notebook name, for example 'Incident 2025-03-22 - Suspicious PowerShell'."`
	Description          string `json:"description,omitempty" jsonschema:"What this notebook investigates."`
	TimeRangeType        string `json:"time_range_type,omitempty" jsonschema:"Range applied to the notebook's queries: 1h, 24h, 7d, 30d, all or custom. Default 24h."`
	MaxResultsPerSection int    `json:"max_results_per_section,omitempty" jsonschema:"Row cap per query section. Default 1000."`
}

func createNotebook(ctx context.Context, c Client, in createNotebookArgs) (any, error) {
	timeRange := in.TimeRangeType
	if timeRange == "" {
		timeRange = "24h"
	}
	maxResults := in.MaxResultsPerSection
	if maxResults == 0 {
		maxResults = 1000
	}
	return c.Post(ctx, "/notebooks", map[string]any{
		"name":                    in.Name,
		"description":             in.Description,
		"time_range_type":         timeRange,
		"max_results_per_section": maxResults,
	})
}

type addNotebookSectionArgs struct {
	NotebookID  string   `json:"notebook_id" jsonschema:"The notebook UUID."`
	SectionType string   `json:"section_type" jsonschema:"'markdown' for narrative, or 'query' for a runnable BQL step. Evidence is filed with add_comment instead."`
	Content     string   `json:"content" jsonschema:"The markdown text, or the BQL query for a query section."`
	Title       string   `json:"title,omitempty" jsonschema:"Optional section heading."`
	OrderIndex  *int     `json:"order_index,omitempty" jsonschema:"Position, 0-based. Omit to append at the end."`
	Tags        []string `json:"tags,omitempty" jsonschema:"Tags for the section, for example ['timeline', 'lateral-movement']."`
}

func addNotebookSection(ctx context.Context, c Client, in addNotebookSectionArgs) (any, error) {
	path := "/notebooks/" + url.PathEscape(in.NotebookID)

	// Appending needs the current length: the API places a section at an explicit
	// index, so without this a second section overwrites the first's position.
	order := 0
	if in.OrderIndex != nil && *in.OrderIndex >= 0 {
		order = *in.OrderIndex
	} else {
		current, err := c.Get(ctx, path, nil)
		if err != nil {
			return nil, err
		}
		notebook := current
		if nested := Field[map[string]any](current, "notebook"); nested != nil {
			notebook = nested
		}
		order = len(Field[[]any](notebook, "sections"))
	}

	body := map[string]any{
		"section_type": in.SectionType,
		"content":      in.Content,
		"order_index":  order,
	}
	if in.Title != "" {
		body["title"] = in.Title
	}
	if in.Tags != nil {
		body["tags"] = in.Tags
	}
	return c.Post(ctx, path+"/sections", body)
}

package aitools

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"bifract/pkg/api"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// maxRecallRows caps an archive read. Recall finds evidence; it is not a bulk
// export, and a wider answer comes from a narrower query.
const maxRecallRows = 250

// terminal are the job states that will not change again.
var terminal = map[string]bool{"succeeded": true, "failed": true, "canceled": true}

func registerRecallTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "search_archive",
		Annotations: mutates(),
		Description: "Search archived logs, for hunts reaching past the fractal's hot retention.\n\n" +
			"query_logs reads hot storage only; this runs the same BQL against object " +
			"storage, which takes minutes rather than seconds. Use it only when the range " +
			"query_logs covers cannot answer the question.\n\n" +
			"Returns a job id to poll with get_archive_search, and whether an identical " +
			"recent search was reused.",
	}, searchArchive)

	add(d, &mcp.Tool{
		Name:        "get_archive_search",
		Annotations: readOnly(),
		Description: "Read a Recall job: its status, and its rows once it has finished.\n\n" +
			"Status is one of pending, running, succeeded, failed or canceled. While it is " +
			"pending or running there are no rows yet; wait before polling again rather than " +
			"calling in a tight loop, as each call costs a round trip and the scan itself is " +
			"what takes the time.",
	}, getArchiveSearch, needsAccess(api.AccessAnalyst))

	add(d, &mcp.Tool{
		Name:        "cancel_archive_search",
		Annotations: mutates(),
		Description: "Cancel a Recall job that is still pending or running.\n\n" +
			"Worth doing when a search turns out to be too broad: it stops the scan rather " +
			"than leaving it consuming read capacity. A job that has already finished cannot " +
			"be canceled.",
	}, cancelArchiveSearch, noConfirm())
}

type searchArchiveArgs struct {
	Query string `json:"query" jsonschema:"BQL, the same syntax query_logs takes. Some pipeline commands are not available over the archive; a rejected query says which."`
	Start string `json:"start" jsonschema:"Start of the window, RFC3339. Required, and worth keeping tight: the archive has no safe default and a wide window scans everything."`
	End   string `json:"end" jsonschema:"End of the window, RFC3339. Must be after start."`
	// Named max_rows to match the API rather than 'limit', so the two describe the
	// same thing under the same name.
	MaxRows int `json:"max_rows,omitempty" jsonschema:"Rows to return, capped at 250. Default 250. Narrow the query rather than raising this."`
}

func searchArchive(ctx context.Context, c Client, in searchArchiveArgs) (any, error) {
	if strings.TrimSpace(in.Query) == "" {
		return nil, errors.New("a query is required")
	}
	start, end := strings.TrimSpace(in.Start), strings.TrimSpace(in.End)
	if start == "" || end == "" {
		return nil, errors.New("search_archive needs both start and end, as RFC3339 times")
	}

	fractal, err := c.FractalID(ctx)
	if err != nil {
		return nil, err
	}
	result, err := c.Post(ctx, "/recall/"+url.PathEscape(fractal), map[string]any{
		"query":    in.Query,
		"from":     start,
		"to":       end,
		"max_rows": clamp(in.MaxRows, maxRecallRows, 1, maxRecallRows),
	})
	if err != nil {
		return nil, err
	}
	if job, ok := result.(map[string]any); ok && job["id"] != nil {
		job["note"] = "Poll get_archive_search with this id. Archive scans can take minutes."
	}
	return result, nil
}

type recallJobArgs struct {
	JobID string `json:"job_id" jsonschema:"The job id returned by search_archive."`
}

func getArchiveSearch(ctx context.Context, c Client, in recallJobArgs) (any, error) {
	fractal, err := c.FractalID(ctx)
	if err != nil {
		return nil, err
	}
	job, err := c.Get(ctx, "/recall/"+url.PathEscape(fractal)+"/"+url.PathEscape(in.JobID), nil)
	if err != nil {
		return nil, err
	}
	// A running job carries no rows. Returning it as-is reads as "no matches",
	// which is a different and wrong answer.
	status := strings.ToLower(Field[string](job, "status"))
	if status != "" && !terminal[status] {
		id := Field[string](job, "id")
		if id == "" {
			id = in.JobID
		}
		return map[string]any{
			"id":     id,
			"status": status,
			"note":   "Still scanning. Poll again in a few seconds; results appear when it succeeds.",
		}, nil
	}
	return job, nil
}

func cancelArchiveSearch(ctx context.Context, c Client, in recallJobArgs) (any, error) {
	fractal, err := c.FractalID(ctx)
	if err != nil {
		return nil, err
	}
	return c.Post(ctx, "/recall/"+url.PathEscape(fractal)+"/"+url.PathEscape(in.JobID)+"/cancel", nil)
}

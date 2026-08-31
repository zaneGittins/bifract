package aitools

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// aiTag marks every comment this server writes, so an analyst reading the
	// thread can tell what came from a model.
	aiTag = "AI-Generated"

	maxTagLength    = 100
	maxBulkComments = 500
)

func registerCommentTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "add_comment",
		Annotations: mutates(),
		Description: "Record that a log matters, and why.\n\n" +
			"This is the only way to mark an event. A comment is what an analyst sees as the " +
			"row's mark in search results, what comments() finds, and, with notebook_id, what " +
			"appears in that notebook's timeline. The tag \"AI-Generated\" is always added.\n\n" +
			"Pass notebook_id to file the event into an investigation, which is where evidence " +
			"belongs: create_notebook once at the start of a hunt, then file every finding into " +
			"it as you go. Filing the same log twice is a no-op, so re-running is safe.\n\n" +
			"Text may be empty when filing: that marks the event as evidence without claiming " +
			"anything about it yet.\n\n" +
			"Also give related comments a shared tag of the form IR-<OneWord> (IR-BruteForce, " +
			"IR-Exfiltration), which is how they are pulled up together outside a notebook.\n\n" +
			"Returns the created comment.",
	}, addComment)

	add(d, &mcp.Tool{
		Name:        "list_comments",
		Annotations: readOnly(),
		Description: "List every comment in the fractal, most recent first.\n\n" +
			"Returns comments with their text, tags, author, the log they annotate, and the " +
			"notebooks each is filed into. A comment with no notebooks was never collected.",
	}, listComments)

	add(d, &mcp.Tool{
		Name:        "list_comment_tags",
		Annotations: readOnly(),
		Description: "List the comment tags in use in this fractal.\n\n" +
			"Use this to find the IR-<Name> tag for an investigation already under way, so a " +
			"new finding joins it instead of starting a parallel thread.",
	}, listCommentTags)

	add(d, &mcp.Tool{
		Name:        "add_tag",
		Annotations: mutates(),
		Description: "Add a tag to existing comments.\n\n" +
			"Use this to pull comments that were written separately into one investigation: " +
			"tag each with the same IR-<OneWord> tag. Comment ids come from list_comments or " +
			"get_log_comments (the `id` field), not the log_id.\n\n" +
			"Returns the number of comments updated.",
	}, addTag)

	add(d, &mcp.Tool{
		Name:        "remove_tag",
		Annotations: mutates(),
		Description: "Remove a tag from existing comments. A comment without it is left unchanged.\n\n" +
			"Returns the number of comments updated.",
	}, removeTag)

	add(d, &mcp.Tool{
		Name:        "get_log_comments",
		Annotations: readOnly(),
		Description: "Get the comments on one log entry.\n\n" +
			"Worth checking before adding a comment: an analyst may have already triaged it.",
	}, getLogComments)
}

type addCommentArgs struct {
	LogID      string   `json:"log_id" jsonschema:"The log_id of the entry to comment on. Query results carry it."`
	Text       string   `json:"text" jsonschema:"The comment body; markdown is supported. May be empty when filing into a notebook."`
	Tags       []string `json:"tags,omitempty" jsonschema:"Additional tags to attach, for example ['IR-BruteForce']."`
	NotebookID string   `json:"notebook_id,omitempty" jsonschema:"File the event into this notebook as evidence. Ids come from list_notebooks or create_notebook. Omit to leave the comment unfiled."`
	Title      string   `json:"title,omitempty" jsonschema:"One line naming the event in the notebook outline, for example 'WKSTN-4471 - rundll32'. Ignored without notebook_id."`
}

func addComment(ctx context.Context, c Client, in addCommentArgs) (any, error) {
	tags := []string{aiTag}
	for _, tag := range in.Tags {
		if tag != aiTag {
			tags = append(tags, tag)
		}
	}

	body := map[string]any{
		"log_id": in.LogID,
		"text":   in.Text,
		"tags":   tags,
	}
	if notebookID := resolveNotebook(ctx, c, in.NotebookID); notebookID != "" {
		body["notebook_id"] = notebookID
		if in.Title != "" {
			body["title"] = in.Title
		}
	}
	return c.Post(ctx, "/comments", body)
}

// resolveNotebook picks where a comment is filed: what the caller named, or the
// notebook the analyst is capturing into.
//
// The active notebook is per-user state, so it answers only when the tool runs
// inside someone's session (the chat tab). Over MCP the credential is a machine
// principal with no such state, and the model has to name a notebook.
func resolveNotebook(ctx context.Context, c Client, explicit string) string {
	if explicit != "" {
		return explicit
	}
	state, err := c.Get(ctx, "/notebooks/active", nil)
	if err != nil {
		return ""
	}
	return Field[string](state, "notebook_id")
}

func listComments(ctx context.Context, c Client, _ noArgs) (any, error) {
	return c.Get(ctx, "/comments/flat", nil)
}

func listCommentTags(ctx context.Context, c Client, _ noArgs) (any, error) {
	return c.Get(ctx, "/comments/tags", nil)
}

type bulkTagArgs struct {
	CommentIDs []string `json:"comment_ids" jsonschema:"Ids of the comments to change, 1 to 500. These are comment ids, not log ids."`
	Tag        string   `json:"tag" jsonschema:"The tag to add or remove."`
}

// bulkBody applies the server's limits locally, so a call that would be rejected
// fails without spending a round trip.
func (in bulkTagArgs) bulkBody() (map[string]any, error) {
	tag := strings.TrimSpace(in.Tag)
	if tag == "" || len(tag) > maxTagLength {
		return nil, fmt.Errorf("tag must be 1-%d characters", maxTagLength)
	}
	ids := make([]string, 0, len(in.CommentIDs))
	for _, id := range in.CommentIDs {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			ids = append(ids, trimmed)
		}
	}
	if len(ids) == 0 || len(ids) > maxBulkComments {
		return nil, fmt.Errorf("must provide 1-%d comment ids", maxBulkComments)
	}
	return map[string]any{"comment_ids": ids, "tag": tag}, nil
}

func addTag(ctx context.Context, c Client, in bulkTagArgs) (any, error) {
	body, err := in.bulkBody()
	if err != nil {
		return nil, err
	}
	return c.Post(ctx, "/comments/bulk-add-tag", body)
}

func removeTag(ctx context.Context, c Client, in bulkTagArgs) (any, error) {
	body, err := in.bulkBody()
	if err != nil {
		return nil, err
	}
	return c.Post(ctx, "/comments/bulk-remove-tag", body)
}

type logIDArgs struct {
	LogID string `json:"log_id" jsonschema:"The log_id to look up."`
}

func getLogComments(ctx context.Context, c Client, in logIDArgs) (any, error) {
	return c.Get(ctx, "/logs/"+url.PathEscape(in.LogID)+"/comments", nil)
}

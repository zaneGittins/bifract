package mcpserver

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

func registerCommentTools(s *mcp.Server, c *Client) {
	register(s, c, &mcp.Tool{
		Name: "add_comment",
		Description: "Attach a comment to a log entry.\n\n" +
			"Comments are how findings are recorded for other analysts. The tag " +
			"\"AI-Generated\" is always added.\n\n" +
			"When several logs belong to one investigation, give them a shared tag of the " +
			"form IR-<OneWord> (IR-BruteForce, IR-Exfiltration, IR-LateralMovement) and " +
			"reuse it across every comment in that investigation so they can be pulled up " +
			"together.\n\n" +
			"Returns the created comment.",
	}, addComment)

	register(s, c, &mcp.Tool{
		Name: "list_comments",
		Description: "List every comment in the fractal, most recent first.\n\n" +
			"Returns comments with their text, tags, author, and the log they annotate.",
	}, listComments)

	register(s, c, &mcp.Tool{
		Name: "list_comment_tags",
		Description: "List the comment tags in use in this fractal.\n\n" +
			"Use this to find the IR-<Name> tag for an investigation already under way, so a " +
			"new finding joins it instead of starting a parallel thread.",
	}, listCommentTags)

	register(s, c, &mcp.Tool{
		Name: "add_tag",
		Description: "Add a tag to existing comments.\n\n" +
			"Use this to pull comments that were written separately into one investigation: " +
			"tag each with the same IR-<OneWord> tag. Comment ids come from list_comments or " +
			"get_log_comments (the `id` field), not the log_id.\n\n" +
			"Returns the number of comments updated.",
	}, addTag)

	register(s, c, &mcp.Tool{
		Name: "remove_tag",
		Description: "Remove a tag from existing comments. A comment without it is left unchanged.\n\n" +
			"Returns the number of comments updated.",
	}, removeTag)

	register(s, c, &mcp.Tool{
		Name: "get_log_comments",
		Description: "Get the comments on one log entry.\n\n" +
			"Worth checking before adding a comment: an analyst may have already triaged it.",
	}, getLogComments)
}

type addCommentArgs struct {
	LogID string   `json:"log_id" jsonschema:"The log_id of the entry to comment on. Query results carry it."`
	Text  string   `json:"text" jsonschema:"The comment body; markdown is supported."`
	Tags  []string `json:"tags,omitempty" jsonschema:"Additional tags to attach, for example ['IR-BruteForce']."`
}

func addComment(ctx context.Context, c *Client, in addCommentArgs) (any, error) {
	tags := []string{aiTag}
	for _, tag := range in.Tags {
		if tag != aiTag {
			tags = append(tags, tag)
		}
	}
	return c.Post(ctx, "/comments", map[string]any{
		"log_id": in.LogID,
		"text":   in.Text,
		"tags":   tags,
	})
}

func listComments(ctx context.Context, c *Client, _ noArgs) (any, error) {
	return c.Get(ctx, "/comments/flat", nil)
}

func listCommentTags(ctx context.Context, c *Client, _ noArgs) (any, error) {
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

func addTag(ctx context.Context, c *Client, in bulkTagArgs) (any, error) {
	body, err := in.bulkBody()
	if err != nil {
		return nil, err
	}
	return c.Post(ctx, "/comments/bulk-add-tag", body)
}

func removeTag(ctx context.Context, c *Client, in bulkTagArgs) (any, error) {
	body, err := in.bulkBody()
	if err != nil {
		return nil, err
	}
	return c.Post(ctx, "/comments/bulk-remove-tag", body)
}

type logIDArgs struct {
	LogID string `json:"log_id" jsonschema:"The log_id to look up."`
}

func getLogComments(ctx context.Context, c *Client, in logIDArgs) (any, error) {
	return c.Get(ctx, "/logs/"+url.PathEscape(in.LogID)+"/comments", nil)
}

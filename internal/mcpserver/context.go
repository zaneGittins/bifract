package mcpserver

import (
	"context"
	"encoding/json"
	"strings"

	"bifract/pkg/aitools"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// get_context stays here rather than in aitools because it reports the binding
// of an MCP session in particular: the URL, the key's scope, and how the
// environment settings resolved against it. Chat has none of that, and shows
// the fractal it is scoped to on screen.
func addContextTool(s *mcp.Server, c *Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_context",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true},
		Description: "Report which Bifract instance and fractal this session is bound to.\n\n" +
			"Call this first in a new conversation. The API key fixes the fractal, so nothing " +
			"here is selectable; it tells you the scope your queries will run in and the role " +
			"that governs what you may write.\n\n" +
			"Returns the instance URL, server version, fractal id, and effective role.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		out, err := getContext(ctx, c)
		if err != nil {
			return nil, nil, err
		}
		text, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(text)}}}, nil, nil
	})
}

func getContext(ctx context.Context, c *Client) (any, error) {
	identity, err := c.Get(ctx, "/auth/user", nil)
	if err != nil {
		return nil, err
	}
	user := identity
	if nested := aitools.Field[map[string]any](identity, "user"); nested != nil {
		user = nested
	}
	version, err := c.Get(ctx, "/version", nil)
	if err != nil {
		return nil, err
	}

	role := aitools.Field[string](user, "fractal_role")
	if role == "" {
		role = aitools.Field[string](user, "prism_role")
	}
	if role == "" {
		role = "none"
	}
	name := aitools.Field[string](user, "display_name")
	if name == "" {
		name = aitools.Field[string](user, "username")
	}

	fractal, prism := aitools.Field[string](user, "selected_fractal"), aitools.Field[string](user, "selected_prism")

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
		"server_version": orUnknown(aitools.Field[string](version, "version")),
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

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

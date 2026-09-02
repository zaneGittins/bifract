package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"bifract/pkg/aitools"
)

// pendingToolCallTTL bounds how long an unapproved write stays offered. A
// proposal the user walks away from must not be approvable an hour later.
const pendingToolCallTTL = 15 * time.Minute

// exposed names the tools chat may call. It is an allowlist on purpose: a tool
// added to aitools reaches MCP at once, but nobody can call it from chat until
// it is named here.
//
// Chat's threat model is worse than MCP's. Tool results carry ingested log
// data, which is attacker-controlled text, so anything left out here is left
// out because an injected instruction could otherwise reach it:
//
//	delete_alert             a deleted detection fails silently, and nobody
//	                         notices until the thing it watched for happens
//	add_dictionary_rows      writes watchlists that feed live detections
//	create_instruction_*     instruction pages are fed back into this model's
//	update_instruction_page  own prompt, so a write is injection that persists
//	                         across conversations and across users
//
// get_context and list_fractals are left out for a duller reason: a
// conversation is fixed to one scope, which the UI already shows.
var exposed = map[string]bool{
	// Query and discovery.
	"query_logs": true, "validate_bql": true, "get_fields": true,
	"get_field_stats": true, "get_recent_logs": true, "get_bql_reference": true,
	// Detections.
	"list_alerts": true, "get_alert": true, "get_alert_executions": true,
	"get_attack_coverage": true, "get_attack_gaps": true,
	// Investigation.
	"find_processes": true, "get_provenance_graph": true,
	// Enrichment.
	"list_dictionaries": true, "get_dictionary": true, "search_dictionary": true,
	"list_models": true, "get_model": true, "get_model_data": true,
	// What other analysts have already done.
	"list_comments": true, "get_log_comments": true, "list_comment_tags": true,
	"list_notebooks": true, "get_notebook": true,
	"list_saved_queries": true, "list_dashboards": true, "get_dashboard": true,
	"list_instruction_libraries": true, "get_instruction_library": true,
	"read_instruction_page": true,
	// Archive.
	"search_archive": true, "get_archive_search": true, "cancel_archive_search": true,
	// Writes, every one of which the user is asked about first.
	"create_alert": true, "update_alert": true,
	"add_comment": true, "add_tag": true, "remove_tag": true,
	"create_notebook": true, "add_notebook_section": true,
}

// toolset is the tool surface chat offers, resolved once at startup.
type toolset struct {
	byName map[string]aitools.Tool
	defs   []llmTool
}

// newToolset selects the exposed tools and renders their schemas for the LLM.
// A name in the allowlist that no tool answers to is a programming error: it
// would silently narrow the surface without anyone noticing.
func newToolset() (*toolset, error) {
	s := &toolset{byName: map[string]aitools.Tool{}}
	for _, tool := range aitools.All() {
		if !exposed[tool.Name()] {
			continue
		}
		s.byName[tool.Name()] = tool
		s.defs = append(s.defs, llmTool{
			Type: "function",
			Function: llmFunction{
				Name:        tool.Name(),
				Description: tool.Def.Description,
				Parameters:  tool.Def.InputSchema,
			},
		})
	}
	for name := range exposed {
		if _, ok := s.byName[name]; !ok {
			return nil, fmt.Errorf("chat exposes %q, which is not a registered tool", name)
		}
	}
	return s, nil
}

// lookup returns an exposed tool by name. An unknown name is refused rather
// than guessed at: the model may only reach what the allowlist named.
func (s *toolset) lookup(name string) (aitools.Tool, bool) {
	tool, ok := s.byName[name]
	return tool, ok
}

// client builds an API client that runs as the caller of origin, capped at what
// tool declared it needs.
func (m *Manager) client(origin *http.Request, tool aitools.Tool, fractalID string) (aitools.Client, error) {
	if m.router == nil {
		return nil, errors.New("the chat tool runtime is not wired up")
	}
	return aitools.NewInProcess(m.router, origin, tool.Ceiling(), fractalID), nil
}

// runTool executes one tool call under the caller's own identity, with exactly
// the arguments given. Nothing is rewritten here: an approved call has to run
// what the user was shown, so the range is resolved before the call is offered.
func (m *Manager) runTool(ctx context.Context, origin *http.Request, tool aitools.Tool, args json.RawMessage, fractalID string) (any, error) {
	client, err := m.client(origin, tool, fractalID)
	if err != nil {
		return nil, err
	}
	return tool.Call(ctx, client, args)
}

// withUserWindow forces the range the UI picker selected onto any tool that
// takes one. The picker governs what a tool may scan; the model does not.
func withUserWindow(tool aitools.Tool, args json.RawMessage, timeRange string) json.RawMessage {
	if !tool.TakesWindow() {
		return args
	}

	fields := map[string]any{}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &fields); err != nil {
			return args
		}
	}
	start, end := resolveWindow(timeRange)
	fields["start"] = start.UTC().Format(time.RFC3339)
	fields["end"] = end.UTC().Format(time.RFC3339)

	rewritten, err := json.Marshal(fields)
	if err != nil {
		return args
	}
	return rewritten
}

// resolveWindow turns the picker's value into an absolute range.
func resolveWindow(timeRange string) (start, end time.Time) {
	end = time.Now()
	switch timeRange {
	case "5m":
		return end.Add(-5 * time.Minute), end
	case "15m":
		return end.Add(-15 * time.Minute), end
	case "1h":
		return end.Add(-1 * time.Hour), end
	case "6h":
		return end.Add(-6 * time.Hour), end
	case "12h":
		return end.Add(-12 * time.Hour), end
	case "7d":
		return end.Add(-7 * 24 * time.Hour), end
	case "30d":
		return end.Add(-30 * 24 * time.Hour), end
	case "all":
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), end
	}
	return end.Add(-24 * time.Hour), end
}

// PendingToolCall is a write the model proposed and the user has not approved
// yet. The arguments are held here rather than in the browser, so nothing
// between showing the user what will run and running it can change what runs.
type PendingToolCall struct {
	ID         string          `json:"id"`
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments"`
}

// OfferedToolCall is a proposed write and what became of it. Reopening a
// conversation rebuilds the approval card from this record, so the arguments
// are the ones that were offered rather than the ones the model asked for.
type OfferedToolCall struct {
	ID         string          `json:"id"`
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments"`
	// Status is "open", "approve", "deny", "superseded" or "expired". Only an
	// open call is still answerable, and claimToolCall enforces that again.
	Status string `json:"status"`
}

// ListOfferedToolCalls returns every call this conversation put to the user, in
// the order they were offered.
func (m *Manager) ListOfferedToolCalls(ctx context.Context, conversationID string) ([]*OfferedToolCall, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id, tool_call_id, tool_name, arguments,
		       CASE
		           WHEN resolved_at IS NOT NULL THEN COALESCE(decision, 'superseded')
		           WHEN expires_at <= NOW() THEN 'expired'
		           ELSE 'open'
		       END
		FROM chat_pending_tool_calls
		WHERE conversation_id = $1
		ORDER BY created_at ASC`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var offers []*OfferedToolCall
	for rows.Next() {
		offer := &OfferedToolCall{}
		var arguments []byte
		if err := rows.Scan(&offer.ID, &offer.ToolCallID, &offer.ToolName, &arguments, &offer.Status); err != nil {
			return nil, err
		}
		// Re-encode so the browser sees the same bytes the live stream sends:
		// JSONB does not give the object's keys back in the order they went in.
		offer.Arguments = normalizeJSON(arguments)
		offers = append(offers, offer)
	}
	return offers, rows.Err()
}

// normalizeJSON re-encodes a value so its object keys are ordered the way Go
// writes them, which is what every other path serving these arguments does.
func normalizeJSON(raw []byte) json.RawMessage {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return json.RawMessage(raw)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(raw)
	}
	return encoded
}

// offerToolCall records a call for the user to approve.
func (m *Manager) offerToolCall(ctx context.Context, conversationID, toolCallID, toolName string, args json.RawMessage, requestedBy string) (*PendingToolCall, error) {
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	pending := &PendingToolCall{ToolCallID: toolCallID, ToolName: toolName, Arguments: args}
	err := m.pg.QueryRow(ctx, `
		INSERT INTO chat_pending_tool_calls
			(conversation_id, tool_call_id, tool_name, arguments, requested_by, expires_at)
		VALUES ($1, $2, $3, $4, $5, NOW() + $6::interval)
		RETURNING id`,
		conversationID, toolCallID, toolName, []byte(args), requestedBy,
		fmt.Sprintf("%d seconds", int(pendingToolCallTTL.Seconds())),
	).Scan(&pending.ID)
	if err != nil {
		return nil, err
	}
	return pending, nil
}

// claimToolCall resolves one offered call, atomically, once. The requester,
// the conversation and the deadline are all part of the match, so an approval
// cannot be replayed, reused, or applied to somebody else's conversation.
func (m *Manager) claimToolCall(ctx context.Context, conversationID, id, requestedBy, decision string) (*PendingToolCall, error) {
	pending := &PendingToolCall{ID: id}
	var arguments []byte
	err := m.pg.QueryRow(ctx, `
		UPDATE chat_pending_tool_calls
		SET resolved_at = NOW(), decision = $1
		WHERE id = $2 AND conversation_id = $3 AND requested_by = $4
		  AND resolved_at IS NULL AND expires_at > NOW()
		RETURNING tool_call_id, tool_name, arguments`,
		decision, id, conversationID, requestedBy,
	).Scan(&pending.ToolCallID, &pending.ToolName, &arguments)
	if err != nil {
		return nil, err
	}
	pending.Arguments = arguments
	return pending, nil
}

// supersedePendingToolCalls closes anything still waiting on this conversation.
// A user who types instead of answering has moved on, and an action they left
// unanswered must not stay approvable behind the turn that replaced it.
func (m *Manager) supersedePendingToolCalls(ctx context.Context, conversationID string) error {
	_, err := m.pg.Exec(ctx, `
		UPDATE chat_pending_tool_calls
		SET resolved_at = NOW(), decision = 'superseded'
		WHERE conversation_id = $1 AND resolved_at IS NULL`, conversationID)
	return err
}

// openToolCalls counts the calls still waiting on this conversation. The stream
// resumes only once none are left.
func (m *Manager) openToolCalls(ctx context.Context, conversationID string) (int, error) {
	var open int
	err := m.pg.QueryRow(ctx, `
		SELECT count(*) FROM chat_pending_tool_calls
		WHERE conversation_id = $1 AND resolved_at IS NULL AND expires_at > NOW()`,
		conversationID).Scan(&open)
	return open, err
}

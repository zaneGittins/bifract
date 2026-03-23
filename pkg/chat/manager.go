package chat

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"

	"bifract/pkg/fractals"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

const bqlSyntaxRef = `
BQL query syntax reference:

IMPORTANT: Every query MUST start with a filter expression. You cannot start with a pipe command.
The simplest "match all" filter is: level=*

FILTER SYNTAX:
  field=value              # exact match
  field=*value*            # wildcard match (contains)
  field=/regex/            # regex match (case sensitive)
  field=/regex/i           # regex match (case insensitive)
  field=*                  # field exists / not empty
  field1=val field2=val    # multiple filters (AND)

PIPE COMMANDS (after filter, separated by |):
  groupby(field)           # group results by field, adds count
  groupby(field1, field2)  # group by multiple fields
  groupby(field, limit=10) # group with result limit
  count()                  # total count of matching logs
  multi(count())           # same as count()
  multi(count(), avg(field), max(field)) # multiple aggregations
  sum(field), avg(field), max(field), min(field) # single aggregation
  table(field1, field2)    # select specific columns
  head(N)                  # limit to first N results (e.g. head(10))
  sort(field, order=desc)  # sort results
  bucket("1h", "count()")  # time-bucketed aggregation

HAVING CONDITIONS (after groupby):
  groupby(field) | count > 100       # filter groups by count
  groupby(field) | avg(field) > 500  # filter groups by aggregate

VALID query examples:
  level=*                             # all logs (match any level)
  level=error                         # filter by exact level
  level=error host=web01              # multiple AND filters
  /powershell/i                        # search full log text for "powershell"
  /powershell/i | count()             # count logs containing "powershell"
  /error|fail/i | head(20)            # regex search across raw log text
  level=* | groupby(level)            # count per level (THIS is how you group)
  level=* | groupby(level, host)      # count per level+host combo
  level=* | count()                   # total count of all logs
  level=* | head(20)                  # first 20 logs
  level=* | table(timestamp, level, message) # specific columns
  level=error | groupby(host) | count > 5   # hosts with >5 errors
  source=*nginx* | groupby(level)     # nginx logs grouped by level
  message=/timeout/i | count()        # count timeout messages
  level=* | bucket("1h", "count()")   # hourly log counts

INVALID queries (WILL FAIL, never generate these):
  level=* | multi count() by level    # WRONG: no "by" clause, use groupby()
  multi count() by level              # WRONG: must start with filter
  * | groupby(level)                  # WRONG: * alone is not a valid filter
  level=* | sort by timestamp desc    # WRONG: use sort(timestamp, order=desc)

SEARCHING LOG CONTENT:
- To search across all log content, use /keyword/i (regex) or "keyword" (bare string)
- raw_log contains the full original log text. Use raw_log=/keyword/i for explicit field regex.
- Only use specific fields (e.g. command_line=/powershell/i) when you KNOW that field exists from get_fields
- Do NOT use "full_log". It does not exist. Use raw_log instead.

RULES:
- Always start with a filter like field=value, field=*, or a bare content search like *keyword*
- Use groupby() to aggregate by field (NOT "multi ... by ...")
- Use count(), sum(), avg(), max(), min() as standalone pipe commands or inside multi()
- All log fields are strings in a map
- ALWAYS call get_fields first in a new conversation to discover available field names
- Do NOT assume fields exist. Use get_fields to discover them.
- Do NOT use "full_log" as a field name. It does not exist. Use bare filters (*keyword*) to search log content.
- After discovering fields, use those real field names in your queries
`

// Manager handles chat conversation persistence and LLM communication.
type Manager struct {
	pg                  *storage.PostgresClient
	ch                  *storage.ClickHouseClient
	fractalManager      *fractals.Manager
	normalizerManager   *normalizers.Manager
	litellmURL          string
	litellmKey          string
	httpClient          *http.Client
}

// NewManager creates a new chat manager.
func NewManager(
	pg *storage.PostgresClient,
	ch *storage.ClickHouseClient,
	fractalManager *fractals.Manager,
	normalizerManager *normalizers.Manager,
	litellmURL, litellmKey string,
) *Manager {
	return &Manager{
		pg:                pg,
		ch:                ch,
		fractalManager:    fractalManager,
		normalizerManager: normalizerManager,
		litellmURL:        litellmURL,
		litellmKey:        litellmKey,
		httpClient:        &http.Client{Timeout: 120 * time.Second},
	}
}

// ---- Conversation CRUD ----

func (m *Manager) CreateConversation(ctx context.Context, fractalID, title, username string, instructionID *string) (*Conversation, error) {
	if title == "" {
		title = "New conversation"
	}
	conv := &Conversation{}
	err := m.pg.QueryRow(ctx, `
		INSERT INTO chat_conversations (fractal_id, title, created_by, instruction_id)
		VALUES ($1, $2, $3, $4)
		RETURNING id, fractal_id, title, instruction_id, created_by, created_at, updated_at
	`, fractalID, title, username, instructionID).Scan(
		&conv.ID, &conv.FractalID, &conv.Title, &conv.InstructionID, &conv.CreatedBy, &conv.CreatedAt, &conv.UpdatedAt,
	)
	return conv, err
}

func (m *Manager) ListConversations(ctx context.Context, fractalID, username string) ([]*Conversation, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id, fractal_id, title, instruction_id, created_by, created_at, updated_at
		FROM chat_conversations
		WHERE fractal_id = $1 AND created_by = $2
		ORDER BY updated_at DESC
		LIMIT 100
	`, fractalID, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var convs []*Conversation
	for rows.Next() {
		c := &Conversation{}
		if err := rows.Scan(&c.ID, &c.FractalID, &c.Title, &c.InstructionID, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		convs = append(convs, c)
	}
	return convs, rows.Err()
}

func (m *Manager) GetConversation(ctx context.Context, id string) (*Conversation, error) {
	c := &Conversation{}
	err := m.pg.QueryRow(ctx, `
		SELECT id, fractal_id, title, instruction_id, created_by, created_at, updated_at
		FROM chat_conversations WHERE id = $1
	`, id).Scan(&c.ID, &c.FractalID, &c.Title, &c.InstructionID, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("conversation not found")
	}
	return c, err
}

func (m *Manager) RenameConversation(ctx context.Context, id, title string) (*Conversation, error) {
	c := &Conversation{}
	err := m.pg.QueryRow(ctx, `
		UPDATE chat_conversations SET title = $1
		WHERE id = $2
		RETURNING id, fractal_id, title, instruction_id, created_by, created_at, updated_at
	`, title, id).Scan(&c.ID, &c.FractalID, &c.Title, &c.InstructionID, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("conversation not found")
	}
	return c, err
}

func (m *Manager) DeleteConversation(ctx context.Context, id string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM chat_conversations WHERE id = $1`, id)
	return err
}

func (m *Manager) ClearMessages(ctx context.Context, conversationID string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM chat_messages WHERE conversation_id = $1`, conversationID)
	return err
}

func (m *Manager) DeleteAllConversations(ctx context.Context, fractalID, username string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM chat_conversations WHERE fractal_id = $1 AND created_by = $2`, fractalID, username)
	return err
}

func (m *Manager) SetConversationInstruction(ctx context.Context, id string, instructionID *string) (*Conversation, error) {
	c := &Conversation{}
	err := m.pg.QueryRow(ctx, `
		UPDATE chat_conversations SET instruction_id = $1
		WHERE id = $2
		RETURNING id, fractal_id, title, instruction_id, created_by, created_at, updated_at
	`, instructionID, id).Scan(&c.ID, &c.FractalID, &c.Title, &c.InstructionID, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("conversation not found")
	}
	return c, err
}

// ---- Instruction CRUD ----

func (m *Manager) CreateInstruction(ctx context.Context, fractalID, name, content, username string, isDefault bool) (*Instruction, error) {
	// If setting as default, clear any existing default for this fractal first
	if isDefault {
		m.pg.Exec(ctx, `UPDATE chat_instructions SET is_default = false WHERE fractal_id = $1 AND is_default = true`, fractalID)
	}
	inst := &Instruction{}
	err := m.pg.QueryRow(ctx, `
		INSERT INTO chat_instructions (fractal_id, name, content, is_default, created_by)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, fractal_id, name, content, is_default, created_by, created_at, updated_at
	`, fractalID, name, content, isDefault, username).Scan(
		&inst.ID, &inst.FractalID, &inst.Name, &inst.Content, &inst.IsDefault, &inst.CreatedBy, &inst.CreatedAt, &inst.UpdatedAt,
	)
	return inst, err
}

func (m *Manager) ListInstructions(ctx context.Context, fractalID string) ([]*Instruction, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id, fractal_id, name, content, is_default, created_by, created_at, updated_at
		FROM chat_instructions
		WHERE fractal_id = $1
		ORDER BY is_default DESC, name ASC
	`, fractalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var insts []*Instruction
	for rows.Next() {
		inst := &Instruction{}
		if err := rows.Scan(&inst.ID, &inst.FractalID, &inst.Name, &inst.Content, &inst.IsDefault, &inst.CreatedBy, &inst.CreatedAt, &inst.UpdatedAt); err != nil {
			return nil, err
		}
		insts = append(insts, inst)
	}
	return insts, rows.Err()
}

func (m *Manager) GetInstruction(ctx context.Context, id string) (*Instruction, error) {
	inst := &Instruction{}
	err := m.pg.QueryRow(ctx, `
		SELECT id, fractal_id, name, content, is_default, created_by, created_at, updated_at
		FROM chat_instructions WHERE id = $1
	`, id).Scan(&inst.ID, &inst.FractalID, &inst.Name, &inst.Content, &inst.IsDefault, &inst.CreatedBy, &inst.CreatedAt, &inst.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("instruction not found")
	}
	return inst, err
}

func (m *Manager) UpdateInstruction(ctx context.Context, id, name, content string, isDefault bool) (*Instruction, error) {
	// If setting as default, need to clear existing default for the same fractal
	if isDefault {
		m.pg.Exec(ctx, `
			UPDATE chat_instructions SET is_default = false
			WHERE fractal_id = (SELECT fractal_id FROM chat_instructions WHERE id = $1)
			  AND is_default = true AND id != $1
		`, id)
	}
	inst := &Instruction{}
	err := m.pg.QueryRow(ctx, `
		UPDATE chat_instructions SET name = $1, content = $2, is_default = $3
		WHERE id = $4
		RETURNING id, fractal_id, name, content, is_default, created_by, created_at, updated_at
	`, name, content, isDefault, id).Scan(
		&inst.ID, &inst.FractalID, &inst.Name, &inst.Content, &inst.IsDefault, &inst.CreatedBy, &inst.CreatedAt, &inst.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("instruction not found")
	}
	return inst, err
}

func (m *Manager) DeleteInstruction(ctx context.Context, id string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM chat_instructions WHERE id = $1`, id)
	return err
}

func (m *Manager) GetDefaultInstruction(ctx context.Context, fractalID string) (*Instruction, error) {
	inst := &Instruction{}
	err := m.pg.QueryRow(ctx, `
		SELECT id, fractal_id, name, content, is_default, created_by, created_at, updated_at
		FROM chat_instructions
		WHERE fractal_id = $1 AND is_default = true
	`, fractalID).Scan(&inst.ID, &inst.FractalID, &inst.Name, &inst.Content, &inst.IsDefault, &inst.CreatedBy, &inst.CreatedAt, &inst.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return inst, err
}

// ---- Message CRUD ----

func (m *Manager) GetMessages(ctx context.Context, conversationID string) ([]*Message, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id, conversation_id, role, content, tool_calls, tool_results, created_at
		FROM chat_messages
		WHERE conversation_id = $1
		ORDER BY created_at ASC
	`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		msg := &Message{}
		var toolCalls, toolResults []byte
		if err := rows.Scan(&msg.ID, &msg.ConversationID, &msg.Role, &msg.Content, &toolCalls, &toolResults, &msg.CreatedAt); err != nil {
			return nil, err
		}
		if len(toolCalls) > 0 && string(toolCalls) != "[]" {
			msg.ToolCalls = json.RawMessage(toolCalls)
		}
		if len(toolResults) > 0 && string(toolResults) != "[]" {
			msg.ToolResults = json.RawMessage(toolResults)
		}
		msgs = append(msgs, msg)
	}
	return msgs, rows.Err()
}

func (m *Manager) saveMessage(ctx context.Context, conversationID, role, content string, toolCalls, toolResults json.RawMessage) (*Message, error) {
	if toolCalls == nil {
		toolCalls = json.RawMessage("[]")
	}
	if toolResults == nil {
		toolResults = json.RawMessage("[]")
	}
	msg := &Message{}
	var tc, tr []byte
	err := m.pg.QueryRow(ctx, `
		INSERT INTO chat_messages (conversation_id, role, content, tool_calls, tool_results)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, conversation_id, role, content, tool_calls, tool_results, created_at
	`, conversationID, role, content, string(toolCalls), string(toolResults)).Scan(
		&msg.ID, &msg.ConversationID, &msg.Role, &msg.Content, &tc, &tr, &msg.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	if len(tc) > 0 && string(tc) != "[]" {
		msg.ToolCalls = json.RawMessage(tc)
	}
	if len(tr) > 0 && string(tr) != "[]" {
		msg.ToolResults = json.RawMessage(tr)
	}
	// Touch conversation updated_at
	m.pg.Exec(ctx, `UPDATE chat_conversations SET updated_at = NOW() WHERE id = $1`, conversationID)
	return msg, nil
}

// ---- Streaming LLM response ----

// StreamResponse sends the user message, calls LiteLLM, streams events back via the writer,
// and persists the full exchange to the database.
func (m *Manager) StreamResponse(ctx context.Context, w io.Writer, flusher http.Flusher, conv *Conversation, fractal *fractals.Fractal, userContent string, timeRange string) error {
	// Save user message
	if _, err := m.saveMessage(ctx, conv.ID, "user", userContent, nil, nil); err != nil {
		return fmt.Errorf("failed to save user message: %w", err)
	}

	// Auto-title from first user message
	if conv.Title == "New conversation" && userContent != "" {
		title := userContent
		if len(title) > 60 {
			title = title[:60] + "..."
		}
		m.RenameConversation(ctx, conv.ID, title)
		// Emit a title event so frontend can update sidebar
		sendSSEEvent(w, flusher, StreamEvent{Type: "title", Content: title})
	}

	// Build message history for LiteLLM
	history, err := m.GetMessages(ctx, conv.ID)
	if err != nil {
		return fmt.Errorf("failed to load history: %w", err)
	}

	// Resolve custom instructions: explicit on conversation, or fractal default
	var customInstructions string
	if conv.InstructionID != nil {
		if inst, err := m.GetInstruction(ctx, *conv.InstructionID); err == nil && inst != nil {
			customInstructions = inst.Content
		}
	} else {
		if inst, err := m.GetDefaultInstruction(ctx, conv.FractalID); err == nil && inst != nil {
			customInstructions = inst.Content
		}
	}

	recentQueries := m.getRecentSuccessfulQueries(ctx, conv.FractalID, conv.ID)
	normalizerHints := m.getNormalizerHints(ctx)
	systemPrompt := m.buildSystemPrompt(fractal, recentQueries, customInstructions, normalizerHints)
	history = m.trimHistory(history)
	messages := m.buildLLMMessages(systemPrompt, history)

	// Tool definitions
	tools := m.toolDefinitions()

	// Stream loop - may iterate multiple times if tool calls are made
	const maxToolRounds = 15
	for round := 0; round < maxToolRounds; round++ {
		// Call LiteLLM with streaming
		assistantContent, toolCallsRaw, err := m.streamFromLiteLLM(ctx, w, flusher, messages, tools)
		if err != nil {
			sendSSEEvent(w, flusher, StreamEvent{Type: "error", Content: err.Error()})
			return err
		}

		if len(toolCallsRaw) == 0 {
			// Log if content looks like it contains XML tool calls (LiteLLM translation issue)
			if strings.Contains(assistantContent, "<invoke") || strings.Contains(assistantContent, "<tool_use>") || strings.Contains(assistantContent, "present_results") && strings.Contains(assistantContent, "<parameter") {
				log.Printf("[Chat] WARNING: assistant content appears to contain XML tool calls instead of proper function calls. Content length: %d. This is likely a LiteLLM streaming translation issue.", len(assistantContent))
			}
			// No tool calls - done
			if _, err := m.saveMessage(ctx, conv.ID, "assistant", assistantContent, nil, nil); err != nil {
				log.Printf("[Chat] failed to save assistant message: %v", err)
			}
			break
		}

		toolCallsJSON, _ := json.Marshal(toolCallsRaw)

		// Check for display-only tool calls (think, render_chart, present_results)
		isPresentCall := false
		for _, tc := range toolCallsRaw {
			if tc.Function.Name == "think" {
				var args struct {
					Reasoning string `json:"reasoning"`
				}
				json.Unmarshal([]byte(tc.Function.Arguments), &args)
				sendSSEEvent(w, flusher, StreamEvent{
					Type:     "think",
					ToolName: "think",
					ToolArgs: args,
				})
			}
			if tc.Function.Name == "render_chart" {
				var args renderChartArgs
				json.Unmarshal([]byte(tc.Function.Arguments), &args)
				sendSSEEvent(w, flusher, StreamEvent{
					Type:     "chart",
					ToolName: "render_chart",
					ToolArgs: args,
				})
			}
			if tc.Function.Name == "present_results" {
				isPresentCall = true
				var args presentResultsArgs
				json.Unmarshal([]byte(tc.Function.Arguments), &args)
				sendSSEEvent(w, flusher, StreamEvent{
					Type:     "present",
					ToolName: "present_results",
					ToolArgs: args,
				})
				// Save with tool calls so history preserves both chart and severity styling
				if _, err := m.saveMessage(ctx, conv.ID, "assistant", args.Summary, json.RawMessage(toolCallsJSON), nil); err != nil {
					log.Printf("[Chat] failed to save present_results message: %v", err)
				}
				break
			}
		}
		if isPresentCall {
			break
		}

		// Persist assistant message with tool calls
		if _, err := m.saveMessage(ctx, conv.ID, "assistant", assistantContent, json.RawMessage(toolCallsJSON), nil); err != nil {
			log.Printf("[Chat] failed to save assistant message with tool calls: %v", err)
		}

		// Add assistant message to context
		assistantMsg := llmMessage{
			Role:      "assistant",
			Content:   assistantContent,
			ToolCalls: json.RawMessage(toolCallsJSON),
		}
		messages = append(messages, assistantMsg)

		// Execute each tool call
		for _, tc := range toolCallsRaw {
			// Display-only tools: skip execution, provide minimal result
			if tc.Function.Name == "render_chart" || tc.Function.Name == "think" {
				messages = append(messages, llmMessage{
					Role:       "tool",
					Content:    `{"ok":true}`,
					ToolCallID: tc.ID,
					Name:       tc.Function.Name,
				})
				continue
			}

			result, toolErr := m.executeTool(ctx, conv.FractalID, tc, timeRange)
			if toolErr != nil {
				result = map[string]interface{}{"error": toolErr.Error()}
			}

			// Emit tool_result event
			sendSSEEvent(w, flusher, StreamEvent{
				Type:       "tool_result",
				ToolName:   tc.Function.Name,
				ToolResult: result,
			})

			// Add tool result message to context (capped to avoid exceeding LLM context window)
			resultJSON, _ := json.Marshal(result)
			contextContent := capToolResultForContext(resultJSON)
			messages = append(messages, llmMessage{
				Role:       "tool",
				Content:    contextContent,
				ToolCallID: tc.ID,
				Name:       tc.Function.Name,
			})

			// Persist tool result message
			toolMeta := map[string]interface{}{
				"tool_call_id": tc.ID,
				"tool_name":    tc.Function.Name,
				"result":       result,
			}
			toolResultsJSON, _ := json.Marshal([]interface{}{toolMeta})
			m.saveMessage(ctx, conv.ID, "tool", string(resultJSON), nil, json.RawMessage(toolResultsJSON))
		}
		// Continue loop to get next LLM response after tool results
	}

	// If we exhausted all rounds without present_results, emit a fallback
	// so the user doesn't see a blank response
	sendSSEEvent(w, flusher, StreamEvent{Type: "done"})
	return nil
}

// streamFromLiteLLM calls LiteLLM with streaming and writes token events.
// Returns the full assistant content text and any tool calls.
func (m *Manager) streamFromLiteLLM(ctx context.Context, w io.Writer, flusher http.Flusher, messages []llmMessage, tools []llmTool) (string, []llmToolCall, error) {
	reqBody := llmRequest{
		Model:      "bifract-chat",
		Messages:   messages,
		Tools:      tools,
		Stream:     true,
		ToolChoice: "auto",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		m.litellmURL+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if m.litellmKey != "" {
		req.Header.Set("Authorization", "Bearer "+m.litellmKey)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("litellm request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		bodyStr := string(body)
		if resp.StatusCode == http.StatusUnauthorized && (strings.Contains(bodyStr, "api-key") || strings.Contains(bodyStr, "api_key") || strings.Contains(bodyStr, "AuthenticationError")) {
			return "", nil, fmt.Errorf("AI provider API key is not configured. Set LITELLM_API_KEY in your .env file")
		}
		return "", nil, fmt.Errorf("litellm error %d: %s", resp.StatusCode, bodyStr)
	}

	var fullContent strings.Builder
	// Accumulate tool call deltas by index
	toolCallMap := make(map[int]*llmToolCall)
	toolCallArgMap := make(map[int]*strings.Builder)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var chunk llmStreamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}

		for _, choice := range chunk.Choices {
			delta := choice.Delta

			// Accumulate content tokens
			if delta.Content != nil && *delta.Content != "" {
				fullContent.WriteString(*delta.Content)
				sendSSEEvent(w, flusher, StreamEvent{
					Type:    "token",
					Content: *delta.Content,
				})
			}

			// Accumulate tool call deltas (keyed by delta.Index)
			for _, tc := range delta.ToolCalls {
				idx := tc.Index
				if _, exists := toolCallMap[idx]; !exists {
					// First chunk for this tool call
					toolCallMap[idx] = &llmToolCall{
						ID:   tc.ID,
						Type: tc.Type,
						Function: llmToolFunction{
							Name: tc.Function.Name,
						},
					}
					toolCallArgMap[idx] = &strings.Builder{}
				} else {
					// Subsequent chunk: merge non-empty fields
					if tc.ID != "" {
						toolCallMap[idx].ID = tc.ID
					}
					if tc.Function.Name != "" {
						toolCallMap[idx].Function.Name = tc.Function.Name
					}
				}
				toolCallArgMap[idx].WriteString(tc.Function.Arguments)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fullContent.String(), nil, fmt.Errorf("stream read error: %w", err)
	}

	// Assemble tool calls
	var toolCalls []llmToolCall
	for i := 0; i < len(toolCallMap); i++ {
		tc := toolCallMap[i]
		tc.Function.Arguments = toolCallArgMap[i].String()
		toolCalls = append(toolCalls, *tc)

		// Emit tool_call event so frontend can show it
		var args interface{}
		json.Unmarshal([]byte(tc.Function.Arguments), &args)
		sendSSEEvent(w, flusher, StreamEvent{
			Type:     "tool_call",
			ToolName: tc.Function.Name,
			ToolArgs: args,
		})
	}

	return fullContent.String(), toolCalls, nil
}

// executeTool runs a tool call and returns the result.
func (m *Manager) executeTool(ctx context.Context, fractalID string, tc llmToolCall, userTimeRange string) (interface{}, error) {
	switch tc.Function.Name {
	case "run_query":
		var args runQueryArgs
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return nil, fmt.Errorf("invalid run_query args: %w", err)
		}
		// User-selected time range overrides AI's choice
		args.TimeRange = userTimeRange
		args.StartTime = ""
		args.EndTime = ""
		return m.executeQuery(ctx, fractalID, args)
	case "get_fields":
		var args struct {
			Filter string `json:"filter"`
		}
		json.Unmarshal([]byte(tc.Function.Arguments), &args)
		return m.getFields(ctx, fractalID, args.Filter)
	case "validate_bql":
		var args struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return nil, fmt.Errorf("invalid validate_bql args: %w", err)
		}
		return m.validateBQL(args.Query)
	case "search_alerts":
		var args struct {
			Search string `json:"search"`
		}
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return nil, fmt.Errorf("invalid search_alerts args: %w", err)
		}
		return m.searchAlerts(ctx, fractalID, args.Search)
	default:
		return nil, fmt.Errorf("unknown tool: %s", tc.Function.Name)
	}
}

// validateBQL parses a BQL query string without executing it, returning
// whether the syntax is valid. This lets the AI self-correct before wasting
// a tool round on a query that would fail.
func (m *Manager) validateBQL(query string) (interface{}, error) {
	_, err := parser.ParseQuery(query)
	if err != nil {
		return map[string]interface{}{
			"valid": false,
			"error": err.Error(),
		}, nil
	}
	return map[string]interface{}{
		"valid": true,
	}, nil
}

// getFields discovers available field names in the fractal's logs along with
// sample values and cardinality. This "field fingerprinting" gives the AI
// semantic understanding of each field so it can write accurate queries.
func (m *Manager) getFields(ctx context.Context, fractalID string, filter string) (interface{}, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	includeEmptyFractalID := false
	if m.fractalManager != nil {
		if defaultFractal, err := m.fractalManager.GetDefaultFractal(queryCtx); err == nil && defaultFractal.ID == fractalID {
			includeEmptyFractalID = true
		}
	}

	safeFractalID := strings.ReplaceAll(strings.ReplaceAll(fractalID, "\\", "\\\\"), "'", "\\'")
	fractalClause := "fractal_id = '" + safeFractalID + "'"
	if includeEmptyFractalID {
		fractalClause = "fractal_id IN ('" + safeFractalID + "', '')"
	}

	filterClause := ""
	if filter != "" {
		safe := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
				return r
			}
			return -1
		}, filter)
		if safe != "" {
			filterClause = fmt.Sprintf("HAVING field_name LIKE '%%%s%%'", safe)
		}
	}

	limit := 30
	if filter != "" {
		limit = 50
	}

	// Single query that extracts field names, sample values, and cardinality
	// from a recent sample of logs. Uses LIMIT on the inner subquery to keep
	// it fast even on very large datasets.
	sqlStr := fmt.Sprintf(`
		SELECT
			field_name,
			count() AS freq,
			uniqExact(field_value) AS cardinality,
			groupUniqArraySample(5)(field_value) AS samples
		FROM (
			SELECT
				replaceAll(p, '%%2E', '.') AS field_name,
				JSON_VALUE(fields, concat('$.', p)) AS field_value
			FROM (
				SELECT fields
				FROM logs
				WHERE %s AND timestamp >= now() - INTERVAL 1 DAY
				LIMIT 10000
			)
			ARRAY JOIN JSONAllPaths(fields) AS p
		)
		WHERE field_value != ''
		GROUP BY field_name
		%s
		ORDER BY freq DESC
		LIMIT %d
	`, fractalClause, filterClause, limit)

	rows, err := m.ch.Query(queryCtx, sqlStr)
	if err != nil {
		// Fall back to the simpler field-names-only query if fingerprinting fails
		return m.getFieldsSimple(ctx, fractalID, filter)
	}

	type fieldInfo struct {
		Name        string   `json:"name"`
		Count       uint64   `json:"count"`
		Cardinality uint64   `json:"cardinality"`
		Samples     []string `json:"samples"`
		Pattern     string   `json:"pattern"`
	}
	var fields []fieldInfo
	for _, row := range rows {
		fn, _ := row["field_name"].(string)
		if fn == "" {
			continue
		}
		freq, _ := row["freq"].(uint64)
		card, _ := row["cardinality"].(uint64)

		var samples []string
		if s, ok := row["samples"].([]interface{}); ok {
			for _, v := range s {
				if sv, ok := v.(string); ok && sv != "" {
					samples = append(samples, sv)
				}
			}
		}

		pattern := classifyFieldPattern(samples)
		fields = append(fields, fieldInfo{
			Name:        fn,
			Count:       freq,
			Cardinality: card,
			Samples:     samples,
			Pattern:     pattern,
		})
	}

	return map[string]interface{}{
		"fields": fields,
		"count":  len(fields),
	}, nil
}

// getFieldsSimple is the fallback field discovery that only returns names and counts.
func (m *Manager) getFieldsSimple(ctx context.Context, fractalID string, filter string) (interface{}, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	includeEmptyFractalID := false
	if m.fractalManager != nil {
		if defaultFractal, err := m.fractalManager.GetDefaultFractal(queryCtx); err == nil && defaultFractal.ID == fractalID {
			includeEmptyFractalID = true
		}
	}

	safeFractalID := strings.ReplaceAll(strings.ReplaceAll(fractalID, "\\", "\\\\"), "'", "\\'")
	fractalClause := "fractal_id = '" + safeFractalID + "'"
	if includeEmptyFractalID {
		fractalClause = "fractal_id IN ('" + safeFractalID + "', '')"
	}

	filterClause := ""
	if filter != "" {
		safe := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
				return r
			}
			return -1
		}, filter)
		if safe != "" {
			filterClause = fmt.Sprintf("AND field_name LIKE '%%%s%%'", safe)
		}
	}

	limit := 30
	if filter != "" {
		limit = 50
	}

	sqlStr := fmt.Sprintf(`
		SELECT field_name, count() AS freq
		FROM (
			SELECT replaceAll(arrayJoin(JSONAllPaths(fields)), '%%2E', '.') AS field_name
			FROM logs
			WHERE %s AND timestamp >= now() - INTERVAL 7 DAY
		)
		%s
		GROUP BY field_name
		ORDER BY freq DESC
		LIMIT %d
	`, fractalClause, filterClause, limit)

	rows, err := m.ch.Query(queryCtx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	type fieldInfo struct {
		Name  string `json:"name"`
		Count uint64 `json:"count"`
	}
	var fields []fieldInfo
	for _, row := range rows {
		fn, _ := row["field_name"].(string)
		freq, _ := row["freq"].(uint64)
		if fn != "" {
			fields = append(fields, fieldInfo{Name: fn, Count: freq})
		}
	}

	return map[string]interface{}{
		"fields": fields,
		"count":  len(fields),
	}, nil
}

// classifyFieldPattern analyzes sample values and returns a human-readable pattern hint.
func classifyFieldPattern(samples []string) string {
	if len(samples) == 0 {
		return ""
	}

	hasIPv4 := false
	hasNumeric := false
	allUpperOrShort := true

	for _, s := range samples {
		// Check for IPv4 pattern
		if len(s) >= 7 && len(s) <= 45 {
			dotCount := 0
			for _, c := range s {
				if c == '.' {
					dotCount++
				}
			}
			if dotCount == 3 {
				hasIPv4 = true
			}
		}

		// Check numeric
		isNum := len(s) > 0
		for _, c := range s {
			if (c < '0' || c > '9') && c != '.' && c != '-' {
				isNum = false
				break
			}
		}
		if isNum {
			hasNumeric = true
		}

		// Check if values look like enums (short uppercase or mixed)
		if len(s) > 20 {
			allUpperOrShort = false
		}
	}

	switch {
	case hasIPv4:
		return "ip_address"
	case hasNumeric:
		return "numeric"
	case allUpperOrShort && len(samples) > 0:
		// Check if all samples are very short, suggesting enum-like values
		maxLen := 0
		for _, s := range samples {
			if len(s) > maxLen {
				maxLen = len(s)
			}
		}
		if maxLen <= 20 {
			return "enum"
		}
		return "text"
	default:
		return "text"
	}
}

// searchAlerts searches alert detection rules by name/description for the given fractal.
// Returns matching alerts with their BQL queries, useful as detection examples.
// If search is empty, returns all alerts for the fractal.
func (m *Manager) searchAlerts(ctx context.Context, fractalID string, search string) (interface{}, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var rows *sql.Rows
	var err error

	if search == "" {
		rows, err = m.pg.Query(queryCtx, `
			SELECT a.name, a.description, a.query_string, a.alert_type,
			       a.labels, a.enabled, a.feed_id IS NOT NULL AS is_feed_alert,
			       COALESCE(f.name, '') AS feed_name
			FROM alerts a
			LEFT JOIN alert_feeds f ON f.id = a.feed_id
			WHERE a.fractal_id = $1
			ORDER BY a.enabled DESC, a.name ASC
			LIMIT 20
		`, fractalID)
	} else {
		pattern := "%" + search + "%"
		rows, err = m.pg.Query(queryCtx, `
			SELECT a.name, a.description, a.query_string, a.alert_type,
			       a.labels, a.enabled, a.feed_id IS NOT NULL AS is_feed_alert,
			       COALESCE(f.name, '') AS feed_name
			FROM alerts a
			LEFT JOIN alert_feeds f ON f.id = a.feed_id
			WHERE a.fractal_id = $1
			  AND (a.name ILIKE $2 OR a.description ILIKE $2)
			ORDER BY a.enabled DESC, a.name ASC
			LIMIT 20
		`, fractalID, pattern)
	}
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	type alertResult struct {
		Name        string   `json:"name"`
		Description string   `json:"description,omitempty"`
		Query       string   `json:"query"`
		AlertType   string   `json:"alert_type"`
		Labels      []string `json:"labels,omitempty"`
		Enabled     bool     `json:"enabled"`
		IsFeedAlert bool     `json:"is_feed_alert"`
		FeedName    string   `json:"feed_name,omitempty"`
	}

	var alerts []alertResult
	for rows.Next() {
		var a alertResult
		var desc sql.NullString
		if err := rows.Scan(&a.Name, &desc, &a.Query, &a.AlertType, pq.Array(&a.Labels), &a.Enabled, &a.IsFeedAlert, &a.FeedName); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		if desc.Valid {
			a.Description = desc.String
		}
		if !a.IsFeedAlert {
			a.FeedName = ""
		}
		alerts = append(alerts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return map[string]interface{}{
		"alerts": alerts,
		"count":  len(alerts),
	}, nil
}

// executeQuery runs a BQL query against ClickHouse for the given fractal.
func (m *Manager) executeQuery(ctx context.Context, fractalID string, args runQueryArgs) (interface{}, error) {
	end := time.Now()
	start := end.Add(-24 * time.Hour)

	// If explicit start/end times are provided, use them
	if args.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, args.StartTime); err == nil {
			start = t
		}
	}
	if args.EndTime != "" {
		if t, err := time.Parse(time.RFC3339, args.EndTime); err == nil {
			end = t
		}
	}

	// Relative time_range overrides defaults but not explicit start/end
	if args.StartTime == "" {
		switch args.TimeRange {
		case "5m":
			start = end.Add(-5 * time.Minute)
		case "15m":
			start = end.Add(-15 * time.Minute)
		case "1h":
			start = end.Add(-1 * time.Hour)
		case "6h":
			start = end.Add(-6 * time.Hour)
		case "12h":
			start = end.Add(-12 * time.Hour)
		case "7d":
			start = end.Add(-7 * 24 * time.Hour)
		case "30d":
			start = end.Add(-30 * 24 * time.Hour)
		case "all":
			start = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		}
	}

	queryStr := args.Query

	pipeline, err := parser.ParseQuery(queryStr)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	includeEmptyFractalID := false
	if m.fractalManager != nil {
		if defaultFractal, err := m.fractalManager.GetDefaultFractal(ctx); err == nil && defaultFractal.ID == fractalID {
			includeEmptyFractalID = true
		}
	}

	tableName := "logs"
	if m.ch != nil && m.ch.IsCluster() {
		tableName = "logs_distributed"
	}
	opts := parser.QueryOptions{
		StartTime:             start,
		EndTime:               end,
		MaxRows:               20,
		FractalID:             fractalID,
		IncludeEmptyFractalID: includeEmptyFractalID,
		TableName:             tableName,
	}

	result, err := parser.TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	rows, err := m.ch.Query(queryCtx, result.SQL)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	// Cap results for chat context
	isTruncated := len(rows) > 20
	if isTruncated {
		rows = rows[:20]
	}

	// Post-filter: if the LLM provided a post_filter keyword, only keep rows
	// where any field value contains that substring (case-insensitive).
	// This reduces context size when the query is broad but the LLM wants specific rows.
	postFiltered := 0
	if args.PostFilter != "" {
		needle := strings.ToLower(args.PostFilter)
		var filtered []map[string]interface{}
		for _, row := range rows {
			for _, v := range row {
				s, ok := v.(string)
				if ok && strings.Contains(strings.ToLower(s), needle) {
					filtered = append(filtered, row)
					break
				}
			}
		}
		postFiltered = len(rows) - len(filtered)
		rows = filtered
	}

	resp := map[string]interface{}{
		"rows":         rows,
		"count":        len(rows),
		"field_order":  result.FieldOrder,
		"is_truncated": isTruncated,
	}
	if postFiltered > 0 {
		resp["post_filtered_out"] = postFiltered
	}
	return resp, nil
}

// getRecentSuccessfulQueries extracts unique BQL queries from recent assistant
// tool_calls across all conversations in this fractal. Only includes queries whose
// subsequent tool result returned >0 rows.
func (m *Manager) getRecentSuccessfulQueries(ctx context.Context, fractalID, currentConvID string) []string {
	// Get recent assistant tool_calls paired with their tool result content.
	// Scoped to recent conversations (last 10) in this fractal for performance.
	rows, err := m.pg.Query(ctx, `
		WITH recent_convs AS (
			SELECT id FROM chat_conversations
			WHERE fractal_id = $1
			ORDER BY updated_at DESC
			LIMIT 10
		)
		SELECT m.tool_calls, next_msg.content AS tool_content
		FROM chat_messages m
		JOIN recent_convs c ON c.id = m.conversation_id
		LEFT JOIN LATERAL (
			SELECT content FROM chat_messages
			WHERE conversation_id = m.conversation_id
			  AND role = 'tool'
			  AND created_at > m.created_at
			ORDER BY created_at ASC
			LIMIT 1
		) next_msg ON true
		WHERE m.role = 'assistant'
		  AND m.tool_calls IS NOT NULL
		  AND m.tool_calls != '[]'::jsonb
		ORDER BY m.created_at DESC
		LIMIT 20
	`, fractalID)
	if err != nil {
		log.Printf("[Chat] failed to get recent queries: %v", err)
		return nil
	}
	defer rows.Close()

	seen := make(map[string]bool)
	var queries []string
	for rows.Next() {
		var toolCallsRaw []byte
		var toolContent sql.NullString
		if err := rows.Scan(&toolCallsRaw, &toolContent); err != nil {
			continue
		}

		// Check if the tool result had >0 rows
		hasResults := false
		if toolContent.Valid {
			var resultData map[string]interface{}
			if err := json.Unmarshal([]byte(toolContent.String), &resultData); err == nil {
				if count, ok := resultData["count"].(float64); ok && count > 0 {
					hasResults = true
				}
			}
		}
		if !hasResults {
			continue
		}

		var toolCalls []llmToolCall
		if err := json.Unmarshal(toolCallsRaw, &toolCalls); err != nil {
			continue
		}
		for _, tc := range toolCalls {
			if tc.Function.Name != "run_query" {
				continue
			}
			var args runQueryArgs
			if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
				continue
			}
			if args.Query == "" || seen[args.Query] {
				continue
			}
			seen[args.Query] = true
			queries = append(queries, args.Query)
			if len(queries) >= 5 {
				return queries
			}
		}
	}
	return queries
}

// buildSystemPrompt constructs the system prompt for the LLM.
func (m *Manager) buildSystemPrompt(fractal *fractals.Fractal, recentQueries []string, customInstructions string, normalizerHints string) string {
	prompt := fmt.Sprintf(`You are an intelligent assistant embedded in the Bifract log management and collaboration platform.

You are currently analyzing the fractal named "%s" (ID: %s).

Your tools:
1. get_fields - Discover available field names with sample values, cardinality, and value patterns. Call this ONCE at the start of a new conversation.
2. run_query - Execute BQL queries against the fractal's logs.
3. validate_bql - Validate a BQL query without executing it. Returns parse errors if invalid.
4. search_alerts - Search existing alert detection rules. Returns alert names, BQL queries, type, and labels.
5. think - Plan your next step and reason about findings so far. Use this to build multi-step investigations: analyze what you have found, identify gaps, and decide what to query next. The user sees this as a collapsible "thinking" block.
6. render_chart - Render a standalone chart (bar, line, or pie) inline in the chat. Use SELECTIVELY.
7. present_results - Optional structured report for presenting significant findings. Supports inline charts. For simple answers, just respond with plain text.

CRITICAL RULES:
- The user CAN see your plain text responses. For simple answers, just respond naturally.
- Use present_results ONLY for significant findings: security issues, notable data patterns, complex analysis.
- You have a maximum of 15 tool call rounds. For simple questions, 2-3 rounds suffice. For complex investigations, use as many as needed.
- In your FIRST round, call get_fields AND search_alerts together (parallel tool calls). Only skip search_alerts if the question is non-security.
- When the user asks about threats, detections, hunting, or anything security-related, ALWAYS use search_alerts first.
- Before running a complex query, use validate_bql to check the syntax first.

MULTI-STEP INVESTIGATIONS:
- For complex questions (threat hunting, anomaly detection, incident investigation), use the think tool to plan and iterate.
- Build on your results: after each query, use think to analyze what you found and decide what to query next. Reference specific data points from previous results.
- Example flow: get_fields + search_alerts -> run_query (find suspicious users) -> think (analyze results, plan next step) -> run_query (check their login sources) -> think (correlate findings) -> run_query (check for lateral movement) -> present_results.
- Do not artificially limit yourself for complex questions. Follow the investigation thread as deep as the data leads you.
- For simple questions, skip think and answer directly in 2-3 rounds.

Chart guidelines: Use charts ONLY when they add genuine insight. Good uses: distributions (pie), comparisons (bar), trends (line). Do NOT chart single values or tiny datasets. You can include a chart inside present_results using the chart field, or use render_chart for a standalone chart. Keep labels to 15 or fewer.

When the user asks about data, use run_query to fetch real data rather than making assumptions.
The time range for all queries is controlled by the user via a UI selector. Do NOT set time_range, start_time, or end_time in run_query. Just provide the query string.

present_results guidelines:
- summary: 1-3 concise sentences. No markdown, no headers, no bullet points.
- findings: Use for notable data points (counts, top values, comparisons). Omit for simple answers.
- severity: "info" for general responses, "warning" for anomalies worth attention, "critical" for urgent security issues.
- chart: Optional. Include chart data directly in the report to combine visuals with findings in one block.

%s`, fractal.Name, fractal.ID, bqlSyntaxRef)

	if normalizerHints != "" {
		prompt += "\n" + normalizerHints
	}

	if len(recentQueries) > 0 {
		prompt += "\nRecent successful queries in this fractal (use these as examples of valid syntax and available fields):\n"
		for _, q := range recentQueries {
			prompt += "  " + q + "\n"
		}
	}

	if customInstructions != "" {
		prompt += "\n\nADDITIONAL INSTRUCTIONS FROM THE USER (follow these carefully):\n" + customInstructions + "\n"
	}

	return prompt
}

// getNormalizerHints builds a context string describing active normalizer field mappings.
// This helps the AI understand canonical field names and what source fields map to them.
func (m *Manager) getNormalizerHints(ctx context.Context) string {
	if m.normalizerManager == nil {
		return ""
	}

	norms, err := m.normalizerManager.List(ctx)
	if err != nil || len(norms) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("FIELD MAPPING CONTEXT (from normalizers):\n")
	b.WriteString("Ingested logs are processed by normalizers that rename/map fields. Use the TARGET field names in your queries.\n")

	for _, n := range norms {
		if len(n.FieldMappings) == 0 {
			continue
		}
		label := n.Name
		if n.IsDefault {
			label += " [DEFAULT]"
		}
		b.WriteString(fmt.Sprintf("\nNormalizer: %s\n", label))
		for _, fm := range n.FieldMappings {
			b.WriteString(fmt.Sprintf("  %s <- %s\n", fm.Target, strings.Join(fm.Sources, ", ")))
		}
	}

	return b.String()
}

// trimHistory limits conversation history to avoid exceeding the LLM context window.
func (m *Manager) trimHistory(history []*Message) []*Message {
	const maxMessages = 20
	const maxToolContentLen = 500

	// Cap message count, ensuring we start at a 'user' boundary
	// to avoid orphaned tool results that reference a trimmed assistant message.
	if len(history) > maxMessages {
		history = history[len(history)-maxMessages:]
		for i, msg := range history {
			if msg.Role == "user" {
				history = history[i:]
				break
			}
		}
	}

	for _, msg := range history {
		// Trim large tool result content to just metadata
		if msg.Role == "tool" && len(msg.Content) > maxToolContentLen {
			var result map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Content), &result); err == nil {
				summary := map[string]interface{}{
					"count":        result["count"],
					"field_order":  result["field_order"],
					"is_truncated": result["is_truncated"],
					"note":         "Row data omitted from history.",
				}
				if b, err := json.Marshal(summary); err == nil {
					msg.Content = string(b)
				}
			}
		}

	}

	// Validate tool_use/tool_result pairing:
	// Every assistant message with tool_calls must be followed by tool result messages.
	// Strip tool_calls from any assistant message that lacks subsequent tool results.
	for i := 0; i < len(history); i++ {
		msg := history[i]
		if msg.Role != "assistant" || msg.ToolCalls == nil {
			continue
		}
		// Check if next message(s) are tool results
		hasToolResults := i+1 < len(history) && history[i+1].Role == "tool"
		if !hasToolResults {
			// Orphaned tool_calls: strip them to avoid API errors
			msg.ToolCalls = nil
		}
	}

	return history
}

// capToolResultForContext truncates large tool results before adding them to the
// LLM context. The full result is already sent to the frontend via SSE, so the
// LLM only needs enough to reason about what was returned.
func capToolResultForContext(resultJSON []byte) string {
	const maxLen = 4000

	if len(resultJSON) <= maxLen {
		return string(resultJSON)
	}

	// Try to parse as a query result with rows and replace rows with a summary
	var result map[string]interface{}
	if err := json.Unmarshal(resultJSON, &result); err == nil {
		if rows, ok := result["rows"].([]interface{}); ok {
			// Keep first 3 rows as samples so the LLM can see field structure
			sampleRows := rows
			if len(sampleRows) > 3 {
				sampleRows = sampleRows[:3]
			}
			summary := map[string]interface{}{
				"count":        result["count"],
				"field_order":  result["field_order"],
				"is_truncated": true,
				"sample_rows":  sampleRows,
				"note":         fmt.Sprintf("Full result had %d rows. Only 3 sample rows included to save context. Analyze what you have and present findings.", len(rows)),
			}
			if b, err := json.Marshal(summary); err == nil {
				// If even the summary with 3 sample rows is too large, drop to 1
				if len(b) > maxLen {
					if len(sampleRows) > 1 {
						summary["sample_rows"] = sampleRows[:1]
						summary["note"] = fmt.Sprintf("Full result had %d rows. Only 1 sample row included (rows are large). Analyze what you have and present findings.", len(rows))
					}
					if b2, err := json.Marshal(summary); err == nil {
						// If still too large, drop all sample rows
						if len(b2) > maxLen {
							summary["sample_rows"] = nil
							summary["note"] = fmt.Sprintf("Full result had %d rows but each row is very large. Row data omitted to save context. Use the field_order to know what columns exist.", len(rows))
							if b3, err := json.Marshal(summary); err == nil {
								return string(b3)
							}
						}
						return string(b2)
					}
				}
				return string(b)
			}
		}
	}

	// Fallback: hard truncate
	return string(resultJSON[:maxLen]) + "...(truncated)"
}

// buildLLMMessages converts stored messages into the LiteLLM message format.
func (m *Manager) buildLLMMessages(systemPrompt string, history []*Message) []llmMessage {
	msgs := []llmMessage{
		{Role: "system", Content: systemPrompt},
	}
	for _, h := range history {
		msg := llmMessage{
			Role:    h.Role,
			Content: h.Content,
		}
		if h.ToolCalls != nil {
			msg.ToolCalls = h.ToolCalls
		}
		// Restore tool_call_id and name for tool result messages
		if h.Role == "tool" && h.ToolResults != nil {
			var results []map[string]interface{}
			if err := json.Unmarshal(h.ToolResults, &results); err == nil && len(results) > 0 {
				if tcID, ok := results[0]["tool_call_id"].(string); ok {
					msg.ToolCallID = tcID
				}
				if name, ok := results[0]["tool_name"].(string); ok {
					msg.Name = name
				}
			}
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// toolDefinitions returns the tool definitions passed to LiteLLM.
func (m *Manager) toolDefinitions() []llmTool {
	return []llmTool{
		{
			Type: "function",
			Function: llmFunction{
				Name:        "run_query",
				Description: "Execute a BQL query against the current fractal's logs. The time range is controlled by the user's UI selection. Just provide the query string.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"query": map[string]interface{}{
							"type":        "string",
							"description": "The BQL query string. Must start with a filter (e.g. 'field=*'). Use groupby() for aggregation. Examples: 'timestamp=* | head(5)', 'event_id=* | groupby(event_id)', 'user=*admin* | count()'",
						},
						"post_filter": map[string]interface{}{
							"type":        "string",
							"description": "Optional keyword to filter results AFTER the query runs. Only rows where any field value contains this substring (case-insensitive) are kept. Use this to reduce large result sets to relevant rows. Example: query broad logs with 'level=*' but set post_filter='error' to only see error rows in the response.",
						},
					},
					"required": []string{"query"},
				},
			},
		},
		{
			Type: "function",
			Function: llmFunction{
				Name:        "get_fields",
				Description: "Discover available fields in the fractal's logs with sample values, cardinality, and value patterns. Returns the top 30 fields ranked by frequency. Each field includes: name, count, cardinality (number of unique values), samples (up to 5 example values), and pattern (ip_address, numeric, enum, or text). Use this to understand what values a field contains before writing queries. Call this FIRST in new conversations.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"filter": map[string]interface{}{
							"type":        "string",
							"description": "Optional keyword to filter field names (case-insensitive substring match). Use when looking for specific field types like 'ip', 'user', 'host', etc.",
						},
					},
				},
			},
		},
		{
			Type: "function",
			Function: llmFunction{
				Name:        "validate_bql",
				Description: "Validate a BQL query string without executing it. Returns whether the syntax is valid and any parse errors. Use this to check complex queries before running them to avoid wasting a tool round on a syntax error. Zero cost (no database query).",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"query": map[string]interface{}{
							"type":        "string",
							"description": "The BQL query string to validate.",
						},
					},
					"required": []string{"query"},
				},
			},
		},
		{
			Type: "function",
			Function: llmFunction{
				Name:        "search_alerts",
				Description: "Search existing alert detection rules by name or description. Returns alert names, their BQL queries, type, and labels. Call with no search term to list all alerts, or with a search term to filter. Use this to find detection examples, learn which fields are used in real detections, discover query patterns, or find alerts relevant to a threat hunt.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"search": map[string]interface{}{
							"type":        "string",
							"description": "Optional search term to filter alert names and descriptions (case-insensitive). Omit to list all alerts. Examples: 'brute force', 'powershell', 'lateral movement'.",
						},
					},
				},
			},
		},
		{
			Type: "function",
			Function: llmFunction{
				Name:        "think",
				Description: "Plan your next step and reason about findings so far. Use this to build a multi-step investigation: analyze what you have learned, identify gaps, and decide what to query next. The user sees this as a collapsible thinking block. Call this between queries when you need to correlate findings, pivot your approach, or plan a deeper investigation. Each think call does not count against your query budget.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"reasoning": map[string]interface{}{
							"type":        "string",
							"description": "Your analysis of findings so far and plan for the next step. Reference specific data from previous query results. Example: 'User admin had 342 failed logins from 5 unique IPs. Next I should check if any of those IPs successfully authenticated, then look for process execution from those sessions.'",
						},
					},
					"required": []string{"reasoning"},
				},
			},
		},
		{
			Type: "function",
			Function: llmFunction{
				Name:        "render_chart",
				Description: "Render a standalone chart inline in the chat. Use ONLY when a visual genuinely helps: distributions (pie), comparisons (bar), or trends over time (line). Do NOT use for simple counts or tiny datasets. For charts combined with a written report, use present_results with its chart field instead.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"chart_type": map[string]interface{}{
							"type":        "string",
							"enum":        []string{"bar", "line", "pie"},
							"description": "Chart type. Use 'bar' for comparing categories (e.g. top users, event types). Use 'line' for trends over time (e.g. hourly counts). Use 'pie' for showing proportions of a whole.",
						},
						"title": map[string]interface{}{
							"type":        "string",
							"description": "Short chart title describing what is shown (e.g. 'Events by Source IP', 'Hourly Error Rate').",
						},
						"labels": map[string]interface{}{
							"type":        "array",
							"items":       map[string]interface{}{"type": "string"},
							"description": "X-axis labels (categories for bar/pie, time buckets for line). Keep to 15 or fewer for readability.",
						},
						"datasets": map[string]interface{}{
							"type": "array",
							"items": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"label": map[string]interface{}{"type": "string", "description": "Series name (e.g. 'Count', 'Error Rate'). For single-series charts use a descriptive name."},
									"data":  map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "number"}, "description": "Numeric values, one per label. Must have the same length as labels."},
								},
								"required": []string{"label", "data"},
							},
							"description": "One or more data series. Use multiple datasets for multi-line charts comparing series.",
						},
					},
					"required": []string{"chart_type", "title", "labels", "datasets"},
				},
			},
		},
		{
			Type: "function",
			Function: llmFunction{
				Name:        "present_results",
				Description: "Present a structured report with findings to the user. Use this when you have significant findings, security issues, or complex analysis worth highlighting. For simple conversational answers, just respond with plain text instead. Supports an optional inline chart.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"summary": map[string]interface{}{
							"type":        "string",
							"description": "A concise paragraph summarizing your analysis or answer. Keep it to 1-3 sentences. No markdown headers, bullet points, or numbered lists.",
						},
						"findings": map[string]interface{}{
							"type":        "array",
							"description": "Optional key findings displayed as a label-value table. Use for notable data points, counts, or comparisons.",
							"items": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"label": map[string]interface{}{"type": "string", "description": "Short label (e.g. 'Total Events', 'Top Source IP', 'Error Rate')"},
									"value": map[string]interface{}{"type": "string", "description": "The value or data point"},
								},
								"required": []string{"label", "value"},
							},
						},
						"severity": map[string]interface{}{
							"type":        "string",
							"enum":        []string{"info", "warning", "critical"},
							"description": "Severity level. Use 'info' for general findings, 'warning' for anomalies worth attention, 'critical' for urgent security issues.",
						},
						"chart": map[string]interface{}{
							"type":        "object",
							"description": "Optional inline chart to include in the report. Same structure as render_chart.",
							"properties": map[string]interface{}{
								"chart_type": map[string]interface{}{
									"type": "string",
									"enum": []string{"bar", "line", "pie"},
								},
								"title": map[string]interface{}{
									"type": "string",
								},
								"labels": map[string]interface{}{
									"type":  "array",
									"items": map[string]interface{}{"type": "string"},
								},
								"datasets": map[string]interface{}{
									"type": "array",
									"items": map[string]interface{}{
										"type": "object",
										"properties": map[string]interface{}{
											"label": map[string]interface{}{"type": "string"},
											"data":  map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "number"}},
										},
										"required": []string{"label", "data"},
									},
								},
							},
						},
					},
					"required": []string{"summary"},
				},
			},
		},
	}
}

// sendSSEEvent writes a single SSE data line.
func sendSSEEvent(w io.Writer, flusher http.Flusher, event StreamEvent) {
	data, _ := json.Marshal(event)
	fmt.Fprintf(w, "data: %s\n\n", data)
	if flusher != nil {
		flusher.Flush()
	}
}

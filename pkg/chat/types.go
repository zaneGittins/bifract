package chat

import (
	"encoding/json"
	"time"
)

// Conversation is a named LLM chat session scoped to a fractal.
type Conversation struct {
	ID            string    `json:"id"`
	FractalID     string    `json:"fractal_id"`
	Title         string    `json:"title"`
	InstructionID *string   `json:"instruction_id"`
	CreatedBy     string    `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Message is a single turn in a conversation.
type Message struct {
	ID             string          `json:"id"`
	ConversationID string          `json:"conversation_id"`
	Role           string          `json:"role"` // "user", "assistant", "tool"
	Content        string          `json:"content"`
	ToolCalls      json.RawMessage `json:"tool_calls,omitempty"`
	ToolResults    json.RawMessage `json:"tool_results,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// StreamEvent is sent over SSE to the frontend.
type StreamEvent struct {
	Type       string      `json:"type"` // "token", "tool_call", "tool_result", "think", "chart", "present", "error", "title", "done"
	Content    string      `json:"content,omitempty"`
	ToolName   string      `json:"tool_name,omitempty"`
	ToolArgs   interface{} `json:"tool_args,omitempty"`
	ToolResult interface{} `json:"tool_result,omitempty"`
}

// Instruction is a reusable system prompt configuration scoped to a fractal.
type Instruction struct {
	ID        string    `json:"id"`
	FractalID string    `json:"fractal_id"`
	Name      string    `json:"name"`
	Content   string    `json:"content"`
	IsDefault bool      `json:"is_default"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// LiteLLM API types

type llmMessage struct {
	Role       string          `json:"role"`
	Content    interface{}     `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	Name       string          `json:"name,omitempty"`
}

type llmToolCall struct {
	Index    int             `json:"index"`
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Function llmToolFunction `json:"function"`
}

type llmToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type llmRequest struct {
	Model      string       `json:"model"`
	Messages   []llmMessage `json:"messages"`
	Tools      []llmTool    `json:"tools"`
	Stream     bool         `json:"stream"`
	ToolChoice string       `json:"tool_choice,omitempty"`
}

type llmTool struct {
	Type     string      `json:"type"`
	Function llmFunction `json:"function"`
}

type llmFunction struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Parameters  interface{} `json:"parameters"`
}

type llmStreamChunk struct {
	Choices []llmStreamChoice `json:"choices"`
}

type llmStreamChoice struct {
	Delta        llmDelta `json:"delta"`
	FinishReason string   `json:"finish_reason"`
}

type llmDelta struct {
	Role      string          `json:"role"`
	Content   *string         `json:"content"`
	ToolCalls []llmToolCall   `json:"tool_calls,omitempty"`
}

type runQueryArgs struct {
	Query      string `json:"query"`
	TimeRange  string `json:"time_range,omitempty"`
	StartTime  string `json:"start_time,omitempty"`
	EndTime    string `json:"end_time,omitempty"`
	PostFilter string `json:"post_filter,omitempty"`
}

type presentResultsArgs struct {
	Summary  string            `json:"summary"`
	Findings []presentFinding  `json:"findings,omitempty"`
	Severity string            `json:"severity,omitempty"` // "info", "warning", "critical"
	Chart    *renderChartArgs  `json:"chart,omitempty"`
}

type presentFinding struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type renderChartArgs struct {
	ChartType string           `json:"chart_type"` // "bar", "line", "pie"
	Title     string           `json:"title"`
	Labels    []string         `json:"labels"`
	Datasets  []chartDataset   `json:"datasets"`
}

type chartDataset struct {
	Label string    `json:"label"`
	Data  []float64 `json:"data"`
}

package chat

import (
	"encoding/json"
	"time"

	"bifract/pkg/llm"
)

// Conversation is a named LLM chat session scoped to a fractal or prism.
type Conversation struct {
	ID            string    `json:"id"`
	FractalID     string    `json:"fractal_id,omitempty"`
	PrismID       string    `json:"prism_id,omitempty"`
	Title         string    `json:"title"`
	InstructionID *string   `json:"instruction_id,omitempty"` // legacy, kept for backward compat during migration
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
	Type       string      `json:"type"` // "token", "tool_call", "tool_result", "tool_confirm", "think", "chart", "present", "error", "title", "done"
	Content    string      `json:"content,omitempty"`
	ToolName   string      `json:"tool_name,omitempty"`
	ToolArgs   interface{} `json:"tool_args,omitempty"`
	ToolResult interface{} `json:"tool_result,omitempty"`
	// ToolCallID ties a result to the call it answers. The model may batch
	// several calls in one round and they are not dispatched in the order they
	// were announced, so position is not enough to pair them up.
	ToolCallID string `json:"tool_call_id,omitempty"`
	// PendingID identifies a tool call waiting on the user, for the request that
	// answers it. It is the only thing the browser sends back: the arguments stay
	// on the server so what was shown is what runs.
	PendingID string `json:"pending_id,omitempty"`
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

// The model wire format is OpenAI-shaped; tool calls are stored in it.
type (
	llmMessage      = llm.Message
	llmToolCall     = llm.ToolCall
	llmToolFunction = llm.FunctionCall
	llmTool         = llm.Tool
	llmFunction     = llm.Function
)

type presentResultsArgs struct {
	Summary  string           `json:"summary"`
	Findings []presentFinding `json:"findings,omitempty"`
	Severity string           `json:"severity,omitempty"` // "info", "warning", "critical"
	Chart    *renderChartArgs `json:"chart,omitempty"`
}

type presentFinding struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type renderChartArgs struct {
	ChartType string         `json:"chart_type"` // "bar", "line", "pie"
	Title     string         `json:"title"`
	Labels    []string       `json:"labels"`
	Datasets  []chartDataset `json:"datasets"`
}

type chartDataset struct {
	Label string    `json:"label"`
	Data  []float64 `json:"data"`
}

package notebooks

import (
	"encoding/json"
	"time"

	"bifract/pkg/storage"
)

// Notebook represents a notebook document with metadata
type Notebook struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`

	// Time range settings
	TimeRangeType  string     `json:"time_range_type"`            // '1h', '24h', '7d', '30d', 'custom'
	TimeRangeStart *time.Time `json:"time_range_start,omitempty"` // For custom ranges
	TimeRangeEnd   *time.Time `json:"time_range_end,omitempty"`   // For custom ranges

	// Settings
	MaxResultsPerSection int `json:"max_results_per_section"`

	// Timezone is the IANA zone this notebook's calendar-aligned buckets snap
	// to. It lives on the notebook, not the viewer, because a query section's
	// results are cached and read by everyone.
	Timezone string `json:"timezone"`

	// Multi-tenant and ownership
	FractalID string          `json:"fractal_id"`
	Variables json.RawMessage `json:"variables"`
	CreatedBy string          `json:"created_by"`

	// Author metadata
	AuthorDisplayName     string `json:"author_display_name"`
	AuthorGravatarColor   string `json:"author_gravatar_color"`
	AuthorGravatarInitial string `json:"author_gravatar_initial"`

	// Timestamps
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Optional: sections will be loaded separately but can be included in responses
	Sections []NotebookSection `json:"sections,omitempty"`
}

// NotebookSection represents a section within a notebook
type NotebookSection struct {
	ID         string `json:"id"`
	NotebookID string `json:"notebook_id"`

	// Section metadata
	SectionType     string  `json:"section_type"` // 'markdown', 'query', 'ai_summary', 'comment_context', or 'ai_attack_chain'
	Title           *string `json:"title,omitempty"`
	Content         string  `json:"content"`
	RenderedContent *string `json:"rendered_content,omitempty"` // For cached markdown
	OrderIndex      int     `json:"order_index"`

	// Query section specific fields
	LastExecutedAt *time.Time      `json:"last_executed_at,omitempty"`
	LastResults    json.RawMessage `json:"last_results,omitempty"`
	ChartType      *string         `json:"chart_type,omitempty"`
	ChartConfig    json.RawMessage `json:"chart_config,omitempty"`

	// Tags for filtering/grouping sections
	Tags []string `json:"tags,omitempty"`

	// EventTime is when the section's subject happened, which is what a
	// chronological reading of the notebook orders by. Nil for sections with no
	// single point in time.
	EventTime *time.Time `json:"event_time,omitempty"`

	// Timestamps
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// NotebookPresence represents a user's presence in a notebook
type NotebookPresence struct {
	NotebookID string    `json:"notebook_id"`
	Username   string    `json:"username"`
	LastSeenAt time.Time `json:"last_seen_at"`

	// User metadata for display
	UserDisplayName     string `json:"user_display_name"`
	UserGravatarColor   string `json:"user_gravatar_color"`
	UserGravatarInitial string `json:"user_gravatar_initial"`
}

// CreateNotebookRequest represents the request to create a new notebook
type CreateNotebookRequest struct {
	Name                 string     `json:"name"`
	Description          string     `json:"description"`
	TimeRangeType        string     `json:"time_range_type"`
	TimeRangeStart       *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd         *time.Time `json:"time_range_end,omitempty"`
	MaxResultsPerSection int        `json:"max_results_per_section"`
	Timezone             string     `json:"timezone,omitempty"`
}

// UpdateNotebookRequest represents the request to update notebook metadata
type UpdateNotebookRequest struct {
	Name                 *string    `json:"name,omitempty"`
	Description          *string    `json:"description,omitempty"`
	TimeRangeType        *string    `json:"time_range_type,omitempty"`
	TimeRangeStart       *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd         *time.Time `json:"time_range_end,omitempty"`
	MaxResultsPerSection *int       `json:"max_results_per_section,omitempty"`
	Timezone             *string    `json:"timezone,omitempty"`
}

// CreateSectionRequest represents the request to create a new section
type CreateSectionRequest struct {
	SectionType string     `json:"section_type"` // 'markdown', 'query', 'ai_summary', 'comment_context', or 'ai_attack_chain'
	Title       *string    `json:"title,omitempty"`
	Content     string     `json:"content"`
	OrderIndex  int        `json:"order_index"`
	Tags        []string   `json:"tags,omitempty"`
	EventTime   *time.Time `json:"event_time,omitempty"`
	// Append places the section after the notebook's current last one, so a
	// client adding to a notebook it has not loaded does not have to guess an
	// order index and collide with a concurrent writer.
	Append bool `json:"append,omitempty"`
}

// UpdateSectionRequest represents the request to update a section
type UpdateSectionRequest struct {
	Title       *string     `json:"title,omitempty"`
	Content     *string     `json:"content,omitempty"`
	ChartConfig interface{} `json:"chart_config,omitempty"`
	Tags        *[]string   `json:"tags,omitempty"`
	EventTime   *time.Time  `json:"event_time,omitempty"`
	// ClearEventTime removes the section's event time. Separate from EventTime
	// because an absent field means "leave it alone".
	ClearEventTime bool `json:"clear_event_time,omitempty"`
}

// NotebookSummary is a notebook plus its section outline, without any cached
// query results. It backs the search page's notebook rail.
type NotebookSummary struct {
	ID            string                           `json:"id"`
	Name          string                           `json:"name"`
	Description   string                           `json:"description"`
	TimeRangeType string                           `json:"time_range_type"`
	Timezone      string                           `json:"timezone"`
	FractalID     string                           `json:"fractal_id,omitempty"`
	PrismID       string                           `json:"prism_id,omitempty"`
	CanEdit       bool                             `json:"can_edit"`
	SectionCount  int                              `json:"section_count"`
	Counts        map[string]int                   `json:"counts"`
	Sections      []storage.NotebookSectionSummary `json:"sections"`
	UpdatedAt     time.Time                        `json:"updated_at"`
}

// ReorderSectionsRequest represents the request to reorder sections
type ReorderSectionsRequest struct {
	SectionOrder []string `json:"section_order"` // Array of section IDs in new order
}

// ExecuteQueryRequest represents the request to execute a query section
type ExecuteQueryRequest struct {
	// Optional: override notebook time range settings
	TimeRangeType  *string    `json:"time_range_type,omitempty"`
	TimeRangeStart *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd   *time.Time `json:"time_range_end,omitempty"`
}

// NotebookListResponse represents the paginated response for notebook listing
type NotebookListResponse struct {
	Notebooks []Notebook `json:"notebooks"`
	Total     int        `json:"total"`
	Limit     int        `json:"limit"`
	Offset    int        `json:"offset"`
}

// UpdateVariablesRequest saves notebook variables
type UpdateVariablesRequest struct {
	Variables json.RawMessage `json:"variables"`
}

// GenerateFromCommentsRequest represents the request to generate a notebook from tagged comments.
type GenerateFromCommentsRequest struct {
	Tag         string `json:"tag"`
	AttackChain bool   `json:"attack_chain"`
	// Overwrite acknowledges that an existing notebook of the same name will be
	// replaced. Without it a name collision returns 409 and changes nothing.
	Overwrite bool `json:"overwrite"`
}

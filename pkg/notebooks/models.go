package notebooks

import (
	"encoding/json"
	"time"

	"bifract/pkg/storage"
)

// NotebookWithSections is a notebook and everything in it, in one response.
//
// It embeds the storage type rather than restating its fields: this endpoint
// used to hand-copy them into a mirrored struct, which silently dropped every
// column added afterwards, locked_at, external_ref_url and comment_id among
// them, with nothing failing to say so.
type NotebookWithSections struct {
	storage.Notebook
	Sections []storage.NotebookSection `json:"sections"`
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
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	// ExternalRefURL and ExternalRefLabel point at the case that owns this
	// investigation elsewhere. Empty strings clear the link.
	ExternalRefURL       *string    `json:"external_ref_url,omitempty"`
	ExternalRefLabel     *string    `json:"external_ref_label,omitempty"`
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

// FileEvidenceRequest files comments that already exist into a notebook.
type FileEvidenceRequest struct {
	Evidence []FileEvidenceItem `json:"evidence"`
}

// FileEvidenceItem names one comment to file. OrderIndex places it explicitly,
// which duplicating a notebook needs to keep evidence interleaved where it was;
// omitted, the section is appended.
type FileEvidenceItem struct {
	CommentID  string `json:"comment_id"`
	OrderIndex *int   `json:"order_index,omitempty"`
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

// UpdateVariablesRequest saves notebook variables
type UpdateVariablesRequest struct {
	Variables json.RawMessage `json:"variables"`
}

// GenerateFromCommentsRequest files every comment carrying a tag into a
// notebook. Filing is additive: nothing already in the notebook is removed.
type GenerateFromCommentsRequest struct {
	Tag         string `json:"tag"`
	AttackChain bool   `json:"attack_chain"`
	// NotebookID files into an existing notebook. Empty uses the tag's own
	// notebook, created on first use.
	NotebookID string `json:"notebook_id,omitempty"`
}

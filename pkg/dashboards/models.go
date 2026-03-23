package dashboards

import (
	"encoding/json"
	"time"
)

// Dashboard represents a dashboard with a free-form grid of query widgets
type Dashboard struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`

	TimeRangeType  string     `json:"time_range_type"`
	TimeRangeStart *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd   *time.Time `json:"time_range_end,omitempty"`

	FractalID string          `json:"fractal_id"`
	Variables json.RawMessage `json:"variables"`
	CreatedBy string          `json:"created_by"`

	AuthorDisplayName     string `json:"author_display_name"`
	AuthorGravatarColor   string `json:"author_gravatar_color"`
	AuthorGravatarInitial string `json:"author_gravatar_initial"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	Widgets []DashboardWidget `json:"widgets,omitempty"`
}

// DashboardWidget represents a single query widget on a dashboard
type DashboardWidget struct {
	ID          string  `json:"id"`
	DashboardID string  `json:"dashboard_id"`
	Title       *string `json:"title,omitempty"`

	QueryContent string          `json:"query_content"`
	ChartType    string          `json:"chart_type"`
	ChartConfig  json.RawMessage `json:"chart_config,omitempty"`

	// Grid layout (units: 12-col grid, row height ~130px)
	PosX   int `json:"pos_x"`
	PosY   int `json:"pos_y"`
	Width  int `json:"width"`
	Height int `json:"height"`

	LastExecutedAt *time.Time      `json:"last_executed_at,omitempty"`
	LastResults    json.RawMessage `json:"last_results,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreateDashboardRequest is the request body for creating a dashboard
type CreateDashboardRequest struct {
	Name           string     `json:"name"`
	Description    string     `json:"description"`
	TimeRangeType  string     `json:"time_range_type"`
	TimeRangeStart *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd   *time.Time `json:"time_range_end,omitempty"`
}

// UpdateDashboardRequest is the request body for updating a dashboard
type UpdateDashboardRequest struct {
	Name           *string    `json:"name,omitempty"`
	Description    *string    `json:"description,omitempty"`
	TimeRangeType  *string    `json:"time_range_type,omitempty"`
	TimeRangeStart *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd   *time.Time `json:"time_range_end,omitempty"`
}

// CreateWidgetRequest is the request body for adding a widget
type CreateWidgetRequest struct {
	Title        *string `json:"title,omitempty"`
	QueryContent string  `json:"query_content"`
	ChartType    string  `json:"chart_type"`
	PosX         int     `json:"pos_x"`
	PosY         int     `json:"pos_y"`
	Width        int     `json:"width"`
	Height       int     `json:"height"`
}

// UpdateWidgetRequest is the request body for updating widget query/title
type UpdateWidgetRequest struct {
	Title        *string     `json:"title,omitempty"`
	QueryContent *string     `json:"query_content,omitempty"`
	ChartType    *string     `json:"chart_type,omitempty"`
	ChartConfig  interface{} `json:"chart_config,omitempty"`
}

// UpdateWidgetResultsRequest saves cached query results
type UpdateWidgetResultsRequest struct {
	LastResults    string  `json:"last_results"`
	ChartType      *string `json:"chart_type,omitempty"`
}

// UpdateWidgetLayoutRequest saves widget position/size
type UpdateWidgetLayoutRequest struct {
	PosX   int `json:"pos_x"`
	PosY   int `json:"pos_y"`
	Width  int `json:"width"`
	Height int `json:"height"`
}

// UpdateVariablesRequest saves dashboard variables
type UpdateVariablesRequest struct {
	Variables json.RawMessage `json:"variables"`
}

// Response is the standard API response envelope
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

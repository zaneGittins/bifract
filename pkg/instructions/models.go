package instructions

import "time"

// Library is a named collection of instruction pages scoped to a fractal or prism.
type Library struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Description       string     `json:"description"`
	IsDefault         bool       `json:"is_default"`
	FractalID         string     `json:"fractal_id,omitempty"`
	PrismID           string     `json:"prism_id,omitempty"`
	Source            string     `json:"source"`
	RepoURL           string     `json:"repo_url,omitempty"`
	Branch            string     `json:"branch,omitempty"`
	Path              string     `json:"path,omitempty"`
	HasAuthToken      bool       `json:"has_auth_token"`
	SyncSchedule      string     `json:"sync_schedule"`
	LastSyncedAt      *time.Time `json:"last_synced_at,omitempty"`
	LastSyncStatus    string     `json:"last_sync_status"`
	LastSyncPageCount int        `json:"last_sync_page_count"`
	CreatedBy         string     `json:"created_by"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	PageCount         int        `json:"page_count"`
}

// Page is a single instruction document within a library.
type Page struct {
	ID            string    `json:"id"`
	LibraryID     string    `json:"library_id"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	Content       string    `json:"content"`
	AlwaysInclude bool      `json:"always_include"`
	SortOrder     int       `json:"sort_order"`
	SourcePath    string    `json:"source_path,omitempty"`
	SourceHash    string    `json:"source_hash,omitempty"`
	CreatedBy     string    `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// PageSummary is a lightweight page reference used for the AI context index.
type PageSummary struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	AlwaysInclude bool   `json:"always_include"`
}

// CreateLibraryRequest is the payload for creating a new library.
type CreateLibraryRequest struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	IsDefault    bool   `json:"is_default"`
	Source       string `json:"source"`
	RepoURL      string `json:"repo_url"`
	Branch       string `json:"branch"`
	Path         string `json:"path"`
	AuthToken    string `json:"auth_token"`
	SyncSchedule string `json:"sync_schedule"`
}

// UpdateLibraryRequest is the payload for updating a library.
type UpdateLibraryRequest struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	IsDefault    bool   `json:"is_default"`
	Source       string `json:"source"`
	RepoURL      string `json:"repo_url"`
	Branch       string `json:"branch"`
	Path         string `json:"path"`
	AuthToken    string `json:"auth_token"`
	ClearToken   bool   `json:"clear_token"`
	SyncSchedule string `json:"sync_schedule"`
}

// CreatePageRequest is the payload for creating a new page.
type CreatePageRequest struct {
	Name          string `json:"name"`
	Description   string `json:"description"`
	Content       string `json:"content"`
	AlwaysInclude bool   `json:"always_include"`
	SortOrder     int    `json:"sort_order"`
}

// UpdatePageRequest is the payload for updating a page.
type UpdatePageRequest struct {
	Name          string `json:"name"`
	Description   string `json:"description"`
	Content       string `json:"content"`
	AlwaysInclude bool   `json:"always_include"`
	SortOrder     int    `json:"sort_order"`
}

// SyncResult summarizes the outcome of a library sync.
type SyncResult struct {
	Added   int      `json:"added"`
	Updated int      `json:"updated"`
	Deleted int      `json:"deleted"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors,omitempty"`
}

// Schedule constants for library sync frequency (shared with feeds).
const (
	ScheduleNever   = "never"
	ScheduleHourly  = "hourly"
	ScheduleDaily   = "daily"
	ScheduleWeekly  = "weekly"
	ScheduleMonthly = "monthly"

	SourceManual = "manual"
	SourceRepo   = "repo"
)

// ValidSchedules is the set of accepted sync schedule values.
var ValidSchedules = map[string]bool{
	ScheduleNever:   true,
	ScheduleHourly:  true,
	ScheduleDaily:   true,
	ScheduleWeekly:  true,
	ScheduleMonthly: true,
}

// ValidSources is the set of accepted library source values.
var ValidSources = map[string]bool{
	SourceManual: true,
	SourceRepo:   true,
}

// ScheduleInterval returns the duration between syncs for a schedule.
func ScheduleInterval(schedule string) time.Duration {
	switch schedule {
	case ScheduleHourly:
		return 1 * time.Hour
	case ScheduleDaily:
		return 24 * time.Hour
	case ScheduleWeekly:
		return 7 * 24 * time.Hour
	case ScheduleMonthly:
		return 30 * 24 * time.Hour
	default:
		return 0
	}
}

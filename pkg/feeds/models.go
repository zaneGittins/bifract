package feeds

import "time"

// Schedule constants for feed sync frequency.
const (
	ScheduleNever   = "never"
	ScheduleHourly  = "hourly"
	ScheduleDaily   = "daily"
	ScheduleWeekly  = "weekly"
	ScheduleMonthly = "monthly"
)

// ValidSchedules is the set of accepted sync schedule values.
var ValidSchedules = map[string]bool{
	ScheduleNever:   true,
	ScheduleHourly:  true,
	ScheduleDaily:   true,
	ScheduleWeekly:  true,
	ScheduleMonthly: true,
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

// Feed represents an alert feed that syncs rules from a git repository.
type Feed struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Description       string     `json:"description"`
	RepoURL           string     `json:"repo_url"`
	Branch            string     `json:"branch"`
	Path              string     `json:"path"`
	AuthToken         string     `json:"auth_token,omitempty"` // never returned in API responses
	HasAuthToken      bool       `json:"has_auth_token"`       // indicates whether token is set
	NormalizerID      string     `json:"normalizer_id,omitempty"`
	SyncSchedule      string     `json:"sync_schedule"`
	MinLevel          string     `json:"min_level"`
	MinStatus         string     `json:"min_status"`
	Enabled           bool       `json:"enabled"`
	FractalID         string     `json:"fractal_id,omitempty"`
	PrismID           string     `json:"prism_id,omitempty"`
	LastSyncedAt      *time.Time `json:"last_synced_at,omitempty"`
	LastSyncStatus    string     `json:"last_sync_status"`
	LastSyncRuleCount int        `json:"last_sync_rule_count"`
	CreatedBy         string     `json:"created_by"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	AlertCount        int        `json:"alert_count,omitempty"` // computed
}

// CreateRequest is the payload for creating a new feed.
type CreateRequest struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	RepoURL      string `json:"repo_url"`
	Branch       string `json:"branch"`
	Path         string `json:"path"`
	AuthToken    string `json:"auth_token"`
	NormalizerID string `json:"normalizer_id"`
	SyncSchedule string `json:"sync_schedule"`
	MinLevel     string `json:"min_level"`
	MinStatus    string `json:"min_status"`
	Enabled      bool   `json:"enabled"`
}

// UpdateRequest is the payload for updating an existing feed.
type UpdateRequest struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	RepoURL      string `json:"repo_url"`
	Branch       string `json:"branch"`
	Path         string `json:"path"`
	AuthToken    string `json:"auth_token"`     // empty string = no change
	ClearToken   bool   `json:"clear_token"`    // explicitly clear the token
	NormalizerID string `json:"normalizer_id"`
	SyncSchedule string `json:"sync_schedule"`
	MinLevel     string `json:"min_level"`
	MinStatus    string `json:"min_status"`
	Enabled      bool   `json:"enabled"`
}

// SyncResult summarizes the outcome of a feed sync operation.
type SyncResult struct {
	Added   int      `json:"added"`
	Updated int      `json:"updated"`
	Deleted int      `json:"deleted"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors,omitempty"`
}

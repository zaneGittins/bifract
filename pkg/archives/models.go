package archives

import "time"

// Archive represents an archive record in PostgreSQL.
type Archive struct {
	ID             string     `json:"id"`
	FractalID      string     `json:"fractal_id"`
	Filename       string     `json:"filename"`
	StorageType    string     `json:"storage_type"`
	StoragePath    string     `json:"storage_path"`
	SizeBytes      int64      `json:"size_bytes"`
	LogCount       int64      `json:"log_count"`
	TimeRangeStart *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd   *time.Time `json:"time_range_end,omitempty"`
	Status         string     `json:"status"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	CreatedBy      string     `json:"created_by"`
	CreatedAt      time.Time  `json:"created_at"`
	ArchiveType    string     `json:"archive_type"`
	FormatVersion  int        `json:"format_version"`
	ArchiveEndTS   *time.Time `json:"archive_end_ts,omitempty"`
	Checksum         string     `json:"checksum,omitempty"`
	CursorTS         *time.Time `json:"-"`
	CursorID         *string    `json:"-"`
	RestoreLinesSent int64      `json:"restore_lines_sent"`
	RestoreError     string     `json:"restore_error,omitempty"`
	GroupID          *string    `json:"group_id,omitempty"`
	PeriodLabel      string     `json:"period_label,omitempty"`
}

// ArchiveGroup represents a set of period-split archives created together.
type ArchiveGroup struct {
	ID               string     `json:"id"`
	FractalID        string     `json:"fractal_id"`
	SplitGranularity string     `json:"split_granularity"`
	Status           string     `json:"status"`
	ErrorMessage     string     `json:"error_message,omitempty"`
	TotalLogCount    int64      `json:"total_log_count"`
	TotalSizeBytes   int64      `json:"total_size_bytes"`
	ArchiveCount     int        `json:"archive_count"`
	CompletedCount   int        `json:"completed_count"`
	ArchiveType      string     `json:"archive_type"`
	CreatedBy        string     `json:"created_by"`
	CreatedAt        time.Time  `json:"created_at"`
	Archives         []*Archive `json:"archives,omitempty"`
}

const (
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusRestoring  = "restoring"
	StatusPartial    = "partial"

	ArchiveTypeAdhoc     = "adhoc"
	ArchiveTypeScheduled = "scheduled"

	SplitNone = "none"
	SplitHour = "hour"
	SplitDay  = "day"
	SplitWeek = "week"
)

// ValidSplitGranularity returns true if the split value is recognized.
func ValidSplitGranularity(s string) bool {
	switch s {
	case SplitNone, SplitHour, SplitDay, SplitWeek:
		return true
	}
	return false
}

// APIResponse is the standard response format for archive endpoints.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// CreateArchiveRequest is the request body for creating an archive.
type CreateArchiveRequest struct {
	Split string `json:"split"` // none, hour, day, week
}

// RestoreArchiveRequest is the request body for restoring an archive.
// The target fractal is derived from the ingest token (tokens are scoped
// to a single fractal). Cross-fractal restore is done by providing a
// token for the desired target fractal.
type RestoreArchiveRequest struct {
	ClearExisting bool   `json:"clear_existing"`
	IngestToken   string `json:"ingest_token"`
}

// ArchiveListItem wraps either a group or a standalone archive for the list response.
type ArchiveListItem struct {
	Type    string        `json:"type"` // "group" or "archive"
	Group   *ArchiveGroup `json:"group,omitempty"`
	Archive *Archive      `json:"archive,omitempty"`
}

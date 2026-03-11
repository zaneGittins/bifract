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
	CursorTS       *time.Time `json:"-"`
	CursorID       *string    `json:"-"`
}

const (
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusRestoring  = "restoring"

	ArchiveTypeAdhoc     = "adhoc"
	ArchiveTypeScheduled = "scheduled"
)

// APIResponse is the standard response format for archive endpoints.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// CreateArchiveRequest is the request body for creating an archive.
type CreateArchiveRequest struct{}

// RestoreArchiveRequest is the request body for restoring an archive.
// The target fractal is derived from the ingest token (tokens are scoped
// to a single fractal). Cross-fractal restore is done by providing a
// token for the desired target fractal.
type RestoreArchiveRequest struct {
	ClearExisting bool   `json:"clear_existing"`
	IngestToken   string `json:"ingest_token"`
}

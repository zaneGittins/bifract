package fractals

import (
	"time"
)

// Fractal represents a log fractal for multi-tenant isolation
type Fractal struct {
	ID            string     `json:"id" db:"id"`
	Name          string     `json:"name" db:"name"`
	Description   string     `json:"description,omitempty" db:"description"`
	IsDefault     bool       `json:"is_default" db:"is_default"`
	IsSystem      bool       `json:"is_system" db:"is_system"`
	CreatedBy     string     `json:"created_by" db:"created_by"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
	RetentionDays   *int   `json:"retention_days" db:"retention_days"`
	ArchiveSchedule string `json:"archive_schedule" db:"archive_schedule"`
	MaxArchives     *int   `json:"max_archives" db:"max_archives"`
	DiskQuotaBytes  *int64 `json:"disk_quota_bytes" db:"disk_quota_bytes"`
	DiskQuotaAction string `json:"disk_quota_action" db:"disk_quota_action"`

	// Statistics (computed via background jobs)
	LogCount    int64      `json:"log_count" db:"log_count"`
	SizeBytes   int64      `json:"size_bytes" db:"size_bytes"`
	EarliestLog *time.Time `json:"earliest_log,omitempty" db:"earliest_log"`
	LatestLog   *time.Time `json:"latest_log,omitempty" db:"latest_log"`

	// RBAC: populated by handler, not stored in DB
	UserRole string `json:"user_role,omitempty" db:"-"`
}

// CreateFractalRequest represents the request to create a new fractal
type CreateFractalRequest struct {
	Name        string `json:"name" validate:"required,min=1,max=100"`
	Description string `json:"description,omitempty"`
}

// UpdateFractalRequest represents the request to update a fractal
type UpdateFractalRequest struct {
	Name        string `json:"name" validate:"required,min=1,max=100"`
	Description string `json:"description,omitempty"`
}

// FractalStats represents statistics for a fractal
type FractalStats struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	LogCount    int64      `json:"log_count"`
	SizeBytes   int64      `json:"size_bytes"`
	EarliestLog *time.Time `json:"earliest_log,omitempty"`
	LatestLog   *time.Time `json:"latest_log,omitempty"`
	LastUpdated time.Time  `json:"last_updated"`
}

// APIResponse represents a standard API response for fractal operations
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// FractalListResponse represents the response for listing fractals and prisms.
type FractalListResponse struct {
	Fractals []*Fractal    `json:"fractals"`
	Prisms   interface{}   `json:"prisms"`
	Total    int           `json:"total"`
}

// FractalSelectRequest represents the request to select a fractal for the session
type FractalSelectRequest struct {
	FractalID string `json:"fractal_id" validate:"required"`
}

// UpdateRetentionRequest sets the retention period for a fractal
type UpdateRetentionRequest struct {
	RetentionDays *int `json:"retention_days"` // nil = unlimited
}

// UpdateArchiveScheduleRequest sets the archive schedule for a fractal
type UpdateArchiveScheduleRequest struct {
	ArchiveSchedule string `json:"archive_schedule"` // never, daily, weekly, monthly
	MaxArchives     *int   `json:"max_archives"`     // nil = unlimited
}

// UpdateDiskQuotaRequest sets the disk quota for a fractal
type UpdateDiskQuotaRequest struct {
	QuotaBytes *int64 `json:"quota_bytes"` // nil = no limit
	Action     string `json:"action"`      // "reject" or "rollover"
}
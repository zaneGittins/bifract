package storage

import (
	"context"
	"encoding/json"
	"time"
)

// archiveSpoolStatusSetting holds the ingest tier's most recent spool state as JSON.
// In a split deployment the archive spool lives in the dedicated ingest container, so
// the app/UI tier can no longer read it in-process; the ingest tier publishes it here
// and the app reads it for the archive status/provisioned checks.
const archiveSpoolStatusSetting = "archive_spool_status"

// SpoolStatus is the ingest tier's published archive-spool state.
type SpoolStatus struct {
	Provisioned bool      `json:"provisioned"`
	UsedBytes   int64     `json:"used_bytes"`
	MaxBytes    int64     `json:"max_bytes"`
	Pressure    bool      `json:"pressure"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// PublishSpoolStatus records the ingest tier's current spool state (called on the
// archive-enabled poll tick). Stamped with UpdatedAt so the reader can judge staleness.
func (c *PostgresClient) PublishSpoolStatus(ctx context.Context, s SpoolStatus) error {
	s.UpdatedAt = time.Now().UTC()
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return c.SetSetting(ctx, archiveSpoolStatusSetting, string(b))
}

// ReadSpoolStatus returns the last-published spool state, or a zero value
// (Provisioned=false) if the ingest tier has never published.
func (c *PostgresClient) ReadSpoolStatus(ctx context.Context) SpoolStatus {
	var s SpoolStatus
	v, err := c.GetSetting(ctx, archiveSpoolStatusSetting)
	if err != nil || v == "" {
		return s
	}
	_ = json.Unmarshal([]byte(v), &s)
	return s
}

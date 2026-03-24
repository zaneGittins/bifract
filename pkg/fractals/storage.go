package fractals

import (
	"context"
	"fmt"
	"time"

	"bifract/pkg/storage"
)


// Storage handles database operations for fractals
type Storage struct {
	pg *storage.PostgresClient
	ch *storage.ClickHouseClient
}

// NewStorage creates a new storage instance
func NewStorage(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *Storage {
	return &Storage{
		pg: pg,
		ch: ch,
	}
}

// CreateFractal creates a new index in the database
func (s *Storage) CreateFractal(ctx context.Context, req CreateFractalRequest, createdBy string) (*Fractal, error) {
	fractal := &Fractal{}

	query := `
		INSERT INTO fractals (name, description, created_by)
		VALUES ($1, $2, $3)
		RETURNING id, name, description, is_default, is_system, COALESCE(created_by, ''), created_at, updated_at,
		          retention_days, archive_schedule, max_archives, COALESCE(archive_split, 'none'), disk_quota_bytes, COALESCE(disk_quota_action, 'reject'), log_count, size_bytes, earliest_log, latest_log
	`

	err := s.pg.QueryRow(ctx, query, req.Name, req.Description, createdBy).Scan(
		&fractal.ID, &fractal.Name, &fractal.Description, &fractal.IsDefault, &fractal.IsSystem,
		&fractal.CreatedBy, &fractal.CreatedAt, &fractal.UpdatedAt,
		&fractal.RetentionDays, &fractal.ArchiveSchedule, &fractal.MaxArchives, &fractal.ArchiveSplit,
		&fractal.DiskQuotaBytes, &fractal.DiskQuotaAction,
		&fractal.LogCount, &fractal.SizeBytes, &fractal.EarliestLog, &fractal.LatestLog,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create fractal: %w", err)
	}

	return fractal, nil
}

// GetFractal retrieves an index by ID
func (s *Storage) GetFractal(ctx context.Context, fractalID string) (*Fractal, error) {
	fractal := &Fractal{}

	query := `
		SELECT id, name, description, is_default, is_system, COALESCE(created_by, ''), created_at, updated_at,
		       retention_days, archive_schedule, max_archives, COALESCE(archive_split, 'none'), disk_quota_bytes, COALESCE(disk_quota_action, 'reject'), log_count, size_bytes, earliest_log, latest_log
		FROM fractals
		WHERE id = $1
	`

	err := s.pg.QueryRow(ctx, query, fractalID).Scan(
		&fractal.ID, &fractal.Name, &fractal.Description, &fractal.IsDefault, &fractal.IsSystem,
		&fractal.CreatedBy, &fractal.CreatedAt, &fractal.UpdatedAt,
		&fractal.RetentionDays, &fractal.ArchiveSchedule, &fractal.MaxArchives, &fractal.ArchiveSplit,
		&fractal.DiskQuotaBytes, &fractal.DiskQuotaAction,
		&fractal.LogCount, &fractal.SizeBytes, &fractal.EarliestLog, &fractal.LatestLog,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get fractal: %w", err)
	}

	return fractal, nil
}

// GetFractalByName retrieves an index by name
func (s *Storage) GetFractalByName(ctx context.Context, name string) (*Fractal, error) {
	fractal := &Fractal{}

	query := `
		SELECT id, name, description, is_default, is_system, COALESCE(created_by, ''), created_at, updated_at,
		       retention_days, archive_schedule, max_archives, COALESCE(archive_split, 'none'), disk_quota_bytes, COALESCE(disk_quota_action, 'reject'), log_count, size_bytes, earliest_log, latest_log
		FROM fractals
		WHERE name = $1
	`

	err := s.pg.QueryRow(ctx, query, name).Scan(
		&fractal.ID, &fractal.Name, &fractal.Description, &fractal.IsDefault, &fractal.IsSystem,
		&fractal.CreatedBy, &fractal.CreatedAt, &fractal.UpdatedAt,
		&fractal.RetentionDays, &fractal.ArchiveSchedule, &fractal.MaxArchives, &fractal.ArchiveSplit,
		&fractal.DiskQuotaBytes, &fractal.DiskQuotaAction,
		&fractal.LogCount, &fractal.SizeBytes, &fractal.EarliestLog, &fractal.LatestLog,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get fractal by name: %w", err)
	}

	return fractal, nil
}

// GetDefaultFractal retrieves the default index
func (s *Storage) GetDefaultFractal(ctx context.Context) (*Fractal, error) {
	fractal := &Fractal{}

	query := `
		SELECT id, name, description, is_default, is_system, COALESCE(created_by, ''), created_at, updated_at,
		       retention_days, archive_schedule, max_archives, COALESCE(archive_split, 'none'), disk_quota_bytes, COALESCE(disk_quota_action, 'reject'), log_count, size_bytes, earliest_log, latest_log
		FROM fractals
		WHERE is_default = true
		LIMIT 1
	`

	err := s.pg.QueryRow(ctx, query).Scan(
		&fractal.ID, &fractal.Name, &fractal.Description, &fractal.IsDefault, &fractal.IsSystem,
		&fractal.CreatedBy, &fractal.CreatedAt, &fractal.UpdatedAt,
		&fractal.RetentionDays, &fractal.ArchiveSchedule, &fractal.MaxArchives, &fractal.ArchiveSplit,
		&fractal.DiskQuotaBytes, &fractal.DiskQuotaAction,
		&fractal.LogCount, &fractal.SizeBytes, &fractal.EarliestLog, &fractal.LatestLog,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get default fractal: %w", err)
	}

	return fractal, nil
}

// ListFractals retrieves all fractals with optional filtering
func (s *Storage) ListFractals(ctx context.Context) ([]*Fractal, error) {
	query := `
		SELECT id, name, description, is_default, is_system, COALESCE(created_by, ''), created_at, updated_at,
		       retention_days, archive_schedule, max_archives, COALESCE(archive_split, 'none'), disk_quota_bytes, COALESCE(disk_quota_action, 'reject'), log_count, size_bytes, earliest_log, latest_log
		FROM fractals
		ORDER BY is_default DESC, name ASC
	`

	rows, err := s.pg.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list fractals: %w", err)
	}
	defer rows.Close()

	var fractals []*Fractal
	for rows.Next() {
		fractal := &Fractal{}
		err := rows.Scan(
			&fractal.ID, &fractal.Name, &fractal.Description, &fractal.IsDefault, &fractal.IsSystem,
			&fractal.CreatedBy, &fractal.CreatedAt, &fractal.UpdatedAt,
			&fractal.RetentionDays, &fractal.ArchiveSchedule, &fractal.MaxArchives, &fractal.ArchiveSplit,
			&fractal.DiskQuotaBytes, &fractal.DiskQuotaAction,
			&fractal.LogCount, &fractal.SizeBytes, &fractal.EarliestLog, &fractal.LatestLog,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan fractal: %w", err)
		}
		fractals = append(fractals, fractal)
	}

	return fractals, nil
}

// UpdateFractal updates an existing index
func (s *Storage) UpdateFractal(ctx context.Context, fractalID string, req UpdateFractalRequest) (*Fractal, error) {
	query := `
		UPDATE fractals
		SET name = $2, description = $3, updated_at = NOW()
		WHERE id = $1
		RETURNING id, name, description, is_default, is_system, COALESCE(created_by, ''), created_at, updated_at,
		          retention_days, archive_schedule, max_archives, COALESCE(archive_split, 'none'), disk_quota_bytes, COALESCE(disk_quota_action, 'reject'), log_count, size_bytes, earliest_log, latest_log
	`

	fractal := &Fractal{}
	err := s.pg.QueryRow(ctx, query, fractalID, req.Name, req.Description).Scan(
		&fractal.ID, &fractal.Name, &fractal.Description, &fractal.IsDefault, &fractal.IsSystem,
		&fractal.CreatedBy, &fractal.CreatedAt, &fractal.UpdatedAt,
		&fractal.RetentionDays, &fractal.ArchiveSchedule, &fractal.MaxArchives, &fractal.ArchiveSplit,
		&fractal.DiskQuotaBytes, &fractal.DiskQuotaAction,
		&fractal.LogCount, &fractal.SizeBytes, &fractal.EarliestLog, &fractal.LatestLog,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to update fractal: %w", err)
	}

	return fractal, nil
}

// SetRetention updates the retention_days for a fractal (nil = unlimited)
func (s *Storage) SetRetention(ctx context.Context, fractalID string, days *int) error {
	_, err := s.pg.Exec(ctx,
		"UPDATE fractals SET retention_days = $2, updated_at = NOW() WHERE id = $1",
		fractalID, days,
	)
	if err != nil {
		return fmt.Errorf("failed to set retention: %w", err)
	}
	return nil
}

// SetArchiveSchedule updates the archive schedule, max archives, and split granularity for a fractal.
func (s *Storage) SetArchiveSchedule(ctx context.Context, fractalID string, schedule string, maxArchives *int, archiveSplit string) error {
	if archiveSplit == "" {
		archiveSplit = "none"
	}
	_, err := s.pg.Exec(ctx,
		"UPDATE fractals SET archive_schedule = $2, max_archives = $3, archive_split = $4, updated_at = NOW() WHERE id = $1",
		fractalID, schedule, maxArchives, archiveSplit,
	)
	if err != nil {
		return fmt.Errorf("failed to set archive schedule: %w", err)
	}
	return nil
}

// SetDiskQuota sets the disk quota and enforcement action for a fractal.
// quotaBytes == nil means no limit.
func (s *Storage) SetDiskQuota(ctx context.Context, fractalID string, quotaBytes *int64, action string) error {
	_, err := s.pg.Exec(ctx,
		"UPDATE fractals SET disk_quota_bytes = $2, disk_quota_action = $3, updated_at = NOW() WHERE id = $1",
		fractalID, quotaBytes, action,
	)
	if err != nil {
		return fmt.Errorf("failed to set disk quota: %w", err)
	}
	return nil
}

// HasActiveArchive checks if a fractal has an in-progress or restoring archive operation.
func (s *Storage) HasActiveArchive(ctx context.Context, fractalID string) (bool, error) {
	var count int
	err := s.pg.QueryRow(ctx,
		"SELECT COUNT(*) FROM archives WHERE fractal_id = $1 AND status IN ('in_progress', 'restoring')",
		fractalID,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check active archive: %w", err)
	}
	return count > 0, nil
}

// DeleteOldLogs removes logs older than retentionDays for the given fractal
func (s *Storage) DeleteOldLogs(ctx context.Context, fractalID string, retentionDays int, isDefault bool) error {
	var query string
	if isDefault {
		query = fmt.Sprintf(
			"ALTER TABLE logs DELETE WHERE fractal_id IN ('%s', '') AND timestamp < subtractDays(now(), %d)",
			storage.EscCHStr(fractalID), retentionDays,
		)
	} else {
		query = fmt.Sprintf(
			"ALTER TABLE logs DELETE WHERE fractal_id = '%s' AND timestamp < subtractDays(now(), %d)",
			storage.EscCHStr(fractalID), retentionDays,
		)
	}
	return s.ch.Exec(ctx, query)
}

// DeleteFractal deletes an index and all associated data
func (s *Storage) DeleteFractal(ctx context.Context, fractalID string) error {
	// Start transaction
	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if it's a protected fractal (cannot delete)
	var isDefault, isSystem bool
	err = tx.QueryRow(ctx, "SELECT is_default, is_system FROM fractals WHERE id = $1", fractalID).Scan(&isDefault, &isSystem)
	if err != nil {
		return fmt.Errorf("failed to check if fractal is protected: %w", err)
	}

	if isDefault || isSystem {
		return fmt.Errorf("cannot delete a system fractal")
	}

	// Delete the fractal (cascade will handle related data)
	result, err := tx.Exec(ctx, "DELETE FROM fractals WHERE id = $1", fractalID)
	if err != nil {
		return fmt.Errorf("failed to delete fractal: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("fractal not found")
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Delete ClickHouse log data for this fractal. ALTER TABLE DELETE is
	// asynchronous in ClickHouse so this returns quickly. If it fails the
	// fractal is already gone from PostgreSQL (no new queries or ingestion
	// can reach it), so we log the error and move on.
	deleteQuery := fmt.Sprintf(
		"ALTER TABLE logs DELETE WHERE fractal_id = '%s'",
		storage.EscCHStr(fractalID),
	)
	if err := s.ch.Exec(ctx, deleteQuery); err != nil {
		fmt.Printf("Warning: failed to delete ClickHouse data for fractal %s: %v\n", fractalID, err)
	}

	return nil
}

// UpdateFractalStats updates the cached statistics for an index
func (s *Storage) UpdateFractalStats(ctx context.Context, fractalID string, stats FractalStats) error {
	query := `
		UPDATE fractals
		SET log_count = $2, size_bytes = $3, earliest_log = $4, latest_log = $5, updated_at = NOW()
		WHERE id = $1
	`

	_, err := s.pg.Exec(ctx, query, fractalID, stats.LogCount, stats.SizeBytes, stats.EarliestLog, stats.LatestLog)
	if err != nil {
		return fmt.Errorf("failed to update fractal stats: %w", err)
	}

	return nil
}

// ComputeAllFractalStats computes statistics for all fractals in two cheap
// queries instead of N expensive ones:
//  1. count/min/max per fractal_id from the logs table (uses the primary key,
//     no full scan needed).
//  2. bytes_on_disk per fractal_id from system.columns_mark_compressed
//     via system.parts (metadata only, no data decompression).
func (s *Storage) ComputeAllFractalStats(ctx context.Context, fractals []*Fractal) (map[string]*FractalStats, error) {
	statsMap := make(map[string]*FractalStats, len(fractals))
	for _, f := range fractals {
		statsMap[f.ID] = &FractalStats{
			ID:          f.ID,
			Name:        f.Name,
			LastUpdated: time.Now(),
		}
	}

	// Query 1: count, min/max timestamp per fractal_id (cheap: uses primary key)
	countQuery := fmt.Sprintf(`
		SELECT fractal_id,
		       count()          AS count,
		       min(timestamp)   AS oldest,
		       max(timestamp)   AS newest
		FROM %s
		GROUP BY fractal_id
	`, s.ch.ReadTable())
	rows, err := s.ch.Query(ctx, countQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to compute fractal count stats: %w", err)
	}

	// Build a lookup from fractal_id to stats; also accumulate counts for the
	// default fractal which owns rows with fractal_id = ''.
	var defaultFractalID string
	for _, f := range fractals {
		if f.IsDefault {
			defaultFractalID = f.ID
			break
		}
	}

	countByFID := make(map[string]int64)
	oldestByFID := make(map[string]time.Time)
	newestByFID := make(map[string]time.Time)
	for _, row := range rows {
		fid, _ := row["fractal_id"].(string)
		count, _ := row["count"].(uint64)

		// Rows with empty fractal_id belong to the default fractal
		targetID := fid
		if targetID == "" {
			targetID = defaultFractalID
		}
		if targetID == "" {
			continue
		}

		countByFID[targetID] += int64(count)

		var oldest, newest time.Time
		if t, ok := row["oldest"].(time.Time); ok {
			oldest = t
		}
		if t, ok := row["newest"].(time.Time); ok {
			newest = t
		}

		if prev, exists := oldestByFID[targetID]; !exists || oldest.Before(prev) {
			oldestByFID[targetID] = oldest
		}
		if prev, exists := newestByFID[targetID]; !exists || newest.After(prev) {
			newestByFID[targetID] = newest
		}
	}

	for id, st := range statsMap {
		st.LogCount = countByFID[id]
		if st.LogCount > 0 {
			if t, ok := oldestByFID[id]; ok {
				cp := t
				st.EarliestLog = &cp
			}
			if t, ok := newestByFID[id]; ok {
				cp := t
				st.LatestLog = &cp
			}
		}
	}

	// Query 2: on-disk bytes per fractal_id from system.parts (metadata only).
	// ClickHouse can extract per-column compressed bytes from the
	// system.parts_columns table, but since fractal_id is not a partition key
	// we approximate with total part bytes split proportionally by row count.
	// However, a simpler and accurate approach: just use total table bytes
	// split by each fractal's row-count proportion.
	sizeQuery := `
		SELECT sum(data_uncompressed_bytes) AS total_bytes
		FROM system.parts
		WHERE database = '` + s.ch.Database + `' AND table = 'logs' AND active = 1
	`
	sizeRows, err := s.ch.Query(ctx, sizeQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to compute table size: %w", err)
	}

	var totalBytes uint64
	if len(sizeRows) > 0 {
		if b, ok := sizeRows[0]["total_bytes"].(uint64); ok {
			totalBytes = b
		}
	}

	// Distribute bytes proportionally by log count
	var totalLogs int64
	for _, st := range statsMap {
		totalLogs += st.LogCount
	}
	if totalLogs > 0 && totalBytes > 0 {
		for _, st := range statsMap {
			st.SizeBytes = int64(float64(totalBytes) * float64(st.LogCount) / float64(totalLogs))
		}
	}

	return statsMap, nil
}

// ValidateFractalExists checks if an index exists and is accessible
func (s *Storage) ValidateFractalExists(ctx context.Context, fractalID string) (bool, error) {
	var count int
	query := "SELECT COUNT(*) FROM fractals WHERE id = $1"

	err := s.pg.QueryRow(ctx, query, fractalID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to validate index: %w", err)
	}

	return count > 0, nil
}
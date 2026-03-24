package archives

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"bifract/pkg/storage"
)

// Storage provides CRUD operations for archive records in PostgreSQL.
type Storage struct {
	pg *storage.PostgresClient
}

// NewStorage creates a new archive storage.
func NewStorage(pg *storage.PostgresClient) *Storage {
	return &Storage{pg: pg}
}

// ========================================================================
// Archive CRUD
// ========================================================================

// archiveColumns is the standard column list used in SELECT queries.
const archiveColumns = `id, fractal_id, filename, storage_type, storage_path, size_bytes, log_count,
	time_range_start, time_range_end, status, error_message, COALESCE(created_by, ''), created_at, archive_type,
	format_version, archive_end_ts, cursor_ts, cursor_id, COALESCE(checksum, ''),
	restore_lines_sent, COALESCE(restore_error, ''), group_id, COALESCE(period_label, '')`

// scanArchive scans a row into an Archive struct.
func scanArchive(scanner interface{ Scan(dest ...interface{}) error }) (*Archive, error) {
	var a Archive
	var errMsg, cursorID, groupID sql.NullString
	var archiveEndTS, cursorTS sql.NullTime
	err := scanner.Scan(
		&a.ID, &a.FractalID, &a.Filename, &a.StorageType, &a.StoragePath,
		&a.SizeBytes, &a.LogCount, &a.TimeRangeStart, &a.TimeRangeEnd,
		&a.Status, &errMsg, &a.CreatedBy, &a.CreatedAt, &a.ArchiveType,
		&a.FormatVersion, &archiveEndTS, &cursorTS, &cursorID, &a.Checksum,
		&a.RestoreLinesSent, &a.RestoreError, &groupID, &a.PeriodLabel,
	)
	if err != nil {
		return nil, err
	}
	a.ErrorMessage = errMsg.String
	if archiveEndTS.Valid {
		a.ArchiveEndTS = &archiveEndTS.Time
	}
	if cursorTS.Valid {
		a.CursorTS = &cursorTS.Time
	}
	if cursorID.Valid {
		a.CursorID = &cursorID.String
	}
	if groupID.Valid {
		a.GroupID = &groupID.String
	}
	return &a, nil
}

// CreateArchive inserts a new archive record with format_version=1 and
// archive_end_ts pinned to the current time.
func (s *Storage) CreateArchive(ctx context.Context, fractalID, filename, storageType, storagePath, createdBy, archiveType string) (*Archive, error) {
	if archiveType == "" {
		archiveType = ArchiveTypeAdhoc
	}
	row := s.pg.DB().QueryRowContext(ctx,
		`INSERT INTO archives (fractal_id, filename, storage_type, storage_path, created_by, archive_type, format_version, archive_end_ts)
		 VALUES ($1, $2, $3, $4, $5, $6, 1, NOW())
		 RETURNING `+archiveColumns,
		fractalID, filename, storageType, storagePath, createdBy, archiveType,
	)
	a, err := scanArchive(row)
	if err != nil {
		return nil, fmt.Errorf("insert archive: %w", err)
	}
	return a, nil
}

// CreateArchiveInGroup inserts an archive record belonging to a group with
// explicit time bounds and period label.
func (s *Storage) CreateArchiveInGroup(ctx context.Context, fractalID, filename, storageType, storagePath, createdBy, archiveType, groupID, periodLabel string, archiveEndTS time.Time) (*Archive, error) {
	if archiveType == "" {
		archiveType = ArchiveTypeAdhoc
	}
	row := s.pg.DB().QueryRowContext(ctx,
		`INSERT INTO archives (fractal_id, filename, storage_type, storage_path, created_by, archive_type, format_version, archive_end_ts, group_id, period_label)
		 VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9)
		 RETURNING `+archiveColumns,
		fractalID, filename, storageType, storagePath, createdBy, archiveType, archiveEndTS, groupID, periodLabel,
	)
	a, err := scanArchive(row)
	if err != nil {
		return nil, fmt.Errorf("insert archive in group: %w", err)
	}
	return a, nil
}

// GetArchive retrieves an archive by ID.
func (s *Storage) GetArchive(ctx context.Context, archiveID string) (*Archive, error) {
	row := s.pg.DB().QueryRowContext(ctx,
		`SELECT `+archiveColumns+` FROM archives WHERE id = $1`, archiveID,
	)
	a, err := scanArchive(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("archive not found")
		}
		return nil, fmt.Errorf("get archive: %w", err)
	}
	return a, nil
}

// ListArchives returns all archives for a fractal, ordered by creation time descending.
func (s *Storage) ListArchives(ctx context.Context, fractalID string) ([]*Archive, error) {
	rows, err := s.pg.DB().QueryContext(ctx,
		`SELECT `+archiveColumns+` FROM archives WHERE fractal_id = $1 ORDER BY created_at DESC`, fractalID,
	)
	if err != nil {
		return nil, fmt.Errorf("list archives: %w", err)
	}
	defer rows.Close()

	var archives []*Archive
	for rows.Next() {
		a, err := scanArchive(rows)
		if err != nil {
			return nil, fmt.Errorf("scan archive: %w", err)
		}
		archives = append(archives, a)
	}
	if archives == nil {
		archives = []*Archive{}
	}
	return archives, rows.Err()
}

// ListArchivesByGroup returns archives in a group ordered by time_range_start ascending.
func (s *Storage) ListArchivesByGroup(ctx context.Context, groupID string) ([]*Archive, error) {
	rows, err := s.pg.DB().QueryContext(ctx,
		`SELECT `+archiveColumns+` FROM archives WHERE group_id = $1 ORDER BY time_range_start ASC NULLS LAST, created_at ASC`, groupID,
	)
	if err != nil {
		return nil, fmt.Errorf("list archives by group: %w", err)
	}
	defer rows.Close()

	var archives []*Archive
	for rows.Next() {
		a, err := scanArchive(rows)
		if err != nil {
			return nil, fmt.Errorf("scan archive: %w", err)
		}
		archives = append(archives, a)
	}
	if archives == nil {
		archives = []*Archive{}
	}
	return archives, rows.Err()
}

// UpdateArchiveStatus updates the status and metadata of an archive.
func (s *Storage) UpdateArchiveStatus(ctx context.Context, archiveID, status, errMsg string, sizeBytes, logCount int64, timeStart, timeEnd *time.Time, checksum string) error {
	_, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET status = $1, error_message = $2, size_bytes = $3, log_count = $4,
		 time_range_start = $5, time_range_end = $6, checksum = $7 WHERE id = $8`,
		status, errMsg, sizeBytes, logCount, timeStart, timeEnd, checksum, archiveID,
	)
	if err != nil {
		return fmt.Errorf("update archive status: %w", err)
	}
	return nil
}

// UpdateArchiveCursor persists the cursor position and log count for an
// in-progress archive. This enables progress tracking and crash diagnostics.
func (s *Storage) UpdateArchiveCursor(ctx context.Context, archiveID string, cursorTS time.Time, cursorID string, logCount int64) {
	s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET cursor_ts = $1, cursor_id = $2, log_count = $3
		 WHERE id = $4 AND status = $5`,
		cursorTS, cursorID, logCount, archiveID, StatusInProgress,
	)
}

// SetArchiveStatus updates only the status field.
func (s *Storage) SetArchiveStatus(ctx context.Context, archiveID, status string) error {
	_, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET status = $1 WHERE id = $2`, status, archiveID,
	)
	if err != nil {
		return fmt.Errorf("set archive status: %w", err)
	}
	return nil
}

// SetArchiveStatusWithError updates the status and error message without
// touching log_count or time_range fields, preserving cursor progress.
func (s *Storage) SetArchiveStatusWithError(ctx context.Context, archiveID, status, errMsg string) error {
	_, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET status = $1, error_message = $2 WHERE id = $3`,
		status, errMsg, archiveID,
	)
	if err != nil {
		return fmt.Errorf("set archive status with error: %w", err)
	}
	return nil
}

// FailInterruptedArchives marks in_progress archives as failed and restoring
// archives back to completed (preserving restore_lines_sent for resume).
// Called on startup to clean up after crashes.
func (s *Storage) FailInterruptedArchives(ctx context.Context) (int64, error) {
	// In-progress archives are genuinely failed (incomplete file).
	res1, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET status = $1, error_message = 'interrupted by server restart'
		 WHERE status = $2`,
		StatusFailed, StatusInProgress,
	)
	if err != nil {
		return 0, fmt.Errorf("fail interrupted archives: %w", err)
	}
	count1, _ := res1.RowsAffected()

	// Restoring archives: the archive file is still valid. Set back to
	// completed so restore can be resumed. Preserve restore_lines_sent.
	res2, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET status = $1, restore_error = 'interrupted by server restart'
		 WHERE status = $2`,
		StatusCompleted, StatusRestoring,
	)
	if err != nil {
		return count1, fmt.Errorf("recover interrupted restores: %w", err)
	}
	count2, _ := res2.RowsAffected()
	return count1 + count2, nil
}

// DeleteArchive removes an archive record.
func (s *Storage) DeleteArchive(ctx context.Context, archiveID string) error {
	_, err := s.pg.DB().ExecContext(ctx, `DELETE FROM archives WHERE id = $1`, archiveID)
	if err != nil {
		return fmt.Errorf("delete archive: %w", err)
	}
	return nil
}

// HasActiveOperation checks if a fractal has any in-progress or restoring archives.
func (s *Storage) HasActiveOperation(ctx context.Context, fractalID string) (bool, error) {
	var count int
	err := s.pg.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM archives WHERE fractal_id = $1 AND status IN ($2, $3)`,
		fractalID, StatusInProgress, StatusRestoring,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check active operation: %w", err)
	}
	return count > 0, nil
}

// GetOldestCompletedGroups returns IDs of completed archive groups beyond
// the keep limit (oldest first). Standalone archives without a group are
// counted as individual items.
func (s *Storage) GetOldestCompletedGroups(ctx context.Context, fractalID string, keepCount int) ([]string, error) {
	rows, err := s.pg.DB().QueryContext(ctx,
		`SELECT id FROM archive_groups
		 WHERE fractal_id = $1 AND status IN ($2, $3)
		 ORDER BY created_at DESC
		 OFFSET $4`,
		fractalID, StatusCompleted, StatusPartial, keepCount,
	)
	if err != nil {
		return nil, fmt.Errorf("get oldest groups: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// GetOldestCompletedArchives returns IDs of completed standalone archives
// (no group) beyond the keep limit (oldest first).
func (s *Storage) GetOldestCompletedArchives(ctx context.Context, fractalID string, keepCount int) ([]string, error) {
	rows, err := s.pg.DB().QueryContext(ctx,
		`SELECT id FROM archives
		 WHERE fractal_id = $1 AND status = $2 AND group_id IS NULL
		 ORDER BY created_at DESC
		 OFFSET $3`,
		fractalID, StatusCompleted, keepCount,
	)
	if err != nil {
		return nil, fmt.Errorf("get oldest archives: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// UpdateRestoreCursor persists the number of lines confirmed sent during restore.
// Does not filter on status so the final cursor update succeeds even if the
// status was already changed to completed by a concurrent cancel operation.
func (s *Storage) UpdateRestoreCursor(ctx context.Context, archiveID string, linesSent int64) {
	s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET restore_lines_sent = $1 WHERE id = $2`,
		linesSent, archiveID,
	)
}

// SetRestoreError records a restore failure. Sets status back to completed
// (archive file is still valid) and preserves restore_lines_sent for resume.
func (s *Storage) SetRestoreError(ctx context.Context, archiveID, errMsg string) error {
	_, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET status = $1, restore_error = $2 WHERE id = $3`,
		StatusCompleted, errMsg, archiveID,
	)
	return err
}

// ClearRestoreState resets restore progress for a fresh restore attempt.
func (s *Storage) ClearRestoreState(ctx context.Context, archiveID string) error {
	_, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archives SET restore_lines_sent = 0, restore_error = NULL WHERE id = $1`,
		archiveID,
	)
	return err
}

// ========================================================================
// Archive Group CRUD
// ========================================================================

// CreateArchiveGroup inserts a new archive group record.
func (s *Storage) CreateArchiveGroup(ctx context.Context, fractalID, splitGranularity, archiveType, createdBy string, archiveCount int) (*ArchiveGroup, error) {
	if archiveType == "" {
		archiveType = ArchiveTypeAdhoc
	}
	var g ArchiveGroup
	err := s.pg.DB().QueryRowContext(ctx,
		`INSERT INTO archive_groups (fractal_id, split_granularity, archive_type, created_by, archive_count)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, fractal_id, split_granularity, status, COALESCE(error_message, ''),
		           total_log_count, total_size_bytes, archive_count, completed_count,
		           archive_type, COALESCE(created_by, ''), created_at`,
		fractalID, splitGranularity, archiveType, createdBy, archiveCount,
	).Scan(
		&g.ID, &g.FractalID, &g.SplitGranularity, &g.Status, &g.ErrorMessage,
		&g.TotalLogCount, &g.TotalSizeBytes, &g.ArchiveCount, &g.CompletedCount,
		&g.ArchiveType, &g.CreatedBy, &g.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert archive group: %w", err)
	}
	return &g, nil
}

// groupColumns is the standard column list for archive_groups SELECT queries.
const groupColumns = `id, fractal_id, split_granularity, status, COALESCE(error_message, ''),
	total_log_count, total_size_bytes, archive_count, completed_count,
	archive_type, COALESCE(created_by, ''), created_at`

func scanGroup(scanner interface{ Scan(dest ...interface{}) error }) (*ArchiveGroup, error) {
	var g ArchiveGroup
	err := scanner.Scan(
		&g.ID, &g.FractalID, &g.SplitGranularity, &g.Status, &g.ErrorMessage,
		&g.TotalLogCount, &g.TotalSizeBytes, &g.ArchiveCount, &g.CompletedCount,
		&g.ArchiveType, &g.CreatedBy, &g.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &g, nil
}

// GetArchiveGroup retrieves an archive group by ID.
func (s *Storage) GetArchiveGroup(ctx context.Context, groupID string) (*ArchiveGroup, error) {
	row := s.pg.DB().QueryRowContext(ctx,
		`SELECT `+groupColumns+` FROM archive_groups WHERE id = $1`, groupID,
	)
	g, err := scanGroup(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("archive group not found")
		}
		return nil, fmt.Errorf("get archive group: %w", err)
	}
	return g, nil
}

// ListArchiveGroups returns all archive groups for a fractal, ordered by creation time descending.
func (s *Storage) ListArchiveGroups(ctx context.Context, fractalID string) ([]*ArchiveGroup, error) {
	rows, err := s.pg.DB().QueryContext(ctx,
		`SELECT `+groupColumns+` FROM archive_groups WHERE fractal_id = $1 ORDER BY created_at DESC`, fractalID,
	)
	if err != nil {
		return nil, fmt.Errorf("list archive groups: %w", err)
	}
	defer rows.Close()

	var groups []*ArchiveGroup
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, fmt.Errorf("scan archive group: %w", err)
		}
		groups = append(groups, g)
	}
	if groups == nil {
		groups = []*ArchiveGroup{}
	}
	return groups, rows.Err()
}

// UpdateArchiveGroupProgress updates the running totals for a group after a child completes.
func (s *Storage) UpdateArchiveGroupProgress(ctx context.Context, groupID string, addLogCount, addSizeBytes int64) {
	s.pg.DB().ExecContext(ctx,
		`UPDATE archive_groups SET completed_count = completed_count + 1,
		 total_log_count = total_log_count + $1, total_size_bytes = total_size_bytes + $2
		 WHERE id = $3`,
		addLogCount, addSizeBytes, groupID,
	)
}

// SetArchiveGroupStatus updates the status and optional error message for a group.
func (s *Storage) SetArchiveGroupStatus(ctx context.Context, groupID, status, errMsg string) error {
	_, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archive_groups SET status = $1, error_message = $2 WHERE id = $3`,
		status, errMsg, groupID,
	)
	if err != nil {
		return fmt.Errorf("set archive group status: %w", err)
	}
	return nil
}

// SetArchiveGroupArchiveCount updates the total expected archive count for a group.
func (s *Storage) SetArchiveGroupArchiveCount(ctx context.Context, groupID string, count int) {
	s.pg.DB().ExecContext(ctx,
		`UPDATE archive_groups SET archive_count = $1 WHERE id = $2`,
		count, groupID,
	)
}

// DeleteArchiveGroup removes an archive group record. Child archives are
// cascade-deleted by the FK constraint.
func (s *Storage) DeleteArchiveGroup(ctx context.Context, groupID string) error {
	_, err := s.pg.DB().ExecContext(ctx, `DELETE FROM archive_groups WHERE id = $1`, groupID)
	if err != nil {
		return fmt.Errorf("delete archive group: %w", err)
	}
	return nil
}

// RecoverInterruptedGroups updates group statuses to reflect child archive
// statuses after FailInterruptedArchives has run.
func (s *Storage) RecoverInterruptedGroups(ctx context.Context) (int64, error) {
	// Groups with all children completed or failed (none in_progress/restoring).
	// If any child failed, group is partial. If all completed, group is completed.
	// If all failed, group is failed.
	res, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archive_groups g SET
		 status = CASE
		   WHEN (SELECT COUNT(*) FROM archives a WHERE a.group_id = g.id AND a.status = $1) = g.archive_count THEN $1
		   WHEN (SELECT COUNT(*) FROM archives a WHERE a.group_id = g.id AND a.status = $2) = g.archive_count THEN $2
		   WHEN (SELECT COUNT(*) FROM archives a WHERE a.group_id = g.id AND a.status IN ($1, $2)) = g.archive_count THEN $3
		   ELSE g.status
		 END,
		 error_message = CASE
		   WHEN g.status = $4 THEN 'interrupted by server restart'
		   ELSE g.error_message
		 END
		 WHERE g.status = $4`,
		StatusCompleted, StatusFailed, StatusPartial, StatusInProgress,
	)
	if err != nil {
		return 0, fmt.Errorf("recover interrupted groups: %w", err)
	}
	count, _ := res.RowsAffected()

	// Also recover groups in restoring state
	res2, err := s.pg.DB().ExecContext(ctx,
		`UPDATE archive_groups SET status = $1, error_message = 'interrupted by server restart'
		 WHERE status = $2`,
		StatusCompleted, StatusRestoring,
	)
	if err != nil {
		return count, fmt.Errorf("recover restoring groups: %w", err)
	}
	count2, _ := res2.RowsAffected()
	return count + count2, nil
}

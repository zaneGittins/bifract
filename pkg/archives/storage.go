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

// archiveColumns is the standard column list used in SELECT queries.
const archiveColumns = `id, fractal_id, filename, storage_type, storage_path, size_bytes, log_count,
	time_range_start, time_range_end, status, error_message, created_by, created_at, archive_type,
	format_version, archive_end_ts, cursor_ts, cursor_id, COALESCE(checksum, ''),
	restore_lines_sent, COALESCE(restore_error, '')`

// scanArchive scans a row into an Archive struct.
func scanArchive(scanner interface{ Scan(dest ...interface{}) error }) (*Archive, error) {
	var a Archive
	var errMsg, cursorID sql.NullString
	var archiveEndTS, cursorTS sql.NullTime
	err := scanner.Scan(
		&a.ID, &a.FractalID, &a.Filename, &a.StorageType, &a.StoragePath,
		&a.SizeBytes, &a.LogCount, &a.TimeRangeStart, &a.TimeRangeEnd,
		&a.Status, &errMsg, &a.CreatedBy, &a.CreatedAt, &a.ArchiveType,
		&a.FormatVersion, &archiveEndTS, &cursorTS, &cursorID, &a.Checksum,
		&a.RestoreLinesSent, &a.RestoreError,
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

// GetOldestCompletedArchives returns IDs of completed archives beyond the keep limit (oldest first).
func (s *Storage) GetOldestCompletedArchives(ctx context.Context, fractalID string, keepCount int) ([]string, error) {
	rows, err := s.pg.DB().QueryContext(ctx,
		`SELECT id FROM archives
		 WHERE fractal_id = $1 AND status = $2
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

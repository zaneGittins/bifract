package datasets

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"bifract/pkg/storage"
)

// Import statuses. An import runs downloading, then importing, then ends
// succeeded or failed.
const (
	StatusPending     = "pending"
	StatusDownloading = "downloading"
	StatusImporting   = "importing"
	StatusSucceeded   = "succeeded"
	StatusFailed      = "failed"
)

// ErrNotFound reports an import that does not exist, or one no longer held by
// the worker asking: its fractal was deleted or another worker reclaimed it.
var ErrNotFound = errors.New("dataset import not found")

// Import is one dataset being loaded into its fractal.
type Import struct {
	ID          int64      `json:"id"`
	DatasetID   string     `json:"dataset_id"`
	FractalID   string     `json:"fractal_id"`
	FractalName string     `json:"fractal_name"`
	URL         string     `json:"url"`
	SHA256      string     `json:"sha256"`
	BytesTotal  int64      `json:"bytes_total"`
	LogsTotal   int64      `json:"logs_total"`
	Status      string     `json:"status"`
	BytesDone   int64      `json:"bytes_done"`
	LogsDone    int64      `json:"logs_done"`
	Error       string     `json:"error,omitempty"`
	RequestedBy string     `json:"requested_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

const importColumns = `i.id, i.dataset_id, i.fractal_id::text, f.name, i.url, i.sha256, i.bytes_total, i.logs_total,
	i.status, i.bytes_done, i.logs_done, i.error, i.requested_by, i.created_at, i.started_at, i.finished_at, i.updated_at`

func (i *Import) scanDest() []any {
	return []any{&i.ID, &i.DatasetID, &i.FractalID, &i.FractalName, &i.URL, &i.SHA256, &i.BytesTotal, &i.LogsTotal,
		&i.Status, &i.BytesDone, &i.LogsDone, &i.Error, &i.RequestedBy, &i.CreatedAt, &i.StartedAt, &i.FinishedAt, &i.UpdatedAt}
}

// Store keeps dataset imports in Postgres.
type Store struct {
	pg *storage.PostgresClient
}

func NewStore(pg *storage.PostgresClient) *Store { return &Store{pg: pg} }

// Create queues an import of a dataset into a fractal made for it.
func (s *Store) Create(ctx context.Context, datasetID, fractalID, requestedBy, url, sha256 string, bytes, logs int64) (*Import, error) {
	var id int64
	err := s.pg.QueryRow(ctx, `
		INSERT INTO dataset_imports (dataset_id, fractal_id, requested_by, url, sha256, bytes_total, logs_total)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`, datasetID, fractalID, requestedBy, url, sha256, bytes, logs).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("create dataset import: %w", err)
	}
	return s.Get(ctx, id)
}

func (s *Store) Get(ctx context.Context, id int64) (*Import, error) {
	imp := &Import{}
	err := s.pg.QueryRow(ctx, `SELECT `+importColumns+`
		FROM dataset_imports i JOIN fractals f ON f.id = i.fractal_id
		WHERE i.id = $1`, id).Scan(imp.scanDest()...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get dataset import: %w", err)
	}
	return imp, nil
}

// List returns every import, newest first.
func (s *Store) List(ctx context.Context) ([]*Import, error) {
	rows, err := s.pg.Query(ctx, `SELECT `+importColumns+`
		FROM dataset_imports i JOIN fractals f ON f.id = i.fractal_id
		ORDER BY i.id DESC`)
	if err != nil {
		return nil, fmt.Errorf("list dataset imports: %w", err)
	}
	defer rows.Close()
	out := []*Import{}
	for rows.Next() {
		imp := &Import{}
		if err := rows.Scan(imp.scanDest()...); err != nil {
			return nil, fmt.Errorf("scan dataset import: %w", err)
		}
		out = append(out, imp)
	}
	return out, rows.Err()
}

// Claim takes the oldest pending import, or one whose worker stopped updating it
// for staleAfter (a crash or a lost node), and starts it over. The returned
// StartedAt identifies this claim: Progress fails once another worker reclaims it.
func (s *Store) Claim(ctx context.Context, staleAfter time.Duration) (*Import, error) {
	var id int64
	err := s.pg.QueryRow(ctx, `
		UPDATE dataset_imports
		SET status = 'downloading', bytes_done = 0, logs_done = 0, error = '',
		    started_at = clock_timestamp(), finished_at = NULL, updated_at = NOW()
		WHERE id = (
			SELECT id FROM dataset_imports
			WHERE status = 'pending'
			   OR (status IN ('downloading', 'importing') AND updated_at < NOW() - make_interval(secs => $1))
			ORDER BY id
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING id`, staleAfter.Seconds()).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim dataset import: %w", err)
	}
	return s.Get(ctx, id)
}

// Progress records how far a claimed import has got, which also keeps the claim.
func (s *Store) Progress(ctx context.Context, imp *Import) error {
	return s.held(s.pg.Exec(ctx, `
		UPDATE dataset_imports SET status = $3, bytes_done = $4, logs_done = $5, updated_at = NOW()
		WHERE id = $1 AND started_at = $2 AND status IN ('downloading', 'importing')`,
		imp.ID, imp.StartedAt, imp.Status, imp.BytesDone, imp.LogsDone))
}

// Finish ends a claimed import: succeeded without an error, failed with one.
func (s *Store) Finish(ctx context.Context, imp *Import, cause error) error {
	imp.Status, imp.Error = StatusSucceeded, ""
	if cause != nil {
		imp.Status, imp.Error = StatusFailed, cause.Error()
	}
	return s.held(s.pg.Exec(ctx, `
		UPDATE dataset_imports SET status = $3, error = $4, bytes_done = $5, logs_done = $6,
		       finished_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND started_at = $2 AND status IN ('downloading', 'importing')`,
		imp.ID, imp.StartedAt, imp.Status, imp.Error, imp.BytesDone, imp.LogsDone))
}

// Retry queues a failed import to run again from the start.
func (s *Store) Retry(ctx context.Context, id int64) error {
	return s.held(s.pg.Exec(ctx, `
		UPDATE dataset_imports SET status = 'pending', error = '', updated_at = NOW()
		WHERE id = $1 AND status = 'failed'`, id))
}

func (s *Store) held(res sql.Result, err error) error {
	if err != nil {
		return fmt.Errorf("update dataset import: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return ErrNotFound
	}
	return nil
}

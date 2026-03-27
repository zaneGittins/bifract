package feeds

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"bifract/pkg/storage"
)

// Manager handles CRUD operations for alert feeds.
type Manager struct {
	pg *storage.PostgresClient
}

// NewManager creates a new feed manager.
func NewManager(pg *storage.PostgresClient) *Manager {
	if !IsEncryptionConfigured() {
		log.Println("[Feeds] WARNING: BIFRACT_FEED_ENCRYPTION_KEY not set, feed auth tokens will be stored in plaintext")
	}
	return &Manager{pg: pg}
}

// List returns all feeds for a given fractal or prism, with alert counts.
func (m *Manager) List(ctx context.Context, fractalID, prismID string) ([]*Feed, error) {
	var whereClause string
	var arg interface{}
	if prismID != "" {
		whereClause = "f.prism_id = $1"
		arg = prismID
	} else {
		whereClause = "f.fractal_id = $1"
		arg = fractalID
	}

	query := fmt.Sprintf(`
		SELECT f.id, f.name, f.description, f.repo_url, f.branch, f.path,
		       f.auth_token, f.normalizer_id, f.sync_schedule, COALESCE(f.min_level, ''), COALESCE(f.min_status, ''),
		       f.enabled, COALESCE(f.fractal_id::text, ''), COALESCE(f.prism_id::text, ''),
		       f.last_synced_at, f.last_sync_status, f.last_sync_rule_count,
		       COALESCE(f.created_by, ''), f.created_at, f.updated_at,
		       (SELECT COUNT(*) FROM alerts a WHERE a.feed_id = f.id) as alert_count
		FROM alert_feeds f
		WHERE %s
		ORDER BY f.name
	`, whereClause)

	rows, err := m.pg.Query(ctx, query, arg)
	if err != nil {
		return nil, fmt.Errorf("list feeds: %w", err)
	}
	defer rows.Close()

	var feeds []*Feed
	for rows.Next() {
		f, err := m.scanFeed(rows)
		if err != nil {
			return nil, err
		}
		feeds = append(feeds, f)
	}
	return feeds, nil
}

// ListAllEnabled returns all enabled feeds across all fractals and prisms (for the syncer).
func (m *Manager) ListAllEnabled(ctx context.Context) ([]*Feed, error) {
	query := `
		SELECT f.id, f.name, f.description, f.repo_url, f.branch, f.path,
		       f.auth_token, f.normalizer_id, f.sync_schedule, COALESCE(f.min_level, ''), COALESCE(f.min_status, ''),
		       f.enabled, COALESCE(f.fractal_id::text, ''), COALESCE(f.prism_id::text, ''),
		       f.last_synced_at, f.last_sync_status, f.last_sync_rule_count,
		       COALESCE(f.created_by, ''), f.created_at, f.updated_at,
		       (SELECT COUNT(*) FROM alerts a WHERE a.feed_id = f.id) as alert_count
		FROM alert_feeds f
		WHERE f.enabled = true AND f.sync_schedule != 'never'
		ORDER BY f.name
	`

	rows, err := m.pg.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list enabled feeds: %w", err)
	}
	defer rows.Close()

	var feeds []*Feed
	for rows.Next() {
		f, err := m.scanFeed(rows)
		if err != nil {
			return nil, err
		}
		feeds = append(feeds, f)
	}
	return feeds, nil
}

// Get returns a single feed by ID.
func (m *Manager) Get(ctx context.Context, id string) (*Feed, error) {
	query := `
		SELECT f.id, f.name, f.description, f.repo_url, f.branch, f.path,
		       f.auth_token, f.normalizer_id, f.sync_schedule, COALESCE(f.min_level, ''), COALESCE(f.min_status, ''),
		       f.enabled, COALESCE(f.fractal_id::text, ''), COALESCE(f.prism_id::text, ''),
		       f.last_synced_at, f.last_sync_status, f.last_sync_rule_count,
		       COALESCE(f.created_by, ''), f.created_at, f.updated_at,
		       (SELECT COUNT(*) FROM alerts a WHERE a.feed_id = f.id) as alert_count
		FROM alert_feeds f
		WHERE f.id = $1
	`

	row := m.pg.QueryRow(ctx, query, id)
	return m.scanFeedRow(row)
}

// Create creates a new feed scoped to either a fractal or a prism.
func (m *Manager) Create(ctx context.Context, req CreateRequest, fractalID, prismID, createdBy string) (*Feed, error) {
	if err := m.validateRequest(req.Name, req.RepoURL, req.SyncSchedule); err != nil {
		return nil, err
	}
	if fractalID == "" && prismID == "" {
		return nil, fmt.Errorf("feed must be scoped to a fractal or prism")
	}

	// Encrypt auth token
	encToken, err := EncryptToken(req.AuthToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt token: %w", err)
	}

	branch := req.Branch
	if branch == "" {
		branch = "main"
	}

	var normalizerID interface{}
	if req.NormalizerID != "" {
		normalizerID = req.NormalizerID
	}

	var fractalIDPtr, prismIDPtr interface{}
	if fractalID != "" {
		fractalIDPtr = fractalID
	}
	if prismID != "" {
		prismIDPtr = prismID
	}

	query := `
		INSERT INTO alert_feeds (name, description, repo_url, branch, path, auth_token,
		                         normalizer_id, sync_schedule, min_level, min_status, enabled,
		                         fractal_id, prism_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		RETURNING id
	`
	var id string
	err = m.pg.QueryRow(ctx, query,
		req.Name, req.Description, req.RepoURL, branch, req.Path, encToken,
		normalizerID, req.SyncSchedule, req.MinLevel, req.MinStatus, req.Enabled,
		fractalIDPtr, prismIDPtr, createdBy,
	).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "unique") || strings.Contains(err.Error(), "duplicate") {
			return nil, fmt.Errorf("a feed with this name already exists in this scope")
		}
		return nil, fmt.Errorf("create feed: %w", err)
	}

	return m.Get(ctx, id)
}

// Update updates an existing feed.
func (m *Manager) Update(ctx context.Context, id string, req UpdateRequest) (*Feed, error) {
	if err := m.validateRequest(req.Name, req.RepoURL, req.SyncSchedule); err != nil {
		return nil, err
	}

	branch := req.Branch
	if branch == "" {
		branch = "main"
	}

	var normalizerID interface{}
	if req.NormalizerID != "" {
		normalizerID = req.NormalizerID
	}

	// Handle token update
	var tokenUpdate string
	var args []interface{}

	if req.ClearToken {
		// Explicitly clear the token
		tokenUpdate = ", auth_token = ''"
	} else if req.AuthToken != "" {
		// New token provided, encrypt it
		encToken, err := EncryptToken(req.AuthToken)
		if err != nil {
			return nil, fmt.Errorf("encrypt token: %w", err)
		}
		tokenUpdate = ", auth_token = $11"
		args = append(args, encToken)
	}
	// If neither ClearToken nor AuthToken, token is unchanged

	query := fmt.Sprintf(`
		UPDATE alert_feeds
		SET name = $1, description = $2, repo_url = $3, branch = $4, path = $5,
		    normalizer_id = $6, sync_schedule = $7, min_level = $8, min_status = $9, enabled = $10%s
		WHERE id = $%d
	`, tokenUpdate, 11+len(args))

	baseArgs := []interface{}{req.Name, req.Description, req.RepoURL, branch, req.Path, normalizerID, req.SyncSchedule, req.MinLevel, req.MinStatus, req.Enabled}
	baseArgs = append(baseArgs, args...)
	baseArgs = append(baseArgs, id)

	result, err := m.pg.Exec(ctx, query, baseArgs...)
	if err != nil {
		if strings.Contains(err.Error(), "unique") || strings.Contains(err.Error(), "duplicate") {
			return nil, fmt.Errorf("a feed with this name already exists in this scope")
		}
		return nil, fmt.Errorf("update feed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, fmt.Errorf("feed not found")
	}

	return m.Get(ctx, id)
}

// Delete removes a feed. Feed alerts are cascade-deleted via FK.
func (m *Manager) Delete(ctx context.Context, id string) error {
	result, err := m.pg.Exec(ctx, "DELETE FROM alert_feeds WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("delete feed: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("feed not found")
	}
	return nil
}

// UpdateSyncStatus updates the sync metadata after a sync completes.
func (m *Manager) UpdateSyncStatus(ctx context.Context, id, status string, ruleCount int) error {
	_, err := m.pg.Exec(ctx,
		`UPDATE alert_feeds SET last_synced_at = NOW(), last_sync_status = $1, last_sync_rule_count = $2 WHERE id = $3`,
		status, ruleCount, id)
	return err
}

// GetDecryptedToken retrieves and decrypts the auth token for a feed.
func (m *Manager) GetDecryptedToken(ctx context.Context, id string) (string, error) {
	var encToken string
	err := m.pg.QueryRow(ctx, "SELECT COALESCE(auth_token, '') FROM alert_feeds WHERE id = $1", id).Scan(&encToken)
	if err != nil {
		return "", fmt.Errorf("get feed token: %w", err)
	}
	return DecryptToken(encToken)
}

func (m *Manager) validateRequest(name, repoURL, schedule string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("feed name is required")
	}
	if strings.TrimSpace(repoURL) == "" {
		return fmt.Errorf("repository URL is required")
	}
	if !ValidSchedules[schedule] {
		return fmt.Errorf("invalid sync schedule %q, must be one of: never, hourly, daily, weekly, monthly", schedule)
	}
	return nil
}

// scanFeed scans a feed row from a Rows result.
func (m *Manager) scanFeed(rows interface{ Scan(dest ...interface{}) error }) (*Feed, error) {
	var f Feed
	var encToken string
	var normalizerID sql.NullString
	var lastSyncedAt sql.NullTime
	var createdBy sql.NullString

	err := rows.Scan(
		&f.ID, &f.Name, &f.Description, &f.RepoURL, &f.Branch, &f.Path,
		&encToken, &normalizerID, &f.SyncSchedule, &f.MinLevel, &f.MinStatus,
		&f.Enabled, &f.FractalID, &f.PrismID,
		&lastSyncedAt, &f.LastSyncStatus, &f.LastSyncRuleCount,
		&createdBy, &f.CreatedAt, &f.UpdatedAt, &f.AlertCount,
	)
	if err != nil {
		return nil, fmt.Errorf("scan feed: %w", err)
	}

	f.HasAuthToken = encToken != ""
	// Never return the actual token in API responses
	f.AuthToken = ""

	if normalizerID.Valid {
		f.NormalizerID = normalizerID.String
	}
	if lastSyncedAt.Valid {
		f.LastSyncedAt = &lastSyncedAt.Time
	}
	if createdBy.Valid {
		f.CreatedBy = createdBy.String
	}

	return &f, nil
}

// scanFeedRow scans a single feed row from QueryRow.
func (m *Manager) scanFeedRow(row interface{ Scan(dest ...interface{}) error }) (*Feed, error) {
	return m.scanFeed(row)
}

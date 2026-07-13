package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SharedLinksEnabledSetting is the Postgres settings key backing the global
// on/off toggle for the public Shared Links feature. Default off (opt-in): an
// install must explicitly enable no-auth dashboard access.
const SharedLinksEnabledSetting = "shared_links_enabled"

// DashboardSharedLink is a public, no-auth, read-only access grant to a single
// dashboard (a wallboard link). Only the SHA-256 hash of the token is persisted;
// the plaintext token is returned to the creator exactly once and never stored,
// so a leaked database cannot reveal working links.
type DashboardSharedLink struct {
	ID             string     `json:"id"`
	DashboardID    string     `json:"dashboard_id"`
	TokenPrefix    string     `json:"token_prefix"`
	Label          string     `json:"label,omitempty"`
	CreatedBy      string     `json:"created_by,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	RevokedAt      *time.Time `json:"revoked_at,omitempty"`
	LastAccessedAt *time.Time `json:"last_accessed_at,omitempty"`
}

// CreateDashboardSharedLink inserts a new link. tokenHash is the hex SHA-256 of
// the plaintext token; tokenPrefix is a short non-sensitive display fragment.
func (c *PostgresClient) CreateDashboardSharedLink(ctx context.Context, dashboardID, tokenHash, tokenPrefix, label, createdBy string, expiresAt *time.Time) (*DashboardSharedLink, error) {
	var labelPtr, createdByPtr interface{}
	if label != "" {
		labelPtr = label
	}
	if createdBy != "" {
		createdByPtr = createdBy
	}
	var l DashboardSharedLink
	var scanLabel, scanCreatedBy sql.NullString
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO dashboard_shared_links (dashboard_id, token_hash, token_prefix, label, created_by, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, dashboard_id, token_prefix, label, created_by, created_at, expires_at, revoked_at, last_accessed_at
	`, dashboardID, tokenHash, tokenPrefix, labelPtr, createdByPtr, expiresAt).Scan(
		&l.ID, &l.DashboardID, &l.TokenPrefix, &scanLabel, &scanCreatedBy,
		&l.CreatedAt, &l.ExpiresAt, &l.RevokedAt, &l.LastAccessedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared link: %w", err)
	}
	l.Label = scanLabel.String
	l.CreatedBy = scanCreatedBy.String
	return &l, nil
}

// ListDashboardSharedLinks returns the active (non-revoked) links for a dashboard.
// The token hash is never selected, so it cannot leak through the management API.
func (c *PostgresClient) ListDashboardSharedLinks(ctx context.Context, dashboardID string) ([]DashboardSharedLink, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT id, dashboard_id, token_prefix, label, created_by, created_at, expires_at, revoked_at, last_accessed_at
		FROM dashboard_shared_links
		WHERE dashboard_id = $1 AND revoked_at IS NULL
		ORDER BY created_at DESC
	`, dashboardID)
	if err != nil {
		return nil, fmt.Errorf("failed to list shared links: %w", err)
	}
	defer rows.Close()

	var links []DashboardSharedLink
	for rows.Next() {
		var l DashboardSharedLink
		var scanLabel, scanCreatedBy sql.NullString
		if err := rows.Scan(
			&l.ID, &l.DashboardID, &l.TokenPrefix, &scanLabel, &scanCreatedBy,
			&l.CreatedAt, &l.ExpiresAt, &l.RevokedAt, &l.LastAccessedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan shared link: %w", err)
		}
		l.Label = scanLabel.String
		l.CreatedBy = scanCreatedBy.String
		links = append(links, l)
	}
	return links, nil
}

// GetDashboardSharedLinkByHash resolves a link by its token hash. Validity
// (expiry/revocation) is the caller's responsibility so it can return an
// indistinguishable 404 for any failure.
func (c *PostgresClient) GetDashboardSharedLinkByHash(ctx context.Context, tokenHash string) (*DashboardSharedLink, error) {
	var l DashboardSharedLink
	var scanLabel, scanCreatedBy sql.NullString
	err := c.db.QueryRowContext(ctx, `
		SELECT id, dashboard_id, token_prefix, label, created_by, created_at, expires_at, revoked_at, last_accessed_at
		FROM dashboard_shared_links
		WHERE token_hash = $1
	`, tokenHash).Scan(
		&l.ID, &l.DashboardID, &l.TokenPrefix, &scanLabel, &scanCreatedBy,
		&l.CreatedAt, &l.ExpiresAt, &l.RevokedAt, &l.LastAccessedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("shared link not found")
		}
		return nil, fmt.Errorf("failed to get shared link: %w", err)
	}
	l.Label = scanLabel.String
	l.CreatedBy = scanCreatedBy.String
	return &l, nil
}

// RevokeDashboardSharedLink soft-deletes a link (sets revoked_at) so audit
// history is retained. Scoped by dashboard so one dashboard cannot revoke
// another's links. Returns an error when no matching active link exists.
func (c *PostgresClient) RevokeDashboardSharedLink(ctx context.Context, dashboardID, linkID string) error {
	res, err := c.db.ExecContext(ctx, `
		UPDATE dashboard_shared_links SET revoked_at = NOW()
		WHERE id = $1 AND dashboard_id = $2 AND revoked_at IS NULL
	`, linkID, dashboardID)
	if err != nil {
		return fmt.Errorf("failed to revoke shared link: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("shared link not found")
	}
	return nil
}

// TouchDashboardSharedLink best-effort records anonymous access time. Errors are
// intentionally ignored: a failed audit write must never break a read.
func (c *PostgresClient) TouchDashboardSharedLink(ctx context.Context, linkID string) {
	_, _ = c.db.ExecContext(ctx, `UPDATE dashboard_shared_links SET last_accessed_at = NOW() WHERE id = $1`, linkID)
}

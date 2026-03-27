package contextlinks

import (
	"context"
	"fmt"
	"time"

	"bifract/pkg/storage"

	"github.com/lib/pq"
)

type ContextLink struct {
	ID              string   `json:"id"`
	ShortName       string   `json:"short_name"`
	MatchFields     []string `json:"match_fields"`
	ValidationRegex string   `json:"validation_regex"`
	ContextLink     string   `json:"context_link"`
	RedirectWarning bool     `json:"redirect_warning"`
	Enabled         bool     `json:"enabled"`
	IsDefault       bool     `json:"is_default"`
	CreatedBy       string   `json:"created_by"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

type Manager struct {
	pg *storage.PostgresClient
}

func NewManager(pg *storage.PostgresClient) *Manager {
	return &Manager{pg: pg}
}

func (m *Manager) List(ctx context.Context) ([]ContextLink, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, short_name, match_fields, validation_regex, context_link,
		        redirect_warning, enabled, is_default, COALESCE(created_by, ''), created_at, updated_at
		 FROM context_links ORDER BY short_name`)
	if err != nil {
		return nil, fmt.Errorf("query context_links: %w", err)
	}
	defer rows.Close()

	var links []ContextLink
	for rows.Next() {
		var cl ContextLink
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&cl.ID, &cl.ShortName, pq.Array(&cl.MatchFields),
			&cl.ValidationRegex, &cl.ContextLink, &cl.RedirectWarning,
			&cl.Enabled, &cl.IsDefault, &cl.CreatedBy, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan context_link: %w", err)
		}
		cl.CreatedAt = createdAt.Format(time.RFC3339)
		cl.UpdatedAt = updatedAt.Format(time.RFC3339)
		links = append(links, cl)
	}
	return links, nil
}

func (m *Manager) ListEnabled(ctx context.Context) ([]ContextLink, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, short_name, match_fields, validation_regex, context_link,
		        redirect_warning, enabled, is_default, COALESCE(created_by, ''), created_at, updated_at
		 FROM context_links WHERE enabled = true ORDER BY short_name`)
	if err != nil {
		return nil, fmt.Errorf("query enabled context_links: %w", err)
	}
	defer rows.Close()

	var links []ContextLink
	for rows.Next() {
		var cl ContextLink
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&cl.ID, &cl.ShortName, pq.Array(&cl.MatchFields),
			&cl.ValidationRegex, &cl.ContextLink, &cl.RedirectWarning,
			&cl.Enabled, &cl.IsDefault, &cl.CreatedBy, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan context_link: %w", err)
		}
		cl.CreatedAt = createdAt.Format(time.RFC3339)
		cl.UpdatedAt = updatedAt.Format(time.RFC3339)
		links = append(links, cl)
	}
	return links, nil
}

func (m *Manager) Get(ctx context.Context, id string) (*ContextLink, error) {
	var cl ContextLink
	var createdAt, updatedAt time.Time
	err := m.pg.QueryRow(ctx,
		`SELECT id, short_name, match_fields, validation_regex, context_link,
		        redirect_warning, enabled, is_default, COALESCE(created_by, ''), created_at, updated_at
		 FROM context_links WHERE id = $1`, id).Scan(
		&cl.ID, &cl.ShortName, pq.Array(&cl.MatchFields),
		&cl.ValidationRegex, &cl.ContextLink, &cl.RedirectWarning,
		&cl.Enabled, &cl.IsDefault, &cl.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return nil, fmt.Errorf("get context_link %s: %w", id, err)
	}
	cl.CreatedAt = createdAt.Format(time.RFC3339)
	cl.UpdatedAt = updatedAt.Format(time.RFC3339)
	return &cl, nil
}

type CreateRequest struct {
	ShortName       string   `json:"short_name"`
	MatchFields     []string `json:"match_fields"`
	ValidationRegex string   `json:"validation_regex"`
	ContextLink     string   `json:"context_link"`
	RedirectWarning bool     `json:"redirect_warning"`
	Enabled         bool     `json:"enabled"`
}

func (m *Manager) Create(ctx context.Context, req CreateRequest, createdBy string) (*ContextLink, error) {
	var cl ContextLink
	var createdAt, updatedAt time.Time
	err := m.pg.QueryRow(ctx,
		`INSERT INTO context_links (short_name, match_fields, validation_regex, context_link, redirect_warning, enabled, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id, short_name, match_fields, validation_regex, context_link, redirect_warning, enabled, is_default, COALESCE(created_by, ''), created_at, updated_at`,
		req.ShortName, pq.Array(req.MatchFields), req.ValidationRegex, req.ContextLink,
		req.RedirectWarning, req.Enabled, createdBy).Scan(
		&cl.ID, &cl.ShortName, pq.Array(&cl.MatchFields),
		&cl.ValidationRegex, &cl.ContextLink, &cl.RedirectWarning,
		&cl.Enabled, &cl.IsDefault, &cl.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return nil, fmt.Errorf("create context_link: %w", err)
	}
	cl.CreatedAt = createdAt.Format(time.RFC3339)
	cl.UpdatedAt = updatedAt.Format(time.RFC3339)
	return &cl, nil
}

type UpdateRequest struct {
	ShortName       string   `json:"short_name"`
	MatchFields     []string `json:"match_fields"`
	ValidationRegex string   `json:"validation_regex"`
	ContextLink     string   `json:"context_link"`
	RedirectWarning bool     `json:"redirect_warning"`
	Enabled         bool     `json:"enabled"`
}

func (m *Manager) Update(ctx context.Context, id string, req UpdateRequest) (*ContextLink, error) {
	var cl ContextLink
	var createdAt, updatedAt time.Time
	err := m.pg.QueryRow(ctx,
		`UPDATE context_links
		 SET short_name = $1, match_fields = $2, validation_regex = $3, context_link = $4,
		     redirect_warning = $5, enabled = $6, updated_at = NOW()
		 WHERE id = $7
		 RETURNING id, short_name, match_fields, validation_regex, context_link, redirect_warning, enabled, is_default, COALESCE(created_by, ''), created_at, updated_at`,
		req.ShortName, pq.Array(req.MatchFields), req.ValidationRegex, req.ContextLink,
		req.RedirectWarning, req.Enabled, id).Scan(
		&cl.ID, &cl.ShortName, pq.Array(&cl.MatchFields),
		&cl.ValidationRegex, &cl.ContextLink, &cl.RedirectWarning,
		&cl.Enabled, &cl.IsDefault, &cl.CreatedBy, &createdAt, &updatedAt)
	if err != nil {
		return nil, fmt.Errorf("update context_link %s: %w", id, err)
	}
	cl.CreatedAt = createdAt.Format(time.RFC3339)
	cl.UpdatedAt = updatedAt.Format(time.RFC3339)
	return &cl, nil
}

func (m *Manager) Delete(ctx context.Context, id string) error {
	result, err := m.pg.Exec(ctx, `DELETE FROM context_links WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete context_link %s: %w", id, err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("context link not found")
	}
	return nil
}

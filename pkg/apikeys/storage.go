package apikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// Storage handles API key database operations
type Storage struct {
	pg *storage.PostgresClient
}

// NewStorage creates a new Storage instance
func NewStorage(pg *storage.PostgresClient) *Storage {
	return &Storage{pg: pg}
}

// TenantKeyPrefix names an instance-wide key, which belongs to no fractal and
// so has no scope name to carry.
const TenantKeyPrefix = "admin"

// GenerateAPIKey creates a new API key with format: bifract_<scope_name>_<random>
func (s *Storage) GenerateAPIKey(ctx context.Context, scopeName string) (string, string, error) {
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", "", fmt.Errorf("failed to generate random key: %w", err)
	}

	randomStr := hex.EncodeToString(randomBytes)

	sanitized := strings.ReplaceAll(scopeName, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")

	fullKey := fmt.Sprintf("bifract_%s_%s", sanitized, randomStr)
	keyID := randomStr[:8]

	return fullKey, keyID, nil
}

// HashKey creates SHA-256 hash of API key for storage
func (s *Storage) HashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// selectColumns is the standard column list for API key queries.
const selectColumns = `ak.id, ak.name, ak.description, ak.key_id,
	COALESCE(ak.fractal_id::text, ''), COALESCE(f.name, ''),
	COALESCE(ak.prism_id::text, ''), COALESCE(p.name, ''),
	COALESCE(ak.created_by, ''), ak.expires_at, ak.is_active, ak.role, ak.tenant_admin,
	ak.created_at, ak.updated_at, ak.last_used_at, ak.usage_count`

// fromClause is the standard FROM + LEFT JOINs for API key queries.
const fromClause = `FROM api_keys ak
	LEFT JOIN fractals f ON ak.fractal_id = f.id
	LEFT JOIN prisms p ON ak.prism_id = p.id`

// scopeFilter confines a key operation to the scope that owns the key. An empty
// column is the instance-wide scope: a tenant admin key holds neither a fractal
// nor a prism.
type scopeFilter struct {
	column string
	id     string
}

func fractalScope(id string) scopeFilter { return scopeFilter{column: "fractal_id", id: id} }
func prismScope(id string) scopeFilter   { return scopeFilter{column: "prism_id", id: id} }

var tenantScope = scopeFilter{}

// where renders the predicate for this scope, using alias ("ak." or "") and the
// next free placeholder index.
func (f scopeFilter) where(alias string, next int) (string, []interface{}) {
	if f.column == "" {
		return fmt.Sprintf("%[1]sfractal_id IS NULL AND %[1]sprism_id IS NULL AND %[1]stenant_admin", alias), nil
	}
	return fmt.Sprintf("%s%s = $%d", alias, f.column, next), []interface{}{f.id}
}

// scanAPIKey scans a row into an APIKey struct.
func scanAPIKey(scanner interface {
	Scan(dest ...interface{}) error
}) (*APIKey, error) {
	var key APIKey

	err := scanner.Scan(
		&key.ID, &key.Name, &key.Description, &key.KeyID,
		&key.FractalID, &key.FractalName,
		&key.PrismID, &key.PrismName,
		&key.CreatedBy, &key.ExpiresAt, &key.IsActive, &key.Role, &key.TenantAdmin,
		&key.CreatedAt, &key.UpdatedAt, &key.LastUsedAt, &key.UsageCount,
	)
	if err != nil {
		return nil, err
	}

	return &key, nil
}

// CreateFractalAPIKey stores a new fractal-scoped API key.
func (s *Storage) CreateFractalAPIKey(ctx context.Context, req CreateAPIKeyRequest, fractalID, username, fullKey, keyID string) (*APIKey, error) {
	return s.createAPIKey(ctx, req, username, fullKey, keyID, fractalID, "")
}

// CreatePrismAPIKey stores a new prism-scoped API key.
func (s *Storage) CreatePrismAPIKey(ctx context.Context, req CreateAPIKeyRequest, prismID, username, fullKey, keyID string) (*APIKey, error) {
	return s.createAPIKey(ctx, req, username, fullKey, keyID, "", prismID)
}

// CreateTenantAPIKey stores a new instance-wide key. It is bound to no fractal
// or prism, so its authorization comes only from the tenant admin grant.
func (s *Storage) CreateTenantAPIKey(ctx context.Context, req CreateAPIKeyRequest, username, fullKey, keyID string) (*APIKey, error) {
	if !req.TenantAdmin {
		return nil, fmt.Errorf("an unscoped key must be a tenant admin key")
	}
	return s.createAPIKey(ctx, req, username, fullKey, keyID, "", "")
}

func (s *Storage) createAPIKey(ctx context.Context, req CreateAPIKeyRequest, username, fullKey, keyID, fractalID, prismID string) (*APIKey, error) {
	if fractalID != "" && prismID != "" {
		return nil, fmt.Errorf("a key is scoped to a fractal or a prism, not both")
	}
	if (fractalID == "" && prismID == "") != req.TenantAdmin {
		return nil, fmt.Errorf("a tenant admin key holds no scope, and a scoped key is not a tenant admin")
	}

	keyHash := s.HashKey(fullKey)
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var fractalArg, prismArg interface{}
	if fractalID != "" {
		fractalArg = fractalID
	}
	if prismID != "" {
		prismArg = prismID
	}

	row := s.pg.DB().QueryRowContext(ctx, `
		INSERT INTO api_keys (name, description, key_id, key_hash, fractal_id, prism_id, created_by, expires_at, role, tenant_admin)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`, req.Name, req.Description, keyID, keyHash, fractalArg, prismArg, storage.NullableUser(username), req.ExpiresAt, req.Role, req.TenantAdmin)

	var id string
	if err := row.Scan(&id); err != nil {
		return nil, fmt.Errorf("failed to create API key: %w", err)
	}

	return s.getAPIKeyByID(ctx, id)
}

// ValidateAPIKey checks if an API key is valid and returns associated data
func (s *Storage) ValidateAPIKey(ctx context.Context, key string) (*ValidatedAPIKey, error) {
	keyHash := s.HashKey(key)

	row := s.pg.DB().QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		%s
		WHERE ak.key_hash = $1 AND ak.is_active = true
	`, selectColumns, fromClause), keyHash)

	apiKey, err := scanAPIKey(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid API key")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to validate API key: %w", err)
	}

	if apiKey.ExpiresAt != nil && time.Now().After(*apiKey.ExpiresAt) {
		return nil, fmt.Errorf("API key expired")
	}

	return &ValidatedAPIKey{APIKey: *apiKey}, nil
}

// UpdateLastUsed updates the last used timestamp and increments usage count
func (s *Storage) UpdateLastUsed(ctx context.Context, keyID string) error {
	_, err := s.pg.DB().ExecContext(ctx, `
		UPDATE api_keys
		SET last_used_at = NOW(), usage_count = usage_count + 1, updated_at = NOW()
		WHERE key_id = $1
	`, keyID)
	return err
}

// ListAllAPIKeys returns every key in the instance, newest first. Tenant admins
// use it to see the whole issued surface in one place, including the
// instance-wide grants that no single fractal page would show.
func (s *Storage) ListAllAPIKeys(ctx context.Context) ([]APIKey, error) {
	return s.listAPIKeys(ctx, "TRUE")
}

// ListAPIKeysByFractal returns all API keys for a specific fractal.
func (s *Storage) ListAPIKeysByFractal(ctx context.Context, fractalID string) ([]APIKey, error) {
	return s.listAPIKeys(ctx, "ak.fractal_id = $1", fractalID)
}

// ListAPIKeysByPrism returns all API keys for a specific prism.
func (s *Storage) ListAPIKeysByPrism(ctx context.Context, prismID string) ([]APIKey, error) {
	return s.listAPIKeys(ctx, "ak.prism_id = $1", prismID)
}

func (s *Storage) listAPIKeys(ctx context.Context, where string, args ...interface{}) ([]APIKey, error) {
	query := fmt.Sprintf(`SELECT %s %s WHERE %s ORDER BY ak.created_at DESC`, selectColumns, fromClause, where)

	rows, err := s.pg.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list API keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		key, err := scanAPIKey(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan API key: %w", err)
		}
		keys = append(keys, *key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating API keys: %w", err)
	}

	return keys, nil
}

// getAPIKeyByID retrieves an API key by its primary key ID (internal helper).
func (s *Storage) getAPIKeyByID(ctx context.Context, id string) (*APIKey, error) {
	row := s.pg.DB().QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s %s WHERE ak.id = $1
	`, selectColumns, fromClause), id)

	key, err := scanAPIKey(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("API key not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get API key: %w", err)
	}
	return key, nil
}

// GetFractalAPIKey retrieves a specific API key scoped to a fractal.
func (s *Storage) GetFractalAPIKey(ctx context.Context, keyID, fractalID string) (*APIKey, error) {
	return s.getAPIKeyScoped(ctx, keyID, fractalScope(fractalID))
}

// GetPrismAPIKey retrieves a specific API key scoped to a prism.
func (s *Storage) GetPrismAPIKey(ctx context.Context, keyID, prismID string) (*APIKey, error) {
	return s.getAPIKeyScoped(ctx, keyID, prismScope(prismID))
}

// GetTenantAPIKey retrieves a specific instance-wide API key.
func (s *Storage) GetTenantAPIKey(ctx context.Context, keyID string) (*APIKey, error) {
	return s.getAPIKeyScoped(ctx, keyID, tenantScope)
}

func (s *Storage) getAPIKeyScoped(ctx context.Context, keyID string, scope scopeFilter) (*APIKey, error) {
	where, args := scope.where("ak.", 2)
	row := s.pg.DB().QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s %s WHERE ak.id = $1 AND %s
	`, selectColumns, fromClause, where), append([]interface{}{keyID}, args...)...)

	key, err := scanAPIKey(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("API key not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get API key: %w", err)
	}
	return key, nil
}

// UpdateFractalAPIKey updates a fractal-scoped API key.
func (s *Storage) UpdateFractalAPIKey(ctx context.Context, keyID, fractalID string, req UpdateAPIKeyRequest) (*APIKey, error) {
	return s.updateAPIKey(ctx, keyID, fractalScope(fractalID), req)
}

// UpdatePrismAPIKey updates a prism-scoped API key.
func (s *Storage) UpdatePrismAPIKey(ctx context.Context, keyID, prismID string, req UpdateAPIKeyRequest) (*APIKey, error) {
	return s.updateAPIKey(ctx, keyID, prismScope(prismID), req)
}

// UpdateTenantAPIKey updates an instance-wide API key.
func (s *Storage) UpdateTenantAPIKey(ctx context.Context, keyID string, req UpdateAPIKeyRequest) (*APIKey, error) {
	return s.updateAPIKey(ctx, keyID, tenantScope, req)
}

func (s *Storage) updateAPIKey(ctx context.Context, keyID string, scope scopeFilter, req UpdateAPIKeyRequest) (*APIKey, error) {
	setParts := []string{}
	args := []interface{}{}
	argIndex := 1

	if req.Name != nil {
		setParts = append(setParts, fmt.Sprintf("name = $%d", argIndex))
		args = append(args, *req.Name)
		argIndex++
	}

	if req.Description != nil {
		setParts = append(setParts, fmt.Sprintf("description = $%d", argIndex))
		args = append(args, *req.Description)
		argIndex++
	}

	if req.ExpiresAt != nil {
		setParts = append(setParts, fmt.Sprintf("expires_at = $%d", argIndex))
		args = append(args, *req.ExpiresAt)
		argIndex++
	}

	if req.IsActive != nil {
		setParts = append(setParts, fmt.Sprintf("is_active = $%d", argIndex))
		args = append(args, *req.IsActive)
		argIndex++
	}

	if req.Role != nil {
		setParts = append(setParts, fmt.Sprintf("role = $%d", argIndex))
		args = append(args, *req.Role)
		argIndex++
	}

	if req.TenantAdmin != nil {
		setParts = append(setParts, fmt.Sprintf("tenant_admin = $%d", argIndex))
		args = append(args, *req.TenantAdmin)
		argIndex++
	}

	if len(setParts) == 0 {
		return nil, fmt.Errorf("no fields to update")
	}

	setParts = append(setParts, "updated_at = NOW()")

	args = append(args, keyID)
	where, scopeArgs := scope.where("", argIndex+1)
	args = append(args, scopeArgs...)

	query := fmt.Sprintf(`
		UPDATE api_keys
		SET %s
		WHERE id = $%d AND %s
	`, strings.Join(setParts, ", "), argIndex, where)

	result, err := s.pg.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update API key: %w", err)
	}
	if err := matched(result); err != nil {
		return nil, err
	}

	return s.getAPIKeyByID(ctx, keyID)
}

// DeleteFractalAPIKey removes a fractal-scoped API key.
func (s *Storage) DeleteFractalAPIKey(ctx context.Context, keyID, fractalID string) error {
	return s.deleteAPIKey(ctx, keyID, fractalScope(fractalID))
}

// DeletePrismAPIKey removes a prism-scoped API key.
func (s *Storage) DeletePrismAPIKey(ctx context.Context, keyID, prismID string) error {
	return s.deleteAPIKey(ctx, keyID, prismScope(prismID))
}

// DeleteTenantAPIKey removes an instance-wide API key.
func (s *Storage) DeleteTenantAPIKey(ctx context.Context, keyID string) error {
	return s.deleteAPIKey(ctx, keyID, tenantScope)
}

func (s *Storage) deleteAPIKey(ctx context.Context, keyID string, scope scopeFilter) error {
	where, args := scope.where("", 2)
	result, err := s.pg.DB().ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM api_keys
		WHERE id = $1 AND %s
	`, where), append([]interface{}{keyID}, args...)...)

	if err != nil {
		return fmt.Errorf("failed to delete API key: %w", err)
	}

	return matched(result)
}

// ErrKeyNotFound is returned when a key does not exist in the scope the caller
// addressed it through.
var ErrKeyNotFound = errors.New("API key not found")

// matched reports whether a scoped write hit a row. A miss means the key does
// not exist in that scope, which must not read back as success.
func matched(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return ErrKeyNotFound
	}
	return nil
}

// ToggleFractalAPIKey toggles the active status of a fractal-scoped API key.
func (s *Storage) ToggleFractalAPIKey(ctx context.Context, keyID, fractalID string) (*APIKey, error) {
	return s.toggleAPIKey(ctx, keyID, fractalScope(fractalID))
}

// TogglePrismAPIKey toggles the active status of a prism-scoped API key.
func (s *Storage) TogglePrismAPIKey(ctx context.Context, keyID, prismID string) (*APIKey, error) {
	return s.toggleAPIKey(ctx, keyID, prismScope(prismID))
}

// ToggleTenantAPIKey toggles the active status of an instance-wide API key.
func (s *Storage) ToggleTenantAPIKey(ctx context.Context, keyID string) (*APIKey, error) {
	return s.toggleAPIKey(ctx, keyID, tenantScope)
}

func (s *Storage) toggleAPIKey(ctx context.Context, keyID string, scope scopeFilter) (*APIKey, error) {
	where, args := scope.where("", 2)
	result, err := s.pg.DB().ExecContext(ctx, fmt.Sprintf(`
		UPDATE api_keys
		SET is_active = NOT is_active, updated_at = NOW()
		WHERE id = $1 AND %s
	`, where), append([]interface{}{keyID}, args...)...)

	if err != nil {
		return nil, fmt.Errorf("failed to toggle API key: %w", err)
	}
	if err := matched(result); err != nil {
		return nil, err
	}

	return s.getAPIKeyByID(ctx, keyID)
}

// GetAPIKeysByUser returns all API keys created by a specific user
func (s *Storage) GetAPIKeysByUser(ctx context.Context, username string) ([]APIKey, error) {
	query := fmt.Sprintf(`SELECT %s %s WHERE ak.created_by = $1 ORDER BY ak.created_at DESC`, selectColumns, fromClause)

	rows, err := s.pg.DB().QueryContext(ctx, query, username)
	if err != nil {
		return nil, fmt.Errorf("failed to list API keys by user: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		key, err := scanAPIKey(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan API key: %w", err)
		}
		keys = append(keys, *key)
	}

	return keys, nil
}

// GetFractalName returns the name of a fractal by ID.
func (s *Storage) GetFractalName(ctx context.Context, fractalID string) (string, error) {
	var name string
	err := s.pg.DB().QueryRowContext(ctx, `SELECT name FROM fractals WHERE id = $1`, fractalID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

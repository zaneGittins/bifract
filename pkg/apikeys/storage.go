package apikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
	COALESCE(ak.created_by, ''), ak.expires_at, ak.is_active, ak.permissions,
	ak.created_at, ak.updated_at, ak.last_used_at, ak.usage_count`

// fromClause is the standard FROM + LEFT JOINs for API key queries.
const fromClause = `FROM api_keys ak
	LEFT JOIN fractals f ON ak.fractal_id = f.id
	LEFT JOIN prisms p ON ak.prism_id = p.id`

// scanAPIKey scans a row into an APIKey struct and parses its permissions JSON.
func scanAPIKey(scanner interface{ Scan(dest ...interface{}) error }) (*APIKey, error) {
	var key APIKey
	var permissionsJSON string

	err := scanner.Scan(
		&key.ID, &key.Name, &key.Description, &key.KeyID,
		&key.FractalID, &key.FractalName,
		&key.PrismID, &key.PrismName,
		&key.CreatedBy, &key.ExpiresAt, &key.IsActive, &permissionsJSON,
		&key.CreatedAt, &key.UpdatedAt, &key.LastUsedAt, &key.UsageCount,
	)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(permissionsJSON), &key.Permissions); err != nil {
		return nil, fmt.Errorf("failed to parse permissions: %w", err)
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

func (s *Storage) createAPIKey(ctx context.Context, req CreateAPIKeyRequest, username, fullKey, keyID, fractalID, prismID string) (*APIKey, error) {
	if (fractalID == "") == (prismID == "") {
		return nil, fmt.Errorf("exactly one of fractalID or prismID must be provided")
	}

	keyHash := s.HashKey(fullKey)
	permissions, err := ValidatePermissions(req.Permissions)
	if err != nil {
		return nil, fmt.Errorf("invalid permissions: %w", err)
	}

	permissionsJSON, err := json.Marshal(permissions)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal permissions: %w", err)
	}

	var fractalArg, prismArg interface{}
	if fractalID != "" {
		fractalArg = fractalID
	}
	if prismID != "" {
		prismArg = prismID
	}

	row := s.pg.DB().QueryRowContext(ctx, `
		INSERT INTO api_keys (name, description, key_id, key_hash, fractal_id, prism_id, created_by, expires_at, permissions)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, req.Name, req.Description, keyID, keyHash, fractalArg, prismArg, username, req.ExpiresAt, string(permissionsJSON))

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

// ListAPIKeysByFractal returns all API keys for a specific fractal.
func (s *Storage) ListAPIKeysByFractal(ctx context.Context, fractalID string) ([]APIKey, error) {
	return s.listAPIKeys(ctx, "ak.fractal_id = $1", fractalID)
}

// ListAPIKeysByPrism returns all API keys for a specific prism.
func (s *Storage) ListAPIKeysByPrism(ctx context.Context, prismID string) ([]APIKey, error) {
	return s.listAPIKeys(ctx, "ak.prism_id = $1", prismID)
}

func (s *Storage) listAPIKeys(ctx context.Context, where string, scopeID string) ([]APIKey, error) {
	query := fmt.Sprintf(`SELECT %s %s WHERE %s ORDER BY ak.created_at DESC`, selectColumns, fromClause, where)

	rows, err := s.pg.DB().QueryContext(ctx, query, scopeID)
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
	return s.getAPIKeyScoped(ctx, keyID, "ak.fractal_id", fractalID)
}

// GetPrismAPIKey retrieves a specific API key scoped to a prism.
func (s *Storage) GetPrismAPIKey(ctx context.Context, keyID, prismID string) (*APIKey, error) {
	return s.getAPIKeyScoped(ctx, keyID, "ak.prism_id", prismID)
}

func (s *Storage) getAPIKeyScoped(ctx context.Context, keyID, scopeCol, scopeID string) (*APIKey, error) {
	row := s.pg.DB().QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s %s WHERE ak.id = $1 AND %s = $2
	`, selectColumns, fromClause, scopeCol), keyID, scopeID)

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
	return s.updateAPIKey(ctx, keyID, "fractal_id", fractalID, req)
}

// UpdatePrismAPIKey updates a prism-scoped API key.
func (s *Storage) UpdatePrismAPIKey(ctx context.Context, keyID, prismID string, req UpdateAPIKeyRequest) (*APIKey, error) {
	return s.updateAPIKey(ctx, keyID, "prism_id", prismID, req)
}

func (s *Storage) updateAPIKey(ctx context.Context, keyID, scopeCol, scopeID string, req UpdateAPIKeyRequest) (*APIKey, error) {
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

	if req.Permissions != nil {
		validated, err := ValidatePermissions(req.Permissions)
		if err != nil {
			return nil, fmt.Errorf("invalid permissions: %w", err)
		}
		permJSON, err := json.Marshal(validated)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal permissions: %w", err)
		}
		setParts = append(setParts, fmt.Sprintf("permissions = $%d", argIndex))
		args = append(args, string(permJSON))
		argIndex++
	}

	if len(setParts) == 0 {
		return nil, fmt.Errorf("no fields to update")
	}

	setParts = append(setParts, "updated_at = NOW()")

	args = append(args, keyID, scopeID)

	query := fmt.Sprintf(`
		UPDATE api_keys
		SET %s
		WHERE id = $%d AND %s = $%d
	`, strings.Join(setParts, ", "), argIndex, scopeCol, argIndex+1)

	_, err := s.pg.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update API key: %w", err)
	}

	return s.getAPIKeyByID(ctx, keyID)
}

// DeleteFractalAPIKey removes a fractal-scoped API key.
func (s *Storage) DeleteFractalAPIKey(ctx context.Context, keyID, fractalID string) error {
	return s.deleteAPIKey(ctx, keyID, "fractal_id", fractalID)
}

// DeletePrismAPIKey removes a prism-scoped API key.
func (s *Storage) DeletePrismAPIKey(ctx context.Context, keyID, prismID string) error {
	return s.deleteAPIKey(ctx, keyID, "prism_id", prismID)
}

func (s *Storage) deleteAPIKey(ctx context.Context, keyID, scopeCol, scopeID string) error {
	result, err := s.pg.DB().ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM api_keys
		WHERE id = $1 AND %s = $2
	`, scopeCol), keyID, scopeID)

	if err != nil {
		return fmt.Errorf("failed to delete API key: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("API key not found")
	}

	return nil
}

// ToggleFractalAPIKey toggles the active status of a fractal-scoped API key.
func (s *Storage) ToggleFractalAPIKey(ctx context.Context, keyID, fractalID string) (*APIKey, error) {
	return s.toggleAPIKey(ctx, keyID, "fractal_id", fractalID)
}

// TogglePrismAPIKey toggles the active status of a prism-scoped API key.
func (s *Storage) TogglePrismAPIKey(ctx context.Context, keyID, prismID string) (*APIKey, error) {
	return s.toggleAPIKey(ctx, keyID, "prism_id", prismID)
}

func (s *Storage) toggleAPIKey(ctx context.Context, keyID, scopeCol, scopeID string) (*APIKey, error) {
	_, err := s.pg.DB().ExecContext(ctx, fmt.Sprintf(`
		UPDATE api_keys
		SET is_active = NOT is_active, updated_at = NOW()
		WHERE id = $1 AND %s = $2
	`, scopeCol), keyID, scopeID)

	if err != nil {
		return nil, fmt.Errorf("failed to toggle API key: %w", err)
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

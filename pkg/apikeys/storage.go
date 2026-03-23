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

// GenerateAPIKey creates a new API key with format: bifract_<fractal_name>_<random>
func (s *Storage) GenerateAPIKey(ctx context.Context, fractalName string) (string, string, error) {
	// Generate 32 random bytes (64 hex chars)
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", "", fmt.Errorf("failed to generate random key: %w", err)
	}

	randomStr := hex.EncodeToString(randomBytes)

	// Sanitize fractal name for key format (replace non-alphanumeric with underscores)
	sanitizedFractalName := strings.ReplaceAll(fractalName, "-", "_")
	sanitizedFractalName = strings.ReplaceAll(sanitizedFractalName, " ", "_")

	fullKey := fmt.Sprintf("bifract_%s_%s", sanitizedFractalName, randomStr)

	// Create public key ID (first 8 chars of random part to ensure uniqueness)
	keyID := randomStr[:8]

	return fullKey, keyID, nil
}

// HashKey creates SHA-256 hash of API key for storage
func (s *Storage) HashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// CreateAPIKey stores a new API key in the database
func (s *Storage) CreateAPIKey(ctx context.Context, req CreateAPIKeyRequest, fractalID, username, fullKey, keyID string) (*APIKey, error) {
	keyHash := s.HashKey(fullKey)
	permissions, err := ValidatePermissions(req.Permissions)
	if err != nil {
		return nil, fmt.Errorf("invalid permissions: %w", err)
	}

	// Convert permissions to JSON
	permissionsJSON, err := json.Marshal(permissions)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal permissions: %w", err)
	}

	var apiKey APIKey
	var permissionsStr string
	err = s.pg.DB().QueryRowContext(ctx, `
		INSERT INTO api_keys (name, description, key_id, key_hash, fractal_id, created_by, expires_at, permissions)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, name, description, key_id, fractal_id, COALESCE(created_by, ''), expires_at,
		          is_active, permissions, created_at, updated_at, last_used_at, usage_count
	`, req.Name, req.Description, keyID, keyHash, fractalID, username, req.ExpiresAt, string(permissionsJSON)).Scan(
		&apiKey.ID, &apiKey.Name, &apiKey.Description, &apiKey.KeyID,
		&apiKey.FractalID, &apiKey.CreatedBy, &apiKey.ExpiresAt,
		&apiKey.IsActive, &permissionsStr, &apiKey.CreatedAt,
		&apiKey.UpdatedAt, &apiKey.LastUsedAt, &apiKey.UsageCount,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create API key: %w", err)
	}

	// Parse permissions JSON back into map
	if err := json.Unmarshal([]byte(permissionsStr), &apiKey.Permissions); err != nil {
		return nil, fmt.Errorf("failed to parse permissions: %w", err)
	}

	return &apiKey, nil
}

// ValidateAPIKey checks if an API key is valid and returns associated data
func (s *Storage) ValidateAPIKey(ctx context.Context, key string) (*ValidatedAPIKey, error) {
	keyHash := s.HashKey(key)

	var apiKey ValidatedAPIKey
	var permissionsJSON string

	err := s.pg.DB().QueryRowContext(ctx, `
		SELECT ak.id, ak.name, ak.description, ak.key_id, ak.fractal_id, i.name as fractal_name,
		       COALESCE(ak.created_by, ''), ak.expires_at, ak.is_active, ak.permissions,
		       ak.created_at, ak.updated_at, ak.last_used_at, ak.usage_count
		FROM api_keys ak
		JOIN fractals i ON ak.fractal_id = i.id
		WHERE ak.key_hash = $1 AND ak.is_active = true
	`, keyHash).Scan(
		&apiKey.ID, &apiKey.Name, &apiKey.Description, &apiKey.KeyID,
		&apiKey.FractalID, &apiKey.FractalName, &apiKey.CreatedBy,
		&apiKey.ExpiresAt, &apiKey.IsActive, &permissionsJSON,
		&apiKey.CreatedAt, &apiKey.UpdatedAt, &apiKey.LastUsedAt, &apiKey.UsageCount,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid API key")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to validate API key: %w", err)
	}

	// Parse permissions JSON
	if err := json.Unmarshal([]byte(permissionsJSON), &apiKey.Permissions); err != nil {
		return nil, fmt.Errorf("failed to parse permissions: %w", err)
	}

	// Check expiration
	if apiKey.ExpiresAt != nil && time.Now().After(*apiKey.ExpiresAt) {
		return nil, fmt.Errorf("API key expired")
	}

	return &apiKey, nil
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

// ListAPIKeys returns all API keys for a specific fractal
func (s *Storage) ListAPIKeys(ctx context.Context, fractalID string) ([]APIKey, error) {
	rows, err := s.pg.DB().QueryContext(ctx, `
		SELECT ak.id, ak.name, ak.description, ak.key_id, ak.fractal_id, i.name as fractal_name,
		       COALESCE(ak.created_by, ''), ak.expires_at, ak.is_active, ak.permissions,
		       ak.created_at, ak.updated_at, ak.last_used_at, ak.usage_count
		FROM api_keys ak
		JOIN fractals i ON ak.fractal_id = i.id
		WHERE ak.fractal_id = $1
		ORDER BY ak.created_at DESC
	`, fractalID)

	if err != nil {
		return nil, fmt.Errorf("failed to list API keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var key APIKey
		var permissionsJSON string

		err := rows.Scan(
			&key.ID, &key.Name, &key.Description, &key.KeyID,
			&key.FractalID, &key.FractalName, &key.CreatedBy,
			&key.ExpiresAt, &key.IsActive, &permissionsJSON,
			&key.CreatedAt, &key.UpdatedAt, &key.LastUsedAt, &key.UsageCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan API key: %w", err)
		}

		// Parse permissions JSON
		if err := json.Unmarshal([]byte(permissionsJSON), &key.Permissions); err != nil {
			return nil, fmt.Errorf("failed to parse permissions for key %s: %w", key.ID, err)
		}

		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating API keys: %w", err)
	}

	return keys, nil
}

// GetAPIKey retrieves a specific API key by ID
func (s *Storage) GetAPIKey(ctx context.Context, keyID, fractalID string) (*APIKey, error) {
	var apiKey APIKey
	var permissionsJSON string

	err := s.pg.DB().QueryRowContext(ctx, `
		SELECT ak.id, ak.name, ak.description, ak.key_id, ak.fractal_id, i.name as fractal_name,
		       COALESCE(ak.created_by, ''), ak.expires_at, ak.is_active, ak.permissions,
		       ak.created_at, ak.updated_at, ak.last_used_at, ak.usage_count
		FROM api_keys ak
		JOIN fractals i ON ak.fractal_id = i.id
		WHERE ak.id = $1 AND ak.fractal_id = $2
	`, keyID, fractalID).Scan(
		&apiKey.ID, &apiKey.Name, &apiKey.Description, &apiKey.KeyID,
		&apiKey.FractalID, &apiKey.FractalName, &apiKey.CreatedBy,
		&apiKey.ExpiresAt, &apiKey.IsActive, &permissionsJSON,
		&apiKey.CreatedAt, &apiKey.UpdatedAt, &apiKey.LastUsedAt, &apiKey.UsageCount,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("API key not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get API key: %w", err)
	}

	// Parse permissions JSON
	if err := json.Unmarshal([]byte(permissionsJSON), &apiKey.Permissions); err != nil {
		return nil, fmt.Errorf("failed to parse permissions: %w", err)
	}

	return &apiKey, nil
}

// UpdateAPIKey updates an existing API key
func (s *Storage) UpdateAPIKey(ctx context.Context, keyID, fractalID string, req UpdateAPIKeyRequest) (*APIKey, error) {
	// Build dynamic update query
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

	// Always update the updated_at timestamp
	setParts = append(setParts, "updated_at = NOW()")

	// Add WHERE clause arguments
	args = append(args, keyID, fractalID)

	query := fmt.Sprintf(`
		UPDATE api_keys
		SET %s
		WHERE id = $%d AND fractal_id = $%d
	`, strings.Join(setParts, ", "), argIndex, argIndex+1)

	_, err := s.pg.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update API key: %w", err)
	}

	// Return the updated API key
	return s.GetAPIKey(ctx, keyID, fractalID)
}

// DeleteAPIKey removes an API key from the database
func (s *Storage) DeleteAPIKey(ctx context.Context, keyID, fractalID string) error {
	result, err := s.pg.DB().ExecContext(ctx, `
		DELETE FROM api_keys
		WHERE id = $1 AND fractal_id = $2
	`, keyID, fractalID)

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

// ToggleAPIKey toggles the active status of an API key
func (s *Storage) ToggleAPIKey(ctx context.Context, keyID, fractalID string) (*APIKey, error) {
	_, err := s.pg.DB().ExecContext(ctx, `
		UPDATE api_keys
		SET is_active = NOT is_active, updated_at = NOW()
		WHERE id = $1 AND fractal_id = $2
	`, keyID, fractalID)

	if err != nil {
		return nil, fmt.Errorf("failed to toggle API key: %w", err)
	}

	// Return the updated API key
	return s.GetAPIKey(ctx, keyID, fractalID)
}

// parsePermissions is a helper function to parse permissions JSON stored in database
func (s *Storage) parsePermissions(apiKey *APIKey) error {
	// The permissions are parsed via json.Unmarshal in the calling functions
	// This function exists for consistency and future extensibility
	if apiKey.Permissions == nil {
		apiKey.Permissions = DefaultPermissions()
	}
	return nil
}

// GetAPIKeysByUser returns all API keys created by a specific user
func (s *Storage) GetAPIKeysByUser(ctx context.Context, username string) ([]APIKey, error) {
	rows, err := s.pg.DB().QueryContext(ctx, `
		SELECT ak.id, ak.name, ak.description, ak.key_id, ak.fractal_id, i.name as fractal_name,
		       COALESCE(ak.created_by, ''), ak.expires_at, ak.is_active, ak.permissions,
		       ak.created_at, ak.updated_at, ak.last_used_at, ak.usage_count
		FROM api_keys ak
		JOIN fractals i ON ak.fractal_id = i.id
		WHERE ak.created_by = $1
		ORDER BY ak.created_at DESC
	`, username)

	if err != nil {
		return nil, fmt.Errorf("failed to list API keys by user: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var key APIKey
		var permissionsJSON string

		err := rows.Scan(
			&key.ID, &key.Name, &key.Description, &key.KeyID,
			&key.FractalID, &key.FractalName, &key.CreatedBy,
			&key.ExpiresAt, &key.IsActive, &permissionsJSON,
			&key.CreatedAt, &key.UpdatedAt, &key.LastUsedAt, &key.UsageCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan API key: %w", err)
		}

		// Parse permissions JSON
		if err := json.Unmarshal([]byte(permissionsJSON), &key.Permissions); err != nil {
			return nil, fmt.Errorf("failed to parse permissions for key %s: %w", key.ID, err)
		}

		keys = append(keys, key)
	}

	return keys, nil
}
package ingesttokens

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"bifract/pkg/fractals"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"
)

type Storage struct {
	pg *storage.PostgresClient
}

func NewStorage(pg *storage.PostgresClient) *Storage {
	return &Storage{pg: pg}
}

// GenerateToken creates a new ingest token: bifract_ingest_{32_hex_chars}.
// Returns (fullToken, prefix).
func GenerateToken() (string, string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	random := hex.EncodeToString(b)
	full := "bifract_ingest_" + random
	prefix := random[:12]
	return full, prefix, nil
}

func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// getDefaultNormalizerID returns the ID of the default normalizer, or nil if none exists.
func (s *Storage) getDefaultNormalizerID(ctx context.Context) *string {
	var id string
	err := s.pg.DB().QueryRowContext(ctx,
		`SELECT id FROM normalizers WHERE is_default = true LIMIT 1`).Scan(&id)
	if err != nil {
		return nil
	}
	return &id
}

func (s *Storage) CreateToken(ctx context.Context, req CreateTokenRequest, fractalID, username string) (*IngestToken, string, error) {
	fullToken, prefix, err := GenerateToken()
	if err != nil {
		return nil, "", err
	}
	tokenHash := HashToken(fullToken)

	parserType := req.ParserType
	if parserType == "" {
		parserType = "json"
	}
	if parserType != "json" && parserType != "kv" && parserType != "syslog" {
		return nil, "", fmt.Errorf("invalid parser_type: must be 'json', 'kv', or 'syslog'")
	}

	// Determine normalizer_id: use provided, or default to the default normalizer
	var normalizerID *string
	if req.NormalizerID != nil {
		normalizerID = req.NormalizerID
	} else {
		normalizerID = s.getDefaultNormalizerID(ctx)
	}

	tsFieldsJSON, _ := json.Marshal(req.TimestampFields)
	if req.TimestampFields == nil {
		tsFieldsJSON = []byte("[]")
	}

	token := &IngestToken{}
	var tsRaw []byte
	var normID, normName sql.NullString
	err = s.pg.DB().QueryRowContext(ctx, `
		INSERT INTO ingest_tokens (name, description, token_prefix, token_hash, token_value, fractal_id,
			parser_type, normalizer_id, timestamp_fields, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id, name, description, token_prefix, token_value, fractal_id,
			parser_type, normalizer_id,
			(SELECT n.name FROM normalizers n WHERE n.id = ingest_tokens.normalizer_id),
			timestamp_fields, is_active, is_default,
			COALESCE(created_by, ''), created_at, updated_at, last_used_at, usage_count, log_count
	`, req.Name, req.Description, prefix, tokenHash, fullToken, fractalID,
		parserType, normalizerID, string(tsFieldsJSON), username,
	).Scan(
		&token.ID, &token.Name, &token.Description, &token.TokenPrefix, &token.TokenValue, &token.FractalID,
		&token.ParserType, &normID, &normName,
		&tsRaw, &token.IsActive, &token.IsDefault,
		&token.CreatedBy, &token.CreatedAt, &token.UpdatedAt, &token.LastUsedAt,
		&token.UsageCount, &token.LogCount,
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create ingest token: %w", err)
	}
	if normID.Valid {
		token.NormalizerID = &normID.String
	}
	if normName.Valid {
		token.NormalizerName = normName.String
	}
	json.Unmarshal(tsRaw, &token.TimestampFields)

	return token, fullToken, nil
}

func (s *Storage) CreateDefaultToken(ctx context.Context, fractalID, fractalName, username string) (*IngestToken, string, error) {
	fullToken, prefix, err := GenerateToken()
	if err != nil {
		return nil, "", err
	}
	tokenHash := HashToken(fullToken)

	defaultNormID := s.getDefaultNormalizerID(ctx)

	token := &IngestToken{}
	var tsRaw []byte
	var normID, normName sql.NullString
	err = s.pg.DB().QueryRowContext(ctx, `
		INSERT INTO ingest_tokens (name, description, token_prefix, token_hash, token_value, fractal_id,
			parser_type, normalizer_id, timestamp_fields, is_default, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'json', $7, '[]', true, $8)
		RETURNING id, name, description, token_prefix, token_value, fractal_id,
			parser_type, normalizer_id,
			(SELECT n.name FROM normalizers n WHERE n.id = ingest_tokens.normalizer_id),
			timestamp_fields, is_active, is_default,
			COALESCE(created_by, ''), created_at, updated_at, last_used_at, usage_count, log_count
	`, "Default", fmt.Sprintf("Default ingest token for %s", fractalName),
		prefix, tokenHash, fullToken, fractalID, defaultNormID, username,
	).Scan(
		&token.ID, &token.Name, &token.Description, &token.TokenPrefix, &token.TokenValue, &token.FractalID,
		&token.ParserType, &normID, &normName,
		&tsRaw, &token.IsActive, &token.IsDefault,
		&token.CreatedBy, &token.CreatedAt, &token.UpdatedAt, &token.LastUsedAt,
		&token.UsageCount, &token.LogCount,
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create default ingest token: %w", err)
	}
	if normID.Valid {
		token.NormalizerID = &normID.String
	}
	if normName.Valid {
		token.NormalizerName = normName.String
	}
	json.Unmarshal(tsRaw, &token.TimestampFields)
	return token, fullToken, nil
}

// EnsureDefaultTokens creates default tokens for any fractal that lacks one.
func (s *Storage) EnsureDefaultTokens(ctx context.Context, fm *fractals.Manager) error {
	allFractals, err := fm.ListFractals(ctx)
	if err != nil {
		return fmt.Errorf("failed to list fractals: %w", err)
	}
	for _, f := range allFractals {
		var count int
		err := s.pg.DB().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM ingest_tokens WHERE fractal_id = $1`, f.ID,
		).Scan(&count)
		if err != nil {
			log.Printf("[IngestTokens] failed to check tokens for fractal %s: %v", f.Name, err)
			continue
		}
		if count == 0 {
			creator := f.CreatedBy
			if creator == "" {
				creator = "admin"
			}
			_, _, err := s.CreateDefaultToken(ctx, f.ID, f.Name, creator)
			if err != nil {
				log.Printf("[IngestTokens] failed to create default token for fractal %s: %v", f.Name, err)
			} else {
				log.Printf("[IngestTokens] created default ingest token for fractal %s", f.Name)
			}
		}
	}
	return nil
}

// ValidateToken checks a raw token string and returns validated data for the hot path.
func (s *Storage) ValidateToken(ctx context.Context, rawToken string) (*ValidatedToken, error) {
	tokenHash := HashToken(rawToken)

	var v ValidatedToken
	var tsRaw []byte
	var normalizerID sql.NullString
	var transformsRaw, mappingsRaw, normTsFieldsRaw []byte
	err := s.pg.DB().QueryRowContext(ctx, `
		SELECT t.id, t.fractal_id, t.parser_type, t.timestamp_fields,
		       t.normalizer_id, n.transforms, n.field_mappings, n.timestamp_fields
		FROM ingest_tokens t
		LEFT JOIN normalizers n ON t.normalizer_id = n.id
		WHERE t.token_hash = $1 AND t.is_active = true
	`, tokenHash).Scan(&v.TokenID, &v.FractalID, &v.ParserType, &tsRaw,
		&normalizerID, &transformsRaw, &mappingsRaw, &normTsFieldsRaw)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid ingest token")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to validate ingest token: %w", err)
	}
	json.Unmarshal(tsRaw, &v.TimestampFields)

	if normalizerID.Valid {
		v.Normalizer = normalizers.CompileFromRaw(transformsRaw, mappingsRaw, normTsFieldsRaw)
	}
	return &v, nil
}

func (s *Storage) ListTokens(ctx context.Context, fractalID string) ([]IngestToken, error) {
	rows, err := s.pg.DB().QueryContext(ctx, `
		SELECT t.id, t.name, t.description, t.token_prefix, t.token_value, t.fractal_id,
			t.parser_type, t.normalizer_id, n.name,
			t.timestamp_fields, t.is_active, t.is_default,
			COALESCE(t.created_by, ''), t.created_at, t.updated_at, t.last_used_at, t.usage_count, t.log_count
		FROM ingest_tokens t
		LEFT JOIN normalizers n ON t.normalizer_id = n.id
		WHERE t.fractal_id = $1
		ORDER BY t.is_default DESC, t.created_at DESC
	`, fractalID)
	if err != nil {
		return nil, fmt.Errorf("failed to list ingest tokens: %w", err)
	}
	defer rows.Close()

	var tokens []IngestToken
	for rows.Next() {
		var t IngestToken
		var tsRaw []byte
		var normID, normName sql.NullString
		if err := rows.Scan(
			&t.ID, &t.Name, &t.Description, &t.TokenPrefix, &t.TokenValue, &t.FractalID,
			&t.ParserType, &normID, &normName,
			&tsRaw, &t.IsActive, &t.IsDefault,
			&t.CreatedBy, &t.CreatedAt, &t.UpdatedAt, &t.LastUsedAt,
			&t.UsageCount, &t.LogCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan ingest token: %w", err)
		}
		if normID.Valid {
			t.NormalizerID = &normID.String
		}
		if normName.Valid {
			t.NormalizerName = normName.String
		}
		json.Unmarshal(tsRaw, &t.TimestampFields)
		tokens = append(tokens, t)
	}
	return tokens, rows.Err()
}

func (s *Storage) GetToken(ctx context.Context, tokenID, fractalID string) (*IngestToken, error) {
	var t IngestToken
	var tsRaw []byte
	var normID, normName sql.NullString
	err := s.pg.DB().QueryRowContext(ctx, `
		SELECT t.id, t.name, t.description, t.token_prefix, t.token_value, t.fractal_id,
			t.parser_type, t.normalizer_id, n.name,
			t.timestamp_fields, t.is_active, t.is_default,
			COALESCE(t.created_by, ''), t.created_at, t.updated_at, t.last_used_at, t.usage_count, t.log_count
		FROM ingest_tokens t
		LEFT JOIN normalizers n ON t.normalizer_id = n.id
		WHERE t.id = $1 AND t.fractal_id = $2
	`, tokenID, fractalID).Scan(
		&t.ID, &t.Name, &t.Description, &t.TokenPrefix, &t.TokenValue, &t.FractalID,
		&t.ParserType, &normID, &normName,
		&tsRaw, &t.IsActive, &t.IsDefault,
		&t.CreatedBy, &t.CreatedAt, &t.UpdatedAt, &t.LastUsedAt,
		&t.UsageCount, &t.LogCount,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("ingest token not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get ingest token: %w", err)
	}
	if normID.Valid {
		t.NormalizerID = &normID.String
	}
	if normName.Valid {
		t.NormalizerName = normName.String
	}
	json.Unmarshal(tsRaw, &t.TimestampFields)
	return &t, nil
}

func (s *Storage) UpdateToken(ctx context.Context, tokenID, fractalID string, req UpdateTokenRequest) (*IngestToken, error) {
	setParts := []string{}
	args := []interface{}{}
	idx := 1

	if req.Name != nil {
		setParts = append(setParts, fmt.Sprintf("name = $%d", idx))
		args = append(args, *req.Name)
		idx++
	}
	if req.Description != nil {
		setParts = append(setParts, fmt.Sprintf("description = $%d", idx))
		args = append(args, *req.Description)
		idx++
	}
	if req.ParserType != nil {
		if *req.ParserType != "json" && *req.ParserType != "kv" && *req.ParserType != "syslog" {
			return nil, fmt.Errorf("invalid parser_type: must be 'json', 'kv', or 'syslog'")
		}
		setParts = append(setParts, fmt.Sprintf("parser_type = $%d", idx))
		args = append(args, *req.ParserType)
		idx++
	}
	if req.ClearNormalizer {
		setParts = append(setParts, "normalizer_id = NULL")
	} else if req.NormalizerID != nil {
		setParts = append(setParts, fmt.Sprintf("normalizer_id = $%d", idx))
		args = append(args, *req.NormalizerID)
		idx++
	}
	if req.TimestampFields != nil {
		tsJSON, _ := json.Marshal(req.TimestampFields)
		setParts = append(setParts, fmt.Sprintf("timestamp_fields = $%d", idx))
		args = append(args, string(tsJSON))
		idx++
	}

	if len(setParts) == 0 {
		return nil, fmt.Errorf("no fields to update")
	}

	args = append(args, tokenID, fractalID)
	query := fmt.Sprintf(`UPDATE ingest_tokens SET %s WHERE id = $%d AND fractal_id = $%d`,
		strings.Join(setParts, ", "), idx, idx+1)

	_, err := s.pg.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update ingest token: %w", err)
	}
	return s.GetToken(ctx, tokenID, fractalID)
}

func (s *Storage) DeleteToken(ctx context.Context, tokenID, fractalID string) error {
	var isActive, isDefault bool
	err := s.pg.DB().QueryRowContext(ctx,
		`SELECT is_active, is_default FROM ingest_tokens WHERE id = $1 AND fractal_id = $2`,
		tokenID, fractalID,
	).Scan(&isActive, &isDefault)
	if err == sql.ErrNoRows {
		return fmt.Errorf("ingest token not found")
	}
	if err != nil {
		return fmt.Errorf("failed to check token: %w", err)
	}

	if isDefault {
		return fmt.Errorf("cannot delete the default ingest token")
	}

	if isActive {
		var activeCount int
		err := s.pg.DB().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM ingest_tokens WHERE fractal_id = $1 AND is_active = true`,
			fractalID,
		).Scan(&activeCount)
		if err != nil {
			return fmt.Errorf("failed to check active token count: %w", err)
		}
		if activeCount <= 1 {
			return fmt.Errorf("cannot delete the last active ingest token")
		}
	}

	result, err := s.pg.DB().ExecContext(ctx,
		`DELETE FROM ingest_tokens WHERE id = $1 AND fractal_id = $2`,
		tokenID, fractalID,
	)
	if err != nil {
		return fmt.Errorf("failed to delete ingest token: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return fmt.Errorf("ingest token not found")
	}
	return nil
}

func (s *Storage) ToggleToken(ctx context.Context, tokenID, fractalID string) (*IngestToken, error) {
	var currentlyActive bool
	err := s.pg.DB().QueryRowContext(ctx,
		`SELECT is_active FROM ingest_tokens WHERE id = $1 AND fractal_id = $2`,
		tokenID, fractalID,
	).Scan(&currentlyActive)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("ingest token not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to check token: %w", err)
	}

	if currentlyActive {
		var activeCount int
		s.pg.DB().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM ingest_tokens WHERE fractal_id = $1 AND is_active = true`,
			fractalID,
		).Scan(&activeCount)
		if activeCount <= 1 {
			return nil, fmt.Errorf("cannot deactivate the last active ingest token")
		}
	}

	_, err = s.pg.DB().ExecContext(ctx,
		`UPDATE ingest_tokens SET is_active = NOT is_active WHERE id = $1 AND fractal_id = $2`,
		tokenID, fractalID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to toggle ingest token: %w", err)
	}
	return s.GetToken(ctx, tokenID, fractalID)
}

func (s *Storage) UpdateUsageStats(ctx context.Context, tokenID string, logCount int) error {
	_, err := s.pg.DB().ExecContext(ctx, `
		UPDATE ingest_tokens
		SET last_used_at = NOW(), usage_count = usage_count + 1, log_count = log_count + $2
		WHERE id = $1
	`, tokenID, logCount)
	return err
}

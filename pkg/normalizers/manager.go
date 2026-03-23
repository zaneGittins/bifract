package normalizers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/storage"
)

type Manager struct {
	pg *storage.PostgresClient
}

func NewManager(pg *storage.PostgresClient) *Manager {
	return &Manager{pg: pg}
}

func (m *Manager) List(ctx context.Context) ([]Normalizer, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, name, description, transforms, field_mappings, timestamp_fields,
		        is_default, COALESCE(created_by, ''), created_at, updated_at
		 FROM normalizers ORDER BY is_default DESC, name`)
	if err != nil {
		return nil, fmt.Errorf("query normalizers: %w", err)
	}
	defer rows.Close()

	var normalizers []Normalizer
	for rows.Next() {
		n, err := scanNormalizer(rows)
		if err != nil {
			return nil, err
		}
		normalizers = append(normalizers, n)
	}
	return normalizers, rows.Err()
}

func (m *Manager) Get(ctx context.Context, id string) (*Normalizer, error) {
	row := m.pg.QueryRow(ctx,
		`SELECT id, name, description, transforms, field_mappings, timestamp_fields,
		        is_default, COALESCE(created_by, ''), created_at, updated_at
		 FROM normalizers WHERE id = $1`, id)
	n, err := scanNormalizerRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("normalizer not found")
		}
		return nil, fmt.Errorf("get normalizer %s: %w", id, err)
	}
	return &n, nil
}

func (m *Manager) GetDefault(ctx context.Context) (*Normalizer, error) {
	row := m.pg.QueryRow(ctx,
		`SELECT id, name, description, transforms, field_mappings, timestamp_fields,
		        is_default, COALESCE(created_by, ''), created_at, updated_at
		 FROM normalizers WHERE is_default = true LIMIT 1`)
	n, err := scanNormalizerRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no default normalizer found")
		}
		return nil, fmt.Errorf("get default normalizer: %w", err)
	}
	return &n, nil
}

// GetDefaultID returns just the UUID of the default normalizer, or empty string if none.
func (m *Manager) GetDefaultID(ctx context.Context) string {
	var id string
	err := m.pg.QueryRow(ctx,
		`SELECT id FROM normalizers WHERE is_default = true LIMIT 1`).Scan(&id)
	if err != nil {
		return ""
	}
	return id
}

func (m *Manager) Create(ctx context.Context, req CreateRequest, createdBy string) (*Normalizer, error) {
	if err := validateRequest(req.Name, req.Transforms, req.FieldMappings); err != nil {
		return nil, err
	}

	transformsJSON, _ := json.Marshal(req.Transforms)
	mappingsJSON, _ := json.Marshal(req.FieldMappings)
	tsFieldsJSON, _ := json.Marshal(req.TimestampFields)
	if req.TimestampFields == nil {
		tsFieldsJSON = []byte("[]")
	}

	row := m.pg.QueryRow(ctx,
		`INSERT INTO normalizers (name, description, transforms, field_mappings, timestamp_fields, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, name, description, transforms, field_mappings, timestamp_fields,
		           is_default, COALESCE(created_by, ''), created_at, updated_at`,
		req.Name, req.Description, string(transformsJSON), string(mappingsJSON), string(tsFieldsJSON), createdBy)

	n, err := scanNormalizerRow(row)
	if err != nil {
		return nil, fmt.Errorf("create normalizer: %w", err)
	}
	return &n, nil
}

func (m *Manager) Update(ctx context.Context, id string, req UpdateRequest) (*Normalizer, error) {
	if err := validateRequest(req.Name, req.Transforms, req.FieldMappings); err != nil {
		return nil, err
	}

	transformsJSON, _ := json.Marshal(req.Transforms)
	mappingsJSON, _ := json.Marshal(req.FieldMappings)
	tsFieldsJSON, _ := json.Marshal(req.TimestampFields)
	if req.TimestampFields == nil {
		tsFieldsJSON = []byte("[]")
	}

	row := m.pg.QueryRow(ctx,
		`UPDATE normalizers
		 SET name = $1, description = $2, transforms = $3, field_mappings = $4, timestamp_fields = $5, updated_at = NOW()
		 WHERE id = $6
		 RETURNING id, name, description, transforms, field_mappings, timestamp_fields,
		           is_default, COALESCE(created_by, ''), created_at, updated_at`,
		req.Name, req.Description, string(transformsJSON), string(mappingsJSON), string(tsFieldsJSON), id)

	n, err := scanNormalizerRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("normalizer not found")
		}
		return nil, fmt.Errorf("update normalizer %s: %w", id, err)
	}
	return &n, nil
}

func (m *Manager) Delete(ctx context.Context, id string) error {
	// Prevent deleting the default normalizer
	var isDefault bool
	err := m.pg.QueryRow(ctx,
		`SELECT is_default FROM normalizers WHERE id = $1`, id).Scan(&isDefault)
	if err == sql.ErrNoRows {
		return fmt.Errorf("normalizer not found")
	}
	if err != nil {
		return fmt.Errorf("check normalizer: %w", err)
	}
	if isDefault {
		return fmt.Errorf("cannot delete the default normalizer")
	}

	result, err := m.pg.Exec(ctx, `DELETE FROM normalizers WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete normalizer %s: %w", id, err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("normalizer not found")
	}
	return nil
}

func (m *Manager) SetDefault(ctx context.Context, id string) error {
	// Verify the target exists
	var exists bool
	err := m.pg.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM normalizers WHERE id = $1)`, id).Scan(&exists)
	if err != nil || !exists {
		return fmt.Errorf("normalizer not found")
	}

	// Unset current default, set new one
	tx, err := m.pg.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`UPDATE normalizers SET is_default = false WHERE is_default = true`); err != nil {
		return fmt.Errorf("unset default: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE normalizers SET is_default = true WHERE id = $1`, id); err != nil {
		return fmt.Errorf("set default: %w", err)
	}
	return tx.Commit()
}

func validateRequest(name string, transforms []Transform, mappings []FieldMapping) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("name is required")
	}
	for _, t := range transforms {
		if !ValidTransforms[t] {
			return fmt.Errorf("invalid transform: %s", t)
		}
	}
	for _, fm := range mappings {
		if len(fm.Sources) == 0 {
			return fmt.Errorf("field mapping must have at least one source")
		}
		if strings.TrimSpace(fm.Target) == "" {
			return fmt.Errorf("field mapping target is required")
		}
	}
	return nil
}

// scanNormalizer scans a normalizer from a rows iterator.
func scanNormalizer(rows *sql.Rows) (Normalizer, error) {
	var n Normalizer
	var transformsRaw, mappingsRaw, tsFieldsRaw []byte
	if err := rows.Scan(&n.ID, &n.Name, &n.Description,
		&transformsRaw, &mappingsRaw, &tsFieldsRaw,
		&n.IsDefault, &n.CreatedBy, &n.CreatedAt, &n.UpdatedAt); err != nil {
		return n, fmt.Errorf("scan normalizer: %w", err)
	}
	json.Unmarshal(transformsRaw, &n.Transforms)
	json.Unmarshal(mappingsRaw, &n.FieldMappings)
	json.Unmarshal(tsFieldsRaw, &n.TimestampFields)
	return n, nil
}

type scannable interface {
	Scan(dest ...interface{}) error
}

// scanNormalizerRow scans a normalizer from a single row.
func scanNormalizerRow(row scannable) (Normalizer, error) {
	var n Normalizer
	var transformsRaw, mappingsRaw, tsFieldsRaw []byte
	err := row.Scan(&n.ID, &n.Name, &n.Description,
		&transformsRaw, &mappingsRaw, &tsFieldsRaw,
		&n.IsDefault, &n.CreatedBy, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return n, err
	}
	json.Unmarshal(transformsRaw, &n.Transforms)
	json.Unmarshal(mappingsRaw, &n.FieldMappings)
	json.Unmarshal(tsFieldsRaw, &n.TimestampFields)
	return n, nil
}

// CompileByID fetches a normalizer by ID and returns its compiled form.
// Returns nil if the ID is empty or the normalizer is not found.
func (m *Manager) CompileByID(ctx context.Context, id string) *CompiledNormalizer {
	if id == "" {
		return nil
	}
	n, err := m.Get(ctx, id)
	if err != nil {
		return nil
	}
	return n.Compile()
}

// CompileFromRaw builds a CompiledNormalizer from raw JSONB columns.
// Used in the hot path to avoid an extra DB round-trip.
func CompileFromRaw(transformsRaw, mappingsRaw, tsFieldsRaw []byte) *CompiledNormalizer {
	if transformsRaw == nil && mappingsRaw == nil {
		return nil
	}
	var n Normalizer
	json.Unmarshal(transformsRaw, &n.Transforms)
	json.Unmarshal(mappingsRaw, &n.FieldMappings)
	json.Unmarshal(tsFieldsRaw, &n.TimestampFields)
	return n.Compile()
}

// GetTokenCount returns the number of ingest tokens using this normalizer.
func (m *Manager) GetTokenCount(ctx context.Context, normalizerID string) (int, error) {
	var count int
	err := m.pg.QueryRow(ctx,
		`SELECT COUNT(*) FROM ingest_tokens WHERE normalizer_id = $1`, normalizerID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count tokens for normalizer: %w", err)
	}
	return count, nil
}

// GetTokenUsage returns tokens using a normalizer along with their fractal names.
func (m *Manager) GetTokenUsage(ctx context.Context, normalizerID string) ([]TokenUsageInfo, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT t.id, t.name, t.fractal_id, COALESCE(f.name, t.fractal_id) as fractal_name
		 FROM ingest_tokens t
		 LEFT JOIN fractals f ON f.id = t.fractal_id
		 WHERE t.normalizer_id = $1
		 ORDER BY fractal_name, t.name`, normalizerID)
	if err != nil {
		return nil, fmt.Errorf("query token usage: %w", err)
	}
	defer rows.Close()

	var tokens []TokenUsageInfo
	for rows.Next() {
		var ti TokenUsageInfo
		if err := rows.Scan(&ti.TokenID, &ti.TokenName, &ti.FractalID, &ti.FractalName); err != nil {
			return nil, fmt.Errorf("scan token usage: %w", err)
		}
		tokens = append(tokens, ti)
	}
	if tokens == nil {
		tokens = []TokenUsageInfo{}
	}
	return tokens, rows.Err()
}

// Duplicate creates a copy of an existing normalizer with " (Copy)" appended to the name.
func (m *Manager) Duplicate(ctx context.Context, id string, createdBy string) (*Normalizer, error) {
	original, err := m.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("source normalizer not found")
	}

	// Find a unique name
	baseName := original.Name + " (Copy)"
	candidateName := baseName
	for i := 2; ; i++ {
		var exists bool
		err := m.pg.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM normalizers WHERE name = $1)`, candidateName).Scan(&exists)
		if err != nil {
			return nil, fmt.Errorf("check name uniqueness: %w", err)
		}
		if !exists {
			break
		}
		candidateName = fmt.Sprintf("%s (%d)", baseName, i)
	}

	req := CreateRequest{
		Name:            candidateName,
		Description:     original.Description,
		Transforms:      original.Transforms,
		FieldMappings:   original.FieldMappings,
		TimestampFields: original.TimestampFields,
	}
	return m.Create(ctx, req, createdBy)
}

// TimeFormat is a helper for JSON serialization of timestamps.
func TimeFormat(t time.Time) string {
	return t.Format(time.RFC3339)
}

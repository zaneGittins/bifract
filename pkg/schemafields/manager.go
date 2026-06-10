package schemafields

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"bifract/pkg/storage"
)

var validFieldName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

type Manager struct {
	pg *storage.PostgresClient
}

func NewManager(pg *storage.PostgresClient) *Manager {
	return &Manager{pg: pg}
}

// List returns all user-defined custom schema fields. Project defaults are not stored here.
func (m *Manager) List(ctx context.Context) ([]SchemaField, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, field_name, index_type, COALESCE(created_by, ''), created_at
		 FROM clickhouse_schema_fields ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("query schema fields: %w", err)
	}
	defer rows.Close()

	var fields []SchemaField
	for rows.Next() {
		var f SchemaField
		if err := rows.Scan(&f.ID, &f.FieldName, &f.IndexType, &f.CreatedBy, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan schema field: %w", err)
		}
		fields = append(fields, f)
	}
	return fields, rows.Err()
}

// Create adds a new custom schema field. Rejects names that conflict with project defaults.
func (m *Manager) Create(ctx context.Context, req CreateRequest, createdBy string) (*SchemaField, error) {
	req.FieldName = strings.TrimSpace(req.FieldName)
	if req.FieldName == "" {
		return nil, fmt.Errorf("field_name is required")
	}
	if !validFieldName.MatchString(req.FieldName) {
		return nil, fmt.Errorf("field_name %q is invalid: use only letters, digits, and underscores, starting with a letter or underscore", req.FieldName)
	}
	if !validIndexTypes[req.IndexType] {
		return nil, fmt.Errorf("invalid index_type %q (must be bloom_filter or set)", req.IndexType)
	}
	if ProjectDefaultFieldMap()[req.FieldName] {
		return nil, fmt.Errorf("field %q is a project default and cannot be added as a custom field", req.FieldName)
	}

	var f SchemaField
	err := m.pg.QueryRow(ctx,
		`INSERT INTO clickhouse_schema_fields (field_name, index_type, created_by)
		 VALUES ($1, $2, $3)
		 RETURNING id, field_name, index_type, COALESCE(created_by, ''), created_at`,
		req.FieldName, string(req.IndexType), createdBy,
	).Scan(&f.ID, &f.FieldName, &f.IndexType, &f.CreatedBy, &f.CreatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "unique") {
			return nil, fmt.Errorf("field %q already exists", req.FieldName)
		}
		return nil, fmt.Errorf("create schema field: %w", err)
	}
	return &f, nil
}

// Delete removes a custom field from Postgres. The type hint and skip index remain
// in ClickHouse until the next schema reset.
func (m *Manager) Delete(ctx context.Context, fieldName string) error {
	if ProjectDefaultFieldMap()[fieldName] {
		return fmt.Errorf("field %q is a project default and cannot be removed", fieldName)
	}
	result, err := m.pg.Exec(ctx,
		`DELETE FROM clickhouse_schema_fields WHERE field_name = $1`, fieldName)
	if err != nil {
		return fmt.Errorf("delete schema field: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("field %q not found", fieldName)
	}
	return nil
}

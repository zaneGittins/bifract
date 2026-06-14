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
		`SELECT id, field_name, index_type, COALESCE(created_by, ''), created_at,
		        COALESCE(sync_status, 'active'), COALESCE(sync_error, '')
		 FROM clickhouse_schema_fields ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("query schema fields: %w", err)
	}
	defer rows.Close()

	var fields []SchemaField
	for rows.Next() {
		var f SchemaField
		if err := rows.Scan(&f.ID, &f.FieldName, &f.IndexType, &f.CreatedBy, &f.CreatedAt, &f.SyncStatus, &f.SyncError); err != nil {
			return nil, fmt.Errorf("scan schema field: %w", err)
		}
		fields = append(fields, f)
	}
	return fields, rows.Err()
}

// Create adds a new custom schema field. Rejects names that conflict with project defaults.
func (m *Manager) Create(ctx context.Context, req CreateRequest, createdBy string) (*SchemaField, error) {
	if err := validateCreate(&req); err != nil {
		return nil, err
	}

	var f SchemaField
	err := m.pg.QueryRow(ctx,
		`INSERT INTO clickhouse_schema_fields (field_name, index_type, created_by, sync_status)
		 VALUES ($1, $2, $3, 'pending')
		 RETURNING id, field_name, index_type, COALESCE(created_by, ''), created_at,
		           COALESCE(sync_status, 'pending'), COALESCE(sync_error, '')`,
		req.FieldName, string(req.IndexType), createdBy,
	).Scan(&f.ID, &f.FieldName, &f.IndexType, &f.CreatedBy, &f.CreatedAt, &f.SyncStatus, &f.SyncError)
	if err != nil {
		if strings.Contains(err.Error(), "unique") {
			return nil, fmt.Errorf("field %q already exists", req.FieldName)
		}
		return nil, fmt.Errorf("create schema field: %w", err)
	}
	return &f, nil
}

// UpdateSyncStatus records the outcome of a background ClickHouse reconcile for
// a field. errMsg is stored only when status is SyncStatusError.
func (m *Manager) UpdateSyncStatus(ctx context.Context, fieldName, status, errMsg string) error {
	_, err := m.pg.Exec(ctx,
		`UPDATE clickhouse_schema_fields SET sync_status = $2, sync_error = $3 WHERE field_name = $1`,
		fieldName, status, errMsg)
	if err != nil {
		return fmt.Errorf("update sync status for %q: %w", fieldName, err)
	}
	return nil
}

// validateCreate validates and normalizes a single field request without writing.
func validateCreate(req *CreateRequest) error {
	req.FieldName = strings.TrimSpace(req.FieldName)
	if req.FieldName == "" {
		return fmt.Errorf("field_name is required")
	}
	if !validFieldName.MatchString(req.FieldName) {
		return fmt.Errorf("field_name %q is invalid: use only letters, digits, and underscores, starting with a letter or underscore", req.FieldName)
	}
	if req.IndexType == "" {
		req.IndexType = IndexTypeBloomFilter
	}
	if !validIndexTypes[req.IndexType] {
		return fmt.Errorf("invalid index_type %q (must be bloom_filter or set)", req.IndexType)
	}
	if ProjectDefaultFieldMap()[req.FieldName] {
		return fmt.Errorf("field %q is a project default and cannot be added as a custom field", req.FieldName)
	}
	return nil
}

// ReplaceAll atomically replaces the entire set of custom fields with the
// provided list (used by YAML import). All incoming fields are validated up
// front; the existing custom set is then deleted and the new set inserted in a
// single transaction. New rows are marked pending for background reconcile.
func (m *Manager) ReplaceAll(ctx context.Context, reqs []CreateRequest, createdBy string) ([]SchemaField, error) {
	seen := make(map[string]bool, len(reqs))
	for i := range reqs {
		if err := validateCreate(&reqs[i]); err != nil {
			return nil, err
		}
		if seen[reqs[i].FieldName] {
			return nil, fmt.Errorf("duplicate field %q in import", reqs[i].FieldName)
		}
		seen[reqs[i].FieldName] = true
	}

	tx, err := m.pg.DB().BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin import transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM clickhouse_schema_fields`); err != nil {
		return nil, fmt.Errorf("clear existing fields: %w", err)
	}
	for i := range reqs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO clickhouse_schema_fields (field_name, index_type, created_by, sync_status)
			 VALUES ($1, $2, $3, 'pending')`,
			reqs[i].FieldName, string(reqs[i].IndexType), createdBy); err != nil {
			return nil, fmt.Errorf("insert field %q: %w", reqs[i].FieldName, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit import: %w", err)
	}
	return m.List(ctx)
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

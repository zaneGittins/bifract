package schemafields

import (
	"time"

	"bifract/pkg/storage"
)

type IndexType string

const (
	// IndexTypeNone applies the type hint (dedicated sub-column) with no skip index.
	// This is the default for new custom fields: a skip index adds write/merge cost,
	// so it should be opted into only for fields that need granule pruning.
	IndexTypeNone        IndexType = "none"
	IndexTypeBloomFilter IndexType = "bloom_filter"
	IndexTypeSet         IndexType = "set"
)

var validIndexTypes = map[IndexType]bool{
	IndexTypeNone:        true,
	IndexTypeBloomFilter: true,
	IndexTypeSet:         true,
}

// Sync status values track whether a custom field's ClickHouse schema (type
// hint + skip index) has been applied. The reconcile runs in the background
// because the underlying ALTER can block for minutes on large datasets.
const (
	SyncStatusPending = "pending"
	SyncStatusActive  = "active"
	SyncStatusError   = "error"
)

type SchemaField struct {
	ID         string    `json:"id"`
	FieldName  string    `json:"field_name"`
	IndexType  IndexType `json:"index_type"`
	CreatedBy  string    `json:"created_by"`
	CreatedAt  time.Time `json:"created_at"`
	IsDefault  bool      `json:"is_default"`
	SyncStatus string    `json:"sync_status,omitempty"`
	SyncError  string    `json:"sync_error,omitempty"`
}

type CreateRequest struct {
	FieldName string    `json:"field_name"`
	IndexType IndexType `json:"index_type"`
}

// SchemaExport is the YAML import/export envelope for custom schema fields.
// Project defaults are built into the binary and are intentionally excluded.
type SchemaExport struct {
	Fields []SchemaFieldExport `yaml:"fields"`
}

type SchemaFieldExport struct {
	FieldName string `yaml:"field_name"`
	IndexType string `yaml:"index_type"`
}

// ToSpecs converts a slice of SchemaField to storage.SchemaFieldSpec for ClickHouse DDL calls.
func ToSpecs(fields []SchemaField) []storage.SchemaFieldSpec {
	specs := make([]storage.SchemaFieldSpec, len(fields))
	for i, f := range fields {
		specs[i] = storage.SchemaFieldSpec{FieldName: f.FieldName, IndexType: string(f.IndexType)}
	}
	return specs
}

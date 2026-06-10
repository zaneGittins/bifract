package schemafields

import (
	"time"

	"bifract/pkg/storage"
)

type IndexType string

const (
	IndexTypeBloomFilter IndexType = "bloom_filter"
	IndexTypeSet         IndexType = "set"
)

var validIndexTypes = map[IndexType]bool{
	IndexTypeBloomFilter: true,
	IndexTypeSet:         true,
}

type SchemaField struct {
	ID        string    `json:"id"`
	FieldName string    `json:"field_name"`
	IndexType IndexType `json:"index_type"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	IsDefault bool      `json:"is_default"`
}

type CreateRequest struct {
	FieldName string    `json:"field_name"`
	IndexType IndexType `json:"index_type"`
}

// ToSpecs converts a slice of SchemaField to storage.SchemaFieldSpec for ClickHouse DDL calls.
func ToSpecs(fields []SchemaField) []storage.SchemaFieldSpec {
	specs := make([]storage.SchemaFieldSpec, len(fields))
	for i, f := range fields {
		specs[i] = storage.SchemaFieldSpec{FieldName: f.FieldName, IndexType: string(f.IndexType)}
	}
	return specs
}

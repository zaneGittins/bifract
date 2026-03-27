package sigma

import "bifract/pkg/normalizers"

// BuildFieldMapper returns a function that maps Sigma field names to the field
// names actually stored in ClickHouse, using the same transform + mapping logic
// that the normalizer applies during ingestion.
//
// If compiled is nil, field names pass through unchanged.
func BuildFieldMapper(compiled *normalizers.CompiledNormalizer) func(string) string {
	if compiled == nil {
		return func(field string) string { return field }
	}
	return func(field string) string {
		return compiled.ApplyFieldName(field)
	}
}

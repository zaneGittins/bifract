package schemafields

// ProjectDefaultFields are the type-hinted fields built into every bifract
// deployment. They are always present regardless of user configuration and
// are shown as read-only in the UI. Matches the inline index definitions in
// db/init-clickhouse.sql and the jsonTypeHintedFields map in pkg/parser/helpers.go.
var ProjectDefaultFields = []SchemaField{
	{FieldName: "computer_name",      IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "user",               IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "src_ip",             IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "dst_ip",             IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "src_port",           IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "dst_port",           IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "commandline",        IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "hash",               IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "event_id",           IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "image",              IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "parent_image",       IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "call_chain",         IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "operation",          IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "artifact",           IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "query",              IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "original_file_name", IndexType: IndexTypeBloomFilter, IsDefault: true},
}

// ProjectDefaultFieldMap returns a set of project default field names for O(1) lookup.
func ProjectDefaultFieldMap() map[string]bool {
	m := make(map[string]bool, len(ProjectDefaultFields))
	for _, f := range ProjectDefaultFields {
		m[f.FieldName] = true
	}
	return m
}

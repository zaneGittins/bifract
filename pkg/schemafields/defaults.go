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
	// Network analysis fields (conn logs: Zeek, netflow, firewall). proto/conn_state
	// are low-cardinality categoricals -> set index (like event_id/operation). The
	// numeric-as-string fields (duration/bytes) get a type hint only, no skip index:
	// they are aggregated over a recent window, never equality-filtered.
	{FieldName: "proto",              IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "conn_state",         IndexType: IndexTypeSet,         IsDefault: true},
	{FieldName: "duration",           IndexType: IndexTypeNone,        IsDefault: true},
	{FieldName: "orig_bytes",         IndexType: IndexTypeNone,        IsDefault: true},
	{FieldName: "resp_bytes",         IndexType: IndexTypeNone,        IsDefault: true},
	// Canonical taxonomy field produced by normalizer derived-field (value-map)
	// transforms, e.g. Sysmon event_id -> process_creation. Namespaced with a
	// bifract_ prefix because "category" is a very common source field (ECS
	// event.category, Windows/firewall/proxy logs) that would otherwise collide;
	// bifract_category is a Bifract-controlled signal ptg/pgr can trust. Low-
	// cardinality (~15 values), so a skip index would never prune: type hint only.
	{FieldName: "bifract_category",   IndexType: IndexTypeNone,        IsDefault: true},
	// Process provenance fields (Sysmon EID 1 etc). process_guid gets a bloom for
	// the process-tree leaf-fetch (pgr) that filters logs by it; parent_process_guid
	// is type-hint only (nothing filters logs by it -- proc_lineage carries its own
	// parent_guid bloom), so a bloom here would only tax the ingest path.
	{FieldName: "process_guid",        IndexType: IndexTypeBloomFilter, IsDefault: true},
	{FieldName: "parent_process_guid", IndexType: IndexTypeNone,        IsDefault: true},
}

// ProjectDefaultFieldMap returns a set of project default field names for O(1) lookup.
func ProjectDefaultFieldMap() map[string]bool {
	m := make(map[string]bool, len(ProjectDefaultFields))
	for _, f := range ProjectDefaultFields {
		m[f.FieldName] = true
	}
	return m
}

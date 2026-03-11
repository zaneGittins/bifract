package dictionaries

import "time"

// DictionaryColumn describes a single column in a dictionary.
type DictionaryColumn struct {
	Name  string `json:"name"`
	Type  string `json:"type"`   // "string" (only string supported for now)
	IsKey bool   `json:"is_key"` // if true, a secondary ClickHouse DICTIONARY keyed by this column is maintained
}

// Dictionary is a per-fractal lookup table backed by a ClickHouse dictionary.
type Dictionary struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	FractalID   string             `json:"fractal_id"`
	IsGlobal    bool               `json:"is_global"`
	KeyColumn   string             `json:"key_column"`
	Columns     []DictionaryColumn `json:"columns"`
	RowCount    int64              `json:"row_count"`
	CreatedBy   string             `json:"created_by"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`

	// CHTableName is the ClickHouse backing table name (not stored in PG).
	CHTableName string `json:"ch_table_name,omitempty"`
	// CHDictName is the ClickHouse dictionary object name (not stored in PG).
	CHDictName string `json:"ch_dict_name,omitempty"`
}

// DictionaryRow represents a single row of dictionary data.
type DictionaryRow struct {
	Key    string            `json:"key"`
	Fields map[string]string `json:"fields"`
}

// DictionaryAction is an alert action that populates a dictionary from log fields.
// The target dictionary is identified by name and auto-created if it doesn't exist.
// All log fields become columns; the first field of each log is used as the key.
type DictionaryAction struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	DictionaryName    string `json:"dictionary_name"`
	MaxLogsPerTrigger int    `json:"max_logs_per_trigger"`
	Enabled           bool   `json:"enabled"`
	CreatedBy         string `json:"created_by"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`

	// DictionaryID is resolved at execution time from DictionaryName.
	// Kept for internal use and backwards compat with existing rows.
	DictionaryID string `json:"dictionary_id,omitempty"`
}

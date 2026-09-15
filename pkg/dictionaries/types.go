package dictionaries

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// A dictionary's kind decides the ClickHouse layout it is built with, and so what
// a lookup means.
const (
	// KindValue is a HASHED dictionary: the key is probed byte for byte.
	KindValue = "value"
	// KindNetwork is an IP_TRIE dictionary whose keys are CIDR ranges. A lookup
	// probes an address and resolves to the longest range containing it, so one
	// row covers every address in it.
	KindNetwork = "network"
)

// ValidKind reports whether a kind is one this build knows how to create. An
// unknown kind must never fall back to a layout the author did not ask for: the
// lookups would all miss and nothing would say why.
func ValidKind(kind string) bool {
	return kind == KindValue || kind == KindNetwork
}

// NormalizeKind fills in the default for a dictionary stored before kinds
// existed, where the column reads as empty rather than 'value'.
func NormalizeKind(kind string) string {
	if kind == "" {
		return KindValue
	}
	return kind
}

// ValidateNetworkKey rejects a key a ClickHouse IP_TRIE cannot hold. An entry
// without a prefix length is silently dropped at load, so the dictionary would
// come back smaller than the rows the user uploaded with nothing to say which
// went missing.
func ValidateNetworkKey(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("a network list needs a CIDR range, not an empty key")
	}
	if _, _, err := net.ParseCIDR(key); err != nil {
		return fmt.Errorf("%q is not a CIDR range; it needs an address and a prefix length, e.g. \"10.0.0.0/8\"", key)
	}
	return nil
}

// DictionaryColumn describes a single column in a dictionary.
type DictionaryColumn struct {
	Name  string `json:"name"`
	Type  string `json:"type"`   // "string" (only string supported for now)
	IsKey bool   `json:"is_key"` // if true, a secondary ClickHouse DICTIONARY keyed by this column is maintained
}

// Dictionary is a lookup table backed by a ClickHouse dictionary, scoped to a fractal or prism.
type Dictionary struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	FractalID   string             `json:"fractal_id"`
	PrismID     string             `json:"prism_id,omitempty"`
	IsGlobal    bool               `json:"is_global"`
	KeyColumn   string             `json:"key_column"`
	Columns     []DictionaryColumn `json:"columns"`
	RowCount    int64              `json:"row_count"`
	// CaseInsensitiveKeys builds the ClickHouse dictionary objects over lower(key),
	// so match() hits regardless of the casing the log carries.
	CaseInsensitiveKeys bool `json:"case_insensitive_keys"`
	// Kind is KindValue or KindNetwork. It decides the ClickHouse layout, and a
	// network dictionary is probed with an address rather than a string.
	Kind      string    `json:"kind"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

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
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Description       string    `json:"description"`
	DictionaryName    string    `json:"dictionary_name"`
	MaxLogsPerTrigger int       `json:"max_logs_per_trigger"`
	Enabled           bool      `json:"enabled"`
	CreatedBy         string    `json:"created_by"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	FractalID         string    `json:"fractal_id,omitempty"`
	PrismID           string    `json:"prism_id,omitempty"`

	// DictionaryID is resolved at execution time from DictionaryName.
	// Kept for internal use and backwards compat with existing rows.
	DictionaryID string `json:"dictionary_id,omitempty"`
}

// Scope is what a query needs to resolve match() against the dictionaries visible
// in a fractal or prism. A struct rather than parallel return values: every map is
// keyed by the same dictionary name and must come from the same row, and a third
// bare return value is how they drift apart.
type Scope struct {
	// Mappings is dictionary name -> (key column -> ClickHouse dictionary name).
	Mappings map[string]map[string]string
	// CaseInsensitive marks dictionaries whose keys were hashed lowercased.
	CaseInsensitive map[string]bool
	// Network marks IP_TRIE dictionaries, probed with an address rather than a
	// string and matched on the longest prefix.
	Network map[string]bool
}

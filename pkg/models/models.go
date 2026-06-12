package models

import "time"

type ModelType string

const (
	ModelTypeRarity    ModelType = "rarity"
	ModelTypeFirstSeen ModelType = "first_seen"
)

type FilterCondition struct {
	Field string `json:"field" yaml:"field"`
	Op    string `json:"op" yaml:"op"`
	Value string `json:"value" yaml:"value"`
}

type ExtractionStep struct {
	FromField   string `json:"from_field" yaml:"from_field"`
	Pattern     string `json:"pattern" yaml:"pattern"`
	OutputField string `json:"output_field" yaml:"output_field"`
	Lowercase   bool   `json:"lowercase" yaml:"lowercase,omitempty"`
	MinLength   int    `json:"min_length" yaml:"min_length,omitempty"`
}

type AlertConfig struct {
	Severity            string   `json:"severity" yaml:"severity,omitempty"`
	ActionIDs           []string `json:"action_ids" yaml:"action_ids,omitempty"`
	ConfidenceThreshold float64  `json:"confidence_threshold" yaml:"confidence_threshold,omitempty"`
	PercentThreshold    float64  `json:"percent_threshold" yaml:"percent_threshold,omitempty"`
	AlertOnNew          bool     `json:"alert_on_new" yaml:"alert_on_new,omitempty"`
}

type ModelDefinition struct {
	Filter       []FilterCondition `json:"filter" yaml:"filter,omitempty"`
	Extractions  []ExtractionStep  `json:"extractions" yaml:"extractions,omitempty"`
	PartitionKey string            `json:"partition_key" yaml:"partition_key,omitempty"`
	ValueKey     string            `json:"value_key" yaml:"value_key,omitempty"`
	MinSample    int               `json:"min_sample" yaml:"min_sample,omitempty"`
	KeyFields    []string          `json:"key_fields" yaml:"key_fields,omitempty"`
	Alert        *AlertConfig      `json:"alert,omitempty" yaml:"alert,omitempty"`
}

type Model struct {
	ID            string          `json:"id"`
	FractalID     string          `json:"fractal_id"`
	PrismID       string          `json:"prism_id"`
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	ModelType     ModelType       `json:"model_type"`
	Definition    ModelDefinition `json:"definition"`
	CHTableName   string          `json:"ch_table_name"`
	CHMVName      string          `json:"ch_mv_name"`
	Status        string          `json:"status"`
	AlertMode     string          `json:"alert_mode"`
	LinkedAlertID string          `json:"linked_alert_id"`
	ErrorMessage  string          `json:"error_message"`
	CreatedBy     string          `json:"created_by"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`

	// SourceQuery is the derived BQL source query (filter + extraction) for the
	// model builder editor. It is computed on read, never persisted.
	SourceQuery string `json:"source_query,omitempty"`
}

// ModelInfo is a lightweight representation used in QueryOptions for BQL model_lookup().
type ModelInfo struct {
	ID        string
	TableName string // distributed table name in cluster mode, local otherwise
	ModelType ModelType
	MinSample int
	FractalID string
}

type CreateRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	ModelType   ModelType       `json:"model_type"`
	Definition  ModelDefinition `json:"definition"`
	AlertMode   string          `json:"alert_mode"`
}

type UpdateRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Definition  ModelDefinition `json:"definition"`
	AlertMode   string          `json:"alert_mode"`
}

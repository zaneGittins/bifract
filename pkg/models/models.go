package models

import "time"

type ModelType string

const (
	ModelTypeRarity    ModelType = "rarity"
	ModelTypeFirstSeen ModelType = "first_seen"
)

type FilterCondition struct {
	Field string `json:"field"`
	Op    string `json:"op"`    // "=", "!=", "~", "!~"
	Value string `json:"value"`
}

type ExtractionStep struct {
	FromField   string `json:"from_field"`
	Pattern     string `json:"pattern"`
	OutputField string `json:"output_field"`
	Lowercase   bool   `json:"lowercase"`
	MinLength   int    `json:"min_length"`
}

type AlertConfig struct {
	Severity  string   `json:"severity"`
	ActionIDs []string `json:"action_ids"`
	// Rarity thresholds
	ConfidenceThreshold float64 `json:"confidence_threshold"`
	PercentThreshold    float64 `json:"percent_threshold"`
	// First-seen threshold
	AlertOnNew bool `json:"alert_on_new"`
}

type ModelDefinition struct {
	Filter      []FilterCondition `json:"filter"`
	Extractions []ExtractionStep  `json:"extractions"`
	// Rarity fields
	PartitionKey string `json:"partition_key"`
	ValueKey     string `json:"value_key"`
	MinSample    int    `json:"min_sample"`
	// First-seen fields
	KeyFields []string `json:"key_fields"`
	// Alert auto-create
	Alert *AlertConfig `json:"alert,omitempty"`
}

type Model struct {
	ID             string          `json:"id"`
	FractalID      string          `json:"fractal_id"`
	PrismID        string          `json:"prism_id"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	ModelType      ModelType       `json:"model_type"`
	Definition     ModelDefinition `json:"definition"`
	CHTableName    string          `json:"ch_table_name"`
	CHMVName       string          `json:"ch_mv_name"`
	Status         string          `json:"status"`
	AlertMode      string          `json:"alert_mode"`
	LinkedAlertID  string          `json:"linked_alert_id"`
	ErrorMessage   string          `json:"error_message"`
	CreatedBy      string          `json:"created_by"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
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

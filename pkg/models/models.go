package models

import "time"

type ModelType string

const (
	ModelTypeRarity         ModelType = "rarity"
	ModelTypeFirstSeen      ModelType = "first_seen"
	ModelTypeVolumeBaseline ModelType = "volume_baseline"
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
	// ZThreshold is the modified z-score cutoff for volume_baseline alerts.
	ZThreshold float64 `json:"z_threshold" yaml:"z_threshold,omitempty"`
}

type ModelDefinition struct {
	Filter       []FilterCondition `json:"filter" yaml:"filter,omitempty"`
	Extractions  []ExtractionStep  `json:"extractions" yaml:"extractions,omitempty"`
	PartitionKey string            `json:"partition_key" yaml:"partition_key,omitempty"`
	ValueKey     string            `json:"value_key" yaml:"value_key,omitempty"`
	MinSample    int               `json:"min_sample" yaml:"min_sample,omitempty"`
	KeyFields    []string          `json:"key_fields" yaml:"key_fields,omitempty"`
	// TimeBucket is the volume_baseline aggregation window: "day" (default) or "hour".
	TimeBucket string       `json:"time_bucket" yaml:"time_bucket,omitempty"`
	Alert      *AlertConfig `json:"alert,omitempty" yaml:"alert,omitempty"`
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

	// One-time historical backfill state. The live MV is forward-only, so a new
	// model is empty until matching logs arrive; a backfill seeds it from history.
	BackfillStatus    string     `json:"backfill_status"` // none|running|completed|failed|cancelled
	BackfillWindow    string     `json:"backfill_window"` // 24h|7d|30d|90d
	BackfillTotal     int        `json:"backfill_total"`  // total day-chunks
	BackfillDone      int        `json:"backfill_done"`   // completed day-chunks
	BackfillStartedAt *time.Time `json:"backfill_started_at,omitempty"`
	BackfillError     string     `json:"backfill_error,omitempty"`

	// SourceQuery is the derived BQL source query (filter + extraction) for the
	// model builder editor. It is computed on read, never persisted.
	SourceQuery string `json:"source_query,omitempty"`
}

// ModelInfo is a lightweight representation used in QueryOptions for BQL model_lookup().
type ModelInfo struct {
	ID         string
	TableName  string // distributed table name in cluster mode, local otherwise
	ModelType  ModelType
	MinSample  int
	TimeBucket string // volume_baseline bucket granularity ("day"/"hour")
	FractalID  string
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

// BackfillRequest is the body for POST /models/{id}/backfill.
type BackfillRequest struct {
	Window string `json:"window"` // one of validBackfillWindows
}

// validBackfillWindows maps the allowed lookback windows to their day count.
// 90d is the hard maximum.
var validBackfillWindows = map[string]int{
	"24h": 1,
	"7d":  7,
	"30d": 30,
	"90d": 90,
}

// BackfillWindowDays returns the number of day-chunks for a window string and
// whether the window is valid.
func BackfillWindowDays(window string) (int, bool) {
	d, ok := validBackfillWindows[window]
	return d, ok
}

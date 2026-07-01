package models

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// LinkedAlertSpec describes the alert backing an analytics model. It is kept free
// of any alerts-package types so the models package never imports pkg/alerts
// (which already imports pkg/models for model_lookup support, so the reverse
// would be an import cycle). The alerts package provides an adapter that
// satisfies LinkedAlertManager.
type LinkedAlertSpec struct {
	Name        string
	Description string
	QueryString string
	Severity    string
	Enabled     bool
	FractalID   string
	PrismID     string
	CreatedBy   string
}

// LinkedAlertManager is satisfied by *alerts.Manager. It lets the model manager
// create, update, toggle, and delete the normal alert that backs a model without
// importing the alerts package. Updates preserve operator-managed fields
// (actions, throttle, enabled, severity) that live on the Alerts page; the model
// owns only name, description, and the generated detection query.
type LinkedAlertManager interface {
	CreateLinkedAlert(ctx context.Context, spec LinkedAlertSpec) (string, error)
	UpdateLinkedAlert(ctx context.Context, alertID string, spec LinkedAlertSpec) error
	DeleteLinkedAlert(ctx context.Context, alertID string) error
	SetLinkedAlertEnabled(ctx context.Context, alertID string, enabled bool) error
}

type ModelType string

const (
	ModelTypeRarity         ModelType = "rarity"
	ModelTypeFirstSeen      ModelType = "first_seen"
	ModelTypeVolumeBaseline ModelType = "volume_baseline"
	// Network analysis types. Unlike the streaming types above (incremental MVs),
	// these are scheduled: an MV maintains compact rolling state and a background
	// scorer periodically reads that state, scores in Go, and writes a results table.
	ModelTypeBeacon         ModelType = "beacon"
	ModelTypeLongConnection ModelType = "long_connection"
)

// IsScheduled reports whether the model type is scored by the background scorer
// engine (network analysis) rather than maintained incrementally by a streaming
// materialized view. This is the single switch that routes the model lifecycle.
func (mt ModelType) IsScheduled() bool {
	return mt == ModelTypeBeacon || mt == ModelTypeLongConnection
}

// IsNetwork reports whether the model type operates on network connection data
// (src/dst/port pairs). Currently identical to IsScheduled but kept distinct so
// a future non-network scheduled type does not silently inherit network behavior.
func (mt ModelType) IsNetwork() bool {
	return mt == ModelTypeBeacon || mt == ModelTypeLongConnection
}

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

	// Network analysis (beacon / long_connection). All additive and omitempty:
	// definition is stored as JSONB, so no Postgres migration is needed.
	Network   *NetworkFieldMap `json:"network,omitempty" yaml:"network,omitempty"`
	Window    string           `json:"window,omitempty" yaml:"window,omitempty"` // rolling window in days: "1d"/"7d"/"14d" (default "1d")
	Beacon    *BeaconParams    `json:"beacon,omitempty" yaml:"beacon,omitempty"`
	LongConn  *LongConnParams  `json:"long_conn,omitempty" yaml:"long_conn,omitempty"`
	Modifiers *ModifierParams  `json:"modifiers,omitempty" yaml:"modifiers,omitempty"`
}

// NetworkFieldMap maps model-agnostic roles to the log fields that carry them, so
// heterogeneous sources (Zeek conn, netflow, firewall) work without code changes.
// Defaults follow Zeek conn.log field names.
type NetworkFieldMap struct {
	SrcField      string `json:"src_field,omitempty" yaml:"src_field,omitempty"`
	DstField      string `json:"dst_field,omitempty" yaml:"dst_field,omitempty"`
	PortField     string `json:"port_field,omitempty" yaml:"port_field,omitempty"`
	DurationField string `json:"duration_field,omitempty" yaml:"duration_field,omitempty"`
	BytesField    string `json:"bytes_field,omitempty" yaml:"bytes_field,omitempty"`
}

// WithDefaults returns a copy with Zeek-style defaults filled in for any unset field.
// A nil receiver yields the all-defaults map.
func (n *NetworkFieldMap) WithDefaults() NetworkFieldMap {
	out := NetworkFieldMap{}
	if n != nil {
		out = *n
	}
	if out.SrcField == "" {
		out.SrcField = "src_ip"
	}
	if out.DstField == "" {
		out.DstField = "dst_ip"
	}
	if out.PortField == "" {
		out.PortField = "dst_port"
	}
	if out.DurationField == "" {
		out.DurationField = "duration"
	}
	if out.BytesField == "" {
		out.BytesField = "orig_bytes"
	}
	return out
}

// BeaconParams tunes beacon scoring. Zero values mean "use the default" (applied
// by WithDefaults) so an omitted/partial JSON definition still scores sanely.
type BeaconParams struct {
	MinConnections int     `json:"min_connections,omitempty" yaml:"min_connections,omitempty"`
	StrobeLimit    int     `json:"strobe_limit,omitempty" yaml:"strobe_limit,omitempty"` // 0 => derive = windowSecs
	ScoreThreshold float64 `json:"score_threshold,omitempty" yaml:"score_threshold,omitempty"`
	TsWeight       float64 `json:"ts_weight,omitempty" yaml:"ts_weight,omitempty"`
	DsWeight       float64 `json:"ds_weight,omitempty" yaml:"ds_weight,omitempty"`
	DurWeight      float64 `json:"dur_weight,omitempty" yaml:"dur_weight,omitempty"`
	HistWeight     float64 `json:"hist_weight,omitempty" yaml:"hist_weight,omitempty"`
}

// WithDefaults fills unset beacon params. windowSecs drives the derived strobe limit
// (~1 connection/second averaged over the window) so it scales with window length.
func (b *BeaconParams) WithDefaults(windowSecs int64) BeaconParams {
	out := BeaconParams{}
	if b != nil {
		out = *b
	}
	if out.MinConnections <= 0 {
		out.MinConnections = 4
	}
	if out.StrobeLimit <= 0 {
		out.StrobeLimit = int(windowSecs)
	}
	if out.ScoreThreshold <= 0 {
		out.ScoreThreshold = 0.8
	}
	// Weights: if none are set, use the balanced 0.25 split. If any are set, trust
	// the caller-provided weights verbatim (they may intentionally sum to <1).
	if out.TsWeight == 0 && out.DsWeight == 0 && out.DurWeight == 0 && out.HistWeight == 0 {
		out.TsWeight, out.DsWeight, out.DurWeight, out.HistWeight = 0.25, 0.25, 0.25, 0.25
	}
	return out
}

// LongConnParams tunes long-connection scoring via interpolated duration buckets
// (seconds). base -> score rises from 0; low/med/high are the tier boundaries.
type LongConnParams struct {
	BaseSeconds    float64 `json:"base_seconds,omitempty" yaml:"base_seconds,omitempty"`
	LowSeconds     float64 `json:"low_seconds,omitempty" yaml:"low_seconds,omitempty"`
	MedSeconds     float64 `json:"med_seconds,omitempty" yaml:"med_seconds,omitempty"`
	HighSeconds    float64 `json:"high_seconds,omitempty" yaml:"high_seconds,omitempty"`
	ScoreThreshold float64 `json:"score_threshold,omitempty" yaml:"score_threshold,omitempty"`
}

// WithDefaults fills unset long-connection params (1h base, 4h/8h/12h tiers).
func (l *LongConnParams) WithDefaults() LongConnParams {
	out := LongConnParams{}
	if l != nil {
		out = *l
	}
	if out.BaseSeconds <= 0 {
		out.BaseSeconds = 3600
	}
	if out.LowSeconds <= 0 {
		out.LowSeconds = 14400
	}
	if out.MedSeconds <= 0 {
		out.MedSeconds = 28800
	}
	if out.HighSeconds <= 0 {
		out.HighSeconds = 43200
	}
	if out.ScoreThreshold <= 0 {
		out.ScoreThreshold = 0.5
	}
	return out
}

// ModifierParams tunes the false-positive-reduction layer. Prevalence (how many
// distinct internal hosts contact a destination) reranks the base regularity score:
// rare destinations are boosted, ubiquitous ones (shared services) penalized.
type ModifierParams struct {
	PrevalenceLowThreshold  float64 `json:"prevalence_low_threshold,omitempty" yaml:"prevalence_low_threshold,omitempty"`   // <= => rare, boost
	PrevalenceHighThreshold float64 `json:"prevalence_high_threshold,omitempty" yaml:"prevalence_high_threshold,omitempty"` // >= => common, penalize
	PrevalenceIncrease      float64 `json:"prevalence_increase,omitempty" yaml:"prevalence_increase,omitempty"`
	PrevalenceDecrease      float64 `json:"prevalence_decrease,omitempty" yaml:"prevalence_decrease,omitempty"`
}

// WithDefaults fills unset modifier thresholds with sensible defaults (rare <= 2%,
// common >= 50%, +/-0.15 nudge). A negative increase/decrease disables that side.
func (m *ModifierParams) WithDefaults() ModifierParams {
	out := ModifierParams{PrevalenceIncrease: -1, PrevalenceDecrease: -1}
	if m != nil {
		out = *m
	}
	if out.PrevalenceLowThreshold <= 0 {
		out.PrevalenceLowThreshold = 0.02
	}
	if out.PrevalenceHighThreshold <= 0 {
		out.PrevalenceHighThreshold = 0.5
	}
	if out.PrevalenceIncrease < 0 {
		out.PrevalenceIncrease = 0.15
	}
	if out.PrevalenceDecrease < 0 {
		out.PrevalenceDecrease = 0.15
	}
	return out
}

// WindowDays parses the definition's Window ("1d"/"7d"/"14d") into a whole-day
// count, clamped to [1, 14]. Any unrecognized/empty value defaults to 1 day.
func (def ModelDefinition) WindowDays() int {
	switch def.Window {
	case "7d":
		return 7
	case "14d":
		return 14
	case "", "1d", "24h":
		return 1
	}
	// Tolerate a bare integer-of-days string as a forward-compatible escape hatch.
	if n, err := parseLeadingInt(def.Window); err == nil && n >= 1 {
		if n > 14 {
			n = 14
		}
		return n
	}
	return 1
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

// parseLeadingInt parses the leading run of digits in s (allowing a trailing unit
// suffix like "d"), returning an error if there are no leading digits.
func parseLeadingInt(s string) (int, error) {
	s = strings.TrimSpace(s)
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, strconv.ErrSyntax
	}
	return strconv.Atoi(s[:end])
}

package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"time"

	"bifract/pkg/storage"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Manager handles analytics model CRUD and the ClickHouse table+MV lifecycle.
type Manager struct {
	pg   *storage.PostgresClient
	ch   *storage.ClickHouseClient
	chDB string

	// alerts is the adapter used to keep a model's backing alert in sync. It is
	// wired post-construction (SetAlertManager) to avoid an import cycle. May be
	// nil in setups without alerting, in which case linked-alert work is skipped.
	alerts LinkedAlertManager

	// Backfill engine state. The backfill seeds a model from historical logs via
	// INSERT...SELECT (no DDL), throttled to avoid overwhelming ClickHouse.
	bfCfg     backfillConfig
	bfHealth  BackfillHealth // pause source: yields to ingest backpressure
	bfSem     chan struct{}  // global single-flight gate
	bfMu      sync.Mutex     // guards bfCancels
	bfCancels map[string]context.CancelFunc
}

// NewManager creates a new analytics model manager.
func NewManager(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *Manager {
	cfg := loadBackfillConfig()
	return &Manager{
		pg:        pg,
		ch:        ch,
		chDB:      "logs",
		bfCfg:     cfg,
		bfSem:     make(chan struct{}, cfg.concurrency),
		bfCancels: make(map[string]context.CancelFunc),
	}
}

// SetAlertManager wires in the adapter that manages a model's backing alert.
// Wired post-construction to avoid an import cycle with pkg/alerts.
func (m *Manager) SetAlertManager(a LinkedAlertManager) {
	m.alerts = a
}

// SetBackfillHealth injects the health signal the backfill engine yields to
// (typically the ingest queue's CPU/disk backpressure). Safe to leave nil, in
// which case backfill never pauses for pressure.
func (m *Manager) SetBackfillHealth(h BackfillHealth) {
	m.bfHealth = h
}

// chModelTableName returns the local CH table name for a model UUID.
func chModelTableName(id string) string {
	return "model_" + strings.ReplaceAll(id, "-", "_")
}

// chModelMVName returns the CH materialized view name for a model UUID.
func chModelMVName(id string) string {
	return "model_mv_" + strings.ReplaceAll(id, "-", "_")
}

// chModelDistName returns the distributed table name for cluster mode.
func chModelDistName(id string) string {
	return "model_dist_" + strings.ReplaceAll(id, "-", "_")
}

// chModelStateName returns the CH rolling-state table name for a scheduled
// (network) model. The MV writes here at ingest; the scorer reads from here.
func chModelStateName(id string) string {
	return "model_state_" + strings.ReplaceAll(id, "-", "_")
}

// chModelStateDistName returns the distributed table over the state table, used by
// the scorer in cluster mode so a single-replica scoring pass aggregates state from
// every shard (state is maintained per-shard by the local MV).
func chModelStateDistName(id string) string {
	return "model_diststate_" + strings.ReplaceAll(id, "-", "_")
}

// networkStateReadTable returns the table the scorer reads for a network model:
// the distributed state table in cluster mode, the local state table otherwise.
func (m *Manager) networkStateReadTable(id string) string {
	if m.ch.IsCluster() {
		return chModelStateDistName(id)
	}
	return chModelStateName(id)
}

// ---- Queries ----

// List returns all models for a fractal (V1: fractal-scoped only).
func (m *Manager) List(ctx context.Context, fractalID string) ([]*Model, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, COALESCE(fractal_id::text,''), COALESCE(prism_id::text,''),
		        name, description, model_type, definition, ch_table_name, ch_mv_name,
		        status, alert_mode, COALESCE(linked_alert_id::text,''), error_message,
		        COALESCE(created_by,''), created_at, updated_at,
		        backfill_status, backfill_window, backfill_total, backfill_done,
		        backfill_started_at, backfill_error,
		        (SELECT al.enabled FROM alerts al WHERE al.id = analytics_models.linked_alert_id)
		 FROM analytics_models WHERE fractal_id = $1 ORDER BY name`, fractalID)
	if err != nil {
		return nil, fmt.Errorf("list models: %w", err)
	}
	defer rows.Close()

	var models []*Model
	for rows.Next() {
		model, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		models = append(models, model)
	}
	if models == nil {
		models = []*Model{}
	}
	return models, rows.Err()
}

// Get returns a single model by ID.
func (m *Manager) Get(ctx context.Context, id string) (*Model, error) {
	row := m.pg.QueryRow(ctx,
		`SELECT id, COALESCE(fractal_id::text,''), COALESCE(prism_id::text,''),
		        name, description, model_type, definition, ch_table_name, ch_mv_name,
		        status, alert_mode, COALESCE(linked_alert_id::text,''), error_message,
		        COALESCE(created_by,''), created_at, updated_at,
		        backfill_status, backfill_window, backfill_total, backfill_done,
		        backfill_started_at, backfill_error,
		        (SELECT al.enabled FROM alerts al WHERE al.id = analytics_models.linked_alert_id)
		 FROM analytics_models WHERE id = $1`, id)
	model, err := scanModelRow(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("model not found")
	}
	if err != nil {
		return nil, fmt.Errorf("get model %s: %w", id, err)
	}
	return model, nil
}

// ListModelInfos returns a name→ModelInfo map for all active models in a fractal.
// Used by the query handler and alert engine to populate QueryOptions.Models.
func (m *Manager) ListModelInfos(ctx context.Context, fractalID string) (map[string]ModelInfo, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, name, model_type, definition FROM analytics_models
		 WHERE fractal_id = $1 AND status = 'active'`, fractalID)
	if err != nil {
		return nil, fmt.Errorf("list model infos: %w", err)
	}
	defer rows.Close()

	result := make(map[string]ModelInfo)
	for rows.Next() {
		var id, name, modelType string
		var defRaw []byte
		if err := rows.Scan(&id, &name, &modelType, &defRaw); err != nil {
			return nil, fmt.Errorf("scan model info: %w", err)
		}
		var def ModelDefinition
		_ = json.Unmarshal(defRaw, &def)

		tableName := chModelTableName(id)
		if m.ch.IsCluster() {
			tableName = chModelDistName(id)
		}

		result[name] = ModelInfo{
			ID:         id,
			TableName:  tableName,
			ModelType:  ModelType(modelType),
			MinSample:  def.MinSample,
			TimeBucket: def.TimeBucket,
			FractalID:  fractalID,
		}
	}
	return result, rows.Err()
}

// ---- Mutations ----

// Create creates a new model and its ClickHouse objects.
func (m *Manager) Create(ctx context.Context, fractalID string, req CreateRequest, createdBy string) (*Model, error) {
	if err := validateCreateRequest(req); err != nil {
		return nil, err
	}

	defJSON, _ := json.Marshal(req.Definition)

	var id string
	err := m.pg.QueryRow(ctx,
		`INSERT INTO analytics_models
		    (fractal_id, name, description, model_type, definition, ch_table_name, ch_mv_name,
		     status, alert_mode, created_by)
		 VALUES ($1, $2, $3, $4, $5, '', '', 'rebuilding', $6, $7)
		 RETURNING id`,
		fractalID, req.Name, req.Description, string(req.ModelType),
		string(defJSON), req.AlertMode, createdBy).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("insert model: %w", err)
	}

	tableName := chModelTableName(id)
	mvName := chModelMVName(id)

	// Store CH names in Postgres
	_, err = m.pg.Exec(ctx,
		`UPDATE analytics_models SET ch_table_name = $1, ch_mv_name = $2 WHERE id = $3`,
		tableName, mvName, id)
	if err != nil {
		_, _ = m.pg.Exec(ctx, `DELETE FROM analytics_models WHERE id = $1`, id)
		return nil, fmt.Errorf("update ch names: %w", err)
	}

	// Create the backing alert (paused). It is the single source of truth for
	// alert state; the operator enables it and configures actions/throttle on the
	// Alerts page. Best-effort: a failure here must not fail model creation, the
	// alert can be (re)created on a later update.
	if m.alerts != nil {
		mo := &Model{
			ID: id, FractalID: fractalID, Name: req.Name, Description: req.Description,
			ModelType: req.ModelType, Definition: req.Definition, CreatedBy: createdBy,
		}
		alertID, aerr := m.alerts.CreateLinkedAlert(ctx, m.alertSpec(mo, false))
		if aerr != nil {
			log.Printf("model %s: create linked alert: %v", id, aerr)
		} else if alertID != "" {
			_, _ = m.pg.Exec(ctx,
				`UPDATE analytics_models SET linked_alert_id = $1::uuid WHERE id = $2`, alertID, id)
		}
	}

	// Create ClickHouse objects in a background goroutine so the HTTP handler
	// returns immediately. ON CLUSTER DDL takes 30-70s in some deployments;
	// blocking the request causes duplicate submissions and a stuck UI.
	// The model is visible in the listing with status='rebuilding' while CH work completes.
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := m.createCHObjects(bgCtx, id, req.Definition, req.ModelType, tableName, mvName); err != nil {
			log.Printf("model %s: async create CH objects failed: %v", id, err)
			_, _ = m.pg.Exec(bgCtx, `UPDATE analytics_models SET status='error', error_message=$1 WHERE id=$2`,
				err.Error(), id)
			return
		}
		_, _ = m.pg.Exec(bgCtx, `UPDATE analytics_models SET status='active', error_message='' WHERE id=$1`, id)
	}()

	return m.Get(ctx, id)
}

// Update updates a model's definition. ClickHouse objects (and the model's data)
// are only rebuilt when the detection definition changed -- the filter,
// extractions, or shape that determine what gets captured. Editing only
// metadata (name, description) or alert thresholds preserves the existing data
// and any completed/in-flight backfill. The model type cannot change.
func (m *Manager) Update(ctx context.Context, id string, req UpdateRequest) (*Model, error) {
	existing, err := m.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	rebuild := detectionChanged(existing.Definition, req.Definition)

	if rebuild {
		// A rebuild drops and recreates the model's data, so any in-flight backfill
		// is now stale: cancel it before touching the table it writes to.
		m.CancelBackfill(id)
	}

	defJSON, _ := json.Marshal(req.Definition)
	_, err = m.pg.Exec(ctx,
		`UPDATE analytics_models SET name=$1, description=$2, definition=$3, updated_at=NOW()
		 WHERE id=$4`,
		req.Name, req.Description, string(defJSON), id)
	if err != nil {
		return nil, fmt.Errorf("update model: %w", err)
	}

	if rebuild {
		// Rebuild CH objects (drops old data, forward-only).
		// Use a background context so slow ON CLUSTER DDL does not race the HTTP deadline.
		ddlCtx, ddlCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer ddlCancel()
		_, _ = m.pg.Exec(ctx, `UPDATE analytics_models SET status='rebuilding' WHERE id=$1`, id)
		if err := m.dropCHObjects(ddlCtx, id, existing.CHTableName, existing.CHMVName, existing.ModelType); err != nil {
			log.Printf("model %s: drop CH objects during update: %v", id, err)
		}
		if err := m.createCHObjects(ddlCtx, id, req.Definition, existing.ModelType, existing.CHTableName, existing.CHMVName); err != nil {
			_, _ = m.pg.Exec(context.Background(), `UPDATE analytics_models SET status='error', error_message=$1 WHERE id=$2`,
				err.Error(), id)
			return nil, fmt.Errorf("recreate clickhouse objects: %w", err)
		}
		// Data was dropped; reset backfill state so the data viewer re-offers the
		// "Seed history" CTA against the new definition.
		_, _ = m.pg.Exec(context.Background(),
			`UPDATE analytics_models SET status='active', error_message='',
			    backfill_status='none', backfill_window='', backfill_total=0, backfill_done=0,
			    backfill_anchor=NULL, backfill_started_at=NULL, backfill_error=''
			 WHERE id=$1`, id)
	}

	updated, err := m.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	// Keep the backing alert's detection query in sync (name, thresholds, filter
	// may have changed). Operator-managed fields (actions, throttle, enabled,
	// severity) are preserved by the adapter. Create one lazily if it is missing
	// (e.g. the alert was deleted on the Alerts page, or predates this feature).
	m.syncLinkedAlert(ctx, updated)

	return m.Get(ctx, id)
}

// detectionChanged reports whether the parts of a definition that determine what
// data a model captures changed. Alert thresholds and min_sample are scored at
// query time and never alter the stored table, so they are intentionally
// excluded -- changing only those must not drop the model's data.
func detectionChanged(a, b ModelDefinition) bool {
	return !reflect.DeepEqual(a.Filter, b.Filter) ||
		!reflect.DeepEqual(a.Extractions, b.Extractions) ||
		a.PartitionKey != b.PartitionKey ||
		a.ValueKey != b.ValueKey ||
		!reflect.DeepEqual(a.KeyFields, b.KeyFields) ||
		a.TimeBucket != b.TimeBucket ||
		// Network models: the field map determines what the MV extracts and the
		// window drives the state TTL, so either change requires a state rebuild.
		// Beacon/LongConn/Modifiers params are applied fresh by the scorer at read
		// time (like alert thresholds), so they intentionally do NOT rebuild.
		!reflect.DeepEqual(a.Network, b.Network) ||
		a.Window != b.Window
}

// syncLinkedAlert updates the model's backing alert query, creating it if absent.
// Best-effort: alert sync failures are logged, not surfaced as model errors.
func (m *Manager) syncLinkedAlert(ctx context.Context, model *Model) {
	if m.alerts == nil {
		return
	}
	if model.LinkedAlertID == "" {
		alertID, err := m.alerts.CreateLinkedAlert(ctx, m.alertSpec(model, false))
		if err != nil {
			log.Printf("model %s: create linked alert on update: %v", model.ID, err)
			return
		}
		if alertID != "" {
			_, _ = m.pg.Exec(ctx,
				`UPDATE analytics_models SET linked_alert_id = $1::uuid WHERE id = $2`, alertID, model.ID)
		}
		return
	}
	if err := m.alerts.UpdateLinkedAlert(ctx, model.LinkedAlertID, m.alertSpec(model, false)); err != nil {
		log.Printf("model %s: update linked alert %s: %v", model.ID, model.LinkedAlertID, err)
	}
}

// linkedAlertName derives the backing alert's display name. Manual alert names
// are globally unique (idx_alerts_name_manual) while model names are not, so a
// short, stable fragment of the model UUID is appended to guarantee uniqueness
// (across same-named models and across fractals) and to let operators correlate
// the alert back to its model on the Alerts page.
func linkedAlertName(modelName, modelID string) string {
	frag := strings.ReplaceAll(modelID, "-", "")
	if len(frag) > 8 {
		frag = frag[:8]
	}
	return fmt.Sprintf("%s (model %s)", modelName, frag)
}

// alertSpec builds the LinkedAlertSpec for a model: the generated detection query
// plus identity/scoping. Severity defaults to medium; it is only used at creation
// (updates preserve the alert's current severity).
func (m *Manager) alertSpec(model *Model, enabled bool) LinkedAlertSpec {
	sev := "medium"
	if model.Definition.Alert != nil && model.Definition.Alert.Severity != "" {
		sev = model.Definition.Alert.Severity
	}
	return LinkedAlertSpec{
		Name:        linkedAlertName(model.Name, model.ID),
		Description: model.Description,
		QueryString: GenerateQuery(model.Name, model.Definition, model.ModelType),
		Severity:    sev,
		Enabled:     enabled,
		FractalID:   model.FractalID,
		PrismID:     model.PrismID,
		CreatedBy:   model.CreatedBy,
	}
}

// Delete removes the model from Postgres immediately, then drops ClickHouse
// objects in the background. ON CLUSTER DDL can take 30-70+ seconds; blocking
// the HTTP handler causes duplicate requests and confuses the UI.
func (m *Manager) Delete(ctx context.Context, id string) error {
	model, err := m.Get(ctx, id)
	if err != nil {
		return err
	}
	// Stop any in-flight backfill before its target table is dropped.
	m.CancelBackfill(id)
	// Delete the backing alert so it does not linger as an orphan referencing a
	// model that no longer exists. Best-effort; a failure must not block deletion.
	if m.alerts != nil && model.LinkedAlertID != "" {
		if err := m.alerts.DeleteLinkedAlert(ctx, model.LinkedAlertID); err != nil {
			log.Printf("model %s: delete linked alert %s: %v", id, model.LinkedAlertID, err)
		}
	}
	// Remove from Postgres first so the UI sees it gone immediately.
	if _, err = m.pg.Exec(ctx, `DELETE FROM analytics_models WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete model: %w", err)
	}
	// Fire-and-forget CH cleanup — slow ON CLUSTER DDL must not block the caller.
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := m.dropCHObjects(bgCtx, id, model.CHTableName, model.CHMVName, model.ModelType); err != nil {
			log.Printf("model %s: async drop CH objects: %v", id, err)
		}
	}()
	return nil
}

// SetAlertEnabled enables or pauses a model's backing alert. The alert's enabled
// flag is the source of truth (the model's alert_mode is derived from it on read),
// so this toggles the alert and mirrors the mode into the column as a fallback.
func (m *Manager) SetAlertEnabled(ctx context.Context, id string, enabled bool) error {
	model, err := m.Get(ctx, id)
	if err != nil {
		return err
	}
	// Lazily create the backing alert if it is missing (deleted on the Alerts
	// page, or a model predating linked alerts), then apply the requested state.
	if model.LinkedAlertID == "" {
		m.syncLinkedAlert(ctx, model)
		if model, err = m.Get(ctx, id); err != nil {
			return err
		}
	}
	if m.alerts == nil || model.LinkedAlertID == "" {
		return fmt.Errorf("no alert is linked to this model")
	}
	if err := m.alerts.SetLinkedAlertEnabled(ctx, model.LinkedAlertID, enabled); err != nil {
		return err
	}
	mode := "paused"
	if enabled {
		mode = "active"
	}
	_, err = m.pg.Exec(ctx,
		`UPDATE analytics_models SET alert_mode=$1, updated_at=NOW() WHERE id=$2`, mode, id)
	return err
}

// ---- ClickHouse object lifecycle ----

// isCHDDLTimeout returns true for ClickHouse error code 159 (TIMEOUT_EXCEEDED),
// which occurs when ON CLUSTER DDL exceeds distributed_ddl_task_timeout but the
// task continues running in the background on all nodes. The object is created
// successfully; the error is cosmetic and should be treated as a warning.
func isCHDDLTimeout(err error) bool {
	var ex *proto.Exception
	if errors.As(err, &ex) {
		return ex.Code == 159
	}
	return false
}

func (m *Manager) createCHObjects(ctx context.Context, id string, def ModelDefinition, mt ModelType, tableName, mvName string) error {
	if mt.IsScheduled() {
		return m.createNetworkCHObjects(ctx, id, def, mt, tableName, mvName)
	}
	tableSQL, mvSQL, err := GenerateDDL(def, mt, "`"+tableName+"`", "`"+mvName+"`")
	if err != nil {
		return err
	}

	tableSQL = m.ch.RewriteEngine(tableSQL)
	tableSQL = m.ch.InjectOnCluster(tableSQL)

	if err := m.ch.Exec(ctx, tableSQL); err != nil && !isCHDDLTimeout(err) {
		return fmt.Errorf("create model table: %w", err)
	}

	mvSQL = m.ch.InjectOnCluster(mvSQL)
	if err := m.ch.Exec(ctx, mvSQL); err != nil && !isCHDDLTimeout(err) {
		// Roll back table creation
		_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)))
		return fmt.Errorf("create model mv: %w", err)
	}

	// In cluster mode, create a Distributed table for fan-out reads.
	if m.ch.IsCluster() {
		distName := chModelDistName(id)
		distSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` AS `%s` ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			distName, tableName, storage.EscCHStr(m.ch.Cluster), tableName,
		)
		if err := m.ch.Exec(ctx, distSQL); err != nil {
			log.Printf("model %s: create distributed table: %v", id, err)
		}
	}

	return nil
}

// createNetworkCHObjects creates the three ClickHouse objects a scheduled (network)
// model owns: the rolling-state table (MV target), the results table (scorer output,
// read by model_lookup / the data viewer), and the MV that maintains state at ingest.
// Backfill is N/A: the MV + TTL self-seed the rolling window.
func (m *Manager) createNetworkCHObjects(ctx context.Context, id string, def ModelDefinition, mt ModelType, tableName, mvName string) error {
	stateName := chModelStateName(id)
	windowDays := def.WindowDays()

	stateSQL := m.ch.InjectOnCluster(m.ch.RewriteEngine(BuildNetStateTableDDL("`"+stateName+"`", windowDays)))
	if err := m.ch.Exec(ctx, stateSQL); err != nil && !isCHDDLTimeout(err) {
		return fmt.Errorf("create state table: %w", err)
	}

	resultsSQL := m.ch.InjectOnCluster(m.ch.RewriteEngine(BuildNetResultsTableDDL("`"+tableName+"`")))
	if err := m.ch.Exec(ctx, resultsSQL); err != nil && !isCHDDLTimeout(err) {
		_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", stateName)))
		return fmt.Errorf("create results table: %w", err)
	}

	mvSQL, err := BuildNetStateMV(def, mt, "`"+stateName+"`", "`"+mvName+"`")
	if err != nil {
		return err
	}
	if err := m.ch.Exec(ctx, m.ch.InjectOnCluster(mvSQL)); err != nil && !isCHDDLTimeout(err) {
		// Roll back state + results so a failed create leaves nothing behind.
		_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", stateName)))
		_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)))
		return fmt.Errorf("create state mv: %w", err)
	}

	// Distributed tables (cluster mode): one over the results table for fan-out
	// reads, one over the state table so the scorer aggregates state across shards.
	if m.ch.IsCluster() {
		cl := storage.EscCHStr(m.ch.Cluster)
		distResults := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` AS `%s` ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			chModelDistName(id), tableName, cl, tableName,
		)
		if err := m.ch.Exec(ctx, distResults); err != nil {
			log.Printf("model %s: create distributed results table: %v", id, err)
		}
		distState := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` AS `%s` ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			chModelStateDistName(id), stateName, cl, stateName,
		)
		if err := m.ch.Exec(ctx, distState); err != nil {
			log.Printf("model %s: create distributed state table: %v", id, err)
		}
	}
	return nil
}

func (m *Manager) dropCHObjects(ctx context.Context, id, tableName, mvName string, mt ModelType) error {
	// Order: drop the MV first so it can never write to a half-dropped target, then
	// the target tables. Every drop is IF EXISTS + best-effort (log and continue) so
	// a partial prior failure still fully cleans up. InjectOnCluster fans each drop
	// out to every shard/replica.
	mvDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP VIEW IF EXISTS `%s`", mvName))
	if err := m.ch.Exec(ctx, mvDrop); err != nil {
		log.Printf("drop MV %s: %v", mvName, err)
	}
	// A scheduled model also owns a rolling-state table (the MV's target); drop it.
	if mt.IsScheduled() {
		stateDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", chModelStateName(id)))
		if err := m.ch.Exec(ctx, stateDrop); err != nil {
			log.Printf("drop state table %s: %v", chModelStateName(id), err)
		}
	}
	tableDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	if err := m.ch.Exec(ctx, tableDrop); err != nil {
		log.Printf("drop table %s: %v", tableName, err)
	}
	// Drop the distributed table(s) (cluster mode) so no dangling fan-out object remains.
	if m.ch.IsCluster() {
		distDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", chModelDistName(id)))
		if err := m.ch.Exec(ctx, distDrop); err != nil {
			log.Printf("drop distributed table %s: %v", chModelDistName(id), err)
		}
		if mt.IsScheduled() {
			distStateDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", chModelStateDistName(id)))
			if err := m.ch.Exec(ctx, distStateDrop); err != nil {
				log.Printf("drop distributed state table %s: %v", chModelStateDistName(id), err)
			}
		}
	}
	return nil
}

// RowCount returns the approximate number of rows in a model's aggregating table.
func (m *Manager) RowCount(ctx context.Context, tableName string) (uint64, error) {
	rows, err := m.ch.Query(ctx, fmt.Sprintf("SELECT count() FROM `%s`", tableName))
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	switch v := rows[0]["count()"].(type) {
	case uint64:
		return v, nil
	case *uint64:
		if v != nil {
			return *v, nil
		}
	}
	return 0, nil
}

// readTableName returns the distributed table name in cluster mode, otherwise the local table.
// Always use this for read queries so they fan out across all shards.
func (m *Manager) readTableName(model *Model) string {
	if m.ch.IsCluster() {
		return chModelDistName(model.ID)
	}
	return model.CHTableName
}

// GetData returns paginated model data with computed scores.
// For rarity: runs the triple-nested scoring subquery.
// For first_seen: returns entity_key, first_seen, last_seen, event_count.
func (m *Manager) GetData(ctx context.Context, model *Model, fractalID, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if sortDir != "asc" {
		sortDir = "desc"
	}

	tableName := m.readTableName(model)
	switch model.ModelType {
	case ModelTypeRarity:
		return m.getRarityData(ctx, tableName, fractalID, search, sortCol, sortDir, limit, offset)
	case ModelTypeFirstSeen:
		return m.getFirstSeenData(ctx, tableName, fractalID, search, sortCol, sortDir, limit, offset)
	case ModelTypeVolumeBaseline:
		return m.getVolumeBaselineData(ctx, tableName, fractalID, model.Definition, search, sortCol, sortDir, limit, offset)
	case ModelTypeBeacon, ModelTypeLongConnection:
		return m.getNetworkData(ctx, tableName, fractalID, search, sortCol, sortDir, limit, offset)
	default:
		return nil, 0, fmt.Errorf("unknown model type: %s", model.ModelType)
	}
}

// buildRarityScoredSQL returns the per-(partition,value) scored projection
// (model_count, percent, confidence, days). `source` is the FROM expression
// yielding rows shaped like the rarity model table -- fractal_id, partition_val,
// value_val, event_count, and the groupUniqArray(Date) state `days`. Live
// scoring passes "`tbl` FINAL"; the preview passes a windowed, day-bucketed
// aggregation subquery. The math is identical either way, so a preview matches
// the post-backfill table exactly. fidEsc must already be CH-escaped.
func buildRarityScoredSQL(source, fidEsc string) string {
	return fmt.Sprintf(`
SELECT partition_val, value_val,
    event_count AS model_count,
    _total AS model_total,
    round(event_count / _total * 100.0, 4) AS percent,
    round(((_total - _unique) / _total) * 0.95, 4) AS confidence,
    days
FROM (
    SELECT partition_val, value_val, event_count, days,
        sum(event_count) OVER (PARTITION BY partition_val) AS _total,
        uniqExact(value_val) OVER (PARTITION BY partition_val) AS _unique
    FROM (
        SELECT partition_val, value_val, sum(event_count) AS event_count,
            arraySort(groupUniqArrayMerge(365)(days)) AS days
        FROM %s
        WHERE fractal_id = '%s'
        GROUP BY partition_val, value_val
    )
)
WHERE event_count >= 1`, source, fidEsc)
}

func (m *Manager) getRarityData(ctx context.Context, tableName, fractalID, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"partition_val": true, "value_val": true, "model_count": true, "percent": true, "confidence": true}
	if !allowed[sortCol] {
		sortCol = "confidence"
	}

	baseQuery := buildRarityScoredSQL("`"+tableName+"` FINAL", storage.EscCHStr(fractalID))

	if search != "" {
		baseQuery += fmt.Sprintf(" AND (partition_val ILIKE '%%%s%%' OR value_val ILIKE '%%%s%%')",
			storage.EscCHStr(search), storage.EscCHStr(search))
	}

	countQuery := fmt.Sprintf("SELECT count() FROM (%s)", baseQuery)
	var total uint64
	if err := m.ch.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count rarity data: %w", err)
	}

	dataQuery := fmt.Sprintf("%s ORDER BY %s %s LIMIT %d OFFSET %d", baseQuery, sortCol, strings.ToUpper(sortDir), limit, offset)
	rows, err := m.ch.Query(ctx, dataQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("query rarity data: %w", err)
	}
	convertDaysToStrings(rows)
	return rows, total, nil
}

// firstSeenAggSQL returns the per-entity aggregation (first_seen, last_seen,
// event_count, days) for a first_seen model. `source` is the FROM expression
// (live: "`tbl` FINAL"; preview: a windowed aggregation subquery); extraWhere is
// an optional predicate ANDed into the scan (e.g. a search filter). first_seen's
// aggregates (min/max/sum over exact timestamps) are day-chunk invariant, so the
// preview matches the post-backfill table without day bucketing.
func firstSeenAggSQL(source, fidEsc, extraWhere string) string {
	q := fmt.Sprintf(`
SELECT entity_key,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    sum(event_count) AS event_count,
    arraySort(groupUniqArrayMerge(365)(days)) AS days
FROM %s
WHERE fractal_id = '%s'`, source, fidEsc)
	if extraWhere != "" {
		q += "\nAND " + extraWhere
	}
	q += "\nGROUP BY entity_key"
	return q
}

func (m *Manager) getFirstSeenData(ctx context.Context, tableName, fractalID, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"entity_key": true, "first_seen": true, "last_seen": true, "event_count": true}
	if !allowed[sortCol] {
		sortCol = "first_seen"
	}

	extra := ""
	if search != "" {
		extra = fmt.Sprintf("entity_key ILIKE '%%%s%%'", storage.EscCHStr(search))
	}
	baseQuery := firstSeenAggSQL("`"+tableName+"` FINAL", storage.EscCHStr(fractalID), extra)

	countQuery := fmt.Sprintf("SELECT count() FROM (%s)", baseQuery)
	var total uint64
	if err := m.ch.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count first_seen data: %w", err)
	}

	dataQuery := fmt.Sprintf("%s ORDER BY %s %s LIMIT %d OFFSET %d", baseQuery, sortCol, strings.ToUpper(sortDir), limit, offset)
	rows, err := m.ch.Query(ctx, dataQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("query first_seen data: %w", err)
	}
	convertDaysToStrings(rows)
	return rows, total, nil
}

// volumeMinBuckets returns the minimum number of complete buckets of history an
// entity must have before it is scored, defaulting to 7 when unset.
func volumeMinBuckets(def ModelDefinition) int {
	if def.MinSample > 0 {
		return def.MinSample
	}
	return 7
}

// buildVolumeBaselineScoringSQL returns the per-entity modified z-score query for
// a volume_baseline model. It computes, over the entity's complete buckets, the
// median daily count (baseline), the Median Absolute Deviation (MAD), the most
// recent complete bucket's count, and the modified z-score
// (0.6745 * (count - median) / MAD), matching Bifract's BQL modifiedZScore()
// convention including the mad=0 -> z=0 guard.
//
// `source` is the FROM expression yielding rows shaped like the volume model
// table (fractal_id, entity_val, bucket, event_count): live scoring passes
// "`tbl` FINAL"; the preview passes a windowed aggregation subquery. lower/upper
// bound the scored buckets (upper excludes the current incomplete bucket). Volume
// counts are additive across day chunks, so the preview matches the post-backfill
// table. fidEsc must already be CH-escaped; lower/upper are raw SQL bound exprs.
func buildVolumeBaselineScoringSQL(source, fidEsc string, minBuckets int, lower, upper string) string {
	if minBuckets < 1 {
		minBuckets = 1
	}
	return fmt.Sprintf(`SELECT entity_val, latest_count, baseline_median, mad, n_buckets, latest_bucket, days,
    if(mad = 0, 0, round(0.6745 * (toFloat64(latest_count) - baseline_median) / mad, 4)) AS z_score
FROM (
    SELECT entity_val, latest_count, baseline_median, n_buckets, latest_bucket, days,
        arrayReduce('medianExact', arrayMap(x -> abs(toFloat64(x) - baseline_median), cnts)) AS mad
    FROM (
        SELECT entity_val,
            groupArray(daily_count) AS cnts,
            arrayReduce('medianExact', groupArray(daily_count)) AS baseline_median,
            argMax(daily_count, bucket) AS latest_count,
            max(bucket) AS latest_bucket,
            count() AS n_buckets,
            arraySort(groupUniqArray(365)(toDate(bucket))) AS days
        FROM (
            SELECT entity_val, bucket, sum(event_count) AS daily_count
            FROM %s
            WHERE fractal_id = '%s' AND bucket >= %s AND bucket < %s
            GROUP BY entity_val, bucket
        )
        GROUP BY entity_val
    )
)
WHERE n_buckets >= %d`, source, fidEsc, lower, upper, minBuckets)
}

func (m *Manager) getVolumeBaselineData(ctx context.Context, tableName, fractalID string, def ModelDefinition, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"entity_val": true, "latest_count": true, "baseline_median": true, "mad": true, "z_score": true, "n_buckets": true, "latest_bucket": true}
	if !allowed[sortCol] {
		sortCol = "z_score"
	}

	lower, upper := volumeScoreBounds(def.TimeBucket)
	baseQuery := buildVolumeBaselineScoringSQL("`"+tableName+"` FINAL", storage.EscCHStr(fractalID), volumeMinBuckets(def), lower, upper)
	if search != "" {
		baseQuery += fmt.Sprintf("\nAND entity_val ILIKE '%%%s%%'", storage.EscCHStr(search))
	}

	countQuery := fmt.Sprintf("SELECT count() FROM (%s)", baseQuery)
	var total uint64
	if err := m.ch.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count volume_baseline data: %w", err)
	}

	// Sort by absolute z-score so the largest anomalies (high or low) surface first.
	orderExpr := sortCol
	if sortCol == "z_score" {
		orderExpr = "abs(z_score)"
	}
	dataQuery := fmt.Sprintf("%s ORDER BY %s %s LIMIT %d OFFSET %d", baseQuery, orderExpr, strings.ToUpper(sortDir), limit, offset)
	rows, err := m.ch.Query(ctx, dataQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("query volume_baseline data: %w", err)
	}
	convertDaysToStrings(rows)
	return rows, total, nil
}

// GetStats returns aggregate statistics for a model's data table.
// networkResultCols is the projection returned to the data viewer for a scored
// pair: the final verdict plus the full breakdown (subscores + prevalence modifier)
// so the reviewer can see why a pair scored high.
const networkResultCols = "src_ip, dst_ip, dst_port, " +
	"round(final_score,3) AS final_score, round(regularity_score,3) AS regularity_score, " +
	"round(ts_score,3) AS ts_score, round(ds_score,3) AS ds_score, round(dur_score,3) AS dur_score, round(hist_score,3) AS hist_score, " +
	"round(prevalence,4) AS prevalence, prevalence_total, round(prevalence_score,3) AS prevalence_score, " +
	"conn_count, round(total_duration,1) AS total_duration, first_seen, last_seen, scored_at"

// networkScoreThreshold returns the model's final-score flag threshold.
func networkScoreThreshold(def ModelDefinition, mt ModelType) float64 {
	if mt == ModelTypeLongConnection {
		return def.LongConn.WithDefaults().ScoreThreshold
	}
	return def.Beacon.WithDefaults(int64(def.WindowDays())*86400).ScoreThreshold
}

// getNetworkData returns scored pairs from a network model's results table, ranked
// by final_score (severity order) by default. Both beacon and long_connection share
// this table.
func (m *Manager) getNetworkData(ctx context.Context, tableName, fractalID, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"final_score": true, "regularity_score": true, "conn_count": true, "total_duration": true, "prevalence": true, "last_seen": true}
	if !allowed[sortCol] {
		sortCol = "final_score"
	}
	fid := storage.EscCHStr(fractalID)
	where := fmt.Sprintf("fractal_id = '%s'", fid)
	if search != "" {
		s := storage.EscCHStr(search)
		where += fmt.Sprintf(" AND (src_ip ILIKE '%%%s%%' OR dst_ip ILIKE '%%%s%%')", s, s)
	}
	base := fmt.Sprintf("SELECT %s FROM `%s` FINAL WHERE %s", networkResultCols, tableName, where)

	var total uint64
	if err := m.ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM (%s)", base)).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count network data: %w", err)
	}
	dataQuery := fmt.Sprintf("%s ORDER BY %s %s LIMIT %d OFFSET %d", base, sortCol, strings.ToUpper(sortDir), limit, offset)
	rows, err := m.ch.Query(ctx, dataQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("query network data: %w", err)
	}
	return rows, total, nil
}

func (m *Manager) getNetworkStats(ctx context.Context, qt, fid string, def ModelDefinition, mt ModelType) (map[string]interface{}, error) {
	threshold := networkScoreThreshold(def, mt)
	q := fmt.Sprintf(`SELECT count() AS total_pairs,
       countIf(final_score >= %g) AS flagged,
       countIf(final_score > 0.8) AS critical,
       round(max(final_score), 3) AS max_score
FROM %s FINAL WHERE fractal_id = '%s'`, threshold, qt, fid)
	rows, err := m.ch.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("network stats: %w", err)
	}
	result := map[string]interface{}{}
	if len(rows) > 0 {
		result["total_pairs"] = rows[0]["total_pairs"]
		result["flagged"] = rows[0]["flagged"]
		result["critical"] = rows[0]["critical"]
		result["max_score"] = rows[0]["max_score"]
	}
	return result, nil
}

func (m *Manager) getNetworkHistogram(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	inner := fmt.Sprintf("SELECT final_score FROM %s FINAL WHERE fractal_id = '%s'", qt, fid)
	buckets, err := m.runHistogram(ctx, inner, "least(toUInt64(floor(final_score * 10)), 9)", rarityHistLabels)
	if err != nil {
		return nil, fmt.Errorf("network histogram: %w", err)
	}
	return map[string]interface{}{"metric": "final_score", "buckets": buckets}, nil
}

func (m *Manager) GetStats(ctx context.Context, model *Model, fractalID string) (map[string]interface{}, error) {
	tableName := m.readTableName(model)
	qt := "`" + tableName + "`"
	fid := storage.EscCHStr(fractalID)

	switch model.ModelType {
	case ModelTypeRarity:
		return m.getRarityStats(ctx, qt, fid)
	case ModelTypeFirstSeen:
		return m.getFirstSeenStats(ctx, qt, fid)
	case ModelTypeVolumeBaseline:
		return m.getVolumeBaselineStats(ctx, tableName, model.Definition, fid)
	case ModelTypeBeacon, ModelTypeLongConnection:
		return m.getNetworkStats(ctx, qt, fid, model.Definition, model.ModelType)
	default:
		return nil, fmt.Errorf("unknown model type: %s", model.ModelType)
	}
}

func (m *Manager) getRarityStats(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	summaryQ := fmt.Sprintf(`SELECT count() AS total_rows, uniq(partition_val) AS distinct_partitions FROM %s FINAL WHERE fractal_id = '%s'`, qt, fid)
	rows, err := m.ch.Query(ctx, summaryQ)
	if err != nil {
		return nil, fmt.Errorf("rarity stats: %w", err)
	}
	result := map[string]interface{}{}
	if len(rows) > 0 {
		result["total_rows"] = rows[0]["total_rows"]
		result["distinct_partitions"] = rows[0]["distinct_partitions"]
	}
	topQ := fmt.Sprintf(`SELECT partition_val, sum(event_count) AS cnt FROM %s FINAL WHERE fractal_id = '%s' GROUP BY partition_val ORDER BY cnt DESC LIMIT 5`, qt, fid)
	topRows, err := m.ch.Query(ctx, topQ)
	if err == nil {
		result["top_partitions"] = topRows
	}
	return result, nil
}

func (m *Manager) getFirstSeenStats(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	q := fmt.Sprintf(`
SELECT count() AS total_entities,
       min(first_seen) AS oldest_seen,
       max(last_seen) AS newest_seen,
       countIf(first_seen >= now() - INTERVAL 1 DAY) AS new_today
FROM (
    SELECT entity_key, min(first_seen) AS first_seen, max(last_seen) AS last_seen
    FROM %s FINAL WHERE fractal_id = '%s'
    GROUP BY entity_key
)`, qt, fid)
	rows, err := m.ch.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("first_seen stats: %w", err)
	}
	result := map[string]interface{}{}
	if len(rows) > 0 {
		result["total_entities"] = rows[0]["total_entities"]
		result["oldest_seen"] = rows[0]["oldest_seen"]
		result["newest_seen"] = rows[0]["newest_seen"]
		result["new_today"] = rows[0]["new_today"]
	}
	return result, nil
}

func (m *Manager) getVolumeBaselineStats(ctx context.Context, tableName string, def ModelDefinition, fid string) (map[string]interface{}, error) {
	threshold := 3.5
	if def.Alert != nil && def.Alert.ZThreshold > 0 {
		threshold = def.Alert.ZThreshold
	}
	lower, upper := volumeScoreBounds(def.TimeBucket)
	scoring := buildVolumeBaselineScoringSQL("`"+tableName+"` FINAL", fid, volumeMinBuckets(def), lower, upper)
	q := fmt.Sprintf(`SELECT count() AS total_entities,
       countIf(abs(z_score) > %g) AS anomalous,
       round(max(abs(z_score)), 4) AS max_z
FROM (%s)`, threshold, scoring)
	rows, err := m.ch.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("volume_baseline stats: %w", err)
	}
	result := map[string]interface{}{}
	if len(rows) > 0 {
		result["total_entities"] = rows[0]["total_entities"]
		result["anomalous"] = rows[0]["anomalous"]
		result["max_z"] = rows[0]["max_z"]
	}
	return result, nil
}

// histBucket is one bar of a score-distribution histogram.
type histBucket struct {
	Label string `json:"label"`
	Count uint64 `json:"count"`
}

// GetHistogram returns a type-aware score distribution so creators can see the
// shape of a model's output: rarity -> confidence (0..1), volume_baseline ->
// |z-score| bands, first_seen -> event_count on a log scale. It runs over the
// already aggregated per-model result table (never the raw log table), so it is
// a single cheap GROUP BY.
func (m *Manager) GetHistogram(ctx context.Context, model *Model, fractalID string) (map[string]interface{}, error) {
	tableName := m.readTableName(model)
	qt := "`" + tableName + "`"
	fid := storage.EscCHStr(fractalID)
	switch model.ModelType {
	case ModelTypeRarity:
		return m.getRarityHistogram(ctx, qt, fid)
	case ModelTypeFirstSeen:
		return m.getFirstSeenHistogram(ctx, qt, fid)
	case ModelTypeVolumeBaseline:
		return m.getVolumeBaselineHistogram(ctx, tableName, model.Definition, fid)
	case ModelTypeBeacon, ModelTypeLongConnection:
		return m.getNetworkHistogram(ctx, qt, fid)
	default:
		return nil, fmt.Errorf("unknown model type: %s", model.ModelType)
	}
}

// runHistogram buckets the rows produced by innerSQL using bucketExpr (which must
// evaluate to a 0-based bucket index) and returns counts zero-filled to labels so
// the distribution always has a stable, complete x-axis.
func (m *Manager) runHistogram(ctx context.Context, innerSQL, bucketExpr string, labels []string) ([]histBucket, error) {
	rows, err := m.ch.Query(ctx, histogramQuerySQL(innerSQL, bucketExpr))
	if err != nil {
		return nil, err
	}
	return fillHistogram(rows, labels), nil
}

// histogramQuerySQL wraps an inner SELECT (which projects the metric column) into
// the bucket-count query. It is kept separate from execution so the preview can
// append top-level SETTINGS (which are illegal inside a subquery).
func histogramQuerySQL(innerSQL, bucketExpr string) string {
	return fmt.Sprintf("SELECT %s AS bucket, count() AS cnt FROM (%s) GROUP BY bucket", bucketExpr, innerSQL)
}

// fillHistogram zero-fills bucket-count rows to labels so the distribution always
// has a stable, complete x-axis.
func fillHistogram(rows []map[string]interface{}, labels []string) []histBucket {
	counts := make([]uint64, len(labels))
	for _, row := range rows {
		idx := int(numToUint64(row["bucket"]))
		if idx >= 0 && idx < len(labels) {
			counts[idx] += numToUint64(row["cnt"])
		}
	}
	out := make([]histBucket, len(labels))
	for i, l := range labels {
		out[i] = histBucket{Label: l, Count: counts[i]}
	}
	return out
}

// numToUint64 tolerantly converts a ClickHouse row value to uint64.
func numToUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case uint32:
		return uint64(n)
	case int64:
		if n > 0 {
			return uint64(n)
		}
	case float64:
		if n > 0 {
			return uint64(n)
		}
	}
	return 0
}

// Score-distribution histogram specs. Shared by the built-model histogram
// (GetHistogram) and the pre-save preview so both render identically. Each pair
// is (bucketExpr -> 0-based band index, labels). The inner SQL that feeds them is
// type-specific and built from the shared scoring/aggregation builders.
var (
	rarityHistLabels        = []string{"0.0-0.1", "0.1-0.2", "0.2-0.3", "0.3-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0"}
	firstSeenHistLabels     = []string{"1-9", "10-99", "100-999", "1K-9.9K", "10K-99K", "100K+"}
	volumeHistLabels        = []string{"0-1", "1-2", "2-3", "3-4", "4-5", "5+"}
	rarityHistBucketExpr    = "least(toUInt64(floor(confidence * 10)), 9)"
	firstSeenHistBucketExpr = "least(toUInt64(floor(log10(event_count))), 5)"
	volumeHistBucketExpr    = "least(toUInt64(floor(abs(z_score))), 5)"
)

// rarityConfidenceInner returns the SQL projecting one `confidence` column per
// scored rarity row, ready for histogram bucketing. `source` is the scored-rows
// FROM expression (see buildRarityScoredSQL).
func rarityConfidenceInner(source, fidEsc string) string {
	return "SELECT confidence FROM (" + buildRarityScoredSQL(source, fidEsc) + ")"
}

// firstSeenCountInner returns the SQL projecting one `event_count` column per
// first_seen entity, ready for histogram bucketing.
func firstSeenCountInner(source, fidEsc string) string {
	return "SELECT toUInt64(event_count) AS event_count FROM (" + firstSeenAggSQL(source, fidEsc, "") + ") WHERE event_count >= 1"
}

func (m *Manager) getRarityHistogram(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	buckets, err := m.runHistogram(ctx, rarityConfidenceInner(qt+" FINAL", fid), rarityHistBucketExpr, rarityHistLabels)
	if err != nil {
		return nil, fmt.Errorf("rarity histogram: %w", err)
	}
	return map[string]interface{}{"metric": "confidence", "buckets": buckets}, nil
}

func (m *Manager) getFirstSeenHistogram(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	buckets, err := m.runHistogram(ctx, firstSeenCountInner(qt+" FINAL", fid), firstSeenHistBucketExpr, firstSeenHistLabels)
	if err != nil {
		return nil, fmt.Errorf("first_seen histogram: %w", err)
	}
	return map[string]interface{}{"metric": "event_count", "buckets": buckets}, nil
}

func (m *Manager) getVolumeBaselineHistogram(ctx context.Context, tableName string, def ModelDefinition, fid string) (map[string]interface{}, error) {
	lower, upper := volumeScoreBounds(def.TimeBucket)
	inner := buildVolumeBaselineScoringSQL("`"+tableName+"` FINAL", fid, volumeMinBuckets(def), lower, upper)
	buckets, err := m.runHistogram(ctx, inner, volumeHistBucketExpr, volumeHistLabels)
	if err != nil {
		return nil, fmt.Errorf("volume_baseline histogram: %w", err)
	}
	return map[string]interface{}{"metric": "z_score", "buckets": buckets}, nil
}

// TestExtraction runs a sample extraction against logs and returns matched values plus the generated SQL.
func (m *Manager) TestExtraction(ctx context.Context, fractalID string, filter []FilterCondition, extractions []ExtractionStep) ([]map[string]interface{}, string, error) {
	tableName := m.ch.ReadTable()
	if len(extractions) == 0 {
		return nil, "", fmt.Errorf("no extractions provided")
	}

	var b strings.Builder
	b.WriteString("WITH\nbase AS (\n    SELECT timestamp, raw_log, log_id")
	seen := map[string]bool{}
	for _, ext := range extractions {
		if !isExtractionOutput(ext.FromField, extractions) && !seen[ext.FromField] {
			seen[ext.FromField] = true
			b.WriteString(fmt.Sprintf(", %s AS %s", chFieldRef(ext.FromField), ext.FromField))
		}
	}
	b.WriteString(fmt.Sprintf("\n    FROM %s\n    WHERE fractal_id = '%s'", tableName, storage.EscCHStr(fractalID)))
	for _, fc := range filter {
		b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
	}
	b.WriteString("\n    ORDER BY timestamp DESC\n    LIMIT 1000\n)")

	prevCTE := "base"
	for i, ext := range extractions {
		cteName := fmt.Sprintf("e%d", i)
		fromRef := ext.FromField
		sqlPat := chStringLiteral(extractPattern(ext.Pattern))
		b.WriteString(fmt.Sprintf(",\n%s AS (\n    SELECT *, extract(%s, %s) AS %s\n    FROM %s\n    WHERE extract(%s, %s) != ''",
			cteName, fromRef, sqlPat, ext.OutputField, prevCTE, fromRef, sqlPat))
		if ext.MinLength > 0 {
			b.WriteString(fmt.Sprintf("\n    AND length(extract(%s, %s)) >= %d", fromRef, sqlPat, ext.MinLength))
		}
		b.WriteString("\n)")
		prevCTE = cteName
	}

	// Final select: sample of matched values
	lastExt := extractions[len(extractions)-1]
	outField := lastExt.OutputField
	b.WriteString(fmt.Sprintf("\nSELECT %s, count() AS cnt FROM %s GROUP BY %s ORDER BY cnt DESC LIMIT 50",
		outField, prevCTE, outField))

	sql := b.String()
	results, err := m.ch.Query(ctx, sql)
	return results, sql, err
}

// convertDaysToStrings walks rows returned from ClickHouse and converts any
// "days" column from []time.Time (how the CH driver returns Array(Date)) to
// []string in YYYY-MM-DD format so the JSON response is predictable.
func convertDaysToStrings(rows []map[string]interface{}) {
	for _, row := range rows {
		v, ok := row["days"]
		if !ok {
			continue
		}
		switch d := v.(type) {
		case []time.Time:
			strs := make([]string, len(d))
			for i, t := range d {
				strs[i] = t.UTC().Format("2006-01-02")
			}
			row["days"] = strs
		case []interface{}:
			strs := make([]string, 0, len(d))
			for _, elem := range d {
				if t, ok := elem.(time.Time); ok {
					strs = append(strs, t.UTC().Format("2006-01-02"))
				}
			}
			row["days"] = strs
		}
	}
}

// ---- Scanning helpers ----

type modelScannable interface {
	Scan(dest ...interface{}) error
}

func scanModel(rows interface {
	Scan(dest ...interface{}) error
}) (*Model, error) {
	return scanModelRow(rows)
}

func scanModelRow(row modelScannable) (*Model, error) {
	var mo Model
	var defRaw []byte
	var alertEnabled sql.NullBool
	err := row.Scan(
		&mo.ID, &mo.FractalID, &mo.PrismID,
		&mo.Name, &mo.Description, &mo.ModelType,
		&defRaw, &mo.CHTableName, &mo.CHMVName,
		&mo.Status, &mo.AlertMode, &mo.LinkedAlertID, &mo.ErrorMessage,
		&mo.CreatedBy, &mo.CreatedAt, &mo.UpdatedAt,
		&mo.BackfillStatus, &mo.BackfillWindow, &mo.BackfillTotal, &mo.BackfillDone,
		&mo.BackfillStartedAt, &mo.BackfillError,
		&alertEnabled,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(defRaw, &mo.Definition); err != nil {
		mo.Definition = ModelDefinition{}
	}
	// Derive the alert mode from the backing alert so it stays accurate no matter
	// where the alert was toggled (the Alerts page, the model list, the API). The
	// stored alert_mode column is a fallback when there is no linked alert.
	if mo.LinkedAlertID != "" {
		if alertEnabled.Valid && alertEnabled.Bool {
			mo.AlertMode = "active"
		} else {
			mo.AlertMode = "paused"
		}
	} else {
		mo.AlertMode = "none"
	}
	return &mo, nil
}

// ---- Validation ----

// validateDefinitionShape checks the model type and the per-type required shape
// fields. Shared by create/update validation and the pre-save preview so both
// reject the same invalid definitions with identical messages.
func validateDefinitionShape(mt ModelType, def ModelDefinition) error {
	switch mt {
	case ModelTypeRarity:
		if def.PartitionKey == "" {
			return fmt.Errorf("partition_key is required for rarity models")
		}
		if def.ValueKey == "" {
			return fmt.Errorf("value_key is required for rarity models")
		}
	case ModelTypeFirstSeen:
		if len(def.KeyFields) == 0 {
			return fmt.Errorf("key_fields is required for first_seen models")
		}
	case ModelTypeVolumeBaseline:
		if len(def.KeyFields) == 0 {
			return fmt.Errorf("key_fields is required for volume_baseline models")
		}
		if def.TimeBucket != "" && def.TimeBucket != "day" && def.TimeBucket != "hour" {
			return fmt.Errorf("invalid time_bucket for volume_baseline: %s (use day or hour)", def.TimeBucket)
		}
	case ModelTypeBeacon, ModelTypeLongConnection:
		// Field map defaults make src/dst near-always resolvable; only reject an
		// explicitly blanked src/dst. The window must be one of the supported values.
		nf := def.Network.WithDefaults()
		if nf.SrcField == "" || nf.DstField == "" {
			return fmt.Errorf("network models require a source and destination field")
		}
		switch def.Window {
		case "", "1d", "24h", "7d", "14d":
		default:
			return fmt.Errorf("invalid window for network model: %s (use 1d, 7d, or 14d)", def.Window)
		}
	default:
		return fmt.Errorf("invalid model type: %s", mt)
	}
	return nil
}

func validateCreateRequest(req CreateRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if err := validateDefinitionShape(req.ModelType, req.Definition); err != nil {
		return err
	}
	alertMode := req.AlertMode
	if alertMode == "" {
		alertMode = "none"
	}
	if alertMode != "none" && alertMode != "paused" && alertMode != "active" {
		return fmt.Errorf("invalid alert_mode: %s", alertMode)
	}
	return nil
}

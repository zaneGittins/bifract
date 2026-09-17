package models

import (
	"bifract/pkg/dictionaries"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
	"github.com/lib/pq"
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

	// dicts resolves the context lists a source query consults. Wired
	// post-construction for the same reason as alerts. Nil leaves match() in a
	// source query unresolvable, which it reports rather than compiling to nothing.
	dicts DictionaryResolver

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

// Model object names live in pkg/storage so the log-data reset, which cannot
// import this package, drops exactly what is created here.
func chModelTableName(id string) string     { return storage.ModelCHTableName(id) }
func chModelMVName(id string) string        { return storage.ModelCHMVName(id) }
func chModelDistName(id string) string      { return storage.ModelCHDistName(id) }
func chModelStateName(id string) string     { return storage.ModelCHStateName(id) }
func chModelStateDistName(id string) string { return storage.ModelCHStateDistName(id) }

// networkStateReadTable returns the table the scorer reads for a network model:
// the distributed state table in cluster mode, the local state table otherwise.
func (m *Manager) networkStateReadTable(id string) string {
	if m.ch.Topology().DistributedTables {
		return chModelStateDistName(id)
	}
	return chModelStateName(id)
}

// ---- Queries ----

// List returns all models for a fractal (V1: fractal-scoped only).
func (m *Manager) List(ctx context.Context, fractalID string) ([]*Model, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT `+modelColumns+`
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
		`SELECT `+modelColumns+`
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
		distributed := m.ch.Topology().DistributedTables
		if distributed {
			tableName = chModelDistName(id)
		}

		result[name] = ModelInfo{
			ID:          id,
			TableName:   tableName,
			ModelType:   ModelType(modelType),
			MinSample:   def.MinSample,
			TimeBucket:  def.TimeBucket,
			FractalID:   fractalID,
			Distributed: distributed,
		}
	}
	return result, rows.Err()
}

// ListModelInfosForFractals returns a name→ModelInfo map for all active models
// owned by any of the given fractals. Used for model_lookup() resolution in a
// prism query, where the model being referenced lives in one specific member
// fractal rather than the prism itself (models have no prism_id of their own).
//
// Member fractals can hold same-named models. Postgres returns rows in no
// guaranteed order, so an unordered scan let the winner flip between runs and the
// same query resolve to a different model table. Ordering by creation makes the
// resolution stable: the oldest model with a given name wins, every time.
func (m *Manager) ListModelInfosForFractals(ctx context.Context, fractalIDs []string) (map[string]ModelInfo, error) {
	result := make(map[string]ModelInfo)
	if len(fractalIDs) == 0 {
		return result, nil
	}
	rows, err := m.pg.Query(ctx,
		`SELECT id, name, model_type, definition, fractal_id FROM analytics_models
		 WHERE fractal_id = ANY($1) AND status = 'active'
		 ORDER BY created_at ASC, id ASC`, pq.Array(fractalIDs))
	if err != nil {
		return nil, fmt.Errorf("list model infos for fractals: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, name, modelType, ownerFractalID string
		var defRaw []byte
		if err := rows.Scan(&id, &name, &modelType, &defRaw, &ownerFractalID); err != nil {
			return nil, fmt.Errorf("scan model info: %w", err)
		}
		var def ModelDefinition
		_ = json.Unmarshal(defRaw, &def)

		if _, taken := result[name]; taken {
			continue // first (oldest) wins; see the ordering note above
		}

		tableName := chModelTableName(id)
		distributed := m.ch.Topology().DistributedTables
		if distributed {
			tableName = chModelDistName(id)
		}

		result[name] = ModelInfo{
			ID:          id,
			TableName:   tableName,
			ModelType:   ModelType(modelType),
			MinSample:   def.MinSample,
			TimeBucket:  def.TimeBucket,
			FractalID:   ownerFractalID,
			Distributed: distributed,
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
	// Compile the source here so a query the translator refuses is a rejected
	// request, not a stored model that fails later in the background.
	if _, err := m.ResolveSource(ctx, req.Definition, fractalID, ""); err != nil {
		return nil, err
	}
	// A type that raises no alerts is stored as such rather than rejected. The
	// editor sends its default mode ("paused") for every type, so rejecting here
	// made an index model impossible to create at all.
	if !req.ModelType.SupportsAlert() {
		req.AlertMode = "none"
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
		string(defJSON), req.AlertMode, storage.NullableUser(createdBy)).Scan(&id)
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
	if m.alerts != nil && req.ModelType.SupportsAlert() {
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
		if err := m.createCHObjects(bgCtx, id, fractalID, req.Definition, req.ModelType, tableName, mvName); err != nil {
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

	// The definition is re-validated here, not only on create: an update writes it
	// to Postgres and rebuilds the model's ClickHouse objects from it, so a create
	// with a benign definition followed by an edit reaches the same DDL. The model
	// type cannot change, so the existing one is what the shape is checked against.
	if err := validateDefinitionShape(existing.ModelType, req.Definition); err != nil {
		return nil, err
	}
	if _, err := m.ResolveSource(ctx, req.Definition, existing.FractalID, existing.PrismID); err != nil {
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
		if err := m.createCHObjects(ddlCtx, id, existing.FractalID, req.Definition, existing.ModelType, existing.CHTableName, existing.CHMVName); err != nil {
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
	return a.SourceBQL != b.SourceBQL ||
		!reflect.DeepEqual(a.Filter, b.Filter) ||
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
	if m.alerts == nil || !model.ModelType.SupportsAlert() {
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
	if !model.ModelType.SupportsAlert() {
		return fmt.Errorf("%s models do not raise alerts; they index data for query-time use", model.ModelType)
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

// DictionaryResolver is the slice of the dictionary manager a model source needs:
// the lists visible in its scope, so match() in a source query resolves the same
// way it does in a search.
type DictionaryResolver interface {
	ListDictionaryMappings(ctx context.Context, fractalID, prismID string) (dictionaries.Scope, error)
}

// SetDictionaryResolver wires dictionary resolution for model source queries.
func (m *Manager) SetDictionaryResolver(d DictionaryResolver) { m.dicts = d }

// sourceQueryOptions is the context a model's source query is compiled in: the
// dictionaries of the model's own scope, and nothing else. The scan's fractal and
// window are the model's, applied by the builder rather than by the translator.
func (m *Manager) sourceQueryOptions(ctx context.Context, fractalID, prismID string) parser.QueryOptions {
	opts := parser.QueryOptions{DictionaryDatabase: m.chDB}
	if m.dicts == nil || (fractalID == "" && prismID == "") {
		return opts
	}
	scope, err := m.dicts.ListDictionaryMappings(ctx, fractalID, prismID)
	if err != nil {
		log.Printf("models: resolve dictionaries for source query: %v", err)
		return opts
	}
	opts.Dictionaries = scope.Mappings
	opts.CaseInsensitiveDicts = scope.CaseInsensitive
	opts.NetworkDicts = scope.Network
	opts.PatternDicts = scope.Pattern
	return opts
}

// ResolveSource compiles a definition's source query against the dictionaries of
// its scope, returning a copy carrying the predicates. Every path that renders
// SQL for a model calls this first; a builder handed an unresolved SourceBQL
// errors rather than quietly rendering the structured Filter instead.
func (m *Manager) ResolveSource(ctx context.Context, def ModelDefinition, fractalID, prismID string) (ModelDefinition, error) {
	if strings.TrimSpace(def.SourceBQL) == "" {
		return def, nil
	}
	src, err := compileSourcePredicates(def.SourceBQL, m.sourceQueryOptions(ctx, fractalID, prismID))
	if err != nil {
		return def, err
	}
	def.compiled, def.compiledSet = src.preds, true
	def.projections = modelProjections(src.projections, def.Extractions)
	return def, nil
}

// ---- ClickHouse object lifecycle ----

func isCHDDLTimeout(err error) bool { return storage.IsDDLTimeout(err) }

// dropStateMV removes a model's insert-time view. Safe to call when there is
// none: state maintenance moved to a scheduled reader, and a view left behind
// would write the same rows the reader writes.
func (m *Manager) dropStateMV(ctx context.Context, mvName string) error {
	if mvName == "" {
		return nil
	}
	sql := m.ch.InjectOnCluster(fmt.Sprintf("DROP VIEW IF EXISTS `%s`", mvName))
	if err := m.ch.ExecSchema(ctx, sql); err != nil && !isCHDDLTimeout(err) {
		return fmt.Errorf("drop model mv: %w", err)
	}
	return nil
}

func (m *Manager) createCHObjects(ctx context.Context, id, fractalID string, def ModelDefinition, mt ModelType, tableName, mvName string) error {
	def, err := m.ResolveSource(ctx, def, fractalID, "")
	if err != nil {
		return err
	}
	if mt.IsScheduled() {
		return m.createNetworkCHObjects(ctx, id, fractalID, def, mt, tableName, mvName)
	}
	tableSQL, err := GenerateDDL(def, mt, "`"+tableName+"`")
	if err != nil {
		return err
	}

	tableSQL = m.ch.RewriteEngine(tableSQL)
	tableSQL = m.ch.InjectOnCluster(tableSQL)

	if err := m.ch.ExecSchema(ctx, tableSQL); err != nil && !isCHDDLTimeout(err) {
		return fmt.Errorf("create model table: %w", err)
	}

	// No materialized view: state is maintained by StateMaintainer over
	// logs.ingest_timestamp. Dropping any view left by an older release is what
	// keeps the two from both writing and doubling every aggregate.
	if _, err := m.pg.Exec(ctx,
		`UPDATE analytics_models SET state_watermark = COALESCE(state_watermark, NOW()) WHERE id = $1`, id); err != nil {
		return fmt.Errorf("seed state watermark: %w", err)
	}
	if err := m.dropStateMV(ctx, mvName); err != nil {
		_ = m.ch.ExecSchema(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)))
		return err
	}

	// In cluster mode, create a Distributed table for fan-out reads.
	if m.ch.Topology().DistributedTables {
		distName := chModelDistName(id)
		distSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` AS `%s` ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			distName, tableName, storage.EscCHStr(m.ch.Topology().DDLCluster), tableName,
		)
		if err := m.ch.ExecSchema(ctx, distSQL); err != nil {
			log.Printf("model %s: create distributed table: %v", id, err)
		}
	}

	return nil
}

// createNetworkCHObjects creates the three ClickHouse objects a scheduled (network)
// model owns: the rolling-state table (MV target), the results table (scorer output,
// read by model_lookup / the data viewer), and the MV that maintains state at ingest.
// Backfill is N/A: the MV + TTL self-seed the rolling window.
func (m *Manager) createNetworkCHObjects(ctx context.Context, id, fractalID string, def ModelDefinition, mt ModelType, tableName, mvName string) error {
	stateName := chModelStateName(id)
	windowDays := def.WindowDays()

	stateSQL := m.ch.InjectOnCluster(m.ch.RewriteEngine(BuildNetStateTableDDL("`"+stateName+"`", windowDays)))
	if err := m.ch.ExecSchema(ctx, stateSQL); err != nil && !isCHDDLTimeout(err) {
		return fmt.Errorf("create state table: %w", err)
	}

	resultsSQL := m.ch.InjectOnCluster(m.ch.RewriteEngine(BuildNetResultsTableDDL("`" + tableName + "`")))
	if err := m.ch.ExecSchema(ctx, resultsSQL); err != nil && !isCHDDLTimeout(err) {
		_ = m.ch.ExecSchema(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", stateName)))
		return fmt.Errorf("create results table: %w", err)
	}

	// No materialized view here either: rolling state is maintained by
	// StateMaintainer, which keeps the dictionary lookups a source may carry out
	// of the ingest path.
	if _, err := m.pg.Exec(ctx,
		`UPDATE analytics_models SET state_watermark = COALESCE(state_watermark, NOW()) WHERE id = $1`, id); err != nil {
		return fmt.Errorf("seed state watermark: %w", err)
	}
	if err := m.dropStateMV(ctx, mvName); err != nil {
		_ = m.ch.ExecSchema(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", stateName)))
		_ = m.ch.ExecSchema(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)))
		return err
	}

	// Distributed tables (cluster mode): one over the results table for fan-out
	// reads, one over the state table so the scorer aggregates state across shards.
	if m.ch.Topology().DistributedTables {
		cl := storage.EscCHStr(m.ch.Topology().DDLCluster)
		distResults := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` AS `%s` ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			chModelDistName(id), tableName, cl, tableName,
		)
		if err := m.ch.ExecSchema(ctx, distResults); err != nil {
			log.Printf("model %s: create distributed results table: %v", id, err)
		}
		distState := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` AS `%s` ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			chModelStateDistName(id), stateName, cl, stateName,
		)
		if err := m.ch.ExecSchema(ctx, distState); err != nil {
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
	if err := m.ch.ExecSchema(ctx, mvDrop); err != nil {
		log.Printf("drop MV %s: %v", mvName, err)
	}
	// A scheduled model also owns a rolling-state table (the MV's target); drop it.
	if mt.IsScheduled() {
		stateDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", chModelStateName(id)))
		if err := m.ch.ExecSchema(ctx, stateDrop); err != nil {
			log.Printf("drop state table %s: %v", chModelStateName(id), err)
		}
	}
	tableDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	if err := m.ch.ExecSchema(ctx, tableDrop); err != nil {
		log.Printf("drop table %s: %v", tableName, err)
	}
	// Drop the distributed table(s) (cluster mode) so no dangling fan-out object remains.
	if m.ch.Topology().DistributedTables {
		distDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", chModelDistName(id)))
		if err := m.ch.ExecSchema(ctx, distDrop); err != nil {
			log.Printf("drop distributed table %s: %v", chModelDistName(id), err)
		}
		if mt.IsScheduled() {
			distStateDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", chModelStateDistName(id)))
			if err := m.ch.ExecSchema(ctx, distStateDrop); err != nil {
				log.Printf("drop distributed state table %s: %v", chModelStateDistName(id), err)
			}
		}
	}
	return nil
}

// RowCount returns the approximate number of rows in a model's aggregating table.
func (m *Manager) RowCount(ctx context.Context, tableName string) (uint64, error) {
	rows, err := m.ch.QuerySchema(ctx, fmt.Sprintf("SELECT count() FROM `%s`", tableName))
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
	if m.ch.Topology().DistributedTables {
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
		return m.getFirstSeenData(ctx, tableName, fractalID, search, sortCol, sortDir, limit, offset, "entity_key")
	case ModelTypeTLSH:
		return m.getFirstSeenData(ctx, tableName, fractalID, search, sortCol, sortDir, limit, offset, "digest")
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
	rows, err := m.ch.QuerySchema(ctx, dataQuery)
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
// firstSeenAggSQL collapses the per-row aggregate state into one row per key.
// keyCol is "entity_key" for first_seen models and "digest" for tlsh models, which
// share this shape exactly.
func firstSeenAggSQL(source, fidEsc, extraWhere, keyCol string) string {
	q := fmt.Sprintf(`
SELECT %s,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    sum(event_count) AS event_count,
    arraySort(groupUniqArrayMerge(365)(days)) AS days
FROM %s
WHERE fractal_id = '%s'`, keyCol, source, fidEsc)
	if extraWhere != "" {
		q += "\nAND " + extraWhere
	}
	q += "\nGROUP BY " + keyCol
	return q
}

func (m *Manager) getFirstSeenData(ctx context.Context, tableName, fractalID, search, sortCol, sortDir string, limit, offset int, keyCol string) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{keyCol: true, "first_seen": true, "last_seen": true, "event_count": true}
	if !allowed[sortCol] {
		sortCol = "first_seen"
	}

	extra := ""
	if search != "" {
		extra = fmt.Sprintf("%s ILIKE '%%%s%%'", keyCol, storage.EscCHStr(search))
	}
	baseQuery := firstSeenAggSQL("`"+tableName+"` FINAL", storage.EscCHStr(fractalID), extra, keyCol)

	countQuery := fmt.Sprintf("SELECT count() FROM (%s)", baseQuery)
	var total uint64
	if err := m.ch.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count first_seen data: %w", err)
	}

	dataQuery := fmt.Sprintf("%s ORDER BY %s %s LIMIT %d OFFSET %d", baseQuery, sortCol, strings.ToUpper(sortDir), limit, offset)
	rows, err := m.ch.QuerySchema(ctx, dataQuery)
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
	rows, err := m.ch.QuerySchema(ctx, dataQuery)
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
	return def.Beacon.WithDefaults(int64(def.WindowDays()) * 86400).ScoreThreshold
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
	rows, err := m.ch.QuerySchema(ctx, dataQuery)
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
	rows, err := m.ch.QuerySchema(ctx, q)
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
		return m.getFirstSeenStats(ctx, qt, fid, "entity_key")
	case ModelTypeTLSH:
		return m.getFirstSeenStats(ctx, qt, fid, "digest")
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
	rows, err := m.ch.QuerySchema(ctx, summaryQ)
	if err != nil {
		return nil, fmt.Errorf("rarity stats: %w", err)
	}
	result := map[string]interface{}{}
	if len(rows) > 0 {
		result["total_rows"] = rows[0]["total_rows"]
		result["distinct_partitions"] = rows[0]["distinct_partitions"]
	}
	topQ := fmt.Sprintf(`SELECT partition_val, sum(event_count) AS cnt FROM %s FINAL WHERE fractal_id = '%s' GROUP BY partition_val ORDER BY cnt DESC LIMIT 5`, qt, fid)
	topRows, err := m.ch.QuerySchema(ctx, topQ)
	if err == nil {
		result["top_partitions"] = topRows
	}
	return result, nil
}

func (m *Manager) getFirstSeenStats(ctx context.Context, qt, fid, keyCol string) (map[string]interface{}, error) {
	q := fmt.Sprintf(`
SELECT count() AS total_entities,
       min(first_seen) AS oldest_seen,
       max(last_seen) AS newest_seen,
       countIf(first_seen >= now() - INTERVAL 1 DAY) AS new_today
FROM (
    SELECT %s, min(first_seen) AS first_seen, max(last_seen) AS last_seen
    FROM %s FINAL WHERE fractal_id = '%s'
    GROUP BY %s
)`, keyCol, qt, fid, keyCol)
	rows, err := m.ch.QuerySchema(ctx, q)
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
	rows, err := m.ch.QuerySchema(ctx, q)
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
		return m.getRarityHistogram(ctx, qt, fid, model.Definition)
	case ModelTypeFirstSeen:
		return m.getFirstSeenHistogram(ctx, qt, fid, "entity_key")
	case ModelTypeTLSH:
		// A tlsh index raises no alerts, so there is no threshold a distribution
		// could be read against. The row count in the stats strip is the fact worth
		// knowing, and an empty panel collapses.
		return nil, nil
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
	rows, err := m.ch.QuerySchema(ctx, histogramQuerySQL(innerSQL, bucketExpr))
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
func firstSeenCountInner(source, fidEsc, keyCol string) string {
	return "SELECT toUInt64(event_count) AS event_count FROM (" + firstSeenAggSQL(source, fidEsc, "", keyCol) + ") WHERE event_count >= 1"
}

// getRarityHistogram reports how many values the model's own alert thresholds
// would flag. A confidence distribution used to be charted here, which no reader
// can act on: the alert needs a low percent as well, so being right of a
// confidence marker is necessary and not sufficient, and confidence is a property
// of the partition sampled once per value, which weights it by how many distinct
// values each partition has.
func (m *Manager) getRarityHistogram(ctx context.Context, qt, fid string, def ModelDefinition) (map[string]interface{}, error) {
	preds := rarityFlagPredicates(def)
	q := fmt.Sprintf(`SELECT toUInt64(count()) AS total, toUInt64(countIf(%s)) AS flagged FROM (%s)`,
		preds.SQL(), buildRarityScoredSQL(qt+" FINAL", fid))
	rows, err := m.ch.QuerySchema(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("rarity flag counts: %w", err)
	}
	criterion := ""
	if preds.Thresholded {
		criterion = preds.Text()
	}
	out := map[string]interface{}{"metric": "rarity_flags", "criterion": criterion}
	if len(rows) > 0 {
		out["total"], out["flagged"] = rows[0]["total"], rows[0]["flagged"]
	}
	return out, nil
}

// rarityFlags is the alert's own test: the SQL that counts what it would flag, the
// sentence that explains it, and whether any threshold narrows it at all. One
// source, because the count, its caption and the preview were three renderings of
// the same rule and could disagree.
type rarityFlags struct {
	sql []string
	// words are the conditions in reading order, so a caller can join them the way
	// its own sentence needs.
	words []string
	// Thresholded is false when only the min-sample floor applies, where every
	// scored value passes and "would alert" would be true of a model raising none.
	Thresholded bool
}

// SQL is the predicate the flag count is taken with.
func (f rarityFlags) SQL() string { return strings.Join(f.sql, " AND ") }

// Text reads the rule as a sentence.
func (f rarityFlags) Text() string { return strings.Join(f.words, " and ") }

func rarityFlagPredicates(def ModelDefinition) rarityFlags {
	minSample := def.MinSample
	if minSample < 1 {
		minSample = 1
	}
	f := rarityFlags{sql: []string{fmt.Sprintf("model_count >= %d", minSample)}}
	if minSample > 1 {
		f.words = append(f.words, fmt.Sprintf("seen %d+ time%s", minSample, plural(minSample)))
	}
	if def.Alert != nil {
		if t := def.Alert.ConfidenceThreshold; t > 0 {
			f.sql = append(f.sql, fmt.Sprintf("confidence > %g", t))
			f.words = append(f.words, fmt.Sprintf("confidence > %g", t))
			f.Thresholded = true
		}
		if t := def.Alert.PercentThreshold; t > 0 {
			f.sql = append(f.sql, fmt.Sprintf("percent < %g", t))
			f.words = append(f.words, fmt.Sprintf("percent < %g", t))
			f.Thresholded = true
		}
	}
	return f
}

// firstSeenDiscoveryDays is how far back the new-entity series reaches.
const firstSeenDiscoveryDays = 30

// getFirstSeenHistogram returns how many entities were first seen on each of the
// last few days. The distribution of an entity's event count used to be charted
// here, which says nothing about the question the model answers or its alert
// fires on: whether something new turned up, and how that rate compares to usual.
func (m *Manager) getFirstSeenHistogram(ctx context.Context, qt, fid, keyCol string) (map[string]interface{}, error) {
	q := fmt.Sprintf(`SELECT toString(toDate(first_seen)) AS day, toUInt64(count()) AS cnt
FROM (%s)
WHERE first_seen >= today() - %d
GROUP BY day ORDER BY day`, firstSeenAggSQL(qt+" FINAL", fid, "", keyCol), firstSeenDiscoveryDays)
	rows, err := m.ch.QuerySchema(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("first_seen discovery series: %w", err)
	}
	series := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		series = append(series, map[string]interface{}{"label": fmt.Sprintf("%v", r["day"]), "count": r["cnt"]})
	}
	return map[string]interface{}{"metric": "new_per_day", "series": series}, nil
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
	b.WriteString("WITH\nbase AS (\n    SELECT timestamp, norm_log, log_id")
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
	results, err := m.ch.QuerySchema(ctx, sql)
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

// modelColumns is the column list every model read selects, in the order
// scanModelRow expects. One constant because it was two identical ones: a column
// added to a single copy is missed by a variadic Scan without a compile error, and
// state_watermark was added to the struct and to neither query, so the model's
// state lag read as unknown everywhere.
const modelColumns = `id, COALESCE(fractal_id::text,''), COALESCE(prism_id::text,''),
	       name, description, model_type, definition, ch_table_name, ch_mv_name,
	       status, alert_mode, COALESCE(linked_alert_id::text,''), error_message,
	       COALESCE(created_by,''), created_at, updated_at,
	       backfill_status, backfill_window, backfill_total, backfill_done,
	       backfill_started_at, backfill_error, state_watermark,
	       (SELECT al.enabled FROM alerts al WHERE al.id = analytics_models.linked_alert_id)`

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
		&mo.StateWatermark,
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
	if err := validateDefinitionFieldNames(def); err != nil {
		return err
	}
	if err := validateSourceProducedKeys(mt, def); err != nil {
		return err
	}
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
	case ModelTypeTLSH:
		// One field, because the key is a single digest. A composite key would be
		// concatenated into something no longer parseable as a TLSH digest.
		if len(def.KeyFields) != 1 || strings.TrimSpace(def.KeyFields[0]) == "" {
			return fmt.Errorf("tlsh models take exactly one key field: the log field holding the digest")
		}
		// tlsh() filters logs with `<field> IN (<indexed digests>)`, so an indexed
		// digest has to be the verbatim value stored in that field. An extraction
		// indexes something derived (and lowercases it, if asked), which exists in
		// no log field at all: the filter would then match nothing, silently.
		if len(def.Extractions) > 0 {
			return fmt.Errorf("tlsh models cannot use extractions: the indexed digest must be the value stored in the log field, so tlsh() can filter on it")
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

// validateDefinitionFieldNames rejects any field name in a definition that is not
// shaped like a log field.
//
// A definition is stored configuration, not query text, so the BQL lexer never sees
// it. Most names reach SQL through chFieldRef, which escapes; extraction fields do
// not, because an extraction output becomes a plain CTE column that later steps
// reference by name, so they are written into the statement unquoted. A name of
// "out, (SELECT 1) AS pwned" therefore became a second select expression in the
// model's materialized view, which then ran on every insert into logs.
//
// One check for every name, allowing the dots a log field carries, so a legitimate
// definition is unaffected.
func validateDefinitionFieldNames(def ModelDefinition) error {
	named := []struct {
		what  string
		value string
	}{
		{"partition_key", def.PartitionKey},
		{"value_key", def.ValueKey},
	}
	for _, kf := range def.KeyFields {
		named = append(named, struct{ what, value string }{"key_fields", kf})
	}
	for _, ext := range def.Extractions {
		named = append(named,
			struct{ what, value string }{"extraction from_field", ext.FromField},
			struct{ what, value string }{"extraction output_field", ext.OutputField})
	}
	nf := def.Network.WithDefaults()
	for what, v := range map[string]string{
		"src_field": nf.SrcField, "dst_field": nf.DstField, "port_field": nf.PortField,
		"duration_field": nf.DurationField, "bytes_field": nf.BytesField,
	} {
		named = append(named, struct{ what, value string }{what, v})
	}
	for _, f := range def.Filter {
		named = append(named, struct{ what, value string }{"filter field", f.Field})
	}

	for _, n := range named {
		if n.value == "" {
			continue // absent is a shape question, handled per model type below
		}
		if !parser.IsPlainFieldName(n.value) {
			return fmt.Errorf("%s %q is not a field name: use letters, digits, dot, dash or underscore", n.what, n.value)
		}
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

// ReconcileCHObjects recreates the ClickHouse objects of any model whose table is
// missing. Postgres is the source of truth for which models exist, but their CH
// objects are created imperatively at model create/update, so anything that drops
// them out of band leaves the model defined and permanently broken: the scorer
// fails every tick with "unknown table expression".
//
// The log-data reset is exactly that case by design (model aggregates derive from
// logs that no longer exist), so recreating here is what makes the reset recoverable
// without the operator re-saving every model. Recreated tables are empty until a
// backfill runs.
//
// Best-effort and non-fatal: a model that cannot be recreated is logged and skipped
// so one bad definition never blocks startup.
func (m *Manager) ReconcileCHObjects(ctx context.Context) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, COALESCE(fractal_id::text,''), model_type, definition, ch_table_name, ch_mv_name FROM analytics_models`)
	if err != nil {
		log.Printf("models: reconcile CH objects: %v", err)
		return
	}
	type target struct {
		id, fractalID, table, mv string
		mt                       ModelType
		def                      ModelDefinition
	}
	var targets []target
	for rows.Next() {
		var t target
		var mt, defJSON string
		if err := rows.Scan(&t.id, &t.fractalID, &mt, &defJSON, &t.table, &t.mv); err != nil {
			log.Printf("models: reconcile CH objects: scan: %v", err)
			rows.Close()
			return
		}
		if err := json.Unmarshal([]byte(defJSON), &t.def); err != nil {
			log.Printf("models: reconcile CH objects: model %s definition: %v", t.id, err)
			continue
		}
		t.mt = ModelType(mt)
		targets = append(targets, t)
	}
	rows.Close()

	recreated := 0
	for _, t := range targets {
		exists, err := m.ch.TableExists(ctx, t.table)
		if err != nil {
			log.Printf("models: reconcile CH objects: probe %s: %v", t.table, err)
			continue
		}
		if exists {
			continue
		}
		if err := m.createCHObjects(ctx, t.id, t.fractalID, t.def, t.mt, t.table, t.mv); err != nil {
			log.Printf("models: reconcile CH objects: recreate %s: %v", t.table, err)
			continue
		}
		recreated++
	}
	if recreated > 0 {
		log.Printf("[Models] Recreated ClickHouse objects for %d model(s); run a backfill to repopulate", recreated)
	}
}

// ReconcileMVFractalScope rewrites any model materialized view whose source scan is
// not scoped to its owning fractal. Such an MV aggregates every fractal's inserts
// into one fractal's table: an ingest-time cost paid by fractals that never read it,
// and cross-fractal rows held back by nothing but the read-side predicate.
//
// Only the MV (an insert trigger) is dropped and recreated, never the target table,
// so this loses no data and needs no backfill. It is fix-forward by design: rows
// already indexed from other fractals stay put, unreadable, until their parts age out.
//
// Idempotent and safe at every startup; best-effort, so one bad model never blocks boot.
// ReconcileStateViews removes the insert-time views that used to maintain model
// state. Model state is now kept by StateMaintainer over logs.ingest_timestamp; a
// view left behind by an older release would write the same rows the maintainer
// writes, doubling every aggregate in the state table.
//
// Runs at startup, before the maintainer starts, so the two can never overlap.
func (m *Manager) ReconcileStateViews(ctx context.Context) {
	// Sweep ClickHouse rather than what Postgres references: a model deleted
	// before its view was dropped leaves an orphan that still fires on every
	// insert, writing into a state table nothing reads.
	rows, err := m.ch.QuerySchema(ctx,
		"SELECT name FROM system.tables WHERE database = currentDatabase() AND engine = 'MaterializedView' AND startsWith(name, 'model_mv_')")
	if err != nil {
		log.Printf("models: reconcile state views: list: %v", err)
		return
	}
	var views []string
	for _, r := range rows {
		for _, v := range r {
			if name, ok := v.(string); ok && name != "" {
				views = append(views, name)
			}
		}
	}

	for _, mv := range views {
		if err := m.dropStateMV(ctx, mv); err != nil {
			// Left for the next startup: the maintainer must not run alongside a
			// view that survived, so this is reported rather than swallowed.
			log.Printf("models: reconcile state views: %s: %v (will retry next startup)", mv, err)
			continue
		}
		log.Printf("models: dropped insert-time state view %s", mv)
	}

	// Hand over every model the sweep freed: the views stopped writing at this
	// instant, so this is where the maintainer must start. Done here rather than
	// in a migration because the app's own upgrade path does not run them.
	if _, err := m.pg.Exec(ctx,
		`UPDATE analytics_models SET ch_mv_name = '',
		        state_watermark = COALESCE(state_watermark, NOW())
		 WHERE COALESCE(ch_mv_name, '') <> '' OR state_watermark IS NULL`); err != nil {
		log.Printf("models: reconcile state views: hand over: %v", err)
	}
}

// TLSHIndex describes one fractal's TLSH digest index: the table tlsh() probes for
// the distinct digests present in that fractal.
type TLSHIndex struct {
	ModelID   string
	Name      string
	TableName string // distributed table in cluster mode, local otherwise
	FractalID string
	KeyField  string // the log field whose digests this indexes
}

// ListTLSHIndexes returns the active tlsh model indexing keyField, per fractal.
//
// Keyed by fractal rather than by model name: tlsh() has to cover every fractal in
// scope, and a prism spanning several needs each member's own index. A caller must
// treat a missing entry as a hard failure, not a slow path. The probe is only
// allowed to return a SUPERSET of the digests a query could see; reading a partial
// set would drop real matches with no sign that anything was missed.
//
// A model whose source selects a subset of its fractal's rows is excluded: it
// cannot answer for a query whose filters differ. That test reads the source query
// as well as the structured filter, because a source can carry a filter the
// structured form has no shape for and would otherwise read as unfiltered.
func (m *Manager) ListTLSHIndexes(ctx context.Context, fractalIDs []string, keyField string) (map[string]TLSHIndex, map[string]string, error) {
	result := make(map[string]TLSHIndex)
	// Models excluded because they carry a definition filter, keyed by fractal, so
	// the caller can say why an index that visibly exists is not being used.
	filtered := make(map[string]string)
	if len(fractalIDs) == 0 || keyField == "" {
		return result, filtered, nil
	}
	rows, err := m.pg.Query(ctx,
		`SELECT id, name, definition, COALESCE(fractal_id::text,'') FROM analytics_models
		 WHERE fractal_id = ANY($1) AND status = 'active' AND model_type = $2
		 ORDER BY created_at ASC, id ASC`, pq.Array(fractalIDs), string(ModelTypeTLSH))
	if err != nil {
		return nil, nil, fmt.Errorf("list tlsh indexes: %w", err)
	}
	defer rows.Close()

	distributed := m.ch.Topology().DistributedTables
	for rows.Next() {
		var id, name, ownerFractalID string
		var defRaw []byte
		if err := rows.Scan(&id, &name, &defRaw, &ownerFractalID); err != nil {
			return nil, nil, fmt.Errorf("scan tlsh index: %w", err)
		}
		var def ModelDefinition
		if err := json.Unmarshal(defRaw, &def); err != nil {
			continue
		}
		if len(def.KeyFields) != 1 || def.KeyFields[0] != keyField {
			continue
		}
		if def.SelectsSubset() {
			// Indexes a subset of the fractal's rows, so it cannot answer for a query
			// whose filters differ. Recorded rather than dropped: "no index" is a
			// baffling error when one is sitting there in the UI.
			if _, taken := filtered[ownerFractalID]; !taken {
				filtered[ownerFractalID] = name
			}
			continue
		}
		if _, taken := result[ownerFractalID]; taken {
			continue // oldest wins, matching model_lookup's tie-break
		}

		tableName := chModelTableName(id)
		if distributed {
			tableName = chModelDistName(id)
		}
		result[ownerFractalID] = TLSHIndex{
			ModelID:   id,
			Name:      name,
			TableName: tableName,
			FractalID: ownerFractalID,
			KeyField:  keyField,
		}
	}
	return result, filtered, rows.Err()
}

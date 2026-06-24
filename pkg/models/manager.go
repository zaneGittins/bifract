package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

// ---- Queries ----

// List returns all models for a fractal (V1: fractal-scoped only).
func (m *Manager) List(ctx context.Context, fractalID string) ([]*Model, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT id, COALESCE(fractal_id::text,''), COALESCE(prism_id::text,''),
		        name, description, model_type, definition, ch_table_name, ch_mv_name,
		        status, alert_mode, COALESCE(linked_alert_id::text,''), error_message,
		        COALESCE(created_by,''), created_at, updated_at,
		        backfill_status, backfill_window, backfill_total, backfill_done,
		        backfill_started_at, backfill_error
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
		        backfill_started_at, backfill_error
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

// Update updates a model's definition and rebuilds ClickHouse objects (data resets).
func (m *Manager) Update(ctx context.Context, id string, req UpdateRequest) (*Model, error) {
	existing, err := m.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	// A rebuild drops and recreates the model's data, so any in-flight backfill
	// is now stale: cancel it before touching the table it writes to.
	m.CancelBackfill(id)

	defJSON, _ := json.Marshal(req.Definition)
	_, err = m.pg.Exec(ctx,
		`UPDATE analytics_models SET name=$1, description=$2, definition=$3, alert_mode=$4, updated_at=NOW()
		 WHERE id=$5`,
		req.Name, req.Description, string(defJSON), req.AlertMode, id)
	if err != nil {
		return nil, fmt.Errorf("update model: %w", err)
	}

	// Rebuild CH objects (drops old data, forward-only).
	// Use a background context so slow ON CLUSTER DDL does not race the HTTP deadline.
	ddlCtx, ddlCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer ddlCancel()
	_, _ = m.pg.Exec(ctx, `UPDATE analytics_models SET status='rebuilding' WHERE id=$1`, id)
	if err := m.dropCHObjects(ddlCtx, existing.CHTableName, existing.CHMVName); err != nil {
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

	return m.Get(ctx, id)
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
	// Remove from Postgres first so the UI sees it gone immediately.
	if _, err = m.pg.Exec(ctx, `DELETE FROM analytics_models WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete model: %w", err)
	}
	// Fire-and-forget CH cleanup — slow ON CLUSTER DDL must not block the caller.
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := m.dropCHObjects(bgCtx, model.CHTableName, model.CHMVName); err != nil {
			log.Printf("model %s: async drop CH objects: %v", id, err)
		}
	}()
	return nil
}

// SetAlertMode updates the alert_mode and optionally the linked_alert_id.
func (m *Manager) SetAlertMode(ctx context.Context, id, alertMode, linkedAlertID string) error {
	if linkedAlertID != "" {
		_, err := m.pg.Exec(ctx,
			`UPDATE analytics_models SET alert_mode=$1, linked_alert_id=$2, updated_at=NOW() WHERE id=$3`,
			alertMode, linkedAlertID, id)
		return err
	}
	_, err := m.pg.Exec(ctx,
		`UPDATE analytics_models SET alert_mode=$1, updated_at=NOW() WHERE id=$2`,
		alertMode, id)
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

func (m *Manager) dropCHObjects(ctx context.Context, tableName, mvName string) error {
	// Order: drop MV first, then table.
	mvDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP VIEW IF EXISTS `%s`", mvName))
	if err := m.ch.Exec(ctx, mvDrop); err != nil {
		log.Printf("drop MV %s: %v", mvName, err)
	}
	tableDrop := m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	if err := m.ch.Exec(ctx, tableDrop); err != nil {
		log.Printf("drop table %s: %v", tableName, err)
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
	default:
		return nil, 0, fmt.Errorf("unknown model type: %s", model.ModelType)
	}
}

func (m *Manager) getRarityData(ctx context.Context, tableName, fractalID, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"partition_val": true, "value_val": true, "model_count": true, "percent": true, "confidence": true}
	if !allowed[sortCol] {
		sortCol = "confidence"
	}

	baseQuery := fmt.Sprintf(`
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
        FROM %s FINAL
        WHERE fractal_id = '%s'
        GROUP BY partition_val, value_val
    )
)
WHERE event_count >= 1`, "`"+tableName+"`", storage.EscCHStr(fractalID))

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

func (m *Manager) getFirstSeenData(ctx context.Context, tableName, fractalID, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"entity_key": true, "first_seen": true, "last_seen": true, "event_count": true}
	if !allowed[sortCol] {
		sortCol = "first_seen"
	}

	baseQuery := fmt.Sprintf(`
SELECT entity_key,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    sum(event_count) AS event_count,
    arraySort(groupUniqArrayMerge(365)(days)) AS days
FROM %s FINAL
WHERE fractal_id = '%s'`, "`"+tableName+"`", storage.EscCHStr(fractalID))

	if search != "" {
		baseQuery += fmt.Sprintf("\nAND entity_key ILIKE '%%%s%%'", storage.EscCHStr(search))
	}
	baseQuery += "\nGROUP BY entity_key"

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
// convention including the mad=0 -> z=0 guard. quotedTable must already be
// backtick-quoted; fidEsc must already be CH-escaped.
func buildVolumeBaselineScoringSQL(quotedTable, fidEsc string, minBuckets int, timeBucket string) string {
	if minBuckets < 1 {
		minBuckets = 1
	}
	lower, upper := volumeScoreBounds(timeBucket)
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
            FROM %s FINAL
            WHERE fractal_id = '%s' AND bucket >= %s AND bucket < %s
            GROUP BY entity_val, bucket
        )
        GROUP BY entity_val
    )
)
WHERE n_buckets >= %d`, quotedTable, fidEsc, lower, upper, minBuckets)
}

func (m *Manager) getVolumeBaselineData(ctx context.Context, tableName, fractalID string, def ModelDefinition, search, sortCol, sortDir string, limit, offset int) ([]map[string]interface{}, uint64, error) {
	allowed := map[string]bool{"entity_val": true, "latest_count": true, "baseline_median": true, "mad": true, "z_score": true, "n_buckets": true, "latest_bucket": true}
	if !allowed[sortCol] {
		sortCol = "z_score"
	}

	baseQuery := buildVolumeBaselineScoringSQL("`"+tableName+"`", storage.EscCHStr(fractalID), volumeMinBuckets(def), def.TimeBucket)
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
	scoring := buildVolumeBaselineScoringSQL("`"+tableName+"`", fid, volumeMinBuckets(def), def.TimeBucket)
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
	default:
		return nil, fmt.Errorf("unknown model type: %s", model.ModelType)
	}
}

// runHistogram buckets the rows produced by innerSQL using bucketExpr (which must
// evaluate to a 0-based bucket index) and returns counts zero-filled to labels so
// the distribution always has a stable, complete x-axis.
func (m *Manager) runHistogram(ctx context.Context, innerSQL, bucketExpr string, labels []string) ([]histBucket, error) {
	q := fmt.Sprintf("SELECT %s AS bucket, count() AS cnt FROM (%s) GROUP BY bucket", bucketExpr, innerSQL)
	rows, err := m.ch.Query(ctx, q)
	if err != nil {
		return nil, err
	}
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
	return out, nil
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

func (m *Manager) getRarityHistogram(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	inner := fmt.Sprintf(`
SELECT round(((_total - _unique) / _total) * 0.95, 4) AS confidence
FROM (
    SELECT event_count,
        sum(event_count) OVER (PARTITION BY partition_val) AS _total,
        uniqExact(value_val) OVER (PARTITION BY partition_val) AS _unique
    FROM (
        SELECT partition_val, value_val, sum(event_count) AS event_count
        FROM %s FINAL
        WHERE fractal_id = '%s'
        GROUP BY partition_val, value_val
    )
)
WHERE event_count >= 1`, qt, fid)
	labels := []string{"0.0-0.1", "0.1-0.2", "0.2-0.3", "0.3-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0"}
	buckets, err := m.runHistogram(ctx, inner, "least(toUInt64(floor(confidence * 10)), 9)", labels)
	if err != nil {
		return nil, fmt.Errorf("rarity histogram: %w", err)
	}
	return map[string]interface{}{"metric": "confidence", "buckets": buckets}, nil
}

func (m *Manager) getFirstSeenHistogram(ctx context.Context, qt, fid string) (map[string]interface{}, error) {
	inner := fmt.Sprintf(`
SELECT toUInt64(sum(event_count)) AS event_count
FROM %s FINAL
WHERE fractal_id = '%s'
GROUP BY entity_key
HAVING event_count >= 1`, qt, fid)
	labels := []string{"1-9", "10-99", "100-999", "1K-9.9K", "10K-99K", "100K+"}
	buckets, err := m.runHistogram(ctx, inner, "least(toUInt64(floor(log10(event_count))), 5)", labels)
	if err != nil {
		return nil, fmt.Errorf("first_seen histogram: %w", err)
	}
	return map[string]interface{}{"metric": "event_count", "buckets": buckets}, nil
}

func (m *Manager) getVolumeBaselineHistogram(ctx context.Context, tableName string, def ModelDefinition, fid string) (map[string]interface{}, error) {
	inner := buildVolumeBaselineScoringSQL("`"+tableName+"`", fid, volumeMinBuckets(def), def.TimeBucket)
	labels := []string{"0-1", "1-2", "2-3", "3-4", "4-5", "5+"}
	buckets, err := m.runHistogram(ctx, inner, "least(toUInt64(floor(abs(z_score))), 5)", labels)
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
	err := row.Scan(
		&mo.ID, &mo.FractalID, &mo.PrismID,
		&mo.Name, &mo.Description, &mo.ModelType,
		&defRaw, &mo.CHTableName, &mo.CHMVName,
		&mo.Status, &mo.AlertMode, &mo.LinkedAlertID, &mo.ErrorMessage,
		&mo.CreatedBy, &mo.CreatedAt, &mo.UpdatedAt,
		&mo.BackfillStatus, &mo.BackfillWindow, &mo.BackfillTotal, &mo.BackfillDone,
		&mo.BackfillStartedAt, &mo.BackfillError,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(defRaw, &mo.Definition); err != nil {
		mo.Definition = ModelDefinition{}
	}
	return &mo, nil
}

// ---- Validation ----

func validateCreateRequest(req CreateRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if req.ModelType != ModelTypeRarity && req.ModelType != ModelTypeFirstSeen && req.ModelType != ModelTypeVolumeBaseline {
		return fmt.Errorf("invalid model type: %s", req.ModelType)
	}
	switch req.ModelType {
	case ModelTypeRarity:
		if req.Definition.PartitionKey == "" {
			return fmt.Errorf("partition_key is required for rarity models")
		}
		if req.Definition.ValueKey == "" {
			return fmt.Errorf("value_key is required for rarity models")
		}
	case ModelTypeFirstSeen:
		if len(req.Definition.KeyFields) == 0 {
			return fmt.Errorf("key_fields is required for first_seen models")
		}
	case ModelTypeVolumeBaseline:
		if len(req.Definition.KeyFields) == 0 {
			return fmt.Errorf("key_fields is required for volume_baseline models")
		}
		if req.Definition.TimeBucket != "" && req.Definition.TimeBucket != "day" && req.Definition.TimeBucket != "hour" {
			return fmt.Errorf("invalid time_bucket for volume_baseline: %s (use day or hour)", req.Definition.TimeBucket)
		}
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

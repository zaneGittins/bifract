package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"time"

	"bifract/pkg/storage"
)

// Manager handles analytics model CRUD and the ClickHouse table+MV lifecycle.
type Manager struct {
	pg   *storage.PostgresClient
	ch   *storage.ClickHouseClient
	chDB string
}

// NewManager creates a new analytics model manager.
func NewManager(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *Manager {
	return &Manager{pg: pg, ch: ch, chDB: "logs"}
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
		        COALESCE(created_by,''), created_at, updated_at
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
		        COALESCE(created_by,''), created_at, updated_at
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
			ID:        id,
			TableName: tableName,
			ModelType: ModelType(modelType),
			MinSample: def.MinSample,
			FractalID: fractalID,
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
	_, _ = m.pg.Exec(context.Background(), `UPDATE analytics_models SET status='active', error_message='' WHERE id=$1`, id)

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

func (m *Manager) createCHObjects(ctx context.Context, id string, def ModelDefinition, mt ModelType, tableName, mvName string) error {
	tableSQL, mvSQL, err := GenerateDDL(def, mt, "`"+tableName+"`", "`"+mvName+"`")
	if err != nil {
		return err
	}

	tableSQL = m.ch.RewriteEngine(tableSQL)
	tableSQL = m.ch.InjectOnCluster(tableSQL)

	if err := m.ch.Exec(ctx, tableSQL); err != nil {
		return fmt.Errorf("create model table: %w", err)
	}

	mvSQL = m.ch.InjectOnCluster(mvSQL)
	if err := m.ch.Exec(ctx, mvSQL); err != nil {
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

	switch model.ModelType {
	case ModelTypeRarity:
		return m.getRarityData(ctx, model.CHTableName, fractalID, search, sortCol, sortDir, limit, offset)
	case ModelTypeFirstSeen:
		return m.getFirstSeenData(ctx, model.CHTableName, fractalID, search, sortCol, sortDir, limit, offset)
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
    round(((_total - _unique) / _total) * 0.95, 4) AS confidence
FROM (
    SELECT partition_val, value_val, event_count,
        sum(event_count) OVER (PARTITION BY partition_val) AS _total,
        uniqExact(value_val) OVER (PARTITION BY partition_val) AS _unique
    FROM (
        SELECT partition_val, value_val, sum(event_count) AS event_count
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
    sum(event_count) AS event_count
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
	return rows, total, nil
}

// TestExtraction runs a sample extraction against logs and returns matched values.
func (m *Manager) TestExtraction(ctx context.Context, fractalID string, filter []FilterCondition, extractions []ExtractionStep) ([]map[string]interface{}, error) {
	tableName := m.ch.ReadTable()
	if len(extractions) == 0 {
		return nil, fmt.Errorf("no extractions provided")
	}

	var b strings.Builder
	b.WriteString("WITH\nbase AS (\n    SELECT timestamp, raw_log, log_id")
	for _, ext := range extractions {
		if !isExtractionOutput(ext.FromField, extractions) {
			b.WriteString(fmt.Sprintf(", %s", chFieldRef(ext.FromField)))
		}
	}
	b.WriteString(fmt.Sprintf("\n    FROM %s\n    WHERE fractal_id = '%s'", tableName, storage.EscCHStr(fractalID)))
	for _, fc := range filter {
		b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
	}
	b.WriteString("\n    LIMIT 10000\n)")

	prevCTE := "base"
	for i, ext := range extractions {
		cteName := fmt.Sprintf("e%d", i)
		fromRef := chFieldRef(ext.FromField)
		if isExtractionOutput(ext.FromField, extractions[:i]) {
			fromRef = ext.FromField
		}
		b.WriteString(fmt.Sprintf(",\n%s AS (\n    SELECT *, extract(%s, %s) AS %s\n    FROM %s\n    WHERE extract(%s, %s) != ''",
			cteName, fromRef, chStringLiteral(ext.Pattern), ext.OutputField, prevCTE, fromRef, chStringLiteral(ext.Pattern)))
		if ext.MinLength > 0 {
			b.WriteString(fmt.Sprintf("\n    AND length(extract(%s, %s)) >= %d", fromRef, chStringLiteral(ext.Pattern), ext.MinLength))
		}
		b.WriteString("\n)")
		prevCTE = cteName
	}

	// Final select: sample of matched values
	lastExt := extractions[len(extractions)-1]
	outField := lastExt.OutputField
	b.WriteString(fmt.Sprintf("\nSELECT %s, count() AS cnt FROM %s GROUP BY %s ORDER BY cnt DESC LIMIT 50",
		outField, prevCTE, outField))

	return m.ch.Query(ctx, b.String())
}

// ---- Scanning helpers ----

type modelScannable interface {
	Scan(dest ...interface{}) error
}

func scanModel(rows interface{ Scan(dest ...interface{}) error }) (*Model, error) {
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
	if req.ModelType != ModelTypeRarity && req.ModelType != ModelTypeFirstSeen {
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

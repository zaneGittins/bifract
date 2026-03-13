package query

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"math"
	"strings"
	"sync"
	"time"

	"bifract/pkg/dictionaries"
	"bifract/pkg/fractals"
	"bifract/pkg/parser"
	"bifract/pkg/prisms"
	"bifract/pkg/rbac"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

type QueryHandler struct {
	db                *storage.ClickHouseClient
	pg                *storage.PostgresClient
	maxRows           int
	fractalManager    *fractals.Manager
	dictionaryManager *dictionaries.Manager
	prismManager      *prisms.Manager
	rbacResolver      *rbac.Resolver
	auditFractalID    string
	auditOnce         sync.Once
	geoIPEnabled      bool
}

// SetGeoIPEnabled enables or disables lookupIP() GeoIP enrichment in queries.
func (h *QueryHandler) SetGeoIPEnabled(enabled bool) {
	h.geoIPEnabled = enabled
}

// queryTableName returns "logs_distributed" in cluster mode, "logs" otherwise.
func (h *QueryHandler) queryTableName() string {
	if h.db != nil && h.db.IsCluster() {
		return "logs_distributed"
	}
	return "logs"
}

// SetRBACResolver injects the RBAC resolver for access filtering.
func (h *QueryHandler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

type QueryRequest struct {
	Query     string `json:"query"`
	QueryType string `json:"query_type,omitempty"` // reserved, always treated as "bql"
	Start     string `json:"start,omitempty"`      // RFC3339 format
	End       string `json:"end,omitempty"`        // RFC3339 format
	FractalID string `json:"fractal_id,omitempty"` // Fractal ID for multi-tenant queries
}

type QueryResponse struct {
	Success      bool                     `json:"success"`
	Results      []map[string]interface{} `json:"results,omitempty"`
	Count        int                      `json:"count"`
	Query        string                   `json:"query,omitempty"`
	SQL          string                   `json:"sql,omitempty"`
	Error        string                   `json:"error,omitempty"`
	ExecutionMs  int64                    `json:"execution_ms,omitempty"`
	FieldOrder   []string                 `json:"field_order,omitempty"`
	IsAggregated bool                     `json:"is_aggregated,omitempty"`
	LimitHit     string                   `json:"limit_hit,omitempty"` // "bloom", "search", "truncated", or empty
	ChartType    string                   `json:"chart_type,omitempty"`    // "piechart", "barchart", "" for table
	ChartConfig  map[string]interface{}   `json:"chart_config,omitempty"`  // Chart-specific configuration
}

func NewQueryHandler(db *storage.ClickHouseClient, maxRows int) *QueryHandler {
	return &QueryHandler{
		db:      db,
		maxRows: maxRows,
	}
}

func NewQueryHandlerWithFractals(db *storage.ClickHouseClient, maxRows int, fractalManager *fractals.Manager) *QueryHandler {
	return &QueryHandler{
		db:             db,
		maxRows:        maxRows,
		fractalManager: fractalManager,
	}
}

func NewQueryHandlerWithDictionaries(db *storage.ClickHouseClient, maxRows int, fractalManager *fractals.Manager, dictManager *dictionaries.Manager) *QueryHandler {
	return &QueryHandler{
		db:                db,
		maxRows:           maxRows,
		fractalManager:    fractalManager,
		dictionaryManager: dictManager,
	}
}

func NewQueryHandlerFull(db *storage.ClickHouseClient, maxRows int, fractalManager *fractals.Manager, dictManager *dictionaries.Manager, prismManager *prisms.Manager) *QueryHandler {
	return &QueryHandler{
		db:                db,
		maxRows:           maxRows,
		fractalManager:    fractalManager,
		dictionaryManager: dictManager,
		prismManager:      prismManager,
	}
}

// SetPostgresClient sets the PostgreSQL client used for comment() query resolution
func (h *QueryHandler) SetPostgresClient(pg *storage.PostgresClient) {
	h.pg = pg
}

func (h *QueryHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Viewer+ required to execute queries
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		respondJSON(w, http.StatusUnauthorized, QueryResponse{
			Success: false,
			Error:   "Authentication required",
		})
		return
	}
	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleViewer) {
		respondJSON(w, http.StatusForbidden, QueryResponse{
			Success: false,
			Error:   "Insufficient permissions",
		})
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[QueryHandler] Invalid request body: %v", err)
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	log.Printf("[QueryHandler] Received request - Query: %s, FractalID: %s", req.Query, req.FractalID)

	if req.Query == "" {
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   "Query parameter is required",
		})
		return
	}

	// Input size limit: reject excessively long queries
	const maxQueryLength = 10000 // 10KB
	if len(req.Query) > maxQueryLength {
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   fmt.Sprintf("Query too long (%d chars, max %d)", len(req.Query), maxQueryLength),
		})
		return
	}

	// Enforce API key permissions
	if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
		perms, _ := r.Context().Value("api_key_permissions").(map[string]interface{})
		if canQuery, ok := perms["query"].(bool); !ok || !canQuery {
			respondJSON(w, http.StatusForbidden, QueryResponse{
				Success: false,
				Error:   "API key does not have query permission",
			})
			return
		}
	}

	// Parse time range
	startTime, endTime, err := h.parseTimeRange(req.Start, req.End)
	if err != nil {
		log.Printf("[QueryHandler] Invalid time range: %v", err)
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   "Invalid time range. Use RFC3339 format.",
		})
		return
	}

	// Get selected index for query scoping
	var selectedIndex string

	// Use index_id from request body if provided, otherwise fall back to session/default
	if req.FractalID != "" {
		// Verify the user has access to the requested fractal
		if user, ok := r.Context().Value("user").(*storage.User); ok && !user.IsAdmin && h.rbacResolver != nil {
			role := h.rbacResolver.ResolveRole(r.Context(), user, req.FractalID)
			if !rbac.HasAccess(user, role, rbac.RoleViewer) {
				respondJSON(w, http.StatusForbidden, QueryResponse{
					Success: false,
					Error:   "Insufficient permissions on target fractal",
				})
				return
			}
		}
		selectedIndex = req.FractalID
		log.Printf("[QueryHandler] Using index_id from request: %s", req.FractalID)
	} else {
		selectedIndex, err = h.getSelectedIndex(r)
		if err != nil {
			log.Printf("[QueryHandler] Failed to get selected index: %v", err)
			respondJSON(w, http.StatusInternalServerError, QueryResponse{
				Success: false,
				Error:   "Failed to determine fractal context",
				Query:   req.Query,
			})
			return
		}
		log.Printf("[QueryHandler] Using index_id from session/default: %s", selectedIndex)
	}

	var sql string
	var fieldOrder []string
	var isAggregated bool
	var chartType string
	var chartConfig map[string]interface{}
	var queryMaxRows int
	var isBloomQuery bool

	log.Printf("[QueryHandler] Processing BQL query")
	pipeline, err := parser.ParseQuery(req.Query)
	if err != nil {
		log.Printf("[QueryHandler] Failed to parse query: %v", err)
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   "Failed to parse query: check syntax and try again",
			Query:   req.Query,
		})
		return
	}

	// Determine appropriate row limit based on query type
	maxRows := h.maxRows
	isBloomQuery = isRawLogRegexQuery(pipeline) && !hasExplicitLimit(pipeline)

	// For raw log regex searches, use the same limit as other queries
	if isBloomQuery {
		maxRows = h.maxRows
		log.Printf("[QueryHandler] Raw log regex query detected, applying limit: %d rows", maxRows)
	}

	queryMaxRows = maxRows

	// Check if a prism is selected; resolve to member fractal IDs
	var prismFractalIDs []string
	selectedPrismID, _ := r.Context().Value("selected_prism").(string)
	isPrismContext := selectedPrismID != "" && req.FractalID == ""
	if isPrismContext {
		if h.prismManager != nil {
			prismFractalIDs, _ = h.prismManager.GetMemberFractalIDs(r.Context(), selectedPrismID)
		}
		// Filter prism fractal IDs to only fractals the user can access
		if h.rbacResolver != nil {
			if user, ok := r.Context().Value("user").(*storage.User); ok && !user.IsAdmin {
				accessList, err := h.rbacResolver.GetAccessibleFractals(r.Context(), user.Username)
				if err == nil {
					accessSet := make(map[string]bool, len(accessList))
					for _, a := range accessList {
						accessSet[a.FractalID] = true
					}
					var filtered []string
					for _, id := range prismFractalIDs {
						if accessSet[id] {
							filtered = append(filtered, id)
						}
					}
					prismFractalIDs = filtered
				}
			}
		}
		if len(prismFractalIDs) == 0 {
			respondJSON(w, http.StatusOK, QueryResponse{
				Success:    true,
				Results:    []map[string]interface{}{},
				Count:      0,
				Query:      req.Query,
				FieldOrder: []string{},
			})
			return
		}
	}

	// Include legacy logs with empty fractal_id when querying default fractal (directly or via prism)
	includeEmptyFractalID := false
	if h.fractalManager != nil {
		if defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context()); err == nil {
			if len(prismFractalIDs) > 0 {
				for _, id := range prismFractalIDs {
					if id == defaultFractal.ID {
						includeEmptyFractalID = true
						break
					}
				}
			} else if !isPrismContext && selectedIndex == defaultFractal.ID {
				includeEmptyFractalID = true
			}
		}
	}

	// Load dictionary mappings for match() resolution
	var dictMappings map[string]map[string]string
	if h.dictionaryManager != nil {
		selectedPrismID, _ := r.Context().Value("selected_prism").(string)
		if selectedPrismID != "" {
			if mappings, err := h.dictionaryManager.ListDictionaryMappings(r.Context(), "", selectedPrismID); err == nil {
				dictMappings = mappings
			}
		} else if selectedIndex != "" {
			if mappings, err := h.dictionaryManager.ListDictionaryMappings(r.Context(), selectedIndex, ""); err == nil {
				dictMappings = mappings
			}
		}
	}

	// Pre-process comment() function: fetch matching log_ids from PostgreSQL
	var commentLogIDs []string
	var hasCommentFilter bool
	commentTags, commentKeyword, hasComment := parser.ExtractCommentParams(pipeline)
	if hasComment && h.pg != nil {
		hasCommentFilter = true
		var err error
		if isPrismContext {
			commentLogIDs, err = h.pg.GetCommentedLogIDsFiltered(r.Context(), "", prismFractalIDs, startTime, endTime, commentTags, commentKeyword)
		} else {
			commentLogIDs, err = h.pg.GetCommentedLogIDsFiltered(r.Context(), selectedIndex, nil, startTime, endTime, commentTags, commentKeyword)
		}
		if err != nil {
			log.Printf("[QueryHandler] Failed to fetch comment log IDs: %v", err)
			respondJSON(w, http.StatusInternalServerError, QueryResponse{
				Success: false,
				Error:   "Failed to resolve comment filter",
				Query:   req.Query,
			})
			return
		}
		log.Printf("[QueryHandler] comment() pre-fetch: fractal=%q start=%s end=%s tags=%v keyword=%q -> %d log IDs",
			selectedIndex, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), commentTags, commentKeyword, len(commentLogIDs))
	}

	// Translate to SQL
	fractalIDForQuery := selectedIndex
	if isPrismContext {
		fractalIDForQuery = ""
	}
	opts := parser.QueryOptions{
		StartTime:             startTime,
		EndTime:               endTime,
		MaxRows:               maxRows,
		FractalID:             fractalIDForQuery,
		FractalIDs:            prismFractalIDs,
		IncludeEmptyFractalID: includeEmptyFractalID,
		Dictionaries:          dictMappings,
		HasCommentFilter:      hasCommentFilter,
		CommentLogIDs:         commentLogIDs,
		GeoIPEnabled:          h.geoIPEnabled,
		TableName:             h.queryTableName(),
	}
	translationResult, err := parser.TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		log.Printf("[QueryHandler] Failed to translate query: %v", err)
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   "Failed to translate query: check syntax and try again",
			Query:   req.Query,
		})
		return
	}
	sql = translationResult.SQL
	fieldOrder = translationResult.FieldOrder
	isAggregated = translationResult.IsAggregated
	chartType = translationResult.ChartType
	chartConfig = translationResult.ChartConfig

	// Add LIMIT to queries that don't already have one
	if !strings.Contains(strings.ToUpper(sql), "LIMIT") {
		if isAggregated {
			// Safety limit for aggregated queries (timechart, groupby, etc.)
			sql += " LIMIT 10000"
		} else {
			sql += fmt.Sprintf(" LIMIT %d", h.maxRows)
		}
	}

	// Execute main query with timeout
	queryStart := time.Now()

	// Apply configurable query timeout from admin settings (0 = unlimited)
	queryTimeoutSec := settings.Get().QueryTimeoutSeconds
	var queryCtx context.Context
	var cancel context.CancelFunc
	if queryTimeoutSec > 0 {
		queryCtx, cancel = context.WithTimeout(r.Context(), time.Duration(queryTimeoutSec)*time.Second)
	} else {
		queryCtx, cancel = context.WithCancel(r.Context())
	}
	defer cancel()

	results, err := h.db.Query(queryCtx, sql)
	executionTime := time.Since(queryStart).Milliseconds()

	if err != nil {
		// Check if it was a timeout
		if err == context.DeadlineExceeded || (queryCtx.Err() == context.DeadlineExceeded) {
			respondJSON(w, http.StatusRequestTimeout, QueryResponse{
				Success:     false,
				Error:       "Query timeout: Consider adding more specific filters or reducing time range for raw log searches",
				Query:       req.Query,
				ExecutionMs: executionTime,
			})
			return
		}

		log.Printf("[QueryHandler] Failed to execute query: %v", err)
		respondJSON(w, http.StatusInternalServerError, QueryResponse{
			Success:     false,
			Error:       "Query execution failed",
			Query:       req.Query,
			ExecutionMs: executionTime,
		})
		return
	}

	// Check if result set is too large for efficient JSON handling
	// Prevent massive responses that cause frontend JSON parsing errors
	if len(results) > 1000 {
		// Calculate approximate size of first few results to estimate total size
		estimatedSize := 0
		sampleSize := 10
		if len(results) < sampleSize {
			sampleSize = len(results)
		}
		for i := 0; i < sampleSize; i++ {
			// Rough estimate: convert sample results to JSON and measure
			if jsonBytes, err := json.Marshal(results[i]); err == nil {
				estimatedSize += len(jsonBytes)
			}
		}

		if sampleSize > 0 {
			// Extrapolate total size based on sample
			avgSizePerResult := estimatedSize / sampleSize
			totalEstimatedSize := avgSizePerResult * len(results)

			// If estimated response would be > 50MB, warn and truncate
			const maxResponseSize = 50 * 1024 * 1024 // 50MB
			if totalEstimatedSize > maxResponseSize {
				log.Printf("[QueryHandler] Large result set detected: %d rows, ~%dMB estimated. Truncating to 1000 rows.",
					len(results), totalEstimatedSize/(1024*1024))

				results = results[:1000] // Truncate to first 1000 results

				response := QueryResponse{
					Success:      true,
					Results:      results,
					Count:        len(results),
					Query:        req.Query,
					ExecutionMs:  executionTime,
					FieldOrder:   fieldOrder,
					IsAggregated: isAggregated,
					LimitHit:     "truncated",
					Error:        "Warning: Result set was very large and has been truncated to 1000 rows. Consider adding more specific filters or using head() to limit results.",
				}

				response.SQL = sql
				respondJSON(w, http.StatusOK, response)
				return
			}
		}
	}

	// Detect if we hit query limits
	var limitHit string
	if len(results) == queryMaxRows {
		if isBloomQuery {
			limitHit = "bloom"
		} else {
			limitHit = "search"
		}
	}

	response := QueryResponse{
		Success:      true,
		Results:      results,
		Count:        len(results),
		Query:        req.Query,
		ExecutionMs:  executionTime,
		FieldOrder:   fieldOrder,
		IsAggregated: isAggregated,
		LimitHit:     limitHit,
		ChartType:    chartType,
		ChartConfig:  chartConfig,
	}

	response.SQL = sql

	// Log query to audit fractal asynchronously
	username := ""
	if user, ok := r.Context().Value("user").(*storage.User); ok && user != nil {
		username = user.Username
	}
	go h.logQueryAudit(req.Query, "bql", selectedIndex, username, executionTime, len(results))

	respondJSON(w, http.StatusOK, response)
}

func (h *QueryHandler) parseTimeRange(start, end string) (time.Time, time.Time, error) {
	var startTime, endTime time.Time
	var err error

	// Default to last 24 hours if not specified
	if start == "" {
		startTime = time.Now().Add(-24 * time.Hour)
	} else {
		startTime, err = time.Parse(time.RFC3339, start)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start time format: %w", err)
		}
	}

	if end == "" {
		endTime = time.Now()
	} else {
		endTime, err = time.Parse(time.RFC3339, end)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end time format: %w", err)
		}
	}

	if startTime.After(endTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("start time must be before end time")
	}

	return startTime, endTime, nil
}

// isRawLogRegexQuery detects if this is a raw log regex search that could return massive results
func isRawLogRegexQuery(pipeline *parser.PipelineNode) bool {
	// Check for bare regex/string searches (which search raw_log by default)
	if len(pipeline.HavingConditions) > 0 {
		for _, having := range pipeline.HavingConditions {
			if having.Field == "raw_log" && having.IsRegex {
				return true
			}
		}
	}

	// Check for filter conditions targeting raw_log with regex
	if pipeline.Filter != nil {
		if hasRawLogRegexCondition(pipeline.Filter.Conditions) {
			return true
		}
	}

	return false
}

// hasRawLogRegexCondition recursively checks conditions (including compound nodes)
// for any raw_log regex filter.
func hasRawLogRegexCondition(conditions []parser.ConditionNode) bool {
	for _, cond := range conditions {
		if cond.IsCompound {
			if hasRawLogRegexCondition(cond.Children) {
				return true
			}
		} else if cond.Field == "raw_log" && cond.IsRegex {
			return true
		}
	}
	return false
}

// hasExplicitLimit checks if the user has explicitly set a limit with head() or tail()
func hasExplicitLimit(pipeline *parser.PipelineNode) bool {
	for _, cmd := range pipeline.Commands {
		if cmd.Name == "head" || cmd.Name == "tail" {
			return true
		}
	}
	return false
}

// getSelectedIndex retrieves the selected index for the current user session or API key
func (h *QueryHandler) getSelectedIndex(r *http.Request) (string, error) {
	// If no index manager is available, use default behavior (backwards compatibility)
	if h.fractalManager == nil {
		return "", nil
	}

	// Check authentication type
	authType := r.Context().Value("auth_type")

	if authType == "api_key" {
		// For API keys, always use the fractal associated with the key
		// The selected_fractal is set by the auth middleware for API keys
		if selectedFractal := r.Context().Value("selected_fractal"); selectedFractal != nil {
			if fractalID, ok := selectedFractal.(string); ok && fractalID != "" {
				return fractalID, nil
			}
		}
		return "", fmt.Errorf("API key fractal context missing")
	}

	if authType == "session" {
		// For session auth, get selected fractal from session context
		if selectedFractal := r.Context().Value("selected_fractal"); selectedFractal != nil {
			if fractalID, ok := selectedFractal.(string); ok && fractalID != "" {
				return fractalID, nil
			}
		}
	}

	// Fall back to default fractal if no specific selection
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", fmt.Errorf("failed to get default fractal: %w", err)
	}

	return defaultFractal.ID, nil
}

// HandleGetLogByTimestamp fetches a specific log by timestamp and optional log_id
func (h *QueryHandler) HandleGetLogByTimestamp(w http.ResponseWriter, r *http.Request) {
	log.Println("[HandleGetLogByTimestamp] Request received")

	if r.Method != http.MethodPost {
		log.Printf("[HandleGetLogByTimestamp] Wrong method: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Timestamp string `json:"timestamp"`
		LogID     string `json:"log_id,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[HandleGetLogByTimestamp] Decode error: %v", err)
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}

	log.Printf("[HandleGetLogByTimestamp] Timestamp: %s, LogID: %s", req.Timestamp, req.LogID)

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid timestamp format (use RFC3339)",
		})
		return
	}

	// Query ClickHouse for logs at this exact timestamp
	logEntry, err := h.db.GetLogByTimestamp(r.Context(), timestamp, req.LogID)
	if err != nil {
		log.Printf("[QueryHandler] Failed to fetch log: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to retrieve log",
		})
		return
	}

	if logEntry == nil {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Log not found",
		})
		return
	}

	// Verify the user has access to the log's fractal
	if logFractalID, ok := logEntry["fractal_id"].(string); ok && logFractalID != "" {
		if user, ok := r.Context().Value("user").(*storage.User); ok && !user.IsAdmin && h.rbacResolver != nil {
			role := h.rbacResolver.ResolveRole(r.Context(), user, logFractalID)
			if !rbac.HasAccess(user, role, rbac.RoleViewer) {
				respondJSON(w, http.StatusNotFound, map[string]interface{}{
					"success": false,
					"error":   "Log not found",
				})
				return
			}
		}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"log":     logEntry,
	})
}

// HandleGetRecentLogs returns a sample of recent logs (last 24h) for a fractal
// along with an hourly histogram for the timechart.
func (h *QueryHandler) HandleGetRecentLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	selectedFractal, err := h.getSelectedIndex(r)
	if err != nil {
		log.Printf("[QueryHandler] Failed to get selected fractal: %v", err)
		respondJSON(w, http.StatusInternalServerError, QueryResponse{
			Success: false,
			Error:   "Failed to determine fractal context",
		})
		return
	}

	now := time.Now().UTC()
	oneDayAgo := now.Add(-24 * time.Hour)
	timeCondition := fmt.Sprintf("timestamp >= '%s'", oneDayAgo.Format("2006-01-02 15:04:05"))

	var fractalCondition string

	// Check prism context first
	if selectedPrismID, _ := r.Context().Value("selected_prism").(string); selectedPrismID != "" && h.prismManager != nil {
		prismFractalIDs, _ := h.prismManager.GetMemberFractalIDs(r.Context(), selectedPrismID)
		if len(prismFractalIDs) == 0 {
			respondJSON(w, http.StatusOK, QueryResponse{Success: true, Results: []map[string]interface{}{}, Count: 0})
			return
		}
		quoted := make([]string, len(prismFractalIDs))
		for i, id := range prismFractalIDs {
			quoted[i] = fmt.Sprintf("'%s'", escCHStr(id))
		}
		fractalCondition = "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
		if h.fractalManager != nil {
			if defaultFractal, ferr := h.fractalManager.GetDefaultFractal(r.Context()); ferr == nil {
				for _, id := range prismFractalIDs {
					if id == defaultFractal.ID {
						quoted = append(quoted, "''")
						fractalCondition = "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
						break
					}
				}
			}
		}
	} else if selectedFractal != "" {
		if h.fractalManager != nil {
			defaultFractal, ferr := h.fractalManager.GetDefaultFractal(r.Context())
			if ferr == nil && defaultFractal.ID == selectedFractal {
				fractalCondition = fmt.Sprintf("fractal_id IN ('%s', '')", escCHStr(selectedFractal))
			} else {
				fractalCondition = fmt.Sprintf("fractal_id = '%s'", escCHStr(selectedFractal))
			}
		} else {
			fractalCondition = fmt.Sprintf("fractal_id = '%s'", escCHStr(selectedFractal))
		}
	}

	whereClause := "WHERE " + timeCondition
	if fractalCondition != "" {
		whereClause += " AND " + fractalCondition
	}

	// Run logs query and histogram query in parallel
	readTbl := h.queryTableName()
	logsSQL := fmt.Sprintf("SELECT timestamp, toString(fields) AS fields, log_id FROM %s %s ORDER BY timestamp DESC LIMIT 50", readTbl, whereClause)
	histogramSQL := fmt.Sprintf("SELECT toStartOfInterval(timestamp, INTERVAL 15 MINUTE) AS bucket, count() AS cnt FROM %s %s GROUP BY bucket ORDER BY bucket", readTbl, whereClause)

	type logsResult struct {
		rows []map[string]interface{}
		err  error
	}
	type histResult struct {
		rows []map[string]interface{}
		err  error
	}

	logsCh := make(chan logsResult, 1)
	histCh := make(chan histResult, 1)

	queryStart := time.Now()
	queryCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	go func() {
		raw, qErr := h.db.Query(queryCtx, logsSQL)
		logsCh <- logsResult{rows: raw, err: qErr}
	}()
	go func() {
		raw, qErr := h.db.Query(queryCtx, histogramSQL)
		histCh <- histResult{rows: raw, err: qErr}
	}()

	logsRes := <-logsCh
	histRes := <-histCh
	executionTime := time.Since(queryStart).Milliseconds()

	if logsRes.err != nil {
		log.Printf("[QueryHandler] Failed to fetch recent logs: %v", logsRes.err)
		respondJSON(w, http.StatusInternalServerError, QueryResponse{
			Success:     false,
			Error:       "Failed to retrieve recent logs",
			ExecutionMs: executionTime,
		})
		return
	}

	results := make([]map[string]interface{}, 0, len(logsRes.rows))
	for _, rawResult := range logsRes.rows {
		result := make(map[string]interface{})
		if timestamp, exists := rawResult["timestamp"]; exists {
			result["timestamp"] = timestamp
		}
		if fieldsValue, exists := rawResult["fields"]; exists {
			result["fields"] = fieldsValue
		}
		if logID, exists := rawResult["log_id"]; exists {
			result["log_id"] = logID
		}
		results = append(results, result)
	}

	// Build histogram: 96 quarter-hour buckets for the last day
	histogram := make([]int, 96)
	if histRes.err == nil {
		for _, row := range histRes.rows {
			bucketVal, bOk := row["bucket"]
			cntVal, cOk := row["cnt"]
			if !bOk || !cOk {
				continue
			}
			var bucketTime time.Time
			switch b := bucketVal.(type) {
			case time.Time:
				bucketTime = b
			case string:
				bucketTime, _ = time.Parse("2006-01-02 15:04:05", b)
			}
			if bucketTime.IsZero() {
				continue
			}
			idx := int(bucketTime.Sub(oneDayAgo).Minutes() / 15)
			if idx >= 0 && idx < 96 {
				switch c := cntVal.(type) {
				case uint64:
					histogram[idx] = int(c)
				case int64:
					histogram[idx] = int(c)
				case float64:
					histogram[idx] = int(c)
				case int:
					histogram[idx] = c
				}
			}
		}
	}

	log.Printf("[QueryHandler] Fetched %d recent logs for fractal %s (%dms)", len(results), selectedFractal, executionTime)

	type recentLogsResponse struct {
		Success     bool                     `json:"success"`
		Results     []map[string]interface{} `json:"results"`
		Count       int                      `json:"count"`
		Query       string                   `json:"query"`
		ExecutionMs int64                    `json:"execution_ms"`
		FieldOrder  []string                 `json:"field_order"`
		Histogram   []int                    `json:"histogram"`
		TimeStart   string                   `json:"time_start"`
		TimeEnd     string                   `json:"time_end"`
	}

	respondJSON(w, http.StatusOK, recentLogsResponse{
		Success:     true,
		Results:     results,
		Count:       len(results),
		Query:       "Recent logs (last 24h)",
		ExecutionMs: executionTime,
		FieldOrder:  []string{"timestamp", "fields", "log_id"},
		Histogram:   histogram,
		TimeStart:   oneDayAgo.Format(time.RFC3339),
		TimeEnd:     now.Format(time.RFC3339),
	})
}

// escCHStr escapes a value for use inside single-quoted ClickHouse strings.
func escCHStr(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	sanitizeFloats(data)
	json.NewEncoder(w).Encode(data)
}

// sanitizeFloats replaces NaN and Inf float values with null-safe alternatives
// so that json.Encode does not fail (Go's encoding/json cannot marshal NaN/Inf).
func sanitizeFloats(v interface{}) {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, elem := range val {
			switch f := elem.(type) {
			case float64:
				if math.IsNaN(f) || math.IsInf(f, 0) {
					val[k] = nil
				}
			case float32:
				if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
					val[k] = nil
				}
			case map[string]interface{}, []interface{}, []map[string]interface{}:
				sanitizeFloats(elem)
			}
		}
	case []map[string]interface{}:
		for _, item := range val {
			sanitizeFloats(item)
		}
	case []interface{}:
		for i, elem := range val {
			switch f := elem.(type) {
			case float64:
				if math.IsNaN(f) || math.IsInf(f, 0) {
					val[i] = nil
				}
			case float32:
				if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
					val[i] = nil
				}
			case map[string]interface{}, []interface{}, []map[string]interface{}:
				sanitizeFloats(elem)
			}
		}
	}
}

// getAuditFractalID lazily resolves and caches the audit fractal ID.
func (h *QueryHandler) getAuditFractalID(ctx context.Context) string {
	h.auditOnce.Do(func() {
		if h.fractalManager == nil {
			return
		}
		f, err := h.fractalManager.GetFractalByName(ctx, "audit")
		if err == nil {
			h.auditFractalID = f.ID
		}
	})
	return h.auditFractalID
}

// logQueryAudit writes a query audit entry to the audit fractal asynchronously.
func (h *QueryHandler) logQueryAudit(query, queryType, fractalID, username string, executionMs int64, resultCount int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	auditID := h.getAuditFractalID(ctx)
	if auditID == "" {
		return
	}

	now := time.Now()
	rawLog := fmt.Sprintf(`{"event":"query","user":%q,"query_type":%q,"fractal_id":%q,"execution_ms":%d,"result_count":%d}`,
		username, queryType, fractalID, executionMs, resultCount)
	entry := storage.LogEntry{
		Timestamp: now,
		LogID:     storage.GenerateLogID(now, query+username+strconv.FormatInt(now.UnixNano(), 10)),
		FractalID: auditID,
		RawLog:    rawLog,
		Fields: map[string]string{
			"event":        "query",
			"user":         username,
			"query":        query,
			"query_type":   queryType,
			"fractal_id":   fractalID,
			"execution_ms": strconv.FormatInt(executionMs, 10),
			"result_count": strconv.Itoa(resultCount),
		},
	}

	if err := h.db.InsertLogs(ctx, []storage.LogEntry{entry}); err != nil {
		log.Printf("[QueryHandler] Failed to write audit log: %v", err)
	}
}

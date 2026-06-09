package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
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
	Profile   bool   `json:"profile,omitempty"`    // collect per-shard profiling data via system.query_log
	Cursor    string `json:"cursor,omitempty"`     // opaque token for next-page cursor pagination
}

// ProfileShardRow holds per-node metrics fetched from system.query_log.
type ProfileShardRow struct {
	Shard         string  `json:"shard"`
	Coordinator   uint64  `json:"coordinator"`
	DurationMs    uint64  `json:"duration_ms"`
	ReadBytes     string  `json:"read_bytes"`
	ReadRows      uint64  `json:"read_rows"`
	PartsScanned  uint64  `json:"parts_scanned"`
	MarksSelected uint64  `json:"marks_selected"`
	MarksSkipped  uint64  `json:"marks_skipped"`
	RowsSurviving uint64  `json:"rows_surviving"`
	FileOpens     uint64  `json:"file_opens"`
	DiskMs        uint64  `json:"disk_ms"`
	NetWaitMs     uint64  `json:"net_wait_ms"`
	BytesFromDisk string  `json:"bytes_from_disk"`
}

// SkipIndexRow holds skip-index effectiveness per shard.
type SkipIndexRow struct {
	Shard             string  `json:"shard"`
	MarksRead         uint64  `json:"marks_read"`
	MarksSkipped      uint64  `json:"marks_skipped"`
	TotalMarks        uint64  `json:"total_marks"`
	PctMarksSurviving float64 `json:"pct_marks_surviving"`
}

// ProfileData is the profiling payload attached to a query response.
type ProfileData struct {
	QueryID   string            `json:"query_id"`
	Shards    []ProfileShardRow `json:"shards"`
	SkipIndex []SkipIndexRow    `json:"skip_index,omitempty"`
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
	Histogram    []int                    `json:"histogram,omitempty"`     // Time-bucketed counts for timeline
	TimeStart    string                   `json:"time_start,omitempty"`    // Query time range start (RFC3339)
	TimeEnd      string                   `json:"time_end,omitempty"`      // Query time range end (RFC3339)
	Profile      *ProfileData             `json:"profile,omitempty"`       // Per-shard profiling data (only when requested)
	NextCursor   string                   `json:"next_cursor,omitempty"`   // Cursor token for next page (non-aggregated only)
	HasMore      bool                     `json:"has_more,omitempty"`      // True when more rows exist beyond this page
}

// cursorPageSize is the number of raw log rows returned per cursor page.
const cursorPageSize = 200

// sqlLimitRE matches the LIMIT SQL clause (whitespace before, whitespace or digit after)
// so string literals containing "LIMIT" (e.g. WHERE msg = 'LIMIT reached') don't falsely match.
// '\bLIMIT\b' is insufficient because '\b' fires at quote-letter boundaries like 'LIMIT'.
var sqlLimitRE = regexp.MustCompile(`(?i)\sLIMIT[\s\d]`)

// histogramBucketSeconds returns the bucket interval and total bucket count
// for a histogram query, adapting granularity to the query time range.
func histogramBucketSeconds(start, end time.Time) (bucketSec int, bucketCount int) {
	dur := end.Sub(start)
	switch {
	case dur <= time.Hour:
		bucketSec = 30
	case dur <= 24*time.Hour:
		bucketSec = 900 // 15 minutes
	case dur <= 7*24*time.Hour:
		bucketSec = 3600 // 1 hour
	default:
		bucketSec = 10800 // 3 hours
	}
	bucketCount = int(dur.Seconds())/bucketSec + 1
	return
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
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleViewer) && !rbac.HasAccess(user, prismRole, rbac.RoleViewer) {
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
		// Verify user has at least viewer access on this prism
		hasAccess := false
		if user, ok := r.Context().Value("user").(*storage.User); ok {
			if user.IsAdmin {
				hasAccess = true
			} else if ctxRole := rbac.PrismRoleFromContext(r.Context()); ctxRole.Satisfies(rbac.RoleViewer) {
				// Context role is set by auth middleware for both session and API key auth
				hasAccess = true
			} else if h.rbacResolver != nil {
				if role, err := h.rbacResolver.ResolvePrismRole(r.Context(), user.Username, selectedPrismID); err == nil {
					hasAccess = role.Satisfies(rbac.RoleViewer)
				}
			}
		}
		if !hasAccess {
			respondJSON(w, http.StatusOK, QueryResponse{
				Success:    true,
				Results:    []map[string]interface{}{},
				Count:      0,
				Query:      req.Query,
				FieldOrder: []string{},
			})
			return
		}
		// Prism access grants query access to all member fractals
		if h.prismManager != nil {
			prismFractalIDs, _ = h.prismManager.GetMemberFractalIDs(r.Context(), selectedPrismID)
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
			commentLogIDs, err = h.pg.GetCommentedLogIDsFiltered(r.Context(), "", selectedPrismID, startTime, endTime, commentTags, commentKeyword)
		} else {
			commentLogIDs, err = h.pg.GetCommentedLogIDsFiltered(r.Context(), selectedIndex, "", startTime, endTime, commentTags, commentKeyword)
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

	// Track whether cursor pagination applies (non-aggregated, no explicit LIMIT keyword in query)
	appliedCursorPaging := !isAggregated && !sqlLimitRE.MatchString(sql)

	// Add LIMIT to queries that don't already have one
	if !sqlLimitRE.MatchString(sql) {
		if isAggregated {
			sql += " LIMIT 10000"
		} else {
			// Ensure stable secondary sort so cursor pages don't drift on timestamp ties
			if idx := strings.LastIndex(sql, " ORDER BY"); idx >= 0 {
				if !strings.Contains(strings.ToUpper(sql[idx:]), "LOG_ID") {
					sql += ", log_id DESC"
				}
			}
			// Inject cursor condition for page N > 1
			if req.Cursor != "" {
				if cur, cerr := decodeCursor(req.Cursor); cerr == nil {
					if idx := strings.LastIndex(sql, " ORDER BY"); idx >= 0 {
						cond := fmt.Sprintf("(toUnixTimestamp64Milli(timestamp), log_id) < (%d, '%s')", cur.TSMilli, escCHStr(cur.LID))
						// Use WHERE or AND depending on whether a WHERE clause already exists
						if strings.Contains(strings.ToUpper(sql[:idx]), " WHERE ") {
							cond = " AND " + cond
						} else {
							cond = " WHERE " + cond
						}
						sql = sql[:idx] + cond + sql[idx:]
					}
				}
			}
			sql += fmt.Sprintf(" LIMIT %d", cursorPageSize+1)
		}
	}

	// Build histogram query for non-aggregated raw log queries (skip on cursor pages — already shown)
	needsHistogram := !isAggregated && chartType == "" && req.Cursor == ""
	var histogramSQL string
	var histBucketSec, histBucketCount int
	if needsHistogram {
		histBucketSec, histBucketCount = histogramBucketSeconds(startTime, endTime)
		histogramSQL, err = parser.BuildHistogramSQL(pipeline, opts, histBucketSec)
		if err != nil {
			log.Printf("[QueryHandler] Failed to build histogram SQL: %v", err)
			needsHistogram = false
		}
	}

	// Generate a stable query_id when profiling is requested so we can
	// correlate this run with system.query_log entries afterward.
	var profileQueryID string
	if req.Profile {
		profileQueryID = fmt.Sprintf("bif-prof-%d", time.Now().UnixNano())
	}

	// Execute main query (and histogram if needed) with timeout
	queryStart := time.Now()

	queryTimeoutSec := settings.Get().QueryTimeoutSeconds
	var queryCtx context.Context
	var cancel context.CancelFunc
	if queryTimeoutSec > 0 {
		queryCtx, cancel = context.WithTimeout(r.Context(), time.Duration(queryTimeoutSec)*time.Second)
	} else {
		queryCtx, cancel = context.WithCancel(r.Context())
	}
	defer cancel()

	type mainResult struct {
		rows []map[string]interface{}
		err  error
	}
	type histResult struct {
		rows []map[string]interface{}
		err  error
	}

	// Histogram gets its own cancellable context so it can be abandoned
	// if it hasn't finished by the time the main query returns.
	histCtx, histCancel := context.WithCancel(queryCtx)
	defer histCancel()

	mainCh := make(chan mainResult, 1)
	histCh := make(chan histResult, 1)

	go func() {
		var raw []map[string]interface{}
		var qErr error
		if profileQueryID != "" {
			raw, qErr = h.db.QueryWithID(queryCtx, profileQueryID, sql)
		} else {
			raw, qErr = h.db.Query(queryCtx, sql)
		}
		mainCh <- mainResult{rows: raw, err: qErr}
	}()

	if needsHistogram {
		go func() {
			raw, qErr := h.db.Query(histCtx, histogramSQL)
			histCh <- histResult{rows: raw, err: qErr}
		}()
	}

	mainRes := <-mainCh

	var histRes histResult
	if needsHistogram {
		// Give the histogram up to 500ms after the main query returns.
		// In cluster mode the histogram fans out to all shards and can dominate
		// executionTime (max(main, hist)). If it isn't done yet, skip it rather
		// than blocking the response — the timeline chart is non-essential.
		select {
		case histRes = <-histCh:
		case <-time.After(500 * time.Millisecond):
			histCancel()
			needsHistogram = false
		}
	}
	executionTime := time.Since(queryStart).Milliseconds()

	results := mainRes.rows
	err = mainRes.err

	if err != nil {
		// Client disconnected; nothing to write back.
		if r.Context().Err() == context.Canceled {
			return
		}
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

	// Cursor pagination: trim to page size and encode next cursor when more rows exist.
	// hasMore is only set when cursor encoding succeeds — avoids showing a broken button.
	var nextCursor string
	var hasMore bool
	if appliedCursorPaging && len(results) > cursorPageSize {
		results = results[:cursorPageSize]
		if nc, encErr := encodeCursor(results[len(results)-1]); encErr == nil {
			nextCursor = nc
			hasMore = true
		}
	}

	// Process histogram into bucketed array
	var histogram []int
	if needsHistogram && histRes.err == nil {
		histogram = make([]int, histBucketCount)
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
			idx := int(bucketTime.Sub(startTime).Seconds()) / histBucketSec
			if idx >= 0 && idx < histBucketCount {
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
	} else if needsHistogram && histRes.err != nil {
		log.Printf("[QueryHandler] Histogram query failed (non-critical): %v", histRes.err)
	}

	// Collect per-shard profiling data when requested (before any early returns).
	var profileData *ProfileData
	if profileQueryID != "" && err == nil {
		profileData = h.fetchProfileData(profileQueryID)
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
					Histogram:    histogram,
					Profile:      profileData,
				}
				if histogram != nil {
					response.TimeStart = startTime.Format(time.RFC3339)
					response.TimeEnd = endTime.Format(time.RFC3339)
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
		Histogram:    histogram,
		Profile:      profileData,
		NextCursor:   nextCursor,
		HasMore:      hasMore,
	}
	if histogram != nil {
		response.TimeStart = startTime.Format(time.RFC3339)
		response.TimeEnd = endTime.Format(time.RFC3339)
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
		// For fractal API keys, use the fractal associated with the key
		if selectedFractal := r.Context().Value("selected_fractal"); selectedFractal != nil {
			if fractalID, ok := selectedFractal.(string); ok && fractalID != "" {
				return fractalID, nil
			}
		}
		// For prism API keys, return empty so the prism path in the caller handles it
		if selectedPrism, _ := r.Context().Value("selected_prism").(string); selectedPrism != "" {
			return "", nil
		}
		return "", fmt.Errorf("API key context missing")
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Timestamp string `json:"timestamp"`
		LogID     string `json:"log_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid request body",
		})
		return
	}

	timestamp, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "Invalid timestamp format (use RFC3339)",
		})
		return
	}

	// Resolve the set of fractal IDs the caller is allowed to read from based
	// on the SESSION scope - never trust the request body. Admins fall through
	// without a scope filter; all other users see a log if and only if the
	// log's fractal_id is in their accessible set. This prevents both
	// cross-fractal probing via crafted log_ids and the legacy "empty
	// fractal_id = public" bypass the old code path had.
	user, _ := r.Context().Value("user").(*storage.User)
	accessible, err := h.accessibleFractalIDs(r)
	if err != nil {
		log.Printf("[QueryHandler] Failed to resolve accessible fractals: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to determine scope",
		})
		return
	}

	// Fetch without a scope filter so we can read the log's fractal_id for
	// verification. Callers with no accessible fractals short-circuit here.
	isAdmin := user != nil && user.IsAdmin
	if !isAdmin && len(accessible) == 0 {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Log not found",
		})
		return
	}

	logEntry, err := h.db.GetLogByTimestamp(r.Context(), timestamp, req.LogID, "")
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

	if !isAdmin {
		logFractalID, _ := logEntry["fractal_id"].(string)
		// Legacy rows with empty fractal_id belong to the default fractal -
		// treat them as such for the access check rather than fail-open.
		if logFractalID == "" && h.fractalManager != nil {
			if def, err := h.fractalManager.GetDefaultFractal(r.Context()); err == nil {
				logFractalID = def.ID
			}
		}
		allowed := false
		for _, id := range accessible {
			if id == logFractalID {
				allowed = true
				break
			}
		}
		if !allowed {
			respondJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   "Log not found",
			})
			return
		}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"log":     logEntry,
	})
}

// HandleGetLogFields returns the parsed fields map for a single log by log_id.
// Used by the frontend to lazy-load field details when a log row is expanded.
func (h *QueryHandler) HandleGetLogFields(w http.ResponseWriter, r *http.Request) {
	logID := r.URL.Query().Get("log_id")
	if logID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "log_id is required",
		})
		return
	}

	user, _ := r.Context().Value("user").(*storage.User)
	accessible, err := h.accessibleFractalIDs(r)
	if err != nil {
		log.Printf("[QueryHandler] HandleGetLogFields: failed to resolve accessible fractals: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to determine scope",
		})
		return
	}

	isAdmin := user != nil && user.IsAdmin
	if !isAdmin && len(accessible) == 0 {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Log not found",
		})
		return
	}

	// Fetch without fractal filter, then verify the log's fractal_id is in the
	// accessible set. This correctly handles prism sessions (multiple fractals)
	// without trusting anything from the request body.
	results, err := h.db.GetLogFieldsByIDs(r.Context(), []string{logID}, "")
	if err != nil {
		log.Printf("[QueryHandler] HandleGetLogFields: failed to fetch fields: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to retrieve log fields",
		})
		return
	}

	if len(results) == 0 {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Log not found",
		})
		return
	}

	if !isAdmin {
		logFractalID, _ := results[0]["fractal_id"].(string)
		if logFractalID == "" && h.fractalManager != nil {
			if def, err := h.fractalManager.GetDefaultFractal(r.Context()); err == nil {
				logFractalID = def.ID
			}
		}
		allowed := false
		for _, id := range accessible {
			if id == logFractalID {
				allowed = true
				break
			}
		}
		if !allowed {
			respondJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false,
				"error":   "Log not found",
			})
			return
		}
	}

	fields, _ := results[0]["fields"].(map[string]interface{})
	if fields == nil {
		fields = map[string]interface{}{}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"fields":  fields,
	})
}

// accessibleFractalIDs returns the list of fractal IDs the current session is
// scoped to: a single ID for a fractal session, the member fractal IDs for a
// prism session, or an empty list if no scope is set. Admin bypass is handled
// by callers.
func (h *QueryHandler) accessibleFractalIDs(r *http.Request) ([]string, error) {
	if prismID, _ := r.Context().Value("selected_prism").(string); prismID != "" {
		if h.prismManager == nil {
			return nil, nil
		}
		return h.prismManager.GetMemberFractalIDs(r.Context(), prismID)
	}
	if fractalID, _ := r.Context().Value("selected_fractal").(string); fractalID != "" {
		return []string{fractalID}, nil
	}
	// No session scope: fall back to the default fractal so single-fractal
	// callers still work.
	if h.fractalManager != nil {
		def, err := h.fractalManager.GetDefaultFractal(r.Context())
		if err != nil {
			return nil, err
		}
		return []string{def.ID}, nil
	}
	return nil, nil
}

// buildFractalCondition constructs the fractal_id WHERE fragment for the given request.
// noData is true when a prism context is active but contains no fractals (caller should return empty results).
func (h *QueryHandler) buildFractalCondition(r *http.Request, selectedFractal string) (condition string, noData bool, err error) {
	if selectedPrismID, _ := r.Context().Value("selected_prism").(string); selectedPrismID != "" && h.prismManager != nil {
		prismFractalIDs, _ := h.prismManager.GetMemberFractalIDs(r.Context(), selectedPrismID)
		if len(prismFractalIDs) == 0 {
			return "", true, nil
		}
		quoted := make([]string, len(prismFractalIDs))
		for i, id := range prismFractalIDs {
			quoted[i] = fmt.Sprintf("'%s'", escCHStr(id))
		}
		condition = "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
		if h.fractalManager != nil {
			if defaultFractal, ferr := h.fractalManager.GetDefaultFractal(r.Context()); ferr == nil {
				for _, id := range prismFractalIDs {
					if id == defaultFractal.ID {
						quoted = append(quoted, "''")
						condition = "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
						break
					}
				}
			}
		}
		return condition, false, nil
	}
	if selectedFractal != "" {
		if h.fractalManager != nil {
			defaultFractal, ferr := h.fractalManager.GetDefaultFractal(r.Context())
			if ferr == nil && defaultFractal.ID == selectedFractal {
				return fmt.Sprintf("fractal_id IN ('%s', '')", escCHStr(selectedFractal)), false, nil
			}
		}
		return fmt.Sprintf("fractal_id = '%s'", escCHStr(selectedFractal)), false, nil
	}
	return "", false, nil
}

// HandleGetRecentLogs returns the 50 most recent logs in the last 24h for a fractal.
func (h *QueryHandler) HandleGetRecentLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	selectedFractal, err := h.getSelectedIndex(r)
	if err != nil {
		log.Printf("[QueryHandler] Failed to get selected fractal: %v", err)
		respondJSON(w, http.StatusInternalServerError, QueryResponse{Success: false, Error: "Failed to determine fractal context"})
		return
	}

	fractalCondition, noData, err := h.buildFractalCondition(r, selectedFractal)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, QueryResponse{Success: false, Error: "Failed to determine fractal context"})
		return
	}
	if noData {
		respondJSON(w, http.StatusOK, QueryResponse{Success: true, Results: []map[string]interface{}{}, Count: 0})
		return
	}

	now := time.Now().UTC()
	oneDayAgo := now.Add(-24 * time.Hour)
	whereClause := fmt.Sprintf("WHERE timestamp >= '%s'", oneDayAgo.Format("2006-01-02 15:04:05"))
	if fractalCondition != "" {
		whereClause += " AND " + fractalCondition
	}

	logsSQL := fmt.Sprintf("SELECT timestamp, raw_log AS fields, log_id FROM %s %s ORDER BY timestamp DESC LIMIT 50", h.queryTableName(), whereClause)

	queryStart := time.Now()
	queryCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, qErr := h.db.Query(queryCtx, logsSQL)
	executionTime := time.Since(queryStart).Milliseconds()

	if qErr != nil {
		if r.Context().Err() == context.Canceled {
			return
		}
		if qErr == context.DeadlineExceeded || queryCtx.Err() == context.DeadlineExceeded {
			respondJSON(w, http.StatusRequestTimeout, QueryResponse{
				Success:     false,
				Error:       "Recent logs query timed out. ClickHouse may be under heavy load.",
				ExecutionMs: executionTime,
			})
			return
		}
		log.Printf("[QueryHandler] Failed to fetch recent logs: %v", qErr)
		respondJSON(w, http.StatusInternalServerError, QueryResponse{
			Success:     false,
			Error:       "Failed to retrieve recent logs",
			ExecutionMs: executionTime,
		})
		return
	}

	results := make([]map[string]interface{}, 0, len(rows))
	for _, rawResult := range rows {
		result := make(map[string]interface{})
		if v, ok := rawResult["timestamp"]; ok {
			result["timestamp"] = v
		}
		if v, ok := rawResult["fields"]; ok {
			result["fields"] = v
		}
		if v, ok := rawResult["log_id"]; ok {
			result["log_id"] = v
		}
		results = append(results, result)
	}

	log.Printf("[QueryHandler] Fetched %d recent logs for fractal %s (%dms)", len(results), selectedFractal, executionTime)

	type recentLogsResponse struct {
		Success     bool                     `json:"success"`
		Results     []map[string]interface{} `json:"results"`
		Count       int                      `json:"count"`
		Query       string                   `json:"query"`
		ExecutionMs int64                    `json:"execution_ms"`
		FieldOrder  []string                 `json:"field_order"`
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
		TimeStart:   oneDayAgo.Format(time.RFC3339),
		TimeEnd:     now.Format(time.RFC3339),
	})
}

// HandleGetRecentHistogram returns the 96-bucket quarter-hour event-count histogram for the last 24h.
func (h *QueryHandler) HandleGetRecentHistogram(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	selectedFractal, err := h.getSelectedIndex(r)
	if err != nil {
		log.Printf("[QueryHandler] Failed to get selected fractal: %v", err)
		respondJSON(w, http.StatusInternalServerError, QueryResponse{Success: false, Error: "Failed to determine fractal context"})
		return
	}

	fractalCondition, noData, err := h.buildFractalCondition(r, selectedFractal)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, QueryResponse{Success: false, Error: "Failed to determine fractal context"})
		return
	}

	now := time.Now().UTC()
	oneDayAgo := now.Add(-24 * time.Hour)
	histogram := make([]int, 96)

	if !noData {
		histWhereClause := fmt.Sprintf("WHERE minute >= '%s'", oneDayAgo.Format("2006-01-02 15:04:05"))
		if fractalCondition != "" {
			histWhereClause += " AND " + fractalCondition
		}
		histogramSQL := fmt.Sprintf(
			"SELECT toStartOfInterval(minute, INTERVAL 15 MINUTE) AS bucket, sum(cnt) AS cnt FROM %s %s GROUP BY bucket ORDER BY bucket",
			h.db.HistogramReadTable(), histWhereClause,
		)

		queryCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		rows, qErr := h.db.Query(queryCtx, histogramSQL)
		if qErr == nil {
			for _, row := range rows {
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
	}

	type histogramResponse struct {
		Success   bool   `json:"success"`
		Histogram []int  `json:"histogram"`
		TimeStart string `json:"time_start"`
		TimeEnd   string `json:"time_end"`
	}

	respondJSON(w, http.StatusOK, histogramResponse{
		Success:   true,
		Histogram: histogram,
		TimeStart: oneDayAgo.Format(time.RFC3339),
		TimeEnd:   now.Format(time.RFC3339),
	})
}

// cursorToken is the decoded form of the opaque cursor string sent between client and server.
type cursorToken struct {
	TSMilli int64  `json:"ts"`  // Unix milliseconds of the last-seen row's timestamp
	LID     string `json:"lid"` // log_id of the last-seen row
}

func encodeCursor(row map[string]interface{}) (string, error) {
	ts, ok := row["timestamp"].(time.Time)
	if !ok {
		return "", fmt.Errorf("unexpected timestamp type %T", row["timestamp"])
	}
	lid, _ := row["log_id"].(string)
	b, err := json.Marshal(cursorToken{TSMilli: ts.UnixMilli(), LID: lid})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func decodeCursor(s string) (cursorToken, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return cursorToken{}, err
	}
	var tok cursorToken
	return tok, json.Unmarshal(b, &tok)
}

// escCHStr escapes a value for use inside single-quoted ClickHouse strings.
func escCHStr(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

// fetchProfileData polls system.query_log until the query finishes, then
// returns per-shard metrics. Returns nil if the log entry never appears.
func (h *QueryHandler) fetchProfileData(queryID string) *ProfileData {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Poll the coordinator's local query_log until the entry appears.
	found := false
	for i := 0; i < 15; i++ {
		select {
		case <-time.After(time.Duration(500+i*300) * time.Millisecond):
		case <-ctx.Done():
			return nil
		}
		rows, err := h.db.Query(ctx, fmt.Sprintf(
			`SELECT count() AS cnt FROM system.query_log WHERE query_id = '%s' AND type IN ('QueryFinish','ExceptionWhileProcessing')`,
			escCHStr(queryID)))
		if err == nil && len(rows) > 0 {
			if cnt, ok := rows[0]["cnt"].(uint64); ok && cnt > 0 {
				found = true
				break
			}
		}
	}
	if !found {
		log.Printf("[Profile] query_log entry not found for %s", queryID)
		return nil
	}

	profile := &ProfileData{QueryID: queryID}

	var shardSource string
	if h.db.IsCluster() {
		shardSource = fmt.Sprintf("cluster('%s', system.query_log)", escCHStr(h.db.Cluster))
	} else {
		shardSource = "system.query_log"
	}

	shardSQL := fmt.Sprintf(`
SELECT
    hostname()                                                                  AS shard,
    toUInt64(is_initial_query)                                                  AS coordinator,
    query_duration_ms                                                           AS duration_ms,
    formatReadableSize(read_bytes)                                              AS read_bytes,
    read_rows,
    ProfileEvents['SelectedParts']                                              AS parts_scanned,
    ProfileEvents['SelectedMarks']                                              AS marks_selected,
    ProfileEvents['SelectedMarksTotal'] - ProfileEvents['SelectedMarks']        AS marks_skipped,
    ProfileEvents['SelectedRows']                                               AS rows_surviving,
    ProfileEvents['FileOpen']                                                   AS file_opens,
    toUInt64(ProfileEvents['DiskReadElapsedMicroseconds'] / 1000)               AS disk_ms,
    toUInt64(ProfileEvents['NetworkReceiveElapsedMicroseconds'] / 1000)         AS net_wait_ms,
    formatReadableSize(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'])   AS bytes_from_disk
FROM %s
WHERE initial_query_id = '%s'
  AND type = 'QueryFinish'
ORDER BY coordinator DESC, duration_ms DESC`, shardSource, escCHStr(queryID))

	shardRows, err := h.db.Query(ctx, shardSQL)
	if err != nil {
		log.Printf("[Profile] shard query failed: %v", err)
		return profile
	}
	for _, row := range shardRows {
		profile.Shards = append(profile.Shards, ProfileShardRow{
			Shard:         profStr(row["shard"]),
			Coordinator:   profU64(row["coordinator"]),
			DurationMs:    profU64(row["duration_ms"]),
			ReadBytes:     profStr(row["read_bytes"]),
			ReadRows:      profU64(row["read_rows"]),
			PartsScanned:  profU64(row["parts_scanned"]),
			MarksSelected: profU64(row["marks_selected"]),
			MarksSkipped:  profU64(row["marks_skipped"]),
			RowsSurviving: profU64(row["rows_surviving"]),
			FileOpens:     profU64(row["file_opens"]),
			DiskMs:        profU64(row["disk_ms"]),
			NetWaitMs:     profU64(row["net_wait_ms"]),
			BytesFromDisk: profStr(row["bytes_from_disk"]),
		})
	}

	// Skip index effectiveness — only meaningful in cluster mode (multiple shards).
	if h.db.IsCluster() {
		skipSQL := fmt.Sprintf(`
SELECT
    hostname()                                                       AS shard,
    ProfileEvents['SelectedMarks']                                              AS marks_read,
    ProfileEvents['SelectedMarksTotal'] - ProfileEvents['SelectedMarks']        AS marks_skipped,
    ProfileEvents['SelectedMarksTotal']                                         AS total_marks,
    if(ProfileEvents['SelectedMarksTotal'] > 0,
       round(100.0 * ProfileEvents['SelectedMarks'] /
             ProfileEvents['SelectedMarksTotal'], 1),
       toFloat64(0))                                                            AS pct_marks_surviving
FROM cluster('%s', system.query_log)
WHERE initial_query_id = '%s'
  AND is_initial_query = 0
  AND type = 'QueryFinish'
ORDER BY shard`, escCHStr(h.db.Cluster), escCHStr(queryID))

		skipRows, err := h.db.Query(ctx, skipSQL)
		if err != nil {
			log.Printf("[Profile] skip index query failed: %v", err)
		} else {
			for _, row := range skipRows {
				profile.SkipIndex = append(profile.SkipIndex, SkipIndexRow{
					Shard:             profStr(row["shard"]),
					MarksRead:         profU64(row["marks_read"]),
					MarksSkipped:      profU64(row["marks_skipped"]),
					TotalMarks:        profU64(row["total_marks"]),
					PctMarksSurviving: profF64(row["pct_marks_surviving"]),
				})
			}
		}
	}

	return profile
}

func profStr(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func profU64(v interface{}) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case uint8:
		return uint64(x)
	case int64:
		if x > 0 {
			return uint64(x)
		}
	case float64:
		if x > 0 {
			return uint64(x)
		}
	case string:
		var n uint64
		fmt.Sscanf(x, "%d", &n)
		return n
	}
	return 0
}

func profF64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case uint64:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
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

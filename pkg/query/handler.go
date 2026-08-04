package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"bifract/pkg/bqlvars"
	"bifract/pkg/dictionaries"
	"bifract/pkg/fractals"
	"bifract/pkg/models"
	"bifract/pkg/parser"
	"bifract/pkg/prisms"
	"bifract/pkg/rbac"
	"bifract/pkg/settings"
	"bifract/pkg/storage"

	"github.com/google/uuid"
)

type QueryHandler struct {
	db                *storage.ClickHouseClient
	pg                *storage.PostgresClient
	maxRows           int
	fractalManager    *fractals.Manager
	dictionaryManager *dictionaries.Manager
	modelManager      *models.Manager
	prismManager      *prisms.Manager
	rbacResolver      *rbac.Resolver
	auditFractalID    string
	auditOnce         sync.Once
	geoIPEnabled      bool
}

// newSearchID returns the ClickHouse query_id prefix for one search request.
// Every query the request issues (result windows, histogram chunks) carries an id
// built from this prefix, so an abandoned search can be killed server-side.
func newSearchID() string {
	return "bif-search-" + uuid.NewString()
}

// searchContext builds the context a search's ClickHouse queries run on: tagged as
// an interactive user search (CPU/memory limits) and carrying the query timeout
// both as a Go deadline and as a ClickHouse-enforced ceiling. A Go deadline alone
// only stops this process reading; the server keeps scanning what nobody is
// listening to.
func searchContext(parent context.Context, timeoutSec int) (context.Context, context.CancelFunc) {
	ctx := storage.UserSearchContext(parent)
	if timeoutSec <= 0 {
		return context.WithCancel(ctx)
	}
	ctx = storage.QueryBudgetContext(ctx, timeoutSec)
	return context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
}

// abandonSearch kills every ClickHouse query the search issued. Without it an
// abandoned scan (client gone, request timed out, histogram dropped once the
// results were in) keeps running unowned, which is how the admin activity view
// fills with queries nobody is waiting for. Detached: nothing in the response
// depends on it, and on a cluster the kill travels through the DDL queue.
func (h *QueryHandler) abandonSearch(searchID string) {
	if h.db == nil || searchID == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := h.db.KillQueryPrefix(ctx, searchID); err != nil {
			log.Printf("[QueryHandler] Failed to kill abandoned search %s: %v", searchID, err)
		}
	}()
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

// procLineageTableName returns the process-lineage read table for ptg() traversal,
// mirroring queryTableName's cluster-vs-single-node selection.
func (h *QueryHandler) procLineageTableName() string {
	if h.db != nil {
		return h.db.ProcLineageReadTable()
	}
	return "proc_lineage"
}

// procFreqTableName returns the frequency-baseline read table for pgr() scoring.
func (h *QueryHandler) procFreqTableName() string {
	if h.db != nil {
		return h.db.ProcFreqReadTable()
	}
	return "proc_freq"
}

// procEdgesTableName returns the edge-rollup read table for pgr()'s file/net/dns leaf edges.
func (h *QueryHandler) procEdgesTableName() string {
	if h.db != nil {
		return h.db.ProcEdgesReadTable()
	}
	return "process_edges"
}

// SetRBACResolver injects the RBAC resolver for access filtering.
func (h *QueryHandler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

type QueryRequest struct {
	Query      string   `json:"query"`
	QueryType  string   `json:"query_type,omitempty"`  // reserved, always treated as "bql"
	Start      string   `json:"start,omitempty"`       // RFC3339 format
	End        string   `json:"end,omitempty"`         // RFC3339 format
	FractalID  string   `json:"fractal_id,omitempty"`  // Fractal ID for multi-tenant queries
	Profile    bool     `json:"profile,omitempty"`     // collect per-shard profiling data via system.query_log
	Cursor     string   `json:"cursor,omitempty"`      // opaque token for next-page cursor pagination
	Selective  bool     `json:"selective,omitempty"`   // run active-days preflight and skip empty 8h windows
	ActiveDays []string `json:"active_days,omitempty"` // pre-computed active days (YYYY-MM-DD); skips preflight when provided
	// Variables carries the @name -> value bindings for query variables, as a
	// JSON array of {"name","value"}. The server substitutes them into Query
	// before parsing (empty value -> "*"), so the raw @var form is what the
	// editor and saved artifacts keep.
	Variables json.RawMessage `json:"variables,omitempty"`
}

// ProfileShardRow holds per-node metrics fetched from system.query_log.
type ProfileShardRow struct {
	Shard         string `json:"shard"`
	Coordinator   uint64 `json:"coordinator"`
	DurationMs    uint64 `json:"duration_ms"`
	ReadBytes     string `json:"read_bytes"`
	ReadRows      uint64 `json:"read_rows"`
	PartsScanned  uint64 `json:"parts_scanned"`
	MarksSelected uint64 `json:"marks_selected"`
	MarksSkipped  uint64 `json:"marks_skipped"`
	RowsSurviving uint64 `json:"rows_surviving"`
	FileOpens     uint64 `json:"file_opens"`
	DiskMs        uint64 `json:"disk_ms"`
	NetWaitMs     uint64 `json:"net_wait_ms"`
	BytesFromDisk string `json:"bytes_from_disk"`
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
	Success   bool                     `json:"success"`
	Results   []map[string]interface{} `json:"results,omitempty"`
	Count     int                      `json:"count"`
	Query     string                   `json:"query,omitempty"`
	SQL       string                   `json:"sql,omitempty"`
	Error     string                   `json:"error,omitempty"`
	ErrorType string                   `json:"error_type,omitempty"` // "parse", "translate", "execution", "timeout" - lets the UI route display
	ErrorPos  *ErrorSpan               `json:"error_pos,omitempty"`  // rune span of the offending text, for editor underlining

	ExecutionMs  int64                  `json:"execution_ms,omitempty"`
	FieldOrder   []string               `json:"field_order,omitempty"`
	IsAggregated bool                   `json:"is_aggregated,omitempty"`
	LimitHit     string                 `json:"limit_hit,omitempty"`      // "bloom", "search", "truncated", or empty
	ChartType    string                 `json:"chart_type,omitempty"`     // "piechart", "barchart", "" for table
	ChartConfig  map[string]interface{} `json:"chart_config,omitempty"`   // Chart-specific configuration
	Histogram    []int                  `json:"histogram,omitempty"`      // Time-bucketed counts for timeline
	TimeStart    string                 `json:"time_start,omitempty"`     // Histogram range start, bucket-snapped (RFC3339)
	TimeEnd      string                 `json:"time_end,omitempty"`       // Histogram range end, bucket-snapped (RFC3339)
	BucketSec    int                    `json:"bucket_seconds,omitempty"` // Histogram bucket interval
	Profile      *ProfileData           `json:"profile,omitempty"`        // Per-shard profiling data (only when requested)
	NextCursor   string                 `json:"next_cursor,omitempty"`    // Cursor token for next page (non-aggregated only)
	HasMore      bool                   `json:"has_more,omitempty"`       // True when more rows exist beyond this page
}

// ErrorSpan is the rune (code point) offset range of an error in the original
// query string. Start is inclusive, End exclusive. Editors use it to underline
// the offending text. Offsets are code-point indices, not byte offsets, so the
// frontend must index by code point too.
type ErrorSpan struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// errorSpanFrom returns the ErrorSpan for an error that carries position info,
// or nil when it does not (e.g. translator semantic errors).
func errorSpanFrom(err error) *ErrorSpan {
	if start, end, ok := parser.ErrorPosition(err); ok {
		return &ErrorSpan{Start: start, End: end}
	}
	return nil
}

// cursorPageSize is the number of raw log rows returned per cursor page.
const cursorPageSize = 200

// sqlLimitRE matches the LIMIT SQL clause (whitespace before, whitespace or digit after)
// so string literals containing "LIMIT" (e.g. WHERE msg = 'LIMIT reached') don't falsely match.
// '\bLIMIT\b' is insufficient because '\b' fires at quote-letter boundaries like 'LIMIT'.
var sqlLimitRE = regexp.MustCompile(`(?i)\sLIMIT[\s\d]`)

const (
	// maxHistogramBuckets caps the bucket array so a malformed or absurd time
	// range widens the interval instead of allocating without bound.
	maxHistogramBuckets = 5000

	// histogramChunkDiv sets the newest histogram chunk to this fraction of the
	// range; each older chunk doubles. The recent end paints after one small read
	// while the rest arrives in a handful of large ones instead of hundreds of
	// small ones.
	histogramChunkDiv = 16

	// histogramCacheSettings caches a histogram chunk in ClickHouse. The generated
	// SQL depends only on the query's WHERE clause and its bucket-snapped range, so
	// editing downstream pipeline commands (| stats, | table) or re-running the same
	// search reuses the scan instead of repeating it. The duration floor keeps cheap
	// chunks out of the cache. Applied only to chunks behind the newest one, which is
	// where freshly ingested logs land and must never be served stale.
	histogramCacheSettings = "use_query_cache = 1, query_cache_ttl = 60, query_cache_min_query_duration = 200"
)

// histogramBucketSeconds returns the bucket interval for a histogram over the
// given range, adapting granularity to its length.
func histogramBucketSeconds(start, end time.Time) int {
	dur := end.Sub(start)
	switch {
	case dur <= time.Hour:
		return 30
	case dur <= 24*time.Hour:
		return 900 // 15 minutes
	case dur <= 7*24*time.Hour:
		return 3600 // 1 hour
	case dur <= 30*24*time.Hour:
		return 10800 // 3 hours
	case dur <= 365*24*time.Hour:
		return 86400 // 1 day
	default:
		return 604800 // 1 week
	}
}

// histogramWindow snaps [start, end] outward onto whole bucket boundaries and
// returns the bucket interval, bucket count, and snapped bounds. Snapping keeps
// every bucket a full interval wide, and it holds the generated SQL steady for a
// relative range over the life of one bucket, which is what lets the ClickHouse
// query cache serve repeat runs of the same search.
func histogramWindow(start, end time.Time) (bucketSec, bucketCount int, snapStart, snapEnd time.Time) {
	if !end.After(start) {
		return 0, 0, start, end
	}
	bucketSec = histogramBucketSeconds(start, end)
	for {
		bs := int64(bucketSec)
		lo := start.Unix() / bs * bs
		hi := (end.Unix() + bs - 1) / bs * bs
		if hi == lo {
			hi = lo + bs
		}
		bucketCount = int((hi - lo) / bs)
		if bucketCount <= maxHistogramBuckets {
			return bucketSec, bucketCount, time.Unix(lo, 0).UTC(), time.Unix(hi, 0).UTC()
		}
		bucketSec *= 2
	}
}

// histogramChunk is one bucket-aligned slice of the histogram scan. loIdx/hiIdx
// is the half-open bucket index range the chunk owns; counts landing outside it
// (a row on the shared boundary second) belong to the adjacent chunk.
type histogramChunk struct {
	start, end   time.Time
	loIdx, hiIdx int
}

// histogramChunks slices the snapped range into chunks ordered newest-first,
// each twice the span of the one before it. Chunk bounds land on bucket
// boundaries, and because logs is partitioned by (fractal_id, toDate(timestamp))
// they also land on partition boundaries, so chunking reads the same granules a
// single query would.
func histogramChunks(snapStart time.Time, bucketSec, bucketCount int) []histogramChunk {
	if bucketSec <= 0 || bucketCount <= 0 {
		return nil
	}
	step := bucketCount / histogramChunkDiv
	if step < 1 {
		step = 1
	}
	var chunks []histogramChunk
	for hi := bucketCount; hi > 0; {
		n := step
		if n > hi {
			n = hi
		}
		// Absorb a small remainder rather than trailing a tiny extra query.
		if hi-n < step/2 {
			n = hi
		}
		lo := hi - n
		chunks = append(chunks, histogramChunk{
			start: snapStart.Add(time.Duration(lo*bucketSec) * time.Second),
			end:   snapStart.Add(time.Duration(hi*bucketSec) * time.Second),
			loIdx: lo,
			hiIdx: hi,
		})
		hi = lo
		step *= 2
	}
	return chunks
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

// SetModelManager wires in the analytics model manager for model_lookup() BQL support.
func (h *QueryHandler) SetModelManager(m *models.Manager) {
	h.modelManager = m
}

// SetPostgresClient sets the PostgreSQL client used for comment() query resolution
func (h *QueryHandler) SetPostgresClient(pg *storage.PostgresClient) {
	h.pg = pg
}

// preparedQuery holds everything resolved from a search request (auth, fractal
// scoping, translated SQL, cursor/limit, histogram) that the buffered and
// streaming handlers both need. It is produced by prepareQuery.
type preparedQuery struct {
	req                 QueryRequest
	sql                 string
	fieldOrder          []string
	isAggregated        bool
	chartType           string
	chartConfig         map[string]interface{}
	streamable          bool
	appliedCursorPaging bool
	queryMaxRows        int
	isBloomQuery        bool
	startTime           time.Time
	endTime             time.Time
	selectedIndex       string
	needsHistogram      bool
	histogramSQL        string
	histBucketSec       int
	histBucketCount     int
	histBucketStart     time.Time
	histBucketEnd       time.Time
	histChunks          []histogramChunk
	histRollupTable     string // non-empty when the histogram reads the per-minute rollup
	pipeline            *parser.PipelineNode
	translationOpts     parser.QueryOptions
}

// buildHistogramSQL re-translates the histogram for one bucket-aligned slice of
// the range. Only the WHERE portion of the pipeline is reused, so the result is
// unaffected by downstream pipeline commands.
func (p *preparedQuery) buildHistogramSQL(start, end time.Time, cache bool) (string, error) {
	var sql string
	if p.histRollupTable != "" {
		sql = histogramRollupSQL(p.histRollupTable, p.translationOpts, start, end, p.histBucketSec)
	} else {
		opts := p.translationOpts
		opts.StartTime = start
		opts.EndTime = end
		var err error
		sql, err = parser.BuildHistogramSQL(p.pipeline, opts, p.histBucketSec)
		if err != nil {
			return "", err
		}
	}
	if cache {
		sql += " SETTINGS " + histogramCacheSettings
	}
	return sql, nil
}

// fractalScopeSQL renders the fractal predicate for a query's scope, matching the
// scoping the translator applies to the logs table.
func fractalScopeSQL(opts parser.QueryOptions) string {
	var cond string
	switch {
	case len(opts.FractalIDs) > 0:
		quoted := make([]string, len(opts.FractalIDs))
		for i, id := range opts.FractalIDs {
			quoted[i] = fmt.Sprintf("'%s'", escCHStr(id))
		}
		cond = fmt.Sprintf("fractal_id IN (%s)", strings.Join(quoted, ", "))
	case opts.FractalID != "":
		cond = fmt.Sprintf("fractal_id = '%s'", escCHStr(opts.FractalID))
	default:
		return ""
	}
	if opts.IncludeEmptyFractalID {
		cond = fmt.Sprintf("(%s OR fractal_id = '')", cond)
	}
	return cond
}

// histogramRollupSQL reads bucket counts from the pre-aggregated per-minute rollup
// instead of scanning logs, turning a full-range count into a few thousand rows.
// Only valid for a scope-only histogram (parser.HistogramIsScopeOnly) whose bucket
// is a whole number of minutes, since the rollup has no finer resolution and knows
// nothing about the contents of a log. The upper bound is exclusive: end is a
// bucket boundary, so a minute landing on it belongs to the next bucket.
func histogramRollupSQL(table string, opts parser.QueryOptions, start, end time.Time, bucketSec int) string {
	conds := []string{
		fmt.Sprintf("minute >= '%s'", start.Format("2006-01-02 15:04:05")),
		fmt.Sprintf("minute < '%s'", end.Format("2006-01-02 15:04:05")),
	}
	if scope := fractalScopeSQL(opts); scope != "" {
		conds = append(conds, scope)
	}
	return fmt.Sprintf(
		"SELECT toStartOfInterval(minute, INTERVAL %d SECOND) AS bucket, sum(cnt) AS cnt FROM %s WHERE %s GROUP BY bucket ORDER BY bucket",
		bucketSec, table, strings.Join(conds, " AND "),
	)
}

// buildWindowSQL re-translates the query with a narrower time window and a
// per-window row limit. Used by the windowed streaming path to issue 8-hour
// chunks newest-first instead of one large distributed scan.
func (p *preparedQuery) buildWindowSQL(windowStart, windowEnd time.Time, limit int) (string, error) {
	opts := p.translationOpts
	opts.StartTime = windowStart
	opts.EndTime = windowEnd
	opts.MaxRows = limit
	result, err := parser.TranslateToSQLWithOrder(p.pipeline, opts)
	if err != nil {
		return "", err
	}
	sql := result.SQL
	if !sqlLimitRE.MatchString(sql) {
		if idx := strings.LastIndex(sql, " ORDER BY"); idx >= 0 {
			if !strings.Contains(strings.ToUpper(sql[idx:]), "LOG_ID") {
				sql += ", log_id DESC"
			}
		}
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return sql, nil
}

// buildActiveDaysSQL returns a query that lists every calendar day containing at
// least one log row for the given fractal/prism scope. Used by the selective
// windowing path to skip 8-hour windows that have no data at all.
func buildActiveDaysSQL(opts parser.QueryOptions) string {
	var conds []string
	conds = append(conds,
		fmt.Sprintf("timestamp >= '%s'", opts.StartTime.Format("2006-01-02 15:04:05")),
		fmt.Sprintf("timestamp <= '%s'", opts.EndTime.Format("2006-01-02 15:04:05")),
	)
	if scope := fractalScopeSQL(opts); scope != "" {
		conds = append(conds, scope)
	}
	return fmt.Sprintf(
		"SELECT DISTINCT toDate(timestamp) AS day FROM %s WHERE %s ORDER BY day DESC",
		opts.EffectiveTableName(), strings.Join(conds, " AND "),
	)
}

// prepareQuery handles auth, fractal/prism resolution, BQL parsing, SQL
// translation, cursor/limit injection, and histogram SQL construction shared by
// HandleQuery and HandleQueryStream. On any failure or short-circuit it writes
// the response itself and returns nil (named return), so callers only check for
// nil. The named return lets the existing early-exit `return` statements stay
// unchanged.
func (h *QueryHandler) prepareQuery(w http.ResponseWriter, r *http.Request) (prep *preparedQuery) {
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

	// Substitute @name query variables (quote/boundary-aware, empty -> "*")
	// before parsing, so search, dashboards and notebooks share identical
	// semantics and the parser never sees @var.
	req.Query = bqlvars.Substitute(req.Query, req.Variables)

	// Re-check the size limit on the expanded query: variable values can grow it
	// well past the raw-input limit, and the parser/translator must stay bounded.
	if len(req.Query) > maxQueryLength {
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success: false,
			Error:   fmt.Sprintf("Query too long after variable expansion (%d chars, max %d)", len(req.Query), maxQueryLength),
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
		// Parse errors describe the user's own BQL grammar (unexpected token,
		// expected token, position). They contain no backend or schema detail,
		// so surfacing them verbatim is safe and actionable.
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success:   false,
			Error:     "Parse error: " + err.Error(),
			ErrorType: "parse",
			Query:     req.Query,
			ErrorPos:  errorSpanFrom(err),
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

	// Load analytics model infos for model_lookup() resolution. In prism context
	// the model lives in a member fractal (models have no prism_id), so resolve
	// across every member fractal instead of the single querying fractal.
	var modelInfos map[string]parser.AnalyticsModelInfo
	if h.modelManager != nil {
		var infos map[string]models.ModelInfo
		var err error
		if isPrismContext {
			infos, err = h.modelManager.ListModelInfosForFractals(r.Context(), prismFractalIDs)
		} else if selectedIndex != "" {
			infos, err = h.modelManager.ListModelInfos(r.Context(), selectedIndex)
		}
		if err == nil {
			modelInfos = make(map[string]parser.AnalyticsModelInfo, len(infos))
			for name, mi := range infos {
				modelInfos[name] = parser.AnalyticsModelInfo{
					ID:         mi.ID,
					TableName:  mi.TableName,
					ModelType:  string(mi.ModelType),
					MinSample:  mi.MinSample,
					TimeBucket: mi.TimeBucket,
					FractalID:  mi.FractalID,
				}
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
		Models:                modelInfos,
		HasCommentFilter:      hasCommentFilter,
		CommentLogIDs:         commentLogIDs,
		GeoIPEnabled:          h.geoIPEnabled,
		TableName:             h.queryTableName(),
		ProcLineageTable:      h.procLineageTableName(),
		ProcFreqTable:         h.procFreqTableName(),
		ProcEdgesTable:        h.procEdgesTableName(),
		IncludeShardNum:       h.db != nil && h.db.IsCluster(),
	}

	// Source commands (e.g. pgr()) generate the pipeline's source rather than filtering the
	// logs table. Resolve the source command into a SQL subquery source (opts.SourceSubquery)
	// and drop it from the pipeline; the rest of the pipeline (filters/aggregations/charts,
	// including pgraph()) then translates over that source like any other query. Everything
	// downstream keys off opts.SourceSubquery -- no per-command special-casing.
	var sourceFocus string
	var sourceQuerySettings string
	if src, ok := parser.FirstSourceCommand(pipeline); ok {
		if perr := parser.ValidateSourceCommandPlacement(pipeline); perr != nil {
			respondJSON(w, http.StatusBadRequest, QueryResponse{
				Success: false, Query: req.Query, ErrorType: "translate", Error: perr.Error(),
			})
			return
		}
		rs, rerr := h.resolveSourceCommand(r.Context(), src, opts)
		if rerr != nil {
			respondJSON(w, http.StatusBadRequest, QueryResponse{
				Success: false, Query: req.Query, ErrorType: "translate", Error: rerr.Error(),
			})
			return
		}
		opts.SourceSubquery = rs.SQL
		opts.SourceColumns = rs.Columns
		opts.SourceNumericColumns = rs.NumericColumns
		// A source resolver (e.g. pgr()) may prefer its own result-row cap and ordering over the
		// deployment's generic MaxQueryRows/timestamp-DESC default -- its subquery rows are already
		// meaningfully ordered (spawn structure before low-signal leaves), so re-sorting by
		// timestamp and truncating at a generic cap can drop structure instead of noise. An
		// explicit `| limit(N)` / `| sort(...)` downstream in the query still takes precedence.
		if rs.Limit > 0 {
			opts.MaxRows = rs.Limit
		}
		if len(rs.OrderBy) > 0 {
			opts.SourceOrderBy = rs.OrderBy
		}
		sourceFocus = rs.Focus
		sourceQuerySettings = rs.QuerySettings
		pipeline = parser.StripCommand(pipeline, src.Name)
	}

	translationResult, err := parser.TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		log.Printf("[QueryHandler] Failed to translate query: %v", err)
		// Translation errors describe BQL semantics (unsupported command,
		// invalid field name, incompatible function combinations) and only ever
		// echo the user's own identifiers. No table names or generated SQL leak.
		respondJSON(w, http.StatusBadRequest, QueryResponse{
			Success:   false,
			Error:     "Query error: " + err.Error(),
			ErrorType: "translate",
			Query:     req.Query,
			ErrorPos:  errorSpanFrom(err),
		})
		return
	}
	sql = translationResult.SQL
	fieldOrder = translationResult.FieldOrder
	isAggregated = translationResult.IsAggregated
	chartType = translationResult.ChartType
	chartConfig = translationResult.ChartConfig
	// Surface the source command's focus node (pgr start guid) so pgraph() can center/highlight it.
	if sourceFocus != "" && chartType == "pgraph" {
		if chartConfig == nil {
			chartConfig = map[string]interface{}{}
		}
		chartConfig["focus"] = sourceFocus
	}

	// A subquery source (a resolved source command like pgr()) is already scoped inside the
	// subquery and exposes a formatted-String timestamp, so cursor/timestamp paging, the
	// timestamp histogram, and progressive streaming don't apply -- the query is always fully
	// computed. This is a property of the source, not of any one command.
	hasSubquerySource := opts.SourceSubquery != ""

	// Track whether cursor pagination applies (non-aggregated, no explicit LIMIT keyword in query)
	appliedCursorPaging := !isAggregated && !sqlLimitRE.MatchString(sql) && !hasSubquerySource

	// A query can be progressively streamed newest-first only when it is a plain
	// non-aggregated, non-charted search left in the translator's default
	// timestamp-DESC order. Explicit sorts, aggregations, and charts must be
	// fully computed before any meaningful result exists.
	streamable := translationResult.DefaultTimeOrder && !isAggregated && chartType == "" && !hasSubquerySource

	// Add LIMIT to queries that don't already have one
	if !sqlLimitRE.MatchString(sql) {
		if isAggregated {
			sql += " LIMIT 10000"
		} else if hasSubquerySource {
			// No cursor/timestamp paging over a subquery source; a plain generous cap.
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

	// Build histogram query for non-aggregated raw log queries (skip on cursor pages — already
	// shown, and for composed pgr() whose source has no scannable timestamp partition).
	needsHistogram := !isAggregated && chartType == "" && req.Cursor == "" && !hasSubquerySource
	var histogramSQL string
	var histBucketSec, histBucketCount int
	var histBucketStart, histBucketEnd time.Time
	var histChunks []histogramChunk
	var histRollupTable string
	if needsHistogram {
		histBucketSec, histBucketCount, histBucketStart, histBucketEnd = histogramWindow(startTime, endTime)
		if histBucketCount <= 0 {
			needsHistogram = false
		} else {
			histChunks = histogramChunks(histBucketStart, histBucketSec, histBucketCount)
			// A histogram filtered by nothing but time and fractal is exactly what the
			// per-minute rollup already stores, so read it from there and skip the log
			// scan entirely. Buckets finer than a minute have no rollup equivalent, but
			// they only occur on ranges under an hour, which are cheap to scan anyway.
			if histBucketSec >= 60 && histBucketSec%60 == 0 && parser.HistogramIsScopeOnly(pipeline, opts) {
				histRollupTable = h.db.HistogramReadTable()
			}
			// Single-shot histogram for the buffered path. Deliberately uncached: it
			// spans the newest bucket, and it is the path the profiler uses, where a
			// cache hit would misreport the query's real cost.
			if histRollupTable != "" {
				histogramSQL = histogramRollupSQL(histRollupTable, opts, histBucketStart, histBucketEnd, histBucketSec)
			} else {
				histOpts := opts
				histOpts.StartTime = histBucketStart
				histOpts.EndTime = histBucketEnd
				histogramSQL, err = parser.BuildHistogramSQL(pipeline, histOpts, histBucketSec)
				if err != nil {
					log.Printf("[QueryHandler] Failed to build histogram SQL: %v", err)
					needsHistogram = false
				}
			}
		}
	}

	// A source command (pgr) may request a top-level scan budget; SETTINGS must be last (illegal
	// inside the subquery), so append after all LIMIT/cursor edits. Server-enforced, so a
	// pathological tree aborts here instead of scanning the whole window across every shard.
	if sourceQuerySettings != "" {
		sql += " SETTINGS " + sourceQuerySettings
	}

	prep = &preparedQuery{
		req:                 req,
		sql:                 sql,
		fieldOrder:          fieldOrder,
		isAggregated:        isAggregated,
		chartType:           chartType,
		chartConfig:         chartConfig,
		streamable:          streamable,
		appliedCursorPaging: appliedCursorPaging,
		queryMaxRows:        queryMaxRows,
		isBloomQuery:        isBloomQuery,
		startTime:           startTime,
		endTime:             endTime,
		selectedIndex:       selectedIndex,
		needsHistogram:      needsHistogram,
		histogramSQL:        histogramSQL,
		histBucketSec:       histBucketSec,
		histBucketCount:     histBucketCount,
		histBucketStart:     histBucketStart,
		histBucketEnd:       histBucketEnd,
		histChunks:          histChunks,
		histRollupTable:     histRollupTable,
		pipeline:            pipeline,
		translationOpts:     opts,
	}
	return
}

// ValidateResponse is the result of a parse+translate-only validation. It never
// touches ClickHouse, so it is cheap enough to call live as the user types.
type ValidateResponse struct {
	Valid     bool       `json:"valid"`
	Error     string     `json:"error,omitempty"`
	ErrorType string     `json:"error_type,omitempty"` // "parse" or "translate"
	ErrorPos  *ErrorSpan `json:"error_pos,omitempty"`
}

// HandleValidate parses and translates a BQL query WITHOUT executing it, so the
// editor can surface syntax/semantic errors (and their source span for
// underlining) live without hitting ClickHouse. It loads dictionaries/models
// best-effort so functions like match() and model_lookup() resolve the same way
// they would at execution time, avoiding false-positive errors.
func (h *QueryHandler) HandleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Viewer+ required, same as executing a query.
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		respondJSON(w, http.StatusUnauthorized, ValidateResponse{Valid: false, Error: "Authentication required"})
		return
	}
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleViewer) && !rbac.HasAccess(user, prismRole, rbac.RoleViewer) {
		respondJSON(w, http.StatusForbidden, ValidateResponse{Valid: false, Error: "Insufficient permissions"})
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, ValidateResponse{Valid: false, Error: "Invalid request body"})
		return
	}

	// An empty query is not an error to flag while typing.
	if strings.TrimSpace(req.Query) == "" {
		respondJSON(w, http.StatusOK, ValidateResponse{Valid: true})
		return
	}
	const maxQueryLength = 10000
	if len(req.Query) > maxQueryLength {
		respondJSON(w, http.StatusOK, ValidateResponse{
			Valid:     false,
			Error:     fmt.Sprintf("Query too long (%d chars, max %d)", len(req.Query), maxQueryLength),
			ErrorType: "parse",
		})
		return
	}

	// Validate the substituted query (what would actually run), so live editors
	// do not flag @var tokens as syntax errors.
	req.Query = bqlvars.Substitute(req.Query, req.Variables)

	// Variable values can expand the query past the raw-input limit.
	if len(req.Query) > maxQueryLength {
		respondJSON(w, http.StatusOK, ValidateResponse{
			Valid:     false,
			Error:     fmt.Sprintf("Query too long after variable expansion (%d chars, max %d)", len(req.Query), maxQueryLength),
			ErrorType: "parse",
		})
		return
	}

	pipeline, err := parser.ParseQuery(req.Query)
	if err != nil {
		respondJSON(w, http.StatusOK, ValidateResponse{
			Valid:     false,
			Error:     "Parse error: " + err.Error(),
			ErrorType: "parse",
			ErrorPos:  errorSpanFrom(err),
		})
		return
	}

	// Resolve fractal/prism context best-effort for dictionary/model loading.
	// Honor a caller-supplied fractal only when the user can access it (mirrors
	// prepareQuery); otherwise fall back to the session context. This prevents
	// probing another fractal's dictionary/model names through validation.
	selectedIndex := req.FractalID
	if selectedIndex != "" && !user.IsAdmin && h.rbacResolver != nil {
		role := h.rbacResolver.ResolveRole(r.Context(), user, selectedIndex)
		if !rbac.HasAccess(user, role, rbac.RoleViewer) {
			selectedIndex = ""
		}
	}
	if selectedIndex == "" {
		selectedIndex, _ = h.getSelectedIndex(r)
	}
	var prismFractalIDs []string
	selectedPrismID, _ := r.Context().Value("selected_prism").(string)
	isPrismContext := selectedPrismID != "" && req.FractalID == ""
	if isPrismContext && h.prismManager != nil {
		prismFractalIDs, _ = h.prismManager.GetMemberFractalIDs(r.Context(), selectedPrismID)
	}

	var dictMappings map[string]map[string]string
	if h.dictionaryManager != nil {
		if isPrismContext {
			dictMappings, _ = h.dictionaryManager.ListDictionaryMappings(r.Context(), "", selectedPrismID)
		} else if selectedIndex != "" {
			dictMappings, _ = h.dictionaryManager.ListDictionaryMappings(r.Context(), selectedIndex, "")
		}
	}

	var modelInfos map[string]parser.AnalyticsModelInfo
	if h.modelManager != nil {
		var infos map[string]models.ModelInfo
		var err error
		if isPrismContext {
			infos, err = h.modelManager.ListModelInfosForFractals(r.Context(), prismFractalIDs)
		} else if selectedIndex != "" {
			infos, err = h.modelManager.ListModelInfos(r.Context(), selectedIndex)
		}
		if err == nil {
			modelInfos = make(map[string]parser.AnalyticsModelInfo, len(infos))
			for name, mi := range infos {
				modelInfos[name] = parser.AnalyticsModelInfo{
					ID:         mi.ID,
					TableName:  mi.TableName,
					ModelType:  string(mi.ModelType),
					MinSample:  mi.MinSample,
					TimeBucket: mi.TimeBucket,
					FractalID:  mi.FractalID,
				}
			}
		}
	}

	// Time range affects only the generated WHERE clause, which we discard; fall
	// back to a nominal 1h window when the request omits or malforms it.
	startTime, endTime, err := h.parseTimeRange(req.Start, req.End)
	if err != nil {
		endTime = time.Now()
		startTime = endTime.Add(-time.Hour)
	}

	fractalIDForQuery := selectedIndex
	if isPrismContext {
		fractalIDForQuery = ""
	}
	_, _, hasComment := parser.ExtractCommentParams(pipeline)
	opts := parser.QueryOptions{
		StartTime:             startTime,
		EndTime:               endTime,
		MaxRows:               h.maxRows,
		FractalID:             fractalIDForQuery,
		FractalIDs:            prismFractalIDs,
		IncludeEmptyFractalID: false,
		Dictionaries:          dictMappings,
		Models:                modelInfos,
		HasCommentFilter:      hasComment,
		GeoIPEnabled:          h.geoIPEnabled,
		TableName:             h.queryTableName(),
		ProcLineageTable:      h.procLineageTableName(),
		ProcFreqTable:         h.procFreqTableName(),
		ProcEdgesTable:        h.procEdgesTableName(),
		IncludeShardNum:       h.db != nil && h.db.IsCluster(),
	}
	if _, err := parser.TranslateToSQLWithOrder(pipeline, opts); err != nil {
		respondJSON(w, http.StatusOK, ValidateResponse{
			Valid:     false,
			Error:     "Query error: " + err.Error(),
			ErrorType: "translate",
			ErrorPos:  errorSpanFrom(err),
		})
		return
	}

	respondJSON(w, http.StatusOK, ValidateResponse{Valid: true})
}

// HandleQuery executes a BQL search and returns the full result set in one
// buffered JSON response. Used by API consumers, alerts, and saved-query runs.
func (h *QueryHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	prep := h.prepareQuery(w, r)
	if prep == nil {
		return
	}
	req := prep.req
	sql := prep.sql
	fieldOrder := prep.fieldOrder
	isAggregated := prep.isAggregated
	chartType := prep.chartType
	chartConfig := prep.chartConfig
	appliedCursorPaging := prep.appliedCursorPaging
	queryMaxRows := prep.queryMaxRows
	isBloomQuery := prep.isBloomQuery
	selectedIndex := prep.selectedIndex
	needsHistogram := prep.needsHistogram
	histogramSQL := prep.histogramSQL
	histBucketSec := prep.histBucketSec
	histBucketCount := prep.histBucketCount
	var err error

	// Every ClickHouse query this request issues is identified by the search id, so
	// profiling can correlate it with system.query_log and anything abandoned can
	// be killed server-side.
	searchID := newSearchID()
	mainQueryID := searchID + "-main"

	// Execute main query (and histogram if needed) with timeout
	queryStart := time.Now()

	queryCtx, cancel := searchContext(r.Context(), settings.Get().QueryTimeoutSeconds)
	// A query left running server-side is the expensive kind of leak: cancel the
	// client side first, then kill anything that outlived the request.
	searchClean := true
	defer func() {
		abandoned := !searchClean || queryCtx.Err() != nil
		cancel()
		if abandoned {
			h.abandonSearch(searchID)
		}
	}()

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
		raw, qErr := h.db.QueryUserSearchWithID(queryCtx, mainQueryID, sql)
		mainCh <- mainResult{rows: raw, err: qErr}
	}()

	if needsHistogram {
		go func() {
			raw, qErr := h.db.QueryUserSearchWithID(histCtx, searchID+"-hist", histogramSQL)
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
			// Dropped, not finished: the scan is still running server-side and
			// nothing is left to consume it, so the request must kill it.
			searchClean = false
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
				ErrorType:   "timeout",
				Query:       req.Query,
				ExecutionMs: executionTime,
			})
			return
		}

		// Never surface the raw ClickHouse exception: it can contain column
		// names, table names, and sampled data values. Map known-safe error
		// codes to friendly messages; otherwise stay generic and attach only the
		// public numeric code for support correlation. The full detail is logged.
		log.Printf("[QueryHandler] Failed to execute query: %v", err)
		errMsg := "Query execution failed"
		if friendly, ok := clickhouseUserMessage(err); ok {
			errMsg = friendly
		} else if code := clickhouseErrorCode(err); code != 0 {
			errMsg = fmt.Sprintf("Query execution failed (ClickHouse error %d)", code)
		}
		respondJSON(w, http.StatusInternalServerError, QueryResponse{
			Success:     false,
			Error:       errMsg,
			ErrorType:   "execution",
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

	// Process histogram into bucketed array. Buckets are aligned to the snapped
	// window, not the raw query range.
	var histogram []int
	if needsHistogram && histRes.err == nil {
		histogram = bucketHistogram(histRes.rows, prep.histBucketStart, histBucketSec, histBucketCount)
	} else if needsHistogram && histRes.err != nil {
		log.Printf("[QueryHandler] Histogram query failed (non-critical): %v", histRes.err)
	}

	// Collect per-shard profiling data when requested (before any early returns).
	var profileData *ProfileData
	if req.Profile && err == nil {
		profileData = h.fetchProfileData(mainQueryID)
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
					response.TimeStart = prep.histBucketStart.Format(time.RFC3339)
					response.TimeEnd = prep.histBucketEnd.Format(time.RFC3339)
					response.BucketSec = histBucketSec
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
		response.TimeStart = prep.histBucketStart.Format(time.RFC3339)
		response.TimeEnd = prep.histBucketEnd.Format(time.RFC3339)
		response.BucketSec = histBucketSec
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

// bucketHistogram folds histogram query rows ({bucket, cnt}) into a fixed-size
// count array aligned to startTime. Shared by the buffered and streaming paths.
func bucketHistogram(rows []map[string]interface{}, startTime time.Time, histBucketSec, histBucketCount int) []int {
	if histBucketCount <= 0 || histBucketSec <= 0 {
		return nil
	}
	histogram := make([]int, histBucketCount)
	foldHistogramRows(histogram, rows, startTime, histBucketSec, 0, histBucketCount)
	return histogram
}

// foldHistogramRows writes histogram query rows into dst, keeping only buckets
// in the half-open index range [loIdx, hiIdx). Chunked scans use the bound to
// discard a boundary row that the adjacent chunk owns, which keeps the folded
// result identical to a single full-range scan.
func foldHistogramRows(dst []int, rows []map[string]interface{}, startTime time.Time, bucketSec, loIdx, hiIdx int) {
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
		idx := int(bucketTime.Sub(startTime).Seconds()) / bucketSec
		if idx < loIdx || idx >= hiIdx || idx >= len(dst) {
			continue
		}
		switch c := cntVal.(type) {
		case uint64:
			dst[idx] = int(c)
		case int64:
			dst[idx] = int(c)
		case float64:
			dst[idx] = int(c)
		case int:
			dst[idx] = c
		}
	}
}

// Sentinel errors used to unwind StreamQuery's onRow callback for non-failure
// reasons: the page cap was reached, or the client connection went away.
var (
	errStreamPageFull = errors.New("stream page full")
	errStreamClient   = errors.New("stream client gone")
)

const (
	streamFlushRows = 50                     // flush a row batch once this many accumulate
	streamFlushIval = 80 * time.Millisecond  // ...or this long has passed since the last flush
	streamProgIval  = 100 * time.Millisecond // throttle progress frames
)

// HandleQueryStream executes a BQL search and streams the result as NDJSON
// frames (one JSON object per line). For plain non-aggregated searches left in
// the default timestamp-DESC order it emits rows newest-first as ClickHouse
// produces them, with progress frames driving a loading indicator. Aggregations,
// charts, and explicitly sorted queries are not progressively streamable, so
// they fall back to a single buffered batch delivered through the same frame
// protocol (meta -> rows -> done), keeping the frontend path uniform.
func (h *QueryHandler) HandleQueryStream(w http.ResponseWriter, r *http.Request) {
	prep := h.prepareQuery(w, r)
	if prep == nil {
		return // prepareQuery already wrote the response
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		respondJSON(w, http.StatusInternalServerError, QueryResponse{
			Success: false,
			Error:   "Streaming not supported by server",
		})
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	// writeFrame encodes one NDJSON frame and flushes it. Returns false when the
	// connection is gone so callers can stop early. The clickhouse-go progress
	// callback fires from the driver's background read goroutine, concurrently
	// with the row-iteration goroutine, so all frame writes are serialized by a
	// mutex to keep the NDJSON stream from interleaving.
	var writeMu sync.Mutex
	writeFrame := func(frame map[string]interface{}) bool {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := enc.Encode(frame); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	startWall := time.Now()

	meta := map[string]interface{}{
		"type":          "meta",
		"streaming":     prep.streamable,
		"sql":           prep.sql,
		"field_order":   prep.fieldOrder,
		"is_aggregated": prep.isAggregated,
		"chart_type":    prep.chartType,
		"chart_config":  prep.chartConfig,
		"time_start":    prep.startTime.Format(time.RFC3339),
		"time_end":      prep.endTime.Format(time.RFC3339),
	}
	// Announce the histogram's shape up front so the client can clear the previous
	// search's timeline and reserve the axis before any counts exist, instead of
	// leaving a stale chart on screen while the scan runs.
	if prep.needsHistogram {
		meta["histogram_meta"] = map[string]interface{}{
			"bucket_start":   prep.histBucketStart.Format(time.RFC3339),
			"bucket_seconds": prep.histBucketSec,
			"bucket_count":   prep.histBucketCount,
		}
	}
	if !writeFrame(meta) {
		return
	}

	// Every ClickHouse query this request issues carries an id built from the search
	// id, so whatever is still running when the request ends -- the common case is a
	// client that navigated away mid-scan -- gets killed instead of running unowned.
	searchID := newSearchID()
	searchClean := false
	queryCtx, cancel := searchContext(r.Context(), settings.Get().QueryTimeoutSeconds)
	defer func() {
		cancel()
		if !searchClean {
			h.abandonSearch(searchID)
		}
	}()

	// Histogram (non-aggregated, first page only). The main query stops at the row
	// limit after scanning only the newest window, while the histogram must count
	// every matching row across the whole range, so it is routinely the slower of
	// the two. Rather than race it against a deadline and discard it, scan it in
	// bucket-aligned chunks newest-first and stream each cumulative result as it
	// lands: the timeline fills in right-to-left while the table is already usable.
	type histUpdate struct {
		buckets     []int // cumulative snapshot across all chunks scanned so far
		coveredFrom int   // buckets [coveredFrom, bucketCount) have been scanned
	}
	histCh := make(chan histUpdate, len(prep.histChunks))
	if prep.needsHistogram {
		go func() {
			defer close(histCh)
			cumulative := make([]int, prep.histBucketCount)
			for i, chunk := range prep.histChunks {
				// Chunks are ordered newest-first; only cache the historical ones.
				chunkSQL, sqlErr := prep.buildHistogramSQL(chunk.start, chunk.end, i > 0)
				if sqlErr != nil {
					log.Printf("[QueryHandler] Streaming histogram SQL failed (non-critical): %v", sqlErr)
					return
				}
				rows, qErr := h.db.QueryUserSearchWithID(queryCtx, fmt.Sprintf("%s-h%d", searchID, i), chunkSQL)
				if qErr != nil {
					// Keep whatever earlier chunks already delivered; the client
					// renders it as a partial histogram rather than losing it.
					log.Printf("[QueryHandler] Streaming histogram chunk failed (non-critical): %v", qErr)
					return
				}
				foldHistogramRows(cumulative, rows, prep.histBucketStart, prep.histBucketSec, chunk.loIdx, chunk.hiIdx)
				snapshot := make([]int, len(cumulative))
				copy(snapshot, cumulative)
				histCh <- histUpdate{buckets: snapshot, coveredFrom: chunk.loIdx}
			}
		}()
	}

	histDone := !prep.needsHistogram
	histComplete := !prep.needsHistogram
	// drainHistogram writes every chunk result that is ready. With block=true it
	// waits for the scan to finish, bounded by the query timeout, so the complete
	// histogram always reaches the client. Returns false if the client went away.
	drainHistogram := func(block bool) bool {
		for !histDone {
			var update histUpdate
			var ok bool
			if block {
				update, ok = <-histCh
			} else {
				select {
				case update, ok = <-histCh:
				default:
					return true
				}
			}
			if !ok {
				histDone = true
				break
			}
			if update.coveredFrom == 0 {
				histComplete = true
			}
			if !writeFrame(map[string]interface{}{
				"type":           "histogram",
				"buckets":        update.buckets,
				"bucket_start":   prep.histBucketStart.Format(time.RFC3339),
				"bucket_seconds": prep.histBucketSec,
				"bucket_count":   prep.histBucketCount,
				"covered_from":   update.coveredFrom,
				"complete":       update.coveredFrom == 0,
			}) {
				return false
			}
		}
		return true
	}

	// Outcome fields populated by whichever branch runs, emitted in the done frame.
	var count int
	var nextCursorOut string
	var hasMoreOut bool
	var limitHitOut string

	emitExecError := func(execErr error) {
		log.Printf("[QueryHandler] Streaming query failed: %v", execErr)
		msg := "Query execution failed"
		if friendly, ok := clickhouseUserMessage(execErr); ok {
			msg = friendly
		} else if code := clickhouseErrorCode(execErr); code != 0 {
			msg = fmt.Sprintf("Query execution failed (ClickHouse error %d)", code)
		}
		writeFrame(map[string]interface{}{
			"type":       "error",
			"error":      msg,
			"error_type": "execution",
		})
	}

	if prep.streamable {
		batch := make([]map[string]interface{}, 0, streamFlushRows)
		lastFlush := time.Now()
		lastProgress := time.Time{}
		var lastKept map[string]interface{}
		clientGone := false

		flushBatch := func() bool {
			if len(batch) == 0 {
				return true
			}
			frame := map[string]interface{}{"type": "rows", "data": batch}
			ok := writeFrame(frame)
			batch = make([]map[string]interface{}, 0, streamFlushRows)
			lastFlush = time.Now()
			return ok
		}

		onRow := func(row map[string]interface{}) error {
			// When cursor paging is active the SQL is LIMIT cursorPageSize+1; the
			// extra row signals has_more. Otherwise the SQL already carries the
			// effective limit (explicit head() or the default max-rows cap), so we
			// stream every row it returns rather than capping at a page.
			if prep.appliedCursorPaging && count >= cursorPageSize {
				hasMoreOut = true
				return errStreamPageFull
			}
			sanitizeFloats(row)
			batch = append(batch, row)
			count++
			lastKept = row
			if len(batch) >= streamFlushRows || time.Since(lastFlush) >= streamFlushIval {
				if !flushBatch() {
					clientGone = true
					return errStreamClient
				}
				drainHistogram(false)
			}
			return nil
		}

		onProgress := func(read, total uint64) {
			now := time.Now()
			if now.Sub(lastProgress) < streamProgIval {
				return
			}
			lastProgress = now
			ratio := 0.0
			if total > 0 {
				ratio = float64(read) / float64(total)
				if ratio > 1 {
					ratio = 1
				}
			}
			writeFrame(map[string]interface{}{
				"type":       "progress",
				"read_rows":  read,
				"total_rows": total,
				"ratio":      ratio,
			})
		}

		// For streamable queries spanning more than 8 hours, iterate 8-hour windows
		// newest-first and stop as soon as the row cap is reached. This avoids
		// issuing a single distributed scan over the full range: ClickHouse must
		// scan every granule in the range regardless of LIMIT when non-primary-key
		// filters (fractal_id, fields.*) are involved. Windowing lets the scan stop
		// as soon as enough rows are found in recent data.
		useWindowing := prep.req.Cursor == "" && !prep.appliedCursorPaging &&
			prep.queryMaxRows > 0 && prep.endTime.Sub(prep.startTime) > 8*time.Hour

		if useWindowing {
			const windowDuration = 8 * time.Hour
			windowEnd := prep.endTime
			numWindows := int(math.Ceil(prep.endTime.Sub(prep.startTime).Hours() / 8))
			log.Printf("[QueryHandler] Windowed streaming: %d x 8h windows over %s",
				numWindows, prep.endTime.Sub(prep.startTime).Round(time.Minute))

			// Selective mode: use pre-computed active days when provided by the
			// caller (e.g. from model row data), otherwise run the cheap preflight
			// query to discover which calendar days have data.
			var activeDays map[string]bool
			if len(prep.req.ActiveDays) > 0 {
				activeDays = make(map[string]bool, len(prep.req.ActiveDays))
				for _, d := range prep.req.ActiveDays {
					if len(d) >= 10 {
						activeDays[d[:10]] = true
					}
				}
				log.Printf("[QueryHandler] Selective windowing: %d pre-computed active days", len(activeDays))
			} else if prep.req.Selective {
				activeDaySQL := buildActiveDaysSQL(prep.translationOpts)
				if dayRows, dayErr := h.db.Query(queryCtx, activeDaySQL); dayErr != nil {
					log.Printf("[QueryHandler] Active-days preflight failed (non-fatal): %v", dayErr)
				} else if len(dayRows) > 0 {
					activeDays = make(map[string]bool, len(dayRows))
					for _, row := range dayRows {
						switch v := row["day"].(type) {
						case time.Time:
							activeDays[v.UTC().Format("2006-01-02")] = true
						case string:
							// scanRowMap formats time.Time as "2006-01-02 15:04:05.000";
							// take only the date portion.
							if len(v) >= 10 {
								activeDays[v[:10]] = true
							}
						}
					}
					log.Printf("[QueryHandler] Selective windowing: %d active days found", len(activeDays))
				} else {
					log.Printf("[QueryHandler] Selective windowing: preflight returned no days, skipping selective optimization")
				}
			}

			firstWindow := true
			windowIdx := 0
			for !clientGone && count < prep.queryMaxRows && windowEnd.After(prep.startTime) {
				windowStart := windowEnd.Add(-windowDuration)
				if windowStart.Before(prep.startTime) {
					windowStart = prep.startTime
				}
				// From window 2 onward subtract 1 second from the upper bound so that
				// rows landing exactly on the 8-hour boundary are not returned twice.
				// The format string is second-precision so 1s is the smallest unit that
				// changes the rendered literal.
				queryEnd := windowEnd
				if !firstWindow {
					queryEnd = windowEnd.Add(-time.Second)
				}
				firstWindow = false

				// Selective: skip windows whose entire date range has no data.
				if activeDays != nil {
					d1 := windowStart.UTC().Format("2006-01-02")
					d2 := queryEnd.UTC().Format("2006-01-02")
					if !activeDays[d1] && !activeDays[d2] {
						windowEnd = windowStart
						continue
					}
				}

				windowSQL, sqlErr := prep.buildWindowSQL(windowStart, queryEnd, prep.queryMaxRows-count)
				if sqlErr != nil {
					emitExecError(sqlErr)
					return
				}
				streamErr := h.db.StreamQuery(queryCtx, fmt.Sprintf("%s-w%d", searchID, windowIdx), windowSQL, onRow, onProgress)
				windowIdx++
				flushBatch()
				if clientGone || r.Context().Err() != nil {
					return
				}
				if streamErr != nil && !errors.Is(streamErr, errStreamPageFull) && !errors.Is(streamErr, errStreamClient) {
					if queryCtx.Err() == context.DeadlineExceeded {
						writeFrame(map[string]interface{}{"type": "error", "error": "Query timeout: add more specific filters or reduce the time range", "error_type": "timeout"})
					} else {
						emitExecError(streamErr)
					}
					return
				}
				drainHistogram(false)
				windowEnd = windowStart
			}
			if count >= prep.queryMaxRows {
				if prep.isBloomQuery {
					limitHitOut = "bloom"
				} else {
					limitHitOut = "search"
				}
			}
		} else {
			streamErr := h.db.StreamQuery(queryCtx, searchID+"-main", prep.sql, onRow, onProgress)
			flushBatch()
			if clientGone || r.Context().Err() != nil {
				return
			}
			if streamErr != nil && !errors.Is(streamErr, errStreamPageFull) && !errors.Is(streamErr, errStreamClient) {
				if queryCtx.Err() == context.DeadlineExceeded {
					writeFrame(map[string]interface{}{"type": "error", "error": "Query timeout: add more specific filters or reduce the time range", "error_type": "timeout"})
				} else {
					emitExecError(streamErr)
				}
				return
			}
			if prep.appliedCursorPaging {
				if hasMoreOut && lastKept != nil {
					if nc, encErr := encodeCursor(lastKept); encErr == nil {
						nextCursorOut = nc
					} else {
						hasMoreOut = false
					}
				}
			} else if count == prep.queryMaxRows {
				if prep.isBloomQuery {
					limitHitOut = "bloom"
				} else {
					limitHitOut = "search"
				}
			}
		}
	} else {
		rows, qErr := h.db.QueryUserSearchWithID(queryCtx, searchID+"-main", prep.sql)
		if qErr != nil {
			if r.Context().Err() != nil {
				return
			}
			if queryCtx.Err() == context.DeadlineExceeded {
				writeFrame(map[string]interface{}{"type": "error", "error": "Query timeout: add more specific filters or reduce the time range", "error_type": "timeout"})
			} else {
				emitExecError(qErr)
			}
			return
		}

		if prep.appliedCursorPaging && len(rows) > cursorPageSize {
			rows = rows[:cursorPageSize]
			if nc, encErr := encodeCursor(rows[len(rows)-1]); encErr == nil {
				nextCursorOut = nc
				hasMoreOut = true
			}
		}
		if len(rows) == prep.queryMaxRows {
			if prep.isBloomQuery {
				limitHitOut = "bloom"
			} else {
				limitHitOut = "search"
			}
		}
		for _, row := range rows {
			sanitizeFloats(row)
		}
		count = len(rows)
		drainHistogram(false)
		if !writeFrame(map[string]interface{}{"type": "rows", "data": rows}) {
			return
		}
	}

	drainHistogram(false)

	executionMs := time.Since(startWall).Milliseconds()
	// done reports the row scan, not the histogram. histogram_pending tells the
	// client more histogram frames are still coming so it can show the unscanned
	// span as pending instead of treating a partial timeline as final.
	if !writeFrame(map[string]interface{}{
		"type":              "done",
		"count":             count,
		"has_more":          hasMoreOut,
		"next_cursor":       nextCursorOut,
		"execution_ms":      executionMs,
		"limit_hit":         limitHitOut,
		"histogram_pending": !histDone,
	}) {
		return
	}

	// Rows are delivered; let the remaining histogram chunks finish on the still
	// open stream instead of discarding a scan the client is waiting on.
	searchClean = drainHistogram(true) && histDone
	if !histComplete {
		// The scan stopped early (error or timeout). Say so, so the client stops
		// waiting and marks the uncounted span as unscanned rather than as zero.
		writeFrame(map[string]interface{}{"type": "histogram_end", "complete": false})
	}

	username := ""
	if user, ok := r.Context().Value("user").(*storage.User); ok && user != nil {
		username = user.Username
	}
	go h.logQueryAudit(prep.req.Query, "bql", prep.selectedIndex, username, executionMs, count)
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
		FractalID string `json:"fractal_id,omitempty"`
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

	// Use the client-supplied fractal_id as a partition-pruning filter, but only
	// after confirming it is in the caller's accessible set (admins may use any).
	// An invalid or absent value falls back to an unscoped lookup; the post-fetch
	// verification below is the authoritative access check either way.
	scopeFractalID := scopedFractalFilter(req.FractalID, accessible, isAdmin)

	logEntry, err := h.db.GetLogByTimestamp(r.Context(), timestamp, req.LogID, scopeFractalID)
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

	// The exact timestamp from the search result is required: it is the leading
	// sort key and part of the partition key, so it turns the lookup from a
	// whole-table bloom-filter scan into a near-pinpoint read. There is a single,
	// deterministic lookup path (no log_id-only fallback), so a missing or
	// malformed timestamp is a client error rather than a slow-path trigger.
	ts := parseLogTimestamp(r.URL.Query().Get("timestamp"))
	if ts.IsZero() {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "valid timestamp is required",
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

	// fractal_id is required: every log has one, and it is both a partition key
	// and a mandatory pruning filter. The translator always projects it in search
	// results, so absence means a malformed or very old client request.
	rawFractalID := r.URL.Query().Get("fractal_id")
	if rawFractalID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "fractal_id is required",
		})
		return
	}
	scopeFractalID := scopedFractalFilter(rawFractalID, accessible, isAdmin)
	if scopeFractalID == "" {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"error":   "Log not found",
		})
		return
	}

	var shardNum uint64
	if s := r.URL.Query().Get("shard_num"); s != "" {
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			shardNum = n
		}
	}

	logEntry, err := h.db.GetLogFieldsByIDDirect(r.Context(), logID, ts, scopeFractalID, shardNum)
	if err != nil {
		log.Printf("[QueryHandler] HandleGetLogFields: failed to fetch fields: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "Failed to retrieve log fields",
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

	fields, _ := logEntry["fields"].(map[string]interface{})
	if fields == nil {
		fields = map[string]interface{}{}
	}
	// raw_log is the original pre-normalization text for the detail "Raw" tab. It is
	// TTL'd, so it is empty ("") once the row ages past the retention window; the UI
	// renders that as "unavailable". It is lazy-loaded here rather than shipped with
	// every search result row.
	rawLog, _ := logEntry["raw_log"].(string)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"fields":  fields,
		"raw_log": rawLog,
	})
}

// parseLogTimestamp parses a timestamp string supplied by the frontend for a
// log lookup. It accepts the ClickHouse result format (what the search results
// carry, e.g. "2026-03-22 18:37:11.329", interpreted as UTC) as well as RFC3339
// variants. It returns the zero time on any failure so callers treat the
// timestamp as an absent optimization hint rather than an error.
func parseLogTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// scopedFractalFilter validates a client-supplied fractal_id for use as a
// partition-pruning filter on a single-log lookup. It returns the value only
// when the caller is an admin or the value is in the session's accessible set;
// otherwise it returns "" so the lookup runs unscoped and the caller's
// post-fetch access check stays authoritative. This lets the fast path prune to
// one fractal partition without ever trusting the client for access control.
func scopedFractalFilter(requested string, accessible []string, isAdmin bool) string {
	if requested == "" {
		return ""
	}
	if isAdmin {
		return requested
	}
	for _, id := range accessible {
		if id == requested {
			return requested
		}
	}
	return ""
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

	selectCols := "timestamp, norm_log, log_id, fractal_id"
	if h.db.IsCluster() {
		selectCols += ", toString(_shard_num) AS _shard_num"
	}
	logsSQL := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY timestamp DESC LIMIT 50", selectCols, h.queryTableName(), whereClause)

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
		for _, col := range []string{"timestamp", "norm_log", "log_id", "fractal_id", "_shard_num"} {
			if v, ok := rawResult[col]; ok {
				result[col] = v
			}
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
		FieldOrder:  []string{"timestamp", "norm_log", "log_id"},
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
	var tsMilli int64
	switch v := row["timestamp"].(type) {
	case time.Time:
		tsMilli = v.UnixMilli()
	case string:
		t := parseLogTimestamp(v)
		if t.IsZero() {
			return "", fmt.Errorf("cannot parse timestamp string %q for cursor", v)
		}
		tsMilli = t.UnixMilli()
	default:
		return "", fmt.Errorf("unexpected timestamp type %T", row["timestamp"])
	}
	lid, _ := row["log_id"].(string)
	b, err := json.Marshal(cursorToken{TSMilli: tsMilli, LID: lid})
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

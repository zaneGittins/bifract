package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/settings"
)

// ExecuteResult is the output of a server-side (non-HTTP) BQL execution. Its
// fields mirror the subset of QueryResponse that dashboard widgets render.
type ExecuteResult struct {
	Results      []map[string]interface{} `json:"results"`
	Count        int                      `json:"count"`
	ExecutionMs  int64                    `json:"execution_ms"`
	SQL          string                   `json:"sql"`
	ChartType    string                   `json:"chart_type"`
	ChartConfig  map[string]interface{}   `json:"chart_config"`
	FieldOrder   []string                 `json:"field_order"`
	IsAggregated bool                     `json:"is_aggregated"`
}

// ExecuteBQL runs a BQL query server-side without an HTTP request, scoped to a
// single fractal or prism over the given time window. It mirrors the core of
// prepareQuery + HandleQuery (parse -> enrich -> translate -> execute) minus
// auth, cursor pagination, histogram, and profiling.
//
// Callers are responsible for authorization: this is a trusted path used by the
// dashboard executor and the on-demand widget execute endpoints, where access is
// already gated at the dashboard level. Queries run at low ClickHouse priority so
// background refreshes never starve interactive searches or ingestion.
//
// Pass either fractalID (direct scope) or prismID (prism scope), not both.
func (h *QueryHandler) ExecuteBQL(ctx context.Context, queryStr, fractalID, prismID string, start, end time.Time) (*ExecuteResult, error) {
	if strings.TrimSpace(queryStr) == "" {
		return nil, fmt.Errorf("query is empty")
	}
	const maxQueryLength = 10000
	if len(queryStr) > maxQueryLength {
		return nil, fmt.Errorf("query too long (%d chars, max %d)", len(queryStr), maxQueryLength)
	}

	pipeline, err := parser.ParseQuery(queryStr)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	maxRows := h.maxRows

	// Resolve prism member fractals when scoped to a prism.
	var prismFractalIDs []string
	isPrismContext := prismID != "" && fractalID == ""
	if isPrismContext {
		if h.prismManager != nil {
			prismFractalIDs, _ = h.prismManager.GetMemberFractalIDs(ctx, prismID)
		}
		if len(prismFractalIDs) == 0 {
			return &ExecuteResult{Results: []map[string]interface{}{}, FieldOrder: []string{}, ChartConfig: map[string]interface{}{}}, nil
		}
	}

	// Include legacy logs with empty fractal_id when targeting the default fractal.
	includeEmptyFractalID := false
	if h.fractalManager != nil {
		if defaultFractal, derr := h.fractalManager.GetDefaultFractal(ctx); derr == nil {
			if len(prismFractalIDs) > 0 {
				for _, id := range prismFractalIDs {
					if id == defaultFractal.ID {
						includeEmptyFractalID = true
						break
					}
				}
			} else if !isPrismContext && fractalID == defaultFractal.ID {
				includeEmptyFractalID = true
			}
		}
	}

	// Dictionary mappings for match() resolution.
	var dictMappings map[string]map[string]string
	if h.dictionaryManager != nil {
		if isPrismContext {
			if m, derr := h.dictionaryManager.ListDictionaryMappings(ctx, "", prismID); derr == nil {
				dictMappings = m
			}
		} else if fractalID != "" {
			if m, derr := h.dictionaryManager.ListDictionaryMappings(ctx, fractalID, ""); derr == nil {
				dictMappings = m
			}
		}
	}

	// Analytics model infos for model_lookup() resolution.
	var modelInfos map[string]parser.AnalyticsModelInfo
	if h.modelManager != nil && fractalID != "" {
		if infos, derr := h.modelManager.ListModelInfos(ctx, fractalID); derr == nil {
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

	// Pre-resolve comment() filter to matching log_ids from PostgreSQL.
	var commentLogIDs []string
	var hasCommentFilter bool
	commentTags, commentKeyword, hasComment := parser.ExtractCommentParams(pipeline)
	if hasComment && h.pg != nil {
		hasCommentFilter = true
		if isPrismContext {
			commentLogIDs, err = h.pg.GetCommentedLogIDsFiltered(ctx, "", prismID, start, end, commentTags, commentKeyword)
		} else {
			commentLogIDs, err = h.pg.GetCommentedLogIDsFiltered(ctx, fractalID, "", start, end, commentTags, commentKeyword)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to resolve comment filter: %w", err)
		}
	}

	fractalIDForQuery := fractalID
	if isPrismContext {
		fractalIDForQuery = ""
	}
	opts := parser.QueryOptions{
		StartTime:             start,
		EndTime:               end,
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
		IncludeShardNum:       h.db != nil && h.db.IsCluster(),
	}
	translationResult, err := parser.TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	sql := translationResult.SQL
	isAggregated := translationResult.IsAggregated

	// Apply a LIMIT when the query does not already carry one. This mirrors the
	// non-cursor branch of HandleQuery so widget results match interactive runs.
	if !sqlLimitRE.MatchString(sql) {
		if isAggregated {
			sql += " LIMIT 10000"
		} else {
			if idx := strings.LastIndex(sql, " ORDER BY"); idx >= 0 {
				if !strings.Contains(strings.ToUpper(sql[idx:]), "LOG_ID") {
					sql += ", log_id DESC"
				}
			}
			sql += fmt.Sprintf(" LIMIT %d", cursorPageSize)
		}
	}

	queryStart := time.Now()
	qctx := ctx
	if queryTimeoutSec := settings.Get().QueryTimeoutSeconds; queryTimeoutSec > 0 {
		var cancel context.CancelFunc
		qctx, cancel = context.WithTimeout(ctx, time.Duration(queryTimeoutSec)*time.Second)
		defer cancel()
	}
	rows, err := h.db.QueryLowPriority(qctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	execMs := time.Since(queryStart).Milliseconds()

	if rows == nil {
		rows = []map[string]interface{}{}
	}
	fieldOrder := translationResult.FieldOrder
	if fieldOrder == nil {
		fieldOrder = []string{}
	}
	chartConfig := translationResult.ChartConfig
	if chartConfig == nil {
		chartConfig = map[string]interface{}{}
	}

	return &ExecuteResult{
		Results:      rows,
		Count:        len(rows),
		ExecutionMs:  execMs,
		SQL:          sql,
		ChartType:    translationResult.ChartType,
		ChartConfig:  chartConfig,
		FieldOrder:   fieldOrder,
		IsAggregated: isAggregated,
	}, nil
}

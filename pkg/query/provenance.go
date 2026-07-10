package query

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"bifract/pkg/parser"
)

// respondProvenanceGraph writes the already-computed pgr() scored edge rows as a buffered
// JSON QueryResponse (the non-streaming HandleQuery path).
func (h *QueryHandler) respondProvenanceGraph(w http.ResponseWriter, prep *preparedQuery) {
	respondJSON(w, http.StatusOK, QueryResponse{
		Success:     true,
		Results:     prep.provenanceRows,
		Count:       len(prep.provenanceRows),
		Query:       prep.req.Query,
		FieldOrder:  prep.fieldOrder,
		ChartType:   prep.chartType,
		ChartConfig: prep.chartConfig,
		TimeStart:   prep.startTime.Format(time.RFC3339),
		TimeEnd:     prep.endTime.Format(time.RFC3339),
	})
}

// streamProvenanceGraph emits the already-computed pgr() rows over the NDJSON stream
// protocol (meta -> rows -> done), matching HandleQueryStream's non-streamable branch so the
// frontend's streaming client parses it identically to an aggregation/chart result.
func (h *QueryHandler) streamProvenanceGraph(w http.ResponseWriter, prep *preparedQuery) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondJSON(w, http.StatusInternalServerError, QueryResponse{Success: false, Error: "Streaming not supported by server"})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	write := func(frame map[string]interface{}) { _ = enc.Encode(frame); flusher.Flush() }

	write(map[string]interface{}{
		"type":          "meta",
		"streaming":     false,
		"field_order":   prep.fieldOrder,
		"is_aggregated": false,
		"chart_type":    prep.chartType,
		"chart_config":  prep.chartConfig,
		"time_start":    prep.startTime.Format(time.RFC3339),
		"time_end":      prep.endTime.Format(time.RFC3339),
	})
	for _, row := range prep.provenanceRows {
		sanitizeFloats(row)
	}
	write(map[string]interface{}{"type": "rows", "data": prep.provenanceRows})
	write(map[string]interface{}{"type": "done", "count": len(prep.provenanceRows), "execution_ms": 0})
}

// runProvenanceGraph orchestrates the pgr() two-pass query: pass 1 traverses the ptg()
// spawn tree to collect the process-guid set (tree membership), pass 2 fetches + scores
// every edge among/from those guids against the proc_freq baseline. Returns the scored,
// threshold-pruned edge rows (columns: parent, child, label, event_type, anomaly_score).
func (h *QueryHandler) runProvenanceGraph(ctx context.Context, p parser.ProvenanceParams, opts parser.QueryOptions) ([]map[string]interface{}, error) {
	treeSQL, err := parser.BuildProcessTreeQuery(p, opts)
	if err != nil {
		return nil, fmt.Errorf("pgr: build tree query: %w", err)
	}
	treeRows, err := h.db.QueryLowPriority(ctx, treeSQL)
	if err != nil {
		return nil, fmt.Errorf("pgr: tree pass: %w", err)
	}
	seen := make(map[string]struct{}, len(treeRows))
	guids := make([]string, 0, len(treeRows))
	for _, r := range treeRows {
		g, _ := r["process_guid"].(string)
		if g == "" {
			continue
		}
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		guids = append(guids, g)
	}
	if len(guids) == 0 {
		return []map[string]interface{}{}, nil
	}
	scoreSQL, err := parser.BuildProvenanceScoringSQL(guids, p.Threshold, p.EdgeTypes, opts)
	if err != nil {
		return nil, fmt.Errorf("pgr: build scoring query: %w", err)
	}
	rows, err := h.db.QueryLowPriority(ctx, scoreSQL)
	if err != nil {
		return nil, fmt.Errorf("pgr: scoring pass: %w", err)
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	return rows, nil
}

// provenanceFieldOrder is the column order of the pgr() scored edge list.
// provenanceFieldOrder is the display column order. fractal_id is intentionally omitted here
// (the frontend hides it) but IS present in every row so the standard /logs/fields detail
// fetch -- which needs log_id + timestamp + fractal_id -- works from a pgr row or pgraph node.
var provenanceFieldOrder = []string{"parent", "child", "label", "event_type", "anomaly_score", "log_id", "timestamp"}

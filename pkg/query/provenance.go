package query

import (
	"context"
	"fmt"

	"bifract/pkg/parser"
)

// provenanceColumns are the flat columns the pgr() scored edge list exposes. pgr() is a source
// command (parser/source_command.go): it is resolved into a SQL subquery source and these
// resolve as bare columns for any downstream BQL (filter/aggregate/sort/table/pgraph).
var provenanceColumns = []string{"parent", "child", "label", "event_type", "anomaly_score", "log_id", "timestamp", "fractal_id", "command_line", "proc_user"}

// provenanceNumericColumns is the subset of provenanceColumns that are already numeric in the
// subquery (so downstream numeric comparisons must not string-coerce them).
var provenanceNumericColumns = []string{"anomaly_score"}

// provenanceEmptyScoreSQL yields zero rows with the pgr output shape, so a query over an empty
// tree behaves correctly (count() -> 0, etc.) without special-casing every caller.
const provenanceEmptyScoreSQL = "SELECT '' AS parent, '' AS child, '' AS label, '' AS event_type, toFloat64(0) AS anomaly_score, '' AS log_id, '' AS timestamp, '' AS fractal_id, '' AS command_line, '' AS proc_user WHERE 1 = 0"

// provenanceScoreSQL runs pass 1 (tree traversal, collect guids) and returns the pass-2
// scored-edge SQL, which becomes the query's subquery source. Returns a zero-row stub when the
// tree is empty. The pass-2 SQL already carries `ORDER BY (event_type='spawn') DESC,
// anomaly_score DESC LIMIT`, so the spawn backbone is prioritized inside the subquery and an
// outer LIMIT never re-truncates process structure.
func (h *QueryHandler) provenanceScoreSQL(ctx context.Context, p parser.ProvenanceParams, opts parser.QueryOptions) (string, error) {
	treeSQL, err := parser.BuildProcessTreeQuery(p, opts)
	if err != nil {
		return "", fmt.Errorf("pgr: build tree query: %w", err)
	}
	treeRows, err := h.db.QueryLowPriority(ctx, treeSQL)
	if err != nil {
		return "", fmt.Errorf("pgr: tree pass: %w", err)
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
		return provenanceEmptyScoreSQL, nil
	}
	scoreSQL, err := parser.BuildProvenanceScoringSQL(guids, p.Threshold, p.EdgeTypes, opts)
	if err != nil {
		return "", fmt.Errorf("pgr: build scoring query: %w", err)
	}
	return scoreSQL, nil
}

// resolvedSource is a source command's SQL subquery + the flat columns it exposes.
type resolvedSource struct {
	SQL            string
	Columns        []string // exposed flat columns, in default projection order
	NumericColumns []string // subset of Columns already numeric (no string-coercion on compare)
	Focus          string   // optional node identifier the viz should center/highlight (pgr start)
}

// sourceResolver produces a resolvedSource for a source command. Resolution runs in the query
// layer because it may need the database (e.g. pgr()'s pass-1 tree traversal). Register one per
// source-command name; the core translate/handler path stays generic.
type sourceResolver func(h *QueryHandler, ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error)

var sourceResolvers = map[string]sourceResolver{
	"pgr": resolvePgrSource,
}

// resolveSourceCommand resolves a source command into its subquery source.
func (h *QueryHandler) resolveSourceCommand(ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error) {
	r, ok := sourceResolvers[cmd.Name]
	if !ok {
		return nil, fmt.Errorf("unknown source command: %s()", cmd.Name)
	}
	return r(h, ctx, cmd, opts)
}

// resolvePgrSource resolves pgr() into its two-pass scored-edge subquery.
func resolvePgrSource(h *QueryHandler, ctx context.Context, cmd parser.CommandNode, opts parser.QueryOptions) (*resolvedSource, error) {
	if opts.SourceMode == parser.SourceIceberg {
		return nil, fmt.Errorf("pgr() operates on live process lineage and is not available over archived data")
	}
	p, ok := parser.ParseProvenanceParams(cmd)
	if !ok {
		return nil, fmt.Errorf("pgr() requires start=\"<process_guid>\"")
	}
	sql, err := h.provenanceScoreSQL(ctx, p, opts)
	if err != nil {
		return nil, err
	}
	return &resolvedSource{SQL: sql, Columns: provenanceColumns, NumericColumns: provenanceNumericColumns, Focus: p.Start}, nil
}

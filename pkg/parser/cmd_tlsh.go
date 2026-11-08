package parser

import (
	"fmt"
	"strings"
)

// TLSHMatch is one digest present in the data that fell within the threshold of a
// needle. Digest is the verbatim value as stored, never a canonicalised form: it
// goes straight into an equality filter over the log field, so it has to match
// what is on disk. Producers differ in case and in whether they write the T1
// prefix, and the same logical digest can legitimately appear in more than one
// spelling, each of which indexes and matches independently.
type TLSHMatch struct {
	Digest   string
	Distance int
	Needle   string
}

// tlshMaxLiteralBytes is the backstop on the inline payload tlsh() renders (the IN
// list plus the two transform() arrays), guarding against unusually long needle
// labels. The resolver's own match cap is the limit that normally binds, and it
// reports when it does; this one only stops a query from tripping ClickHouse's
// max_query_size. Half of maxGeneratedQuerySize (16MB), so the rest of the query
// keeps its headroom.
const tlshMaxLiteralBytes = 8 * 1024 * 1024

// TLSHDistanceField and TLSHMatchField are the columns tlsh() projects, so a hunt
// can rank by closeness and see which needle it hit.
const (
	TLSHDistanceField = "tlsh_distance"
	TLSHMatchField    = "tlsh_match"
)

// tlshHandler handles tlsh(field=..., hash=|dict=..., threshold=N): a similarity
// filter over a fuzzy-hash field.
//
// The distance itself is computed server-side in Go, not in SQL. Comparing a field
// against many needles has no workable SQL formulation: an unrolled OR of a
// thousand terms exceeds the query-tree limit, and arrayExists over a constant
// tuple array materialises that array per row and exhausts memory. Because digests
// repeat heavily across rows, the work collapses to the number of DISTINCT digests,
// which a tlsh model indexes. The resolved matches then come back as an ordinary
// IN filter, so the rest of the pipeline composes normally.
type tlshHandler struct{ rowFilter }

func (h *tlshHandler) appliesNegate() {}
func (h *tlshHandler) readsLogRows()  {}

func (h *tlshHandler) predicate(cmd CommandNode, ctx *CommandContext) (string, error) {
	p, found, err := extractTLSHFromCommand(cmd)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("tlsh() needs field= and hash= or dict=")
	}
	// Reached only where a caller translates without pre-resolving the digests.
	// Name the contexts that legitimately cannot, so the message is actionable
	// rather than describing an internal contract: a join() subquery is translated
	// with its own options and never carries the outer command's matches (copying
	// them would filter the inner field by the outer field's digests), and archive
	// search has no TLSH model index to resolve against.
	if !ctx.Opts.HasTLSHFilter {
		return "", fmt.Errorf("tlsh() cannot be resolved in this context; it is not supported inside a join() subquery or in archive search")
	}
	// The digests were resolved against the stored field, so a field computed
	// before the filter is not what they describe.
	if ctx.Registry.IsComputed(p.Field) {
		return "", fmt.Errorf("tlsh(): field %q is computed by an earlier command; tlsh() can only filter a stored log field", p.Field)
	}
	// A similarity filter reads as "keep the rows whose digest looks like this".
	// After an aggregation it would silently filter the rows before it instead.
	if lastAggregatingCommandIndex(ctx.Plan.commands, ctx.CmdIndex) >= 0 {
		return "", fmt.Errorf("tlsh() must come before groupby/stats: it filters log rows, not aggregated ones")
	}
	digests, _, _ := renderTLSHLiterals(ctx.Opts.TLSHMatches)
	// No match, or nothing that fits. An empty IN list is a syntax error, so emit a
	// constant predicate: nothing is similar, so the positive form matches no rows
	// and the negated form matches every row.
	if len(digests) == 0 {
		if cmd.Negate {
			return "1 = 1", nil
		}
		return "1 = 0", nil
	}
	op := "IN"
	if cmd.Negate {
		op = "NOT IN"
	}
	return fmt.Sprintf("%s %s (%s)", groupableCast(jsonFieldRef(p.Field)), op, strings.Join(digests, ", ")), nil
}

// renderTLSHLiterals turns resolved matches into the quoted SQL literals the IN
// list and the two transform() arrays share. Matches arrive closest-first, so
// trimming the tail against the payload backstop drops the weakest.
func renderTLSHLiterals(matches []TLSHMatch) (digests, distances, needles []string) {
	digests = make([]string, 0, len(matches))
	distances = make([]string, 0, len(matches))
	needles = make([]string, 0, len(matches))
	budget := 0
	for _, m := range matches {
		d := "'" + escapeString(m.Digest) + "'"
		n := "'" + escapeString(m.Needle) + "'"
		dist := fmt.Sprintf("%d", m.Distance)
		// Each digest is carried twice: the IN list and the transform key array.
		budget += 2*len(d) + len(dist) + len(n) + 4
		if budget > tlshMaxLiteralBytes && len(digests) > 0 {
			break
		}
		digests = append(digests, d)
		distances = append(distances, dist)
		needles = append(needles, n)
	}
	return digests, distances, needles
}

// projectTLSHTransforms maps each matched digest back to its distance and needle,
// so the scores travel with the rows instead of needing a second lookup. Rows that
// matched the query some other way (tlsh() as one side of an OR) fall to the
// defaults, which is why this is correct wherever the filter itself sits.
func projectTLSHTransforms(ctx *CommandContext, source *QueryStage, fieldRef string, digests, distances, needles []string) {
	distanceExpr := fmt.Sprintf("transform(%s, [%s], [%s], -1)",
		fieldRef, strings.Join(digests, ", "), strings.Join(distances, ", "))
	matchExpr := fmt.Sprintf("transform(%s, [%s], [%s], '')",
		fieldRef, strings.Join(digests, ", "), strings.Join(needles, ", "))

	source.Layer.UpsertSelect(SelectExpr{Expr: distanceExpr, Alias: TLSHDistanceField})
	source.Layer.UpsertSelect(SelectExpr{Expr: matchExpr, Alias: TLSHMatchField})
	publishTLSHExprs(ctx, distanceExpr, matchExpr)
}

// publishTLSHExprs tells the registry what these columns actually compute.
//
// Declare registers them as placeholders (Expr == name), which FieldRegistry.Resolve
// reads as "no expression yet" and answers with a raw fields.`name` JSON path. That
// is what made `| table(..., tlsh_distance)` render an always-empty column: table()
// clears the source SELECT and rebuilds it from the registry, so the projection
// added above was discarded and the placeholder resolved to a log field that does
// not exist. Publishing the real expression is the convention every transform
// command follows (see cmd_transforms.go).
func publishTLSHExprs(ctx *CommandContext, distanceExpr, matchExpr string) {
	ctx.Registry.SetResolveExpr(TLSHDistanceField, distanceExpr)
	ctx.Registry.SetResolveExpr(TLSHMatchField, matchExpr)
}

// projectTLSHConstants emits the "no match on this row" form of the projected
// columns, so a downstream sort or table still resolves them.
func projectTLSHConstants(ctx *CommandContext, source *QueryStage) {
	source.Layer.UpsertSelect(SelectExpr{Expr: "toInt32(-1)", Alias: TLSHDistanceField})
	source.Layer.UpsertSelect(SelectExpr{Expr: "''", Alias: TLSHMatchField})
	publishTLSHExprs(ctx, "toInt32(-1)", "''")
}

// projectTLSHColumns registers and projects tlsh_distance and tlsh_match for a
// query using tlsh(), so a hunt can rank by closeness and see which needle it hit.
// The filter itself compiles as a condition wherever it is written; the columns
// are the same transform() of the field in every form, a match or not.
func projectTLSHColumns(pipeline *PipelineNode, ctx *CommandContext) error {
	if !ctx.Opts.HasTLSHFilter {
		return nil
	}
	p, found, err := ExtractTLSHParams(pipeline)
	if err != nil || !found {
		return err
	}
	ctx.Registry.Register(TLSHDistanceField, FieldKindPerRow, TLSHDistanceField, -1)
	ctx.Registry.Register(TLSHMatchField, FieldKindPerRow, TLSHMatchField, -1)
	source := ctx.Plan.SourceStage()
	digests, distances, needles := renderTLSHLiterals(ctx.Opts.TLSHMatches)
	if len(digests) == 0 {
		projectTLSHConstants(ctx, source)
		return nil
	}
	projectTLSHTransforms(ctx, source, groupableCast(jsonFieldRef(p.Field)), digests, distances, needles)
	return nil
}

// extractTLSHFromCommand parses one tlsh() node, reusing the pipeline-level parser
// so the command and the server-side resolver can never disagree on the arguments.
func extractTLSHFromCommand(cmd CommandNode) (TLSHParams, bool, error) {
	return ExtractTLSHParams(&PipelineNode{Steps: []Step{{Command: &cmd}}})
}

func init() {
	registerCommand(&tlshHandler{}, "tlsh")
}

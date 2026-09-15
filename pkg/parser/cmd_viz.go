package parser

import (
	"fmt"
	"strings"
)

// piechartHandler handles piechart(limit=N)
type piechartHandler struct{}

func (h *piechartHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *piechartHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	if len(source.Layer.GroupBy) == 0 && !ctx.Plan.HasGroupBy {
		return fmt.Errorf("piechart() needs grouped rows: put it straight after groupby(), e.g. groupby(level) | piechart(). A bare count() in between collapses the groups to a single row.")
	}
	if !ctx.Plan.IsAggregated {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
		ctx.Plan.IsAggregated = true
	}
	ctx.Plan.ChartType = "piechart"
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	ctx.Plan.ChartConfig["render"] = chartRender(b, 10, 0)
	if err := applyChartLimit(b, ctx); err != nil {
		return fmt.Errorf("piechart(): %w", err)
	}
	return nil
}

// barchartHandler handles barchart(limit=N)
type barchartHandler struct{}

func (h *barchartHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *barchartHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	if len(source.Layer.GroupBy) == 0 && !ctx.Plan.HasGroupBy {
		return fmt.Errorf("barchart() needs grouped rows: put it straight after groupby(), e.g. groupby(level) | barchart(). A bare count() in between collapses the groups to a single row.")
	}
	if !ctx.Plan.IsAggregated {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
		ctx.Plan.IsAggregated = true
	}
	ctx.Plan.ChartType = "barchart"
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	ctx.Plan.ChartConfig["render"] = chartRender(b, 10, 0)
	if err := applyChartLimit(b, ctx); err != nil {
		return fmt.Errorf("barchart(): %w", err)
	}
	return nil
}

// graphHandler handles graph(child=field, parent=field, labels=field1,field2, limit=N)
type graphHandler struct{}

func (h *graphHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *graphHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.ChartType = "graph"
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	childField, err := chartFieldName(b, "graph", "child")
	if err != nil {
		return err
	}
	parentField, err := chartFieldName(b, "graph", "parent")
	if err != nil {
		return err
	}
	labelFields := b.Strings("labels")
	ctx.Plan.ChartConfig["render"] = chartRender(b, 100, 500)
	if err := applyChartLimit(b, ctx); err != nil {
		return fmt.Errorf("graph(): %w", err)
	}

	if childField == "" || parentField == "" {
		return fmt.Errorf("graph() requires both child= and parent= parameters, e.g. graph(child=process_guid, parent=parent_process_guid)")
	}

	ctx.Plan.ChartConfig["childField"] = childField
	ctx.Plan.ChartConfig["parentField"] = parentField
	if len(labelFields) > 0 {
		ctx.Plan.ChartConfig["labels"] = labelFields
	}
	return nil
}

// pgraphHandler handles pgraph(limit=N): the provenance-native visualization for a pgr()
// scored edge list (process/file/socket/dns nodes shaped by type, edges colored by
// anomaly_score). Unlike graph()/mesh() it reads pgr()'s fixed output columns, so its only
// argument is an optional render limit. It is a plain chart command -- pgr() (the source
// command) is already resolved into the query's subquery source before this runs.
type pgraphHandler struct{}

func (h *pgraphHandler) Declare(cmd CommandNode, ctx *CommandContext) error { return nil }

func (h *pgraphHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.ChartType = "pgraph"
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	ctx.Plan.ChartConfig["render"] = chartRender(b, 3000, 0)
	if err := applyChartLimit(b, ctx); err != nil {
		return fmt.Errorf("pgraph(): %w", err)
	}
	return nil
}

// meshHandler handles mesh(src=field, dst=field, weight=field, size=field,
// color=field, label=field1,field2, directed=bool, limit=N). Unlike graph()
// (a directed parent-child tree), mesh() renders an undirected, weighted,
// bidirectional network (Arkime-style connections). It expects a pre-aggregated
// edge list, typically from groupby(src, dst), whose auto count column is _count.
type meshHandler struct{}

func (h *meshHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *meshHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.ChartType = "mesh"
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	srcField, err := chartFieldName(b, "mesh", "src")
	if err != nil {
		return err
	}
	dstField, err := chartFieldName(b, "mesh", "dst")
	if err != nil {
		return err
	}
	weightField, err := chartFieldName(b, "mesh", "weight")
	if err != nil {
		return err
	}
	sizeField, err := chartFieldName(b, "mesh", "size")
	if err != nil {
		return err
	}
	colorField := b.Str("color", "")
	labelFields := b.Strings("labels")
	ctx.Plan.ChartConfig["directed"] = b.Flag("directed", false)
	ctx.Plan.ChartConfig["render"] = chartRender(b, 100, 500)
	if err := applyChartLimit(b, ctx); err != nil {
		return fmt.Errorf("mesh(): %w", err)
	}

	if srcField == "" || dstField == "" {
		return fmt.Errorf("mesh() requires both src= and dst= parameters, e.g. mesh(src=src_ip, dst=dst_ip)")
	}

	// Default edge weight and node size to the groupby auto-count column so the
	// minimal form (mesh(src=..., dst=...)) works after groupby(src, dst).
	if weightField == "" {
		weightField = "_count"
	}
	if sizeField == "" {
		sizeField = "_count"
	}

	ctx.Plan.ChartConfig["srcField"] = srcField
	ctx.Plan.ChartConfig["dstField"] = dstField
	ctx.Plan.ChartConfig["weightField"] = weightField
	ctx.Plan.ChartConfig["sizeField"] = sizeField
	if colorField != "" {
		ctx.Plan.ChartConfig["color"] = colorField
	}
	if len(labelFields) > 0 {
		ctx.Plan.ChartConfig["labels"] = labelFields
	}
	return nil
}

// singlevalHandler handles singleval(label="Label")
type singlevalHandler struct{}

func (h *singlevalHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *singlevalHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if !ctx.Plan.IsAggregated {
		return fmt.Errorf("singleval() requires an aggregation function (e.g. count(), avg(), sum())")
	}
	// Only reject if the current stage still has an active GROUP BY with no
	// outer aggregation wrapping it. When a second stage was pushed (e.g.
	// groupby(field) | count() | singleval()), the current stage has no
	// GROUP BY and produces a single row. Similarly, outerAggregations
	// collapse grouped results into a single value.
	currentStage := ctx.Plan.CurrentStage()
	if len(currentStage.Layer.GroupBy) > 0 && len(ctx.Plan.outerAggregations) == 0 {
		return fmt.Errorf("singleval() cannot be used with groupBy() - it displays a single value only")
	}
	ctx.Plan.ChartType = "singleval"
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if title := b.Str("title", ""); title != "" {
		ctx.Plan.ChartConfig["title"] = title
	}
	return nil
}

// timechartHandler handles timechart(span=5m, function=count())
type timechartHandler struct{}

func (h *timechartHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.IsAggregated = true
	ctx.Plan.HasGroupBy = true
	return nil
}

func (h *timechartHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	span := b.Str("span", "1h")
	fn := b.Agg("function")
	if fn == nil {
		fn = &AggSpec{Name: "count"}
	}

	n, unit := parseBucketSpan(span)
	bucketExpr := getBucketExpression(n, unit, bucketTimezone(ctx))
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: bucketExpr, Alias: "time_bucket"})

	switch fn.Name {
	case "count":
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
	case "sum", "avg", "max", "min":
		if len(fn.Args) == 0 {
			return fmt.Errorf("timechart %s(): needs a field, e.g. timechart(span=1h, %s(bytes))", fn.Name, fn.Name)
		}
		operand, err := aggOperandNumeric(fn.Args[0], ctx.Registry)
		if err != nil {
			return fmt.Errorf("timechart %s(): %w", fn.Name, err)
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr:  fmt.Sprintf("%s(%s)", fn.Name, operand),
			Alias: "_" + fn.Name,
		})
	case "percent":
		// One series per field: the share of rows in each bucket where that
		// (boolean) field is truthy. Fields resolve through the registry so a
		// case()/computed boolean column is inlined.
		var percentFields []string
		for _, a := range aggFields(fn) {
			f := a.FieldName()
			if f == "" {
				return fmt.Errorf("timechart percent(): %s is not a field", a)
			}
			safe, err := sanitizeIdentifier(f)
			if err != nil {
				return fmt.Errorf("timechart percent(): %w", err)
			}
			// Resolve BEFORE re-registering so a case()/computed boolean column is
			// inlined as its CASE expression, not referenced by its own alias.
			ref := resolveFieldRef(f, ctx.Registry)
			expr := fmt.Sprintf("round(100.0 * countIf(toString(%s) IN ('true', '1')) / count(*), 2)", ref)
			// Upsert so a same-named per-row column from a prior case() is replaced
			// by this aggregate rather than duplicated; re-register as an aggregate
			// output so stage assembly keeps it (a per-row alias would be stripped).
			source.Layer.UpsertSelect(SelectExpr{Expr: expr, Alias: safe})
			ctx.Registry.Register(f, FieldKindAggregate, f, ctx.CmdIndex)
			ctx.Registry.SetResolveExpr(f, f)
			ctx.Plan.aggregationOutputs[f] = expr
			percentFields = append(percentFields, f)
		}
		ctx.Plan.IsAggregated = true
		ctx.Plan.ChartConfig["valueFields"] = percentFields
		ctx.Plan.ChartConfig["unit"] = "percent"
		ctx.Plan.ChartConfig["yLabel"] = "Percent"
	case "groupby":
		fields := aggFields(fn)
		if len(fields) == 0 {
			return fmt.Errorf("timechart groupby(): needs a field")
		}
		refs := make([]string, len(fields))
		aliases := make([]string, len(fields))
		for i, a := range fields {
			ref, err := ResolveArg(a, ctx.Registry)
			if err != nil {
				return fmt.Errorf("timechart groupby(): %w", err)
			}
			alias, err := ArgAlias(a)
			if err != nil {
				return fmt.Errorf("timechart groupby(): %w", err)
			}
			refs[i], aliases[i] = ref, alias
		}
		if aggFlag(fn, false, "distinct", "unique") {
			// Cardinality over time: count distinct values (or tuples) of the
			// field(s) within each bucket as a single series.
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr:  fmt.Sprintf("uniqExact(%s)", strings.Join(refs, ", ")),
				Alias: "_count",
			})
		} else {
			for i, ref := range refs {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: ref, Alias: aliases[i]})
				source.Layer.GroupBy = append(source.Layer.GroupBy, ref)
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
		}
		source.Layer.OrderBy = append([]string{"_count DESC"}, source.Layer.OrderBy...)
	default:
		return fmt.Errorf("timechart(): unknown function %s()", fn.Name)
	}

	source.Layer.GroupBy = append(source.Layer.GroupBy, bucketExpr)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "time_bucket ASC")

	ctx.Plan.ChartType = "timechart"
	ctx.Plan.ChartConfig["span"] = span
	return nil
}

// graphWorldHandler handles graphWorld(lat=field, lon=field, label=field, limit=N)
type graphWorldHandler struct{}

func (h *graphWorldHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *graphWorldHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.ChartType = "worldmap"

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	latField, err := chartFieldName(b, "graphWorld", "lat")
	if err != nil {
		return err
	}
	lonField, err := chartFieldName(b, "graphWorld", "lon")
	if err != nil {
		return err
	}
	labelField, err := chartFieldName(b, "graphWorld", "label")
	if err != nil {
		return err
	}
	ctx.Plan.ChartConfig["render"] = chartRender(b, 5000, 50000)
	if err := applyChartLimit(b, ctx); err != nil {
		return fmt.Errorf("graphworld(): %w", err)
	}

	if latField == "" {
		latField = "latitude"
	}
	if lonField == "" {
		lonField = "longitude"
	}

	ctx.Plan.ChartConfig["latField"] = latField
	ctx.Plan.ChartConfig["lonField"] = lonField
	if labelField != "" {
		ctx.Plan.ChartConfig["labelField"] = labelField
	}

	return nil
}

func init() {
	registerAggregatingCommand(&piechartHandler{}, "piechart")
	registerAggregatingCommand(&barchartHandler{}, "barchart")
	registerCommand(&graphHandler{}, "graph")
	registerCommand(&meshHandler{}, "mesh")
	registerCommand(&pgraphHandler{}, "pgraph")
	registerAggregatingCommand(&singlevalHandler{}, "singleval")
	registerAggregatingCommand(&timechartHandler{}, "timechart")
	registerCommand(&graphWorldHandler{}, "graphWorld", "graphworld", "worldmap")
}

func init() {
	// Charts take no positional fields: they read the columns the preceding
	// aggregation produced. render= caps the marks drawn, limit= the rows scanned.
	registerSpec(&CommandSpec{Name: "piechart", Params: []ParamSpec{namedLit("render"), namedLit("limit")}})
	registerSpec(&CommandSpec{Name: "barchart", Params: []ParamSpec{namedLit("render"), namedLit("limit")}})
	// heatmap(x=a, y=b, count()) puts the aggregate positionally, as timechart does.
	registerSpec(&CommandSpec{Name: "heatmap", Params: []ParamSpec{
		ParamSpec{Name: "value", Kind: ParamAggSpec, Positional: true},
		namedField("x"), namedField("y"), namedLit("limit"),
	}})
	// #11: bucket() was timechart() with fewer aggregates and different output
	// column names. timechart() is the one spelling.
	registerSpec(&CommandSpec{Name: "singleval", Params: []ParamSpec{field("field"), namedLit("title")}})
	// timechart(span=1d, count()) puts the aggregate positionally.
	registerSpec(&CommandSpec{Name: "timechart", Params: []ParamSpec{
		ParamSpec{Name: "function", Kind: ParamAggSpec, Positional: true},
		namedLit("span"),
	}})
	registerSpec(&CommandSpec{Name: "graph", Params: []ParamSpec{
		namedField("parent"), namedField("child"), namedList("labels"),
		namedLit("render"), namedLit("limit"),
	}})
	registerSpec(&CommandSpec{Name: "mesh", Params: []ParamSpec{
		namedField("src"), namedField("dst"), namedList("labels"),
		namedField("size"), namedField("weight"),
		namedLit("directed"), namedLit("color"), namedLit("render"), namedLit("limit"),
	}})
	registerSpec(&CommandSpec{Name: "pgraph", Params: []ParamSpec{namedLit("render"), namedLit("limit")}})
	registerSpec(&CommandSpec{Name: "graphworld", Params: []ParamSpec{
		namedField("lat"), namedField("lon"), namedField("label"),
		namedLit("render"), namedLit("limit"),
	}}, "graphworld", "graphWorld", "worldmap")
}

// chartFieldName is a column name a chart hands to the browser. The renderer
// looks the name up in the returned rows, so an expression there would name a
// column no row has; it is rejected rather than drawn as an empty chart.
func chartFieldName(b *Bound, cmd, param string) (string, error) {
	a, ok := b.First(param)
	if !ok {
		return "", nil
	}
	name := a.FieldName()
	if name == "" {
		return "", fmt.Errorf("%s(): %s= takes a field name, not an expression (%s); compute it first with eval() or :=", cmd, param, a)
	}
	return name, nil
}

// chartRender reads a chart command's render=, the number of marks the browser
// draws. It is separate from limit=, which bounds the rows the query returns:
// one caps the picture, the other caps the scan.
func chartRender(b *Bound, def, maxRender int) int {
	n := b.Int("render", def)
	if n <= 0 {
		return def
	}
	if maxRender > 0 {
		n = min(n, maxRender)
	}
	return n
}

// applyChartLimit applies a chart command's limit= as a row limit on the query.
func applyChartLimit(b *Bound, ctx *CommandContext) error {
	if !b.Has("limit") {
		return nil
	}
	n, err := validateInt(b.Str("limit", ""))
	if err != nil {
		return err
	}
	ctx.Plan.CurrentStage().Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	return nil
}

// aggFields returns an aggregate spec's field arguments, ignoring its named
// options (distinct=, unique=).
func aggFields(spec *AggSpec) []Argument {
	var out []Argument
	for _, a := range spec.Args {
		if a.Name != "" {
			continue
		}
		if a.Kind == ArgList {
			out = append(out, a.List...)
			continue
		}
		out = append(out, a)
	}
	return out
}

// aggFlag reads a boolean option of an aggregate spec, under any of its names.
func aggFlag(spec *AggSpec, def bool, names ...string) bool {
	for _, a := range spec.Args {
		for _, n := range names {
			if !strings.EqualFold(a.Name, n) {
				continue
			}
			switch strings.ToLower(a.Value()) {
			case "false", "0", "no":
				return false
			case "true", "1", "yes":
				return true
			}
		}
	}
	return def
}

package parser

import (
	"fmt"
	"strconv"
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
		return fmt.Errorf("piechart() requires groupby() - cannot create pie chart without grouped data")
	}
	if !ctx.Plan.IsAggregated {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
		ctx.Plan.IsAggregated = true
	}
	ctx.Plan.ChartType = "piechart"
	ctx.Plan.ChartConfig["limit"] = 10
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "limit=") {
			if limit, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && limit > 0 {
				ctx.Plan.ChartConfig["limit"] = limit
			}
		}
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
		return fmt.Errorf("barchart() requires groupby() - cannot create bar chart without grouped data")
	}
	if !ctx.Plan.IsAggregated {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
		ctx.Plan.IsAggregated = true
	}
	ctx.Plan.ChartType = "barchart"
	ctx.Plan.ChartConfig["limit"] = 10
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "limit=") {
			if limit, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && limit > 0 {
				ctx.Plan.ChartConfig["limit"] = limit
			}
		}
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
	ctx.Plan.ChartConfig["limit"] = 100
	var childField, parentField string
	var labelFields []string

	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "child=") {
			childField = strings.TrimPrefix(arg, "child=")
		} else if strings.HasPrefix(arg, "parent=") {
			parentField = strings.TrimPrefix(arg, "parent=")
		} else if strings.HasPrefix(arg, "labels=") {
			labelsArg := strings.TrimPrefix(arg, "labels=")
			labelsArg = strings.Trim(labelsArg, "[]")
			for _, f := range strings.Split(labelsArg, ",") {
				f = strings.TrimSpace(f)
				if f != "" {
					labelFields = append(labelFields, f)
				}
			}
		} else if strings.HasPrefix(arg, "limit=") {
			if limit, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && limit > 0 {
				if limit > 500 {
					limit = 500
				}
				ctx.Plan.ChartConfig["limit"] = limit
			}
		}
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
	ctx.Plan.ChartConfig["limit"] = 100
	ctx.Plan.ChartConfig["directed"] = false
	var srcField, dstField, weightField, sizeField, colorField string
	var labelFields []string

	for _, arg := range cmd.Arguments {
		switch {
		case strings.HasPrefix(arg, "src="):
			srcField = strings.TrimPrefix(arg, "src=")
		case strings.HasPrefix(arg, "dst="):
			dstField = strings.TrimPrefix(arg, "dst=")
		case strings.HasPrefix(arg, "weight="):
			weightField = strings.TrimPrefix(arg, "weight=")
		case strings.HasPrefix(arg, "size="):
			sizeField = strings.TrimPrefix(arg, "size=")
		case strings.HasPrefix(arg, "color="):
			colorField = strings.TrimPrefix(arg, "color=")
		case strings.HasPrefix(arg, "directed="):
			v := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "directed=")))
			ctx.Plan.ChartConfig["directed"] = v == "true" || v == "1" || v == "yes"
		case strings.HasPrefix(arg, "labels="), strings.HasPrefix(arg, "label="):
			labelsArg := arg[strings.IndexByte(arg, '=')+1:]
			labelsArg = strings.Trim(labelsArg, "[]")
			for _, f := range strings.Split(labelsArg, ",") {
				f = strings.TrimSpace(f)
				if f != "" {
					labelFields = append(labelFields, f)
				}
			}
		case strings.HasPrefix(arg, "limit="):
			if limit, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && limit > 0 {
				if limit > 500 {
					limit = 500
				}
				ctx.Plan.ChartConfig["limit"] = limit
			}
		}
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
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "label=") {
			label := strings.TrimPrefix(arg, "label=")
			label = strings.Trim(label, `"'`)
			ctx.Plan.ChartConfig["label"] = label
		}
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
	var span, function string
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "span=") {
			span = strings.TrimPrefix(arg, "span=")
		} else if strings.HasPrefix(arg, "function=") {
			val := strings.TrimPrefix(arg, "function=")
			if val != "" {
				function = val
			}
		} else if strings.Contains(arg, "(") {
			function = arg
		}
	}

	if span == "" {
		span = "1h"
	}
	if function == "" {
		function = "count()"
	}

	n, unit := parseBucketSpan(span)
	bucketExpr := getBucketExpression(n, unit)
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: bucketExpr, Alias: "time_bucket"})

	if strings.Contains(function, "count()") {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
	} else if strings.Contains(function, "sum(") {
		field := extractFunctionField(function, "sum")
		cast := numericCast(field, resolveFieldRef(field, ctx.Registry), ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("sum(%s)", cast), Alias: "_sum"})
	} else if strings.Contains(function, "avg(") {
		field := extractFunctionField(function, "avg")
		cast := numericCast(field, resolveFieldRef(field, ctx.Registry), ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("avg(%s)", cast), Alias: "_avg"})
	} else if strings.Contains(function, "max(") {
		field := extractFunctionField(function, "max")
		cast := numericCast(field, resolveFieldRef(field, ctx.Registry), ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("max(%s)", cast), Alias: "_max"})
	} else if strings.Contains(function, "min(") {
		field := extractFunctionField(function, "min")
		cast := numericCast(field, resolveFieldRef(field, ctx.Registry), ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("min(%s)", cast), Alias: "_min"})
	} else if strings.Contains(function, "percent(") {
		// percent(f1, f2, ...): one series per field giving the share of rows in
		// each bucket where that (boolean) field is truthy. Fields resolve through
		// the registry so case()/computed boolean columns are inlined.
		fields := parseTimechartFieldList(function, "percent")
		var percentFields []string
		for _, f := range fields {
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
	} else if strings.Contains(function, "groupby(") {
		fields, distinct := parseTimechartGroupBy(function)
		if distinct && len(fields) > 0 {
			// Cardinality over time: count distinct values (or tuples) of the
			// field(s) within each bucket as a single series.
			refs := make([]string, len(fields))
			for i, f := range fields {
				refs[i] = resolveFieldRef(f, ctx.Registry)
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr:  fmt.Sprintf("uniqExact(%s)", strings.Join(refs, ", ")),
				Alias: "_count",
			})
		} else {
			for _, f := range fields {
				ref := resolveFieldRef(f, ctx.Registry)
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: ref, Alias: f})
				source.Layer.GroupBy = append(source.Layer.GroupBy, ref)
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
		}
		source.Layer.OrderBy = append([]string{"_count DESC"}, source.Layer.OrderBy...)
	}

	source.Layer.GroupBy = append(source.Layer.GroupBy, bucketExpr)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "time_bucket ASC")

	ctx.Plan.ChartType = "timechart"
	ctx.Plan.ChartConfig["span"] = span
	return nil
}

// parseTimechartFieldList extracts the comma-separated field list from a
// timechart function spec like name(field[,field...]). A leading field= prefix
// on any entry is stripped.
func parseTimechartFieldList(function, name string) []string {
	prefix := name + "("
	start := strings.Index(function, prefix)
	if start < 0 {
		return nil
	}
	inner := function[start+len(prefix):]
	if i := strings.LastIndex(inner, ")"); i >= 0 {
		inner = inner[:i]
	}
	var fields []string
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields = append(fields, strings.TrimPrefix(part, "field="))
	}
	return fields
}

// parseTimechartGroupBy extracts the field list and distinct flag from a
// timechart function=groupby(field[,field...][,distinct=true]) spec. With
// distinct, the result is a single cardinality series; otherwise rows are
// grouped by the field(s) and counted per bucket.
func parseTimechartGroupBy(function string) (fields []string, distinct bool) {
	for _, part := range parseTimechartFieldList(function, "groupby") {
		switch part {
		case "distinct=true", "unique=true":
			distinct = true
		case "distinct=false", "unique=false":
			distinct = false
		default:
			fields = append(fields, part)
		}
	}
	return fields, distinct
}

// graphWorldHandler handles graphWorld(lat=field, lon=field, label=field, limit=N)
type graphWorldHandler struct{}

func (h *graphWorldHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *graphWorldHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.ChartType = "worldmap"
	ctx.Plan.ChartConfig["limit"] = 5000

	var latField, lonField, labelField string

	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "lat=") {
			latField = strings.TrimPrefix(arg, "lat=")
		} else if strings.HasPrefix(arg, "lon=") {
			lonField = strings.TrimPrefix(arg, "lon=")
		} else if strings.HasPrefix(arg, "label=") {
			labelField = strings.TrimPrefix(arg, "label=")
		} else if strings.HasPrefix(arg, "limit=") {
			if limit, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && limit > 0 {
				if limit > 50000 {
					limit = 50000
				}
				ctx.Plan.ChartConfig["limit"] = limit
			}
		}
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
	registerAggregatingCommand(&singlevalHandler{}, "singleval")
	registerAggregatingCommand(&timechartHandler{}, "timechart")
	registerCommand(&graphWorldHandler{}, "graphWorld", "graphworld", "worldmap")
}

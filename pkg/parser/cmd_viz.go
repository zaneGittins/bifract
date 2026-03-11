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

// singlevalHandler handles singleval(label="Label")
type singlevalHandler struct{}

func (h *singlevalHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *singlevalHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if !ctx.Plan.IsAggregated {
		return fmt.Errorf("singleval() requires an aggregation function (e.g. count(), avg(), sum())")
	}
	if ctx.Plan.HasGroupBy {
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
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("sum(toFloat64OrNull(%s))", jsonFieldRef(field)), Alias: "_sum"})
	} else if strings.Contains(function, "avg(") {
		field := extractFunctionField(function, "avg")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("avg(toFloat64OrNull(%s))", jsonFieldRef(field)), Alias: "_avg"})
	} else if strings.Contains(function, "max(") {
		field := extractFunctionField(function, "max")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("max(toFloat64OrNull(%s))", jsonFieldRef(field)), Alias: "_max"})
	} else if strings.Contains(function, "min(") {
		field := extractFunctionField(function, "min")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("min(toFloat64OrNull(%s))", jsonFieldRef(field)), Alias: "_min"})
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
	registerCommand(&piechartHandler{}, "piechart")
	registerCommand(&barchartHandler{}, "barchart")
	registerCommand(&graphHandler{}, "graph")
	registerCommand(&singlevalHandler{}, "singleval")
	registerCommand(&timechartHandler{}, "timechart")
	registerCommand(&graphWorldHandler{}, "graphWorld", "graphworld", "worldmap")
}

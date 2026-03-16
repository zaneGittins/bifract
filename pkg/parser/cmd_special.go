package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// tableHandler handles table(field1, field2, ...)
type tableHandler struct{}

func (h *tableHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasTableCmd = true
	return nil
}

func (h *tableHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasTableCmd = true
	source := ctx.Plan.CurrentStage()

	// Clear existing selects (table replaces default)
	source.Layer.Selects = nil

	var nonAggregateFields []string

	for _, field := range cmd.Arguments {
		if strings.HasPrefix(field, "limit=") {
			if n, err := validateInt(strings.TrimPrefix(field, "limit=")); err == nil {
				source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
			}
			continue
		}
		if field == "timestamp" {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "timestamp"})
			nonAggregateFields = append(nonAggregateFields, "timestamp")
		} else if field == "count" || strings.HasPrefix(field, "count(") {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _count"})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "sum(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "sum("), ")")
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("sum(toFloat64OrNull(%s)) AS _sum", jsonFieldRef(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "avg(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "avg("), ")")
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("avg(toFloat64OrNull(%s)) AS _avg", jsonFieldRef(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "max(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "max("), ")")
			if innerField == "timestamp" {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "max(timestamp) AS max_timestamp"})
			} else {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
					Expr: fmt.Sprintf("max(toFloat64OrNull(%s)) AS _max", jsonFieldRef(innerField)),
				})
			}
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "min(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "min("), ")")
			if innerField == "timestamp" {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "min(timestamp) AS min_timestamp"})
			} else {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
					Expr: fmt.Sprintf("min(toFloat64OrNull(%s)) AS _min", jsonFieldRef(innerField)),
				})
			}
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "percentile(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "percentile("), ")")
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(toFloat64OrNull(%s)) AS percentile_%s", jsonFieldRef(innerField), escapeString(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "stdDev(") || strings.HasPrefix(field, "stddev(") {
			var innerField string
			if strings.HasPrefix(field, "stdDev(") {
				innerField = strings.TrimSuffix(strings.TrimPrefix(field, "stdDev("), ")")
			} else {
				innerField = strings.TrimSuffix(strings.TrimPrefix(field, "stddev("), ")")
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("stddevPop(toFloat64OrNull(%s)) AS stddev_%s", jsonFieldRef(innerField), escapeString(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "median(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "median("), ")")
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("median(toFloat64OrNull(%s)) AS median_%s", jsonFieldRef(innerField), escapeString(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "mad(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "mad("), ")")
			ref := jsonFieldRef(innerField)
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', groupArray(toFloat64OrNull(%s)))), groupArray(toFloat64OrNull(%s)))) AS mad_%s", ref, ref, escapeString(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "skewness(") || strings.HasPrefix(field, "skew(") {
			var innerField string
			if strings.HasPrefix(field, "skewness(") {
				innerField = strings.TrimSuffix(strings.TrimPrefix(field, "skewness("), ")")
			} else {
				innerField = strings.TrimSuffix(strings.TrimPrefix(field, "skew("), ")")
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("skewPop(toFloat64OrNull(%s)) AS skewness_%s", jsonFieldRef(innerField), escapeString(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "kurtosis(") || strings.HasPrefix(field, "kurt(") {
			var innerField string
			if strings.HasPrefix(field, "kurtosis(") {
				innerField = strings.TrimSuffix(strings.TrimPrefix(field, "kurtosis("), ")")
			} else {
				innerField = strings.TrimSuffix(strings.TrimPrefix(field, "kurt("), ")")
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("kurtPop(toFloat64OrNull(%s)) AS kurtosis_%s", jsonFieldRef(innerField), escapeString(innerField)),
			})
			ctx.Plan.IsAggregated = true
		} else if strings.HasPrefix(field, "iqr(") {
			innerField := strings.TrimSuffix(strings.TrimPrefix(field, "iqr("), ")")
			ref := jsonFieldRef(innerField)
			source.Layer.Selects = append(source.Layer.Selects,
				SelectExpr{Expr: fmt.Sprintf("quantile(0.25)(toFloat64OrNull(%s)) AS _q1", ref)},
				SelectExpr{Expr: fmt.Sprintf("quantile(0.75)(toFloat64OrNull(%s)) AS _q3", ref)},
				SelectExpr{Expr: fmt.Sprintf("quantile(0.75)(toFloat64OrNull(%s)) - quantile(0.25)(toFloat64OrNull(%s)) AS _iqr", ref, ref)},
			)
			ctx.Plan.IsAggregated = true
		} else if entry := ctx.Registry.Get(field); entry != nil && (entry.Kind == FieldKindPerRow || entry.Kind == FieldKindAssignment) {
			safeAlias, err := sanitizeIdentifier(field)
			if err != nil {
				return fmt.Errorf("table(): %w", err)
			}
			computedExpr := ctx.Registry.Resolve(field)
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", computedExpr, safeAlias)})
			nonAggregateFields = append(nonAggregateFields, field)
		} else {
			safeAlias, err := sanitizeIdentifier(field)
			if err != nil {
				return fmt.Errorf("table(): %w", err)
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", jsonFieldRef(field), safeAlias)})
			nonAggregateFields = append(nonAggregateFields, field)
		}
	}

	// Auto GROUP BY for non-aggregate fields when table() includes aggregations
	if ctx.Plan.IsAggregated && len(nonAggregateFields) > 0 {
		for _, field := range nonAggregateFields {
			if field == "timestamp" {
				source.Layer.GroupBy = append(source.Layer.GroupBy, "timestamp")
			} else {
				source.Layer.GroupBy = append(source.Layer.GroupBy, jsonFieldRef(field))
			}
		}
	}
	return nil
}

// bfsHandler handles bfs/dfs traversal commands
type bfsHandler struct{}

func (h *bfsHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// Set traversal mode early so condition routing knows to use HAVING for post-CTE filters
	ctx.Plan.IsTraversal = true
	// _depth and _path are CTE-produced fields (FieldKindWindow).
	// routeConditions sends Window fields to HAVING for traversal queries,
	// which buildTraversalSQL uses as the post-CTE filter.
	ctx.Registry.Register("_depth", FieldKindWindow, "_depth", ctx.CmdIndex)
	ctx.Registry.Register("_path", FieldKindWindow, "_path", ctx.CmdIndex)
	return nil
}

func (h *bfsHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if ctx.Plan.TraversalMode != "" {
		return fmt.Errorf("cannot use multiple traversal functions (bfs/dfs) in the same query")
	}
	var traversalChild, traversalParent, traversalStart string
	traversalDepth := 0
	var traversalInclude []string

	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "child=") {
			traversalChild = strings.TrimPrefix(arg, "child=")
		} else if strings.HasPrefix(arg, "parent=") {
			traversalParent = strings.TrimPrefix(arg, "parent=")
		} else if strings.HasPrefix(arg, "start=") {
			traversalStart = strings.TrimPrefix(arg, "start=")
		} else if strings.HasPrefix(arg, "depth=") {
			depthStr := strings.TrimPrefix(arg, "depth=")
			if d, err := strconv.Atoi(depthStr); err == nil && d > 0 {
				traversalDepth = d
			}
		} else if strings.HasPrefix(arg, "include=") {
			val := strings.TrimPrefix(arg, "include=")
			val = strings.Trim(val, "[]")
			for _, c := range strings.Split(val, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					traversalInclude = append(traversalInclude, c)
				}
			}
		}
	}
	if traversalChild == "" || traversalParent == "" || traversalStart == "" {
		return fmt.Errorf("%s() requires child=, parent=, and start= parameters", cmd.Name)
	}
	if traversalDepth == 0 {
		traversalDepth = 10
	}
	if traversalDepth > 50 {
		traversalDepth = 50
	}
	ctx.Plan.IsTraversal = true
	ctx.Plan.TraversalMode = cmd.Name
	ctx.Plan.TraversalChild = traversalChild
	ctx.Plan.TraversalParent = traversalParent
	ctx.Plan.TraversalStart = traversalStart
	ctx.Plan.TraversalDepth = traversalDepth
	ctx.Plan.TraversalInclude = traversalInclude
	ctx.Registry.SetResolveExpr("_depth", "_depth")
	ctx.Registry.SetResolveExpr("_path", "_path")
	return nil
}

// analyzefieldsHandler handles analyzefields(field1, field2, limit=N)
type analyzefieldsHandler struct{}

func (h *analyzefieldsHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("field_name", FieldKindPerRow, "field_name", ctx.CmdIndex)
	ctx.Registry.Register("_events", FieldKindPerRow, "_events", ctx.CmdIndex)
	ctx.Registry.Register("_distinct_vals", FieldKindPerRow, "_distinct_vals", ctx.CmdIndex)
	ctx.Registry.Register("_mean", FieldKindPerRow, "_mean", ctx.CmdIndex)
	ctx.Registry.Register("_min", FieldKindPerRow, "_min", ctx.CmdIndex)
	ctx.Registry.Register("_max", FieldKindPerRow, "_max", ctx.CmdIndex)
	ctx.Registry.Register("_stdev", FieldKindPerRow, "_stdev", ctx.CmdIndex)
	return nil
}

func (h *analyzefieldsHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.IsAnalyze = true
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "limit=") {
			limitVal := strings.TrimPrefix(arg, "limit=")
			if strings.EqualFold(limitVal, "max") {
				ctx.Plan.AnalyzeFieldsScanLimit = 200000
			} else if n, err := strconv.Atoi(limitVal); err == nil && n > 0 {
				if n > 200000 {
					n = 200000
				}
				ctx.Plan.AnalyzeFieldsScanLimit = n
			}
		} else {
			ctx.Plan.AnalyzeFieldsList = append(ctx.Plan.AnalyzeFieldsList, arg)
		}
	}
	return nil
}

// chainHandler handles chain(fields, steps, within=5m)
type chainHandler struct{}

func (h *chainHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("chain_count", FieldKindAggregate, "chain_count", ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *chainHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) < 2 {
		return fmt.Errorf("chain() requires grouping field(s) and step definitions")
	}
	source := ctx.Plan.CurrentStage()

	chainFieldsStr := cmd.Arguments[0]
	blockBody := cmd.Arguments[1]
	var withinSeconds int
	if len(cmd.Arguments) >= 3 {
		withinSeconds = spanToSeconds(cmd.Arguments[2])
	}

	chainFields := strings.Split(chainFieldsStr, ",")
	for i, f := range chainFields {
		chainFields[i] = strings.TrimSpace(f)
	}

	steps, err := parseChainSteps(blockBody)
	if err != nil {
		return fmt.Errorf("chain(): %w", err)
	}
	if len(steps) < 2 {
		return fmt.Errorf("chain() requires at least 2 steps, got %d", len(steps))
	}

	// Build sequenceMatch pattern
	var pattern strings.Builder
	for i := range steps {
		if i > 0 && withinSeconds > 0 {
			pattern.WriteString(fmt.Sprintf("(?t<=%d)", withinSeconds))
		}
		pattern.WriteString(fmt.Sprintf("(?%d)", i+1))
	}
	patternStr := pattern.String()
	condArgs := strings.Join(steps, ", ")
	tsExpr := "toDateTime(timestamp)"

	// Multi-identity mode: when multiple fields are provided, they all represent
	// the same entity (e.g., user, source_user, target_user). We use arrayJoin
	// to expand each row into one row per non-empty identity field value, so an
	// event naturally lands in every entity group it belongs to.
	if len(chainFields) > 1 {
		var arrayElems []string
		for _, f := range chainFields {
			if f == "timestamp" || f == "raw_log" || f == "log_id" {
				arrayElems = append(arrayElems, f)
			} else {
				arrayElems = append(arrayElems, jsonFieldRef(f))
			}
		}
		entityExpr := fmt.Sprintf("arrayJoin(arrayFilter(x -> x != '', [%s]))", strings.Join(arrayElems, ", "))

		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("%s AS _entity", entityExpr),
		})
		source.Layer.GroupBy = append(source.Layer.GroupBy, "_entity")
		ctx.Registry.Register("_entity", FieldKindPerRow, "_entity", ctx.CmdIndex)
		ctx.Registry.SetResolveExpr("_entity", entityExpr)
	} else {
		// Single field: original behavior, GROUP BY that field directly.
		chainField := chainFields[0]
		safeField, err := sanitizeIdentifier(chainField)
		if err != nil {
			return fmt.Errorf("chain(): invalid field name: %w", err)
		}
		var fieldRef string
		if chainField == "timestamp" || chainField == "raw_log" || chainField == "log_id" {
			fieldRef = chainField
		} else {
			fieldRef = jsonFieldRef(chainField)
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", fieldRef, safeField)})
		source.Layer.GroupBy = append(source.Layer.GroupBy, fieldRef)
		ctx.Registry.SetResolveExpr(safeField, fieldRef)
	}

	// SELECT chain_count
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
		Expr: fmt.Sprintf("sequenceCount('%s')(%s, %s) AS chain_count", patternStr, tsExpr, condArgs),
	})

	// HAVING: only groups where the sequence matched
	source.Layer.Having = append(source.Layer.Having,
		fmt.Sprintf("sequenceMatch('%s')(%s, %s) = 1", patternStr, tsExpr, condArgs))

	ctx.Plan.IsAggregated = true
	ctx.Plan.IsChain = true
	ctx.Registry.SetResolveExpr("chain_count", fmt.Sprintf("sequenceCount('%s')(%s, %s)", patternStr, tsExpr, condArgs))
	source.Layer.OrderBy = append(source.Layer.OrderBy, "chain_count DESC")
	return nil
}

// heatmapHandler handles heatmap(x=field, y=field, value=count(), limit=N)
type heatmapHandler struct{}

func (h *heatmapHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *heatmapHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	var xField, yField, valueFunc string
	limit := 50
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "x=") {
			xField = strings.TrimPrefix(arg, "x=")
		} else if strings.HasPrefix(arg, "y=") {
			yField = strings.TrimPrefix(arg, "y=")
		} else if strings.HasPrefix(arg, "value=") {
			valueFunc = strings.TrimPrefix(arg, "value=")
		} else if strings.HasPrefix(arg, "limit=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && n > 0 {
				if n > 200 {
					n = 200
				}
				limit = n
			}
		} else if strings.Contains(arg, "(") {
			valueFunc = arg
		}
	}
	if xField == "" || yField == "" {
		return fmt.Errorf("heatmap() requires x= and y= parameters")
	}

	ctx.Plan.ChartType = "heatmap"
	ctx.Plan.ChartConfig["xField"] = xField
	ctx.Plan.ChartConfig["yField"] = yField
	ctx.Plan.ChartConfig["limit"] = limit

	source := ctx.Plan.CurrentStage()

	if len(source.Layer.GroupBy) > 0 || ctx.Plan.IsAggregated {
		// Visualization-only mode: data already aggregated
		return nil
	}

	// Standalone mode: heatmap does its own aggregation
	xRef := resolveFieldRef(xField, ctx.Registry)
	yRef := resolveFieldRef(yField, ctx.Registry)

	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: fmt.Sprintf("%s AS _heatmap_x", xRef)},
		SelectExpr{Expr: fmt.Sprintf("%s AS _heatmap_y", yRef)},
	)

	if valueFunc == "" || strings.Contains(valueFunc, "count()") {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _heatmap_value"})
	} else if strings.Contains(valueFunc, "sum(") {
		f := extractFunctionField(valueFunc, "sum")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("sum(toFloat64OrNull(%s)) AS _heatmap_value", jsonFieldRef(f)),
		})
	} else if strings.Contains(valueFunc, "avg(") {
		f := extractFunctionField(valueFunc, "avg")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("avg(toFloat64OrNull(%s)) AS _heatmap_value", jsonFieldRef(f)),
		})
	} else if strings.Contains(valueFunc, "max(") {
		f := extractFunctionField(valueFunc, "max")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("max(toFloat64OrNull(%s)) AS _heatmap_value", jsonFieldRef(f)),
		})
	} else if strings.Contains(valueFunc, "min(") {
		f := extractFunctionField(valueFunc, "min")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("min(toFloat64OrNull(%s)) AS _heatmap_value", jsonFieldRef(f)),
		})
	} else {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _heatmap_value"})
	}

	source.Layer.GroupBy = append(source.Layer.GroupBy, xRef, yRef)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "_heatmap_value DESC")
	ctx.Plan.IsAggregated = true
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", limit*limit)

	ctx.Registry.SetResolveExpr("_heatmap_x", xRef)
	ctx.Registry.SetResolveExpr("_heatmap_y", yRef)
	return nil
}

func init() {
	registerCommand(&tableHandler{}, "table")
	registerCommand(&bfsHandler{}, "bfs", "dfs")
	registerCommand(&analyzefieldsHandler{}, "analyzefields")
	registerCommand(&chainHandler{}, "chain")
	registerCommand(&heatmapHandler{}, "heatmap")
}

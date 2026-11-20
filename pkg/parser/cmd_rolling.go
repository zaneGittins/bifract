package parser

import (
	"fmt"
	"slices"
	"strings"
)

// rollingHandler handles rolling(keys, function=[...]): running values per group
// in time order, the equivalent of SPL streamstats. Every event is kept and gains
// columns computed from the events of its group up to and including it.
//
// The columns are a window over the scan, rendered as a row cap (see capRows):
// filters before rolling() select the rows it reads, and everything after it
// reads its output. A window sorts each group, so its cost grows with the rows it
// reads; a selective filter belongs in front of it. On a Distributed table the
// shards return the filtered rows and the initiator orders each group.
type rollingHandler struct{}

// rollingAggregates are the aggregates rolling() computes as running values.
var rollingAggregates = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}

func (h *rollingHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	specs, err := rollingFunctions(cmd)
	if err != nil {
		return nil
	}
	for _, spec := range specs {
		if name, err := rollingOutputName(spec); err == nil {
			ctx.Registry.Register(unquoteIdent(name), FieldKindPerRow, name, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *rollingHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if !onScan(ctx) {
		return fmt.Errorf("rolling() reads events in time order, so it must come before any aggregation")
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	specs, err := rollingFunctions(cmd)
	if err != nil {
		return err
	}
	readsFrom := ctx.Registry.mark()
	w := &rollingWindow{reads: ctx.Registry.rec}
	types := map[string]valueType{}
	got, err := ctx.Registry.track(func() error {
		for _, key := range b.Flat("fields") {
			r, err := resolveArgRef(key, ctx.Registry)
			if err != nil {
				return fmt.Errorf("rolling(): %w", err)
			}
			w.partition = append(w.partition, r.sql)
		}
		for _, spec := range specs {
			col, typ, err := rollingColumn(spec, ctx.Registry)
			if err != nil {
				return err
			}
			if slices.ContainsFunc(w.cols, func(s SelectExpr) bool { return s.Name() == col.Name() }) {
				return fmt.Errorf("rolling(): two functions produce the column %s; name one with as=", col.Name())
			}
			w.cols = append(w.cols, col)
			types[col.Name()] = typ
		}
		return nil
	})
	if err != nil {
		return err
	}
	w.deps = got.deps()
	if w.deps.wrap {
		return fmt.Errorf("rolling(): it reads a column join() adds, which exists only after the join; use rolling() before the join")
	}

	layer := &ctx.Plan.SourceStage().Layer
	layer.Caps = append(layer.Caps, RowCap{At: ctx.CmdIndex, window: w})
	for _, col := range w.cols {
		name := col.Name()
		layer.project(SelectExpr{Expr: col.Alias})
		ctx.Registry.Register(name, FieldKindPerRow, col.Alias, ctx.CmdIndex)
		ctx.Registry.define(name, col.Alias, readsFrom)
		ctx.Registry.setType(name, types[name])
	}
	return nil
}

// rollingFunctions reads function=: one function or a list, count() without it.
func rollingFunctions(cmd CommandNode) ([]*AggSpec, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil, err
	}
	a, ok := b.First("function")
	if !ok {
		return []*AggSpec{{Name: "count"}}, nil
	}
	items := []Argument{a}
	if a.Kind == ArgList {
		items = a.List
	}
	var specs []*AggSpec
	for _, item := range items {
		spec, ok := asAggSpec(item)
		if !ok {
			return nil, fmt.Errorf("rolling(): function= takes functions such as previous(timestamp) or count(), got %s", item)
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// rollingOutputName is the column a function projects: as= or _<name>. It may
// not replace a column every event has.
func rollingOutputName(spec *AggSpec) (string, error) {
	name := strings.ToLower(spec.Name)
	alias, err := aggregateAlias(readAggOptions(spec).alias, name)
	if err != nil {
		return "", fmt.Errorf("rolling(): %w", err)
	}
	if scanColumns[unquoteIdent(alias)] || unquoteIdent(alias) == "raw_log" {
		return "", fmt.Errorf("rolling(): %s() would replace %s, which every event has; name it with as=", name, unquoteIdent(alias))
	}
	return alias, nil
}

// rollingColumn renders one function as a column over the rolling window, with
// the type of its value.
func rollingColumn(spec *AggSpec, registry *FieldRegistry) (SelectExpr, valueType, error) {
	name := strings.ToLower(spec.Name)
	alias, err := rollingOutputName(spec)
	if err != nil {
		return SelectExpr{}, typeUnknown, err
	}
	over := " OVER " + rollingWindowName
	if name == "previous" {
		opts := readAggOptions(spec)
		if !opts.hasField {
			return SelectExpr{}, typeUnknown, fmt.Errorf("rolling(): previous() needs a field, e.g. previous(timestamp)")
		}
		r, err := resolveArgRef(opts.operand, registry)
		if err != nil {
			return SelectExpr{}, typeUnknown, fmt.Errorf("rolling(): previous(): %w", err)
		}
		typ := r.typ
		if f := opts.operand.FieldName(); f != "" && isTimeField(f, registry) {
			typ = typeTime
		}
		// The first event of a group has no previous one. lagInFrame returns the
		// type's default there (1970-01-01, 0), so anything but a String is made
		// Nullable to return NULL; a String's default is '', which is empty.
		if typ == typeText {
			return selectAs("lagInFrame("+r.sql+")"+over, alias), typeText, nil
		}
		return selectAs("lagInFrame(toNullable("+r.sql+"))"+over, alias), typ, nil
	}
	if !rollingAggregates[name] {
		return SelectExpr{}, typeUnknown, fmt.Errorf("rolling(): %s() has no running form; use previous(), count(), sum(), avg(), min() or max()", name)
	}
	var selects []SelectExpr
	if _, err := processAggSpec(spec, &selects, map[string]bool{}, registry); err != nil {
		return SelectExpr{}, typeUnknown, fmt.Errorf("rolling(): %w", err)
	}
	if len(selects) != 1 || selects[0].agg == nil {
		return SelectExpr{}, typeUnknown, fmt.Errorf("rolling(): %s() has no running form", name)
	}
	return selectAs(selects[0].Expr+over, alias), aggType(selects[0].agg), nil
}

// rollingOnlyError is the error for previous() outside rolling(): without an
// order there is no previous event.
func rollingOnlyError(name string) error {
	if strings.EqualFold(name, "previous") {
		return fmt.Errorf("previous() is only valid inside rolling(), which orders each group's events by time, e.g. rolling(user, function=previous(timestamp))")
	}
	return nil
}

func init() {
	registerCommand(&rollingHandler{}, "rolling")
	registerSpec(&CommandSpec{Name: "rolling", Params: []ParamSpec{fields("fields"), namedAgg("function")}})
}

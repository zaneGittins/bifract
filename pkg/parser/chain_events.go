package parser

import (
	"fmt"
	"sort"
	"strings"
)

// ChainMeta describes how to fetch the events behind a chain() match. chain()
// aggregates, so its rows carry only the entity and a count; the anchors in
// chainAnchorColumn identify the events that formed each matched sequence.
type ChainMeta struct {
	// EntityColumn is the result column holding the grouped identity.
	EntityColumn string
	// EntityExpr is the SQL that produced it, for the fetch predicate.
	EntityExpr string
	// AnchorColumn is the result column holding the matched timestamps.
	AnchorColumn string
	// StepConditions is the per-step SQL, in step order.
	StepConditions []string
	// StepFields are the fields the steps filter on, in first-seen order: what the
	// query is about, so clients lead their event summaries with these.
	StepFields []string
	// MultiIdentity is set when chain() grouped several fields into one entity.
	// arrayJoin produced the value, so it cannot be matched with a plain
	// equality and the fetch falls back to the anchors alone.
	MultiIdentity bool
}

// WindowContractor is implemented by commands that correlate across events. A
// windowed evaluator reads the contract to widen its range and then discard rows
// an earlier window already reported.
type WindowContractor interface {
	// WindowContract returns how much extra history the command needs and the
	// column marking when a row's evidence completed, both zero when unbounded.
	WindowContract(cmd CommandNode) (lookbackSeconds int, completionColumn string)
}

// QueryWindowContract returns the windowed-evaluation contract a pipeline
// declares. It inspects command handlers directly, so unlike a translation it
// cannot fail on options the caller does not have.
func QueryWindowContract(pipeline *PipelineNode) (int, string) {
	if pipeline == nil {
		return 0, ""
	}
	for _, cmd := range pipeline.Commands() {
		if wc, ok := getCommandHandler(cmd.Name).(WindowContractor); ok {
			return wc.WindowContract(cmd)
		}
	}
	return 0, ""
}

// maxChainFetchEntities caps how many matched entities one fetch resolves. The
// cost of the fetch is bounded by the result page rather than by how many
// entities matched, so this only guards a pathologically large page.
const maxChainFetchEntities = 500

// ChainEventFetch is a bounded request for the events behind a page of chain rows.
type ChainEventFetch struct {
	SQL string
	// Truncated reports that entities were dropped to stay within the cap, so
	// the caller can say so rather than presenting partial evidence as complete.
	Truncated int
}

// BuildChainEventsSQL builds the second pass that resolves chain() anchors into
// real log rows. Both predicates matter: timestamp is the leading sorting key and
// does the granule pruning, while the step conditions discard same-millisecond
// events that were never part of a sequence.
//
// entities and anchors are parallel: anchors[i] holds the matched timestamps
// (unix milliseconds) for entities[i]. Returns nil when there is nothing to fetch.
func BuildChainEventsSQL(meta *ChainMeta, entities []string, anchors [][]int64, opts QueryOptions) *ChainEventFetch {
	if meta == nil || len(anchors) == 0 {
		return nil
	}

	truncated := 0
	if len(anchors) > maxChainFetchEntities {
		truncated = len(anchors) - maxChainFetchEntities
		anchors = anchors[:maxChainFetchEntities]
		if len(entities) > maxChainFetchEntities {
			entities = entities[:maxChainFetchEntities]
		}
	}

	// Deduplicate: entities commonly share a step timestamp, and a repeated
	// literal only widens the IN list without selecting another granule.
	tsSeen := make(map[int64]bool)
	var tsList []int64
	for _, row := range anchors {
		for _, ts := range row {
			if ts > 0 && !tsSeen[ts] {
				tsSeen[ts] = true
				tsList = append(tsList, ts)
			}
		}
	}
	if len(tsList) == 0 {
		return nil
	}
	sort.Slice(tsList, func(i, j int) bool { return tsList[i] < tsList[j] })

	tsLits := make([]string, len(tsList))
	for i, ts := range tsList {
		tsLits[i] = fmt.Sprintf("fromUnixTimestamp64Milli(toInt64(%d))", ts)
	}

	table := opts.TableName
	if table == "" {
		table = "logs"
	}

	where := []string{fmt.Sprintf("timestamp IN (%s)", strings.Join(tsLits, ", "))}
	if scope := fractalScopeCond(opts, ""); scope != "" {
		where = append(where, scope)
	}
	// The anchors are event timestamps, which only prune on a table sorted by them.
	// logs_hot is sorted and partitioned by ingest_timestamp, so also bound the read
	// by the query's own window in its own basis, or the fetch scans the whole table.
	if !opts.StartTime.IsZero() && !opts.EndTime.IsZero() {
		where = append(where, windowConditions(opts)...)
	}
	// Narrow to the entities on this page. arrayJoin'd multi-identity values have
	// no single source column to compare, so those fall back to anchors only.
	if !meta.MultiIdentity && meta.EntityExpr != "" && len(entities) > 0 {
		lits := make([]string, 0, len(entities))
		seen := make(map[string]bool)
		for _, e := range entities {
			if e == "" || seen[e] {
				continue
			}
			seen[e] = true
			lits = append(lits, sqlString(e))
		}
		if len(lits) > 0 {
			where = append(where, fmt.Sprintf("%s IN (%s)", meta.EntityExpr, strings.Join(lits, ", ")))
		}
	}
	if len(meta.StepConditions) > 0 {
		where = append(where, "("+strings.Join(meta.StepConditions, " OR ")+")")
	}

	entitySelect := "''"
	if meta.EntityExpr != "" && !meta.MultiIdentity {
		entitySelect = meta.EntityExpr
	}

	// No `toString(timestamp) AS timestamp` here: ClickHouse alias scope is global
	// within the SELECT, so that alias shadows the real column and the millisecond
	// conversion is handed a String (code 43). The caller formats from _ts_ms.
	sql := fmt.Sprintf(
		"SELECT %s AS _entity_key, toUnixTimestamp64Milli(timestamp) AS _ts_ms, log_id, %s "+
			"FROM %s WHERE %s ORDER BY timestamp",
		entitySelect, normLogColumn, table, andOf(atoms(where...)...).sql)

	return &ChainEventFetch{SQL: sql, Truncated: truncated}
}

// ForEachCommand visits every command in a pipeline, including condition
// functions used as boolean operands and the commands inside a chain() step, a
// case branch, a join() subquery and a let binding. Anything looking for a command
// by name must use this rather than Commands, which holds only the standalone ones.
func ForEachCommand(pipeline *PipelineNode, fn func(CommandNode)) {
	walkCommands(pipeline, nil, func(cmd CommandNode, _ *BindingNode) { fn(cmd) })
}

// walkCommands is ForEachCommand that also passes the let binding a command sits
// in, nil outside one.
func walkCommands(pipeline *PipelineNode, binding *BindingNode, fn func(CommandNode, *BindingNode)) {
	if pipeline == nil {
		return
	}
	visit := func(cmd CommandNode) {
		fn(cmd, binding)
		for _, sub := range nestedPipelines(cmd) {
			walkCommands(sub, binding, fn)
		}
	}
	for _, st := range pipeline.Steps {
		switch {
		case st.Command != nil:
			visit(*st.Command)
		case st.Filter != nil:
			forEachConditionCommand(*st.Filter, visit)
		}
	}
	for _, b := range pipeline.Bindings {
		walkCommands(b.Pipe, b, fn)
	}
}

// ForEachCondition visits every node of every filter in a pipeline, each AND/OR
// before its operands, including the filters inside nested pipelines and let
// bindings.
func ForEachCondition(pipeline *PipelineNode, fn func(Condition)) {
	if pipeline == nil {
		return
	}
	for _, f := range pipeline.Filters() {
		f.Walk(fn)
	}
	ForEachCommand(pipeline, func(cmd CommandNode) {
		for _, sub := range nestedPipelines(cmd) {
			for _, f := range sub.Filters() {
				f.Walk(fn)
			}
		}
	})
	for _, b := range pipeline.Bindings {
		ForEachCondition(b.Pipe, fn)
	}
}

// Walk visits c and every condition beneath it, each AND/OR before its operands.
func (c Condition) Walk(fn func(Condition)) {
	fn(c)
	for _, child := range c.Children {
		child.Walk(fn)
	}
}

// forEachConditionCommand visits the condition commands in a filter.
func forEachConditionCommand(c Condition, fn func(CommandNode)) {
	c.Walk(func(n Condition) {
		if n.Command != nil {
			fn(*n.Command)
		}
	})
}

// validateCommands runs check on every command, naming the binding a failure is in.
func validateCommands(pipeline *PipelineNode, check func(CommandNode) error) error {
	var err error
	walkCommands(pipeline, nil, func(cmd CommandNode, b *BindingNode) {
		if err != nil {
			return
		}
		if err = check(cmd); err != nil && b != nil {
			err = fmt.Errorf("%s: %w", b.Name, err)
		}
	})
	return err
}

// nestedPipelines returns the pipelines a command carries in its block.
func nestedPipelines(cmd CommandNode) []*PipelineNode {
	var out []*PipelineNode
	if cmd.Body != nil {
		out = append(out, cmd.Body)
	}
	out = append(out, cmd.Sequence...)
	for _, br := range cmd.Branches {
		if br.When != nil {
			out = append(out, br.When)
		}
		for _, r := range br.Results {
			if r.Command != nil {
				out = append(out, r.Command)
			}
		}
	}
	return out
}

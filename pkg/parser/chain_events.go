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
	for _, cmd := range pipeline.Commands {
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
	if scope := fractalScopePredicate(opts); scope != "" {
		where = append(where, scope)
	}
	// The anchors are event timestamps, which only prune on a table sorted by them.
	// logs_hot is sorted and partitioned by ingest_timestamp, so also bound the read
	// by the query's own window in its own basis, or the fetch scans the whole table.
	if !opts.StartTime.IsZero() && !opts.EndTime.IsZero() {
		col := "timestamp"
		if opts.UseIngestTimestamp {
			col = "ingest_timestamp"
		}
		where = append(where, fmt.Sprintf("%s >= '%s' AND %s <= '%s'",
			col, chTimeLiteral(opts.StartTime), col, chTimeLiteral(opts.EndTime)))
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
			lits = append(lits, "'"+escapeString(e)+"'")
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
		entitySelect, normLogColumn, table, strings.Join(where, " AND "))

	return &ChainEventFetch{SQL: sql, Truncated: truncated}
}

// fractalScopePredicate mirrors the source scan's fractal scoping so the fetch
// cannot read across fractals.
func fractalScopePredicate(opts QueryOptions) string {
	if len(opts.FractalIDs) > 0 {
		quoted := make([]string, len(opts.FractalIDs))
		for i, id := range opts.FractalIDs {
			quoted[i] = "'" + escapeString(id) + "'"
		}
		return "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
	}
	if opts.FractalID != "" {
		if opts.IncludeEmptyFractalID {
			return fmt.Sprintf("fractal_id IN ('%s', '')", escapeString(opts.FractalID))
		}
		return fmt.Sprintf("fractal_id = '%s'", escapeString(opts.FractalID))
	}
	return ""
}

// collectConditionFieldsOrdered is collectConditionFields preserving first-seen
// order, which the map-returning variant loses.
func collectConditionFieldsOrdered(conds []ConditionNode, seen map[string]bool, out []string) []string {
	for _, c := range conds {
		if c.IsCompound {
			out = collectConditionFieldsOrdered(c.Children, seen, out)
			continue
		}
		// Bare-term searches match norm_log as a whole and name no field.
		if c.Field == "" || c.Field == normLogColumn || seen[c.Field] {
			continue
		}
		seen[c.Field] = true
		out = append(out, c.Field)
	}
	return out
}

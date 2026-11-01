package parser

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// modelLookupHandler handles: model_lookup(model="name", key=[field1, field2])
//
// It joins the model's scoring subquery, one row per scored key, onto the rows it
// reads and adds the model's output columns (modelOutputColumns). With no
// aggregation before it the join sits in the log scan's FROM (see
// QueryPlan.ModelLookupAtScan); after one it joins the aggregated rows, as the layer
// between that stage and the next (see modelJoinLayer). Either way, every command
// after it reads the model's columns as ordinary columns.
type modelLookupHandler struct{}

func (h *modelLookupHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if ctx.AsCondition {
		// A condition tests the key and adds no columns.
		return nil
	}
	modelName, _, _, err := parseModelLookupArgs(cmd)
	if err != nil {
		return err
	}

	info, ok := ctx.Opts.Models[modelName]
	if !ok {
		// Model not found: register every model column as a placeholder so downstream
		// conditions don't fail during Declare. Execute returns the real error.
		for _, t := range slices.Sorted(maps.Keys(modelColumns)) {
			for _, n := range modelColumns[t] {
				ctx.Registry.Register(n, FieldKindPerRow, "NULL", ctx.CmdIndex)
			}
		}
		return nil
	}

	// Register each output field as a model-lookup column: it exists only after the
	// JOIN, so it resolves to its bare output name, and anything reading it is
	// placed after the join (see FieldRegistry.DependsOnJoin).
	if info.ModelType == "rarity" && conditionsReference(ctx.Pipeline.HavingConditions, "confidence") {
		// confidence was the old name for coverage. A filter on it would read an
		// absent log field and silently match nothing, so name the rename instead.
		return fmt.Errorf("modelLookup(): rarity models report coverage, not confidence; filter on coverage instead")
	}
	names := modelColumns[info.ModelType]
	for _, n := range names {
		ctx.Registry.Register(n, FieldKindJoined, n, ctx.CmdIndex)
	}
	ctx.Plan.ModelLookupOutputs = append([]string(nil), names...)
	return nil
}

// modelColumns lists the columns each model type's scoring subquery adds, in the
// order they are shown.
var modelColumns = map[string][]string{
	"rarity":          {"model_count", "model_total", "percent", "coverage"},
	"first_seen":      {"first_seen", "last_seen", "event_count", "is_new"},
	"volume_baseline": {"latest_count", "baseline_median", "mad", "n_buckets", "latest_bucket", "z_score"},
	// beacon_score is the final verdict; the rest is the breakdown ("why").
	"beacon":          {"beacon_score", "regularity_score", "ts_score", "ds_score", "dur_score", "hist_score", "prevalence", "prevalence_score", "first_seen_score", "threat_intel_score", "conn_count"},
	"long_connection": {"longconn_score", "total_duration", "conn_count", "prevalence", "prevalence_score", "first_seen_score", "threat_intel_score"},
}

func (h *modelLookupHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if ctx.AsCondition {
		return h.executeCondition(cmd, ctx)
	}
	if ctx.Plan.IsJoin {
		return fmt.Errorf("modelLookup() cannot be combined with join()")
	}
	if ctx.Plan.hasModelLookup() {
		return fmt.Errorf("modelLookup() cannot be used more than once")
	}

	_, keyFields, strict, err := parseModelLookupArgs(cmd)
	if err != nil {
		return err
	}
	ks, err := resolveModelKeySet(cmd, ctx)
	if err != nil {
		return err
	}

	ctx.Plan.ModelLookupFields = ks.fields
	ctx.Plan.modelKeys = ks
	ctx.Plan.ModelLookupCmd = ctx.CmdIndex
	ctx.Plan.ModelLookupStrict = strict
	keyExprs := make([]string, len(keyFields))
	for i, kf := range keyFields {
		keyExprs[i] = modelLookupFieldRef(kf, ctx.Registry)
	}
	if ctx.Plan.ModelLookupAtScan {
		ctx.Plan.ModelLookupKeyExprs = keyExprs
		return nil
	}
	return joinModelAfterAggregation(ctx, keyFields, keyExprs)
}

// joinModelAfterAggregation joins the model onto aggregated rows: the stage after
// the aggregation reads (aggregated rows JOIN model) on the group columns the keys
// name, and carries the model's columns on like any column of its own.
func joinModelAfterAggregation(ctx *CommandContext, keyFields, keyExprs []string) error {
	plan := ctx.Plan
	// The strict prefilter can still narrow the scan when the stage the lookup reads
	// aggregates the scan, groups by every key and keeps every group: a group whose
	// key the model does not score is dropped by the join, so its rows need not be
	// read. A row cap or group limit first would select from the narrowed rows.
	src := plan.SourceStage().Layer
	enrichesScan := len(plan.Stages) == 1 && len(src.Caps) == 0 && src.Limit == "" && src.LimitBy == "" && src.ScanLimitBy == ""
	carried, err := pushCarryForwardStage(ctx)
	if err != nil {
		return fmt.Errorf("modelLookup(): %w", err)
	}
	if enrichesScan && len(plan.keysLostToAggregation(plan.SourceStage(), keyExprs, keyFields)) == 0 {
		plan.ModelLookupKeyExprs = keyExprs
	}
	left := make([]string, len(keyFields))
	for i, kf := range keyFields {
		if !slices.Contains(carried, kf) {
			return fmt.Errorf("modelLookup() key %[1]s must be part of the grouping to survive aggregation: add %[1]s to the group/table columns, or drop the aggregation", kf)
		}
		left[i] = modelKeyString("_outer." + quoteIdent(kf))
	}
	stage := plan.CurrentStage()
	for _, f := range plan.ModelLookupFields {
		if slices.Contains(carried, f) {
			return fmt.Errorf("modelLookup(): the aggregated rows already have a column %q, which the model adds", f)
		}
		stage.Layer.Selects = append(stage.Layer.Selects, SelectExpr{Expr: f})
		ctx.Registry.Register(f, FieldKindAssignment, f, ctx.CmdIndex)
		ctx.Registry.SetResolveExpr(f, f)
	}
	stage.Layer.ModelJoin = plan.modelJoinOn(left)
	stage.Layer.ModelJoinFields = plan.ModelLookupFields
	return nil
}

// executeCondition compiles modelLookup() used as a row condition: the log's key is
// one the model scores, exactly the rows the default join keeps. NOT around it keeps
// the rows the model does not score.
func (h *modelLookupHandler) executeCondition(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	for _, name := range []string{"require", "strict"} {
		if b.Has(name) {
			return fmt.Errorf("modelLookup() as a condition only tests whether the model scores the key, so %s= has no effect; drop it", name)
		}
	}
	ks, err := resolveModelKeySet(cmd, ctx)
	if err != nil {
		return err
	}
	keys := make([]string, len(ks.keyFields))
	for i, kf := range ks.keyFields {
		keys[i] = modelLookupFieldRef(kf, ctx.Registry)
	}
	ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where, ks.membership(keys, nil))
	return nil
}

// modelKeySet is what a model scores: its scoring subquery and the key set that
// subquery returns, which the join, its scan prefilter and the condition form all
// test a log's key against.
type modelKeySet struct {
	keyFields  []string // the log fields matched against the model key, in order
	table      string   // the model table
	fractalIDs []string
	scoringSQL string   // one row per scored key, carrying the output columns
	fields     []string // the output columns the join adds, in display order
	rightCols  []string // the scoring subquery's key columns, aligned with keyFields
	// rawWhere narrows the raw key columns the way the scoring subquery narrows its
	// rows; exact says the two key sets are then identical, so membership can read
	// the raw columns. exact starts false so a model type that forgets it falls back
	// to the scored set, which is always right.
	rawWhere string
	exact    bool
	global   bool // the model table is Distributed: emit GLOBAL IN/JOIN
	// firstSeen holds a first_seen model's scoring inputs, nil for other types.
	firstSeen *firstSeenScoring
}

// resolveModelKeySet validates a modelLookup() against its model and builds the
// model's key set.
func resolveModelKeySet(cmd CommandNode, ctx *CommandContext) (*modelKeySet, error) {
	modelName, keyFields, _, err := parseModelLookupArgs(cmd)
	if err != nil {
		return nil, err
	}
	if len(keyFields) == 0 {
		return nil, fmt.Errorf("modelLookup() requires key=[field1, ...] parameter")
	}
	info, ok := ctx.Opts.Models[modelName]
	if !ok {
		return nil, fmt.Errorf("model %q not found: create it in the Models UI first", modelName)
	}

	// In prism context FractalIDs holds every member fractal: a model's table can
	// hold rows sourced from any member's logs (the model's MV scans all fractals
	// matching its filter), so scoring must scan every member, not just the one
	// the model happens to be registered under.
	fractalIDs := ctx.Opts.FractalIDs
	if len(fractalIDs) == 0 {
		fractalIDs = []string{ctx.Opts.FractalID}
	}
	ks := &modelKeySet{keyFields: keyFields, table: info.TableName, fractalIDs: fractalIDs, global: info.Distributed,
		fields: modelColumns[info.ModelType]}

	switch info.ModelType {
	case "rarity":
		if len(keyFields) != 2 {
			return nil, fmt.Errorf("modelLookup() for rarity models requires exactly 2 key fields: [partition_key, value_key]")
		}
		ks.scoringSQL = RarityScoredSQL("`"+info.TableName+"`",
			fractalIDInClause(fractalIDs), info.MinSample, info.HistoryDays, false)
		ks.rightCols = []string{"partition_val", "value_val"}
		ks.rawWhere = HistoryPredicate(info.HistoryDays, modelPrefilterAlias+".")
		// Partitions still learning are scored away, so the raw key set is a superset.
		ks.exact = info.MinSample <= 1

	case "first_seen":
		ks.firstSeen = &firstSeenScoring{historyDays: info.HistoryDays, newSince: ctx.Opts.ModelNewSince}
		ks.scoringSQL = ks.firstSeenScoringSQL(false)
		ks.rightCols = []string{"entity_key"}
		// A key whose days have all left the window is no longer in the model.
		ks.rawWhere = HistoryPredicate(info.HistoryDays, modelPrefilterAlias+".")
		ks.exact = true // the scoring subquery drops no key

	case "volume_baseline":
		ks.scoringSQL = VolumeScoredSQL("`"+info.TableName+"`", fractalIDInClause(fractalIDs), info.TimeBucket, info.MinSample, "", false)
		ks.rightCols = []string{"entity_val"}
		ks.rawWhere = volumeScoredKeysPredicate(modelPrefilterAlias, info.TimeBucket, info.MinSample)
		ks.exact = true // the predicate admits exactly the entities with enough history

	case "beacon", "long_connection":
		if len(keyFields) != 3 {
			return nil, fmt.Errorf("modelLookup() for %s models requires exactly 3 key fields: [src_ip, dst_ip, dst_port]", info.ModelType)
		}
		if info.ModelType == "beacon" {
			ks.scoringSQL = buildBeaconScoringSQL(info.TableName, fractalIDs)
		} else {
			ks.scoringSQL = buildLongConnScoringSQL(info.TableName, fractalIDs)
		}
		// Positional key mapping: key[0]->src_ip, key[1]->dst_ip, key[2]->dst_port.
		ks.rightCols = []string{"src_ip", "dst_ip", "dst_port"}
		ks.exact = true // every scored pair is returned

	default:
		return nil, fmt.Errorf("unknown model type %q for model %q", info.ModelType, modelName)
	}
	return ks, nil
}

// membership is the predicate "the model scores this key, and its scored row
// passes modelWhere": exactly the keys the join matches. It reads the raw key
// columns where they hold exactly that set, and the scored set otherwise.
func (k *modelKeySet) membership(keys, modelWhere []string) string {
	var cols, set string
	if k.exact && len(modelWhere) == 0 {
		qualified := make([]string, len(k.rightCols))
		for i, c := range k.rightCols {
			qualified[i] = modelPrefilterAlias + "." + c
		}
		cols = k.keyColumns(qualified)
		where := modelPrefilterAlias + "." + fractalIDInClause(k.fractalIDs)
		if k.rawWhere != "" {
			where += " AND " + k.rawWhere
		}
		set = fmt.Sprintf("SELECT %s FROM `%s` AS %s WHERE %s", cols, k.table, modelPrefilterAlias, where)
	} else {
		set = fmt.Sprintf("SELECT %s FROM (%s)", k.keyColumns(k.rightCols), k.scoringSQL)
		if len(modelWhere) > 0 {
			set += " WHERE " + andJoin(modelWhere)
		}
	}

	// Both sides mirror the JOIN ON exactly (see QueryPlan.modelJoinOn).
	lhs := keys[0]
	switch {
	case !modelKeysAligned(keys, k.rightCols):
		lhs = concatModelKeys(keys)
	case len(keys) > 1:
		lhs = "(" + strings.Join(keys, ", ") + ")"
	}
	return fmt.Sprintf("%s %s (%s)", lhs, k.inOp(), set)
}

// inOp is the IN operator for a subquery over the model table.
func (k *modelKeySet) inOp() string {
	if k.global {
		return "GLOBAL IN"
	}
	return "IN"
}

// keyColumns renders the model-side key columns the way the log side compares them.
func (k *modelKeySet) keyColumns(cols []string) string {
	if !modelKeysAligned(k.keyFields, k.rightCols) {
		return concatModelKeys(cols)
	}
	return strings.Join(cols, ", ")
}

// modelPrefilterAlias names the model table inside the strict prefilter subquery. That
// subquery sits in the log scan's WHERE, the one position where an unqualified column
// could bind to the outer scan instead (both tables have fractal_id); qualifying every
// reference keeps it uncorrelated whatever columns `logs` grows.
const modelPrefilterAlias = "_mlk_src"

// modelKeysAligned reports whether each key field maps to its own model key column.
// When it does not, several key fields are matched against a single column holding the
// model's char(30) composite encoding (how first_seen and volume_baseline store a
// multi-field entity). The JOIN ON and the strict prefilter must agree on this, so
// both ask here rather than testing the lengths themselves.
func modelKeysAligned(keys, rightCols []string) bool {
	return len(keys) == len(rightCols)
}

// concatModelKeys joins key parts with the char(30) separator used throughout the
// model composite-key encoding. A single part needs no concat.
func concatModelKeys(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return "concat(" + strings.Join(parts, ", char(30), ") + ")"
}

// fractalIDInClause renders a `fractal_id IN (...)` predicate over one or more
// fractal IDs, each individually escaped.
func fractalIDInClause(fractalIDs []string) string {
	quoted := make([]string, len(fractalIDs))
	for i, id := range fractalIDs {
		quoted[i] = "'" + escapeString(id) + "'"
	}
	return "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
}

// buildBeaconScoringSQL returns the latest scored row per pair from a beacon model's
// results table. beacon_score is the final (modifier-adjusted) verdict; the subscores
// explain it.
//
// argMax(..., scored_at) rather than FINAL: same newest-scored row, and it guarantees
// ONE row per pair where FINAL cannot. FINAL collapses per shard (a later scoring pass
// can land on another one) and never across the member fractals of a prism, either of
// which would multiply log rows through the join.
func buildBeaconScoringSQL(tableName string, fractalIDs []string) string {
	return fmt.Sprintf(`SELECT src_ip, dst_ip, dst_port,
    argMax(final_score, scored_at) AS beacon_score,
    argMax(regularity_score, scored_at) AS regularity_score,
    argMax(ts_score, scored_at) AS ts_score,
    argMax(ds_score, scored_at) AS ds_score,
    argMax(dur_score, scored_at) AS dur_score,
    argMax(hist_score, scored_at) AS hist_score,
    argMax(prevalence, scored_at) AS prevalence,
    argMax(prevalence_score, scored_at) AS prevalence_score,
    argMax(first_seen_score, scored_at) AS first_seen_score,
    argMax(threat_intel_score, scored_at) AS threat_intel_score,
    argMax(conn_count, scored_at) AS conn_count
FROM %s
WHERE %s
GROUP BY src_ip, dst_ip, dst_port`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
	)
}

// buildLongConnScoringSQL returns the latest scored row per pair from a
// long_connection model's results table. One row per pair, for the reasons given on
// buildBeaconScoringSQL.
func buildLongConnScoringSQL(tableName string, fractalIDs []string) string {
	return fmt.Sprintf(`SELECT src_ip, dst_ip, dst_port,
    argMax(final_score, scored_at) AS longconn_score,
    argMax(total_duration, scored_at) AS total_duration,
    argMax(conn_count, scored_at) AS conn_count,
    argMax(prevalence, scored_at) AS prevalence,
    argMax(prevalence_score, scored_at) AS prevalence_score,
    argMax(first_seen_score, scored_at) AS first_seen_score,
    argMax(threat_intel_score, scored_at) AS threat_intel_score
FROM %s
WHERE %s
GROUP BY src_ip, dst_ip, dst_port`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
	)
}

// RarityScoredSQL scores every (partition, value) pair of a rarity model, the one
// definition the data view, preview, alert counts and modelLookup() all read.
// Counts are days: model_count the value's, model_total its partition's, percent
// their ratio, and coverage is Good-Turing coverage, 1 - (values seen on one day /
// total value-days).
//
// scope is a WHERE predicate on source; minHistory is the partition's learning
// period in days; historyDays reads a windowed table over its window. withDays
// adds the sorted day list and since, the pair's first day (its return, for a
// windowed model). The trailing WHERE lets callers append AND clauses.
func RarityScoredSQL(source, scope string, minHistory, historyDays int, withDays bool) string {
	if minHistory < 1 {
		minHistory = 1
	}
	daysCol, sinceMid, sinceInner := "", "", ""
	if withDays {
		daysCol = ",\n    seen_days AS days,\n    since"
		sinceMid = ", since"
	}
	var inner string
	if historyDays > 0 {
		if withDays {
			sinceInner = ",\n            lookback_days[" + historyReturnIndex("lookback_days", "", historyDays) + "] AS since"
		}
		inner = fmt.Sprintf(`        SELECT partition_val, value_val,
            arraySort(groupUniqArray(%[1]s)) AS lookback_days,
            arrayFilter(d -> d > %[2]s, lookback_days) AS seen_days,
            length(seen_days) AS days_seen%[3]s
        FROM %[4]s
        WHERE %[5]s AND %[6]s
        GROUP BY partition_val, value_val
        HAVING days_seen > 0`, HistoryDayColumn, historyWindowStart(historyDays), sinceInner,
			source, scope, historyLookbackPredicate(historyDays))
	} else {
		if withDays {
			sinceInner = ",\n            seen_days[1] AS since"
		}
		inner = fmt.Sprintf(`        SELECT partition_val, value_val,
            arraySort(groupUniqArrayMerge(365)(days)) AS seen_days,
            length(seen_days) AS days_seen%s
        FROM %s
        WHERE %s
        GROUP BY partition_val, value_val`, sinceInner, source, scope)
	}
	return fmt.Sprintf(`SELECT partition_val, value_val,
    days_seen AS model_count,
    days_observed AS model_total,
    round(days_seen / days_observed * 100.0, 4) AS percent,
    round(1 - singletons / value_days, 4) AS coverage%s
FROM (
    SELECT partition_val, value_val, seen_days, days_seen%s,
        length(groupUniqArrayArray(seen_days) OVER w) AS days_observed,
        sum(days_seen) OVER w AS value_days,
        countIf(days_seen = 1) OVER w AS singletons
    FROM (
%s
    )
    WINDOW w AS (PARTITION BY partition_val)
)
WHERE days_observed >= %d`, daysCol, sinceMid, inner, minHistory)
}

// firstSeenScoring is what a first_seen model's scoring subquery depends on
// beyond its table: its window and an alert's window start.
type firstSeenScoring struct {
	historyDays int
	newSince    time.Time
}

// firstSeenScoringSQL returns the scoring subquery for a first_seen model.
// is_new reads when the model recorded the entity, not its event time, so a late
// or replayed log is still new to it and seeded history never is; newSince, an
// alert's window start, widens the hour for a backlogged alert.
//
// newOnly scores only keys with a state row recorded since the horizon, which
// is_new = '1' needs; those keys score the same.
func (k *modelKeySet) firstSeenScoringSQL(newOnly bool) string {
	horizon := "now() - INTERVAL 1 HOUR"
	if since := k.firstSeen.newSince; !since.IsZero() {
		horizon = fmt.Sprintf("least(%s, toDateTime64('%s', 3, 'UTC'))", horizon, chTimeLiteral(since.UTC()))
	}
	table := "`" + k.table + "`"
	scope := fractalIDInClause(k.fractalIDs)
	if newOnly {
		// A minute early, so the set stays a superset however each now() is read.
		scope += fmt.Sprintf(" AND entity_key %s (SELECT entity_key FROM %s WHERE %s AND first_recorded >= %s - INTERVAL 1 MINUTE)",
			k.inOp(), table, scope, horizon)
	}
	agg := FirstSeenAggSQL(FirstSeenAgg{
		Source:      table,
		Scope:       scope,
		KeyCol:      "entity_key",
		HistoryDays: k.firstSeen.historyDays,
		Recorded:    true,
	})
	return fmt.Sprintf(`SELECT entity_key,
    toString(first_seen) AS first_seen,
    toString(last_seen) AS last_seen,
    event_count,
    if(recorded_at >= %s, '1', '0') AS is_new
FROM (%s
)`, horizon, agg)
}

// scoreOnlyNew narrows a first_seen model's scoring to the keys is_new = '1' can
// hold for, once a strict filter requires it.
func (k *modelKeySet) scoreOnlyNew() {
	if k.firstSeen != nil {
		k.scoringSQL = k.firstSeenScoringSQL(true)
	}
}

// parseModelLookupArgs parses the model=, key=[] and strict= arguments.
//
// strict defaults to true: a log the model never scored is dropped rather than carried
// with the model's columns at their type defaults, which is what made `| percent < 0.1`
// match every unscored log.
func parseModelLookupArgs(cmd CommandNode) (modelName string, keyFields []string, strict bool, err error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return "", nil, false, err
	}
	// require= is the spelling; strict= is the original one and still parses.
	// Unlike match(), a model lookup defaults to keeping only scored rows: an
	// unscored row carries the model's type defaults, which read as a real score.
	strict = true
	name := "strict"
	if b.Has("require") {
		name = "require"
	}
	if raw := b.Str(name, ""); raw != "" {
		switch strings.ToLower(raw) {
		case "true":
			strict = true
		case "false":
			strict = false
		default:
			return "", nil, false, fmt.Errorf("modelLookup() %s= must be true or false", name)
		}
	}
	modelName = b.Str("model", "")
	if modelName == "" {
		return "", nil, false, fmt.Errorf("modelLookup() requires model= parameter")
	}
	return modelName, b.Strings("key"), strict, nil
}

// modelLookupFieldRef is the reference a key field is matched by, as a String like
// every model key column: a JOIN does not convert a number to compare it with a
// String (ClickHouse code 386). A stored field already reads as ::String, bare, so
// the scan prefilter keeps its skip indexes.
func modelLookupFieldRef(field string, registry *FieldRegistry) string {
	// Resolved through the registry, so a key an earlier command produced is looked
	// up as the value that command computed rather than as a stored field of the
	// same name, which is empty on every row.
	return modelKeyString(resolveFieldRef(field, registry))
}

// modelKeyString casts a key reference to String unless it already is one. A NULL
// key reads as the empty string, as a stored field the log lacks does, so NOT
// modelLookup() keeps the row rather than NOT (NULL IN ...) dropping it.
func modelKeyString(ref string) string {
	if strings.HasPrefix(ref, "fields.`") && strings.HasSuffix(ref, "::String") {
		return ref
	}
	return "ifNull(toString(" + ref + "), '')"
}

func init() {
	// model_lookup is the original spelling, kept so stored queries keep running.
	registerCommand(&modelLookupHandler{}, "modelLookup", "model_lookup")
}

func init() {
	registerSpec(&CommandSpec{Name: "modelLookup", Params: []ParamSpec{
		ParamSpec{Name: "model", Kind: ParamLiteral, Required: true},
		ParamSpec{Name: "key", Kind: ParamList, Required: true},
		namedLit("require"), namedLit("strict"),
	}}, "modelLookup", "model_lookup")
}

// conditionsReference reports whether any condition reads field on either side.
func conditionsReference(conds []HavingCondition, field string) bool {
	for _, c := range conds {
		if c.IsCompound {
			if conditionsReference(c.Children, field) {
				return true
			}
		} else if c.Field == field || c.ValueField == field {
			return true
		}
	}
	return false
}

package schemafields

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// maxDynamicPaths mirrors the max_dynamic_paths setting declared on logs.fields.
// Beyond this, ClickHouse stops giving new JSON paths their own sub-column and
// spills them into shared storage, where reads are dramatically slower. It is
// the scarce resource the schema tab exists to manage.
const maxDynamicPaths = 1024

// staleAfter is how far behind the last sweep may fall before the tab says so.
// It is a multiple of the sweep interval rather than a fixed age, so raising the
// interval does not make the page permanently claim to be stale.
const staleAfter = 3

// FieldInsight is what the sample says about one field.
type FieldInsight struct {
	Name        string  `json:"name"`
	Present     uint64  `json:"present"`     // rows in the sample where the field is populated
	Cardinality uint64  `json:"cardinality"` // distinct values within the sample
	Coverage    float64 `json:"coverage"`    // Present / sample size, 0..1
}

// Verdict is the closed set of recommendations. Every field carries exactly one,
// which is what lets the UI render a single table filtered by verdict rather
// than splitting suggestions and configuration into separate sections. A blank
// cell would read as an error; VerdictNone says "nothing to do" explicitly.
const (
	VerdictUrgent  = "urgent"  // spilled out of capacity, degrading right now
	VerdictReserve = "reserve" // worth a dedicated column
	VerdictIndex   = "index"   // reserved and queried, but unindexed
	VerdictUnused  = "unused"  // reserved but absent from the data
	VerdictKeep    = "keep"    // configured appropriately
	VerdictNone    = "none"    // no action warranted
)

// verdictRank is the default sort order, so the top of the table is the ranked
// worklist without a separate suggestions section.
var verdictRank = map[string]int{
	VerdictUrgent: 0, VerdictReserve: 1, VerdictIndex: 2,
	VerdictUnused: 3, VerdictKeep: 4, VerdictNone: 5,
}

// Field status values. "builtin" and "custom" are both reserved; they differ
// only in whether the admin may edit them.
const (
	StatusBuiltin    = "builtin"
	StatusCustom     = "custom"
	StatusUnreserved = "unreserved"
	StatusIgnored    = "ignored"
)

// TopValue is one frequent value of a field within the sample.
type TopValue struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
	// Approx marks a count that came from a bounded-memory estimator rather than
	// an exact tally. It is set only for high-cardinality fields, where the top
	// values are not a meaningful distribution anyway.
	Approx bool `json:"approx,omitempty"`
}

// Field is one row of the schema table: what the data says, how it is
// configured, and what to do about it. Configured and unconfigured fields share
// this shape so the UI never has to merge two lists.
type Field struct {
	FieldInsight
	Status           string     `json:"status"`
	IndexType        string     `json:"index_type"`
	SyncStatus       string     `json:"sync_status,omitempty"`
	SyncError        string     `json:"sync_error,omitempty"`
	Queried          int        `json:"queried"`    // 0..3: Never, Rarely, Sometimes, Often
	QueryRefs        int        `json:"query_refs"` // weighted reference count
	Refs             []FieldRef `json:"refs"`
	Top              []TopValue `json:"top"`
	Verdict          string     `json:"verdict"`
	RecommendedIndex string     `json:"recommended_index"`
	// BytesOnDisk is the field's compressed footprint across the parts the sweep
	// inspected, read from ClickHouse part metadata. It is what makes "reserve or
	// drop" answerable: capacity is one cost, storage is the other. ClickHouse
	// accounts separately only for a type-hinted path's sub-column, so this is 0
	// for a field that has not been reserved.
	BytesOnDisk uint64 `json:"bytes_on_disk"`
	// Addable is false for names ClickHouse can hold but Bifract cannot accept
	// (hyphens, dots). Such a field still gets a row when it is overflowing:
	// warning about a problem whose subject cannot be found is worse than
	// listing it and saying why it is stuck.
	Addable bool `json:"addable"`
	Score   int  `json:"-"`
}

// Capacity describes how close the deployment is to exhausting the dynamic
// column budget, read from part metadata rather than estimated from a sample.
type Capacity struct {
	// Limit is max_dynamic_paths: how many undeclared paths one part may give
	// their own sub-column before the rest spill into shared storage.
	Limit int `json:"limit"`
	// DynamicUsed is the largest number of dynamic paths held by any single part,
	// across every fractal. It is a per-part budget, so the worst part is the
	// figure that matters, not the union of names across parts.
	DynamicUsed int `json:"dynamic_used"`
	// Reserved counts type-hinted fields. These always hold their own sub-column
	// and sit OUTSIDE the budget, so reserving a field that is currently dynamic
	// frees a slot rather than consuming one.
	Reserved int `json:"reserved"`
	// Overflowed are fields that have already lost their own column and are
	// scanning every row.
	Overflowed []OverflowField `json:"overflowed"`
	CheckedAt  *time.Time      `json:"checked_at,omitempty"`
}

// Insights is the whole schema tab in one payload, read entirely from the
// Postgres tables the background sweep writes. No ClickHouse query runs on the
// request path, so the page renders at the same speed on an empty install and on
// a cluster holding billions of rows, and cannot fail because a scan timed out.
type Insights struct {
	SampleSize uint64   `json:"sample_size"`
	Capacity   Capacity `json:"capacity"`
	// Fields is every field the table shows: configured, unconfigured but seen in
	// the data, and ignored. Sorted by verdict priority, so the head of the list
	// is the worklist.
	Fields []Field `json:"fields"`
	// TotalBytes is the on-disk size of the JSON field column across the inspected
	// parts, the denominator for a field's share of storage.
	TotalBytes uint64 `json:"total_bytes"`
	Fractals   int    `json:"fractals"`
	// ComputedAt is when the sweep last completed. Zero means it has not run yet,
	// which the UI reports as measuring rather than as an empty schema.
	ComputedAt   *time.Time `json:"computed_at,omitempty"`
	Stale        bool       `json:"stale"`
	IntervalSecs int        `json:"interval_secs"`
}

// FieldRef names one thing that queries a field. The detail drawer shows these
// so an admin can see a field's blast radius before acting: "12 references" is a
// number, but "alert: Auth failure spike" is a reason.
type FieldRef struct {
	Kind  string `json:"kind"` // alert | dashboard | saved query | recent query
	Title string `json:"title"`
}

// maxRefsPerField bounds the payload. A field referenced by 200 saved queries
// does not need all 200 listed to justify reserving it.
const maxRefsPerField = 8

// maxHistoryQueries bounds how many distinct historical queries are parsed for
// the usage ranking. The most-run queries dominate the weighting anyway, and the
// tail is one-off exploration that should not vote on schema decisions.
const maxHistoryQueries = 5000

type fieldUsage struct {
	Weight int
	Refs   []FieldRef
}

// queryUsage records how often each field is referenced by saved BQL, and by
// what, across alerts, dashboards, saved queries, and recent history.
//
// Alerts weigh heaviest: an alert re-evaluates on a schedule forever, so a field
// it filters on pays its cost continuously rather than once. Saved queries are
// weighted by actual use, and history by run count, so a query someone ran once
// does not look like a query someone runs hourly.
//
// It runs in the background sweep, not on the request path: every row here is
// BQL that has to be lexed and parsed, and history alone can hold tens of
// thousands of them.
func queryUsage(ctx context.Context, pg *storage.PostgresClient) map[string]*fieldUsage {
	usage := map[string]*fieldUsage{}
	if pg == nil {
		return usage
	}

	type src struct {
		sql  string
		kind string
	}
	// Each row yields (title, bql, weight). run_count/use_count multiply the
	// weight so the text is parsed once per distinct query, not once per run.
	// History is grouped by text and capped for the same reason: the same query
	// re-run from different pages is one field reference, not many, and parsing
	// is the expensive part.
	sources := []src{
		{`SELECT COALESCE(name,''), query_string, 5 FROM alerts WHERE enabled = true`, "alert"},
		{`SELECT COALESCE(title,''), query_content, 2 FROM dashboard_widgets WHERE query_content <> ''`, "dashboard"},
		{`SELECT COALESCE(name,''), query_text, GREATEST(COALESCE(use_count,1),1) FROM saved_queries`, "saved query"},
		{`SELECT left(query_text,60), query_text, LEAST(SUM(GREATEST(COALESCE(run_count,1),1))::int, 1000)
		  FROM query_history
		  WHERE last_run_at > NOW() - INTERVAL '30 days' AND query_text <> ''
		  GROUP BY query_text
		  ORDER BY 3 DESC
		  LIMIT ` + strconv.Itoa(maxHistoryQueries), "recent query"},
	}

	for _, s := range sources {
		rows, err := pg.Query(ctx, s.sql)
		if err != nil {
			// A missing or renamed table must not take the whole tab down; the
			// remaining signals still produce a useful ranking.
			log.Printf("[SchemaFields] usage source unavailable: %v", err)
			continue
		}
		for rows.Next() {
			var title, text string
			var weight int
			if err := rows.Scan(&title, &text, &weight); err != nil {
				continue
			}
			for _, f := range referencedFields(text) {
				u := usage[f]
				if u == nil {
					u = &fieldUsage{}
					usage[f] = u
				}
				u.Weight += weight
				if len(u.Refs) < maxRefsPerField && title != "" {
					u.Refs = append(u.Refs, FieldRef{Kind: s.kind, Title: title})
				}
			}
		}
		rows.Close()
	}
	return usage
}

// queriedBucket compresses a raw reference weight into the coarse band the UI
// shows. A bucket is honest about the precision available (this is a weighted
// count over a rolling window, not a measurement) and sorts far better than a
// raw number nobody can calibrate.
func queriedBucket(weight int) int {
	switch {
	case weight == 0:
		return 0 // Never
	case weight < 5:
		return 1 // Rarely
	case weight < 25:
		return 2 // Sometimes
	default:
		return 3 // Often
	}
}

// referencedFields extracts the field names a BQL query filters or aggregates on.
// Parsing rather than pattern-matching the text means a field mentioned inside a
// quoted string value is not mistaken for a field reference.
func referencedFields(bql string) []string {
	if strings.TrimSpace(bql) == "" {
		return nil
	}
	pipeline, err := parser.ParseQuery(bql)
	if err != nil || pipeline == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(f string) {
		f = strings.TrimSpace(f)
		if f == "" || seen[f] {
			return
		}
		seen[f] = true
		out = append(out, f)
	}

	var walk func(conds []parser.ConditionNode)
	walk = func(conds []parser.ConditionNode) {
		for _, c := range conds {
			if c.IsCompound {
				walk(c.Children)
				continue
			}
			add(c.Field)
		}
	}
	if pipeline.Filter != nil {
		walk(pipeline.Filter.Conditions)
	}
	for _, hc := range pipeline.HavingConditions {
		add(hc.Field)
	}

	// Commands reference fields too: `table(src_ip, user)` and `stats count() by
	// user` are every bit as much "this field is queried" as a filter is, and
	// counting only filters under-reports the fields people actually work with.
	//
	// Arguments are accepted conservatively, since the slice is untyped and holds
	// whatever a command takes: anything with an operator, quote, or call syntax
	// is a value or expression rather than a field, and clause keywords are
	// skipped by name.
	parser.ForEachCommand(pipeline, func(cmd parser.CommandNode) {
		for _, arg := range cmd.Arguments {
			if looksLikeFieldRef(arg) {
				add(arg)
			}
		}
	})
	return out
}

// commandArgKeywords are clause words that appear in an argument slice but name
// no field.
var commandArgKeywords = map[string]bool{
	"by": true, "asc": true, "desc": true, "as": true, "with": true,
	"true": true, "false": true, "and": true, "or": true, "not": true,
}

// looksLikeFieldRef reports whether a raw command argument is a bare field
// reference rather than a literal, expression, or named parameter.
func looksLikeFieldRef(arg string) bool {
	arg = strings.TrimSpace(arg)
	if arg == "" || len(arg) > 255 {
		return false
	}
	if commandArgKeywords[strings.ToLower(arg)] {
		return false
	}
	// Operators, quotes, calls, and wildcards all mean this is not a bare name.
	if strings.ContainsAny(arg, "=<>!\"'()*,|+/ \t") {
		return false
	}
	// A leading digit means a number or duration, not an identifier.
	if arg[0] >= '0' && arg[0] <= '9' {
		return false
	}
	return true
}

// systemColumns are the table's own columns. They are always addressable and can
// never be type-hinted as JSON paths, so proposing them would be a dead end.
var systemColumns = map[string]bool{
	"timestamp": true, "ingest_timestamp": true, "log_id": true,
	"fractal_id": true, "raw_log": true, "norm_log": true, "normalizer": true,
}

// suggestable reports whether a field observed in the data can actually be added
// as a custom field. Suggesting a name that Create would reject offers the admin
// a button that cannot work, so those are filtered out before ranking rather
// than failing at click time.
func suggestable(name string) bool {
	return !systemColumns[name] && validFieldName.MatchString(name)
}

// recommendIndex maps observed cardinality to a skip-index type, mirroring the
// reasoning behind the built-in defaults: a set index only prunes while the
// distinct count stays under its size, a bloom filter handles high-cardinality
// identifiers, and a field nobody filters on is better left unindexed than
// taxed on every write.
func recommendIndex(s FieldInsight, queryRefs int) string {
	if queryRefs == 0 {
		// Nothing queries it yet. Reserve the column, skip the write cost until
		// there is evidence the index would be used.
		return string(IndexTypeNone)
	}
	switch {
	case s.Cardinality == 0:
		return string(IndexTypeNone)
	case s.Cardinality <= 256:
		return string(IndexTypeSet)
	default:
		return string(IndexTypeBloomFilter)
	}
}

// verdictFor decides the single recommendation a field carries.
func verdictFor(f Field, overflowed bool) string {
	reserved := f.Status == StatusBuiltin || f.Status == StatusCustom
	switch {
	case overflowed:
		// Degrading right now: this field is scanning every row, and reserving it
		// is the fix. Outranks everything advisory.
		return VerdictUrgent
	case f.Status == StatusIgnored:
		return VerdictNone
	case reserved && f.Present == 0:
		// Holding a column it never uses. Harmless, but it is capacity an admin
		// could reclaim, and nothing else in the product would ever reveal it.
		return VerdictUnused
	case reserved && f.IndexType == string(IndexTypeNone) && f.Queried >= 2 && f.Cardinality > 0:
		return VerdictIndex
	case reserved:
		return VerdictKeep
	case f.Queried > 0 || f.Coverage >= 0.5:
		return VerdictReserve
	default:
		// Seen in the data but nothing queries it and it is not prevalent. Saying
		// "no action" beats manufacturing a recommendation from thin evidence.
		return VerdictNone
	}
}

// buildFields produces the unified table: one row per field, configured or not,
// each with a verdict, sorted so the worklist is at the top.
func buildFields(measured map[string]*Field, sampledRows uint64,
	usage map[string]*fieldUsage, configured map[string]IndexType,
	custom map[string]SchemaField, ignored, overflowed map[string]bool) []Field {

	// Union of what the data shows and what is configured: a reserved field
	// absent from the sample still needs a row, or "unused" could never surface.
	names := make(map[string]struct{}, len(measured)+len(configured))
	for n := range measured {
		names[n] = struct{}{}
	}
	for n := range configured {
		names[n] = struct{}{}
	}
	for n := range ignored {
		names[n] = struct{}{}
	}

	out := make([]Field, 0, len(names))
	for name := range names {
		idx, isConfigured := configured[name]
		addable := suggestable(name)
		// Unconfigured names that can never be added are noise, not choices, so
		// they are dropped. The exception is a name that has already overflowed:
		// it is a real, live degradation and must stay findable even though
		// reserving it is not possible.
		if !isConfigured && !addable && !overflowed[name] {
			continue
		}

		var f Field
		if m := measured[name]; m != nil {
			f = *m
		}
		f.IndexType, f.Addable = string(IndexTypeNone), addable
		f.Name = name
		// Coverage is computed here rather than stored, so it stays consistent
		// with whatever set of fractals the last sweep managed to measure.
		if sampledRows > 0 {
			f.Coverage = float64(f.Present) / float64(sampledRows)
			if f.Coverage > 1 {
				f.Coverage = 1
			}
		}
		if isConfigured {
			f.IndexType = string(idx)
			if c, ok := custom[name]; ok {
				f.Status, f.SyncStatus, f.SyncError = StatusCustom, c.SyncStatus, c.SyncError
			} else {
				f.Status = StatusBuiltin
			}
		} else if ignored[name] {
			f.Status = StatusIgnored
		} else {
			f.Status = StatusUnreserved
		}

		if u := usage[name]; u != nil {
			f.QueryRefs, f.Refs = u.Weight, u.Refs
		}
		if f.Refs == nil {
			f.Refs = []FieldRef{}
		}
		if f.Top == nil {
			f.Top = []TopValue{}
		}
		f.Queried = queriedBucket(f.QueryRefs)
		f.RecommendedIndex = recommendIndex(f.FieldInsight, f.QueryRefs)
		f.Verdict = verdictFor(f, overflowed[name])

		// Score orders within a verdict band: usage first, then prevalence.
		f.Score = f.QueryRefs*10 + int(f.Coverage*20)
		out = append(out, f)
	}

	sort.SliceStable(out, func(a, b int) bool {
		if ra, rb := verdictRank[out[a].Verdict], verdictRank[out[b].Verdict]; ra != rb {
			return ra < rb
		}
		if out[a].Score != out[b].Score {
			return out[a].Score > out[b].Score
		}
		return out[a].Name < out[b].Name
	})
	return out
}

func formatPct(f float64) string {
	pct := f * 100
	if pct < 1 {
		return "<1%"
	}
	return fmt.Sprintf("%.0f%%", pct)
}

func formatCount(n uint64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.0fk", float64(n)/1_000)
	default:
		return strconv.FormatUint(n, 10)
	}
}

// BuildInsights composes the persisted sweep results with the current
// configuration into the single payload the schema tab renders from.
//
// Every input is a Postgres read. The measurements it reports (distribution,
// storage, capacity, usage) are produced by the background Sweeper, which is
// what keeps this endpoint fast and, more importantly, incapable of failing
// because ClickHouse is busy. A never-run sweep reports zero fields with a nil
// ComputedAt, which the UI renders as measuring rather than as an empty schema.
func (h *Handler) BuildInsights(ctx context.Context) (*Insights, error) {
	stats, err := loadStats(ctx, h.manager.pg)
	if err != nil {
		return nil, err
	}

	custom, err := h.manager.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list custom fields: %w", err)
	}
	ignoredSet, err := h.manager.ListIgnored(ctx)
	if err != nil {
		// Losing the ignore list would resurrect dismissed suggestions, which is
		// exactly the nagging this feature exists to avoid. Fail rather than
		// silently show them again.
		return nil, fmt.Errorf("list ignored fields: %w", err)
	}

	// Configured carries the index type, not just membership, so the table can
	// show what each field is actually set to.
	configured := make(map[string]IndexType, len(ProjectDefaultFields)+len(custom))
	for _, f := range ProjectDefaultFields {
		configured[f.FieldName] = f.IndexType
	}
	customByName := make(map[string]SchemaField, len(custom))
	for _, f := range custom {
		configured[f.FieldName] = f.IndexType
		customByName[f.FieldName] = f
	}

	usage, err := loadUsage(ctx, h.manager.pg)
	if err != nil {
		// Advisory ranking only; the rest of the payload is still worth rendering.
		log.Printf("[SchemaFields] read usage: %v", err)
		usage = map[string]*fieldUsage{}
	}

	overflowNames, checkedAt, err := listOverflow(ctx, h.manager.pg)
	if err != nil {
		log.Printf("[SchemaFields] read overflow state: %v", err)
	}
	overflowed := make(map[string]bool, len(overflowNames))
	for _, o := range overflowNames {
		overflowed[o.Name] = true
	}

	interval := sweepInterval()
	out := &Insights{
		SampleSize: stats.SampledRows,
		Capacity: Capacity{
			Limit:       maxDynamicPaths,
			DynamicUsed: stats.MaxPaths,
			Reserved:    len(configured),
			Overflowed:  overflowNames,
			CheckedAt:   checkedAtPtr(checkedAt),
		},
		Fields:       buildFields(stats.Fields, stats.SampledRows, usage, configured, customByName, ignoredSet, overflowed),
		TotalBytes:   stats.TotalBytes,
		Fractals:     stats.Fractals,
		ComputedAt:   checkedAtPtr(stats.ComputedAt),
		Stale:        !stats.ComputedAt.IsZero() && time.Since(stats.ComputedAt) > staleAfter*interval,
		IntervalSecs: int(interval / time.Second),
	}
	return out, nil
}

// checkedAtPtr omits a never-run monitor from the payload rather than sending a
// zero time the UI would have to special-case.
func checkedAtPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func asUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case uint32:
		return uint64(n)
	case int32:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case float64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	}
	return 0
}

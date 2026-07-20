package schemafields

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bifract/pkg/parser"
)

// maxDynamicPaths mirrors the max_dynamic_paths setting declared on logs.fields.
// Beyond this, ClickHouse stops giving new JSON paths their own sub-column and
// spills them into shared storage, where reads are dramatically slower. It is
// the scarce resource the schema tab exists to manage.
const maxDynamicPaths = 1024

// insightsCacheTTL keeps the sampled ClickHouse aggregation off the request path
// for repeat loads. Field distributions move slowly; a stale minute is harmless
// and this makes tab navigation instant.
const insightsCacheTTL = 60 * time.Second

// insightsSampleSize bounds the aggregation to the newest N rows. Cost is a
// function of the sample, not the table, so this stays flat whether the fractal
// holds a million rows or a hundred billion.
func insightsSampleSize() int {
	if v := os.Getenv("BIFRACT_SCHEMA_INSIGHTS_SAMPLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 50000
}

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
	// Addable is false for names ClickHouse can hold but Bifract cannot accept
	// (hyphens, dots). Such a field still gets a row when it is overflowing:
	// warning about a problem whose subject cannot be found is worse than
	// listing it and saying why it is stuck.
	Addable bool `json:"addable"`
	Score   int  `json:"-"`
}

// Capacity describes how close the fractal is to exhausting its column budget.
type Capacity struct {
	Limit int `json:"limit"`
	// InData is distinct field names observed in the sample. This measures the
	// fields actually carrying values, which is the number an admin can act on.
	// It is deliberately not the exact per-part dynamic-path allocation: that
	// requires reading the JSON column (~10x the cost, and it scales with path
	// count rather than rows). The precise allocation and the set of fields that
	// have already spilled are reported by the background overflow check.
	InData   int `json:"in_data"`
	Reserved int `json:"reserved"` // type-hinted fields, which never spill
	// Overflowed are fields that have already lost their own column and are
	// scanning every row. Measured by the background monitor, not this request.
	Overflowed []OverflowField `json:"overflowed"`
	CheckedAt  *time.Time      `json:"checked_at,omitempty"`
}

// Insights is the whole schema tab in one payload: one ClickHouse query plus a
// few cheap Postgres reads, so the page renders from a single request.
type Insights struct {
	SampleSize  uint64   `json:"sample_size"`
	Approximate bool     `json:"approximate"`
	Capacity    Capacity `json:"capacity"`
	// Fields is every field the table shows: configured, unconfigured but seen in
	// the data, and ignored. Sorted by verdict priority, so the head of the list
	// is the worklist.
	Fields []Field `json:"fields"`
}

type insightsCache struct {
	mu   sync.Mutex
	at   time.Time
	data *Insights
}

// sampleFieldStats runs the same sampled aggregation the query page's Fields rail
// uses, with an empty BQL filter so it covers everything recently ingested.
//
// Reusing parser.BuildFieldStatsSQL rather than hand-rolling the SQL is
// deliberate: that builder encodes a subtle correctness rule (empty values are
// excluded, because the typed sub-columns serialize as "" on every row and
// counting raw keys would report every field as 100% present). Duplicating the
// query here would duplicate that trap and let the two drift.
func (h *Handler) sampleFieldStats(ctx context.Context) (map[string]FieldInsight, map[string][]TopValue, uint64, error) {
	pipeline, err := parser.ParseQuery("")
	if err != nil {
		return nil, nil, 0, fmt.Errorf("build empty pipeline: %w", err)
	}
	sample := insightsSampleSize()
	// TopN feeds the detail drawer's value distribution. The builder computes it
	// either way, so asking for 5 instead of 1 costs nothing extra.
	sql, err := parser.BuildFieldStatsSQL(pipeline, parser.QueryOptions{
		StartTime: time.Now().Add(-insightsWindow()),
		EndTime:   time.Now(),
		MaxRows:   sample,
	}, parser.FieldStatsParams{SampleSize: sample, TopN: 5})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("build field stats sql: %w", err)
	}
	if sql == "" {
		return nil, nil, 0, fmt.Errorf("field stats unsupported for this source")
	}

	rows, err := h.ch.QueryLowPriority(ctx, sql)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("field stats query: %w", err)
	}

	stats := make(map[string]FieldInsight, len(rows))
	tops := make(map[string][]TopValue, len(rows))
	var sampleSize uint64
	for _, row := range rows {
		name, _ := row["key"].(string)
		present := asUint64(row["present"])
		if name == "__rows__" {
			sampleSize = present // sentinel: exact rows scanned, the coverage denominator
			continue
		}
		if name == "" {
			continue
		}
		stats[name] = FieldInsight{
			Name:        name,
			Present:     present,
			Cardinality: asUint64(row["cardinality"]),
		}
		if vals, ok := row["top_values"].([]string); ok {
			counts, _ := row["top_counts"].([]uint64)
			tv := make([]TopValue, 0, len(vals))
			for i, v := range vals {
				var c uint64
				if i < len(counts) {
					c = counts[i]
				}
				tv = append(tv, TopValue{Value: v, Count: c})
			}
			// groupArray order is not guaranteed; present count-desc.
			sort.SliceStable(tv, func(a, b int) bool { return tv[a].Count > tv[b].Count })
			tops[name] = tv
		}
	}
	if sampleSize > 0 {
		for name, s := range stats {
			s.Coverage = float64(s.Present) / float64(sampleSize)
			stats[name] = s
		}
	}
	return stats, tops, sampleSize, nil
}

// insightsWindow bounds how far back the sample may reach. Without it, a fractal
// that stopped ingesting would still report its long-dead field distribution as
// current.
func insightsWindow() time.Duration {
	if v := os.Getenv("BIFRACT_SCHEMA_INSIGHTS_WINDOW_HOURS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Hour
		}
	}
	return 7 * 24 * time.Hour
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
func (h *Handler) queryUsage(ctx context.Context) map[string]*fieldUsage {
	usage := map[string]*fieldUsage{}
	if h.manager == nil || h.manager.pg == nil {
		return usage
	}

	type src struct {
		sql  string
		kind string
	}
	// Each row yields (title, bql, weight). run_count/use_count multiply the
	// weight so the text is parsed once per distinct query, not once per run.
	sources := []src{
		{`SELECT COALESCE(name,''), query_string, 5 FROM alerts WHERE enabled = true`, "alert"},
		{`SELECT COALESCE(title,''), query_content, 2 FROM dashboard_widgets WHERE query_content <> ''`, "dashboard"},
		{`SELECT COALESCE(name,''), query_text, GREATEST(COALESCE(use_count,1),1) FROM saved_queries`, "saved query"},
		{`SELECT left(query_text,60), query_text, GREATEST(COALESCE(run_count,1),1) FROM query_history
		  WHERE last_run_at > NOW() - INTERVAL '30 days'`, "recent query"},
	}

	for _, s := range sources {
		rows, err := h.manager.pg.Query(ctx, s.sql)
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
	for _, cmd := range pipeline.Commands {
		for _, arg := range cmd.Arguments {
			if looksLikeFieldRef(arg) {
				add(arg)
			}
		}
	}
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
func buildFields(stats map[string]FieldInsight, tops map[string][]TopValue,
	usage map[string]*fieldUsage, configured map[string]IndexType,
	custom map[string]SchemaField, ignored, overflowed map[string]bool) []Field {

	// Union of what the data shows and what is configured: a reserved field
	// absent from the sample still needs a row, or "unused" could never surface.
	names := make(map[string]struct{}, len(stats)+len(configured))
	for n := range stats {
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

		f := Field{FieldInsight: stats[name], IndexType: string(IndexTypeNone), Addable: addable}
		f.Name = name
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
		if f.Top = tops[name]; f.Top == nil {
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

// BuildInsights composes the sampled ClickHouse distribution with the Postgres
// usage signals and the current configuration into the single payload the schema
// tab renders from. Results are cached briefly so repeat tab loads are free.
func (h *Handler) BuildInsights(ctx context.Context) (*Insights, error) {
	h.insights.mu.Lock()
	if h.insights.data != nil && time.Since(h.insights.at) < insightsCacheTTL {
		cached := h.insights.data
		h.insights.mu.Unlock()
		return cached, nil
	}
	h.insights.mu.Unlock()

	stats, tops, sampleSize, err := h.sampleFieldStats(ctx)
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

	usage := h.queryUsage(ctx)

	// Overflow is measured by the background monitor, not here: it needs the JSON
	// column, which costs ~10x the flat sample above and scales with path count.
	overflowNames, checkedAt, err := listOverflow(ctx, h.manager.pg)
	if err != nil {
		// Advisory only; the rest of the payload is still worth rendering.
		log.Printf("[SchemaFields] read overflow state: %v", err)
	}
	overflowed := make(map[string]bool, len(overflowNames))
	for _, o := range overflowNames {
		overflowed[o.Name] = true
	}

	// Distinct fields seen in the sample, plus any reserved field that did not
	// appear (it still holds a column even when empty).
	inData := len(stats)
	for name := range configured {
		if _, seen := stats[name]; !seen {
			inData++
		}
	}

	out := &Insights{
		SampleSize:  sampleSize,
		Approximate: sampleSize >= uint64(insightsSampleSize()),
		Capacity: Capacity{
			Limit:      maxDynamicPaths,
			InData:     inData,
			Reserved:   len(configured),
			Overflowed: overflowNames,
			CheckedAt:  checkedAtPtr(checkedAt),
		},
		Fields: buildFields(stats, tops, usage, configured, customByName, ignoredSet, overflowed),
	}

	h.insights.mu.Lock()
	h.insights.data, h.insights.at = out, time.Now()
	h.insights.mu.Unlock()
	return out, nil
}

// invalidateInsights drops the cache so a field added or ignored is reflected on
// the next load rather than up to a minute later.
func (h *Handler) invalidateInsights() {
	h.insights.mu.Lock()
	h.insights.data = nil
	h.insights.mu.Unlock()
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

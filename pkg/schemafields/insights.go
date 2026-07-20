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

// Suggestion is a field worth reserving, with the evidence behind it.
type Suggestion struct {
	FieldInsight
	Reasons          []string `json:"reasons"`           // human-readable evidence, ranked
	RecommendedIndex string   `json:"recommended_index"` // none | set | bloom_filter
	QueryRefs        int      `json:"query_refs"`        // times referenced across saved BQL
	Overflowed       bool     `json:"overflowed"`        // already spilled; degrading now
	Score            int      `json:"-"`
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
	SampleSize  uint64                  `json:"sample_size"`
	Approximate bool                    `json:"approximate"`
	Capacity    Capacity                `json:"capacity"`
	Stats       map[string]FieldInsight `json:"stats"`       // by field name, for configured rows
	Suggestions []Suggestion            `json:"suggestions"` // ranked, excludes configured and ignored
	Ignored     []string                `json:"ignored"`
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
func (h *Handler) sampleFieldStats(ctx context.Context) (map[string]FieldInsight, uint64, error) {
	pipeline, err := parser.ParseQuery("")
	if err != nil {
		return nil, 0, fmt.Errorf("build empty pipeline: %w", err)
	}
	sample := insightsSampleSize()
	sql, err := parser.BuildFieldStatsSQL(pipeline, parser.QueryOptions{
		StartTime: time.Now().Add(-insightsWindow()),
		EndTime:   time.Now(),
		MaxRows:   sample,
	}, parser.FieldStatsParams{SampleSize: sample, TopN: 1})
	if err != nil {
		return nil, 0, fmt.Errorf("build field stats sql: %w", err)
	}
	if sql == "" {
		return nil, 0, fmt.Errorf("field stats unsupported for this source")
	}

	rows, err := h.ch.QueryLowPriority(ctx, sql)
	if err != nil {
		return nil, 0, fmt.Errorf("field stats query: %w", err)
	}

	stats := make(map[string]FieldInsight, len(rows))
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
	}
	if sampleSize > 0 {
		for name, s := range stats {
			s.Coverage = float64(s.Present) / float64(sampleSize)
			stats[name] = s
		}
	}
	return stats, sampleSize, nil
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

// queryUsage counts how often each field name is referenced by saved BQL across
// alerts, saved queries, and recent history.
//
// Alerts weigh heaviest: an alert re-evaluates on a schedule forever, so a field
// it filters on pays its cost continuously rather than once. Saved queries are
// weighted by actual use, and history by run count, so a query someone ran once
// does not look like a query someone runs hourly.
func (h *Handler) queryUsage(ctx context.Context) map[string]int {
	usage := map[string]int{}
	if h.manager == nil || h.manager.pg == nil {
		return usage
	}

	type src struct {
		sql    string
		weight int
	}
	// run_count/use_count multiply the weight, so the query text is scanned once
	// per distinct query rather than once per execution.
	sources := []src{
		{`SELECT query_string, 5 FROM alerts WHERE enabled = true`, 1},
		{`SELECT query_text, GREATEST(COALESCE(use_count, 1), 1) FROM saved_queries`, 1},
		{`SELECT query_text, GREATEST(COALESCE(run_count, 1), 1) FROM query_history
		  WHERE last_run_at > NOW() - INTERVAL '30 days'`, 1},
		{`SELECT query_content, 2 FROM dashboard_widgets WHERE query_content <> ''`, 1},
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
			var text string
			var weight int
			if err := rows.Scan(&text, &weight); err != nil {
				continue
			}
			for _, f := range referencedFields(text) {
				usage[f] += weight
			}
		}
		rows.Close()
	}
	return usage
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

// buildSuggestions ranks unconfigured fields by how much reserving them would help.
func buildSuggestions(stats map[string]FieldInsight, usage map[string]int,
	configured, ignored, overflowed map[string]bool) []Suggestion {

	out := make([]Suggestion, 0, len(stats))
	for name, s := range stats {
		if configured[name] || ignored[name] || !suggestable(name) {
			continue
		}
		refs := usage[name]
		sug := Suggestion{
			FieldInsight:     s,
			QueryRefs:        refs,
			RecommendedIndex: recommendIndex(s, refs),
			Overflowed:       overflowed[name],
		}

		// Score and evidence are built together so the reasons shown always
		// explain the rank rather than being a separate cosmetic list.
		if sug.Overflowed {
			// Already degrading, so it outranks everything advisory: this field
			// is scanning every row right now and reserving it fixes that.
			sug.Score += 1000
			sug.Reasons = append(sug.Reasons, "out of capacity")
		}
		if refs > 0 {
			sug.Score += refs * 10
			sug.Reasons = append(sug.Reasons, pluralize(refs, "saved query", "saved queries"))
		}
		if s.Coverage > 0 {
			sug.Score += int(s.Coverage * 20)
			sug.Reasons = append(sug.Reasons,
				fmt.Sprintf("in %s of logs", formatPct(s.Coverage)))
		}
		if s.Cardinality > 0 {
			sug.Reasons = append(sug.Reasons,
				fmt.Sprintf("%s distinct", formatCount(s.Cardinality)))
		}
		out = append(out, sug)
	}

	sort.SliceStable(out, func(a, b int) bool {
		if out[a].Score != out[b].Score {
			return out[a].Score > out[b].Score
		}
		return out[a].Name < out[b].Name
	})
	return out
}

func pluralize(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", one)
	}
	return fmt.Sprintf("%d %s", n, many)
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

	stats, sampleSize, err := h.sampleFieldStats(ctx)
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

	configured := ProjectDefaultFieldMap()
	for _, f := range custom {
		configured[f.FieldName] = true
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

	ignoredList := make([]string, 0, len(ignoredSet))
	for name := range ignoredSet {
		ignoredList = append(ignoredList, name)
	}
	sort.Strings(ignoredList)

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
		Stats:       stats,
		Suggestions: buildSuggestions(stats, usage, configured, ignoredSet, overflowed),
		Ignored:     ignoredList,
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

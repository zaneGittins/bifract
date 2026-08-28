package alerts

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/storage"

	"github.com/lib/pq"
)

// Feed alert listing is paginated, filtered and sorted in Postgres: a single feed
// can carry thousands of Sigma rules, and shipping every query_string to the
// browser to filter client-side costs megabytes per page load.

// FeedAlertRow is the trimmed projection the feed alerts table renders.
// query_string and references are deliberately absent; the details panel
// fetches the full alert by ID.
type FeedAlertRow struct {
	ID                  string     `json:"id"`
	Name                string     `json:"name"`
	Description         string     `json:"description"`
	Enabled             bool       `json:"enabled"`
	Labels              []string   `json:"labels"`
	FeedID              string     `json:"feed_id"`
	FeedName            string     `json:"feed_name"`
	DisabledReason      string     `json:"disabled_reason,omitempty"`
	LastTriggered       *time.Time `json:"last_triggered,omitempty"`
	LastExecutionTimeMs *int       `json:"last_execution_time_ms,omitempty"`
}

// FeedAlertQuery describes one page of the feed alerts table.
type FeedAlertQuery struct {
	FractalID string
	PrismID   string
	Search    string
	Status    string // "", "enabled", "disabled"
	FeedID    string
	Severity  string // sigma level, e.g. "high"
	Label     string
	Sort      string // name | severity | exec_time | last_triggered
	Dir       string // asc | desc
	Limit     int
	Offset    int
}

// FeedRef identifies a feed that currently owns at least one alert in scope.
type FeedRef struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// FeedAlertFacets holds the filter-dropdown inputs. Computed only when asked
// for, so paging and sorting stay a single indexed query.
type FeedAlertFacets struct {
	Labels     []string  `json:"labels"`
	Feeds      []FeedRef `json:"feeds"`
	Unfiltered int       `json:"unfiltered"`
}

// FeedAlertPage is one rendered page plus the counts the table header needs.
type FeedAlertPage struct {
	Alerts []FeedAlertRow   `json:"alerts"`
	Total  int              `json:"total"`
	Facets *FeedAlertFacets `json:"facets,omitempty"`
}

// severityRankSQL mirrors the client's severity ordering, which reads the
// sigma:<level> label rather than the severity column (feed sync writes "info"
// there and defaults level-less rules to "medium").
const severityRankSQL = `CASE
	WHEN 'sigma:critical' = ANY(a.labels) THEN 5
	WHEN 'sigma:high' = ANY(a.labels) THEN 4
	WHEN 'sigma:medium' = ANY(a.labels) THEN 3
	WHEN 'sigma:low' = ANY(a.labels) THEN 2
	WHEN 'sigma:informational' = ANY(a.labels) THEN 1
	ELSE 0 END`

var feedAlertSortColumns = map[string]string{
	"name":           "lower(a.name)",
	"severity":       severityRankSQL,
	"exec_time":      "COALESCE(a.last_execution_time_ms, -1)",
	"last_triggered": "COALESCE(a.last_triggered, to_timestamp(0))",
}

var sigmaLevels = Severity("").EnumValues()

// argBuilder appends bind parameters and hands back their placeholders.
type argBuilder struct {
	args []interface{}
}

func (b *argBuilder) next(v interface{}) string {
	b.args = append(b.args, v)
	return "$" + strconv.Itoa(len(b.args))
}

// escapeLike neutralizes wildcards so a user's "%" searches for a literal "%".
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// where builds the shared filter predicate for listing, counting and bulk
// toggling, so every one of those paths sees exactly the same row set.
func (q FeedAlertQuery) where(b *argBuilder) string {
	clauses := []string{"a.feed_id IS NOT NULL"}

	if q.PrismID != "" {
		clauses = append(clauses, "a.prism_id = "+b.next(q.PrismID))
	} else {
		clauses = append(clauses, "a.fractal_id = "+b.next(q.FractalID))
	}

	if s := strings.TrimSpace(q.Search); s != "" {
		p := b.next("%" + likeEscaper.Replace(s) + "%")
		clauses = append(clauses, fmt.Sprintf(
			`(a.name ILIKE %[1]s ESCAPE '\' OR COALESCE(a.description,'') ILIKE %[1]s ESCAPE '\'`+
				` OR COALESCE(a.feed_rule_path,'') ILIKE %[1]s ESCAPE '\' OR a.query_string ILIKE %[1]s ESCAPE '\')`, p))
	}

	switch q.Status {
	case "enabled":
		clauses = append(clauses, "a.enabled = true")
	case "disabled":
		clauses = append(clauses, "a.enabled = false")
	}

	if q.FeedID != "" {
		clauses = append(clauses, "a.feed_id = "+b.next(q.FeedID))
	}
	if q.Severity != "" {
		clauses = append(clauses, b.next("sigma:"+strings.ToLower(q.Severity))+" = ANY(a.labels)")
	}
	if q.Label != "" {
		clauses = append(clauses, b.next(q.Label)+" = ANY(a.labels)")
	}

	return strings.Join(clauses, " AND ")
}

func (q FeedAlertQuery) orderBy() string {
	col, ok := feedAlertSortColumns[q.Sort]
	if !ok {
		// Default matches the feed-grouped ordering the table opened with.
		return "COALESCE(f.name,''), a.name, a.id"
	}
	dir := "ASC"
	if strings.EqualFold(q.Dir, "desc") {
		dir = "DESC"
	}
	// a.id breaks ties so paging can never repeat or skip a row.
	return fmt.Sprintf("%s %s, a.name, a.id", col, dir)
}

// ListFeedAlertsPage returns one page of feed alerts for the scope, along with
// the total number of rows matching the filters.
func (m *Manager) ListFeedAlertsPage(ctx context.Context, q FeedAlertQuery) (*FeedAlertPage, error) {
	if q.Limit <= 0 || q.Limit > 500 {
		q.Limit = 25
	}
	if q.Offset < 0 {
		q.Offset = 0
	}

	b := &argBuilder{}
	where := q.where(b)
	limit := b.next(q.Limit)
	offset := b.next(q.Offset)

	query := fmt.Sprintf(`
		SELECT a.id, a.name, LEFT(COALESCE(a.description,''), 200), a.enabled, a.labels,
		       COALESCE(a.feed_id::text, ''), COALESCE(f.name, ''), COALESCE(a.disabled_reason, ''),
		       a.last_triggered, a.last_execution_time_ms,
		       COUNT(*) OVER () AS total
		FROM alerts a
		LEFT JOIN alert_feeds f ON a.feed_id = f.id
		WHERE %s
		ORDER BY %s
		LIMIT %s OFFSET %s`, where, q.orderBy(), limit, offset)

	rows, err := m.pg.Query(ctx, query, b.args...)
	if err != nil {
		return nil, fmt.Errorf("list feed alerts page: %w", err)
	}
	defer rows.Close()

	page := &FeedAlertPage{Alerts: []FeedAlertRow{}}
	for rows.Next() {
		var r FeedAlertRow
		var total int
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Enabled, pq.Array(&r.Labels),
			&r.FeedID, &r.FeedName, &r.DisabledReason,
			&r.LastTriggered, &r.LastExecutionTimeMs, &total); err != nil {
			return nil, fmt.Errorf("scan feed alert row: %w", err)
		}
		page.Total = total
		page.Alerts = append(page.Alerts, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list feed alerts page: %w", err)
	}
	return page, nil
}

// FeedAlertFacetsFor returns the filter-dropdown options for a scope: every
// distinct label, the feeds that own alerts, and the unfiltered total.
func (m *Manager) FeedAlertFacetsFor(ctx context.Context, fractalID, prismID string) (*FeedAlertFacets, error) {
	scope := FeedAlertQuery{FractalID: fractalID, PrismID: prismID}
	b := &argBuilder{}
	where := scope.where(b)

	facets := &FeedAlertFacets{Labels: []string{}, Feeds: []FeedRef{}}

	labelRows, err := m.pg.Query(ctx, fmt.Sprintf(`
		SELECT DISTINCT unnest(a.labels) AS label
		FROM alerts a
		WHERE %s
		ORDER BY label`, where), b.args...)
	if err != nil {
		return nil, fmt.Errorf("feed alert label facets: %w", err)
	}
	defer labelRows.Close()

	skip := make(map[string]bool, len(sigmaLevels))
	for _, l := range sigmaLevels {
		skip["sigma:"+l] = true
	}
	for labelRows.Next() {
		var label string
		if err := labelRows.Scan(&label); err != nil {
			return nil, fmt.Errorf("scan label facet: %w", err)
		}
		// Severity has its own filter; feed: was a legacy display-only label.
		if skip[label] || strings.HasPrefix(label, "feed:") {
			continue
		}
		facets.Labels = append(facets.Labels, label)
	}
	if err := labelRows.Err(); err != nil {
		return nil, fmt.Errorf("feed alert label facets: %w", err)
	}

	fb := &argBuilder{}
	feedWhere := scope.where(fb)
	feedRows, err := m.pg.Query(ctx, fmt.Sprintf(`
		SELECT COALESCE(a.feed_id::text, ''), COALESCE(f.name, ''), COUNT(*)
		FROM alerts a
		LEFT JOIN alert_feeds f ON a.feed_id = f.id
		WHERE %s
		GROUP BY 1, 2
		ORDER BY 2`, feedWhere), fb.args...)
	if err != nil {
		return nil, fmt.Errorf("feed alert feed facets: %w", err)
	}
	defer feedRows.Close()

	for feedRows.Next() {
		var ref FeedRef
		if err := feedRows.Scan(&ref.ID, &ref.Name, &ref.Count); err != nil {
			return nil, fmt.Errorf("scan feed facet: %w", err)
		}
		facets.Unfiltered += ref.Count
		facets.Feeds = append(facets.Feeds, ref)
	}
	if err := feedRows.Err(); err != nil {
		return nil, fmt.Errorf("feed alert feed facets: %w", err)
	}
	return facets, nil
}

// BatchToggleFeedAlertsFiltered enables or disables every feed alert matching
// the filters. The client only ever holds one page of IDs, so "Enable Filtered"
// resolves the set server-side against the same predicate the table renders.
// Re-enabling resets last_evaluated_at to near-now; see EnableFeedAlerts.
func (m *Manager) BatchToggleFeedAlertsFiltered(ctx context.Context, q FeedAlertQuery, enabled bool, updatedBy string) (int, error) {
	b := &argBuilder{}
	enabledArg := b.next(enabled)
	updatedByArg := b.next(storage.NullableUser(updatedBy))
	where := q.where(b)

	result, err := m.pg.Exec(ctx, fmt.Sprintf(`
		UPDATE alerts SET enabled = %[1]s, updated_by = %[2]s, disabled_reason = '',
		    last_evaluated_at = CASE WHEN %[1]s = true AND enabled = false THEN NOW() - INTERVAL '5 minutes' ELSE last_evaluated_at END
		WHERE id IN (SELECT a.id FROM alerts a WHERE %[3]s)`, enabledArg, updatedByArg, where), b.args...)
	if err != nil {
		return 0, fmt.Errorf("batch toggle filtered feed alerts: %w", err)
	}
	rows, _ := result.RowsAffected()
	m.engine.RefreshAlerts(ctx)
	return int(rows), nil
}

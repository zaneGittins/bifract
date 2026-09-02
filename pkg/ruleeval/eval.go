package ruleeval

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"bifract/pkg/ingest"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// WindowSpan is how far either side of now an evaluation looks. Entries are stamped
// with an ingest timestamp of now, so this only has to absorb clock skew and a slow run.
const WindowSpan = time.Hour

// visibilityTimeout bounds the wait for inserted rows to become queryable.
const visibilityTimeout = 30 * time.Second

// Window is the ingest_timestamp range every query in a run shares.
//
// UTC because the translator formats bounds with the location it is given and
// ClickHouse reads naive literals in server time, which is UTC.
type Window struct {
	Start time.Time
	End   time.Time
}

// NewWindow centres a window on the given instant.
func NewWindow(now time.Time) Window {
	now = now.UTC()
	return Window{Start: now.Add(-WindowSpan), End: now.Add(WindowSpan)}
}

// Unit is one independent evaluation: a set of events in their own synthetic fractal,
// and the query run against just them.
//
// The fractal ID is generated per unit and registered nowhere, which is what isolates
// one test's events from another's and from every real fractal.
type Unit struct {
	FractalID string
	Label     string
	Entries   []storage.LogEntry
}

// NewUnit normalizes events into a unit under a fresh synthetic fractal.
//
// A nil normalizer treats each object as already-normalized fields, which is the path
// the alert editor uses: events picked from search results or pasted as normalized JSON
// are already in the shape BQL runs against.
func NewUnit(label string, events []map[string]interface{}, norm *normalizers.CompiledNormalizer) (Unit, error) {
	u := Unit{FractalID: uuid.NewString(), Label: label}

	u.Entries = make([]storage.LogEntry, 0, len(events))
	for i, obj := range events {
		entry, err := ingest.BuildLogEntry(obj, norm, nil)
		if err != nil {
			return Unit{}, fmt.Errorf("event %d: %w", i+1, err)
		}
		entry.FractalID = u.FractalID
		u.Entries = append(u.Entries, entry)
	}
	return u, nil
}

// Insert writes every unit's entries to the scratch table in one batch.
func (s *Scratch) Insert(ctx context.Context, units []Unit) error {
	var batch []storage.LogEntry
	for _, u := range units {
		batch = append(batch, u.Entries...)
	}
	if len(batch) == 0 {
		return nil
	}
	if err := s.client.InsertLogsInto(ctx, s.table, batch); err != nil {
		return fmt.Errorf("inserting test events: %w", err)
	}
	return nil
}

// WaitVisible blocks until every unit's rows are queryable within the window.
//
// This is a correctness guard, not just a wait. If a run cannot see its own rows,
// whether from clock skew, an async insert still in flight, or a fractal scoping
// mistake, then every rule returns nothing and each "should not match" test passes for
// entirely the wrong reason. A silent vacuous pass is the worst outcome for a detection
// test, so an invisible row is a hard error rather than a verdict.
func (s *Scratch) WaitVisible(ctx context.Context, units []Unit, w Window) error {
	want := make(map[string]int, len(units))
	labels := make(map[string]string, len(units))
	for _, u := range units {
		want[u.FractalID] += len(u.Entries)
		labels[u.FractalID] = u.Label
	}
	if len(want) == 0 {
		return nil
	}

	query := fmt.Sprintf(
		"SELECT fractal_id, count() AS c FROM %s WHERE ingest_timestamp >= '%s' AND ingest_timestamp <= '%s' GROUP BY fractal_id",
		s.Qualified(),
		w.Start.Format("2006-01-02 15:04:05"),
		w.End.Format("2006-01-02 15:04:05"))

	deadline := time.Now().Add(visibilityTimeout)
	var missing []string

	for {
		rows, err := s.client.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("verifying test events are queryable: %w", err)
		}

		seen := make(map[string]int, len(rows))
		for _, r := range rows {
			if id, ok := r["fractal_id"].(string); ok {
				seen[id] = int(toUint64(r["c"]))
			}
		}

		missing = nil
		for id, n := range want {
			if seen[id] < n {
				missing = append(missing, fmt.Sprintf("%s (%d/%d rows)", labels[id], seen[id], n))
			}
		}
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	return fmt.Errorf("test events were inserted but are not visible to the query window: %v", missing)
}

// QueryOptions mirrors alerts.Engine.buildQueryOpts: alerts filter on ingest_timestamp,
// so an event whose own timestamp is months old is still evaluated when it arrives.
func (s *Scratch) QueryOptions(u Unit, w Window) parser.QueryOptions {
	return parser.QueryOptions{
		StartTime:          w.Start,
		EndTime:            w.End,
		MaxRows:            10000,
		UseIngestTimestamp: true,
		TableName:          s.table,
		FractalID:          u.FractalID,
	}
}

// Evaluate runs a parsed rule against one unit and reports how many rows it returned.
// The SQL is returned for explain output whether or not the query succeeded.
func (s *Scratch) Evaluate(ctx context.Context, pipeline *parser.PipelineNode, u Unit, w Window) (rows int, sql string, err error) {
	sql, err = parser.TranslateToSQL(pipeline, s.QueryOptions(u, w))
	if err != nil {
		return 0, "", fmt.Errorf("translating BQL to SQL: %w", err)
	}

	result, err := s.client.Query(ctx, sql)
	if err != nil {
		return 0, sql, fmt.Errorf("query failed: %w", err)
	}
	return len(result), sql, nil
}

func toUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		return uint64(n)
	case int:
		return uint64(n)
	case uint32:
		return uint64(n)
	case float64:
		return uint64(n)
	}
	return 0
}

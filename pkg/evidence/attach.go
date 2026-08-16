package evidence

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"bifract/pkg/parser"
)

// EventsColumn carries the events behind each matched sequence.
const EventsColumn = "_chain_events"

// FieldsColumn carries the fields the query filtered on, so clients can lead an
// event summary with them instead of a hardcoded list.
const FieldsColumn = "_chain_fields"

// Attach enriches result rows with the underlying events their query correlated,
// for any query that declared how to find them. Callers pass the translation
// result and a fetcher; which constructs support enrichment is decided here, not
// by the caller.
//
// Rows are enriched in place. A query that declared nothing is a no-op.
func Attach(ctx context.Context, translated *parser.TranslationResult, rows []map[string]interface{}, opts parser.QueryOptions, fetch Fetcher) {
	if translated == nil || len(rows) == 0 {
		return
	}
	if translated.Chain != nil {
		Hydrate(ctx, translated.Chain, rows, opts, fetch)
	}
}

// Fetcher runs one bounded SQL statement and returns its rows.
type Fetcher func(ctx context.Context, sql string) ([]map[string]interface{}, error)

// Hydrate resolves chain() anchors into the log rows that formed each
// matched sequence and attaches them to their entity row under EventsColumn.
//
// The chain aggregate returns one row per entity carrying only a count, so the
// events behind a match are otherwise invisible without hand-querying every
// timestamp. Cost is bounded by the page: one statement resolving
// len(rows) x len(steps) anchors, pruned on the logs sorting key, regardless of
// how many entities matched or how large the table is.
//
// Row count is unchanged; each row gains a nested array. Failures degrade to the
// un-hydrated rows rather than failing the query.
func Hydrate(ctx context.Context, meta *parser.ChainMeta, rows []map[string]interface{}, opts parser.QueryOptions, fetch Fetcher) {
	if meta == nil || len(rows) == 0 || fetch == nil {
		return
	}
	entities := make([]string, 0, len(rows))
	anchors := make([][]int64, 0, len(rows))
	for _, row := range rows {
		ts := anchorTimestamps(row[meta.AnchorColumn])
		if len(ts) == 0 {
			continue
		}
		anchors = append(anchors, ts)
		entities = append(entities, entityKey(row[meta.EntityColumn]))
	}
	if len(anchors) == 0 {
		return
	}

	fetchPlan := parser.BuildChainEventsSQL(meta, entities, anchors, opts)
	if fetchPlan == nil {
		return
	}
	if fetchPlan.Truncated > 0 {
		log.Printf("[chain] event lookup covered the first %d entities, %d beyond the cap have no events attached",
			len(anchors)-fetchPlan.Truncated, fetchPlan.Truncated)
	}

	found, err := fetch(ctx, fetchPlan.SQL)
	if err != nil {
		log.Printf("[chain] event lookup failed, returning counts without events: %v", err)
		return
	}

	// Index by entity+timestamp. Multi-identity chains have no single source
	// column to compare, so those match on timestamp alone.
	byKey := make(map[string][]map[string]interface{}, len(found))
	for _, ev := range found {
		ms := toInt64(ev["_ts_ms"])
		if ms == 0 {
			continue
		}
		key := eventKey(entityKey(ev["_entity_key"]), ms, meta.MultiIdentity)
		delete(ev, "_entity_key")
		delete(ev, "_ts_ms")
		// Formatted here rather than in SQL: a `toString(timestamp) AS timestamp`
		// alias would shadow the real column for the millisecond conversion.
		ev["timestamp"] = time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.000")
		byKey[key] = append(byKey[key], ev)
	}

	for _, row := range rows {
		ts := anchorTimestamps(row[meta.AnchorColumn])
		if len(ts) == 0 {
			continue
		}
		entity := entityKey(row[meta.EntityColumn])
		events := make([]map[string]interface{}, 0, len(ts))
		for i, ms := range ts {
			for _, ev := range byKey[eventKey(entity, ms, meta.MultiIdentity)] {
				step := map[string]interface{}{"step": i + 1}
				for k, v := range ev {
					step[k] = v
				}
				events = append(events, step)
			}
		}
		if len(events) > 0 {
			row[EventsColumn] = events
			row[FieldsColumn] = meta.StepFields
		}
	}
}

// eventKey scopes an event to its entity, or to the timestamp alone when the
// entity came from an arrayJoin and cannot be compared directly.
func eventKey(entity string, ms int64, multiIdentity bool) string {
	if multiIdentity {
		entity = ""
	}
	return entity + "\x00" + strconv.FormatInt(ms, 10)
}

// anchorTimestamps normalizes the anchor column, which arrives as a ClickHouse
// array of unix milliseconds and may cross a JSON round trip on the way.
func anchorTimestamps(v interface{}) []int64 {
	switch a := v.(type) {
	case []int64:
		return a
	case []uint64:
		out := make([]int64, len(a))
		for i, n := range a {
			out[i] = int64(n)
		}
		return out
	case []interface{}:
		out := make([]int64, 0, len(a))
		for _, e := range a {
			if n := toInt64(e); n != 0 {
				out = append(out, n)
			}
		}
		return out
	default:
		return nil
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}

func entityKey(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

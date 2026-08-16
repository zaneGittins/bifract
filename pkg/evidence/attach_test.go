package evidence

import (
	"context"
	"strings"
	"testing"

	"bifract/pkg/parser"
)

func chainMeta() *parser.ChainMeta {
	return &parser.ChainMeta{
		EntityColumn:   "process_guid",
		EntityExpr:     "fields.`process_guid`::String",
		AnchorColumn:   "_chain_ts",
		StepConditions: []string{"c1", "c2"},
	}
}

func TestHydrateAttachesEventsInStepOrder(t *testing.T) {
	rows := []map[string]interface{}{
		{"process_guid": "G1", "chain_count": uint64(23), "_chain_ts": []int64{200, 100}},
	}
	// Returned out of anchor order on purpose: step order comes from the anchors,
	// not from the fetch's ORDER BY.
	fetch := func(_ context.Context, _ string) ([]map[string]interface{}, error) {
		return []map[string]interface{}{
			{"_entity_key": "G1", "_ts_ms": int64(100), "log_id": "b", "norm_log": `{"dst_ip":"10.0.0.1"}`},
			{"_entity_key": "G1", "_ts_ms": int64(200), "log_id": "a", "norm_log": `{"dst_ip":"8.8.8.8"}`},
		}, nil
	}

	Hydrate(context.Background(), chainMeta(), rows, parser.QueryOptions{FractalID: "f"}, fetch)

	if len(rows) != 1 {
		t.Fatalf("row count must not change, got %d", len(rows))
	}
	events, ok := rows[0][EventsColumn].([]map[string]interface{})
	if !ok || len(events) != 2 {
		t.Fatalf("expected 2 attached events, got %#v", rows[0][EventsColumn])
	}
	if events[0]["step"] != 1 || events[0]["log_id"] != "a" {
		t.Errorf("step 1 must be the first anchor (ts=200, log a), got %#v", events[0])
	}
	if events[1]["step"] != 2 || events[1]["log_id"] != "b" {
		t.Errorf("step 2 must be the second anchor (ts=100, log b), got %#v", events[1])
	}
	// Internal join keys must not leak to the client.
	if _, leaked := events[0]["_ts_ms"]; leaked {
		t.Error("_ts_ms leaked into the attached event")
	}
	if _, leaked := events[0]["_entity_key"]; leaked {
		t.Error("_entity_key leaked into the attached event")
	}
}

func TestHydrateScopesEventsToTheirEntity(t *testing.T) {
	rows := []map[string]interface{}{
		{"process_guid": "G1", "_chain_ts": []int64{100, 200}},
		{"process_guid": "G2", "_chain_ts": []int64{100, 200}},
	}
	// Same timestamps for both entities: only the entity key separates them.
	fetch := func(_ context.Context, _ string) ([]map[string]interface{}, error) {
		return []map[string]interface{}{
			{"_entity_key": "G1", "_ts_ms": int64(100), "log_id": "g1a"},
			{"_entity_key": "G1", "_ts_ms": int64(200), "log_id": "g1b"},
			{"_entity_key": "G2", "_ts_ms": int64(100), "log_id": "g2a"},
			{"_entity_key": "G2", "_ts_ms": int64(200), "log_id": "g2b"},
		}, nil
	}

	Hydrate(context.Background(), chainMeta(), rows, parser.QueryOptions{FractalID: "f"}, fetch)

	for i, want := range [][]string{{"g1a", "g1b"}, {"g2a", "g2b"}} {
		events, _ := rows[i][EventsColumn].([]map[string]interface{})
		if len(events) != 2 {
			t.Fatalf("row %d: expected 2 events, got %d", i, len(events))
		}
		for j, id := range want {
			if events[j]["log_id"] != id {
				t.Errorf("row %d step %d: got %v, want %s", i, j+1, events[j]["log_id"], id)
			}
		}
	}
}

func TestHydrateDegradesOnFetchFailure(t *testing.T) {
	rows := []map[string]interface{}{{"process_guid": "G1", "_chain_ts": []int64{100}}}
	fetch := func(_ context.Context, _ string) ([]map[string]interface{}, error) {
		return nil, context.DeadlineExceeded
	}

	Hydrate(context.Background(), chainMeta(), rows, parser.QueryOptions{FractalID: "f"}, fetch)

	if len(rows) != 1 {
		t.Fatalf("rows must survive a failed lookup, got %d", len(rows))
	}
	if _, present := rows[0][EventsColumn]; present {
		t.Error("a failed lookup must not attach events")
	}
}

func TestBuildChainEventsSQLScopesAndPrunes(t *testing.T) {
	f := parser.BuildChainEventsSQL(chainMeta(), []string{"G1"}, [][]int64{{100, 200}},
		parser.QueryOptions{FractalID: "frac-1"})
	if f == nil {
		t.Fatal("expected a fetch plan")
	}
	for _, want := range []string{
		"fromUnixTimestamp64Milli(toInt64(100))", // primary-key pruning
		"fromUnixTimestamp64Milli(toInt64(200))",
		"fractal_id = 'frac-1'",                   // never reads across fractals
		"fields.`process_guid`::String IN ('G1')", // narrowed to the page
		"(c1 OR c2)", // drops same-ms non-chain events
	} {
		if !strings.Contains(f.SQL, want) {
			t.Errorf("fetch SQL missing %q: %s", want, f.SQL)
		}
	}
}

func TestBuildChainEventsSQLCapsEntities(t *testing.T) {
	var entities []string
	var anchors [][]int64
	for i := 0; i < 600; i++ {
		entities = append(entities, "e")
		anchors = append(anchors, []int64{int64(i + 1)})
	}
	f := parser.BuildChainEventsSQL(chainMeta(), entities, anchors, parser.QueryOptions{FractalID: "f"})
	if f == nil {
		t.Fatal("expected a fetch plan")
	}
	if f.Truncated != 100 {
		t.Errorf("expected 100 entities reported as dropped, got %d", f.Truncated)
	}
}

// The fetch must never alias anything to `timestamp`: ClickHouse alias scope is
// global within a SELECT, so such an alias shadows the real DateTime64 column and
// toUnixTimestamp64Milli() is handed a String (code 43).
func TestBuildChainEventsSQLDoesNotShadowTimestamp(t *testing.T) {
	f := parser.BuildChainEventsSQL(chainMeta(), []string{"G1"}, [][]int64{{100}},
		parser.QueryOptions{FractalID: "f"})
	if f == nil {
		t.Fatal("expected a fetch plan")
	}
	if strings.Contains(f.SQL, "AS timestamp") {
		t.Errorf("fetch SQL aliases over the timestamp column: %s", f.SQL)
	}
	if !strings.Contains(f.SQL, "toUnixTimestamp64Milli(timestamp) AS _ts_ms") {
		t.Errorf("fetch SQL must convert the real timestamp column: %s", f.SQL)
	}
}

func TestHydrateFormatsTimestampFromMillis(t *testing.T) {
	rows := []map[string]interface{}{{"process_guid": "G1", "_chain_ts": []int64{1786502488211}}}
	fetch := func(_ context.Context, _ string) ([]map[string]interface{}, error) {
		return []map[string]interface{}{
			{"_entity_key": "G1", "_ts_ms": int64(1786502488211), "log_id": "a"},
		}, nil
	}
	Hydrate(context.Background(), chainMeta(), rows, parser.QueryOptions{FractalID: "f"}, fetch)

	events, _ := rows[0][EventsColumn].([]map[string]interface{})
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if got := events[0]["timestamp"]; got != "2026-08-12 02:41:28.211" {
		t.Errorf("timestamp = %v, want 2026-08-12 02:41:28.211 (millisecond precision preserved)", got)
	}
}

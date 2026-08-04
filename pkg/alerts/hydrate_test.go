package alerts

import (
	"context"
	"errors"
	"testing"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

type fakeHydrator struct {
	fields map[string]map[string]interface{}
	err    error

	gotTable    string
	gotByIngest bool
	gotKeys     []storage.LogKey
}

func (f *fakeHydrator) HydrateLogFields(_ context.Context, table string, byIngest bool, keys []storage.LogKey, _, _ time.Time) (map[string]map[string]interface{}, error) {
	f.gotTable, f.gotByIngest, f.gotKeys = table, byIngest, keys
	return f.fields, f.err
}

func prunedRow(logID, eventID string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":  "2026-08-03 10:00:00.000",
		"log_id":     logID,
		"fractal_id": "f1",
		"event_id":   eventID,
	}
}

// TestIsPrunedLogRow: hydration must fire only for rows the alert auto-projection
// stripped. A command pipeline keeps the full projection and carries norm_log as
// a raw string, so hydrating it would be a wasted query.
func TestIsPrunedLogRow(t *testing.T) {
	tests := []struct {
		name string
		row  map[string]interface{}
		want bool
	}{
		{"pruned log row", prunedRow("a", "4624"), true},
		{"aggregate row has no log_id", map[string]interface{}{"count": uint64(3)}, false},
		{"empty log_id", map[string]interface{}{"log_id": ""}, false},
		{"already carries fields", map[string]interface{}{"log_id": "a", "fields": map[string]interface{}{"x": "1"}}, false},
		{"command pipeline carries norm_log", map[string]interface{}{"log_id": "a", "norm_log": `{"x":"1"}`}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPrunedLogRow(tc.row); got != tc.want {
				t.Errorf("isPrunedLogRow = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHydrationLimit(t *testing.T) {
	tests := []struct {
		name            string
		actions         []FractalAction
		alertsFractalID string
		want            int
	}{
		{"no consumers", nil, "", 0},
		{"disabled action only", []FractalAction{{Enabled: false, MaxLogsPerTrigger: 50}}, "", 0},
		{"explicit cap", []FractalAction{{Enabled: true, MaxLogsPerTrigger: 50}}, "", 50},
		{"largest cap wins", []FractalAction{
			{Enabled: true, MaxLogsPerTrigger: 50},
			{Enabled: true, MaxLogsPerTrigger: 200},
		}, "", 200},
		{"unlimited action clamps to ceiling", []FractalAction{{Enabled: true, MaxLogsPerTrigger: 0}}, "", hydrationRowCeiling},
		{"over-ceiling cap clamps", []FractalAction{{Enabled: true, MaxLogsPerTrigger: 5000}}, "", hydrationRowCeiling},
		{"system fractal alone", nil, "sys", hydrationRowCeiling},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := &Engine{alertsFractalID: tc.alertsFractalID}
			got := e.hydrationLimit(context.Background(), &Alert{FractalActions: tc.actions})
			if got != tc.want {
				t.Errorf("hydrationLimit = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestHydrateRowsDoesNotMutateInput is what protects every other consumer:
// webhooks, email, dictionaries, and the throttle all keep reading `results`,
// and dictionaries in particular rebuild their ClickHouse schema from whatever
// keys they observe.
func TestHydrateRowsDoesNotMutateInput(t *testing.T) {
	results := []map[string]interface{}{prunedRow("a", "4624")}
	e := &Engine{hydrator: &fakeHydrator{fields: map[string]map[string]interface{}{
		"a": {"event_id": "4624", "user": "zane", "computer_name": "host1"},
	}}}

	hydrated := e.hydrateRows(context.Background(), results, parser.QueryOptions{TableName: "logs_hot"}, 10)

	if _, ok := results[0]["fields"]; ok {
		t.Error("input row was mutated")
	}
	if len(results[0]) != 4 {
		t.Errorf("input row gained keys: %v", results[0])
	}
	f, ok := hydrated[0]["fields"].(map[string]interface{})
	if !ok {
		t.Fatalf("hydrated row has no fields map: %v", hydrated[0])
	}
	if f["user"] != "zane" {
		t.Errorf("fields = %v, want the full field set", f)
	}
}

func TestHydrateRowsPreservesOrderAndPassesThroughMisses(t *testing.T) {
	results := []map[string]interface{}{prunedRow("a", "1"), prunedRow("b", "2"), prunedRow("c", "3")}
	e := &Engine{hydrator: &fakeHydrator{fields: map[string]map[string]interface{}{
		"b": {"user": "zane"},
	}}}

	hydrated := e.hydrateRows(context.Background(), results, parser.QueryOptions{TableName: "logs"}, 10)

	if len(hydrated) != 3 {
		t.Fatalf("got %d rows, want 3", len(hydrated))
	}
	for i, want := range []string{"a", "b", "c"} {
		if hydrated[i]["log_id"] != want {
			t.Errorf("row %d is %v, want %s", i, hydrated[i]["log_id"], want)
		}
	}
	if _, ok := hydrated[0]["fields"]; ok {
		t.Error("a miss should pass through unhydrated")
	}
	if _, ok := hydrated[1]["fields"]; !ok {
		t.Error("a hit should be hydrated")
	}
}

// TestHydrateRowsDegrades: a hydration failure must never drop the forward.
func TestHydrateRowsDegrades(t *testing.T) {
	results := []map[string]interface{}{prunedRow("a", "1")}
	opts := parser.QueryOptions{TableName: "logs"}

	for _, tc := range []struct {
		name string
		h    logHydrator
	}{
		{"no hydrator", nil},
		{"query error", &fakeHydrator{err: errors.New("clickhouse down")}},
		{"nothing found", &fakeHydrator{fields: map[string]map[string]interface{}{}}},
		{"empty field map", &fakeHydrator{fields: map[string]map[string]interface{}{"a": {}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := e2(tc.h).hydrateRows(context.Background(), results, opts, 10)
			if len(got) != 1 || got[0]["log_id"] != "a" {
				t.Fatalf("rows lost: %v", got)
			}
			if _, ok := got[0]["fields"]; ok {
				t.Error("should not have attached an empty fields map")
			}
		})
	}
}

func e2(h logHydrator) *Engine { return &Engine{hydrator: h} }

// TestHydrateRowsRoutesToEvaluationTable: hydrating from logs what was matched on
// logs_hot_distributed would silently return nothing on every cluster install.
func TestHydrateRowsRoutesToEvaluationTable(t *testing.T) {
	results := []map[string]interface{}{prunedRow("a", "1")}
	for _, tc := range []struct {
		table        string
		wantByIngest bool
	}{
		{"logs_hot", true},
		{"logs_hot_distributed", true},
		{"logs", false},
		{"logs_distributed", false},
	} {
		t.Run(tc.table, func(t *testing.T) {
			h := &fakeHydrator{}
			e2(h).hydrateRows(context.Background(), results, parser.QueryOptions{TableName: tc.table}, 10)
			if h.gotTable != tc.table {
				t.Errorf("hydrated from %q, want %q", h.gotTable, tc.table)
			}
			if h.gotByIngest != tc.wantByIngest {
				t.Errorf("byIngest = %v, want %v", h.gotByIngest, tc.wantByIngest)
			}
		})
	}
}

// TestHydrateRowsRespectsLimit: rows beyond what the actions will forward are not
// worth a fat column read.
func TestHydrateRowsRespectsLimit(t *testing.T) {
	results := []map[string]interface{}{prunedRow("a", "1"), prunedRow("b", "2"), prunedRow("c", "3")}
	h := &fakeHydrator{fields: map[string]map[string]interface{}{"a": {"user": "zane"}}}

	hydrated := e2(h).hydrateRows(context.Background(), results, parser.QueryOptions{TableName: "logs"}, 2)

	if len(h.gotKeys) != 2 {
		t.Errorf("requested %d keys, want 2", len(h.gotKeys))
	}
	if len(hydrated) != 3 {
		t.Errorf("got %d rows, want all 3 forwarded", len(hydrated))
	}
}

func TestHydrateRowsParsesRowTimestamp(t *testing.T) {
	h := &fakeHydrator{}
	e2(h).hydrateRows(context.Background(), []map[string]interface{}{prunedRow("a", "1")},
		parser.QueryOptions{TableName: "logs"}, 10)

	if len(h.gotKeys) != 1 {
		t.Fatalf("got %d keys, want 1", len(h.gotKeys))
	}
	if h.gotKeys[0].Timestamp.IsZero() {
		t.Error("timestamp did not parse; the cold path would drop this key")
	}
	if h.gotKeys[0].FractalID != "f1" {
		t.Errorf("fractal_id = %q, want f1", h.gotKeys[0].FractalID)
	}
}

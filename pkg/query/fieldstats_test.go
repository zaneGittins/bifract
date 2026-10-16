package query

import (
	"fmt"
	"testing"
)

func TestFieldStatsFromRows(t *testing.T) {
	rows := []map[string]interface{}{
		{"key": "host", "present": uint64(90), "cardinality": uint64(3),
			"top_values": []string{"b", "a", "c"}, "top_counts": []uint64{10, 70, 10}},
		{"key": "__rows__", "present": uint64(100)},
		// A short counts array must not panic or shift values onto the wrong counts.
		{"key": "user", "present": int64(5), "cardinality": float64(2),
			"top_values": []string{"x", "y"}, "top_counts": []uint64{4}},
		{"key": "empty", "present": int64(-1)},
	}
	resp := fieldStatsFromRows(rows, 50000)
	if !resp.Success || !resp.Supported || resp.SampleSize != 100 || resp.Approximate {
		t.Fatalf("header = %+v", resp)
	}
	if len(resp.Fields) != 3 {
		t.Fatalf("the sentinel row became a field: %+v", resp.Fields)
	}
	host := resp.Fields[0]
	if host.Name != "host" || host.Present != 90 || host.Cardinality != 3 {
		t.Errorf("host = %+v", host)
	}
	// groupArray order is arbitrary; values are presented count-descending, and
	// equal counts keep their order.
	if got := fmt.Sprint(host.Top); got != "[{a 70} {b 10} {c 10}]" {
		t.Errorf("host top = %s", got)
	}
	if got := fmt.Sprint(resp.Fields[1].Top); got != "[{x 4} {y 0}]" || resp.Fields[1].Cardinality != 2 {
		t.Errorf("user = %+v", resp.Fields[1])
	}
	if resp.Fields[2].Present != 0 {
		t.Errorf("a negative count must clamp to zero, got %d", resp.Fields[2].Present)
	}
}

// Reaching the sample cap means more rows matched than were scanned, so the
// distribution is labelled approximate.
func TestFieldStatsApproximateAtCap(t *testing.T) {
	resp := fieldStatsFromRows([]map[string]interface{}{{"key": "__rows__", "present": uint64(500)}}, 500)
	if !resp.Approximate {
		t.Error("a full sample was not marked approximate")
	}
	if resp.Fields == nil {
		t.Error("no fields must encode as an empty array, not null")
	}
}

func TestFieldStatsSampleSize(t *testing.T) {
	for env, want := range map[string]int{"": 50000, "1200": 1200, "0": 50000, "-5": 50000, "many": 50000} {
		t.Setenv("BIFRACT_FIELDSTATS_SAMPLE", env)
		if got := fieldStatsSampleSize(); got != want {
			t.Errorf("BIFRACT_FIELDSTATS_SAMPLE=%q -> %d, want %d", env, got, want)
		}
	}
}

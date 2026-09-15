package alerts

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func rows(field string, values ...string) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(values))
	for _, v := range values {
		out = append(out, map[string]interface{}{field: v, "log_id": v})
	}
	return out
}

func blockedSet(keys ...string) func([]string) map[string]struct{} {
	return func([]string) map[string]struct{} {
		set := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			set[k] = struct{}{}
		}
		return set
	}
}

// A throttle field suppresses per value: the batch used to be decided by whichever
// row happened to come back first, which delivered every value whenever that row's
// value was new and suppressed every value whenever it was not.
func TestThrottleSplitPerValue(t *testing.T) {
	alert := &Alert{ID: "a1", ThrottleTimeSeconds: 60, ThrottleField: "host"}
	res := throttleSplit(alert, rows("host", "web1", "web2", "web1", "web3"), blockedSet("host=web1"))

	if got := len(res.deliver); got != 2 {
		t.Fatalf("delivered %d rows, want 2", got)
	}
	for _, row := range res.deliver {
		if row["host"] == "web1" {
			t.Fatalf("delivered a suppressed value: %v", row)
		}
	}
	if res.suppressed != 2 {
		t.Errorf("suppressed = %d, want 2", res.suppressed)
	}
	if len(res.keys) != 2 || res.keys[0] != "host=web2" || res.keys[1] != "host=web3" {
		t.Errorf("windows to open = %v, want the two delivered values", res.keys)
	}
	if res.auditKey != "host (3 values)" {
		t.Errorf("auditKey = %q", res.auditKey)
	}
}

func TestThrottleSplitGlobal(t *testing.T) {
	alert := &Alert{ID: "a1", ThrottleTimeSeconds: 60}

	open := throttleSplit(alert, rows("host", "web1", "web2"), blockedSet())
	if len(open.deliver) != 2 || len(open.keys) != 1 || open.keys[0] != globalThrottleKey {
		t.Fatalf("open window: delivered %d rows, keys %v", len(open.deliver), open.keys)
	}

	closed := throttleSplit(alert, rows("host", "web1", "web2"), blockedSet(globalThrottleKey))
	if len(closed.deliver) != 0 || closed.suppressed != 2 {
		t.Fatalf("closed window: delivered %d rows, suppressed %d", len(closed.deliver), closed.suppressed)
	}
	if len(closed.keys) != 0 {
		t.Errorf("suppressed batch must not extend its own window: %v", closed.keys)
	}
}

// An alert with no throttle reads no state and delivers everything.
func TestThrottleSplitDisabled(t *testing.T) {
	alert := &Alert{ID: "a1", ThrottleField: "host"}
	res := throttleSplit(alert, rows("host", "web1"), func([]string) map[string]struct{} {
		t.Fatal("looked up throttle state for an unthrottled alert")
		return nil
	})
	if len(res.deliver) != 1 || len(res.keys) != 0 {
		t.Fatalf("delivered %d rows, keys %v", len(res.deliver), res.keys)
	}
}

// A row missing the throttle field falls back to the global key rather than
// sharing a window with an unrelated value.
func TestRowThrottleKeyMissingField(t *testing.T) {
	alert := &Alert{ID: "a1", ThrottleTimeSeconds: 60, ThrottleField: "host"}
	if got := rowThrottleKey(alert, map[string]interface{}{"log_id": "x"}); got != globalThrottleKey {
		t.Errorf("key = %q, want %q", got, globalThrottleKey)
	}
}

// Long values are cut on a rune boundary (an invalid UTF-8 sequence is rejected
// by the text column) and stay distinct from another value sharing their prefix.
func TestRowThrottleKeyTruncation(t *testing.T) {
	alert := &Alert{ID: "a1", ThrottleTimeSeconds: 60, ThrottleField: "cmd"}
	prefix := strings.Repeat("é", maxThrottleKeyLen)

	a := rowThrottleKey(alert, map[string]interface{}{"cmd": prefix + "one"})
	b := rowThrottleKey(alert, map[string]interface{}{"cmd": prefix + "two"})

	if len(a) > maxThrottleKeyLen {
		t.Errorf("key is %d bytes, want at most %d", len(a), maxThrottleKeyLen)
	}
	if !utf8.ValidString(a) {
		t.Errorf("key is not valid UTF-8: %q", a)
	}
	if a == b {
		t.Error("two values sharing a prefix collapsed into one window")
	}
}

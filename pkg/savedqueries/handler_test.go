package savedqueries

import "testing"

func TestRangeArgsKeepsLive(t *testing.T) {
	tr, cs, ce, rn, ru := rangeArgs("live", "", "", "", nil)
	if tr != "live" {
		t.Fatalf("a saved Live search must reopen live, got time_range %v", tr)
	}
	if cs != nil || ce != nil || rn != nil || ru != nil {
		t.Errorf("Live carries no bounds, got %v %v %v %v", cs, ce, rn, ru)
	}
}

func TestRangeArgsKeepsNamedRanges(t *testing.T) {
	for _, r := range []string{"4h", "14d", "today", "yesterday", "this_week", "last_week", "this_month", "last_month"} {
		if tr, _, _, _, _ := rangeArgs(r, "", "", "", nil); tr != r {
			t.Errorf("a saved %q search must reopen as %q, got %v", r, r, tr)
		}
	}
}

func TestRangeArgsDropsRelativeOverflowingColumn(t *testing.T) {
	n := 1 << 31
	if _, _, _, rn, ru := rangeArgs("relative", "", "", "minutes", &n); rn != nil || ru != nil {
		t.Errorf("relative_n is an INTEGER column, %d must not reach it, got %v %v", n, rn, ru)
	}
}

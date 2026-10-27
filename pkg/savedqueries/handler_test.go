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

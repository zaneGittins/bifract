package ruleeval

import (
	"strings"
	"testing"
)

func TestVerdict(t *testing.T) {
	two, zero := 2, 0
	cases := []struct {
		name      string
		expect    Expectation
		matched   bool
		rows      int
		wantCount *int
		pass      bool
		why       string
	}{
		{"match expected and found", ExpectMatch, true, 3, nil, true, ""},
		{"match expected, none", ExpectMatch, false, 0, nil, false, "returned no rows"},
		{"count pinned and equal", ExpectMatch, true, 2, &two, true, ""},
		{"count pinned and different", ExpectMatch, true, 3, &two, false, "expected 2 result rows, got 3"},
		// A pinned count of zero still requires a match: "match" with zero rows is
		// a contradiction, and failing loudly beats passing vacuously.
		{"count pinned at zero", ExpectMatch, false, 0, &zero, false, "returned no rows"},
		{"no match expected, none", ExpectNoMatch, false, 0, nil, true, ""},
		{"no match expected, fired", ExpectNoMatch, true, 4, nil, false, "returned 4 row(s)"},
		// Any expectation other than "match" is judged as no-match.
		{"unknown expectation", Expectation("maybe"), true, 1, nil, false, "not to trigger"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pass, why := Verdict(tc.expect, tc.matched, tc.rows, tc.wantCount)
			if pass != tc.pass {
				t.Fatalf("pass = %v, want %v (%s)", pass, tc.pass, why)
			}
			if tc.why == "" && why != "" {
				t.Fatalf("a pass must carry no explanation, got %q", why)
			}
			if !strings.Contains(why, tc.why) {
				t.Fatalf("explanation %q must contain %q", why, tc.why)
			}
		})
	}
}

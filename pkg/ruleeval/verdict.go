package ruleeval

import "fmt"

// Expectation is what a test asserts about the rule's verdict.
type Expectation string

const (
	ExpectMatch   Expectation = "match"
	ExpectNoMatch Expectation = "no_match"
)

// Verdict compares an observed result to an expectation and explains a failure.
// wantCount, when set, additionally pins the number of result rows.
func Verdict(expect Expectation, matched bool, rows int, wantCount *int) (bool, string) {
	switch expect {
	case ExpectMatch:
		if !matched {
			return false, "expected the rule to trigger, but it returned no rows"
		}
		if wantCount != nil && rows != *wantCount {
			return false, fmt.Sprintf("expected %d result rows, got %d", *wantCount, rows)
		}
		return true, ""
	default:
		if matched {
			return false, fmt.Sprintf("expected the rule not to trigger, but it returned %d row(s)", rows)
		}
		return true, ""
	}
}

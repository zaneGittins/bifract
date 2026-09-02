package alerts

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"bifract/pkg/ruleeval"
)

// Policy severities.
const (
	PolicyWarn  = "warn"
	PolicyBlock = "block"
)

// MaxPoliciesPerFractal bounds the rule set. Every rule runs on every keystroke in the
// editor, and a list nobody can read is not a policy anyone follows.
const MaxPoliciesPerFractal = 50

// Policy is one assertion about an alert's definition.
type Policy struct {
	ID       string `json:"id,omitempty"`
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
	// Message is what an analyst reads when the rule fails. It should say what to do,
	// not restate the rule.
	Message  string `json:"message"`
	Severity string `json:"severity"`
	Enabled  bool   `json:"enabled"`
	Position int    `json:"position"`
}

// Violation is one failed rule against one alert.
type Violation struct {
	PolicyID string `json:"policy_id,omitempty"`
	Field    string `json:"field"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
	// Detail states what was actually found, so a message that says what to do does
	// not also have to say what went wrong.
	Detail string `json:"detail,omitempty"`
}

// PolicyResult is a whole evaluation.
type PolicyResult struct {
	Violations []Violation `json:"violations"`
	Blocking   int         `json:"blocking"`
	Warnings   int         `json:"warnings"`
	// Deferred names rules that could not run in this pass because they need the
	// alert's tests evaluated. They resolve on save.
	Deferred []string `json:"deferred,omitempty"`
}

// OK reports whether a save may proceed.
func (r *PolicyResult) OK() bool { return r.Blocking == 0 }

// Validate checks a rule is well formed before it is stored.
func (p *Policy) Validate() error {
	field, ok := LookupField(p.Field)
	if !ok {
		return fmt.Errorf("unknown field %q (expected one of: %s)", p.Field, FieldNames())
	}
	if !operatorAllowed(field.Type, p.Operator) {
		return fmt.Errorf("operator %q does not apply to %s, which is a %s field",
			p.Operator, p.Field, field.Type)
	}
	if p.Severity != PolicyWarn && p.Severity != PolicyBlock {
		return fmt.Errorf("severity must be %q or %q", PolicyWarn, PolicyBlock)
	}
	if strings.TrimSpace(p.Message) == "" {
		return fmt.Errorf("a message is required: it is what an analyst sees when the rule fails")
	}

	if !operatorNeedsValue(p.Operator) {
		return nil
	}
	if strings.TrimSpace(p.Value) == "" {
		return fmt.Errorf("operator %q needs a value", p.Operator)
	}

	switch p.Operator {
	case "matches", "not_matches", "any_matches", "all_match", "none_match":
		if _, err := regexp.Compile(p.Value); err != nil {
			return fmt.Errorf("invalid regex %q: %w", p.Value, err)
		}
	case "min_length", "max_length", "min_count", "max_count", "gte", "lte", "gt", "lt":
		if _, err := strconv.ParseFloat(strings.TrimSpace(p.Value), 64); err != nil {
			return fmt.Errorf("operator %q needs a number, got %q", p.Operator, p.Value)
		}
	}
	return nil
}

// ValidatePolicies checks a whole rule set.
func ValidatePolicies(policies []Policy) error {
	if len(policies) > MaxPoliciesPerFractal {
		return fmt.Errorf("at most %d policy rules per fractal", MaxPoliciesPerFractal)
	}
	for i := range policies {
		if err := policies[i].Validate(); err != nil {
			return fmt.Errorf("rule %d: %w", i+1, err)
		}
	}
	return nil
}

// PolicySubject is everything a rule set can read about an alert.
//
// It is deliberately not the Alert struct: policies assert about the definition and a
// few facts about its tests, and handing the evaluator the whole record would invite
// rules that depend on runtime state like last_triggered.
type PolicySubject struct {
	Content RevisionContent
	Tests   []AlertTest
	// TestsRun and TestsPassing are set only when the tests have actually been
	// evaluated. Rules that read them are deferred otherwise.
	TestsRun     bool
	TestsPassing bool
}

// NewPolicySubject builds a subject from an update request as it is about to be saved.
func NewPolicySubject(content RevisionContent, tests []AlertTest) PolicySubject {
	return PolicySubject{Content: content, Tests: tests}
}

// EvaluatePolicies runs a rule set against an alert.
//
// Rules needing test outcomes are reported in Deferred rather than failed when the
// tests have not been run, so the editor's live pass does not accuse an author of
// failing a check it never evaluated.
func EvaluatePolicies(policies []Policy, subject PolicySubject) PolicyResult {
	result := PolicyResult{Violations: []Violation{}}

	for i := range policies {
		policy := policies[i]
		if !policy.Enabled {
			continue
		}

		field, ok := LookupField(policy.Field)
		if !ok {
			continue // a field removed from the catalog stops being enforced, not fatal
		}
		if field.RunsTests && !subject.TestsRun {
			result.Deferred = append(result.Deferred, policy.Field)
			continue
		}

		passed, detail := evaluatePolicy(policy, field, subject)
		if passed {
			continue
		}

		result.Violations = append(result.Violations, Violation{
			PolicyID: policy.ID,
			Field:    policy.Field,
			Severity: policy.Severity,
			Message:  policy.Message,
			Detail:   detail,
		})
		if policy.Severity == PolicyBlock {
			result.Blocking++
		} else {
			result.Warnings++
		}
	}

	return result
}

// evaluatePolicy returns whether the rule holds, and what was found when it does not.
func evaluatePolicy(policy Policy, field PolicyField, subject PolicySubject) (bool, string) {
	switch field.Type {
	case FieldString:
		return evaluateString(policy, subjectString(field.Name, subject))
	case FieldList:
		return evaluateList(policy, subjectList(field.Name, subject))
	case FieldNumber:
		return evaluateNumber(policy, subjectNumber(field.Name, subject))
	case FieldBoolean:
		return evaluateBoolean(policy, subjectBoolean(field.Name, subject))
	}
	return true, ""
}

func evaluateString(policy Policy, value string) (bool, string) {
	switch policy.Operator {
	case "not_empty":
		if strings.TrimSpace(value) == "" {
			return false, "it is empty"
		}
	case "min_length":
		n := policyNumber(policy.Value)
		if float64(len([]rune(value))) < n {
			return false, fmt.Sprintf("it is %d characters, minimum is %s", len([]rune(value)), policy.Value)
		}
	case "max_length":
		n := policyNumber(policy.Value)
		if float64(len([]rune(value))) > n {
			return false, fmt.Sprintf("it is %d characters, maximum is %s", len([]rune(value)), policy.Value)
		}
	case "matches":
		re, err := regexp.Compile(policy.Value)
		if err != nil {
			return true, ""
		}
		if !re.MatchString(value) {
			return false, fmt.Sprintf("it does not match %s", policy.Value)
		}
	case "not_matches":
		re, err := regexp.Compile(policy.Value)
		if err != nil {
			return true, ""
		}
		if re.MatchString(value) {
			return false, fmt.Sprintf("it matches %s", policy.Value)
		}
	case "equals":
		if value != policy.Value {
			return false, fmt.Sprintf("it is %q", value)
		}
	case "not_equals":
		if value == policy.Value {
			return false, fmt.Sprintf("it is %q", value)
		}
	case "one_of":
		for _, candidate := range splitList(policy.Value) {
			if value == candidate {
				return true, ""
			}
		}
		return false, fmt.Sprintf("it is %q", value)
	}
	return true, ""
}

func evaluateList(policy Policy, values []string) (bool, string) {
	switch policy.Operator {
	case "not_empty":
		if len(values) == 0 {
			return false, "it is empty"
		}
	case "min_count":
		if float64(len(values)) < policyNumber(policy.Value) {
			return false, fmt.Sprintf("it has %d, minimum is %s", len(values), policy.Value)
		}
	case "max_count":
		if float64(len(values)) > policyNumber(policy.Value) {
			return false, fmt.Sprintf("it has %d, maximum is %s", len(values), policy.Value)
		}
	case "any_matches":
		re, err := regexp.Compile(policy.Value)
		if err != nil {
			return true, ""
		}
		for _, v := range values {
			if re.MatchString(v) {
				return true, ""
			}
		}
		return false, fmt.Sprintf("nothing matches %s (have: %s)", policy.Value, joinForDetail(values))
	case "all_match":
		re, err := regexp.Compile(policy.Value)
		if err != nil {
			return true, ""
		}
		for _, v := range values {
			if !re.MatchString(v) {
				return false, fmt.Sprintf("%q does not match %s", v, policy.Value)
			}
		}
	case "none_match":
		re, err := regexp.Compile(policy.Value)
		if err != nil {
			return true, ""
		}
		for _, v := range values {
			if re.MatchString(v) {
				return false, fmt.Sprintf("%q matches %s", v, policy.Value)
			}
		}
	}
	return true, ""
}

func evaluateNumber(policy Policy, value float64) (bool, string) {
	want := policyNumber(policy.Value)
	found := strconv.FormatFloat(value, 'f', -1, 64)

	switch policy.Operator {
	case "gte":
		if value < want {
			return false, "it is " + found
		}
	case "lte":
		if value > want {
			return false, "it is " + found
		}
	case "gt":
		if value <= want {
			return false, "it is " + found
		}
	case "lt":
		if value >= want {
			return false, "it is " + found
		}
	case "equals":
		if value != want {
			return false, "it is " + found
		}
	case "not_equals":
		if value == want {
			return false, "it is " + found
		}
	}
	return true, ""
}

func evaluateBoolean(policy Policy, value bool) (bool, string) {
	switch policy.Operator {
	case "is_true":
		if !value {
			return false, "it is false"
		}
	case "is_false":
		if value {
			return false, "it is true"
		}
	}
	return true, ""
}

func subjectString(name string, s PolicySubject) string {
	switch name {
	case "name":
		return s.Content.Name
	case "description":
		return s.Content.Description
	case "query_string":
		return s.Content.QueryString
	case "alert_type":
		return s.Content.AlertType
	case "severity":
		return s.Content.Severity
	case "throttle_field":
		return s.Content.ThrottleField
	case "schedule_cron":
		if s.Content.ScheduleCron != nil {
			return *s.Content.ScheduleCron
		}
	}
	return ""
}

func subjectList(name string, s PolicySubject) []string {
	switch name {
	case "labels":
		return s.Content.Labels
	case "references":
		return s.Content.References
	}
	return nil
}

func subjectNumber(name string, s PolicySubject) float64 {
	switch name {
	case "throttle_time_seconds":
		return float64(s.Content.ThrottleTimeSeconds)
	case "window_duration":
		if s.Content.WindowDuration != nil {
			return float64(*s.Content.WindowDuration)
		}
	case "query_window_seconds":
		if s.Content.QueryWindowSeconds != nil {
			return float64(*s.Content.QueryWindowSeconds)
		}
	case "actions.count":
		return float64(len(s.Content.WebhookActionIDs) + len(s.Content.FractalActionIDs) +
			len(s.Content.DictionaryActionIDs) + len(s.Content.EmailActionIDs))
	case "tests.count":
		return float64(len(s.Tests))
	case "tests.match_count":
		return float64(countTests(s.Tests, string(ruleeval.ExpectMatch)))
	case "tests.no_match_count":
		return float64(countTests(s.Tests, string(ruleeval.ExpectNoMatch)))
	}
	return 0
}

func subjectBoolean(name string, s PolicySubject) bool {
	if name == "tests.all_passing" {
		return s.TestsPassing
	}
	return false
}

func countTests(tests []AlertTest, expectation string) int {
	n := 0
	for i := range tests {
		if tests[i].Expectation == expectation {
			n++
		}
	}
	return n
}

func policyNumber(value string) float64 {
	n, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return n
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// joinForDetail renders a list for an error message without flooding it.
func joinForDetail(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	const maxShown = 5
	shown := values
	suffix := ""
	if len(shown) > maxShown {
		shown = shown[:maxShown]
		suffix = fmt.Sprintf(" and %d more", len(values)-maxShown)
	}
	return strings.Join(shown, ", ") + suffix
}

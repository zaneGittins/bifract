package alerts

import (
	"sort"
	"strings"
)

// FieldType decides which operators a field accepts, and how its value is compared.
type FieldType string

const (
	FieldString  FieldType = "string"
	FieldList    FieldType = "list"
	FieldNumber  FieldType = "number"
	FieldBoolean FieldType = "boolean"
)

// PolicyField is one thing a policy can assert about.
//
// The catalog is the alert's own definition plus a few facts about its tests. Nothing
// here needs a query over log data: a check has to be cheap enough to run on every
// keystroke in the editor, which is the whole point of moving it out of a CI job.
type PolicyField struct {
	Name string    `json:"name"`
	Type FieldType `json:"type"`
	// Label names the field in the policy editor.
	Label string `json:"label"`
	// Help explains what the field holds, shown under the picker.
	Help string `json:"help,omitempty"`
	// RunsTests marks a field whose value is only known after the alert's tests have
	// been evaluated. Those fields are skipped in the editor's live pass and resolved
	// on save.
	RunsTests bool `json:"runs_tests,omitempty"`
}

// policyFields is the catalog, keyed by field name.
var policyFields = map[string]PolicyField{
	"name":        {Name: "name", Type: FieldString, Label: "Name", Help: "The alert name"},
	"description": {Name: "description", Type: FieldString, Label: "Description", Help: "What it detects and why"},
	"query_string": {Name: "query_string", Type: FieldString, Label: "Query",
		Help: "The BQL it evaluates"},
	"alert_type": {Name: "alert_type", Type: FieldString, Label: "Alert type", Help: "event, compound or scheduled"},
	"severity":   {Name: "severity", Type: FieldString, Label: "Severity", Help: "info, low, medium, high, critical"},

	"labels":     {Name: "labels", Type: FieldList, Label: "Labels", Help: "Labels, including ATT&CK tags"},
	"references": {Name: "references", Type: FieldList, Label: "References", Help: "Links behind the rule"},

	"throttle_time_seconds": {Name: "throttle_time_seconds", Type: FieldNumber, Label: "Throttle (seconds)", Help: "0 means no throttle"},
	"throttle_field":        {Name: "throttle_field", Type: FieldString, Label: "Throttle field", Help: "Field the throttle groups on"},
	"window_duration":       {Name: "window_duration", Type: FieldNumber, Label: "Window (seconds)", Help: "Compound correlation window"},
	"query_window_seconds":  {Name: "query_window_seconds", Type: FieldNumber, Label: "Query window (seconds)", Help: "Scheduled alert lookback"},
	"schedule_cron":         {Name: "schedule_cron", Type: FieldString, Label: "Schedule", Help: "Cron expression"},

	"actions.count": {Name: "actions.count", Type: FieldNumber, Label: "Actions attached", Help: "Actions of every kind, combined"},

	"tests.count":          {Name: "tests.count", Type: FieldNumber, Label: "Tests", Help: "Tests on the alert"},
	"tests.match_count":    {Name: "tests.match_count", Type: FieldNumber, Label: "Should-match tests", Help: "Tests asserting it fires"},
	"tests.no_match_count": {Name: "tests.no_match_count", Type: FieldNumber, Label: "Should-not-match tests", Help: "Tests asserting it stays quiet"},
	"tests.all_passing": {Name: "tests.all_passing", Type: FieldBoolean, Label: "All tests pass", RunsTests: true,
		Help: "Checked on save, not while typing"},
}

// operatorsByType lists what each type accepts, in the order the editor offers them.
var operatorsByType = map[FieldType][]string{
	FieldString:  {"not_empty", "min_length", "max_length", "matches", "not_matches", "equals", "not_equals", "one_of"},
	FieldList:    {"not_empty", "min_count", "max_count", "any_matches", "all_match", "none_match"},
	FieldNumber:  {"gte", "lte", "gt", "lt", "equals", "not_equals"},
	FieldBoolean: {"is_true", "is_false"},
}

// operatorNeedsValue reports whether an operator reads the rule's value.
func operatorNeedsValue(op string) bool {
	switch op {
	case "not_empty", "is_true", "is_false":
		return false
	}
	return true
}

// operatorLabels render an operator in the policy editor.
var operatorLabels = map[string]string{
	"not_empty":   "is set",
	"min_length":  "is at least N characters",
	"max_length":  "is at most N characters",
	"matches":     "matches regex",
	"not_matches": "does not match regex",
	"equals":      "equals",
	"not_equals":  "does not equal",
	"one_of":      "is one of (comma separated)",
	"min_count":   "has at least N entries",
	"max_count":   "has at most N entries",
	"any_matches": "has an entry matching regex",
	"all_match":   "has every entry matching regex",
	"none_match":  "has no entry matching regex",
	"gte":         "is at least",
	"lte":         "is at most",
	"gt":          "is greater than",
	"lt":          "is less than",
	"is_true":     "is true",
	"is_false":    "is false",
}

// PolicyCatalog is what the policy editor needs to build its pickers.
type PolicyCatalog struct {
	Fields    []PolicyField       `json:"fields"`
	Operators map[string][]string `json:"operators"`
	Labels    map[string]string   `json:"operator_labels"`
	NeedValue map[string]bool     `json:"operator_needs_value"`
}

// Catalog returns the field and operator catalog, fields sorted for a stable picker.
func Catalog() PolicyCatalog {
	fields := make([]PolicyField, 0, len(policyFields))
	for _, f := range policyFields {
		fields = append(fields, f)
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })

	operators := make(map[string][]string, len(operatorsByType))
	needValue := make(map[string]bool, len(operatorLabels))
	for t, ops := range operatorsByType {
		operators[string(t)] = ops
		for _, op := range ops {
			needValue[op] = operatorNeedsValue(op)
		}
	}

	return PolicyCatalog{Fields: fields, Operators: operators, Labels: operatorLabels, NeedValue: needValue}
}

// LookupField returns a catalog entry.
func LookupField(name string) (PolicyField, bool) {
	f, ok := policyFields[name]
	return f, ok
}

// operatorAllowed reports whether an operator applies to a field type.
func operatorAllowed(t FieldType, op string) bool {
	for _, candidate := range operatorsByType[t] {
		if candidate == op {
			return true
		}
	}
	return false
}

// FieldNames lists every field name, for error messages.
func FieldNames() string {
	names := make([]string, 0, len(policyFields))
	for name := range policyFields {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

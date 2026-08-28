package alerts

import "strings"

// Severity is how urgent an alert's findings are.
//
// Unlike AlertType it is not enforced on write: alerts imported from Sigma feeds
// arrive with whatever level the rule declared, and rejecting those would drop
// real detections. These are the values the product ranks, colours and filters
// on, and anything else sorts as unknown.
type Severity string

const (
	SeverityCritical      Severity = "critical"
	SeverityHigh          Severity = "high"
	SeverityMedium        Severity = "medium"
	SeverityLow           Severity = "low"
	SeverityInformational Severity = "informational"
)

// EnumValues lists them most urgent first, which is also their rank order.
func (Severity) EnumValues() []string {
	return []string{
		string(SeverityCritical),
		string(SeverityHigh),
		string(SeverityMedium),
		string(SeverityLow),
		string(SeverityInformational),
	}
}

// SeverityFromLevel maps a Sigma rule's level onto a Severity.
//
// Both the alert importer and the feed syncer used to carry their own copy of
// this, and both mapped Sigma's "informational" to "info", which is not a value
// the product ranks, filters or colours: such a rule sorted as unknown in the
// feed table and in the coverage gap ranking. An unrecognised level is medium,
// which is what a rule that declares nothing gets.
func SeverityFromLevel(level string) Severity {
	switch Severity(strings.ToLower(strings.TrimSpace(level))) {
	case SeverityCritical:
		return SeverityCritical
	case SeverityHigh:
		return SeverityHigh
	case SeverityLow:
		return SeverityLow
	case SeverityInformational:
		return SeverityInformational
	default:
		return SeverityMedium
	}
}

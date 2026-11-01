package alerts

import (
	"fmt"
	"strings"
)

// Severity is how urgent an alert's findings are. Writes are held to these
// values because the UI renders them into markup; Sigma levels go through
// SeverityFromLevel, which never fails, so feed rules are not dropped.
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

// Valid reports whether this is a severity the product ranks and styles.
func (s Severity) Valid() bool {
	switch s {
	case SeverityCritical, SeverityHigh, SeverityMedium, SeverityLow, SeverityInformational:
		return true
	}
	return false
}

func validateSeverity(severity string) error {
	if severity == "" {
		return nil // the caller's default applies
	}
	if !Severity(severity).Valid() {
		return fmt.Errorf("%w: unknown severity %q, expected one of %v", ErrInvalidAlert, severity, Severity("").EnumValues())
	}
	return nil
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

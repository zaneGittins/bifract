package alerts

import (
	"errors"
	"fmt"
)

// AlertType is how an alert is evaluated. It was a bare string, which meant an
// unknown value was stored, shown in the UI, and never evaluated by an engine
// that only switches on the three real ones.
type AlertType string

const (
	// AlertTypeEvent evaluates against newly ingested logs as they arrive.
	AlertTypeEvent AlertType = "event"
	// AlertTypeScheduled evaluates on a cron schedule rather than on ingest.
	AlertTypeScheduled AlertType = "scheduled"
	// AlertTypeCompound correlates several conditions before firing.
	AlertTypeCompound AlertType = "compound"
)

// EnumValues lists the ways an alert can be evaluated, so the API description
// names them instead of saying "string".
func (AlertType) EnumValues() []string {
	return []string{string(AlertTypeEvent), string(AlertTypeScheduled), string(AlertTypeCompound)}
}

// Valid reports whether the engine knows how to evaluate this type.
func (a AlertType) Valid() bool {
	switch a {
	case AlertTypeEvent, AlertTypeScheduled, AlertTypeCompound:
		return true
	}
	return false
}

// ErrInvalidAlert marks a rule the caller can fix, as opposed to a failure of
// ours. Handlers answer 400 for it rather than matching on message text, which
// silently misclassifies any error whose wording was not anticipated.
var ErrInvalidAlert = errors.New("invalid alert")

// validateAlertType rejects a type no engine evaluates, rather than storing a
// rule that can never fire.
func validateAlertType(alertType string) error {
	if alertType == "" {
		return nil // the caller's default applies
	}
	if !AlertType(alertType).Valid() {
		return fmt.Errorf("%w: unknown alert_type %q, expected one of %v", ErrInvalidAlert, alertType, AlertType("").EnumValues())
	}
	return nil
}

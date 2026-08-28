package storage

import "time"

// maxTimezoneLen matches the VARCHAR(64) that users.display_timezone and
// dashboards.timezone are declared as.
const maxTimezoneLen = 64

// ValidTimezone accepts an IANA zone name resolvable by the embedded tzdata
// (see the time/tzdata import in cmd/bifract-server: the runtime image is FROM
// scratch and has no /usr/share/zoneinfo).
//
// "Local" is rejected: it means the server's own zone, which is never a
// meaningful answer to "what zone should this be rendered or bucketed in".
func ValidTimezone(tz string) bool {
	if tz == "" || len(tz) > maxTimezoneLen || tz == "Local" {
		return false
	}
	_, err := time.LoadLocation(tz)
	return err == nil
}

// SafeTimezone falls back to UTC for a row written before its zone column
// existed, or whose stored zone has since been dropped from the IANA database.
func SafeTimezone(tz string) string {
	if !ValidTimezone(tz) {
		return "UTC"
	}
	return tz
}

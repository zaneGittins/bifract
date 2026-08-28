package storage

import "context"

// AttributionUserKey is where the auth middleware records the human to credit
// for a write. It is a plain string rather than a principal object so that any
// package can read it: created_by is a foreign key into users, and a machine
// principal has no row there, so the value has to be the person behind the
// credential.
const AttributionUserKey = "attribution_user"

// AttributionUser returns the username to record as created_by, or empty when
// there is nobody to credit. Pair it with NullableUser so an empty value is
// written as NULL rather than violating the foreign key.
func AttributionUser(ctx context.Context) string {
	user, _ := ctx.Value(AttributionUserKey).(string)
	return user
}

// DisplayTimezone returns the IANA zone the authenticated user renders
// timestamps in, or empty when there is no such preference to honor.
//
// Empty is the right answer for an API key: a machine principal has no display
// zone, and its aggregates must bucket the same way for every caller. Callers
// treat empty as UTC.
func DisplayTimezone(ctx context.Context) string {
	user, ok := ctx.Value("user").(*User)
	if !ok || user == nil {
		return ""
	}
	return user.DisplayTimezone
}

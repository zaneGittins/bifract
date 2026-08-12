package storage

// NullableUser binds a username to a column that references users(username).
// An empty username means "no attributable user" and must be stored as NULL:
// the empty string is not a users row, so it violates the foreign key just as
// an unknown username does. Reads already normalize NULL back to "" via
// COALESCE, so this is symmetric.
func NullableUser(username string) interface{} {
	if username == "" {
		return nil
	}
	return username
}

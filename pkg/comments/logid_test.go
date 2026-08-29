package comments

import "testing"

// TestValidLogID pins the log_id guard. log_id reaches ClickHouse lookups, so
// anything outside hex must be refused at the edge rather than relied on to be
// quoted correctly further down.
func TestValidLogID(t *testing.T) {
	valid := []string{
		"a3f5c9d2e1b48706a3f5c9d2e1b48706", // GenerateLogID width
		"A3F5C9D2E1B48706A3F5C9D2E1B48706",
		"deadbeef",
	}
	for _, id := range valid {
		if !validLogID(id) {
			t.Errorf("validLogID(%q) = false, want true", id)
		}
	}

	invalid := []string{
		"",
		"deadbee",           // under the minimum width
		"x3f5c9d2e1b48706",  // not hex
		"a3f5' OR 1=1--",    // quote break-out
		`a3f5\' OR 1=1--`,   // backslash-prefixed break-out
		"a3f5c9d2 e1b48706", // whitespace
		"a3f5c9d2e1b48706\n' OR 1=1--",
	}
	for _, id := range invalid {
		if validLogID(id) {
			t.Errorf("validLogID(%q) = true, want false", id)
		}
	}
}

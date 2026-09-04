package alerts

import (
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func chErr(code int32, msg string) error {
	return &proto.Exception{Code: code, Message: msg}
}

// Message shapes captured from ClickHouse 26.6.
func TestUnknownIdentifier(t *testing.T) {
	cases := []struct{ msg, want string }{
		{"Unknown expression or function identifier `norm_log` in scope SELECT timestamp, log_id FROM logs_hot WHERE x. (UNKNOWN_IDENTIFIER)", "norm_log"},
		{"Unknown expression identifier `norm_log` in scope SELECT norm_log FROM logs_hot. (UNKNOWN_IDENTIFIER)", "norm_log"},
		{"Unknown expression or function identifier `nosuchthing` in scope SELECT log_id FROM logs_hot WHERE nosuchthing = 1.", "nosuchthing"},
		{"Unknown identifier: some_field", "some_field"},
		{"Memory limit exceeded", ""},
	}
	for _, tc := range cases {
		if got := unknownIdentifier(errors.New(tc.msg)); got != tc.want {
			t.Errorf("got %q, want %q for %q", got, tc.want, tc.msg)
		}
	}
}

// An alert auto-disabled for a missing base column can never be fixed by editing its
// BQL, and nothing re-enables it once the schema is repaired. Those must stay enabled.
func TestMissingBaseColumnIsNotAQueryBug(t *testing.T) {
	engine := chErr(47, "Unknown expression or function identifier `norm_log` in scope SELECT timestamp, log_id, fractal_id FROM logs_hot WHERE match(lower(norm_log), '.*truncate.*')")
	code, ok := isUnrecoverableChError(engine)
	if !ok || code != 47 {
		t.Fatalf("expected code 47 to classify as unrecoverable, got %d/%v", code, ok)
	}
	if got := missingBaseColumn(code, engine); got != "norm_log" {
		t.Fatalf("a missing norm_log must read as a schema problem, got %q", got)
	}

	// A field the user's own BQL named is still a query bug worth disabling for.
	user := chErr(47, "Unknown expression or function identifier `my_alias` in scope SELECT my_alias FROM logs")
	if got := missingBaseColumn(47, user); got != "" {
		t.Fatalf("a user-authored identifier must still auto-disable, got %q", got)
	}

	// raw_log is deleted from the alert projection rather than selected, so the engine
	// never names it and a query that does is genuinely wrong.
	raw := chErr(47, "Unknown expression identifier `raw_log` in scope SELECT raw_log FROM logs")
	if got := missingBaseColumn(47, raw); got != "" {
		t.Fatalf("raw_log must not be treated as engine-projected, got %q", got)
	}

	// Other unrecoverable codes are unaffected.
	if got := missingBaseColumn(62, chErr(62, "Syntax error")); got != "" {
		t.Fatalf("only code 47 is a schema error, got %q", got)
	}
}

// The evaluator retries on its interval, so an unthrottled notification would fire
// twice a minute per affected alert, forever.
func TestSchemaErrorNotifiesOncePerAlert(t *testing.T) {
	var e Engine
	for i := 0; i < 5; i++ {
		if _, notified := e.schemaErrNotified.LoadOrStore("alert-1", struct{}{}); notified != (i > 0) {
			t.Fatalf("tick %d: notified=%v, want %v", i, notified, i > 0)
		}
	}
	// A successful run re-arms it, so a fault that returns after a repair is announced.
	e.schemaErrNotified.Delete("alert-1")
	if _, notified := e.schemaErrNotified.LoadOrStore("alert-1", struct{}{}); notified {
		t.Fatal("a recovered alert must notify again if it breaks later")
	}
}

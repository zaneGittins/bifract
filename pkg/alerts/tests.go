package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"bifract/pkg/ruleeval"
	"bifract/pkg/storage"
)

// Limits on a test corpus. Each run inserts every event into a scratch table and
// queries it, so the corpus is bounded to keep an editor session cheap.
const (
	MaxTestsPerAlert  = 25
	MaxEventsPerTest  = 20
	MaxEventsPerAlert = 50
)

// AlertTest is sample events an alert is expected to match, or expected to ignore.
//
// Events are normalized objects: the shape BQL runs against, which is what both a
// picked search result and pasted JSON already are. Storing them normalized means a
// test needs no normalizer attached and does not change meaning when one is edited.
type AlertTest struct {
	ID          string                   `json:"id,omitempty"`
	Name        string                   `json:"name"`
	Expectation string                   `json:"expectation"`
	Events      []map[string]interface{} `json:"events"`
	Position    int                      `json:"position"`
}

// Validate checks a single test in isolation.
func (t *AlertTest) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("test name is required")
	}
	switch ruleeval.Expectation(t.Expectation) {
	case ruleeval.ExpectMatch, ruleeval.ExpectNoMatch:
	default:
		return fmt.Errorf("test %q: expectation must be %q or %q", t.Name, ruleeval.ExpectMatch, ruleeval.ExpectNoMatch)
	}
	if len(t.Events) == 0 {
		return fmt.Errorf("test %q: at least one event is required", t.Name)
	}
	if len(t.Events) > MaxEventsPerTest {
		return fmt.Errorf("test %q: at most %d events per test", t.Name, MaxEventsPerTest)
	}
	return nil
}

// ValidateTests checks a whole corpus, including the aggregate event budget.
func ValidateTests(tests []AlertTest) error {
	if len(tests) > MaxTestsPerAlert {
		return fmt.Errorf("at most %d tests per alert", MaxTestsPerAlert)
	}

	total := 0
	names := make(map[string]bool, len(tests))
	for i := range tests {
		if err := tests[i].Validate(); err != nil {
			return err
		}
		key := strings.ToLower(strings.TrimSpace(tests[i].Name))
		if names[key] {
			return fmt.Errorf("duplicate test name %q", tests[i].Name)
		}
		names[key] = true
		total += len(tests[i].Events)
	}
	if total > MaxEventsPerAlert {
		return fmt.Errorf("at most %d events across all tests (have %d)", MaxEventsPerAlert, total)
	}
	return nil
}

// ListTests returns an alert's tests in author order.
func (m *Manager) ListTests(ctx context.Context, alertID string) ([]AlertTest, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id::text, name, expectation, events, position
		  FROM alert_tests WHERE alert_id = $1 ORDER BY position, created_at`, alertID)
	if err != nil {
		return nil, fmt.Errorf("list alert tests: %w", err)
	}
	defer rows.Close()

	tests := []AlertTest{}
	for rows.Next() {
		var t AlertTest
		var raw []byte
		if err := rows.Scan(&t.ID, &t.Name, &t.Expectation, &raw, &t.Position); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &t.Events); err != nil {
			return nil, fmt.Errorf("decode events for test %q: %w", t.Name, err)
		}
		tests = append(tests, t)
	}
	return tests, rows.Err()
}

// replaceTests rewrites an alert's whole corpus inside the caller's transaction, so
// tests commit with the alert edit that changed them.
func (m *Manager) replaceTests(ctx context.Context, tx storage.Tx, alertID string, tests []AlertTest, username string) error {
	if err := ValidateTests(tests); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, "DELETE FROM alert_tests WHERE alert_id = $1", alertID); err != nil {
		return fmt.Errorf("clearing alert tests: %w", err)
	}

	for i := range tests {
		raw, err := json.Marshal(tests[i].Events)
		if err != nil {
			return fmt.Errorf("encode events for test %q: %w", tests[i].Name, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO alert_tests (alert_id, name, expectation, events, position, created_by)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			alertID, tests[i].Name, tests[i].Expectation, raw, i, storage.NullableUser(username),
		); err != nil {
			return fmt.Errorf("saving test %q: %w", tests[i].Name, err)
		}
	}
	return nil
}

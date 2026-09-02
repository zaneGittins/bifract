package alerts

import "testing"

func testEvent() map[string]interface{} {
	return map[string]interface{}{"process_name": "cmd.exe"}
}

func TestValidateTestsRejectsBadExpectation(t *testing.T) {
	err := ValidateTests([]AlertTest{{Name: "a", Expectation: "maybe", Events: []map[string]interface{}{testEvent()}}})
	if err == nil {
		t.Fatal("expected an error for an unknown expectation")
	}
}

func TestValidateTestsRequiresEvents(t *testing.T) {
	if err := ValidateTests([]AlertTest{{Name: "a", Expectation: "match"}}); err == nil {
		t.Fatal("a test with no events asserts nothing and must be rejected")
	}
}

func TestValidateTestsRejectsDuplicateNames(t *testing.T) {
	one := []map[string]interface{}{testEvent()}
	err := ValidateTests([]AlertTest{
		{Name: "Fires", Expectation: "match", Events: one},
		{Name: "fires", Expectation: "no_match", Events: one},
	})
	if err == nil {
		t.Fatal("expected duplicate names to be rejected")
	}
}

func TestValidateTestsEnforcesEventBudget(t *testing.T) {
	events := make([]map[string]interface{}, MaxEventsPerTest)
	for i := range events {
		events[i] = testEvent()
	}

	var tests []AlertTest
	for i := 0; i < (MaxEventsPerAlert/MaxEventsPerTest)+1; i++ {
		tests = append(tests, AlertTest{Name: string(rune('a' + i)), Expectation: "match", Events: events})
	}
	if err := ValidateTests(tests); err == nil {
		t.Fatal("expected the aggregate event budget to be enforced")
	}
}

func TestValidateTestsAcceptsAValidCorpus(t *testing.T) {
	one := []map[string]interface{}{testEvent()}
	err := ValidateTests([]AlertTest{
		{Name: "fires on cmd", Expectation: "match", Events: one},
		{Name: "ignores explorer", Expectation: "no_match", Events: one},
	})
	if err != nil {
		t.Fatalf("valid corpus rejected: %v", err)
	}
}

func TestCorpusHashIgnoresQueryOnlyEdits(t *testing.T) {
	// The hash decides whether a session reloads its events. It must depend on the
	// events alone, so iterating on a query reuses the loaded scratch table.
	tests := []AlertTest{{Name: "a", Expectation: "match", Events: []map[string]interface{}{testEvent()}}}

	first, err := corpusHash(tests)
	if err != nil {
		t.Fatal(err)
	}
	tests[0].Expectation = "no_match"
	tests[0].Name = "renamed"
	second, err := corpusHash(tests)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Error("changing only an expectation or a name should not force an event reload")
	}
}

func TestCorpusHashChangesWithTestOrder(t *testing.T) {
	// Run pairs tests with units positionally, so a reordered corpus must reload.
	a := AlertTest{Name: "a", Expectation: "match", Events: []map[string]interface{}{{"process_name": "cmd.exe"}}}
	b := AlertTest{Name: "b", Expectation: "match", Events: []map[string]interface{}{{"process_name": "powershell.exe"}}}

	first, _ := corpusHash([]AlertTest{a, b})
	second, _ := corpusHash([]AlertTest{b, a})
	if first == second {
		t.Error("reordering tests must reload, or outcomes bind to the wrong test")
	}
}

func TestCorpusHashChangesWithEvents(t *testing.T) {
	base := []AlertTest{{Name: "a", Expectation: "match", Events: []map[string]interface{}{testEvent()}}}
	first, _ := corpusHash(base)

	base[0].Events = []map[string]interface{}{{"process_name": "powershell.exe"}}
	second, _ := corpusHash(base)

	if first == second {
		t.Error("editing an event must reload the corpus")
	}
}

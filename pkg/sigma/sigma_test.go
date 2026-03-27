package sigma

import (
	"strings"
	"testing"

	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
)

// --- IsSigmaRule tests ---

func TestIsSigmaRule_ValidSigma(t *testing.T) {
	yaml := `
title: Test Rule
detection:
  selection:
    CommandLine|contains: powershell
  condition: selection
`
	if !IsSigmaRule(yaml) {
		t.Error("expected IsSigmaRule to return true for valid Sigma YAML")
	}
}

func TestIsSigmaRule_BifractYAML(t *testing.T) {
	yaml := `
name: My Alert
queryString: "level=error"
enabled: true
`
	if IsSigmaRule(yaml) {
		t.Error("expected IsSigmaRule to return false for Bifract YAML with queryString")
	}
}

func TestIsSigmaRule_InvalidYAML(t *testing.T) {
	if IsSigmaRule("not: valid: yaml: [") {
		t.Error("expected IsSigmaRule to return false for invalid YAML")
	}
}

func TestIsSigmaRule_NoCondition(t *testing.T) {
	yaml := `
title: Missing Condition
detection:
  selection:
    Field: value
`
	if IsSigmaRule(yaml) {
		t.Error("expected IsSigmaRule to return false when detection has no condition")
	}
}

func TestIsSigmaRule_NoDetection(t *testing.T) {
	yaml := `
title: No Detection
description: Just a description
`
	if IsSigmaRule(yaml) {
		t.Error("expected IsSigmaRule to return false when no detection block")
	}
}

// --- ParseSigmaRule tests ---

func TestParseSigmaRule_Basic(t *testing.T) {
	yaml := `
title: Suspicious PowerShell
id: abc-123
status: test
level: high
description: Detects suspicious PowerShell
author: Test Author
tags:
  - attack.execution
  - attack.t1059
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    CommandLine|contains: Invoke-Expression
  condition: selection
falsepositives:
  - Legitimate admin activity
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rule.Title != "Suspicious PowerShell" {
		t.Errorf("expected title 'Suspicious PowerShell', got '%s'", rule.Title)
	}
	if rule.ID != "abc-123" {
		t.Errorf("expected id 'abc-123', got '%s'", rule.ID)
	}
	if rule.Level != "high" {
		t.Errorf("expected level 'high', got '%s'", rule.Level)
	}
	if rule.Author != "Test Author" {
		t.Errorf("expected author 'Test Author', got '%s'", rule.Author)
	}
	if rule.LogSource.Category != "process_creation" {
		t.Errorf("expected category 'process_creation', got '%s'", rule.LogSource.Category)
	}
	if rule.LogSource.Product != "windows" {
		t.Errorf("expected product 'windows', got '%s'", rule.LogSource.Product)
	}
	if len(rule.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(rule.Tags))
	}
	if rule.Detection.Condition != "selection" {
		t.Errorf("expected condition 'selection', got '%s'", rule.Detection.Condition)
	}

	sel, ok := rule.Detection.Selections["selection"]
	if !ok {
		t.Fatal("expected 'selection' in detection selections")
	}
	if len(sel.FieldConditions) != 1 {
		t.Fatalf("expected 1 field condition, got %d", len(sel.FieldConditions))
	}
	fc := sel.FieldConditions[0]
	if fc.Field != "CommandLine" {
		t.Errorf("expected field 'CommandLine', got '%s'", fc.Field)
	}
	if len(fc.Modifiers) != 1 || fc.Modifiers[0] != "contains" {
		t.Errorf("expected modifiers [contains], got %v", fc.Modifiers)
	}
}

func TestParseSigmaRule_MultipleSelections(t *testing.T) {
	yaml := `
title: Multi Selection
detection:
  selection:
    CommandLine|contains: powershell
  filter:
    User: SYSTEM
  condition: selection and not filter
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rule.Detection.Selections) != 2 {
		t.Fatalf("expected 2 selections, got %d", len(rule.Detection.Selections))
	}
	if _, ok := rule.Detection.Selections["selection"]; !ok {
		t.Error("missing 'selection'")
	}
	if _, ok := rule.Detection.Selections["filter"]; !ok {
		t.Error("missing 'filter'")
	}
}

func TestParseSigmaRule_ModifiersMultiValue(t *testing.T) {
	yaml := `
title: Multi Value
detection:
  selection:
    CommandLine|contains:
      - Invoke-Expression
      - IEX
      - Invoke-Command
  condition: selection
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fc := rule.Detection.Selections["selection"].FieldConditions[0]
	if len(fc.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(fc.Values))
	}
}

func TestParseSigmaRule_NumericValues(t *testing.T) {
	yaml := `
title: Numeric
detection:
  selection:
    EventID: 4688
  condition: selection
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fc := rule.Detection.Selections["selection"].FieldConditions[0]
	if len(fc.Values) != 1 || fc.Values[0] != "4688" {
		t.Errorf("expected ['4688'], got %v", fc.Values)
	}
}

func TestParseSigmaRule_MissingTitle(t *testing.T) {
	yaml := `
detection:
  selection:
    Field: value
  condition: selection
`
	_, err := ParseSigmaRule(yaml)
	if err == nil {
		t.Error("expected error for missing title")
	}
}

// --- Translator tests ---

func TestTranslate_ExactMatch(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"User": "admin",
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != `User="admin"` {
		t.Errorf("expected User=\"admin\", got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_Contains(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"CommandLine|contains": "powershell",
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != `CommandLine=/.*powershell.*/i` {
		t.Errorf("expected CommandLine=/.*powershell.*/i, got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_StartsWith(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"Image|startswith": "C:\\Windows",
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `Image=/^C:\\Windows.*/i`
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_EndsWith(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"Image|endswith": `\explorer.exe`,
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `Image=/.*\\explorer\.exe$/i`
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_Regex(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"CommandLine|re": `.*powershell.*-enc.*`,
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `CommandLine=/.*powershell.*-enc.*/`
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_MultipleValues_OR(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"EventID": []interface{}{1, 3, 11},
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Values should be OR'd
	if !strings.Contains(result, "OR") {
		t.Errorf("expected OR in result, got %s", result)
	}
	if !strings.Contains(result, `EventID="1"`) {
		t.Errorf("expected EventID=\"1\" in result, got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_ContainsAll(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"CommandLine|contains|all": []interface{}{"cmd", "/c", "whoami"},
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Values should be AND'd
	if !strings.Contains(result, "AND") {
		t.Errorf("expected AND in result, got %s", result)
	}
	if !strings.Contains(result, `CommandLine=/.*cmd.*/i`) {
		t.Errorf("expected contains pattern for 'cmd', got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_ConditionAndNot(t *testing.T) {
	yaml := `
title: And Not
detection:
  selection:
    CommandLine|contains: powershell
  filter:
    User: SYSTEM
  condition: selection and not filter
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}

	if !strings.Contains(result, "AND") {
		t.Errorf("expected AND in result, got %s", result)
	}
	if !strings.Contains(result, "NOT") {
		t.Errorf("expected NOT in result, got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_ConditionOr(t *testing.T) {
	yaml := `
title: Or Condition
detection:
  sel1:
    User: admin
  sel2:
    User: root
  condition: sel1 or sel2
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}

	if !strings.Contains(result, "OR") {
		t.Errorf("expected OR in result, got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_1OfThem(t *testing.T) {
	yaml := `
title: One of Them
detection:
  sel1:
    User: admin
  sel2:
    User: root
  condition: 1 of them
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}

	if !strings.Contains(result, "OR") {
		t.Errorf("expected OR for '1 of them', got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_AllOfPattern(t *testing.T) {
	yaml := `
title: All of Pattern
detection:
  selection_proc:
    Image|endswith: \cmd.exe
  selection_args:
    CommandLine|contains: whoami
  filter:
    User: SYSTEM
  condition: all of selection* and not filter
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}

	if !strings.Contains(result, "AND") {
		t.Errorf("expected AND for 'all of selection*', got %s", result)
	}
	if !strings.Contains(result, "NOT") {
		t.Errorf("expected NOT for 'not filter', got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_NullValue(t *testing.T) {
	rule := &SigmaRule{
		Title: "Null Test",
		Detection: Detection{
			Condition: "selection",
			Selections: map[string]SelectionGroup{
				"selection": {
					FieldConditions: []FieldCondition{
						{Field: "ParentImage", Values: nil},
					},
				},
			},
		},
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != "NOT ParentImage=*" {
		t.Errorf("expected 'NOT ParentImage=*', got '%s'", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_SpecialCharsInValues(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"Image|endswith": `\powershell.exe`,
	}, "selection")

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Backslash and dot should be escaped in regex
	if !strings.Contains(result, `\\powershell\.exe`) {
		t.Errorf("expected escaped special chars, got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_UnsupportedBase64(t *testing.T) {
	rule := makeRule("selection", map[string]interface{}{
		"CommandLine|base64": "test",
	}, "selection")

	_, err := Translate(rule, nil)
	if err == nil {
		t.Error("expected error for base64 modifier")
	}
	if !strings.Contains(err.Error(), "base64") {
		t.Errorf("expected base64 error, got: %v", err)
	}
}

func TestTranslate_AggregationConditionUnsupported(t *testing.T) {
	yaml := `
title: Aggregation
detection:
  selection:
    EventID: 4625
  condition: selection | count() > 5
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	_, err = Translate(rule, nil)
	if err == nil {
		t.Error("expected error for aggregation condition")
	}
}

func TestTranslate_MultipleFieldsInSelection(t *testing.T) {
	yaml := `
title: Multi Field
detection:
  selection:
    Image|endswith: \cmd.exe
    ParentImage|endswith: \explorer.exe
    User: admin
  condition: selection
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}

	// All fields should be AND'd within a selection
	if !strings.Contains(result, "AND") {
		t.Errorf("expected AND for multiple fields in selection, got %s", result)
	}
	assertValidBQL(t, result)
}

func TestTranslate_ComplexRealWorldRule(t *testing.T) {
	yaml := `
title: Suspicious PowerShell Download
id: 3b6ab547-8ec2-4991-b9d2-2b06c8e4db78
status: test
level: high
description: Detects suspicious PowerShell download commands
logsource:
  category: process_creation
  product: windows
detection:
  selection_img:
    Image|endswith:
      - \powershell.exe
      - \pwsh.exe
  selection_cmd:
    CommandLine|contains:
      - Invoke-WebRequest
      - wget
      - curl
      - DownloadString
      - DownloadFile
  filter:
    User: SYSTEM
    ParentImage|endswith: \svchost.exe
  condition: selection_img and selection_cmd and not filter
falsepositives:
  - Legitimate admin scripts
tags:
  - attack.execution
  - attack.t1059.001
`
	rule, err := ParseSigmaRule(yaml)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := Translate(rule, nil)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}

	// Verify structure
	if !strings.Contains(result, "AND") {
		t.Errorf("expected AND in complex rule, got %s", result)
	}
	if !strings.Contains(result, "NOT") {
		t.Errorf("expected NOT in complex rule, got %s", result)
	}
	if !strings.Contains(result, "OR") {
		t.Errorf("expected OR for multi-value selections, got %s", result)
	}

	// Verify metadata
	if rule.Title != "Suspicious PowerShell Download" {
		t.Errorf("wrong title: %s", rule.Title)
	}
	if rule.Level != "high" {
		t.Errorf("wrong level: %s", rule.Level)
	}
	if len(rule.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(rule.Tags))
	}

	assertValidBQL(t, result)
}

// --- Field mapper tests ---

func TestBuildFieldMapper_Nil(t *testing.T) {
	mapper := BuildFieldMapper(nil)
	if mapper("CommandLine") != "CommandLine" {
		t.Error("nil normalizer should return identity mapper")
	}
}

func TestBuildFieldMapper_SnakeCase(t *testing.T) {
	compiled := &normalizers.CompiledNormalizer{
		Transforms:      []normalizers.Transform{normalizers.TransformSnakeCase, normalizers.TransformLowercase},
		FieldMappingMap: map[string]string{},
	}
	mapper := BuildFieldMapper(compiled)

	tests := map[string]string{
		"CommandLine":  "command_line",
		"ParentImage":  "parent_image",
		"EventID":      "event_id",
		"User":         "user",
		"SourceIp":     "source_ip",
		"TargetFilename": "target_filename",
	}

	for input, expected := range tests {
		got := mapper(input)
		if got != expected {
			t.Errorf("mapper(%s) = %s, expected %s", input, got, expected)
		}
	}
}

func TestBuildFieldMapper_WithExplicitMapping(t *testing.T) {
	compiled := &normalizers.CompiledNormalizer{
		Transforms:      []normalizers.Transform{normalizers.TransformSnakeCase, normalizers.TransformLowercase},
		FieldMappingMap: map[string]string{
			"parent_image": "parent_process_path",
			"src_ip":       "source_address",
		},
	}
	mapper := BuildFieldMapper(compiled)

	// snake_case("ParentImage") -> "parent_image" -> mapping -> "parent_process_path"
	got := mapper("ParentImage")
	if got != "parent_process_path" {
		t.Errorf("mapper(ParentImage) = %s, expected parent_process_path", got)
	}
}

func TestTranslate_WithFieldMapper(t *testing.T) {
	compiled := &normalizers.CompiledNormalizer{
		Transforms:      []normalizers.Transform{normalizers.TransformSnakeCase, normalizers.TransformLowercase},
		FieldMappingMap: map[string]string{},
	}
	mapper := BuildFieldMapper(compiled)

	rule := makeRule("selection", map[string]interface{}{
		"CommandLine|contains": "powershell",
	}, "selection")

	result, err := Translate(rule, mapper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "command_line=") {
		t.Errorf("expected field name to be snake_cased, got %s", result)
	}
	assertValidBQL(t, result)
}

// --- Helpers ---

func makeRule(selName string, fields map[string]interface{}, condition string) *SigmaRule {
	conditions, _ := parseFieldConditions(fields)
	return &SigmaRule{
		Title: "Test Rule",
		Detection: Detection{
			Condition: condition,
			Selections: map[string]SelectionGroup{
				selName: {FieldConditions: conditions},
			},
		},
	}
}

func assertValidBQL(t *testing.T, query string) {
	t.Helper()
	_, err := parser.ParseQuery(query)
	if err != nil {
		t.Errorf("generated query is not valid BQL: %s\n  error: %v", query, err)
	}
}

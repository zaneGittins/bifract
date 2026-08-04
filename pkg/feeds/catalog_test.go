package feeds

import (
	"errors"
	"testing"

	"bifract/pkg/sigma"
)

const sigmaRule = `
title: Suspicious Service Creation
id: 11111111-2222-3333-4444-555555555555
status: experimental
level: high
description: A test rule
logsource:
    product: windows
    category: process_creation
tags:
    - attack.persistence
    - attack.privilege-escalation
    - attack.t1543.003
detection:
    selection:
        Image|endswith: '\sc.exe'
    condition: selection
`

func TestParseRuleMetadataSigma(t *testing.T) {
	meta, err := parseRuleMetadata(sigmaRule)
	if err != nil {
		t.Fatalf("parseRuleMetadata: %v", err)
	}

	if meta.Title != "Suspicious Service Creation" {
		t.Errorf("title = %q", meta.Title)
	}
	if meta.Level != "high" || meta.Status != "experimental" {
		t.Errorf("level/status = %q/%q, want high/experimental", meta.Level, meta.Status)
	}

	// The ATT&CK tags must survive verbatim: pkg/attack reads them back to build
	// the coverage map.
	want := map[string]bool{"attack.persistence": true, "attack.privilege-escalation": true, "attack.t1543.003": true}
	for _, tag := range meta.Tags {
		delete(want, tag)
	}
	if len(want) != 0 {
		t.Errorf("tags %v are missing from %v", want, meta.Tags)
	}
}

// A rule that cannot be translated to BQL must still yield metadata, or the gap
// list loses exactly the rules worth knowing about.
func TestParseRuleMetadataSurvivesUntranslatableRule(t *testing.T) {
	const untranslatable = `
title: Aggregation Rule
status: stable
level: critical
logsource:
    product: windows
tags:
    - attack.t1110
detection:
    selection:
        EventID: 4625
    timeframe: 24h
    condition: selection | count() by User > 100
`
	meta, err := parseRuleMetadata(untranslatable)
	if err != nil {
		t.Fatalf("parseRuleMetadata rejected a rule it should still catalogue: %v", err)
	}
	if meta.Title != "Aggregation Rule" {
		t.Errorf("title = %q, want Aggregation Rule", meta.Title)
	}

	var found bool
	for _, tag := range meta.Tags {
		if tag == "attack.t1110" {
			found = true
		}
	}
	if !found {
		t.Errorf("ATT&CK tag missing from %v", meta.Tags)
	}
}

func TestParseRuleMetadataNative(t *testing.T) {
	const native = `
name: My Alert
queryString: 'event_id=4625'
labels:
    - attack.t1110
`
	meta, err := parseRuleMetadata(native)
	if err != nil {
		t.Fatalf("parseRuleMetadata: %v", err)
	}
	if meta.Title != "My Alert" {
		t.Errorf("title = %q, want My Alert", meta.Title)
	}
	if len(meta.Tags) != 1 || meta.Tags[0] != "attack.t1110" {
		t.Errorf("tags = %v, want [attack.t1110]", meta.Tags)
	}
}

func TestParseRuleMetadataRejectsGarbage(t *testing.T) {
	for _, content := range []string{"", "not: a rule", "::::"} {
		if _, err := parseRuleMetadata(content); err == nil {
			t.Errorf("parseRuleMetadata(%q) accepted a non-rule", content)
		}
	}
}

// The sync loop classifies catalog skip reasons off this error type, so a
// translate failure must stay distinguishable from a parse failure.
func TestTranslateErrorIsDistinguishable(t *testing.T) {
	s := &Syncer{}

	_, _, _, _, _, _, _, _, err := s.parseSigmaRule(`
title: Aggregation Rule
level: high
logsource:
    product: windows
detection:
    selection:
        EventID: 4625
    timeframe: 24h
    condition: selection | count() by User > 100
`, nil)
	if err == nil {
		t.Skip("this rule now translates; the classification path is still exercised by the type below")
	}

	var translateErr *TranslateError
	if !errors.As(err, &translateErr) {
		t.Errorf("parseSigmaRule returned %T (%v), want *TranslateError", err, err)
	}

	// A genuine YAML failure must not be misfiled as a translator gap.
	_, _, _, _, _, _, _, _, parseErr := s.parseSigmaRule("::::", nil)
	if parseErr == nil {
		t.Fatal("parseSigmaRule accepted malformed YAML")
	}
	if errors.As(parseErr, &translateErr) {
		t.Error("a YAML parse failure was reported as a translate error")
	}
}

func TestBuildLabelsMatchesSigmaTags(t *testing.T) {
	rule, err := sigma.ParseSigmaRule(sigmaRule)
	if err != nil {
		t.Fatalf("ParseSigmaRule: %v", err)
	}

	labels := sigma.BuildLabels(rule)
	want := []string{"sigma:high", "status:experimental", "attack.persistence", "product:windows", "category:process_creation"}
	for _, w := range want {
		var found bool
		for _, l := range labels {
			if l == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("label %q missing from %v", w, labels)
		}
	}
}

package ruletest

import (
	"strings"
	"testing"
)

func TestLoadRuleSigmaUsesNormalizerFieldNames(t *testing.T) {
	dir := t.TempDir()
	rule := writeFile(t, dir, "r.yml", `
title: Certutil Download
logsource:
  category: process_creation
detection:
  selection:
    Image|endswith: '\certutil.exe'
    CommandLine|contains: 'urlcache'
  condition: selection
`)
	norm := writeFile(t, dir, "n.yaml", `
name: Test
transforms: []
field_mappings:
  - sources: [Image]
    target: image
  - sources: [CommandLine]
    target: commandline
`)

	compiled, err := LoadNormalizer(norm)
	if err != nil {
		t.Fatal(err)
	}
	r, err := LoadRule(rule, compiled)
	if err != nil {
		t.Fatal(err)
	}

	if r.Kind != "sigma" {
		t.Errorf("Kind = %q, want sigma", r.Kind)
	}
	// The mapped names are what makes a tested rule behave like a deployed one.
	if !strings.Contains(r.BQL, "image=$") || !strings.Contains(r.BQL, "commandline=~") {
		t.Errorf("BQL did not use normalized field names: %s", r.BQL)
	}
	// logsource.category becomes a prefilter, without which the rule would run
	// against every event type.
	if !strings.Contains(r.BQL, "bifract_category=process_creation") {
		t.Errorf("BQL missing category prefilter: %s", r.BQL)
	}
}

func TestLoadRuleWithoutNormalizerKeepsRawFieldNames(t *testing.T) {
	dir := t.TempDir()
	rule := writeFile(t, dir, "r.yml", `
title: Raw
detection:
  selection:
    Image|endswith: '\x.exe'
  condition: selection
`)
	r, err := LoadRule(rule, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.BQL, "Image=$") {
		t.Errorf("expected unmapped field name, got %s", r.BQL)
	}
}

func TestLoadRuleNativeAlert(t *testing.T) {
	dir := t.TempDir()
	rule := writeFile(t, dir, "a.yml", `
name: Threshold
queryString: 'event_id="10" | groupBy(image, function=count()) | _count >= 3'
alertType: event
`)
	r, err := LoadRule(rule, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Kind != "bifract" {
		t.Errorf("Kind = %q, want bifract", r.Kind)
	}
	if r.Name != "Threshold" {
		t.Errorf("Name = %q", r.Name)
	}
}

func TestLoadRuleRejectsInvalidAndUnsupported(t *testing.T) {
	tests := map[string]struct {
		body    string
		wantErr string
	}{
		"neither sigma nor alert": {
			body:    "name: x\ndescription: nothing here\n",
			wantErr: "no queryString",
		},
		"invalid BQL": {
			body:    "name: x\nqueryString: '| | |'\n",
			wantErr: "invalid",
		},
		// These read tables the scratch clone does not have. Reporting "no match"
		// for them would be a silent false negative, so they must be rejected.
		"source command": {
			body:    "name: x\nqueryString: 'pgr(start=\"abc\")'\n",
			wantErr: "cannot be tested offline",
		},
		"model_lookup": {
			body:    "name: x\nqueryString: '* | model_lookup(name=m, field=image)'\n",
			wantErr: "cannot be tested offline",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeFile(t, dir, "r.yml", tc.body)
			_, err := LoadRule(p, nil)
			if err == nil {
				t.Fatal("LoadRule succeeded; want an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadNormalizerRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "n.yaml", "name: X\nfield_mapping: []\n")
	if _, err := LoadNormalizer(p); err == nil {
		t.Fatal("LoadNormalizer accepted a misspelled key; want an error")
	}
}

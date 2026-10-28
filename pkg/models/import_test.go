package models

import (
	"strings"
	"testing"
)

func TestValidateImport(t *testing.T) {
	name, err := ValidateImport([]byte(`
name: rmm_users
model_type: rarity
definition:
  partition_key: user
  value_key: Name
  min_sample: 1
  source_bql: |-
    event_id=1
    | match(dict=lolrmm, field=image, column=InstallationPaths, include=[Name], require=true)
`))
	if err != nil || name != "rmm_users" {
		t.Fatalf("ValidateImport = %q, %v", name, err)
	}

	bad := map[string]string{
		"no name":            "model_type: rarity\ndefinition: {partition_key: user, value_key: x}\n",
		"no partition key":   "name: m\nmodel_type: rarity\ndefinition: {value_key: x}\n",
		"unknown field":      "name: m\nmodel_type: rarity\ncolour: red\ndefinition: {partition_key: user, value_key: x}\n",
		"invalid source":     "name: m\nmodel_type: rarity\ndefinition: {partition_key: user, value_key: x, source_bql: '| | |'}\n",
		"tlsh with two keys": "name: m\nmodel_type: tlsh\ndefinition: {key_fields: [a, b]}\n",
		// tlsh() refuses an index that holds only part of the fractal.
		"filtered tlsh": "name: m\nmodel_type: tlsh\ndefinition: {key_fields: [tlsh], source_bql: 'tlsh=*'}\n",
	}
	for label, body := range bad {
		if _, err := ValidateImport([]byte(body)); err == nil {
			t.Errorf("%s: ValidateImport succeeded, want an error", label)
		} else if strings.TrimSpace(err.Error()) == "" {
			t.Errorf("%s: empty error", label)
		}
	}
}

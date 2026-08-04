package ruletest

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"bifract/pkg/normalizers"
)

// LoadNormalizer reads a normalizer in the same YAML shape the UI exports
// (normalizers.NormalizerExport) and compiles it. Testing with the normalizer that
// production ingests with is what makes a Sigma rule's field names resolve the same
// way in both places.
func LoadNormalizer(path string) (*normalizers.CompiledNormalizer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var export normalizers.NormalizerExport
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&export); err != nil {
		return nil, fmt.Errorf("parsing normalizer %s: %w", path, err)
	}
	if export.Name == "" {
		return nil, fmt.Errorf("%s: normalizer is missing a name", path)
	}

	n := normalizers.Normalizer{
		Name:            export.Name,
		Description:     export.Description,
		Transforms:      export.Transforms,
		FieldMappings:   export.FieldMappings,
		ValueMappings:   export.ValueMappings,
		TimestampFields: export.TimestampFields,
	}
	return n.Compile(), nil
}

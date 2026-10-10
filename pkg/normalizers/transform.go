package normalizers

import (
	"math"
	"strings"
	"unicode"
)

// ApplyFieldName applies all per-name transforms (skipping flatten, which is
// structural and handled by ApplyTransforms) then checks field mappings.
func (c *CompiledNormalizer) ApplyFieldName(field string) string {
	result := field
	for _, t := range c.Transforms {
		switch t {
		case TransformFlattenLeaf, TransformFlattenFull:
			continue
		default:
			result = applyFieldNameTransform(result, t)
		}
	}
	if target, ok := c.FieldMappingMap[result]; ok {
		return target
	}
	return result
}

// applyNameTransforms runs the structural flatten and per-key name transforms in
// order, stopping before field mappings. Shared by the ingest hot path and the
// editor's Trace so the preview can never disagree with what ingestion does.
//
// Path sources are matched against raw keys: by the first flatten, or before any
// rename when there is no flatten. The fields they claim come back separately.
func applyNameTransforms(fields map[string]string, nestedKeys map[string]bool, transforms []Transform, paths *pathNode) (map[string]string, map[string]pinnedField) {
	var pinned map[string]pinnedField
	if paths != nil && !hasFlatten(transforms) {
		fields, pinned = pinTopLevel(fields, paths)
		paths = nil
	}
	result := fields
	for i := 0; i < len(transforms); i++ {
		mode := flattenMode(transforms[i])
		if mode == FlattenNone {
			renamed := make(map[string]string, len(result))
			for k, v := range result {
				renamed[applyFieldNameTransform(k, transforms[i])] = v
			}
			result = renamed
			continue
		}
		// Fold the renames that follow into the flatten so leaf collisions are
		// judged on final names.
		j := i + 1
		for j < len(transforms) && flattenMode(transforms[j]) == FlattenNone {
			j++
		}
		renames := transforms[i+1 : j]
		var claimed map[string]pinnedField
		result, claimed = flattenFields(result, mode, nestedKeys, func(name string) string {
			for _, t := range renames {
				name = applyFieldNameTransform(name, t)
			}
			return name
		}, paths)
		if claimed != nil {
			pinned = claimed
		}
		nestedKeys = nil // after first flatten, nested tracking no longer applies
		paths = nil      // and paths only ever match the raw event
		i = j - 1
	}
	return result, pinned
}

func hasFlatten(transforms []Transform) bool {
	for _, t := range transforms {
		if flattenMode(t) != FlattenNone {
			return true
		}
	}
	return false
}

func flattenMode(t Transform) FlattenMode {
	switch t {
	case TransformFlattenLeaf:
		return FlattenLeaf
	case TransformFlattenFull:
		return FlattenFull
	}
	return FlattenNone
}

// ApplyTransforms applies all transforms in order to the full field map.
// Flatten transforms expand JSON-string values; other transforms rename keys.
// Field mappings are applied last.
func (c *CompiledNormalizer) ApplyTransforms(fields map[string]string) map[string]string {
	return c.ApplyTransformsWithNested(fields, nil)
}

// mappingRank orders the fields competing for one output name: a field already carrying
// the name comes first, then sources in the order the mappings list them. Trace uses
// the same order, so the editor shows the value ingestion keeps.
func (c *CompiledNormalizer) mappingRank(source, target string) int {
	if source == target {
		return 0
	}
	if r, ok := c.sourceRank[source]; ok {
		return r
	}
	return math.MaxInt
}

// mappingWinner returns the field of fields that keeps target when several map to it.
func (c *CompiledNormalizer) mappingWinner(fields map[string]string, target string) string {
	best, bestRank := "", math.MaxInt
	for k := range fields {
		t, ok := c.FieldMappingMap[k]
		if !ok {
			t = k
		}
		if t != target {
			continue
		}
		if r := c.mappingRank(k, target); r < bestRank || (r == bestRank && k < best) {
			best, bestRank = k, r
		}
	}
	return best
}

// ApplyTransformsWithNested is like ApplyTransforms but accepts a set of keys
// that are known to contain serialized nested objects. Only these keys will be
// expanded by flatten transforms, preventing string values that happen to
// contain valid JSON from being incorrectly flattened.
func (c *CompiledNormalizer) ApplyTransformsWithNested(fields map[string]string, nestedKeys map[string]bool) map[string]string {
	result, pinned := applyNameTransforms(fields, nestedKeys, c.Transforms, c.paths)

	// Apply field mappings last.
	if len(c.FieldMappingMap) > 0 {
		mapped := make(map[string]string, len(result)+len(pinned))
		var contested map[string]bool
		for k, v := range result {
			target, ok := c.FieldMappingMap[k]
			if !ok {
				target = k
			}
			if _, taken := mapped[target]; taken {
				if contested == nil {
					contested = map[string]bool{}
				}
				contested[target] = true
			}
			mapped[target] = v
		}
		for target := range contested {
			mapped[target] = result[c.mappingWinner(result, target)]
		}
		result = mapped
	}
	// Path-claimed fields skip leaf mappings and win over any field with their name.
	for target, p := range pinned {
		result[target] = p.value
	}

	// Apply additive value mappings (derived fields) after field mappings, so
	// FromField references the already-renamed key. Never touches the source.
	for _, vm := range c.ValueMappings {
		src, ok := result[vm.FromField]
		if !ok {
			continue
		}
		if mappedVal, ok := vm.Map[src]; ok {
			result[vm.ToField] = mappedVal
		} else if vm.Default != "" {
			result[vm.ToField] = vm.Default
		}
	}

	return result
}

// toCamelCase converts a string to camelCase.
// snake_case: "query_name" -> "queryName"
// PascalCase: "QueryName" -> "queryName"
// Already camel: "queryName" -> "queryName"
func toCamelCase(s string) string {
	if s == "" {
		return ""
	}
	// If it contains underscores, split on them
	if strings.Contains(s, "_") {
		return fromSnakeToCamel(s, false)
	}
	// Otherwise lowercase the first character
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

// toPascalCase converts a string to PascalCase.
// snake_case: "query_name" -> "QueryName"
// camelCase: "queryName" -> "QueryName"
// Already Pascal: "QueryName" -> "QueryName"
func toPascalCase(s string) string {
	if s == "" {
		return ""
	}
	if strings.Contains(s, "_") {
		return fromSnakeToCamel(s, true)
	}
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

// fromSnakeToCamel converts snake_case to camelCase or PascalCase.
func fromSnakeToCamel(s string, capitalizeFirst bool) string {
	parts := strings.Split(s, "_")
	var result strings.Builder
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(strings.ToLower(part))
		if i == 0 && !capitalizeFirst {
			result.WriteString(string(runes))
		} else {
			runes[0] = unicode.ToUpper(runes[0])
			result.WriteString(string(runes))
		}
	}
	return result.String()
}

package normalizers

import (
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

// ApplyTransforms applies all transforms in order to the full field map.
// Flatten transforms expand JSON-string values; other transforms rename keys.
// Field mappings are applied last.
func (c *CompiledNormalizer) ApplyTransforms(fields map[string]string) map[string]string {
	return c.ApplyTransformsWithNested(fields, nil)
}

// ApplyTransformsWithNested is like ApplyTransforms but accepts a set of keys
// that are known to contain serialized nested objects. Only these keys will be
// expanded by flatten transforms, preventing string values that happen to
// contain valid JSON from being incorrectly flattened.
func (c *CompiledNormalizer) ApplyTransformsWithNested(fields map[string]string, nestedKeys map[string]bool) map[string]string {
	result := fields
	for _, t := range c.Transforms {
		switch t {
		case TransformFlattenLeaf:
			result = FlattenFields(result, FlattenLeaf, nestedKeys)
			nestedKeys = nil // after first flatten, nested tracking no longer applies
		case TransformFlattenFull:
			result = FlattenFields(result, FlattenFull, nestedKeys)
			nestedKeys = nil
		default:
			// Per-key name transform: build a new map with renamed keys.
			renamed := make(map[string]string, len(result))
			for k, v := range result {
				newKey := applyFieldNameTransform(k, t)
				renamed[newKey] = v
			}
			result = renamed
		}
	}

	// Apply field mappings last.
	if len(c.FieldMappingMap) > 0 {
		mapped := make(map[string]string, len(result))
		for k, v := range result {
			if target, ok := c.FieldMappingMap[k]; ok {
				mapped[target] = v
			} else {
				mapped[k] = v
			}
		}
		result = mapped
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

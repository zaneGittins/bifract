package normalizers

import (
	"strings"
	"unicode"

	"bifract/pkg/settings"
)

// ApplyFieldName applies all transforms (except flatten, which is structural)
// to a field name, then checks field mappings.
func (c *CompiledNormalizer) ApplyFieldName(field string) string {
	result := field
	for _, t := range c.Transforms {
		switch t {
		case TransformFlattenLeaf:
			// Flatten leaf is handled structurally during JSON parsing
			continue
		case TransformLowercase:
			result = strings.ToLower(result)
		case TransformUppercase:
			result = strings.ToUpper(result)
		case TransformSnakeCase:
			result = settings.ToSnakeCase(result)
		case TransformCamelCase:
			result = toCamelCase(result)
		case TransformPascalCase:
			result = toPascalCase(result)
		case TransformDedot:
			result = strings.ReplaceAll(result, ".", "_")
		}
	}
	if target, ok := c.FieldMappingMap[result]; ok {
		return target
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

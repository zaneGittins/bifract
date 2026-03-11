package alerts

import (
	"fmt"
	"regexp"
	"strings"
)

var templatePattern = regexp.MustCompile(`\{\{(\w+)\}\}`)

// ResolveTemplateName replaces {{field}} placeholders in name with values
// from the first result row. Unresolved placeholders are left as-is.
func ResolveTemplateName(name string, results []map[string]interface{}) string {
	if len(results) == 0 || !strings.Contains(name, "{{") {
		return name
	}
	row := results[0]
	return templatePattern.ReplaceAllStringFunc(name, func(match string) string {
		field := templatePattern.FindStringSubmatch(match)[1]
		if val, ok := row[field]; ok {
			return fmt.Sprintf("%v", val)
		}
		if fieldsMap, ok := row["fields"].(map[string]interface{}); ok {
			if val, ok := fieldsMap[field]; ok {
				return fmt.Sprintf("%v", val)
			}
		}
		return match
	})
}

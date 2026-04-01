package normalizers

import (
	"encoding/json"
	"fmt"
	"strings"

	"bifract/pkg/settings"
)

const (
	MaxFlattenDepth  = 64
	MaxFlattenFields = 1000
)

// FlattenFields expands any JSON-object string values in fields into individual
// keys according to the given mode. Only keys present in expandable are expanded;
// other string values (even if they look like JSON) are left as-is.
// Dots in all output keys are replaced with underscores to prevent ClickHouse's
// JSON column from re-nesting dot-separated keys.
//
//   - FlattenLeaf: uses only the leaf key name. On collision, falls back to the
//     full underscore-joined path.
//   - FlattenFull: uses the parent key + "_" + child key (underscore-joined).
func FlattenFields(fields map[string]string, mode FlattenMode, nestedKeys map[string]bool) map[string]string {
	if mode == FlattenNone {
		return fields
	}

	out := make(map[string]string, len(fields))
	truncated := false
	truncReason := ""

	// First pass: expand JSON-object values and collect leaf keys for collision detection.
	// Track all leaf keys and their source paths for leaf-mode collision resolution.
	type leafSource struct {
		key      string // output key (leaf or full path)
		fullPath string // full underscore-joined path
		value    string
	}
	var allLeaves []leafSource
	// Count how many distinct sources produce each leaf key (for collision detection).
	leafCount := make(map[string]int)

	for key, val := range fields {
		if len(out) >= MaxFlattenFields {
			truncated = true
			truncReason = "max_fields"
			break
		}

		safeKey := strings.ReplaceAll(key, ".", "_")

		// Only expand values that are known nested objects. If nestedKeys is provided,
		// only expand keys listed there. Otherwise, attempt to expand any JSON object
		// value (used by OTLP and preview paths where nestedKeys isn't available).
		isNested := false
		if nestedKeys != nil {
			isNested = nestedKeys[key]
		} else {
			isNested = len(val) > 1 && val[0] == '{'
		}
		if isNested && len(val) > 1 && val[0] == '{' {
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(val), &obj); err == nil {
				expanded := make(map[string]string)
				flattenObject(obj, safeKey, expanded, mode, 0, &truncated, &truncReason)
				for ek, ev := range expanded {
					if len(out) >= MaxFlattenFields {
						truncated = true
						truncReason = "max_fields"
						break
					}
					out[ek] = ev
					if mode == FlattenLeaf {
						allLeaves = append(allLeaves, leafSource{key: ek, fullPath: ek, value: ev})
						leafCount[ek]++
					}
				}
				continue
			}
		}

		out[safeKey] = val
		if mode == FlattenLeaf {
			allLeaves = append(allLeaves, leafSource{key: safeKey, fullPath: safeKey, value: val})
			leafCount[safeKey]++
		}
	}

	// Leaf mode: if any output key appears more than once, re-expand colliding keys
	// using full paths instead.
	if mode == FlattenLeaf {
		hasCollisions := false
		for _, count := range leafCount {
			if count > 1 {
				hasCollisions = true
				break
			}
		}

		if hasCollisions {
			colliding := make(map[string]bool)
			for k, count := range leafCount {
				if count > 1 {
					colliding[k] = true
				}
			}

			// Re-expand from scratch with collision awareness.
			rebuilt := make(map[string]string, len(fields))
			for key, val := range fields {
				safeKey := strings.ReplaceAll(key, ".", "_")

				shouldExpand := false
				if nestedKeys != nil {
					shouldExpand = nestedKeys[key]
				} else {
					shouldExpand = len(val) > 1 && val[0] == '{'
				}

				if shouldExpand && len(val) > 1 && val[0] == '{' {
					var obj map[string]interface{}
					if err := json.Unmarshal([]byte(val), &obj); err == nil {
						flattenObjectWithCollisions(obj, safeKey, rebuilt, colliding, 0)
						continue
					}
				}

				// Non-expandable key: use safeKey (with dots replaced).
				rebuilt[safeKey] = val
			}
			out = rebuilt
		}
	}

	if truncated {
		out["_bifract_truncated"] = "true"
		out["_bifract_truncation_reason"] = truncReason
	}

	return out
}

// flattenObject recursively walks a parsed JSON object and writes leaf values
// into the expanded map.
func flattenObject(obj map[string]interface{}, prefix string, expanded map[string]string, mode FlattenMode, depth int, truncated *bool, truncReason *string) {
	if depth >= MaxFlattenDepth {
		*truncated = true
		*truncReason = "max_depth"
		return
	}

	for key, value := range obj {
		if len(expanded) >= MaxFlattenFields {
			*truncated = true
			*truncReason = "max_fields"
			return
		}

		safeKey := strings.ReplaceAll(key, ".", "_")

		fullPath := safeKey
		if prefix != "" {
			fullPath = prefix + "_" + safeKey
		}

		switch v := value.(type) {
		case map[string]interface{}:
			flattenObject(v, fullPath, expanded, mode, depth+1, truncated, truncReason)
		default:
			outKey := fullPath
			if mode == FlattenLeaf {
				outKey = safeKey
			}
			// On leaf collision within a single object expansion, fall back to full path.
			if _, exists := expanded[outKey]; exists && mode == FlattenLeaf {
				outKey = fullPath
			}
			expanded[outKey] = stringifyValue(v)
		}
	}
}

// flattenObjectWithCollisions expands using leaf keys but falls back to full
// path for any key in the colliding set. Respects MaxFlattenDepth.
func flattenObjectWithCollisions(obj map[string]interface{}, prefix string, out map[string]string, colliding map[string]bool, depth int) {
	if depth >= MaxFlattenDepth {
		return
	}
	for key, value := range obj {
		safeKey := strings.ReplaceAll(key, ".", "_")
		fullPath := safeKey
		if prefix != "" {
			fullPath = prefix + "_" + safeKey
		}
		switch v := value.(type) {
		case map[string]interface{}:
			flattenObjectWithCollisions(v, fullPath, out, colliding, depth+1)
		default:
			outKey := safeKey
			if colliding[safeKey] {
				outKey = fullPath
			}
			out[outKey] = stringifyValue(v)
		}
	}
}

// stringifyValue converts an arbitrary JSON value to its string representation.
func stringifyValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return fmt.Sprintf("%v", val)
	case bool:
		return fmt.Sprintf("%v", val)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

// FieldsWithNested holds a flat field map and tracks which keys were serialized
// from nested objects (and are therefore safe to expand during flattening).
type FieldsWithNested struct {
	Fields    map[string]string
	NestedKeys map[string]bool // keys whose values are serialized nested objects
}

// BuildFields converts a parsed JSON object into a flat map[string]string
// without any recursion or flattening. Nested objects and arrays are serialized
// as JSON strings. Tracks which keys came from nested objects so that
// FlattenFields only expands those (not arbitrary strings that happen to be JSON).
func BuildFields(obj map[string]interface{}) map[string]string {
	result := BuildFieldsWithNested(obj)
	return result.Fields
}

// BuildFieldsWithNested is like BuildFields but also returns which keys are
// serialized nested objects, so FlattenFields can distinguish them from string
// values that happen to contain JSON.
func BuildFieldsWithNested(obj map[string]interface{}) FieldsWithNested {
	fields := make(map[string]string, len(obj))
	nested := make(map[string]bool)
	for key, value := range obj {
		switch v := value.(type) {
		case map[string]interface{}:
			b, _ := json.Marshal(v)
			fields[key] = string(b)
			nested[key] = true
		case []interface{}:
			b, _ := json.Marshal(v)
			fields[key] = string(b)
		default:
			fields[key] = stringifyValue(v)
		}
	}
	return FieldsWithNested{Fields: fields, NestedKeys: nested}
}

// applyFieldNameTransform applies a single non-flatten transform to a field name.
func applyFieldNameTransform(name string, t Transform) string {
	switch t {
	case TransformLowercase:
		return strings.ToLower(name)
	case TransformUppercase:
		return strings.ToUpper(name)
	case TransformSnakeCase:
		return settings.ToSnakeCase(name)
	case TransformCamelCase:
		return toCamelCase(name)
	case TransformPascalCase:
		return toPascalCase(name)
	case TransformDedot:
		return strings.ReplaceAll(name, ".", "_")
	}
	return name
}

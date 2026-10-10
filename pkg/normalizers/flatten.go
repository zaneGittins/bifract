package normalizers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"bifract/pkg/settings"
)

const (
	MaxFlattenDepth  = 64
	MaxFlattenFields = 1000
	// MaxArrayExpand bounds how many elements of an array-of-objects are expanded
	// into indexed keys. The element index enters the field path, so an unbounded
	// expansion of variable-length arrays would accumulate distinct JSON paths
	// (foo_0, foo_1, ...) across logs and overflow max_dynamic_paths. Beyond this
	// count the whole array is kept as a JSON string (the pre-expansion behavior),
	// still queryable via JSON functions.
	MaxArrayExpand = 16
)

// arrayOfObjects reports whether arr is non-empty and every element is a JSON
// object. Only such arrays are expanded: their elements are records whose fields
// deserve columns (Tetragon args, k8s container lists). Arrays of scalars or
// mixed/nested arrays are kept whole, where indexed keys would add path
// cardinality without a queryable payload.
func arrayOfObjects(arr []interface{}) bool {
	if len(arr) == 0 {
		return false
	}
	for _, el := range arr {
		if _, ok := el.(map[string]interface{}); !ok {
			return false
		}
	}
	return true
}

// FlattenFields expands any JSON-object string values in fields into individual
// keys according to the given mode. Only keys present in expandable are expanded;
// other string values (even if they look like JSON) are left as-is.
// Dots in all output keys are replaced with underscores to prevent ClickHouse's
// JSON column from re-nesting dot-separated keys.
//
//   - FlattenLeaf: uses only the leaf key name. Every leaf whose name collides
//     with another uses its full underscore-joined path instead.
//   - FlattenFull: uses the parent key + "_" + child key (underscore-joined).
func FlattenFields(fields map[string]string, mode FlattenMode, nestedKeys map[string]bool) map[string]string {
	out, _ := flattenFields(fields, mode, nestedKeys, nil, nil)
	return out
}

// leaf is one scalar produced by flattening, with its leaf-mode name and full path.
type leaf struct {
	name, path, value string
}

// flattenFields is FlattenFields with rename applied to every output key. Leaf
// collisions are judged on renamed keys, so names that only differ before the
// rename (ProcessID vs ProcessId under snake_case) both fall back to full paths
// rather than one overwriting the other at random.
//
// Leaves matched by paths are returned separately under their targets, untouched by
// rename. Their targets are reserved: any other leaf named the same falls back to its
// full path, as on any collision.
func flattenFields(fields map[string]string, mode FlattenMode, nestedKeys map[string]bool, rename func(string) string, paths *pathNode) (map[string]string, map[string]pinnedField) {
	if mode == FlattenNone {
		return fields, nil
	}
	var pc *pinCollector
	if paths != nil {
		pc = &pinCollector{}
	}
	if rename == nil {
		rename = func(s string) string { return s }
	}

	leaves := make([]leaf, 0, len(fields))
	truncated := false
	truncReason := ""

	for key, val := range fields {
		if len(leaves) >= MaxFlattenFields {
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
		node := paths.child(key)
		if isNested && len(val) > 1 && val[0] == '{' {
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(val), &obj); err == nil {
				flattenObject(obj, safeKey, &leaves, 0, &truncated, &truncReason, node, pc)
				continue
			}
		}

		l := leaf{name: safeKey, path: safeKey, value: val}
		if node != nil && node.target != "" {
			pc.pin(node, l, &leaves)
			continue
		}
		leaves = append(leaves, l)
	}
	var pinned map[string]pinnedField
	if pc != nil {
		pinned = pc.pinned
	}

	out := make(map[string]string, len(leaves)+2)
	if mode == FlattenFull {
		for _, l := range leaves {
			out[rename(l.path)] = l.value
		}
	} else {
		names := make([]string, len(leaves))
		counts := make(map[string]int, len(leaves)+len(pinned))
		for target := range pinned {
			counts[target]++
		}
		for i, l := range leaves {
			names[i] = rename(l.name)
			counts[names[i]]++
		}
		for i, l := range leaves {
			if counts[names[i]] > 1 {
				names[i] = rename(l.path)
			}
			out[names[i]] = l.value
		}
	}

	if truncated {
		out[rename("_bifract_truncated")] = "true"
		out[rename("_bifract_truncation_reason")] = truncReason
	}

	return out, pinned
}

// flattenObject recursively walks a parsed JSON object and appends its scalar
// leaves. node is the path-source position matching obj (nil when no path continues
// here) and pc collects the leaves paths claim.
func flattenObject(obj map[string]interface{}, prefix string, leaves *[]leaf, depth int, truncated *bool, truncReason *string, node *pathNode, pc *pinCollector) {
	if depth >= MaxFlattenDepth {
		*truncated = true
		*truncReason = "max_depth"
		return
	}

	for key, value := range obj {
		if len(*leaves) >= MaxFlattenFields {
			*truncated = true
			*truncReason = "max_fields"
			return
		}

		safeKey := strings.ReplaceAll(key, ".", "_")

		fullPath := safeKey
		if prefix != "" {
			fullPath = prefix + "_" + safeKey
		}

		child := node.child(key)
		if v, ok := value.(map[string]interface{}); ok {
			flattenObject(v, fullPath, leaves, depth+1, truncated, truncReason, child, pc)
			continue
		}
		// Expand arrays of objects element-wise so nested record fields become leaf
		// keys; the element index sits in the path and surfaces only on leaf
		// collision. Other arrays fall through to scalar (whole-value) handling.
		if arr, ok := value.([]interface{}); ok && len(arr) <= MaxArrayExpand && arrayOfObjects(arr) {
			for i, el := range arr {
				idx := strconv.Itoa(i)
				flattenObject(el.(map[string]interface{}), fullPath+"_"+idx, leaves, depth+1, truncated, truncReason, child.child(idx), pc)
			}
			continue
		}

		l := leaf{name: safeKey, path: fullPath, value: stringifyValue(value)}
		if child != nil && child.target != "" {
			pc.pin(child, l, leaves)
			continue
		}
		*leaves = append(*leaves, l)
	}
}

// stringifyValue converts an arbitrary JSON value to its string representation.
func stringifyValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		// JSON unmarshals all numbers to float64. Format without exponents
		// ('f', -1) so whole/large numbers render as plain decimals ("1000000",
		// not "1e+06"); this keeps stored values exact-match friendly (value maps,
		// BQL equality) while ClickHouse still parses them for aggregation.
		return strconv.FormatFloat(val, 'f', -1, 64)
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
	Fields     map[string]string
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

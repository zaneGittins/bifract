package normalizers

import (
	"fmt"
	"strings"
	"time"
)

type Transform string

const (
	TransformFlattenLeaf Transform = "flatten_leaf"
	TransformFlattenFull Transform = "flatten_full"
	TransformLowercase   Transform = "lowercase"
	TransformUppercase   Transform = "uppercase"
	TransformSnakeCase   Transform = "snake_case"
	TransformCamelCase   Transform = "camelCase"
	TransformPascalCase  Transform = "PascalCase"
	TransformDedot       Transform = "dedot"
)

var ValidTransforms = map[Transform]bool{
	TransformFlattenLeaf: true,
	TransformFlattenFull: true,
	TransformLowercase:   true,
	TransformUppercase:   true,
	TransformSnakeCase:   true,
	TransformCamelCase:   true,
	TransformPascalCase:  true,
	TransformDedot:       true,
}

type FieldMapping struct {
	Sources []string `json:"sources"`
	Target  string   `json:"target"`
}

// ValueMapping is an additive derive transform: it reads FromField's value and,
// if that value is present in Map, writes the mapped value into ToField. The
// source field is never removed (keep-source semantics), distinguishing this
// from FieldMapping's move semantics. Default, when set, is written to ToField
// for any value not found in Map; when Default is empty an unmatched value
// leaves ToField untouched.
type ValueMapping struct {
	FromField string            `json:"from_field"`
	ToField   string            `json:"to_field"`
	Map       map[string]string `json:"map"`
	Default   string            `json:"default,omitempty"`
}

// TimestampField defines a field name and format to check for timestamps during ingestion.
type TimestampField struct {
	Field  string `json:"field"`
	Format string `json:"format"`
}

type Normalizer struct {
	ID              string           `json:"id"`
	Name            string           `json:"name"`
	Description     string           `json:"description"`
	Transforms      []Transform      `json:"transforms"`
	FieldMappings   []FieldMapping   `json:"field_mappings"`
	ValueMappings   []ValueMapping   `json:"value_mappings"`
	TimestampFields []TimestampField `json:"timestamp_fields"`
	IsDefault       bool             `json:"is_default"`
	CreatedBy       string           `json:"created_by"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
	Version         int              `json:"version"`
}

// FlattenMode indicates which flatten strategy the normalizer uses.
type FlattenMode string

const (
	FlattenNone FlattenMode = ""
	FlattenLeaf FlattenMode = "leaf"
	FlattenFull FlattenMode = "full"
)

// CompiledValueMapping is the hot-path form of a ValueMapping.
type CompiledValueMapping struct {
	FromField string
	ToField   string
	Map       map[string]string
	Default   string
}

// CompiledNormalizer is the hot-path version with pre-built lookup maps.
type CompiledNormalizer struct {
	Name            string // for per-log stamping ("name@version")
	Version         int
	Transforms      []Transform
	FieldMappingMap map[string]string // source -> target for O(1) lookup
	ValueMappings   []CompiledValueMapping
	Flatten         FlattenMode
	TimestampFields []TimestampField
}

// Stamp returns the "name@version" identifier written to each ingested log's
// normalizer column. Empty when the normalizer has no name.
func (c *CompiledNormalizer) Stamp() string {
	if c == nil || c.Name == "" {
		return ""
	}
	return fmt.Sprintf("%s@%d", c.Name, c.Version)
}

// Compile produces a hot-path CompiledNormalizer from a Normalizer.
func (n *Normalizer) Compile() *CompiledNormalizer {
	c := &CompiledNormalizer{
		Name:            n.Name,
		Version:         n.Version,
		Transforms:      n.Transforms,
		FieldMappingMap: make(map[string]string, len(n.FieldMappings)*4),
		TimestampFields: n.TimestampFields,
	}
	for _, t := range n.Transforms {
		switch t {
		case TransformFlattenLeaf:
			c.Flatten = FlattenLeaf
		case TransformFlattenFull:
			c.Flatten = FlattenFull
		}
	}
	for _, fm := range n.FieldMappings {
		for _, src := range fm.Sources {
			c.FieldMappingMap[src] = fm.Target
		}
	}
	for _, vm := range n.ValueMappings {
		from := strings.TrimSpace(vm.FromField)
		to := strings.TrimSpace(vm.ToField)
		if from == "" || to == "" {
			continue
		}
		c.ValueMappings = append(c.ValueMappings, CompiledValueMapping{
			FromField: from,
			ToField:   to,
			Map:       vm.Map,
			Default:   vm.Default,
		})
	}
	return c
}

type CreateRequest struct {
	Name            string           `json:"name"`
	Description     string           `json:"description"`
	Transforms      []Transform      `json:"transforms"`
	FieldMappings   []FieldMapping   `json:"field_mappings"`
	ValueMappings   []ValueMapping   `json:"value_mappings"`
	TimestampFields []TimestampField `json:"timestamp_fields"`
}

type UpdateRequest struct {
	Name            string           `json:"name"`
	Description     string           `json:"description"`
	Transforms      []Transform      `json:"transforms"`
	FieldMappings   []FieldMapping   `json:"field_mappings"`
	ValueMappings   []ValueMapping   `json:"value_mappings"`
	TimestampFields []TimestampField `json:"timestamp_fields"`
}

// NormalizerExport is the YAML-serializable form for import/export.
type NormalizerExport struct {
	Name            string           `yaml:"name" json:"name"`
	Description     string           `yaml:"description,omitempty" json:"description,omitempty"`
	Transforms      []Transform      `yaml:"transforms" json:"transforms"`
	FieldMappings   []FieldMapping   `yaml:"field_mappings" json:"field_mappings"`
	ValueMappings   []ValueMapping   `yaml:"value_mappings,omitempty" json:"value_mappings,omitempty"`
	TimestampFields []TimestampField `yaml:"timestamp_fields,omitempty" json:"timestamp_fields,omitempty"`
}

// TokenUsageInfo describes a token that references a normalizer.
type TokenUsageInfo struct {
	TokenID     string `json:"token_id"`
	TokenName   string `json:"token_name"`
	FractalID   string `json:"fractal_id"`
	FractalName string `json:"fractal_name"`
}

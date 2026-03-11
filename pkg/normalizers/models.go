package normalizers

import "time"

type Transform string

const (
	TransformFlattenLeaf Transform = "flatten_leaf"
	TransformLowercase   Transform = "lowercase"
	TransformUppercase   Transform = "uppercase"
	TransformSnakeCase   Transform = "snake_case"
	TransformCamelCase   Transform = "camelCase"
	TransformPascalCase  Transform = "PascalCase"
	TransformDedot       Transform = "dedot"
)

var ValidTransforms = map[Transform]bool{
	TransformFlattenLeaf: true,
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
	TimestampFields []TimestampField `json:"timestamp_fields"`
	IsDefault       bool             `json:"is_default"`
	CreatedBy       string           `json:"created_by"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

// CompiledNormalizer is the hot-path version with pre-built lookup maps.
type CompiledNormalizer struct {
	Transforms      []Transform
	FieldMappingMap map[string]string // source -> target for O(1) lookup
	HasFlatten      bool
	TimestampFields []TimestampField
}

// Compile produces a hot-path CompiledNormalizer from a Normalizer.
func (n *Normalizer) Compile() *CompiledNormalizer {
	c := &CompiledNormalizer{
		Transforms:      n.Transforms,
		FieldMappingMap: make(map[string]string, len(n.FieldMappings)*4),
		TimestampFields: n.TimestampFields,
	}
	for _, t := range n.Transforms {
		if t == TransformFlattenLeaf {
			c.HasFlatten = true
		}
	}
	for _, fm := range n.FieldMappings {
		for _, src := range fm.Sources {
			c.FieldMappingMap[src] = fm.Target
		}
	}
	return c
}

type CreateRequest struct {
	Name            string           `json:"name"`
	Description     string           `json:"description"`
	Transforms      []Transform      `json:"transforms"`
	FieldMappings   []FieldMapping   `json:"field_mappings"`
	TimestampFields []TimestampField `json:"timestamp_fields"`
}

type UpdateRequest struct {
	Name            string           `json:"name"`
	Description     string           `json:"description"`
	Transforms      []Transform      `json:"transforms"`
	FieldMappings   []FieldMapping   `json:"field_mappings"`
	TimestampFields []TimestampField `json:"timestamp_fields"`
}

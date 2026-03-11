package sigma

// SigmaRule is the top-level representation of a Sigma detection rule.
type SigmaRule struct {
	Title          string    `yaml:"title"`
	ID             string    `yaml:"id"`
	Status         string    `yaml:"status"`
	Level          string    `yaml:"level"`
	Description    string    `yaml:"description"`
	Author         string    `yaml:"author"`
	Date           string    `yaml:"date"`
	References     []string  `yaml:"references"`
	Tags           []string  `yaml:"tags"`
	FalsePositives []string  `yaml:"falsepositives"`
	LogSource      LogSource `yaml:"logsource"`
	Detection      Detection `yaml:"-"` // custom unmarshal
}

// LogSource describes the type of log data the rule targets.
type LogSource struct {
	Category string `yaml:"category"`
	Product  string `yaml:"product"`
	Service  string `yaml:"service"`
}

// Detection holds the parsed detection block: named selections + condition string.
type Detection struct {
	Condition  string
	Selections map[string]SelectionGroup
}

// SelectionGroup represents a named detection item.
// It is either a single map of field conditions (FieldConditions)
// or a list of maps (Alternatives), which are OR'd together.
type SelectionGroup struct {
	FieldConditions []FieldCondition
	Alternatives    [][]FieldCondition // populated when the YAML value is a list of maps
}

// FieldCondition is a single field match within a selection.
type FieldCondition struct {
	Field     string   // original Sigma field name (e.g. "CommandLine")
	Modifiers []string // e.g. ["contains"], ["endswith", "all"], ["re"]
	Values    []string // one or more match values
}

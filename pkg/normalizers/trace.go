package normalizers

import "sort"

// TracedField is one output field of a normalizer run, carrying enough
// provenance for the editor to explain why the field ended up with its name.
type TracedField struct {
	Name         string `json:"name"`
	Value        string `json:"value"`
	Source       string `json:"source"`                  // name after transforms, before field mappings
	MappingIndex int    `json:"mapping_index"`           // FieldMappings index that fired, -1 if none
	MatchedAlias string `json:"matched_alias,omitempty"` // the alias that matched
	Derived      bool   `json:"derived,omitempty"`
	Override     bool   `json:"override,omitempty"`
	Collision    bool   `json:"collision,omitempty"`
}

// TraceResult is the outcome of a traced run. Collisions maps a final field
// name to the competing source names that resolved to it.
type TraceResult struct {
	Fields     []TracedField       `json:"fields"`
	Collisions map[string][]string `json:"collisions"`
}

// Trace runs the normalizer over a sample object and records how each output
// field got its name. It reuses applyNameTransforms and Compile so the traced
// result matches ingestion; only the mapping and value-mapping stages are
// re-walked here in order to attribute each field to the rule that produced it.
//
// Unlike the hot path, colliding fields are all retained and flagged rather than
// silently overwritten, so the editor can show what is competing.
func (n *Normalizer) Trace(obj map[string]interface{}) TraceResult {
	built := BuildFieldsWithNested(obj)
	preMapping := applyNameTransforms(built.Fields, built.NestedKeys, n.Transforms)

	// alias -> which mapping rule claims it. Matching is exact, mirroring
	// CompiledNormalizer.FieldMappingMap.
	type aliasOwner struct {
		index  int
		target string
	}
	owners := make(map[string]aliasOwner, len(n.FieldMappings)*4)
	for i, fm := range n.FieldMappings {
		if fm.Target == "" {
			continue
		}
		for _, src := range fm.Sources {
			owners[src] = aliasOwner{index: i, target: fm.Target}
		}
	}

	// Sorted source keys keep the output stable across runs; Go map iteration is
	// randomized and the editor re-renders this list on every keystroke.
	sources := make([]string, 0, len(preMapping))
	for k := range preMapping {
		sources = append(sources, k)
	}
	sort.Strings(sources)

	fields := make([]TracedField, 0, len(preMapping))
	for _, src := range sources {
		f := TracedField{
			Name:         src,
			Value:        preMapping[src],
			Source:       src,
			MappingIndex: -1,
		}
		if owner, ok := owners[src]; ok {
			f.Name = owner.target
			f.MappingIndex = owner.index
			f.MatchedAlias = src
		}
		fields = append(fields, f)
	}

	// Resolved view for the value-mapping stage. Where several sources collide the
	// first sorted one wins; ingestion picks arbitrarily, so this is one of the
	// possible outcomes rather than a guarantee, which is exactly why the
	// collision is surfaced to the user.
	resolved := make(map[string]string, len(fields))
	claimedBy := make(map[string][]string, len(fields))
	for _, f := range fields {
		if _, seen := resolved[f.Name]; !seen {
			resolved[f.Name] = f.Value
		}
		claimedBy[f.Name] = append(claimedBy[f.Name], f.Source)
	}

	collisions := make(map[string][]string)
	for name, srcs := range claimedBy {
		if len(srcs) > 1 {
			collisions[name] = srcs
		}
	}
	for i := range fields {
		if _, bad := collisions[fields[i].Name]; bad {
			fields[i].Collision = true
		}
	}

	// Value mappings run after field mappings and never remove the source.
	// Compile applies the same trimming and validation used at ingest.
	for _, vm := range n.Compile().ValueMappings {
		srcVal, ok := resolved[vm.FromField]
		if !ok {
			continue
		}
		newVal, matched := vm.Map[srcVal]
		if !matched {
			if vm.Default == "" {
				continue
			}
			newVal = vm.Default
		}

		prev, existed := resolved[vm.ToField]
		resolved[vm.ToField] = newVal
		if existed && prev == newVal {
			continue // map did not change anything, so do not badge it
		}

		if existed {
			for i := range fields {
				if fields[i].Name == vm.ToField {
					fields[i].Value = newVal
					fields[i].Derived = true
					fields[i].Override = true
				}
			}
			continue
		}
		fields = append(fields, TracedField{
			Name:         vm.ToField,
			Value:        newVal,
			Source:       vm.FromField,
			MappingIndex: -1,
			Derived:      true,
		})
	}

	return TraceResult{Fields: fields, Collisions: collisions}
}

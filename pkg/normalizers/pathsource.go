package normalizers

import (
	"fmt"
	"strings"
)

// PathSourcePrefix marks a field mapping source as a path into the raw event, matched
// before any transform, rather than a field name after transforms. Flattened names
// never contain dots, so no existing source can be read as a path by accident.
const PathSourcePrefix = "$."

// pathNode is one segment of the path sources, matched against raw event keys. A
// numeric segment selects an element of an expanded array of objects.
type pathNode struct {
	children map[string]*pathNode
	target   string // set when a path source ends here
	mapping  int    // FieldMappings index, for Trace attribution
	source   string // the source as written
	rank     int    // position in the normalizer; the lowest wins a shared target
}

// pinnedField is a value a path source claimed. It bypasses rename transforms and
// leaf field mappings, and takes precedence over any other field with its name.
type pinnedField struct {
	value string
	node  *pathNode
}

// parsePathSource returns the segments of a path source, ok=false when src is not
// one, and an error when it is malformed.
func parsePathSource(src string) (segments []string, ok bool, err error) {
	if !strings.HasPrefix(src, PathSourcePrefix) {
		return nil, false, nil
	}
	segments = strings.Split(strings.TrimPrefix(src, PathSourcePrefix), ".")
	for _, s := range segments {
		if s == "" {
			return nil, true, fmt.Errorf("path source %q has an empty segment", src)
		}
	}
	return segments, true, nil
}

// compilePathSources builds the lookup tree for every path source, or nil when the
// normalizer has none, which keeps the ingest path unchanged for everyone else.
// Malformed or duplicate paths are skipped here; ValidateFieldMappings rejects them.
func compilePathSources(mappings []FieldMapping) *pathNode {
	var root *pathNode
	rank := 0
	for mi, fm := range mappings {
		for _, src := range fm.Sources {
			segments, ok, err := parsePathSource(src)
			if !ok || err != nil {
				continue
			}
			if root == nil {
				root = &pathNode{}
			}
			n := root
			for _, s := range segments {
				if n.children == nil {
					n.children = map[string]*pathNode{}
				}
				child := n.children[s]
				if child == nil {
					child = &pathNode{}
					n.children[s] = child
				}
				n = child
			}
			if n.target == "" {
				n.target, n.mapping, n.source, n.rank = fm.Target, mi, src, rank
			}
			rank++
		}
	}
	return root
}

// child returns the node for key below n, or nil.
func (n *pathNode) child(key string) *pathNode {
	if n == nil {
		return nil
	}
	return n.children[key]
}

// pinCollector gathers path-matched leaves during one flatten. Two paths can share a
// target; the lower rank wins and the other leaf is named as if it had not matched.
type pinCollector struct {
	pinned  map[string]pinnedField
	winners map[string]leaf // the current winner per target, kept so a later one can demote it
}

func (pc *pinCollector) pin(n *pathNode, l leaf, leaves *[]leaf) {
	if pc.pinned == nil {
		pc.pinned = map[string]pinnedField{}
		pc.winners = map[string]leaf{}
	}
	if cur, ok := pc.pinned[n.target]; ok {
		if cur.node.rank < n.rank {
			*leaves = append(*leaves, l)
			return
		}
		*leaves = append(*leaves, pc.winners[n.target])
	}
	pc.pinned[n.target] = pinnedField{value: l.value, node: n}
	pc.winners[n.target] = l
}

// pinTopLevel claims top-level keys matched by single-segment paths, for normalizers
// with no flatten transform. It returns a copy of fields without them.
func pinTopLevel(fields map[string]string, paths *pathNode) (map[string]string, map[string]pinnedField) {
	pc := &pinCollector{}
	var demoted []leaf
	out := make(map[string]string, len(fields))
	for k, v := range fields {
		if n := paths.child(k); n != nil && n.target != "" {
			pc.pin(n, leaf{name: k, path: k, value: v}, &demoted)
			continue
		}
		out[k] = v
	}
	for _, l := range demoted {
		out[l.name] = l.value
	}
	return out, pc.pinned
}

// ValidateFieldMappings rejects path sources the normalizer could never apply as
// written: malformed paths, one path claimed by two targets, nested paths without a
// flatten transform, and renames ahead of the flatten that would change the keys a
// path is matched against.
func ValidateFieldMappings(transforms []Transform, mappings []FieldMapping) error {
	firstFlatten := -1
	for i, t := range transforms {
		if flattenMode(t) != FlattenNone {
			firstFlatten = i
			break
		}
	}
	targets := map[string]string{}
	for _, fm := range mappings {
		for _, src := range fm.Sources {
			segments, ok, err := parsePathSource(src)
			if !ok {
				continue
			}
			if err != nil {
				return err
			}
			if prev, dup := targets[src]; dup && prev != fm.Target {
				return fmt.Errorf("path source %q maps to both %q and %q", src, prev, fm.Target)
			}
			targets[src] = fm.Target
			if firstFlatten < 0 && len(segments) > 1 {
				return fmt.Errorf("path source %q is nested, which needs a flatten transform", src)
			}
			if firstFlatten > 0 {
				return fmt.Errorf("path source %q needs the flatten transform before any rename transform", src)
			}
		}
	}
	return nil
}

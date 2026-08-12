package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// mitreTagAlias is the output column holding one ATT&CK tag per row.
const mitreTagAlias = "attack_tag"

// mitreDefaultTagField is where ATT&CK tags live by convention: Sigma writes
// rule_tags, and other sources are normalized onto it. Reading one JSON
// sub-column is far cheaper than scanning the whole event, which is still
// available as mitre(tags=norm_log) for a source that has not been normalized.
const mitreDefaultTagField = "rule_tags"

// ATT&CK tag patterns, RE2, matched against a lowercased value. Backslashes are
// doubled because ClickHouse strips one level from string literals.
//
// Prefixed tags (attack.t1059.004, attack.execution, mitre.t1059) are matched
// anywhere. A bare technique ID is matched only in a dedicated tag field, where a
// four-digit t-token cannot be arbitrary log text; scanning a whole event would
// turn every hash and identifier containing "t1234" into a false technique.
const (
	mitreTagPattern     = `(?:attack|mitre)\\.[a-z0-9_.-]+`
	mitreBareTagPattern = `\\bt[0-9]{4}(?:\\.[0-9]{3})?\\b`
)

// mitreHandler handles mitre(tags=field, by=field, limit=N): an ATT&CK matrix
// over the events a query matched, built from the attack.* tags those events
// already carry (Sigma rule_tags, LimaCharlie detect tags, alert labels).
//
// It is an aggregating command: the grid is a GROUP BY over the exploded tag
// list, so a filter matching a billion rows still returns a few hundred, and the
// client renders the matrix from tag counts alone.
type mitreHandler struct{}

func (h *mitreHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasGroupBy = true
	ctx.Plan.IsAggregated = true
	ctx.Registry.Register("_count", FieldKindAggregate, "COUNT(*)", ctx.CmdIndex)
	return nil
}

func (h *mitreHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	if len(source.Layer.GroupBy) > 0 {
		return fmt.Errorf("mitre() reads raw events and must come before groupby()/timechart()")
	}

	var tagField, byField string
	limit := 5000

	for _, arg := range cmd.Arguments {
		value := strings.Trim(arg[strings.IndexByte(arg, '=')+1:], `"'`)
		switch {
		case strings.HasPrefix(arg, "tags="), strings.HasPrefix(arg, "tag="), strings.HasPrefix(arg, "field="):
			tagField = value
		case strings.HasPrefix(arg, "by="), strings.HasPrefix(arg, "groupby="):
			byField = value
		case strings.HasPrefix(arg, "limit="):
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return fmt.Errorf("mitre(): limit must be a positive number, got %q", value)
			}
			if n > 50000 {
				n = 50000
			}
			limit = n
		case !strings.Contains(arg, "="):
			// Bare first argument is the tag field: mitre(rule_tags).
			tagField = strings.Trim(arg, `"'`)
		default:
			return fmt.Errorf("mitre(): unknown argument %q (supported: tags=, by=, limit=)", arg)
		}
	}

	if tagField == "" {
		tagField = mitreDefaultTagField
	}
	// Bare technique IDs are only safe in a tag field. tags=norm_log scans the
	// whole event for a source whose tags are not in a field of their own, and
	// there a four-digit t-token is as likely to be part of a hash as a technique.
	pattern := mitreTagPattern
	if tagField != normLogColumn {
		pattern = mitreTagPattern + "|" + mitreBareTagPattern
	}

	tagRef, err := mitreFieldRef(tagField, ctx)
	if err != nil {
		return err
	}
	// arrayDistinct: a single event listing the same technique twice (a tag list
	// plus a free-text mention) must count once, not twice.
	tagExpr := fmt.Sprintf("arrayJoin(arrayDistinct(extractAll(lower(toString(%s)), '%s')))", tagRef, pattern)

	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: tagExpr, Alias: mitreTagAlias})
	source.Layer.GroupBy = append(source.Layer.GroupBy, mitreTagAlias)

	if byField != "" {
		alias, err := sanitizeIdentifier(byField)
		if err != nil {
			return fmt.Errorf("mitre(): by=%w", err)
		}
		byRef, err := mitreFieldRef(byField, ctx)
		if err != nil {
			return err
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: byRef, Alias: alias})
		source.Layer.GroupBy = append(source.Layer.GroupBy, byRef)
		ctx.Plan.ChartConfig["byField"] = byField
	}

	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*)", Alias: "_count"})
	source.Layer.OrderBy = append(source.Layer.OrderBy, "_count DESC")
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", limit)

	// The tag is a group key of this stage, so a downstream filter on it belongs in
	// HAVING; registering it as an aggregate output routes it there instead of to a
	// pre-aggregation JSON reference that does not exist.
	ctx.Registry.Register(mitreTagAlias, FieldKindAggregate, mitreTagAlias, ctx.CmdIndex)
	ctx.Registry.SetResolveExpr(mitreTagAlias, mitreTagAlias)
	ctx.Registry.Register("_count", FieldKindAggregate, "_count", ctx.CmdIndex)
	ctx.Registry.SetResolveExpr("_count", "_count")
	ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
	ctx.Plan.IsAggregated = true

	ctx.Plan.ChartType = "mitre"
	ctx.Plan.ChartConfig["tagField"] = tagField
	ctx.Plan.ChartConfig["limit"] = limit
	return nil
}

// mitreFieldRef resolves a field to its SQL reference, rejecting names that are
// not valid field identifiers before they reach the generated query.
func mitreFieldRef(field string, ctx *CommandContext) (string, error) {
	if _, err := sanitizeIdentifier(field); err != nil {
		return "", fmt.Errorf("mitre(): %w", err)
	}
	return resolveFieldRef(field, ctx.Registry), nil
}

func init() {
	registerAggregatingCommand(&mitreHandler{}, "mitre", "attack")
}

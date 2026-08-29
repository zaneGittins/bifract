package aitools

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxTechniques = 60
	maxGaps       = 40
)

func registerAttackTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "get_attack_coverage",
		Annotations: readOnly(),
		Description: "Report which ATT&CK techniques this fractal's detections cover.\n\n" +
			"Coverage is derived from the attack.* labels on the alerts that exist, so it " +
			"describes what is configured, not what has fired. Use it to answer \"are we " +
			"watching for this technique\" before writing a detection that already exists.\n\n" +
			"Returns the coverage summary, and the covered techniques with the number of " +
			"rules behind each.",
	}, getAttackCoverage)

	add(d, &mcp.Tool{
		Name:        "get_attack_gaps",
		Annotations: readOnly(),
		Description: "List uncovered ATT&CK techniques, ranked by what could be covered today.\n\n" +
			"The ranking accounts for whether rules exist that would detect the technique " +
			"against the fields this fractal actually ingests, so the top entries are the " +
			"ones worth acting on rather than the ones needing new telemetry.\n\n" +
			"Returns the uncovered techniques with candidate rule counts, and the total " +
			"number of gaps so a short list is not mistaken for the whole picture.",
	}, getAttackGaps)
}

type attackCoverageArgs struct {
	Tactic string `json:"tactic,omitempty" jsonschema:"Restrict to one tactic, by ATT&CK id, short name or name, for example 'TA0006', 'credential-access' or 'Credential Access'. Empty covers every tactic."`
	Limit  int    `json:"limit,omitempty" jsonschema:"How many covered techniques to list, capped at 60. Default 40. The summary counts are always for the whole matrix."`
}

// technique is one covered cell, named. The coverage endpoint answers a map of
// bare ids to tallies, which tells a model a count without telling it what was
// counted, so the matrix supplies the name and tactics.
type technique struct {
	ID          string   `json:"id"`
	Name        string   `json:"name,omitempty"`
	Tactics     []string `json:"tactics,omitempty"`
	Direct      int      `json:"direct"`
	Inherited   int      `json:"inherited"`
	Total       int      `json:"total"`
	Enabled     int      `json:"enabled"`
	MaxSeverity string   `json:"max_severity,omitempty"`
}

func getAttackCoverage(ctx context.Context, c Client, in attackCoverageArgs) (any, error) {
	payload, err := c.Get(ctx, "/attack/coverage", nil)
	if err != nil {
		return nil, err
	}
	object, ok := payload.(map[string]any)
	if !ok {
		return payload, nil
	}
	cells, ok := object["techniques"].(map[string]any)
	if !ok {
		return payload, nil
	}

	names, tactics, err := matrixIndex(ctx, c)
	if err != nil {
		return nil, err
	}
	wanted, err := resolveTactic(ctx, c, in.Tactic)
	if err != nil {
		return nil, err
	}

	covered := make([]technique, 0, len(cells))
	for id, cell := range cells {
		total := int(Field[float64](cell, "total"))
		if total == 0 {
			continue
		}
		if wanted != "" && !contains(tactics[id], wanted) {
			continue
		}
		covered = append(covered, technique{
			ID:          id,
			Name:        names[id],
			Tactics:     tactics[id],
			Direct:      int(Field[float64](cell, "direct")),
			Inherited:   int(Field[float64](cell, "inherited")),
			Total:       total,
			Enabled:     int(Field[float64](cell, "enabled")),
			MaxSeverity: Field[string](cell, "max_severity"),
		})
	}
	// Ranked by rule count, then by id, so the same coverage always reads the same
	// way: map iteration order alone would shuffle it between calls.
	sort.Slice(covered, func(i, j int) bool {
		if covered[i].Total != covered[j].Total {
			return covered[i].Total > covered[j].Total
		}
		return covered[i].ID < covered[j].ID
	})

	shown := covered
	if limit := clamp(in.Limit, 40, 1, maxTechniques); len(shown) > limit {
		shown = shown[:limit]
	}
	return map[string]any{
		"summary":            summaryFor(object["summary"], wanted),
		"covered_techniques": len(covered),
		"showing":            len(shown),
		"techniques":         shown,
		"note": "Coverage counts configured detections, not detections that have fired. " +
			"'direct' is a rule on the technique itself; 'inherited' is one on a sub-technique.",
	}, nil
}

// summaryFor narrows the headline strip to the tactic that was asked about. The
// full per-tactic breakdown is fifteen columns, which is a lot of context to
// spend on fourteen tactics the caller did not name.
func summaryFor(summary any, tactic string) any {
	if tactic == "" {
		return summary
	}
	object, ok := summary.(map[string]any)
	if !ok {
		return summary
	}
	perTactic, ok := object["per_tactic"].(map[string]any)
	if !ok {
		return summary
	}
	narrowed := make(map[string]any, len(object))
	for key, value := range object {
		narrowed[key] = value
	}
	narrowed["per_tactic"] = map[string]any{tactic: perTactic[tactic]}
	return narrowed
}

// matrixIndex maps every technique id to its name and its tactics, from the
// matrix the server embeds.
func matrixIndex(ctx context.Context, c Client) (map[string]string, map[string][]string, error) {
	matrix, err := c.Static(ctx, "/attack/matrix")
	if err != nil {
		return nil, nil, err
	}
	names := map[string]string{}
	tactics := map[string][]string{}
	for _, row := range Field[[]any](matrix, "techniques") {
		id := Field[string](row, "id")
		if id == "" {
			continue
		}
		names[id] = Field[string](row, "name")
		for _, tactic := range Field[[]any](row, "tactics") {
			if short, ok := tactic.(string); ok {
				tactics[id] = append(tactics[id], short)
			}
		}
	}
	return names, tactics, nil
}

// resolveTactic maps an id, short name or name onto the short name the matrix
// uses. An unknown tactic errors, since an empty result would read as "nothing
// is covered here".
func resolveTactic(ctx context.Context, c Client, named string) (string, error) {
	needle := strings.ToLower(strings.TrimSpace(named))
	if needle == "" {
		return "", nil
	}
	matrix, err := c.Static(ctx, "/attack/matrix")
	if err != nil {
		return "", err
	}
	var known []string
	for _, row := range Field[[]any](matrix, "tactics") {
		short := Field[string](row, "short")
		known = append(known, short)
		for _, form := range []string{Field[string](row, "id"), short, Field[string](row, "name")} {
			if strings.ToLower(form) == needle {
				return short, nil
			}
		}
	}
	sort.Strings(known)
	return "", fmt.Errorf("unknown tactic %q. Valid tactics: %s", named, strings.Join(known, ", "))
}

type attackGapsArgs struct {
	Limit int `json:"limit,omitempty" jsonschema:"How many gaps to return, capped at 40. Default 20."`
}

func getAttackGaps(ctx context.Context, c Client, in attackGapsArgs) (any, error) {
	limit := clamp(in.Limit, 20, 1, maxGaps)
	payload, err := c.Get(ctx, "/attack/gaps", url.Values{"limit": {strconv.Itoa(limit)}})
	if err != nil {
		return nil, err
	}
	object, ok := payload.(map[string]any)
	if !ok {
		return payload, nil
	}
	// Without a catalog the ranking has nothing to rank, and an empty list would
	// otherwise read as "no gaps".
	if !Field[bool](payload, "catalog_populated") {
		return map[string]any{
			"gaps": []any{},
			"note": "No rule catalog is loaded, so candidate rules cannot be ranked. " +
				"Sync a detection feed first; get_attack_coverage still works.",
		}, nil
	}
	return map[string]any{
		"uncovered_total": object["uncovered_total"],
		"returned":        object["returned"],
		"gaps":            object["gaps"],
	}, nil
}

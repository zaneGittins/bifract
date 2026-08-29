package aitools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// edgeTypes are what pgr() can generate. remote_thread and process_access are
// opt-in: they force an unindexed scan.
var edgeTypes = []string{"dns_query", "file_write", "net_connect", "process_access", "remote_thread"}

var directions = []string{"both", "forward", "backward"}

func registerProvenanceTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "find_processes",
		Annotations: readOnly(),
		Description: "Find process-creation events and return their process GUIDs.\n\n" +
			"This is the step before get_provenance_graph, which needs a GUID to seed on. " +
			"All filters are optional and combine with AND; the text ones are " +
			"case-insensitive substring matches.\n\n" +
			"Returns matching processes with process_guid, image, command line, host, user, and time.",
	}, findProcesses)

	add(d, &mcp.Tool{
		Name:        "get_provenance_graph",
		Annotations: readOnly(),
		Description: "Build the provenance graph for a process: what it descends from and what it did.\n\n" +
			"Runs pgr() on the seed GUID. It walks the spawn tree in both directions, scores " +
			"every file write, network connection, DNS query, and injection by how unusual it " +
			"is across the fleet (0 = ubiquitous, 1 = never seen before), prunes the everyday " +
			"noise, and pulls in other process trees that share a rare artifact with this one.\n\n" +
			"The result is a rendered process tree plus the notable activity and cross-tree " +
			"bridges, ranked by anomaly, rather than a raw edge list.\n\n" +
			"Get a GUID from find_processes. Set the time range to cover the whole " +
			"investigation window: lineage outside it is not included.\n\n" +
			"Requires an admin to have enabled endpoint behavioral analytics, and " +
			"endpoint/EDR data (for example Sysmon) normalized to bifract_category " +
			"process_creation.",
	}, getProvenanceGraph, enumOf("direction", directions))
}

type findProcessesArgs struct {
	Image       string `json:"image,omitempty" jsonschema:"Substring of the executable path, for example 'powershell' or 'rundll32'."`
	Host        string `json:"host,omitempty" jsonschema:"Substring of the hostname."`
	User        string `json:"user,omitempty" jsonschema:"Substring of the account the process ran as."`
	CommandLine string `json:"commandline,omitempty" jsonschema:"Substring of the command line, for example '-enc' or 'Invoke-'."`
	Limit       int    `json:"limit,omitempty" jsonschema:"Max processes to return, 1 to 100. Default 20."`
	window
}

func findProcesses(ctx context.Context, c Client, in findProcessesArgs) (any, error) {
	category, err := bqlEquals("bifract_category", "process_creation")
	if err != nil {
		return nil, err
	}
	conditions := []string{category}
	for _, filter := range []struct{ field, value string }{
		{"image", in.Image},
		{"computer_name", in.Host},
		{"user", in.User},
		{"commandline", in.CommandLine},
	} {
		value := strings.TrimSpace(filter.value)
		if value == "" {
			continue
		}
		condition, err := bqlContains(filter.field, value)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
	}

	limit := clamp(in.Limit, 20, 1, 100)
	query := fmt.Sprintf(
		"%s | table(timestamp, computer_name, image, process_guid, parent_process_guid, commandline, user) | head(%d)",
		strings.Join(conditions, " AND "), limit)

	body := in.window.body()
	body["query"] = query
	result, err := c.Post(ctx, "/query", body)
	if err != nil {
		return nil, err
	}

	rows := Field[[]any](result, "results")
	if len(rows) == 0 {
		return map[string]any{
			"query": query,
			"count": 0,
			"hint": "No process_creation events matched. Widen the time range, loosen the " +
				"filters, or confirm this fractal receives endpoint/EDR data.",
		}, nil
	}
	return map[string]any{"query": query, "count": len(rows), "processes": rows}, nil
}

type provenanceArgs struct {
	GUID        string  `json:"guid" jsonschema:"The process_guid to seed on, from find_processes."`
	Depth       int     `json:"depth,omitempty" jsonschema:"Tree hops to walk from the seed, 1 to 50. Default 10."`
	Direction   string  `json:"direction,omitempty" jsonschema:"'both' (default), 'forward' for descendants, 'backward' for ancestors."`
	Threshold   float64 `json:"threshold,omitempty" jsonschema:"Drop non-spawn edges scoring below this, 0.0 to 1.0. Default 0.7. Lower it to see more of what the tree did."`
	Include     string  `json:"include,omitempty" jsonschema:"Comma-separated edge types to generate instead of the default file_write,net_connect,dns_query. remote_thread and process_access force an unindexed scan, so request them only when hunting injection."`
	Exclude     string  `json:"exclude,omitempty" jsonschema:"Comma-separated edge types to drop."`
	Reconnect   *bool   `json:"reconnect,omitempty" jsonschema:"Pull in other trees sharing a rare file, IP, or domain. Default true."`
	Diffuse     *bool   `json:"diffuse,omitempty" jsonschema:"Propagate anomaly down the tree so a quiet step under a suspicious chain still surfaces. Default true."`
	Peers       int     `json:"peers,omitempty" jsonschema:"Max reconnected peer processes admitted, 1 to 500. Default 50."`
	Limit       int     `json:"limit,omitempty" jsonschema:"Max edges pgr() returns, 1 to 20000. Default 500."`
	MaxActivity int     `json:"max_activity,omitempty" jsonschema:"Max non-spawn actions to list, highest anomaly first. Default 40."`
	window
}

func getProvenanceGraph(ctx context.Context, c Client, in provenanceArgs) (any, error) {
	direction := strings.ToLower(strings.TrimSpace(in.Direction))
	if direction == "" {
		direction = "both"
	}
	if !contains(directions, direction) {
		return nil, fmt.Errorf("direction must be one of %s", strings.Join(directions, ", "))
	}
	threshold := in.Threshold
	if threshold == 0 {
		threshold = 0.7
	}
	if threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("threshold must be between 0.0 and 1.0, got %g", threshold)
	}

	seed, err := bqlLiteral(strings.TrimSpace(in.GUID), "guid")
	if err != nil {
		return nil, err
	}

	limit := clamp(in.Limit, 500, 1, 20000)
	args := []string{
		"start=" + seed,
		fmt.Sprintf("depth=%d", clamp(in.Depth, 10, 1, 50)),
		fmt.Sprintf("direction=%q", direction),
		fmt.Sprintf("threshold=%g", threshold),
		fmt.Sprintf("reconnect=%t", boolOr(in.Reconnect, true)),
		fmt.Sprintf("diffuse=%t", boolOr(in.Diffuse, true)),
		fmt.Sprintf("peers=%d", clamp(in.Peers, 50, 1, 500)),
		fmt.Sprintf("limit=%d", limit),
	}
	for _, list := range []struct{ name, value string }{{"include", in.Include}, {"exclude", in.Exclude}} {
		types, err := edgeList(list.value, list.name)
		if err != nil {
			return nil, err
		}
		if len(types) > 0 {
			args = append(args, fmt.Sprintf("%s=%q", list.name, strings.Join(types, ",")))
		}
	}

	query := fmt.Sprintf("pgr(%s)", strings.Join(args, ", "))
	body := in.window.body()
	body["query"] = query
	result, err := c.Post(ctx, "/query", body)
	if err != nil {
		return nil, err
	}

	rows := Field[[]any](result, "results")
	if len(rows) == 0 {
		return map[string]any{
			"query":     query,
			"seed":      in.GUID,
			"processes": 0,
			"hint": "No provenance edges. The GUID may be outside the time range, the fractal " +
				"may not carry process_creation events, or endpoint behavioral analytics may be " +
				"disabled (Admin > Settings > Features).",
		}, nil
	}

	graph := summarizeGraph(rows, max(1, orDefault(in.MaxActivity, 40)))
	graph["query"] = query
	graph["seed"] = in.GUID
	graph["edges_returned"] = len(rows)
	graph["edges_truncated"] = len(rows) >= limit
	graph["execution_ms"] = int(Field[float64](result, "execution_ms"))
	return graph, nil
}

// edgeList validates a comma-separated edge-type list against what pgr() can
// generate, so an unknown type is named rather than silently ignored by the query.
func edgeList(value, argument string) ([]string, error) {
	var types, unknown []string
	for _, raw := range strings.Split(value, ",") {
		kind := strings.ToLower(strings.TrimSpace(raw))
		if kind == "" {
			continue
		}
		if !contains(edgeTypes, kind) {
			unknown = append(unknown, kind)
		}
		types = append(types, kind)
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("%s has unknown edge type(s) %s. Valid types: %s",
			argument, strings.Join(unknown, ", "), strings.Join(edgeTypes, ", "))
	}
	return types, nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func boolOr(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func orDefault(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

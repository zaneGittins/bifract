package aitools

import (
	"fmt"
	"sort"
	"strings"
)

// pgr() returns a flat edge list. Handing a model hundreds of those buries the
// structure, so this rebuilds the spawn tree as an outline and reports activity
// and cross-tree bridges separately, highest anomaly first.

const (
	spawnEdge       = "spawn"
	reconnectPrefix = "reconnect_"
	maxCommandLine  = 200
	maxTreeDepth    = 64
	maxBridgesShown = 25
)

// process is a node in the spawn tree, built from the edge that created it.
type process struct {
	guid        string
	image       string
	commandLine string
	host        string
	user        string
	anomaly     float64
	logID       string
}

func (p process) headline() string {
	name := p.image
	if name == "" {
		name = p.guid
	}
	parts := []string{name, "guid=" + p.guid, fmt.Sprintf("anomaly=%.2f", p.anomaly)}
	if p.logID != "" {
		parts = append(parts, "log="+p.logID)
	}
	if p.user != "" {
		parts = append(parts, "user="+p.user)
	}
	return strings.Join(parts, "  ")
}

// summarizeGraph turns a pgr() result set into a tree, its notable activity, and
// its cross-tree bridges.
func summarizeGraph(rows []any, maxActivity int) map[string]any {
	spawns, activity, bridges := partitionEdges(rows)
	processes, children, roots := buildTree(spawns)

	counts := map[string]int{}
	for _, row := range rows {
		kind := text(row, "event_type")
		if kind == "" {
			kind = "unknown"
		}
		counts[kind]++
	}

	hostSet := map[string]bool{}
	for _, p := range processes {
		if p.host != "" {
			hostSet[p.host] = true
		}
	}
	hosts := make([]string, 0, len(hostSet))
	for host := range hostSet {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)

	shownActivity := rankActivity(activity, processes, maxActivity)
	shownBridges := rankBridges(bridges, maxBridgesShown)

	return map[string]any{
		"processes":             len(processes),
		"roots":                 len(roots),
		"hosts":                 hosts,
		"edge_counts":           counts,
		"process_tree":          renderTree(processes, children, roots),
		"notable_activity":      shownActivity,
		"activity_omitted":      max(0, len(activity)-len(shownActivity)),
		"reconnections":         shownBridges,
		"reconnections_omitted": max(0, len(bridges)-len(shownBridges)),
	}
}

func partitionEdges(rows []any) (spawns, activity, bridges []any) {
	for _, row := range rows {
		switch kind := text(row, "event_type"); {
		case kind == spawnEdge:
			spawns = append(spawns, row)
		case strings.HasPrefix(kind, reconnectPrefix):
			bridges = append(bridges, row)
		case kind != "":
			activity = append(activity, row)
		}
	}
	return spawns, activity, bridges
}

// buildTree returns the processes by guid, the parent to children map, and the
// root guids. A root is a process whose parent was not itself returned: either the
// walk reached the edge of the time window, or it is a reconnected peer's own tree.
func buildTree(spawns []any) (map[string]process, map[string][]string, []string) {
	processes := map[string]process{}
	children := map[string][]string{}
	parents := map[string]string{}
	var order []string

	for _, row := range spawns {
		child := text(row, "child")
		if child == "" {
			continue
		}
		if _, seen := processes[child]; seen {
			continue
		}
		parent := text(row, "parent")
		processes[child] = process{
			guid:        child,
			image:       text(row, "label"),
			commandLine: text(row, "command_line"),
			host:        text(row, "host"),
			user:        text(row, "proc_user"),
			anomaly:     number64(row, "anomaly_score"),
			logID:       text(row, "log_id"),
		}
		parents[child] = parent
		children[parent] = append(children[parent], child)
		order = append(order, child)
	}

	var roots []string
	for _, guid := range order {
		if _, present := processes[parents[guid]]; !present {
			roots = append(roots, guid)
		}
	}
	return processes, children, roots
}

// renderTree draws the spawn tree as an indented outline, most anomalous sibling
// first.
func renderTree(processes map[string]process, children map[string][]string, roots []string) string {
	var lines []string
	visited := map[string]bool{}

	sortedChildren := func(guid string) []string {
		var kids []string
		for _, kid := range children[guid] {
			if _, ok := processes[kid]; ok {
				kids = append(kids, kid)
			}
		}
		sort.SliceStable(kids, func(i, j int) bool {
			return processes[kids[i]].anomaly > processes[kids[j]].anomaly
		})
		return kids
	}

	var walk func(guid, prefix string, depth int)
	walk = func(guid, prefix string, depth int) {
		if visited[guid] || depth > maxTreeDepth {
			return
		}
		visited[guid] = true
		kids := sortedChildren(guid)
		for i, kid := range kids {
			child := processes[kid]
			branch, indent := "├─ ", "│  "
			if i == len(kids)-1 {
				branch, indent = "└─ ", "   "
			}
			lines = append(lines, prefix+branch+child.headline())
			childPrefix := prefix + indent
			if child.commandLine != "" && child.commandLine != child.image {
				lines = append(lines, childPrefix+"cmd: "+truncateRunes(child.commandLine, maxCommandLine))
			}
			walk(kid, childPrefix, depth+1)
		}
	}

	ordered := append([]string(nil), roots...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return processes[ordered[i]].anomaly > processes[ordered[j]].anomaly
	})
	for _, root := range ordered {
		if visited[root] {
			continue
		}
		p := processes[root]
		host := ""
		if p.host != "" {
			host = "[" + p.host + "] "
		}
		lines = append(lines, host+p.headline())
		if p.commandLine != "" && p.commandLine != p.image {
			lines = append(lines, "cmd: "+truncateRunes(p.commandLine, maxCommandLine))
		}
		walk(root, "", 0)
		lines = append(lines, "")
	}

	// A process not reachable from a root (a cycle in the lineage data) is still
	// listed, so nothing pgr() returned is silently dropped.
	for guid, p := range processes {
		if !visited[guid] {
			lines = append(lines, "(detached) "+p.headline())
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// rankActivity lists the non-spawn edges (file, network, DNS, injection), most
// anomalous first.
func rankActivity(rows []any, processes map[string]process, limit int) []map[string]any {
	entries := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		parent := text(row, "parent")
		proc := processes[parent]
		host := text(row, "host")
		if host == "" {
			host = proc.host
		}
		entries = append(entries, compact(map[string]any{
			"type":      text(row, "event_type"),
			"target":    text(row, "label"),
			"anomaly":   number64(row, "anomaly_score"),
			"process":   parent,
			"image":     proc.image,
			"host":      host,
			"timestamp": text(row, "timestamp"),
			"log_id":    text(row, "log_id"),
		}))
	}
	return topByAnomaly(entries, limit)
}

// rankBridges lists cross-tree reconnections. For net and dns both trees converge
// on the shared artifact, so target is that artifact node; for file it is the
// writer process to the process that executed it.
func rankBridges(rows []any, limit int) []map[string]any {
	entries := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, compact(map[string]any{
			"type":            strings.TrimPrefix(text(row, "event_type"), reconnectPrefix),
			"source":          text(row, "parent"),
			"target":          text(row, "child"),
			"shared_artifact": text(row, "label"),
			"anomaly":         number64(row, "anomaly_score"),
			"host":            text(row, "host"),
			"log_id":          text(row, "log_id"),
		}))
	}
	return topByAnomaly(entries, limit)
}

func topByAnomaly(entries []map[string]any, limit int) []map[string]any {
	sort.SliceStable(entries, func(i, j int) bool {
		a, _ := entries[i]["anomaly"].(float64)
		b, _ := entries[j]["anomaly"].(float64)
		return a > b
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries
}

// compact drops the empty strings, which say nothing and cost context. A zero
// anomaly is kept, because "scored, and unremarkable" is an answer.
func compact(entry map[string]any) map[string]any {
	for key, value := range entry {
		if value == "" {
			delete(entry, key)
		}
	}
	return entry
}

func text(row any, key string) string {
	object, ok := row.(map[string]any)
	if !ok {
		return ""
	}
	switch value := object[key].(type) {
	case string:
		return value
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func number64(row any, key string) float64 {
	object, ok := row.(map[string]any)
	if !ok {
		return 0
	}
	switch value := object[key].(type) {
	case float64:
		return value
	case string:
		var parsed float64
		if _, err := fmt.Sscanf(value, "%g", &parsed); err == nil {
			return parsed
		}
	}
	return 0
}

func truncateRunes(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit-3]) + "..."
}

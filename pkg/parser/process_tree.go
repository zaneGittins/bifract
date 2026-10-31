package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// ProcessTree is ptg()'s tree, walked by the query layer before translation with pgr's
// iterative walk (one indexed hop per level, identical siblings collapsed into one node), so
// translation never runs a recursive query. Rows are one per node with a row of its own in
// range. A nil *ProcessTree on QueryOptions means the tree was not resolved.
type ProcessTree struct {
	Rows []ProcessTreeRow
}

// ProcessTreeRow is one ptg() node. ParentGUID is the parent node, which for a collapsed node is
// the earliest member of its parent group rather than each member's own parent.
type ProcessTreeRow struct {
	Timestamp    string // "YYYY-MM-DD HH:MM:SS.mmm", UTC
	LogID        string
	FractalID    string
	ProcessGUID  string
	ParentGUID   string
	Image        string
	ParentImage  string
	Commandline  string
	ComputerName string
	Depth        int
	Path         string
	GroupSize    int64
	GroupFirst   string
	GroupLast    string
}

// ProcessTreeParams are ptg()'s arguments.
type ProcessTreeParams struct {
	Start     string
	Depth     int
	Direction string
}

// ParseProcessTreeParams binds and checks ptg()'s arguments: depth defaults to 10 (max 50) and
// direction to both.
func ParseProcessTreeParams(cmd CommandNode) (ProcessTreeParams, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return ProcessTreeParams{}, err
	}
	p := ProcessTreeParams{Start: b.Str("start", ""), Depth: 10, Direction: strings.ToLower(b.Str("direction", "both"))}
	if p.Start == "" {
		return p, fmt.Errorf("ptg() requires a start= parameter, e.g. ptg(start=\"<process_guid>\")")
	}
	if d := b.Int("depth", 0); d > 0 {
		p.Depth = min(d, 50)
	}
	if p.Direction != "forward" && p.Direction != "backward" && p.Direction != "both" {
		return p, fmt.Errorf("ptg() direction= must be forward, backward, or both (got %q)", p.Direction)
	}
	return p, nil
}

// ProcessTreeCommand returns the arguments of the pipeline's ptg(), if it has one, so the query
// layer can walk the tree before translating.
func ProcessTreeCommand(pipeline *PipelineNode) (ProcessTreeParams, bool, error) {
	if pipeline == nil {
		return ProcessTreeParams{}, false, nil
	}
	for _, c := range pipeline.Commands {
		if c.Name == "ptg" {
			p, err := ParseProcessTreeParams(c)
			return p, true, err
		}
	}
	return ProcessTreeParams{}, false, nil
}

// processTreeViews draw ptg()'s parent/child rows as they are.
var processTreeViews = map[string]bool{"pgraph": true, "graph": true}

// validateProcessTreeTail rejects what ptg() cannot apply. Its rows are the walked tree, so
// after it only _depth/_path filters and a graph take effect; anything else would be dropped.
func validateProcessTreeTail(pipeline *PipelineNode) error {
	at := commandIndex(pipeline.Commands, "ptg")
	if at < 0 {
		return nil
	}
	const hint = "filter with _depth or _path, or pipe to pgraph() or graph()"
	for _, cmd := range pipeline.Commands[at+1:] {
		if !processTreeViews[canonicalCommandName(cmd.Name)] {
			return fmt.Errorf("ptg() returns its process tree as is, so %s() cannot follow it: %s", cmd.Name, hint)
		}
	}
	for _, a := range pipeline.Assignments {
		if a.CmdIndex > at {
			return fmt.Errorf("ptg() returns its process tree as is, so %s := cannot follow it: %s", a.Field, hint)
		}
	}
	for _, c := range pipeline.HavingConditions {
		if c.CmdIndex > at {
			if f := nonTreeField(c); f != "" {
				return fmt.Errorf("ptg() can only be filtered on _depth and _path, not %s", f)
			}
		}
	}
	return nil
}

// nonTreeField returns the first field in cond that is not _depth or _path, or "".
func nonTreeField(cond HavingCondition) string {
	if cond.IsCompound {
		for _, child := range cond.Children {
			if f := nonTreeField(child); f != "" {
				return f
			}
		}
		return ""
	}
	if cond.Field != "_depth" && cond.Field != "_path" {
		if cond.Field == "" {
			return "that condition"
		}
		return cond.Field
	}
	return ""
}

// BuildProcessTreeRowsSQL reads the own process_creation row of every node in
// ProvenanceTreeTable (a node is its own member), within the query window like the walk.
func BuildProcessTreeRowsSQL(opts QueryOptions) string {
	conds := append(procLineageWindow(opts, ""), "process_guid GLOBAL IN (SELECT guid FROM "+ProvenanceTreeTable+" WHERE guid = node)")
	return fmt.Sprintf("SELECT process_guid, parent_guid, toString(timestamp) AS ts, log_id, fractal_id, image, parent_image, "+
		"substring(commandline, 1, %d) AS commandline, computer_name FROM %s FINAL WHERE %s",
		maxProcessTreeCommandline, procLineageTable(opts), strings.Join(conds, " AND "))
}

// maxProcessTreeCommandline caps each command line inlined into ptg()'s literal source, so
// 1,000 nodes stay well inside max_query_size.
const maxProcessTreeCommandline = 4096

// processTreeValuesSQL renders the tree as one values() source with the columns ptg() selects.
func processTreeValuesSQL(t *ProcessTree) string {
	const structure = "timestamp String, log_id String, fractal_id String, process_guid String, parent_guid String, " +
		"image String, parent_image String, commandline String, computer_name String, _depth UInt32, _path String, " +
		"group_size UInt64, group_first String, group_last String"
	if len(t.Rows) == 0 {
		return "(SELECT * FROM values('" + structure + "', ('', '', '', '', '', '', '', '', '', 0, '', 0, '', '')) WHERE 0)"
	}
	var sb strings.Builder
	sb.WriteString("(SELECT * FROM values('" + structure + "'")
	for _, r := range t.Rows {
		sb.WriteString(", (")
		for i, s := range []string{r.Timestamp, r.LogID, r.FractalID, r.ProcessGUID, r.ParentGUID, r.Image, r.ParentImage, r.Commandline, r.ComputerName} {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(SQLStringLiteral(s))
		}
		sb.WriteString(", " + strconv.Itoa(r.Depth) + ", " + SQLStringLiteral(r.Path) + ", " + strconv.FormatInt(r.GroupSize, 10) +
			", " + SQLStringLiteral(r.GroupFirst) + ", " + SQLStringLiteral(r.GroupLast) + ")")
	}
	sb.WriteString("))")
	return sb.String()
}

var sqlLiteralEscaper = strings.NewReplacer("\\", "\\\\", "'", "\\'", "\n", "\\n", "\r", "\\r", "\t", "\\t", "\x00", "\\0")

// SQLStringLiteral quotes s as a ClickHouse string literal. Unlike escapeString it also escapes
// control characters, for values that come from log data.
func SQLStringLiteral(s string) string {
	return "'" + sqlLiteralEscaper.Replace(s) + "'"
}

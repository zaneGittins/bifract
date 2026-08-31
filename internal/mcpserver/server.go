// Package mcpserver exposes Bifract to AI agents over the Model Context Protocol.
// It runs on the analyst's machine and reaches the instance over the same HTTP API
// the UI uses, so a tool can never do more than its API key allows.
package mcpserver

import (
	"context"
	"fmt"

	"bifract/pkg/aitools"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Version is the build identity reported to the client. cmd/bifract sets it so
// the MCP server and the CLI it ships in always agree.
var Version = "dev"

const instructions = `You are connected to Bifract, a log management and detection platform. The API key
fixes which fractal (an isolated container of logs, alerts, comments and watchlists)
you can see, and the role that governs what you may change.

Start here:
  get_context        which fractal, what role, which server.
  get_fields         the field names this fractal's logs actually carry.
  get_bql_reference  BQL's commands and operators.

Querying. query_logs runs BQL against hot storage. Field values live under ` + "`fields`" + `
but BQL names them bare: host=web-01, not fields.host. Run validate_bql on anything
non-trivial first; it costs one round trip and catches syntax errors before a scan.
Volume reaches billions of rows, so filter and bound the time range before widening.
Times are RFC3339 and default to the last 24 hours.

Investigating. find_processes locates a process_guid, and get_provenance_graph
expands it into a scored process tree for endpoint data. search_dictionary checks an
indicator against the watchlists already in place. When a hunt reaches past hot
retention, search_archive runs the same BQL over the archive; it is minutes-slow and
returns a job to poll, so reach for it only once query_logs cannot answer.

Detections. list_alerts shows what is already watched and is the best guide to this
fractal's real query patterns. get_attack_coverage and get_attack_gaps say which
ATT&CK techniques are covered and which are worth covering next. Validate a query
before creating an alert on it.

Recording. Findings belong in the product, not only in your reply. There is one way
to mark an event: add_comment on the log that evidences it. Pass a notebook_id and
the same call files it into an investigation, so create_notebook once at the start of
a hunt and file every finding into it as you go, with add_notebook_section for the
narrative and the queries between them. Tag related comments IR-<Name> as well. This
is a collaborative platform and other analysts read what you leave behind.

Writes are real. Creating an alert, or adding rows to a dictionary, changes what a
live system detects. Confirm scope with get_context before changing anything, and
prefer reading the current state before overwriting it.`

// New builds the server with every tool registered against c.
func New(c *Client) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: "bifract", Version: Version},
		&mcp.ServerOptions{Instructions: instructions},
	)
	aitools.Serve(s, c, aitools.All())
	addContextTool(s, c)
	return s
}

// Run serves MCP over stdio until the client disconnects. Nothing may be written
// to stdout but protocol traffic, so diagnostics go to stderr.
func Run(ctx context.Context, args []string) error {
	for _, arg := range args {
		switch arg {
		case "--help", "-h":
			fmt.Println(usage)
			return nil
		default:
			return fmt.Errorf("unknown --mcp argument: %s\n\n%s", arg, usage)
		}
	}

	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	return New(NewClient(cfg)).Run(ctx, &mcp.StdioTransport{})
}

const usage = `bifract --mcp

Serve Bifract to an MCP client (Claude Code, Claude Desktop) over stdio.

Configured entirely by environment, so it can be launched by a client that only
knows how to set variables:

  BIFRACT_URL          Base URL of the instance. Required.
  BIFRACT_API_KEY      API key. Required. A key issued for one fractal or prism
                       fixes the scope and the role by itself.
  BIFRACT_FRACTAL_ID   The fractal to act in. Needed only for an instance-wide
                       key (bifract_admin_...), which belongs to no fractal.
  BIFRACT_PRISM_ID     A prism to act in instead, for reading across fractals.
  BIFRACT_CA_CERT      CA bundle, for an instance behind a private CA.
  BIFRACT_CLIENT_CERT  Client certificate for mTLS, or a combined PEM.
  BIFRACT_CLIENT_KEY   Client key, when the certificate does not carry it.
  BIFRACT_VERIFY_SSL   false to skip verification. Prefer BIFRACT_CA_CERT.
  BIFRACT_TIMEOUT      Request timeout in seconds. Default 60.`

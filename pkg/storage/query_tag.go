package storage

import (
	"context"
	"encoding/json"
	"strings"
)

// Query sources. Every ClickHouse query Bifract issues carries the activity that
// issued it in its log_comment, so system.processes and system.query_log read back
// as "who did what" rather than anonymous SQL. Without it the admin activity view
// can only show the query text, which is why it had to guess at Bifract's own
// polling by substring-matching the SQL.
const (
	SourceSearch    = "search"
	SourceDashboard = "dashboard"
	SourceNotebook  = "notebook"
	SourceAlert     = "alert"
	SourceRecall    = "recall"
	SourceModel     = "model"
	SourceChat      = "chat"
	SourceIngest    = "ingest"
	SourceSystem    = "system"
)

// LabelActivity marks the admin activity view's own introspection queries so the
// view can exclude them from what it displays.
const LabelActivity = "activity"

// maxQueryTagLen bounds the rendered log_comment. ClickHouse stores it per query in
// system.query_log, so an unbounded tag would be written once per query forever.
// Label and BQL are truncated to fit; the identity fields are never dropped.
const maxQueryTagLen = 900

// maxTagBQLLen bounds the BQL carried alongside a query. Long enough for a real
// search, short enough that it is not the reason the query log grows.
const maxTagBQLLen = 400

// QueryTag is the attribution one ClickHouse query carries. Source is required;
// the rest are filled in where the caller knows them.
type QueryTag struct {
	Source  string `json:"src"`
	User    string `json:"user,omitempty"`
	Fractal string `json:"fractal,omitempty"`
	Label   string `json:"label,omitempty"`
	// BQL is the query the user actually wrote, carried so the admin activity
	// view can show it beside the SQL it was translated into. Set only for the
	// interactive sources: alert evaluation runs on a timer and is the highest
	// volume tagged class, and its alert name already names the definition.
	//
	// New fields go last. The activity view classifies rows with
	// startsWith(log_comment, '{"src":...'), so "src" has to stay first.
	BQL string `json:"bql,omitempty"`
}

// String renders the tag as the JSON object stored in log_comment. Readers pull
// fields back out with JSONExtractString, so the shape is part of the contract.
func (t QueryTag) String() string {
	if t.Source == "" {
		return ""
	}
	t.Label = truncateTagField(t.Label, 160)
	t.User = truncateTagField(t.User, 64)
	t.BQL = truncateTagField(collapseTagWhitespace(t.BQL), maxTagBQLLen)
	b, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	// Shed the descriptive fields before the identifying ones, biggest first.
	for _, drop := range []*string{&t.BQL, &t.Label} {
		if len(b) <= maxQueryTagLen {
			break
		}
		*drop = ""
		if b, err = json.Marshal(t); err != nil {
			return ""
		}
	}
	if len(b) > maxQueryTagLen {
		return ""
	}
	return string(b)
}

// collapseTagWhitespace flattens a multi-line query onto one line. The tag is read
// back in a table cell, and the newlines only cost bytes.
func collapseTagWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// truncateTagField cuts a tag field at a rune boundary so the rendered JSON stays
// valid UTF-8.
func truncateTagField(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	cut := s[:max]
	for len(cut) > 0 && !isUTF8Boundary(s, len(cut)) {
		cut = cut[:len(cut)-1]
	}
	return cut
}

// isUTF8Boundary reports whether i starts a rune in s.
func isUTF8Boundary(s string, i int) bool {
	return i >= len(s) || s[i]&0xC0 != 0x80
}

// tagKey carries a QueryTag on a context.
type tagKey struct{}

// TagContext attaches attribution to every ClickHouse query run on ctx. Calls
// nest: a tag set closer to the query wins.
func TagContext(ctx context.Context, tag QueryTag) context.Context {
	if tag.Source == "" {
		return ctx
	}
	return context.WithValue(ctx, tagKey{}, tag)
}

// SystemTagContext marks ctx as Bifract's own background work, labelled so the
// activity view can name it.
func SystemTagContext(ctx context.Context, label string) context.Context {
	return TagContext(ctx, QueryTag{Source: SourceSystem, Label: label})
}

// ContextTag returns the tag ctx carries, or the zero value.
func ContextTag(ctx context.Context) QueryTag {
	tag, _ := ctx.Value(tagKey{}).(QueryTag)
	return tag
}

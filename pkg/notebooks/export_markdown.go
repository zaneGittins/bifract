package notebooks

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// Markdown export.
//
// A notebook is opened once, at the end, to compose and hand off, so the
// rendered output is what the feature is judged on. YAML round-trips a notebook
// back into Bifract; this is what goes into someone else's case system, so it
// reads as a report: the evidence in the order it happened, each item carrying
// the fields and the sentence saying why it was kept.

// maxExportRows caps the rows rendered under a query step. A report nobody can
// read is not a report; the notebook itself still holds the full cached page.
const maxExportRows = 25

// markdownExportOptions narrows what an export carries.
type markdownExportOptions struct {
	// Chronological orders by event time rather than by position in the
	// notebook, which is how an investigation is read back.
	Chronological bool
	// Tags, when set, keeps only sections carrying one of them. Slicing an
	// investigation is how one part of it goes to someone else.
	Tags []string
}

// exportMarkdown renders a notebook as a report.
func exportMarkdown(nb *storage.Notebook, sections []storage.NotebookSection, opts markdownExportOptions) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# %s\n\n", nb.Name)
	if d := strings.TrimSpace(nb.Description); d != "" {
		fmt.Fprintf(&b, "%s\n\n", d)
	}
	if isHTTPURL(nb.ExternalRefURL) {
		label := nb.ExternalRefLabel
		if label == "" {
			label = nb.ExternalRefURL
		}
		fmt.Fprintf(&b, "Case: [%s](%s)\n\n", mdLinkText(label), nb.ExternalRefURL)
	}
	fmt.Fprintf(&b, "Exported %s UTC", time.Now().UTC().Format("2006-01-02 15:04"))
	if len(opts.Tags) > 0 {
		fmt.Fprintf(&b, " - filtered to %s", strings.Join(opts.Tags, ", "))
	}
	b.WriteString("\n\n")

	// Whether the notebook can still change is part of the claim the report
	// makes, so it travels with it rather than living only in the UI.
	if nb.IsLocked() {
		fmt.Fprintf(&b, "Locked %s UTC by %s. Query results are the rows stored at that time.\n\n",
			nb.LockedAt.UTC().Format("2006-01-02 15:04"), nb.LockedBy)
	} else {
		b.WriteString("Draft: this notebook is unlocked and can still change.\n\n")
	}
	b.WriteString("---\n\n")

	visible := filterSections(sections, opts)
	if len(visible) == 0 {
		b.WriteString("_Nothing to export._\n")
		return b.String()
	}
	if opts.Chronological {
		sortChronologically(visible)
	}

	for _, s := range visible {
		writeSection(&b, s)
	}
	return b.String()
}

func filterSections(sections []storage.NotebookSection, opts markdownExportOptions) []storage.NotebookSection {
	if len(opts.Tags) == 0 {
		return sections
	}
	want := map[string]bool{}
	for _, t := range opts.Tags {
		want[strings.ToLower(t)] = true
	}
	out := make([]storage.NotebookSection, 0, len(sections))
	for _, s := range sections {
		for _, t := range sectionTags(s) {
			if want[strings.ToLower(t)] {
				out = append(out, s)
				break
			}
		}
	}
	return out
}

// sectionTags reads the tags that matter for this section: evidence carries the
// comment's, everything else its own.
func sectionTags(s storage.NotebookSection) []string {
	if s.SectionType == "comment_context" {
		var data struct {
			Tags []string `json:"tags"`
		}
		if err := json.Unmarshal([]byte(s.Content), &data); err == nil {
			return data.Tags
		}
		return nil
	}
	return s.Tags
}

// sortChronologically puts dated sections in event order and undated ones at the
// end, matching how the rail reads a notebook as a timeline.
func sortChronologically(sections []storage.NotebookSection) {
	sort.SliceStable(sections, func(i, j int) bool {
		a, b := sections[i].EventTime, sections[j].EventTime
		switch {
		case a == nil && b == nil:
			return sections[i].OrderIndex < sections[j].OrderIndex
		case a == nil:
			return false
		case b == nil:
			return true
		case a.Equal(*b):
			return sections[i].OrderIndex < sections[j].OrderIndex
		default:
			return a.Before(*b)
		}
	})
}

func writeSection(b *strings.Builder, s storage.NotebookSection) {
	title := ""
	if s.Title != nil {
		title = strings.TrimSpace(*s.Title)
	}

	switch s.SectionType {
	case "comment_context":
		writeEvidence(b, s, title)
	case "query":
		if title == "" {
			title = "Query"
		}
		fmt.Fprintf(b, "## %s\n\n", title)
		writeWhen(b, s)
		fmt.Fprintf(b, "```bql\n%s\n```\n\n", strings.TrimSpace(s.Content))
		if s.LastExecutedAt != nil {
			fmt.Fprintf(b, "Results as of %s UTC\n\n", s.LastExecutedAt.UTC().Format("2006-01-02 15:04"))
		}
		writeResults(b, s.LastResults)
	default:
		if title != "" {
			fmt.Fprintf(b, "## %s\n\n", title)
		}
		fmt.Fprintf(b, "%s\n\n", strings.TrimSpace(s.Content))
	}

	if tags := sectionTags(s); len(tags) > 0 {
		fmt.Fprintf(b, "Tags: %s\n\n", strings.Join(tags, ", "))
	}
	b.WriteString("---\n\n")
}

func writeEvidence(b *strings.Builder, s storage.NotebookSection, title string) {
	var data struct {
		LogID             string `json:"log_id"`
		LogTimestamp      string `json:"log_timestamp"`
		CommentText       string `json:"comment_text"`
		Query             string `json:"query"`
		AuthorDisplayName string `json:"author_display_name"`
		Author            string `json:"author"`
	}
	json.Unmarshal([]byte(s.Content), &data)

	if title == "" {
		title = "Event " + shortID(data.LogID)
	}
	fmt.Fprintf(b, "## %s\n\n", title)

	when := data.LogTimestamp
	if s.EventTime != nil {
		when = s.EventTime.UTC().Format(time.RFC3339)
	}
	if when != "" {
		fmt.Fprintf(b, "**%s UTC**", strings.Replace(strings.TrimSuffix(when, "Z"), "T", " ", 1))
		if author := firstNonEmpty(data.AuthorDisplayName, data.Author); author != "" {
			fmt.Fprintf(b, " - %s", author)
		}
		b.WriteString("\n\n")
	}

	if text := strings.TrimSpace(data.CommentText); text != "" {
		fmt.Fprintf(b, "%s\n\n", text)
	}
	if data.LogID != "" {
		fmt.Fprintf(b, "`log_id=\"%s\"`\n\n", data.LogID)
	}
	if q := strings.TrimSpace(data.Query); q != "" {
		fmt.Fprintf(b, "Found by:\n\n```bql\n%s\n```\n\n", q)
	}
	writeResults(b, s.LastResults)
}

func writeWhen(b *strings.Builder, s storage.NotebookSection) {
	if s.EventTime == nil {
		return
	}
	fmt.Fprintf(b, "**%s UTC**\n\n", s.EventTime.UTC().Format("2006-01-02 15:04:05"))
}

// writeResults renders a section's cached rows as a table, which is what makes
// an exported investigation readable without Bifract in front of you.
func writeResults(b *strings.Builder, raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var payload struct {
		Results []map[string]interface{} `json:"results"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || len(payload.Results) == 0 {
		return
	}

	rows := payload.Results
	truncated := false
	if len(rows) > maxExportRows {
		rows, truncated = rows[:maxExportRows], true
	}

	cols := columnsOf(rows)
	if len(cols) == 0 {
		return
	}

	fmt.Fprintf(b, "| %s |\n", strings.Join(cols, " | "))
	fmt.Fprintf(b, "|%s\n", strings.Repeat(" --- |", len(cols)))
	for _, row := range rows {
		cells := make([]string, 0, len(cols))
		for _, c := range cols {
			cells = append(cells, cellText(row[c]))
		}
		fmt.Fprintf(b, "| %s |\n", strings.Join(cells, " | "))
	}
	if truncated {
		fmt.Fprintf(b, "\n_%d of %d rows shown._\n", maxExportRows, len(payload.Results))
	}
	b.WriteString("\n")
}

// columnsOf keeps timestamp first, then the rest alphabetically, so every table
// in the report reads the same way.
func columnsOf(rows []map[string]interface{}) []string {
	seen := map[string]bool{}
	var cols []string
	for _, row := range rows {
		for k := range row {
			if k == "_all_fields" || strings.HasPrefix(k, "_") || seen[k] {
				continue
			}
			seen[k] = true
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	for i, c := range cols {
		if c == "timestamp" && i != 0 {
			cols = append([]string{c}, append(cols[:i:i], cols[i+1:]...)...)
			break
		}
	}
	return cols
}

// cellText flattens a value into one table cell. Pipes and newlines would break
// the row, so both are neutralised rather than escaped into noise.
func cellText(v interface{}) string {
	if v == nil {
		return ""
	}
	var s string
	switch typed := v.(type) {
	case string:
		s = typed
	case map[string]interface{}, []interface{}:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		s = string(encoded)
	default:
		s = fmt.Sprint(typed)
	}
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}

// mdLinkText neutralises the characters a label would need to close its own
// link and open another. The label is user-written and the report is pasted into
// someone else's system, which may well render it.
var mdLinkEscaper = strings.NewReplacer("[", "\\[", "]", "\\]", "(", "\\(", ")", "\\)", "\n", " ", "\r", " ")

func mdLinkText(s string) string {
	return strings.TrimSpace(mdLinkEscaper.Replace(s))
}

// isHTTPURL is the same rule the write path enforces, applied again on render:
// a value stored before that check existed must not become a javascript: link
// in a document handed to someone else.
func isHTTPURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

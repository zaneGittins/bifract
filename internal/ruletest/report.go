package ruletest

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"

	"bifract/internal/ingestcli"
)

// Shared styling with the ingest CLI so the two subcommands look like one tool.
func dim(s string) string     { return ingestcli.DimStyle.Render(s) }
func warn(s string) string    { return ingestcli.WarningStyle.Render(s) }
func okMark(s string) string  { return ingestcli.SuccessStyle.Render(s) }
func badMark(s string) string { return ingestcli.ErrorStyle.Render(s) }

// Report writes the summary in the requested format.
func Report(w io.Writer, s *Summary, format string, explain bool) error {
	switch format {
	case "json":
		return reportJSON(w, s)
	case "junit":
		return reportJUnit(w, s)
	default:
		return reportText(w, s, explain)
	}
}

func reportText(w io.Writer, s *Summary, explain bool) error {
	for _, sr := range s.Specs {
		if sr.Error != "" {
			fmt.Fprintf(w, "%s %s\n", badMark("ERROR"), sr.Path)
			fmt.Fprintf(w, "      %s\n", sr.Error)
			continue
		}

		name := sr.Rule
		if name == "" {
			name = sr.Path
		}
		fmt.Fprintf(w, "%s %s\n", ingestcli.BoldStyle.Render(name), dim("("+sr.Path+")"))

		for _, c := range sr.Results {
			if c.Passed {
				fmt.Fprintf(w, "  %s %s %s\n", okMark("PASS"), c.Case, dim(verdictNote(c)))
				continue
			}
			fmt.Fprintf(w, "  %s %s\n", badMark("FAIL"), c.Case)
			fmt.Fprintf(w, "       %s\n", c.Reason)

			if explain {
				writeExplain(w, c)
			}
		}
		fmt.Fprintln(w)
	}

	total := s.Passed + s.Failed
	line := fmt.Sprintf("%d/%d passed", s.Passed, total)
	if s.Failed > 0 {
		line += fmt.Sprintf(", %d failed", s.Failed)
	}
	if s.Errored > 0 {
		line += fmt.Sprintf(", %d spec error(s)", s.Errored)
	}
	line += fmt.Sprintf("  %s", dim(s.Duration.Round(1e6).String()))

	if s.OK() {
		fmt.Fprintln(w, okMark(line))
	} else {
		fmt.Fprintln(w, badMark(line))
	}
	return nil
}

// writeExplain prints what is almost always the actual problem: the field names the
// rule looked for versus the field names normalization actually produced.
func writeExplain(w io.Writer, c CaseResult) {
	if c.BQL != "" {
		fmt.Fprintf(w, "       %s %s\n", dim("BQL:"), c.BQL)
	}
	for i, f := range c.Fields {
		fmt.Fprintf(w, "       %s\n", dim(fmt.Sprintf("normalized fields (log %d):", i+1)))
		keys := make([]string, 0, len(f))
		for k := range f {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "         %s = %s\n", k, truncate(f[k], 120))
		}
	}
	// Printed in full: a truncated query is useless for the copy-into-ClickHouse
	// debugging this flag exists to support.
	if c.SQL != "" {
		fmt.Fprintf(w, "       %s %s\n", dim("SQL:"), c.SQL)
	}
}

func verdictNote(c CaseResult) string {
	// Multiple independently judged logs: report the ratio, which is the assertion
	// that actually held. A single unit reports rows, which is more informative for
	// a batched threshold case.
	if c.Units > 1 {
		return fmt.Sprintf("(%d/%d logs matched)", c.UnitsMatched, c.Units)
	}
	if c.Expect == ExpectMatch {
		return fmt.Sprintf("(%d row(s))", c.Rows)
	}
	return "(no match)"
}

func reportJSON(w io.Writer, s *Summary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}

// JUnit XML, so GitHub and GitLab render per-case results natively.
type junitSuites struct {
	XMLName  xml.Name     `xml:"testsuites"`
	Tests    int          `xml:"tests,attr"`
	Failures int          `xml:"failures,attr"`
	Errors   int          `xml:"errors,attr"`
	Suites   []junitSuite `xml:"testsuite"`
}

type junitSuite struct {
	Name     string      `xml:"name,attr"`
	Tests    int         `xml:"tests,attr"`
	Failures int         `xml:"failures,attr"`
	Errors   int         `xml:"errors,attr"`
	Cases    []junitCase `xml:"testcase"`
}

type junitCase struct {
	Name    string        `xml:"name,attr"`
	Time    float64       `xml:"time,attr"`
	Failure *junitMessage `xml:"failure,omitempty"`
	Error   *junitMessage `xml:"error,omitempty"`
}

type junitMessage struct {
	Message string `xml:"message,attr"`
	Body    string `xml:",chardata"`
}

func reportJUnit(w io.Writer, s *Summary) error {
	out := junitSuites{Tests: s.Passed + s.Failed, Failures: s.Failed, Errors: s.Errored}

	for _, sr := range s.Specs {
		name := sr.Rule
		if name == "" {
			name = sr.Path
		}
		suite := junitSuite{Name: name}

		if sr.Error != "" {
			suite.Errors = 1
			suite.Tests = 1
			suite.Cases = append(suite.Cases, junitCase{
				Name:  "load spec",
				Error: &junitMessage{Message: "spec could not be loaded", Body: sr.Error},
			})
			out.Suites = append(out.Suites, suite)
			continue
		}

		for _, c := range sr.Results {
			jc := junitCase{Name: c.Case, Time: c.Duration.Seconds()}
			if !c.Passed {
				body := c.Reason
				if c.BQL != "" {
					body += "\nBQL: " + c.BQL
				}
				jc.Failure = &junitMessage{Message: c.Reason, Body: body}
				suite.Failures++
			}
			suite.Tests++
			suite.Cases = append(suite.Cases, jc)
		}
		out.Suites = append(out.Suites, suite)
	}

	if _, err := io.WriteString(w, xml.Header); err != nil {
		return err
	}
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(out); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

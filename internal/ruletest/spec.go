package ruletest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// SpecSuffix is the canonical rule test file suffix, used in help and error text.
const SpecSuffix = ".test.yaml"

// specSuffixes are all suffixes discovery recognizes. Both YAML spellings are
// accepted: a repo that standardizes on .yml would otherwise find zero tests and
// report success, which is the worst way for a detection gate to fail.
var specSuffixes = []string{".test.yaml", ".test.yml"}

// IsSpecFile reports whether a filename is a rule test spec. Everything else in a
// detection tree (rules, normalizers, raw event samples) is ignored by discovery.
func IsSpecFile(name string) bool {
	for _, s := range specSuffixes {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

// Expectation is what a case asserts about the rule's verdict.
type Expectation string

const (
	ExpectMatch   Expectation = "match"
	ExpectNoMatch Expectation = "no_match"
)

// Spec is one test file: a rule, the normalizer to interpret it with, and the
// cases to run against it.
type Spec struct {
	Rule       string `yaml:"rule"`
	Normalizer string `yaml:"normalizer"`
	Cases      []Case `yaml:"cases"`

	// Path is the spec file this was loaded from. Relative rule, normalizer and
	// logFile references resolve against its directory, so a detection tree can
	// be relocated wholesale.
	Path string `yaml:"-"`
}

// Case is a single assertion: these logs, against that rule, must (or must not)
// trigger.
type Case struct {
	Name    string                   `yaml:"name"`
	Expect  string                   `yaml:"expect"`
	Count   *int                     `yaml:"count"`
	Log     map[string]interface{}   `yaml:"log"`
	Logs    []map[string]interface{} `yaml:"logs"`
	LogFile string                   `yaml:"logFile"`

	// Together presents the case's logs to the rule as one batch rather than
	// evaluating each on its own. Threshold and correlation rules need it, since they
	// only fire when the rule sees several events at once.
	//
	// By default every log is judged independently and the case passes only if all of
	// them meet the expectation.
	Together bool `yaml:"together"`

	expectation Expectation
	logs        []map[string]interface{}
}

// Expectation returns the parsed, validated expectation.
func (c *Case) Expectation() Expectation { return c.expectation }

// ResolvedLogs returns the log objects the case will run, whether they came from
// 'log', 'logs' or 'logFile'.
func (c *Case) ResolvedLogs() []map[string]interface{} { return c.logs }

// DiscoverSpecs returns every spec file reachable from the given paths. A path may
// be a spec file itself or a directory to walk.
func DiscoverSpecs(paths []string) ([]string, error) {
	var found []string
	seen := make(map[string]bool)

	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}

		if !info.IsDir() {
			abs, _ := filepath.Abs(p)
			if !seen[abs] {
				seen[abs] = true
				found = append(found, p)
			}
			continue
		}

		err = filepath.Walk(p, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() || !IsSpecFile(fi.Name()) {
				return nil
			}
			abs, _ := filepath.Abs(path)
			if !seen[abs] {
				seen[abs] = true
				found = append(found, path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walking %s: %w", p, err)
		}
	}

	sort.Strings(found)
	return found, nil
}

// LoadSpec reads and validates a spec file, resolving each case's logs.
func LoadSpec(path string) (*Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var spec Spec
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&spec); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	spec.Path = path

	if strings.TrimSpace(spec.Rule) == "" {
		return nil, fmt.Errorf("%s: missing required field 'rule'", path)
	}
	if len(spec.Cases) == 0 {
		return nil, fmt.Errorf("%s: no cases defined", path)
	}

	dir := filepath.Dir(path)
	spec.Rule = resolveRelative(dir, spec.Rule)
	if spec.Normalizer != "" {
		spec.Normalizer = resolveRelative(dir, spec.Normalizer)
	}

	for i := range spec.Cases {
		c := &spec.Cases[i]
		if strings.TrimSpace(c.Name) == "" {
			return nil, fmt.Errorf("%s: case %d is missing a name", path, i+1)
		}

		exp, err := parseExpectation(c.Expect)
		if err != nil {
			return nil, fmt.Errorf("%s: case %q: %w", path, c.Name, err)
		}
		c.expectation = exp

		if c.Count != nil {
			if *c.Count < 0 {
				return nil, fmt.Errorf("%s: case %q: count must not be negative", path, c.Name)
			}
			if exp == ExpectNoMatch && *c.Count != 0 {
				return nil, fmt.Errorf("%s: case %q: count is meaningless with expect: no_match", path, c.Name)
			}
			if !c.Together {
				return nil, fmt.Errorf("%s: case %q: count requires 'together: true' "+
					"(logs are judged one at a time by default, so a single row count has no meaning)", path, c.Name)
			}
		}

		logs, err := resolveCaseLogs(dir, c)
		if err != nil {
			return nil, fmt.Errorf("%s: case %q: %w", path, c.Name, err)
		}
		c.logs = logs
	}

	return &spec, nil
}

// parseExpectation normalizes the accepted spellings of the two verdicts. Anything
// else is rejected rather than guessed at: silently treating a typo as "no_match"
// would turn a broken test into a passing one.
func parseExpectation(s string) (Expectation, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "match", "true", "trigger", "triggers":
		return ExpectMatch, nil
	case "no_match", "no-match", "nomatch", "false", "none":
		return ExpectNoMatch, nil
	case "":
		return "", fmt.Errorf("missing required field 'expect' (use 'match' or 'no_match')")
	default:
		return "", fmt.Errorf("unknown expect value %q (use 'match' or 'no_match')", s)
	}
}

// resolveCaseLogs collects the case's logs from exactly one of log/logs/logFile.
func resolveCaseLogs(dir string, c *Case) ([]map[string]interface{}, error) {
	sources := 0
	if c.Log != nil {
		sources++
	}
	if c.Logs != nil {
		sources++
	}
	if c.LogFile != "" {
		sources++
	}

	switch sources {
	case 0:
		return nil, fmt.Errorf("no logs: set one of 'log', 'logs' or 'logFile'")
	case 1:
	default:
		return nil, fmt.Errorf("set only one of 'log', 'logs' or 'logFile'")
	}

	var logs []map[string]interface{}
	switch {
	case c.Log != nil:
		logs = []map[string]interface{}{c.Log}
	case c.Logs != nil:
		logs = c.Logs
	default:
		data, err := os.ReadFile(resolveRelative(dir, c.LogFile))
		if err != nil {
			return nil, err
		}
		logs, err = ParseLogFile(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.LogFile, err)
		}
	}

	if len(logs) == 0 {
		return nil, fmt.Errorf("no logs found")
	}

	// Round-trip through JSON so inline YAML logs carry exactly the Go types an
	// HTTP-ingested log would (numbers as float64, etc). Without this a YAML int
	// and a JSON number could stringify differently and a test would disagree with
	// production for reasons that have nothing to do with the rule.
	out := make([]map[string]interface{}, 0, len(logs))
	for i, l := range logs {
		normalized, err := jsonRoundTrip(l)
		if err != nil {
			return nil, fmt.Errorf("log %d: %w", i+1, err)
		}
		out = append(out, normalized)
	}
	return out, nil
}

func jsonRoundTrip(obj map[string]interface{}) (map[string]interface{}, error) {
	encoded, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ParseLogFile accepts the same shapes the ingest endpoint does: a JSON array, a
// single JSON object, or NDJSON.
func ParseLogFile(data []byte) ([]map[string]interface{}, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	var arr []map[string]interface{}
	if err := json.Unmarshal(trimmed, &arr); err == nil {
		return arr, nil
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(trimmed, &obj); err == nil {
		return []map[string]interface{}{obj}, nil
	}

	var logs []map[string]interface{}
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), 10*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var o map[string]interface{}
		if err := json.Unmarshal([]byte(text), &o); err != nil {
			return nil, fmt.Errorf("line %d is not valid JSON: %w", line, err)
		}
		logs = append(logs, o)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("not valid JSON array, object, or NDJSON")
	}
	return logs, nil
}

// resolveRelative resolves p against dir unless it is already absolute.
func resolveRelative(dir, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(dir, p)
}

package ruletest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"bifract/internal/ingestcli"
	"bifract/pkg/normalizers"
)

// Options is the parsed command line.
type Options struct {
	Paths          []string
	NormalizerPath string
	ClickHouse     string
	CHUser         string
	CHPassword     string
	Format         string
	Explain        bool
	Verbose        bool
	Lint           bool

	// Ad hoc mode: a rule plus logs, with no spec file.
	LogPaths []string
	Expect   string
}

// ExitFailure is returned when tests ran but did not all pass. main maps it to
// exit code 1, distinct from a harness error.
type ExitFailure struct{}

func (ExitFailure) Error() string { return "one or more rule tests failed" }

// RunTest is the entry point for `bifract --test`. It accepts the raw args slice
// after "--test" has been consumed.
func RunTest(args []string) error {
	opts, err := parseArgs(args)
	if err != nil {
		return err
	}
	if opts == nil {
		return nil // help was printed
	}

	if opts.Lint {
		return runLint(opts)
	}

	loads, err := collectSpecs(opts)
	if err != nil {
		return err
	}
	if len(loads) == 0 {
		return fmt.Errorf("no %s files found in the given path(s)", SpecSuffix)
	}

	var target *Target
	if opts.ClickHouse != "" {
		t, err := ParseTarget(opts.ClickHouse, opts.CHUser, opts.CHPassword)
		if err != nil {
			return err
		}
		target = &t
	}

	ctx := context.Background()
	backend, err := Connect(ctx, target, opts.Verbose)
	if err != nil {
		return err
	}
	// Use a background context for teardown so the scratch table is still dropped
	// if the run itself was cancelled.
	defer backend.Close(context.Background())

	summary, err := Run(ctx, backend, loads, *opts)
	if err != nil {
		return err
	}

	if err := Report(os.Stdout, summary, opts.Format, opts.Explain); err != nil {
		return err
	}
	if !summary.OK() {
		return ExitFailure{}
	}
	return nil
}

// collectSpecs builds the run list, either from discovered spec files or from an ad
// hoc --logs/--expect invocation.
func collectSpecs(opts *Options) ([]SpecLoad, error) {
	if len(opts.LogPaths) > 0 {
		spec, err := adHocSpec(opts)
		if err != nil {
			return nil, err
		}
		return []SpecLoad{{Path: spec.Path, Spec: spec}}, nil
	}

	paths, err := DiscoverSpecs(opts.Paths)
	if err != nil {
		return nil, err
	}

	loads := make([]SpecLoad, 0, len(paths))
	for _, p := range paths {
		spec, err := LoadSpec(p)
		loads = append(loads, SpecLoad{Path: p, Spec: spec, Err: err})
	}
	return loads, nil
}

// adHocSpec synthesizes a one-case spec from `--test rule.yml --logs x.json --expect match`.
func adHocSpec(opts *Options) (*Spec, error) {
	if len(opts.Paths) != 1 {
		return nil, fmt.Errorf("--logs requires exactly one rule file argument")
	}
	rulePath := opts.Paths[0]

	expect := opts.Expect
	if expect == "" {
		expect = "match"
	}
	exp, err := parseExpectation(expect)
	if err != nil {
		return nil, err
	}

	var logs []map[string]interface{}
	for _, lp := range opts.LogPaths {
		data, err := os.ReadFile(lp)
		if err != nil {
			return nil, err
		}
		parsed, err := ParseLogFile(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", lp, err)
		}
		for i, l := range parsed {
			normalized, err := jsonRoundTrip(l)
			if err != nil {
				return nil, fmt.Errorf("%s: log %d: %w", lp, i+1, err)
			}
			logs = append(logs, normalized)
		}
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("no logs found in %s", strings.Join(opts.LogPaths, ", "))
	}

	c := Case{
		Name:        strings.Join(opts.LogPaths, ", "),
		Expect:      expect,
		expectation: exp,
		logs:        logs,
	}
	return &Spec{
		Rule:       rulePath,
		Normalizer: opts.NormalizerPath,
		Cases:      []Case{c},
		Path:       rulePath,
	}, nil
}

// runLint translates and parses every rule it can find without touching ClickHouse.
// It is the cheap first gate in CI: it proves the rules are well-formed and can run.
func runLint(opts *Options) error {
	files, err := lintTargets(opts.Paths)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no YAML rule files found in the given path(s)")
	}

	norm, err := loadLintNormalizer(opts)
	if err != nil {
		return err
	}

	checked, failed := 0, 0
	report := func(rule *Rule, path string, err error) {
		checked++
		if err != nil {
			fmt.Printf("%s %s\n       %v\n", badMark("FAIL"), path, err)
			failed++
			return
		}
		if opts.Verbose {
			fmt.Printf("%s %s %s\n", okMark("OK  "), rule.Name, dim(rule.BQL))
		} else {
			fmt.Printf("%s %s\n", okMark("OK  "), rule.Name)
		}
	}

	// Lint spec-referenced rules through their spec, so each is translated with the
	// normalizer it is actually deployed with. Linting such a rule standalone would
	// resolve raw Sigma field names (Image rather than image) and wrongly pass.
	covered := make(map[string]bool)
	for _, f := range files {
		if !IsSpecFile(f) {
			continue
		}
		spec, err := LoadSpec(f)
		if err != nil {
			fmt.Printf("%s %s\n       %v\n", badMark("FAIL"), f, err)
			checked++
			failed++
			continue
		}
		covered[filepath.Clean(spec.Rule)] = true

		specNorm := norm
		if spec.Normalizer != "" {
			specNorm, err = LoadNormalizer(spec.Normalizer)
			if err != nil {
				fmt.Printf("%s %s\n       %v\n", badMark("FAIL"), f, err)
				checked++
				failed++
				continue
			}
		}
		rule, err := LoadRule(spec.Rule, specNorm)
		report(rule, spec.Rule, err)
	}

	for _, f := range files {
		if IsSpecFile(f) || covered[filepath.Clean(f)] {
			continue
		}
		rule, err := LoadRule(f, norm)
		if err != nil {
			// Not every YAML in a detection tree is a rule; only report files that
			// look like one but fail to translate.
			if isNotARule(err) {
				continue
			}
			report(nil, f, err)
			continue
		}
		report(rule, f, nil)
	}

	line := fmt.Sprintf("%d/%d rules valid", checked-failed, checked)
	if failed > 0 {
		fmt.Println(badMark(line))
		return ExitFailure{}
	}
	fmt.Println(okMark(line))
	return nil
}

func loadLintNormalizer(opts *Options) (*normalizers.CompiledNormalizer, error) {
	if opts.NormalizerPath == "" {
		return nil, nil
	}
	return LoadNormalizer(opts.NormalizerPath)
}

func isNotARule(err error) bool {
	return strings.Contains(err.Error(), "no queryString")
}

// lintTargets collects candidate YAML files from the given paths.
func lintTargets(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if !info.IsDir() {
			out = append(out, p)
			continue
		}
		err = filepath.Walk(p, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() {
				return nil
			}
			if ext := strings.ToLower(filepath.Ext(path)); ext == ".yml" || ext == ".yaml" {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func parseArgs(args []string) (*Options, error) {
	opts := &Options{
		CHUser:     "default",
		CHPassword: "bifract",
		Format:     "text",
	}
	if env := os.Getenv("BIFRACT_TEST_CLICKHOUSE"); env != "" {
		opts.ClickHouse = env
	}
	if env := os.Getenv("BIFRACT_TEST_CH_USER"); env != "" {
		opts.CHUser = env
	}
	if env := os.Getenv("BIFRACT_TEST_CH_PASSWORD"); env != "" {
		opts.CHPassword = env
	}

	needValue := func(i int, flag string) (string, error) {
		if i+1 >= len(args) {
			return "", fmt.Errorf("%s requires a value", flag)
		}
		return args[i+1], nil
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--normalizer", "-n":
			v, err := needValue(i, "--normalizer")
			if err != nil {
				return nil, err
			}
			opts.NormalizerPath = v
			i++

		case "--logs":
			v, err := needValue(i, "--logs")
			if err != nil {
				return nil, err
			}
			opts.LogPaths = append(opts.LogPaths, v)
			i++

		case "--expect":
			v, err := needValue(i, "--expect")
			if err != nil {
				return nil, err
			}
			opts.Expect = v
			i++

		case "--clickhouse", "-c":
			v, err := needValue(i, "--clickhouse")
			if err != nil {
				return nil, err
			}
			opts.ClickHouse = v
			i++

		case "--ch-user":
			v, err := needValue(i, "--ch-user")
			if err != nil {
				return nil, err
			}
			opts.CHUser = v
			i++

		case "--ch-password":
			v, err := needValue(i, "--ch-password")
			if err != nil {
				return nil, err
			}
			opts.CHPassword = v
			i++

		case "--format", "-f":
			v, err := needValue(i, "--format")
			if err != nil {
				return nil, err
			}
			switch v {
			case "text", "json", "junit":
			default:
				return nil, fmt.Errorf("unknown --format %q (use text, json or junit)", v)
			}
			opts.Format = v
			i++

		case "--explain":
			opts.Explain = true

		case "--lint":
			opts.Lint = true

		case "--verbose", "-v":
			opts.Verbose = true

		case "--help", "-h":
			PrintUsage()
			return nil, nil

		default:
			if strings.HasPrefix(args[i], "-") {
				return nil, fmt.Errorf("unknown flag %q (see --test --help)", args[i])
			}
			opts.Paths = append(opts.Paths, args[i])
		}
	}

	if len(opts.Paths) == 0 {
		opts.Paths = []string{"."}
	}
	if opts.Expect != "" && len(opts.LogPaths) == 0 {
		return nil, fmt.Errorf("--expect only applies with --logs")
	}
	return opts, nil
}

// PrintUsage prints help for the --test mode.
func PrintUsage() {
	t := ingestcli.TitleStyle.Render
	b := ingestcli.BoldStyle.Render

	fmt.Println(t("bifract --test") + " - test detection rules against sample logs")
	fmt.Println()
	fmt.Println(b("USAGE"))
	fmt.Println("  bifract --test [paths...] [flags]")
	fmt.Println()
	fmt.Println(b("MODES"))
	fmt.Println("  bifract --test ./detections/")
	fmt.Println("      Run every " + SpecSuffix + " found under the given paths.")
	fmt.Println()
	fmt.Println("  bifract --test rule.yml --logs events.json --expect match")
	fmt.Println("      Ad hoc: check one rule against one set of logs.")
	fmt.Println()
	fmt.Println("  bifract --test --lint ./rules/")
	fmt.Println("      Translate and parse every rule without running them. No ClickHouse needed.")
	fmt.Println()
	fmt.Println(b("FLAGS"))
	fmt.Println("  -n, --normalizer <file>   Normalizer YAML applied to test logs (as exported from the UI)")
	fmt.Println("  -c, --clickhouse <addr>   ClickHouse to use, host:port. Default: start a throwaway container")
	fmt.Println("      --ch-user <user>      ClickHouse user (default: default)")
	fmt.Println("      --ch-password <pass>  ClickHouse password (default: bifract)")
	fmt.Println("      --logs <file>         Ad hoc log file: JSON array, single object, or NDJSON")
	fmt.Println("      --expect <verdict>    Ad hoc expectation: match or no_match (default: match)")
	fmt.Println("  -f, --format <fmt>        Output format: text, json or junit (default: text)")
	fmt.Println("      --explain             On failure, print the BQL, SQL and normalized fields")
	fmt.Println("      --lint                Validate rules only; no logs are run")
	fmt.Println("  -v, --verbose             Verbose output")
	fmt.Println("  -h, --help                Show this help")
	fmt.Println()
	fmt.Println(b("ENVIRONMENT"))
	fmt.Println("  BIFRACT_TEST_CLICKHOUSE    Same as --clickhouse")
	fmt.Println("  BIFRACT_TEST_CH_USER       Same as --ch-user")
	fmt.Println("  BIFRACT_TEST_CH_PASSWORD   Same as --ch-password")
	fmt.Println()
	fmt.Println(b("EXIT CODES"))
	fmt.Println("  0  all tests passed")
	fmt.Println("  1  one or more tests failed")
	fmt.Println("  2  could not run (bad spec, unreachable ClickHouse, ...)")
}

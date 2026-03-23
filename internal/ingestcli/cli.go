package ingestcli

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// RunIngest parses ingest-specific flags and runs the ingestion pipeline.
// It accepts the raw args slice after "--ingest" has been consumed.
func RunIngest(args []string) error {
	cfg := &Config{
		URL: "http://localhost:8080",
	}

	var showHelp, recursive bool
	var manualWorkers, manualBatch bool

	i := 0
	for i < len(args) {
		switch args[i] {
		case "--token", "-t":
			if i+1 >= len(args) {
				return fmt.Errorf("--token requires a value")
			}
			i++
			cfg.Token = args[i]

		case "--url", "-u":
			if i+1 >= len(args) {
				return fmt.Errorf("--url requires a value")
			}
			i++
			cfg.URL = args[i]

		case "--batch-size", "-b":
			if i+1 >= len(args) {
				return fmt.Errorf("--batch-size requires a value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return fmt.Errorf("--batch-size must be a positive integer")
			}
			cfg.BatchSize = n
			manualBatch = true

		case "--workers", "-w":
			if i+1 >= len(args) {
				return fmt.Errorf("--workers requires a value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return fmt.Errorf("--workers must be a positive integer")
			}
			cfg.Workers = n
			manualWorkers = true

		case "--limit", "-l":
			if i+1 >= len(args) {
				return fmt.Errorf("--limit requires a value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n < 0 {
				return fmt.Errorf("--limit must be a non-negative integer")
			}
			cfg.Limit = n

		case "--insecure", "-k":
			cfg.Insecure = true

		case "--recursive", "-r":
			recursive = true

		case "--help", "-h":
			showHelp = true

		default:
			if len(args[i]) > 0 && args[i][0] == '-' {
				return fmt.Errorf("unknown flag: %s", args[i])
			}
			if recursive && isGlobPattern(args[i]) {
				pattern := args[i]
				filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return nil
					}
					matched, _ := filepath.Match(pattern, filepath.Base(path))
					if matched {
						cfg.Files = append(cfg.Files, path)
					}
					return nil
				})
			} else {
				matches, err := filepath.Glob(args[i])
				if err != nil {
					return fmt.Errorf("invalid glob pattern: %s", args[i])
				}
				if len(matches) == 0 {
					cfg.Files = append(cfg.Files, args[i])
				} else {
					cfg.Files = append(cfg.Files, matches...)
				}
			}
		}
		i++
	}

	if showHelp {
		PrintIngestUsage()
		return nil
	}

	if len(cfg.Files) == 0 {
		PrintIngestUsage()
		return fmt.Errorf("no files specified")
	}

	if cfg.Token == "" {
		PrintIngestUsage()
		return fmt.Errorf("--token is required")
	}

	var validFiles []string
	for _, f := range cfg.Files {
		info, err := os.Stat(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s file not found: %s\n", WarningStyle.Render("!"), f)
			continue
		}
		if info.IsDir() {
			if !recursive {
				fmt.Fprintf(os.Stderr, "%s skipping directory: %s (use -r to recurse)\n", WarningStyle.Render("!"), f)
				continue
			}
			filepath.WalkDir(f, func(path string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				ext := strings.ToLower(filepath.Ext(path))
				if ext == ".json" || ext == ".ndjson" || ext == ".csv" || ext == ".tsv" || ext == ".jsonl" {
					validFiles = append(validFiles, path)
				}
				return nil
			})
			continue
		}
		validFiles = append(validFiles, f)
	}

	if len(validFiles) == 0 {
		return fmt.Errorf("no valid files to ingest")
	}
	cfg.Files = validFiles

	if !manualWorkers {
		cfg.Workers = autoDetectWorkers()
	}
	if !manualBatch {
		cfg.BatchSize = 5000
	}
	cfg.Adaptive = !manualWorkers && !manualBatch

	return RunWithProgress(cfg)
}

func autoDetectWorkers() int {
	cpus := runtime.NumCPU()
	if cpus < 2 {
		return 2
	}
	if cpus > 32 {
		return 32
	}
	return cpus
}

func isGlobPattern(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

// PrintIngestUsage prints help text for the ingest command.
func PrintIngestUsage() {
	title := TitleStyle.Render("bifract --ingest")

	fmt.Fprintf(os.Stderr, `%s - Bulk log ingestion for Bifract

%s
  bifract --ingest <files...> --token <token> [options]

%s
  --token, -t     Bifract ingest token (required)
  --url, -u       Bifract server URL (default: http://localhost:8080)
  --batch-size, -b  Logs per batch (default: auto, 5000)
  --workers, -w   Concurrent workers (default: auto, based on CPU cores)
  --limit, -l     Max logs per file (default: unlimited)
  --recursive, -r Recursively find matching files in subdirectories
  --insecure, -k  Skip TLS certificate verification
  --help, -h      Show this help

  When --workers and --batch-size are omitted, bifract runs in auto
  mode: it detects optimal parameters from system resources and adapts
  concurrency at runtime based on server feedback.

%s
  JSON array, NDJSON, single JSON object, CSV, TSV

%s
  bifract --ingest logs.json --token bifract_ingest_abc123
  bifract --ingest *.json --token bifract_ingest_abc123
  bifract --ingest access.csv --token bifract_ingest_abc123 --workers 8 --batch-size 2000
  bifract --ingest -r "*.json" --token bifract_ingest_abc123
  bifract --ingest logs.json --token bifract_ingest_abc123 --url https://bifract.local --insecure
`,
		title,
		BoldStyle.Render("Usage:"),
		BoldStyle.Render("Options:"),
		BoldStyle.Render("Supported formats:"),
		BoldStyle.Render("Examples:"),
	)
}

package ingestcli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"bifract/internal/clistyle"
	"bifract/pkg/egress"
)

// defaultURL is this host's ingest port, which only serves TLS.
const defaultURL = "https://localhost:8443"

// RunIngest parses ingest-specific flags and runs the ingestion pipeline.
// It accepts the raw args slice after "--ingest" has been consumed.
func RunIngest(args []string) error {
	cfg := &Config{
		URL: defaultURL,
	}

	var showHelp, recursive bool
	var manualWorkers, manualBatch bool
	var exchangeFlag string
	var datasetIDs []string

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

		case "--exchange":
			if i+1 >= len(args) {
				return fmt.Errorf("--exchange requires a value")
			}
			i++
			exchangeFlag = args[i]

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
			if id, ok := strings.CutPrefix(args[i], datasetPrefix); ok {
				datasetIDs = append(datasetIDs, id)
				break
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

	if len(cfg.Files) == 0 && len(datasetIDs) == 0 {
		PrintIngestUsage()
		return fmt.Errorf("no files specified")
	}

	if cfg.Token == "" {
		PrintIngestUsage()
		return fmt.Errorf("--token is required")
	}

	if len(datasetIDs) > 0 {
		paths, counts, err := fetchDatasets(exchangeFlag, datasetIDs)
		if err != nil {
			return err
		}
		cfg.Files = append(cfg.Files, paths...)
		cfg.Counts = counts
	}

	var validFiles []string
	for _, f := range cfg.Files {
		info, err := os.Stat(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s file not found: %s\n", clistyle.WarningStyle.Render("!"), f)
			continue
		}
		if info.IsDir() {
			if !recursive {
				fmt.Fprintf(os.Stderr, "%s skipping directory: %s (use -r to recurse)\n", clistyle.WarningStyle.Render("!"), f)
				continue
			}
			filepath.WalkDir(f, func(path string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				if ingestible(path) {
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

// ingestible reports whether a file found by -r in a directory is one to read.
func ingestible(path string) bool {
	name := strings.ToLower(filepath.Base(path))
	for _, ext := range []string{".json", ".ndjson", ".jsonl", ".csv", ".tsv", ".parquet", ".jsonl.zst", ".ndjson.zst"} {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

// fetchDatasets downloads each named Exchange dataset, or reuses a verified copy,
// and returns the files to ingest with the number of logs in each.
func fetchDatasets(exchangeFlag string, ids []string) ([]string, map[string]int64, error) {
	indexURL, custom, err := exchangeIndexURL(exchangeFlag)
	if err != nil {
		return nil, nil, err
	}
	dir, err := datasetCacheDir()
	if err != nil {
		return nil, nil, fmt.Errorf("dataset cache: %w", err)
	}
	// Only an index the user chose may point inside their network, as on the server.
	client := egress.NewClient(egress.Options{AllowPrivate: custom})
	// Ctrl-C ends a download cleanly, so its partial file is removed.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	var paths []string
	counts := map[string]int64{}
	for _, id := range ids {
		path, n, err := fetchDataset(ctx, client, indexURL, id, dir, os.Stderr)
		if err != nil {
			return nil, nil, fmt.Errorf("dataset %s: %w", id, err)
		}
		paths = append(paths, path)
		counts[path] = n
	}
	return paths, counts, nil
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
	title := clistyle.TitleStyle.Render("bifract --ingest")

	fmt.Fprintf(os.Stderr, `%s - Bulk log ingestion for Bifract

%s
  bifract --ingest <files...> --token <token> [options]
  bifract --ingest dataset:<id> --token <token> [options]

%s
  --token, -t     Bifract ingest token (required)
  --url, -u       Bifract server URL (default: %s)
  --batch-size, -b  Logs per batch (default: auto, 5000)
  --workers, -w   Concurrent workers (default: auto, based on CPU cores)
  --limit, -l     Max logs per file (default: unlimited)
  --recursive, -r Recursively find matching files in subdirectories
  --exchange      Exchange index for dataset:<id> (default: BIFRACT_EXCHANGE_URL,
                  else the Bifract repository)
  --insecure, -k  Skip TLS certificate verification (self-signed installs)
  --help, -h      Show this help

  When --workers and --batch-size are omitted, bifract runs in auto
  mode: it detects optimal parameters from system resources and adapts
  concurrency at runtime based on server feedback.

%s
  JSON array, NDJSON, single JSON object, CSV, TSV, Parquet, zstd NDJSON (.zst)
  Lines written by bifract --export go back in as the logs they were.

%s
  dataset:<id> downloads an Exchange dataset, checks its SHA-256, keeps a copy
  in the user cache directory for reruns, and ingests it. Use a token with no
  normalizer: datasets are already normalized.

%s
  bifract --ingest logs.json --token bifract_ingest_abc123
  bifract --ingest *.json --token bifract_ingest_abc123
  bifract --ingest access.csv --token bifract_ingest_abc123 --workers 8 --batch-size 2000
  bifract --ingest export.parquet --token bifract_ingest_abc123
  bifract --ingest -r "*.json" --token bifract_ingest_abc123
  bifract --ingest logs.json --token bifract_ingest_abc123 --url https://bifract.local --insecure
  bifract --ingest dataset:bifract-labs-001 --token bifract_ingest_abc123
`,
		title,
		clistyle.BoldStyle.Render("Usage:"),
		clistyle.BoldStyle.Render("Options:"),
		defaultURL,
		clistyle.BoldStyle.Render("Supported formats:"),
		clistyle.BoldStyle.Render("Datasets:"),
		clistyle.BoldStyle.Render("Examples:"),
	)
}

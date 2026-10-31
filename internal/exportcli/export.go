// Package exportcli implements `bifract --export`: download every log a query
// matches in a fractal or prism as JSON Lines.
package exportcli

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"bifract/internal/clistyle"
	"bifract/internal/ingestcli"
	"bifract/internal/mcpserver"
	"bifract/pkg/api"
)

type options struct {
	query  string
	start  string
	end    string
	output string
}

// RunExport parses the args after "--export" and runs the download.
func RunExport(args []string) error {
	opts := options{query: "*", start: "1970-01-01T00:00:00Z"}
	for i := 0; i < len(args); i++ {
		flag := args[i]
		value := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("%s requires a value", flag)
			}
			i++
			return args[i], nil
		}
		var err error
		switch flag {
		case "--query", "-q":
			opts.query, err = value()
		case "--start":
			opts.start, err = value()
		case "--end":
			opts.end, err = value()
		case "--output", "-o":
			opts.output, err = value()
		case "--help", "-h":
			printUsage()
			return nil
		default:
			printUsage()
			return fmt.Errorf("unknown argument: %s", flag)
		}
		if err != nil {
			return err
		}
	}
	for _, t := range []struct{ name, v string }{{"--start", opts.start}, {"--end", opts.end}} {
		if t.v == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339, t.v); err != nil {
			return fmt.Errorf("%s must be RFC3339, e.g. 2026-01-02T15:04:05Z", t.name)
		}
	}
	if opts.output == "" && term.IsTerminal(int(os.Stdout.Fd())) {
		printUsage()
		return fmt.Errorf("--output is required when stdout is a terminal")
	}

	cfg, err := mcpserver.LoadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, cfg, opts)
}

func run(ctx context.Context, cfg mcpserver.Config, opts options) error {
	resp, err := mcpserver.NewClient(cfg).Stream(ctx, api.ExportPath, map[string]string{"query": opts.query, "start": opts.start, "end": opts.end})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}

	dst, finish, err := openOutput(opts.output)
	if err != nil {
		return err
	}
	counter := &countingWriter{w: dst}
	stopProgress := showProgress(counter)
	copyErr := copyBody(counter, resp, strings.HasSuffix(opts.output, ".gz"))
	stopProgress()

	rows := resp.Trailer.Get(api.ExportRowsTrailer)
	if copyErr == nil && rows == "" {
		copyErr = errors.New("the server did not confirm completion")
	}
	if copyErr != nil {
		finish(false)
		if ctx.Err() != nil {
			return errors.New("export cancelled")
		}
		written := ingestcli.FormatBytes(counter.n.Load())
		if opts.output != "" {
			return fmt.Errorf("export interrupted after %s, partial file removed: %w", written, copyErr)
		}
		return fmt.Errorf("export interrupted after %s, output is incomplete: %w", written, copyErr)
	}
	if err := finish(true); err != nil {
		return fmt.Errorf("could not save %s: %w", opts.output, err)
	}

	if opts.output != "" {
		fmt.Fprintf(os.Stderr, "%s %s logs to %s (%s)\n",
			clistyle.SuccessStyle.Render("Exported"), clistyle.ValueStyle.Render(rows), opts.output, ingestcli.FormatBytes(counter.n.Load()))
	}
	return nil
}

// copyBody writes the response to w, decompressing unless keepGzip asks for a
// gzip file, in which case an uncompressed response is compressed locally.
func copyBody(w io.Writer, resp *http.Response, keepGzip bool) error {
	gzipped := resp.Header.Get("Content-Encoding") == "gzip"
	switch {
	case gzipped && keepGzip:
		_, err := io.Copy(w, resp.Body)
		return err
	case gzipped:
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, zr); err != nil {
			return err
		}
		return zr.Close()
	case keepGzip:
		zw := gzip.NewWriter(w)
		if _, err := io.Copy(zw, resp.Body); err != nil {
			return err
		}
		return zw.Close()
	default:
		_, err := io.Copy(w, resp.Body)
		return err
	}
}

// openOutput returns the destination and a finish func. A file is written to a
// fresh temp file beside it and renamed into place only when finish(true)
// succeeds; any other outcome removes the temp file.
func openOutput(path string) (io.Writer, func(ok bool) error, error) {
	if path == "" {
		return os.Stdout, func(bool) error { return nil }, nil
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.partial")
	if err != nil {
		return nil, nil, err
	}
	return f, func(ok bool) error {
		err := f.Close()
		if ok && err == nil {
			err = os.Rename(f.Name(), path)
		}
		if !ok || err != nil {
			os.Remove(f.Name())
		}
		return err
	}, nil
}

func responseError(resp *http.Response) error {
	var body io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		if zr, err := gzip.NewReader(resp.Body); err == nil {
			body = zr
		}
	}
	payload, _ := io.ReadAll(io.LimitReader(body, 1<<20))
	var env struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(payload, &env) == nil && env.Error != "" {
		return fmt.Errorf("export failed (%d): %s", resp.StatusCode, env.Error)
	}
	return fmt.Errorf("export failed: %s", resp.Status)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `%s - Export logs as JSON Lines

%s
  bifract --export --output <file> [options]

%s
  --output, -o   File to write; a .gz name keeps it compressed (default: stdout)
  --query, -q    BQL filter (default: *)
  --start        RFC3339 start (default: 1970-01-01T00:00:00Z)
  --end          RFC3339 end (default: now)

%s
  BIFRACT_URL          Bifract URL, e.g. https://bifract.example.com
  BIFRACT_API_KEY      API key with the admin role on the fractal or prism
  BIFRACT_FRACTAL_ID   Fractal to export, for a tenant-admin key (or BIFRACT_PRISM_ID)
  BIFRACT_CA_CERT, BIFRACT_VERIFY_SSL, BIFRACT_CLIENT_CERT  TLS options

%s
  bifract --export -o logs.jsonl.gz
  bifract --export -q 'event_id=4625' --start 2026-10-01T00:00:00Z -o failed-logons.jsonl
`,
		clistyle.TitleStyle.Render("bifract --export"),
		clistyle.BoldStyle.Render("Usage:"),
		clistyle.BoldStyle.Render("Options:"),
		clistyle.BoldStyle.Render("Environment:"),
		clistyle.BoldStyle.Render("Examples:"))
}

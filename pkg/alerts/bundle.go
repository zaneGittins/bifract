package alerts

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// BundleFormat identifies the archive layout, so a future change can be detected
// rather than mis-parsed.
const BundleFormat = "bifract-alert-bundle"

// BundleVersion is the current layout revision.
const BundleVersion = 1

// MaxBundleBytes bounds an uploaded archive, compressed and expanded alike. A
// bundle is definitions and test events, not log data, so this is generous for a
// whole fractal.
const MaxBundleBytes = 64 << 20

// MaxBundleEntries bounds how many files an archive may hold, so a great many
// tiny entries cannot cost as much as one large one.
const MaxBundleEntries = 5000

// BundleManifest describes what an archive holds and where it came from. The
// origin is provenance only: an import always lands in the caller's own scope.
type BundleManifest struct {
	Format     string `json:"format"`
	Version    int    `json:"version"`
	ExportedAt string `json:"exported_at"`
	Origin     string `json:"origin,omitempty"`
	Alerts     int    `json:"alerts"`
	Tests      int    `json:"tests"`
}

// BundleAlertImport is one alert's outcome, so a partial import reports exactly
// what landed and what did not.
type BundleAlertImport struct {
	File    string `json:"file"`
	Name    string `json:"name"`
	Status  string `json:"status"` // imported, skipped, failed
	Tests   int    `json:"tests,omitempty"`
	Message string `json:"message,omitempty"`
}

// BundleImportResult summarises an import.
type BundleImportResult struct {
	Imported int                 `json:"imported"`
	Skipped  int                 `json:"skipped"`
	Failed   int                 `json:"failed"`
	Alerts   []BundleAlertImport `json:"alerts"`
}

// bundleTests is the on-disk shape of one alert's tests. Positions are implied by
// order, so an edited archive does not have to renumber them.
type bundleTests struct {
	Alert string      `json:"alert"`
	Tests []AlertTest `json:"tests"`
}

var bundleSlugUnsafe = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// bundleSlug turns an alert name into a stable file name. Uniqueness is the
// caller's job, since two alerts may differ only in characters this strips.
func bundleSlug(name string) string {
	s := bundleSlugUnsafe.ReplaceAllString(strings.TrimSpace(name), "-")
	s = strings.Trim(s, "-.")
	if s == "" {
		s = "alert"
	}
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

// ExportBundle writes every manual alert in the scope, with its tests, as a zip.
//
// Feed alerts are left out on purpose: they are owned by their feed, and importing
// a copy elsewhere would collide with what that feed syncs on its own.
func (m *Manager) ExportBundle(ctx context.Context, fractalID, prismID, origin string) ([]byte, error) {
	alerts, err := m.ListAlerts(ctx, false, fractalID, prismID)
	if err != nil {
		return nil, err
	}

	names, err := m.bundleActionNames(ctx, alerts)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	used := make(map[string]int, len(alerts))
	manifest := BundleManifest{
		Format:     BundleFormat,
		Version:    BundleVersion,
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
		Origin:     origin,
	}

	for _, alert := range alerts {
		if alert.FeedID != "" {
			continue
		}

		slug := bundleSlug(alert.Name)
		used[slug]++
		if n := used[slug]; n > 1 {
			slug = fmt.Sprintf("%s-%d", slug, n)
		}

		raw, err := yaml.Marshal(alertToYAML(alert, names))
		if err != nil {
			return nil, fmt.Errorf("encoding %q: %w", alert.Name, err)
		}
		if err := writeBundleFile(zw, "alerts/"+slug+".yaml", raw); err != nil {
			return nil, err
		}
		manifest.Alerts++

		tests, err := m.ListTests(ctx, alert.ID)
		if err != nil {
			return nil, err
		}
		if len(tests) == 0 {
			continue
		}
		// Identity travels with the alert, not with the test: ids are re-minted on
		// import, and a position is just the order in this file.
		for i := range tests {
			tests[i].ID = ""
			tests[i].Position = i
		}
		testsRaw, err := json.MarshalIndent(bundleTests{Alert: alert.Name, Tests: tests}, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("encoding tests for %q: %w", alert.Name, err)
		}
		if err := writeBundleFile(zw, "tests/"+slug+".json", testsRaw); err != nil {
			return nil, err
		}
		manifest.Tests += len(tests)
	}

	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := writeBundleFile(zw, "manifest.json", manifestRaw); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("finishing archive: %w", err)
	}
	return buf.Bytes(), nil
}

func writeBundleFile(zw *zip.Writer, name string, content []byte) error {
	w, err := zw.Create(name)
	if err != nil {
		return fmt.Errorf("adding %s: %w", name, err)
	}
	if _, err := w.Write(content); err != nil {
		return fmt.Errorf("writing %s: %w", name, err)
	}
	return nil
}

// alertToYAML is the inverse of ImportFromYAML, so a bundle round-trips. Actions
// travel by name because ids are per-instance.
func alertToYAML(alert *Alert, names map[string]string) YAMLAlert {
	// An unset window is stored as zero, so a plain event alert would otherwise
	// export a schedule and a window it does not have.
	window, cron, queryWindow := alert.WindowDuration, alert.ScheduleCron, alert.QueryWindowSeconds
	if window != nil && *window == 0 {
		window = nil
	}
	if cron != nil && *cron == "" {
		cron = nil
	}
	if queryWindow != nil && *queryWindow == 0 {
		queryWindow = nil
	}
	return YAMLAlert{
		Name:                alert.Name,
		Description:         alert.Description,
		QueryString:         alert.QueryString,
		AlertType:           alert.AlertType,
		Severity:            alert.Severity,
		ActionNames:         actionNamesFor(alert, names),
		Labels:              alert.Labels,
		References:          alert.References,
		Enabled:             alert.Enabled,
		ThrottleTimeSeconds: alert.ThrottleTimeSeconds,
		ThrottleField:       alert.ThrottleField,
		WindowDuration:      window,
		ScheduleCron:        cron,
		QueryWindowSeconds:  queryWindow,
	}
}

// actionIDs is every action an alert runs, by kind, as the list query reports them.
func actionIDs(alert *Alert) map[string][]string {
	webhook := make([]string, 0, len(alert.WebhookActions))
	for _, a := range alert.WebhookActions {
		webhook = append(webhook, a.ID)
	}
	fractal := make([]string, 0, len(alert.FractalActions))
	for _, a := range alert.FractalActions {
		fractal = append(fractal, a.ID)
	}
	return map[string][]string{
		"webhook_actions":    webhook,
		"fractal_actions":    fractal,
		"dictionary_actions": alert.DictionaryActionIDs,
		"email_actions":      alert.EmailActionIDs,
	}
}

// bundleActionNames resolves ids to names for every alert at once, one query per
// kind. Names are read from the tables rather than from the loaded alert because the
// list query fills only ids for some kinds, and a missing name is a broken export.
func (m *Manager) bundleActionNames(ctx context.Context, alerts []*Alert) (map[string]string, error) {
	wanted := map[string]map[string]bool{}
	for _, alert := range alerts {
		for table, ids := range actionIDs(alert) {
			for _, id := range ids {
				if wanted[table] == nil {
					wanted[table] = map[string]bool{}
				}
				wanted[table][id] = true
			}
		}
	}

	names := map[string]string{}
	for _, kind := range alertActionKinds {
		ids := make([]string, 0, len(wanted[kind.table]))
		for id := range wanted[kind.table] {
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			continue
		}
		found, err := m.actionNames(ctx, kind.table, ids)
		if err != nil {
			return nil, err
		}
		for id, name := range found {
			names[id] = name
		}
	}
	return names, nil
}

// actionNamesFor lists this alert's action names. An id with no name left is one
// whose action was deleted, and it is dropped rather than exported as an id.
func actionNamesFor(alert *Alert, names map[string]string) []string {
	var out []string
	for _, ids := range actionIDs(alert) {
		for _, id := range ids {
			if name := names[id]; name != "" {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

// ImportBundle applies an archive to the caller's scope, one alert at a time.
//
// Each alert is independent: a rule whose query no longer parses, or whose actions
// do not exist here, is reported and skipped rather than failing the whole import.
// An alert whose name is already taken is skipped unless overwrite is set, so
// re-importing the same bundle is safe.
func (m *Manager) ImportBundle(ctx context.Context, data []byte, username, fractalID, prismID string, overwrite bool) (*BundleImportResult, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("not a readable archive: %w", err)
	}

	// The wire size is capped by the handler, but compression hides the real cost:
	// bound what the archive claims to expand to before reading any of it.
	var declared uint64
	for _, f := range zr.File {
		declared += f.UncompressedSize64
	}
	if declared > MaxBundleBytes {
		return nil, fmt.Errorf("the archive expands to more than this instance accepts")
	}
	if len(zr.File) > MaxBundleEntries {
		return nil, fmt.Errorf("the archive holds more than %d files", MaxBundleEntries)
	}

	files := make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		// Reject traversal outright: names are ours to trust only after checking.
		clean := path.Clean(f.Name)
		if strings.HasPrefix(clean, "..") || path.IsAbs(clean) {
			return nil, fmt.Errorf("archive contains an unsafe path: %s", f.Name)
		}
		files[clean] = f
	}

	if raw, ok := files["manifest.json"]; ok {
		var manifest BundleManifest
		if b, err := readBundleFile(raw); err == nil {
			if err := json.Unmarshal(b, &manifest); err == nil && manifest.Format != "" {
				if manifest.Format != BundleFormat {
					return nil, fmt.Errorf("this archive is %q, not a Bifract alert bundle", manifest.Format)
				}
				if manifest.Version > BundleVersion {
					return nil, fmt.Errorf("this bundle was written by a newer Bifract (format %d)", manifest.Version)
				}
			}
		}
	}

	names := make([]string, 0, len(files))
	for name := range files {
		if strings.HasPrefix(name, "alerts/") && strings.HasSuffix(name, ".yaml") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("the archive holds no alert definitions")
	}

	result := &BundleImportResult{Alerts: make([]BundleAlertImport, 0, len(names))}
	for _, name := range names {
		entry := BundleAlertImport{File: name}

		raw, err := readBundleFile(files[name])
		if err != nil {
			entry.Status, entry.Message = "failed", err.Error()
			result.Failed++
			result.Alerts = append(result.Alerts, entry)
			continue
		}

		parsed, err := m.resolveYAMLImport(ctx, string(raw), fractalID, prismID, "")
		if err != nil {
			entry.Status, entry.Message = "failed", err.Error()
			result.Failed++
			result.Alerts = append(result.Alerts, entry)
			continue
		}
		entry.Name = parsed.Request.Name

		// An existing name is left alone unless the caller asked to overwrite: a
		// bundle is someone else's copy, and it must not silently replace local work.
		if parsed.ExistingID != "" && !overwrite {
			entry.Status, entry.Message = "skipped", "an alert with this name already exists here"
			result.Skipped++
			result.Alerts = append(result.Alerts, entry)
			continue
		}

		// Tests ride along in the same write, so an alert never lands without the
		// corpus that proves it.
		testFile := "tests/" + strings.TrimSuffix(strings.TrimPrefix(name, "alerts/"), ".yaml") + ".json"
		if f, ok := files[testFile]; ok {
			tests, err := readBundleTests(f)
			if err != nil {
				entry.Status, entry.Message = "failed", err.Error()
				result.Failed++
				result.Alerts = append(result.Alerts, entry)
				continue
			}
			if len(tests) > 0 {
				parsed.Request.Tests = &tests
				entry.Tests = len(tests)
			}
		}

		if parsed.ExistingID != "" {
			_, err = m.UpdateAlert(ctx, parsed.ExistingID, parsed.Request, username)
		} else {
			_, err = m.CreateAlert(ctx, AlertCreateRequest(parsed.Request), username, fractalID, prismID)
		}
		if err != nil {
			entry.Status, entry.Message, entry.Tests = "failed", err.Error(), 0
			result.Failed++
			result.Alerts = append(result.Alerts, entry)
			continue
		}

		entry.Status = "imported"
		result.Imported++
		result.Alerts = append(result.Alerts, entry)
	}
	return result, nil
}

// readBundleTests reads one alert's corpus, re-minting identity: ids belong to the
// instance that stores them, and a position is just the order in the file.
func readBundleTests(f *zip.File) ([]AlertTest, error) {
	raw, err := readBundleFile(f)
	if err != nil {
		return nil, err
	}
	var parsed bundleTests
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("unreadable tests: %w", err)
	}
	for i := range parsed.Tests {
		parsed.Tests[i].ID = ""
		parsed.Tests[i].Position = i
	}
	return parsed.Tests, nil
}

// readBundleFile reads one archive entry under a size cap, so a zip bomb cannot
// exhaust memory on the way in.
func readBundleFile(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(io.LimitReader(rc, MaxBundleBytes))
}

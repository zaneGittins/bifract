package dictionaries

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"bifract/pkg/egress"
	"bifract/pkg/rowfile"
	"bifract/pkg/settings"
)

// keyFormatGlob marks source keys written as globs, converted to expressions for
// a Patterns list.
const keyFormatGlob = "glob"

const (
	defaultRefreshMinutes = 60
	minRefreshMinutes     = 5
	maxRefreshMinutes     = 7 * 24 * 60
	// maxSourceBytes bounds what one fetch may download. The body is held in
	// memory so an unchanged file can be recognised before anything is written.
	maxSourceBytes = 64 << 20
	// maxSourceExpanded bounds what a compressed body may expand to as it is read.
	maxSourceExpanded = 512 << 20
	// maxSourceLine bounds one line of a source, and so what one record can hold
	// in memory while it is parsed.
	maxSourceLine = 16 << 20
	// maxSourceRows bounds what one source may load into every node's memory.
	maxSourceRows = 2_000_000
	// maxPatternSourceRows is far lower: every row of a Patterns list is an
	// expression compiled into every node's REGEXP_TREE on each reload.
	maxPatternSourceRows = 100_000
	// firstRetry is how soon a failed refresh is tried again. Each further
	// failure doubles it, up to the list's own interval.
	firstRetry = 5 * time.Minute
	// manualRefreshCooldown is how soon after a check a manual refresh may fetch
	// again, so a held-down button cannot hammer the source.
	manualRefreshCooldown = time.Minute
	// sourceSyncTimeout bounds one refresh, fetch and load together.
	sourceSyncTimeout = 10 * time.Minute
	// sourceCallerWait is how long a request waits on a load before leaving it
	// to finish in the background. It sits under the router's 60s timeout.
	sourceCallerWait = 45 * time.Second
	// sourceCheckInterval is how often the scheduler looks for a list that is due.
	sourceCheckInterval = time.Minute
	sourceStartupDelay  = time.Minute
)

// sourceLoads bounds how many sources one process holds in memory at once.
var sourceLoads = make(chan struct{}, 2)

var (
	// errSourceBusy reports a refresh already holding the list.
	errSourceBusy = errors.New("a refresh of this list is running; try again when it finishes")
	// errSourceLoading reports a load still running after the caller stopped
	// waiting for it. It goes on in the background.
	errSourceLoading = errors.New("the source is still loading")
	// errSourceCooldown refuses a manual refresh right after the last check.
	errSourceCooldown = errors.New("this list was checked less than a minute ago; try again shortly")
)

// Source says where a list's rows come from and how a file's records become rows.
type Source struct {
	URL string `json:"url"`
	// Format is csv, tsv, json, ndjson or lines; empty detects it from the file.
	Format string `json:"format,omitempty"`
	// KeyField is the file's column holding the key. For a lines file it names
	// the column the values land in.
	KeyField string `json:"key_field"`
	// Columns are the other file columns to keep. Empty keeps every column.
	Columns []string `json:"columns,omitempty"`
	// SplitKey, when set, splits one key cell into a row per value.
	SplitKey string `json:"split_key,omitempty"`
	// KeyFormat is empty to use keys as written, or "glob".
	KeyFormat      string `json:"key_format,omitempty"`
	RefreshMinutes int    `json:"refresh_minutes"`
}

// SourceStatus is the outcome of the last refresh.
type SourceStatus struct {
	CheckedAt *time.Time `json:"checked_at,omitempty"`
	// ChangedAt is when the rows were last replaced.
	ChangedAt *time.Time `json:"changed_at,omitempty"`
	Error     string     `json:"error,omitempty"`
	// Failures counts refreshes that failed in a row; it spaces out the retries.
	Failures int `json:"failures,omitempty"`
	// Skipped counts records that produced no row, with one reason as a sample.
	Skipped    int    `json:"skipped,omitempty"`
	SkipSample string `json:"skip_sample,omitempty"`
	// Hash identifies the file and settings last loaded, so an unchanged file
	// costs a download and nothing else.
	Hash string `json:"hash,omitempty"`
}

// normalize validates a source for a list of the given kind and fills defaults.
func (s *Source) normalize(kind string) error {
	s.URL = strings.TrimSpace(s.URL)
	if err := egress.ValidateURL(s.URL); err != nil {
		return badInput("source URL: %s", err.Error())
	}
	s.Format = strings.ToLower(strings.TrimSpace(s.Format))
	if s.Format == "auto" {
		s.Format = ""
	}
	if s.Format != "" && !rowfile.ValidFormat(s.Format) {
		return badInput("unknown source format %q; expected one of %s", s.Format, strings.Join(rowfile.Formats, ", "))
	}
	s.KeyField = strings.TrimSpace(s.KeyField)
	if s.KeyField == "" {
		return badInput("name the column in the file that holds the key")
	}
	var cols []string
	for _, c := range s.Columns {
		c = strings.TrimSpace(c)
		if c != "" && c != s.KeyField && !slices.Contains(cols, c) {
			cols = append(cols, c)
		}
	}
	if len(cols) >= maxImportColumns {
		return badInput("a dictionary holds at most %d columns", maxImportColumns)
	}
	s.Columns = cols
	switch s.KeyFormat {
	case "":
	case keyFormatGlob:
		if NormalizeKind(kind) != KindPattern {
			return badInput("glob keys become regular expressions, so they need a Patterns list")
		}
	default:
		return badInput("unknown key format %q; expected %q or none", s.KeyFormat, keyFormatGlob)
	}
	if s.RefreshMinutes == 0 {
		s.RefreshMinutes = defaultRefreshMinutes
	}
	if s.RefreshMinutes < minRefreshMinutes || s.RefreshMinutes > maxRefreshMinutes {
		return badInput("refresh interval must be between %d minutes and %d days", minRefreshMinutes, maxRefreshMinutes/(24*60))
	}
	return nil
}

// hash identifies a body as loaded with these settings. The interval is left
// out: changing only how often a list refreshes does not change its rows.
func (s Source) hash(body []byte) string {
	s.RefreshMinutes = 0
	cfg, _ := json.Marshal(s)
	sum := sha256.New()
	sum.Write(cfg)
	sum.Write(body)
	return hex.EncodeToString(sum.Sum(nil))
}

// globToRegexp converts a glob to the expression a Patterns list matches with.
// * matches any run of characters and ? any one, both across path separators,
// and matching ignores case. A glob with no path separator is matched against
// the last segment of a path, so "tool.exe" matches "C:\bin\tool.exe".
func globToRegexp(glob string) (string, error) {
	glob = strings.TrimSpace(glob)
	if strings.Trim(glob, "*?") == "" {
		return "", fmt.Errorf("%q matches everything", glob)
	}
	var b strings.Builder
	b.WriteString("(?i)")
	if strings.ContainsAny(glob, `\/`) {
		b.WriteString("^")
	} else {
		b.WriteString(`(?:^|[\\/])`)
	}
	for _, r := range glob {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return b.String(), nil
}

// sourceFile is a fetched body, checked, and the plan for turning it into rows.
type sourceFile struct {
	body    []byte
	src     *Source
	kind    string
	format  string
	columns []DictionaryColumn
	// field maps each dictionary column to the file column it is read from.
	field   map[string]string
	rows    int
	skipped int
	sample  string
}

// parseSource reads a body once through to check it before anything is
// written: a file with no usable row, too many rows, a missing or reserved
// column fails here and leaves the list as it was. The key column comes first,
// then the kept columns: as listed, else in the file's order, else (JSON, which
// has none) in name order.
func parseSource(body []byte, src *Source, kind string) (*sourceFile, error) {
	f := &sourceFile{body: body, src: src, kind: kind, format: src.Format}
	if f.format == "" {
		head, err := f.peek()
		if err != nil {
			return nil, err
		}
		f.format = rowfile.Sniff(head)
	}
	rows, err := f.open()
	if err != nil {
		return nil, err
	}
	header := rows.Columns()
	if header != nil {
		for _, c := range append([]string{src.KeyField}, src.Columns...) {
			if !slices.Contains(header, c) {
				return nil, badInput("the file has no column %q; it has %s", c, rowfile.Truncate(strings.Join(header, ", ")))
			}
		}
	}

	limit := maxSourceRows
	if NormalizeKind(kind) == KindPattern {
		limit = maxPatternSourceRows
	}
	collect := header == nil && len(src.Columns) == 0
	seen := map[string]bool{}
	err = f.scan(func(record map[string]string, _ string) error {
		if f.rows++; f.rows > limit {
			return badInput("the source holds more than %d rows, the most a %s list may load", limit, NormalizeKind(kind))
		}
		if collect {
			for k := range record {
				seen[k] = true
			}
			if len(seen) > maxImportColumns {
				return badInput("a dictionary holds at most %d columns, and this file needs more", maxImportColumns)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if f.rows == 0 {
		if f.sample != "" {
			return nil, badInput("the source holds no usable rows (%d skipped, e.g. %s)", f.skipped, f.sample)
		}
		return nil, badInput("the source holds no usable rows")
	}

	fileCols := src.Columns
	switch {
	case len(fileCols) > 0:
	case header != nil:
		fileCols = header
	default:
		for k := range seen {
			fileCols = append(fileCols, k)
		}
		sort.Strings(fileCols)
	}

	key := rowfile.ColumnName(src.KeyField)
	f.columns = []DictionaryColumn{{Name: key, Type: "string"}}
	f.field = map[string]string{key: src.KeyField}
	for _, c := range fileCols {
		name := rowfile.ColumnName(c)
		if _, dup := f.field[name]; dup || c == src.KeyField || strings.TrimSpace(c) == "" {
			continue
		}
		if len(f.columns) >= maxImportColumns {
			return nil, badInput("a dictionary holds at most %d columns, and this file needs more", maxImportColumns)
		}
		f.columns = append(f.columns, DictionaryColumn{Name: name, Type: "string"})
		f.field[name] = c
	}
	if err := ValidateColumnsFor(kind, f.columns); err != nil {
		return nil, badInput("%s", err.Error())
	}
	return f, nil
}

func (f *sourceFile) reader() (*bufio.Reader, error) {
	r, err := rowfile.Decompressed(bufio.NewReader(bytes.NewReader(f.body)))
	if err != nil {
		return nil, badInput("%s", err.Error())
	}
	capped := &cappedReader{r: r, max: maxSourceExpanded}
	return bufio.NewReader(rowfile.LimitLines(capped, maxSourceLine)), nil
}

func (f *sourceFile) peek() ([]byte, error) {
	r, err := f.reader()
	if err != nil {
		return nil, err
	}
	head, err := r.Peek(8 << 10)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
		return nil, badInput("%s", err.Error())
	}
	return head, nil
}

func (f *sourceFile) open() (rowfile.Reader, error) {
	r, err := f.reader()
	if err != nil {
		return nil, err
	}
	rows, err := rowfile.NewReader(f.format, r, f.src.KeyField)
	if err != nil {
		return nil, badInput("%s", err.Error())
	}
	return rows, nil
}

// scan reads every record in file order and yields each key it carries. A
// record or key that cannot be a row is counted as skipped rather than failing
// the load: one bad entry upstream should not stop the rest from refreshing.
func (f *sourceFile) scan(yield func(record map[string]string, key string) error) error {
	rows, err := f.open()
	if err != nil {
		return err
	}
	f.skipped, f.sample = 0, ""
	skip := func(reason string) {
		if f.skipped == 0 {
			f.sample = reason
		}
		f.skipped++
	}
	for {
		record, reason, err := rows.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return badInput("%s", err.Error())
		}
		if reason != "" {
			skip(reason)
			continue
		}
		raw := record[f.src.KeyField]
		if strings.TrimSpace(raw) == "" {
			skip(fmt.Sprintf("no value for the key column %q", f.src.KeyField))
			continue
		}
		parts := []string{raw}
		if f.src.SplitKey != "" {
			parts = strings.Split(raw, f.src.SplitKey)
		}
		for _, part := range parts {
			key := strings.TrimSpace(part)
			if key == "" {
				continue
			}
			if f.src.KeyFormat == keyFormatGlob {
				if key, err = globToRegexp(key); err != nil {
					skip(err.Error())
					continue
				}
			}
			if err := ValidateKeyFor(f.kind, key); err != nil {
				skip(err.Error())
				continue
			}
			if err := yield(record, key); err != nil {
				return err
			}
		}
	}
}

// each yields every row the file holds, in file order.
func (f *sourceFile) each(emit func(DictionaryRow) error) error {
	keyCol := f.columns[0].Name
	return f.scan(func(record map[string]string, key string) error {
		fields := make(map[string]string, len(f.columns))
		for _, c := range f.columns[1:] {
			fields[c.Name] = record[f.field[c.Name]]
		}
		fields[keyCol] = key
		return emit(DictionaryRow{Key: key, Fields: fields})
	})
}

// requireEditable refuses a hand edit to a list whose rows come from a source:
// the next refresh would silently undo it.
func requireEditable(dict *Dictionary) error {
	if dict.Source != nil {
		return badInput("%q is filled from a URL, so its rows change only when the source does. Detach the source to edit it by hand", dict.Name)
	}
	return nil
}

// sourceLockID is a per-list advisory lock. Every refresh, source change,
// detach and delete of a list holds it, on whichever replica runs them.
func sourceLockID(id string) int64 {
	h := fnv.New64a()
	h.Write([]byte("bifract-dictionary-source:" + id))
	return int64(h.Sum64())
}

// withSourceLock runs fn holding the list's lock, or fails with errSourceBusy.
func (m *Manager) withSourceLock(ctx context.Context, id string, fn func() error) error {
	unlock, ok := m.pg.TryAdvisoryLock(ctx, sourceLockID(id))
	if !ok {
		return errSourceBusy
	}
	defer unlock()
	return fn()
}

// runDetached runs a load on a context of its own, so it outlives the request
// that started it, and waits for it up to sourceCallerWait. A load still
// running then reports errSourceLoading and finishes in the background.
func runDetached(ctx context.Context, load func(context.Context) error) error {
	done := make(chan error, 1)
	go func() {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sourceSyncTimeout)
		defer cancel()
		done <- load(loadCtx)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(sourceCallerWait):
		return errSourceLoading
	case <-ctx.Done():
		return errSourceLoading
	}
}

// SyncSource refreshes a list from its source now. errSourceLoading means the
// load goes on in the background; the list returned is as it stands.
func (m *Manager) SyncSource(ctx context.Context, id string) (*Dictionary, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}
	if coolingDown(dict, time.Now()) {
		return nil, errSourceCooldown
	}
	err = runDetached(ctx, func(ctx context.Context) error { return m.syncSource(ctx, id, false) })
	if err != nil && !errors.Is(err, errSourceLoading) {
		return nil, err
	}
	dict, getErr := m.GetDictionary(ctx, id)
	if getErr != nil {
		return nil, getErr
	}
	return dict, err
}

// SetSource attaches or replaces a list's source and loads it. A source that
// fails to load is not kept: the list goes back to the source it had, or to
// none, and its next refresh reloads in full whatever this one changed.
func (m *Manager) SetSource(ctx context.Context, id string, src Source) (*Dictionary, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := src.normalize(dict.Kind); err != nil {
		return nil, err
	}
	err = runDetached(ctx, func(ctx context.Context) error {
		return m.withSourceLock(ctx, id, func() error {
			dict, err := m.GetDictionary(ctx, id)
			if err != nil {
				return err
			}
			prevSource, prevStatus := dict.Source, dict.SourceStatus
			raw, _ := json.Marshal(src)
			if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET source = $1 WHERE id = $2`, raw, id); err != nil {
				return err
			}
			dict.Source = &src
			if dict.SourceStatus == nil {
				dict.SourceStatus = &SourceStatus{}
			}
			loadErr := m.refreshLocked(ctx, dict)
			if loadErr == nil {
				return nil
			}
			var oldSource, oldStatus any
			if prevSource != nil {
				prevStatus.Hash = ""
				oldSource, _ = json.Marshal(prevSource)
				oldStatus, _ = json.Marshal(prevStatus)
			}
			if _, err := m.pg.Exec(context.WithoutCancel(ctx), `UPDATE dictionaries SET source = $1, source_status = $2 WHERE id = $3`,
				oldSource, oldStatus, id); err != nil {
				log.Printf("[Dictionaries] %s: could not restore the previous source: %v", id, err)
			}
			return loadErr
		})
	})
	if err != nil && !errors.Is(err, errSourceLoading) {
		return nil, err
	}
	dict, getErr := m.GetDictionary(ctx, id)
	if getErr != nil {
		return nil, getErr
	}
	return dict, err
}

// firstLoad fills a list created with a source, under its lock. A list whose
// first load fails is deleted: it has no columns, so nothing could use it.
func (m *Manager) firstLoad(ctx context.Context, id string) error {
	return runDetached(ctx, func(ctx context.Context) error {
		return m.withSourceLock(ctx, id, func() error {
			dict, err := m.GetDictionary(ctx, id)
			if err != nil {
				return err
			}
			if err := m.refreshLocked(ctx, dict); err != nil {
				if delErr := m.deleteDictionary(context.WithoutCancel(ctx), dict); delErr != nil {
					log.Printf("[Dictionaries] %s: could not remove the list after its first load failed: %v", id, delErr)
				}
				return err
			}
			return nil
		})
	})
}

// DetachSource stops refreshing a list and makes its rows editable again. The
// rows it holds stay.
func (m *Manager) DetachSource(ctx context.Context, id string) (*Dictionary, error) {
	err := m.withSourceLock(ctx, id, func() error {
		_, err := m.pg.Exec(ctx, `UPDATE dictionaries SET source = NULL, source_status = NULL WHERE id = $1`, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return m.GetDictionary(ctx, id)
}

func (m *Manager) syncSource(ctx context.Context, id string, onlyIfDue bool) error {
	return m.withSourceLock(ctx, id, func() error {
		// Read under the lock: another replica may have just refreshed it.
		dict, err := m.GetDictionary(ctx, id)
		if err != nil {
			return err
		}
		if dict.Source == nil {
			return badInput("%q has no source", dict.Name)
		}
		if onlyIfDue && !sourceDue(dict, time.Now()) {
			return nil
		}
		return m.refreshLocked(ctx, dict)
	})
}

// refreshLocked refreshes a list whose lock the caller holds, and records the
// outcome on it.
func (m *Manager) refreshLocked(ctx context.Context, dict *Dictionary) error {
	ctx, cancel := context.WithTimeout(ctx, sourceSyncTimeout)
	defer cancel()

	now := time.Now().UTC()
	status := *dict.SourceStatus
	status.CheckedAt, status.Error = &now, ""
	// Recorded before the fetch, so a refresh that takes the process down waits
	// out its interval rather than running again a minute after the restart.
	if err := m.saveSourceStatus(ctx, dict.ID, status); err != nil {
		return err
	}

	err := m.refreshSource(ctx, dict, &status, now)
	if err != nil {
		status.Error = sourceErrorText(err)
		status.Failures++
		log.Printf("[Dictionaries] source refresh of %s failed: %v", dict.ID, err)
	} else {
		status.Failures = 0
	}
	// Recorded even when the refresh ran out of time, so the outcome is never lost.
	if saveErr := m.saveSourceStatus(context.WithoutCancel(ctx), dict.ID, status); saveErr != nil && err == nil {
		err = saveErr
	}
	return err
}

func (m *Manager) saveSourceStatus(ctx context.Context, id string, status SourceStatus) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, _ := json.Marshal(status)
	_, err := m.pg.Exec(ctx, `UPDATE dictionaries SET source_status = $1 WHERE id = $2`, raw, id)
	return err
}

// sourceErrorText is what a list shows about a failed refresh, to everyone who
// can see it. A caller-fixable failure says what to fix; anything else stays in
// the server log, where its detail (hosts, SQL) belongs.
func sourceErrorText(err error) string {
	if IsInvalidInput(err) {
		return err.Error()
	}
	return "the refresh failed on the server; the server log has the details"
}

// refreshSource fetches the source and, when it changed, replaces the rows.
func (m *Manager) refreshSource(ctx context.Context, dict *Dictionary, status *SourceStatus, now time.Time) error {
	select {
	case sourceLoads <- struct{}{}:
		defer func() { <-sourceLoads }()
	case <-ctx.Done():
		return ctx.Err()
	}

	body, err := m.fetchSource(ctx, dict.Source.URL)
	if err != nil {
		return err
	}
	hash := dict.Source.hash(body)
	// The rows must still be there to skip: a table rebuilt empty, or a load
	// that never finished, has to be refilled even from an unchanged file.
	if hash == status.Hash && m.hasRows(ctx, dict) {
		return nil
	}

	file, err := parseSource(body, dict.Source, dict.Kind)
	if err != nil {
		return err
	}
	if dict, err = m.applySourceSchema(ctx, dict, file.columns); err != nil {
		return fmt.Errorf("failed to update the list's columns: %w", err)
	}
	if err := m.replaceRows(ctx, dict, file); err != nil {
		return err
	}
	status.Hash, status.ChangedAt = hash, &now
	status.Skipped, status.SkipSample = file.skipped, file.sample
	log.Printf("[Dictionaries] %s refreshed from its source: %d rows, %d skipped", dict.ID, file.rows, file.skipped)
	return nil
}

// hasRows reports whether the list's table holds anything. Unmerged copies make
// the count approximate, which is all a test for empty needs.
func (m *Manager) hasRows(ctx context.Context, dict *Dictionary) bool {
	if len(dict.Columns) == 0 {
		return false
	}
	rows, err := m.ch.QuerySchema(ctx, fmt.Sprintf("SELECT count() AS n FROM `%s`", escCH(m.rowTable(dict.ID))))
	if err != nil || len(rows) == 0 {
		return false
	}
	return toUint64(rows[0]["n"]) > 0
}

// fetchSource downloads a source body under the deployment's current egress
// policy, read per fetch so a change reaches every replica at once.
func (m *Manager) fetchSource(ctx context.Context, url string) ([]byte, error) {
	return downloadSource(ctx, url, settings.AllowPrivate(ctx, m.pg, settings.DictionarySourcesAllowPrivateKey))
}

// downloadSource fetches url through the egress guard, refusing anything past
// maxSourceBytes. A failure is described without the detail of where the
// request went.
func downloadSource(ctx context.Context, url string, allowPrivate bool) ([]byte, error) {
	client := egress.NewClient(egress.Options{AllowPrivate: allowPrivate, Timeout: sourceSyncTimeout})
	resp, err := egress.Get(ctx, client, url)
	if err != nil {
		log.Printf("[Dictionaries] fetching %s: %v", url, err)
		return nil, badInput("fetching the source failed: %s", egress.Describe(err))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSourceBytes+1))
	if err != nil {
		log.Printf("[Dictionaries] reading %s: %v", url, err)
		return nil, badInput("reading the source failed: %s", egress.Describe(err))
	}
	if len(body) > maxSourceBytes {
		return nil, badInput("the source is larger than %d MiB", maxSourceBytes>>20)
	}
	return body, nil
}

// replaceRows loads every row above the ordinals the table already holds, then
// deletes what was there. Lookups see the old rows, then old and new together,
// then the new. A load that fails part way leaves both: deleting what it staged
// could take a key whose old copy a merge has already folded into the new one.
// The next refresh, finding no matching hash, loads again and clears both.
func (m *Manager) replaceRows(ctx context.Context, dict *Dictionary, file *sourceFile) error {
	maxSeq, err := m.maxSeq(ctx, dict)
	if err != nil {
		return err
	}
	threshold := maxSeq + 1
	b := newRowBatcher(threshold-1, csvImportBatchSize, func(rows []DictionaryRow, seqs []uint64) error {
		return m.batchInsertRows(ctx, dict, rows, seqs)
	})
	if err := file.each(b.add); err != nil {
		return err
	}
	if err := b.flush(); err != nil {
		return err
	}
	if err := m.deleteRowsWhere(ctx, dict, fmt.Sprintf("`%s` < %d", seqColumn, threshold)); err != nil {
		return fmt.Errorf("new rows loaded but the old ones could not be removed: %w", err)
	}
	m.reloadDictionaries(ctx, dict)
	m.updateRowCount(ctx, dict)
	return nil
}

// applySourceSchema makes a list's columns match what its source loads. A
// change of key rebuilds the list, which empties it until the load lands;
// adding or dropping a column keeps the rows in place.
func (m *Manager) applySourceSchema(ctx context.Context, dict *Dictionary, cols []DictionaryColumn) (*Dictionary, error) {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	key := names[0]

	if len(dict.Columns) == 0 {
		colsJSON, _ := json.Marshal(cols)
		if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET key_column = $1, columns = $2 WHERE id = $3`,
			key, colsJSON, dict.ID); err != nil {
			return nil, err
		}
		dict.KeyColumn, dict.Columns = key, cols
		if err := m.createCHObjects(ctx, dict); err != nil {
			return nil, err
		}
		return m.GetDictionary(ctx, dict.ID)
	}
	if dict.KeyColumn != key {
		var lookupKeys []string
		for _, c := range dict.Columns {
			if c.IsKey && c.Name != key && slices.Contains(names, c.Name) {
				lookupKeys = append(lookupKeys, c.Name)
			}
		}
		if err := m.rebuildDictionarySchema(ctx, dict, key, names); err != nil {
			return nil, err
		}
		// A rebuild starts every column plain. Columns still there keep the lookup
		// key they had, or match(column=...) on them would stop resolving.
		for _, name := range lookupKeys {
			if _, err := m.SetColumnKey(ctx, dict.ID, name); err != nil {
				return nil, err
			}
		}
		return m.GetDictionary(ctx, dict.ID)
	}

	var err error
	for _, name := range names {
		if !slices.ContainsFunc(dict.Columns, func(c DictionaryColumn) bool { return c.Name == name }) {
			if dict, err = m.addColumn(ctx, dict, name); err != nil {
				return nil, err
			}
		}
	}
	for _, c := range slices.Clone(dict.Columns) {
		if c.Name != key && !slices.Contains(names, c.Name) {
			if dict, err = m.removeColumn(ctx, dict, c.Name); err != nil {
				return nil, err
			}
		}
	}
	return dict, nil
}

// sourceDue reports whether a list's refresh interval has passed.
// A list whose last refresh failed is retried sooner than its interval, backing
// off as failures repeat, so one bad fetch does not leave it stale for a week.
func sourceDue(dict *Dictionary, now time.Time) bool {
	if dict.Source == nil {
		return false
	}
	st := dict.SourceStatus
	if st == nil || st.CheckedAt == nil {
		return true
	}
	wait := time.Duration(dict.Source.RefreshMinutes) * time.Minute
	if st.Error != "" && st.Failures > 0 {
		retry := firstRetry << min(st.Failures-1, 16)
		wait = min(wait, retry)
	}
	return !now.Before(st.CheckedAt.Add(wait))
}

// coolingDown reports whether a manual refresh comes too soon after the last check.
func coolingDown(dict *Dictionary, now time.Time) bool {
	st := dict.SourceStatus
	return st != nil && st.CheckedAt != nil && now.Sub(*st.CheckedAt) < manualRefreshCooldown
}

// StartSourceRefresh refreshes every list with a source on its interval until
// ctx ends. Every replica runs it; the per-list lock lets one load each list.
func (m *Manager) StartSourceRefresh(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(sourceStartupDelay):
		}
		ticker := time.NewTicker(sourceCheckInterval)
		defer ticker.Stop()
		for {
			m.refreshDueSources(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// refreshDueSources refreshes the lists that are due. Due-ness is read without
// the lock, so a list that is not due costs no lock, and checked again under it.
func (m *Manager) refreshDueSources(ctx context.Context) {
	rows, err := m.pg.Query(ctx, `SELECT id, source, source_status FROM dictionaries WHERE source IS NOT NULL`)
	if err != nil {
		log.Printf("[Dictionaries] listing sourced lists: %v", err)
		return
	}
	var due []string
	now := time.Now()
	for rows.Next() {
		d := &Dictionary{Source: &Source{}, SourceStatus: &SourceStatus{}}
		var src, status []byte
		if rows.Scan(&d.ID, &src, &status) != nil || json.Unmarshal(src, d.Source) != nil {
			continue
		}
		if len(status) > 0 {
			_ = json.Unmarshal(status, d.SourceStatus)
		}
		if sourceDue(d, now) {
			due = append(due, d.ID)
		}
	}
	rows.Close()
	for _, id := range due {
		if ctx.Err() != nil {
			return
		}
		// A failure is recorded on the list; a busy list is another replica's.
		_ = m.syncSource(ctx, id, true)
	}
}

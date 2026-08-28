package feeds

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"bifract/pkg/alerts"
	"bifract/pkg/normalizers"
	"bifract/pkg/sigma"

	"gopkg.in/yaml.v3"
)

// severityOrder defines the ascending severity hierarchy for level filtering.
var severityOrder = map[string]int{
	"informational": 1,
	"low":           2,
	"medium":        3,
	"high":          4,
	"critical":      5,
}

// statusOrder defines the ascending maturity hierarchy for status filtering.
var statusOrder = map[string]int{
	"unsupported":  1,
	"deprecated":   2,
	"experimental": 3,
	"test":         4,
	"stable":       5,
}

// Rules without a level (e.g. Bifract native rules) always pass.
func meetsMinLevel(ruleLevel, minLevel string) bool {
	if minLevel == "" {
		return true
	}
	if ruleLevel == "" {
		return true
	}
	return severityOrder[strings.ToLower(ruleLevel)] >= severityOrder[strings.ToLower(minLevel)]
}

// meetsMinStatus returns true if the rule's status meets the feed's minimum maturity threshold.
// Rules without a status (e.g. Bifract native rules) always pass.
func meetsMinStatus(ruleStatus, minStatus string) bool {
	if minStatus == "" {
		return true
	}
	if ruleStatus == "" {
		return true
	}
	return statusOrder[strings.ToLower(ruleStatus)] >= statusOrder[strings.ToLower(minStatus)]
}

// feedSyncTimeout bounds a single feed's sync. A full re-translation of a large Sigma
// repo (a TranslatorVersion bump invalidates every rule hash) runs into the thousands of
// rules, far past the 60s HTTP timeout, so manual syncs run detached from the request.
const feedSyncTimeout = 30 * time.Minute

// maxSyncErrors caps the errors retained per sync. Without it a systemic failure records
// one string per rule, which then goes into the feed's status column.
const maxSyncErrors = 50

// Syncer runs background scheduled syncs for all alert feeds.
type Syncer struct {
	manager           *Manager
	alertManager      *alerts.Manager
	normalizerManager *normalizers.Manager

	stopCh chan struct{}
	wg     sync.WaitGroup

	mu       sync.Mutex
	inFlight map[string]bool // feed IDs currently syncing
}

// NewSyncer creates a new feed syncer.
func NewSyncer(manager *Manager, alertManager *alerts.Manager, normalizerManager *normalizers.Manager) *Syncer {
	return &Syncer{
		manager:           manager,
		alertManager:      alertManager,
		normalizerManager: normalizerManager,
		stopCh:            make(chan struct{}),
		inFlight:          make(map[string]bool),
	}
}

// acquire marks a feed as syncing, returning false if a sync is already running for it.
// Manual and scheduled syncs of the same feed would otherwise race: both clone the repo
// and both run the delete pass against their own partial view.
func (s *Syncer) acquire(feedID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight[feedID] {
		return false
	}
	s.inFlight[feedID] = true
	return true
}

func (s *Syncer) release(feedID string) {
	s.mu.Lock()
	delete(s.inFlight, feedID)
	s.mu.Unlock()
}

// shouldRunDeletePass reports whether the retained paths are a trustworthy picture of the
// repo, and if not, why. DeleteFeedAlertsNotIn reads an empty keep-list as "delete every
// alert in this feed", so a sync that did not see the whole repo must not run it: the
// failure mode is silent loss of every alert the feed owns, not a stale alert.
//
// Retaining nothing is only suspicious alongside errors. A clean run that retains nothing
// means every rule fell below the feed's min level or status, and pruning them is exactly
// what was asked for.
func shouldRunDeletePass(incomplete bool, listed, retained, errCount int) (bool, string) {
	if incomplete {
		return false, "sync did not complete"
	}
	if listed > 0 && retained == 0 && errCount > 0 {
		return false, fmt.Sprintf("%d rules listed, none retained, %d errors", listed, errCount)
	}
	return true, ""
}

// syncContext bounds a sync by feedSyncTimeout and cancels it on shutdown, so Stop() does
// not wait out a sync that may still have half an hour to run.
func (s *Syncer) syncContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), feedSyncTimeout)
	done := make(chan struct{})
	go func() {
		select {
		case <-s.stopCh:
			cancel()
		case <-done:
		}
	}()
	return ctx, func() {
		close(done)
		cancel()
	}
}

// StartManualSync runs a sync in the background, detached from the caller's context, and
// records the outcome in the feed's sync status. Returns false if one is already running.
func (s *Syncer) StartManualSync(feed *Feed) bool {
	if !s.acquire(feed.ID) {
		return false
	}
	s.manager.SetSyncStatus(context.Background(), feed.ID, "syncing")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.release(feed.ID)
		ctx, cancel := s.syncContext()
		defer cancel()
		result, err := s.SyncFeed(ctx, feed)
		if err != nil {
			log.Printf("[Feeds] Manual sync failed for %q: %v", feed.Name, err)
			s.manager.UpdateSyncStatus(context.Background(), feed.ID, fmt.Sprintf("error: %v", err), 0)
			return
		}
		log.Printf("[Feeds] Manual sync completed for %q: +%d ~%d -%d =%d (errors: %d)",
			feed.Name, result.Added, result.Updated, result.Deleted, result.Skipped, len(result.Errors))
		s.manager.UpdateSyncStatus(context.Background(), feed.ID, "success", result.Added+result.Updated+result.Skipped)
	}()
	return true
}

// Start launches the background sync ticker (checks every minute).
func (s *Syncer) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		log.Println("[Feeds] Syncer started (checking every 60s)")

		for {
			select {
			case <-ticker.C:
				s.checkAndSync()
			case <-s.stopCh:
				log.Println("[Feeds] Syncer stopped")
				return
			}
		}
	}()
}

// Stop gracefully shuts down the syncer.
func (s *Syncer) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// checkAndSync checks all enabled feeds and syncs those that are due.
func (s *Syncer) checkAndSync() {
	listCtx, listCancel := context.WithTimeout(context.Background(), time.Minute)
	feeds, err := s.manager.ListAllEnabled(listCtx)
	listCancel()
	if err != nil {
		log.Printf("[Feeds] Failed to list enabled feeds: %v", err)
		return
	}

	now := time.Now()
	for _, feed := range feeds {
		if !s.isDue(feed, now) {
			continue
		}
		if !s.acquire(feed.ID) {
			log.Printf("[Feeds] Skipping %q: a sync is already running", feed.Name)
			continue
		}
		log.Printf("[Feeds] Syncing feed %q (schedule: %s)", feed.Name, feed.SyncSchedule)
		// Each feed gets its own budget. A single shared deadline let one large repo
		// consume it and leave every feed behind it unsynced.
		ctx, cancel := s.syncContext()
		result, err := s.SyncFeed(ctx, feed)
		cancel()
		s.release(feed.ID)
		if err != nil {
			log.Printf("[Feeds] Sync failed for %q: %v", feed.Name, err)
			s.manager.UpdateSyncStatus(context.Background(), feed.ID, fmt.Sprintf("error: %v", err), 0)
			continue
		}
		log.Printf("[Feeds] Sync completed for %q: +%d ~%d -%d =%d (errors: %d)",
			feed.Name, result.Added, result.Updated, result.Deleted, result.Skipped, len(result.Errors))
		s.manager.UpdateSyncStatus(context.Background(), feed.ID, "success", result.Added+result.Updated+result.Skipped)
	}
}

// TranslateError marks a rule that parsed cleanly but could not be expressed in
// BQL. Distinguishing it from a parse failure matters: it means the rule exists
// and the translator is the gap, which is a different piece of work.
type TranslateError struct{ err error }

func (e *TranslateError) Error() string { return "translate Sigma rule: " + e.err.Error() }
func (e *TranslateError) Unwrap() error { return e.err }

// isDue returns true if a feed is due for sync based on its schedule and last sync time.
func (s *Syncer) isDue(feed *Feed, now time.Time) bool {
	interval := ScheduleInterval(feed.SyncSchedule)
	if interval == 0 {
		return false // "never" schedule
	}

	if feed.LastSyncedAt == nil {
		return true // never synced
	}

	return now.After(feed.LastSyncedAt.Add(interval))
}

// SyncFeed performs a full sync for a single feed.
func (s *Syncer) SyncFeed(ctx context.Context, feed *Feed) (*SyncResult, error) {
	result := &SyncResult{}

	// Decrypt auth token
	token, err := s.manager.GetDecryptedToken(ctx, feed.ID)
	if err != nil {
		return nil, fmt.Errorf("decrypt token: %w", err)
	}

	// Clone repo
	repoDir, err := CloneRepo(ctx, feed.RepoURL, feed.Branch, token)
	if err != nil {
		return nil, fmt.Errorf("clone: %w", err)
	}
	defer CleanupRepo(repoDir)

	// List YAML files
	yamlFiles, err := ListYAMLFiles(repoDir, feed.Path)
	if err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}

	// Build normalizer field mapper (explicit or default)
	var fieldMapper func(string) string
	normalizerID := feed.NormalizerID
	if normalizerID == "" && s.normalizerManager != nil {
		normalizerID = s.normalizerManager.GetDefaultID(ctx)
	}
	if normalizerID != "" && s.normalizerManager != nil {
		compiled := s.normalizerManager.CompileByID(ctx, normalizerID)
		fieldMapper = sigma.BuildFieldMapper(compiled)
	}

	// Fall back to "admin" if the feed's creator was deleted
	if feed.CreatedBy == "" {
		feed.CreatedBy = "admin"
	}

	// Paths the delete pass must keep. A path is retained whenever this sync could not
	// establish that the rule is gone, so an error never turns into a deletion; only the
	// min-level/min-status filters below deliberately omit a path.
	foundPaths := make([]string, 0, len(yamlFiles))
	errCount := 0
	recordErr := func(format string, args ...interface{}) {
		errCount++
		if len(result.Errors) < maxSyncErrors {
			result.Errors = append(result.Errors, fmt.Sprintf(format, args...))
		}
	}

	// True when the loop did not visit every file, which makes foundPaths an incomplete
	// picture of the repo and the delete pass unsafe to run.
	incomplete := false

	// Every rule the repository offers, imported or not, so the ATT&CK gap list can
	// answer "what exists for this technique that we are not running, and why not".
	catalog := make([]CatalogEntry, 0, len(yamlFiles))
	catalogPaths := make([]string, 0, len(yamlFiles))
	record := func(e CatalogEntry) {
		catalog = append(catalog, e)
		catalogPaths = append(catalogPaths, e.Path)
	}

	for _, filePath := range yamlFiles {
		if ctx.Err() != nil {
			incomplete = true
			recordErr("sync stopped after %d/%d rules: %v", len(foundPaths), len(yamlFiles), ctx.Err())
			break
		}

		content, err := ReadFile(repoDir, filePath)
		if err != nil {
			// Keep the path: a rule we could not read is not a rule we know was removed.
			// Its catalog row is left untouched for the same reason.
			recordErr("%s: read error: %v", filePath, err)
			foundPaths = append(foundPaths, filePath)
			catalogPaths = append(catalogPaths, filePath)
			continue
		}

		// Mix the translator version into the hash so a translator change forces
		// existing feed alerts to re-translate on the next sync (see sigma.TranslatorVersion).
		h := sha256.New()
		h.Write(content)
		h.Write([]byte(sigma.TranslatorVersion))
		hash := fmt.Sprintf("%x", h.Sum(nil))

		// Metadata is read before any translation so a rule Bifract cannot express
		// in BQL still lands in the catalog with its ATT&CK tags intact. It also
		// lets the unchanged-rule path re-check the level/status filters without
		// paying for a translate.
		entry := CatalogEntry{Path: filePath, RuleHash: hash}
		meta, metaErr := parseRuleMetadata(string(content))
		if metaErr != nil {
			entry.SkipReason = SkipParseError
			entry.SkipDetail = metaErr.Error()
			record(entry)
			recordErr("%s: parse error: %v", filePath, metaErr)
			foundPaths = append(foundPaths, filePath)
			continue
		}
		entry.Title, entry.Level, entry.Status, entry.Tags = meta.Title, meta.Level, meta.Status, meta.Tags

		// Rules below the feed's thresholds are catalogued but not imported, and
		// their path is deliberately left out of foundPaths so the delete pass
		// removes any alert a previous, looser threshold created.
		if !meetsMinLevel(meta.Level, feed.MinLevel) {
			entry.SkipReason = SkipMinLevel
			entry.SkipDetail = fmt.Sprintf("level %q is below the feed minimum %q", meta.Level, feed.MinLevel)
			record(entry)
			continue
		}
		if !meetsMinStatus(meta.Status, feed.MinStatus) {
			entry.SkipReason = SkipMinStatus
			entry.SkipDetail = fmt.Sprintf("status %q is below the feed minimum %q", meta.Status, feed.MinStatus)
			record(entry)
			continue
		}

		// Check if this alert already exists
		existing, existErr := s.alertManager.GetFeedAlertByPath(ctx, feed.ID, filePath)

		if existErr == nil && existing != nil && existing.FeedRuleHash == hash {
			entry.Imported = true
			record(entry)
			result.Skipped++
			foundPaths = append(foundPaths, filePath)
			continue
		}

		// Content changed, or the alert does not exist yet. A path that fails to
		// translate is still kept: when the lookup above failed transiently rather
		// than genuinely finding nothing, dropping it would delete a working alert.
		name, description, queryString, alertType, level, _, labels, references, parseErr := s.parseRule(string(content), fieldMapper)
		if parseErr != nil {
			var translateErr *TranslateError
			if errors.As(parseErr, &translateErr) {
				entry.SkipReason = SkipTranslateError
			} else {
				entry.SkipReason = SkipParseError
			}
			entry.SkipDetail = parseErr.Error()
			record(entry)
			recordErr("%s: parse error: %v", filePath, parseErr)
			foundPaths = append(foundPaths, filePath)
			continue
		}

		if existErr == nil && existing != nil {
			err = s.alertManager.UpdateFeedAlert(ctx, existing.ID, name, description, queryString, alertType, alerts.SeverityFromLevel(level), labels, references, hash, feed.CreatedBy, feed.FractalID, feed.PrismID)
			if err != nil {
				recordErr("%s: update error: %v", filePath, err)
				entry.SkipReason = SkipCreateError
				entry.SkipDetail = err.Error()
			} else {
				entry.Imported = true
				result.Updated++
			}
		} else {
			_, err = s.alertManager.CreateFeedAlert(ctx, name, description, queryString, alertType, alerts.SeverityFromLevel(level),
				labels, references, feed.ID, filePath, hash, feed.FractalID, feed.PrismID, feed.CreatedBy)
			if err != nil {
				recordErr("%s: create error: %v", filePath, err)
				entry.SkipReason = SkipCreateError
				entry.SkipDetail = err.Error()
			} else {
				entry.Imported = true
				result.Added++
			}
		}

		record(entry)
		foundPaths = append(foundPaths, filePath)
	}

	if err := s.manager.UpsertCatalog(ctx, feed.ID, catalog); err != nil {
		// A stale catalog degrades the gap list; it must not fail the sync that
		// actually imported the rules.
		log.Printf("[Feeds] Catalog upsert failed for %q: %v", feed.Name, err)
	}

	if ok, reason := shouldRunDeletePass(incomplete, len(yamlFiles), len(foundPaths), errCount); !ok {
		log.Printf("[Feeds] Skipping delete pass for %q: %s", feed.Name, reason)
	} else {
		deleted, delErr := s.alertManager.DeleteFeedAlertsNotIn(ctx, feed.ID, foundPaths)
		if delErr != nil {
			recordErr("delete pass: %v", delErr)
		} else {
			result.Deleted = deleted
		}
		// The catalog keeps filter-skipped paths that foundPaths omits, so it is
		// pruned against its own list.
		if _, delErr := s.manager.DeleteCatalogNotIn(ctx, feed.ID, catalogPaths); delErr != nil {
			log.Printf("[Feeds] Catalog prune failed for %q: %v", feed.Name, delErr)
		}
	}

	if errCount > len(result.Errors) {
		result.Errors = append(result.Errors, fmt.Sprintf("... and %d more errors", errCount-len(result.Errors)))
	}

	// Refresh alert engine cache once after all changes
	s.alertManager.RefreshCache(ctx)

	return result, nil
}

// parseRule detects and parses a YAML file as either a Sigma rule or a Bifract YAML alert.
// Returns the fields needed to create/update an alert, plus the rule's severity level and status.
func (s *Syncer) parseRule(content string, fieldMapper func(string) string) (
	name, description, queryString, alertType, level, status string, labels, references []string, err error) {

	if sigma.IsSigmaRule(content) {
		return s.parseSigmaRule(content, fieldMapper)
	}

	return s.parseBifractRule(content)
}

// parseSigmaRule translates a Sigma YAML rule to BQL.
func (s *Syncer) parseSigmaRule(content string, fieldMapper func(string) string) (
	name, description, queryString, alertType, level, status string, labels, references []string, err error) {

	rule, err := sigma.ParseSigmaRule(content)
	if err != nil {
		return "", "", "", "", "", "", nil, nil, fmt.Errorf("parse Sigma rule: %w", err)
	}

	queryString, err = sigma.Translate(rule, fieldMapper)
	if err != nil {
		return "", "", "", "", "", "", nil, nil, &TranslateError{err: err}
	}

	level = rule.Level
	status = rule.Status

	name = rule.Title
	alertType = "event"
	references = rule.References

	// Build description
	var descParts []string
	if rule.Description != "" {
		descParts = append(descParts, rule.Description)
	}
	if rule.ID != "" {
		descParts = append(descParts, "Sigma ID: "+rule.ID)
	}
	if rule.Author != "" {
		descParts = append(descParts, "Author: "+rule.Author)
	}
	if len(rule.FalsePositives) > 0 {
		descParts = append(descParts, "False positives: "+strings.Join(rule.FalsePositives, ", "))
	}
	description = strings.Join(descParts, "\n")

	labels = sigma.BuildLabels(rule)

	return name, description, queryString, alertType, level, status, labels, references, nil
}

// parseBifractRule parses a Bifract-native YAML alert definition.
func (s *Syncer) parseBifractRule(content string) (
	name, description, queryString, alertType, level, status string, labels, references []string, err error) {

	var yamlAlert struct {
		Name        string   `yaml:"name"`
		Description string   `yaml:"description"`
		QueryString string   `yaml:"queryString"`
		AlertType   string   `yaml:"alertType"`
		Labels      []string `yaml:"labels"`
		References  []string `yaml:"references"`
	}

	if err := yaml.Unmarshal([]byte(content), &yamlAlert); err != nil {
		return "", "", "", "", "", "", nil, nil, fmt.Errorf("invalid YAML: %w", err)
	}

	if strings.TrimSpace(yamlAlert.Name) == "" {
		return "", "", "", "", "", "", nil, nil, fmt.Errorf("missing required field: name")
	}
	if strings.TrimSpace(yamlAlert.QueryString) == "" {
		return "", "", "", "", "", "", nil, nil, fmt.Errorf("missing required field: queryString")
	}

	alertType = yamlAlert.AlertType
	if alertType == "" {
		alertType = "event"
	}

	// Bifract rules don't have level/status fields; return empty (always passes filters)
	return yamlAlert.Name, yamlAlert.Description, yamlAlert.QueryString, alertType, "", "",
		yamlAlert.Labels, yamlAlert.References, nil
}

package feeds

import (
	"context"
	"crypto/sha256"
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

// meetsMinLevel returns true if the rule's level meets the feed's minimum threshold.
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

// Syncer runs background scheduled syncs for all alert feeds.
type Syncer struct {
	manager           *Manager
	alertManager      *alerts.Manager
	normalizerManager *normalizers.Manager

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewSyncer creates a new feed syncer.
func NewSyncer(manager *Manager, alertManager *alerts.Manager, normalizerManager *normalizers.Manager) *Syncer {
	return &Syncer{
		manager:           manager,
		alertManager:      alertManager,
		normalizerManager: normalizerManager,
		stopCh:            make(chan struct{}),
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	feeds, err := s.manager.ListAllEnabled(ctx)
	if err != nil {
		log.Printf("[Feeds] Failed to list enabled feeds: %v", err)
		return
	}

	now := time.Now()
	for _, feed := range feeds {
		if s.isDue(feed, now) {
			log.Printf("[Feeds] Syncing feed %q (schedule: %s)", feed.Name, feed.SyncSchedule)
			result, err := s.SyncFeed(ctx, feed)
			if err != nil {
				log.Printf("[Feeds] Sync failed for %q: %v", feed.Name, err)
				s.manager.UpdateSyncStatus(ctx, feed.ID, fmt.Sprintf("error: %v", err), 0)
			} else {
				log.Printf("[Feeds] Sync completed for %q: +%d ~%d -%d =%d (errors: %d)",
					feed.Name, result.Added, result.Updated, result.Deleted, result.Skipped, len(result.Errors))
				s.manager.UpdateSyncStatus(ctx, feed.ID, "success", result.Added+result.Updated+result.Skipped)
			}
		}
	}
}

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

	// Track which paths we found (for deletion pass)
	foundPaths := make([]string, 0, len(yamlFiles))

	for _, filePath := range yamlFiles {
		content, err := ReadFile(repoDir, filePath)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: read error: %v", filePath, err))
			continue
		}

		hash := fmt.Sprintf("%x", sha256.Sum256(content))

		// Check if this alert already exists
		existing, existErr := s.alertManager.GetFeedAlertByPath(ctx, feed.ID, filePath)

		if existErr == nil && existing != nil {
			// Alert exists - check if content changed
			if existing.FeedRuleHash == hash {
				// Hash unchanged but filters may have changed - check if rule still qualifies
				_, _, _, _, level, status, _, _, levelErr := s.parseRule(string(content), fieldMapper)
				if levelErr == nil && (!meetsMinLevel(level, feed.MinLevel) || !meetsMinStatus(status, feed.MinStatus)) {
					// Rule no longer meets filters, will be cleaned up by delete pass
					continue
				}
				result.Skipped++
				foundPaths = append(foundPaths, filePath)
				continue
			}

			// Content changed - re-parse and update
			name, description, queryString, alertType, level, status, labels, references, parseErr := s.parseRule(string(content), fieldMapper)
			if parseErr != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("%s: parse error: %v", filePath, parseErr))
				foundPaths = append(foundPaths, filePath)
				continue
			}

			// If rule no longer meets filters, let delete pass clean it up
			if !meetsMinLevel(level, feed.MinLevel) || !meetsMinStatus(status, feed.MinStatus) {
				continue
			}

			err = s.alertManager.UpdateFeedAlert(ctx, existing.ID, name, description, queryString, alertType, labels, references, hash, feed.CreatedBy)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("%s: update error: %v", filePath, err))
			} else {
				result.Updated++
			}
			foundPaths = append(foundPaths, filePath)
			continue
		}

		// New alert - parse and create
		name, description, queryString, alertType, level, status, labels, references, parseErr := s.parseRule(string(content), fieldMapper)
		if parseErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: parse error: %v", filePath, parseErr))
			continue
		}

		// Skip rules below the minimum severity level or status threshold
		if !meetsMinLevel(level, feed.MinLevel) || !meetsMinStatus(status, feed.MinStatus) {
			continue
		}

		_, err = s.alertManager.CreateFeedAlert(ctx, name, description, queryString, alertType,
			labels, references, feed.ID, filePath, hash, feed.FractalID, feed.PrismID, feed.CreatedBy)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: create error: %v", filePath, err))
		} else {
			result.Added++
		}
		foundPaths = append(foundPaths, filePath)
	}

	// Delete alerts for rules no longer in the repo
	deleted, err := s.alertManager.DeleteFeedAlertsNotIn(ctx, feed.ID, foundPaths)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("delete pass: %v", err))
	} else {
		result.Deleted = deleted
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

// parseSigmaRule translates a Sigma YAML rule to Quandrix.
func (s *Syncer) parseSigmaRule(content string, fieldMapper func(string) string) (
	name, description, queryString, alertType, level, status string, labels, references []string, err error) {

	rule, err := sigma.ParseSigmaRule(content)
	if err != nil {
		return "", "", "", "", "", "", nil, nil, fmt.Errorf("parse Sigma rule: %w", err)
	}

	queryString, err = sigma.Translate(rule, fieldMapper)
	if err != nil {
		return "", "", "", "", "", "", nil, nil, fmt.Errorf("translate Sigma rule: %w", err)
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

	// Build labels
	if rule.Level != "" {
		labels = append(labels, "sigma:"+rule.Level)
	}
	if rule.Status != "" {
		labels = append(labels, "status:"+rule.Status)
	}
	for _, tag := range rule.Tags {
		labels = append(labels, tag)
	}
	if rule.LogSource.Product != "" {
		labels = append(labels, "product:"+rule.LogSource.Product)
	}
	if rule.LogSource.Category != "" {
		labels = append(labels, "category:"+rule.LogSource.Category)
	}

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

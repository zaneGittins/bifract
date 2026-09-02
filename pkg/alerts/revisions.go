package alerts

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"

	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

// RevisionContent is the versioned part of an alert: its definition, and nothing
// that changes on its own at runtime.
//
// enabled and disabled_reason are excluded on purpose. A bulk enable/disable or an
// engine auto-disable would otherwise write a revision per alert and evict real
// edits from a capped history.
type RevisionContent struct {
	Name                string   `json:"name"`
	Description         string   `json:"description"`
	QueryString         string   `json:"query_string"`
	AlertType           string   `json:"alert_type"`
	Severity            string   `json:"severity"`
	ThrottleTimeSeconds int      `json:"throttle_time_seconds"`
	ThrottleField       string   `json:"throttle_field"`
	Labels              []string `json:"labels"`
	References          []string `json:"references"`
	WindowDuration      *int     `json:"window_duration"`
	ScheduleCron        *string  `json:"schedule_cron"`
	QueryWindowSeconds  *int     `json:"query_window_seconds"`
	WebhookActionIDs    []string `json:"webhook_action_ids"`
	FractalActionIDs    []string `json:"fractal_action_ids"`
	DictionaryActionIDs []string `json:"dictionary_action_ids"`
	EmailActionIDs      []string `json:"email_action_ids"`
}

// ErrRevisionNotFound distinguishes a missing revision from a failed lookup.
var ErrRevisionNotFound = errors.New("revision not found")

// Revision is one stored definition plus who wrote it.
type Revision struct {
	Revision    int              `json:"revision"`
	Summary     string           `json:"summary"`
	Author      string           `json:"author"`
	AuthorLabel string           `json:"author_label"`
	ContentHash string           `json:"content_hash"`
	CreatedAt   time.Time        `json:"created_at"`
	Content     *RevisionContent `json:"content,omitempty"`
	IsHead      bool             `json:"is_head"`
}

// canonicalize makes the content byte-stable for hashing: no nil slices, and action
// ID sets sorted since their order carries no meaning. Labels and references keep the
// order the author gave them, so reordering them is a real change.
func (c *RevisionContent) canonicalize() {
	c.Labels = normalizeList(c.Labels, false)
	c.References = normalizeList(c.References, false)
	c.WebhookActionIDs = normalizeList(c.WebhookActionIDs, true)
	c.FractalActionIDs = normalizeList(c.FractalActionIDs, true)
	c.DictionaryActionIDs = normalizeList(c.DictionaryActionIDs, true)
	c.EmailActionIDs = normalizeList(c.EmailActionIDs, true)
}

func normalizeList(in []string, sorted bool) []string {
	out := in
	if out == nil {
		out = []string{}
	}
	if sorted {
		out = append([]string(nil), out...)
		if len(out) == 0 {
			out = []string{}
		}
		sort.Strings(out)
	}
	return out
}

// Hash is the identity of a definition. Restore, the no-op check on write, and (later)
// approval binding all key off it.
func (c RevisionContent) Hash() (string, error) {
	c.canonicalize()
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// revisionContentFromRequest builds content from an update/create request, using the
// same resolved alertType and severity the alert row is about to be written with.
func revisionContentFromRequest(req AlertUpdateRequest, alertType, severity string) RevisionContent {
	c := RevisionContent{
		Name:                req.Name,
		Description:         req.Description,
		QueryString:         req.QueryString,
		AlertType:           alertType,
		Severity:            severity,
		ThrottleTimeSeconds: req.ThrottleTimeSeconds,
		ThrottleField:       req.ThrottleField,
		Labels:              req.Labels,
		References:          req.References,
		WindowDuration:      req.WindowDuration,
		ScheduleCron:        req.ScheduleCron,
		QueryWindowSeconds:  req.QueryWindowSeconds,
		WebhookActionIDs:    req.WebhookActionIDs,
		FractalActionIDs:    req.FractalActionIDs,
		DictionaryActionIDs: req.DictionaryActionIDs,
		EmailActionIDs:      req.EmailActionIDs,
	}
	c.canonicalize()
	return c
}

// ToUpdateRequest turns a stored revision back into an update request. Restore runs it
// through UpdateAlert so every validation still applies. Enabled is carried by the
// caller from the live alert, since it is not part of a revision.
func (c RevisionContent) ToUpdateRequest(enabled bool) AlertUpdateRequest {
	return AlertUpdateRequest{
		Name:                c.Name,
		Description:         c.Description,
		QueryString:         c.QueryString,
		AlertType:           AlertType(c.AlertType),
		Severity:            Severity(c.Severity),
		Enabled:             enabled,
		ThrottleTimeSeconds: c.ThrottleTimeSeconds,
		ThrottleField:       c.ThrottleField,
		Labels:              c.Labels,
		References:          c.References,
		WindowDuration:      c.WindowDuration,
		ScheduleCron:        c.ScheduleCron,
		QueryWindowSeconds:  c.QueryWindowSeconds,
		WebhookActionIDs:    c.WebhookActionIDs,
		FractalActionIDs:    c.FractalActionIDs,
		DictionaryActionIDs: c.DictionaryActionIDs,
		EmailActionIDs:      c.EmailActionIDs,
	}
}

// loadRevisionContentTx reads an alert's current definition from inside a transaction.
//
// It reads the association tables directly rather than reusing GetAlert, whose action
// aggregates filter on enabled = true: a disabled action would silently vanish from the
// snapshot and then from anything restored out of it.
func loadRevisionContentTx(ctx context.Context, tx storage.Tx, alertID string) (RevisionContent, error) {
	var c RevisionContent
	err := tx.QueryRow(ctx, `
		SELECT name, COALESCE(description, ''), query_string, COALESCE(alert_type, 'event'),
		       COALESCE(severity, 'medium'), COALESCE(throttle_time_seconds, 0),
		       COALESCE(throttle_field, ''), labels, "references",
		       window_duration, schedule_cron, query_window_seconds
		  FROM alerts WHERE id = $1`, alertID,
	).Scan(&c.Name, &c.Description, &c.QueryString, &c.AlertType, &c.Severity,
		&c.ThrottleTimeSeconds, &c.ThrottleField, pq.Array(&c.Labels), pq.Array(&c.References),
		&c.WindowDuration, &c.ScheduleCron, &c.QueryWindowSeconds)
	if err != nil {
		return c, fmt.Errorf("load alert definition: %w", err)
	}

	links := []struct {
		table  string
		column string
		dest   *[]string
	}{
		{"alert_webhook_actions", "webhook_id", &c.WebhookActionIDs},
		{"alert_fractal_actions", "fractal_action_id", &c.FractalActionIDs},
		{"alert_dictionary_actions", "dictionary_action_id", &c.DictionaryActionIDs},
		{"alert_email_actions", "email_action_id", &c.EmailActionIDs},
	}
	for _, l := range links {
		ids, err := scanIDs(ctx, tx, fmt.Sprintf("SELECT %s::text FROM %s WHERE alert_id = $1", l.column, l.table), alertID)
		if err != nil {
			return c, fmt.Errorf("load %s: %w", l.table, err)
		}
		*l.dest = ids
	}

	c.canonicalize()
	return c, nil
}

func scanIDs(ctx context.Context, tx storage.Tx, query, alertID string) ([]string, error) {
	rows, err := tx.Query(ctx, query, alertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// headRevision returns the newest stored revision for an alert, or ok=false if the
// alert has none yet.
func headRevision(ctx context.Context, tx storage.Tx, alertID string) (num int, hash string, content RevisionContent, ok bool, err error) {
	var raw []byte
	err = tx.QueryRow(ctx, `
		SELECT revision, content_hash, content
		  FROM alert_revisions WHERE alert_id = $1
		 ORDER BY revision DESC LIMIT 1`, alertID,
	).Scan(&num, &hash, &raw)
	if err == sql.ErrNoRows {
		return 0, "", RevisionContent{}, false, nil
	}
	if err != nil {
		return 0, "", RevisionContent{}, false, err
	}
	if err := json.Unmarshal(raw, &content); err != nil {
		return 0, "", RevisionContent{}, false, fmt.Errorf("decode revision %d: %w", num, err)
	}
	return num, hash, content, true, nil
}

// writeRevision appends a revision if the definition actually changed, then prunes to
// the retention limit. It must run inside the same transaction as the alert write so
// history can never diverge from the alert row.
//
// Alerts that predate revision history have no rows, so the first edit seeds the
// pre-edit definition as revision 1 before recording the new one. That keeps the
// invariant that the head revision equals the alert's current definition.
func writeRevision(ctx context.Context, tx storage.Tx, alertID string, content RevisionContent, author, authorLabel string, retention int) error {
	content.canonicalize()
	hash, err := content.Hash()
	if err != nil {
		return err
	}

	headNum, headHash, headContent, hasHead, err := headRevision(ctx, tx, alertID)
	if err != nil {
		return err
	}

	if !hasHead {
		// Seed the pre-edit definition. Callers run this before touching the alert
		// row, so what is read here is the state the edit is replacing.
		seed, err := loadRevisionContentTx(ctx, tx, alertID)
		if err != nil {
			return err
		}
		seedHash, err := seed.Hash()
		if err != nil {
			return err
		}
		if err := insertRevision(ctx, tx, alertID, 1, seed, seedHash, "initial", author, authorLabel, retention); err != nil {
			return err
		}
		headNum, headHash, headContent = 1, seedHash, seed
	}

	if headHash == hash {
		return nil // definition unchanged, nothing to record
	}

	return insertRevision(ctx, tx, alertID, headNum+1, content, hash, summarizeChange(headContent, content), author, authorLabel, retention)
}

func insertRevision(ctx context.Context, tx storage.Tx, alertID string, revision int, content RevisionContent, hash, summary, author, authorLabel string, retention int) error {
	raw, err := json.Marshal(content)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO alert_revisions (alert_id, revision, content, content_hash, summary, author, author_label)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		alertID, revision, raw, hash, summary, storage.NullableUser(author), authorLabel,
	); err != nil {
		return fmt.Errorf("insert alert revision: %w", err)
	}

	// Revision numbers are monotonic and never renumbered, so pruning is a range delete.
	if retention > 0 {
		if _, err := tx.Exec(ctx,
			`DELETE FROM alert_revisions WHERE alert_id = $1 AND revision <= $2`,
			alertID, revision-retention,
		); err != nil {
			return fmt.Errorf("prune alert revisions: %w", err)
		}
	}
	return nil
}

// summarizeChange names the fields that differ, for the one-line entry in the history
// list. Full detail comes from the diff view.
func summarizeChange(before, after RevisionContent) string {
	var changed []string
	add := func(label string, differs bool) {
		if differs {
			changed = append(changed, label)
		}
	}

	add("name", before.Name != after.Name)
	add("description", before.Description != after.Description)
	add("query", before.QueryString != after.QueryString)
	add("type", before.AlertType != after.AlertType)
	add("severity", before.Severity != after.Severity)
	add("throttle", before.ThrottleTimeSeconds != after.ThrottleTimeSeconds || before.ThrottleField != after.ThrottleField)
	add("labels", !equalStrings(before.Labels, after.Labels))
	add("references", !equalStrings(before.References, after.References))
	add("window", !equalIntPtr(before.WindowDuration, after.WindowDuration))
	add("schedule", !equalStringPtr(before.ScheduleCron, after.ScheduleCron) || !equalIntPtr(before.QueryWindowSeconds, after.QueryWindowSeconds))
	add("actions", !equalStrings(before.WebhookActionIDs, after.WebhookActionIDs) ||
		!equalStrings(before.FractalActionIDs, after.FractalActionIDs) ||
		!equalStrings(before.DictionaryActionIDs, after.DictionaryActionIDs) ||
		!equalStrings(before.EmailActionIDs, after.EmailActionIDs))

	if len(changed) == 0 {
		return "no definition change"
	}
	return strings.Join(changed, ", ")
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalIntPtr(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// revisionRetention is how many revisions each alert keeps, from the live admin
// setting.
func revisionRetention() int {
	return settings.Get().AlertRevisionRetention
}

// MissingActionRef is an action a revision refers to that is no longer usable in the
// alert's scope, because it was deleted, disabled, or moved.
type MissingActionRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

// RestoreBlockedError reports a restore that would have silently dropped action
// wiring. The caller decides whether to retry without the missing references.
type RestoreBlockedError struct {
	Missing []MissingActionRef
}

func (e *RestoreBlockedError) Error() string {
	parts := make([]string, 0, len(e.Missing))
	for _, m := range e.Missing {
		label := m.Name
		if label == "" {
			label = m.ID
		}
		parts = append(parts, fmt.Sprintf("%s action %s", m.Kind, label))
	}
	return "revision refers to actions that are no longer available: " + strings.Join(parts, ", ")
}

// actionKinds maps a revision's action ID sets to the tables that hold them.
var actionKinds = []struct {
	kind  string
	table string
	get   func(*RevisionContent) *[]string
}{
	{"webhook", "webhook_actions", func(c *RevisionContent) *[]string { return &c.WebhookActionIDs }},
	{"fractal", "fractal_actions", func(c *RevisionContent) *[]string { return &c.FractalActionIDs }},
	{"dictionary", "dictionary_actions", func(c *RevisionContent) *[]string { return &c.DictionaryActionIDs }},
	{"email", "email_actions", func(c *RevisionContent) *[]string { return &c.EmailActionIDs }},
}

// missingActionRefs returns the action references in a revision that no longer resolve
// in the given scope. Restore reports these up front rather than letting the generic
// update validation fail with an unattributed message.
func (m *Manager) missingActionRefs(ctx context.Context, content *RevisionContent, fractalID, prismID string) ([]MissingActionRef, error) {
	var missing []MissingActionRef

	for _, kind := range actionKinds {
		ids := *kind.get(content)
		if len(ids) == 0 {
			continue
		}

		args := []interface{}{pq.Array(ids)}
		scope := "FALSE"
		if prismID != "" {
			args = append(args, prismID)
			scope = "prism_id = $2"
		} else if fractalID != "" {
			args = append(args, fractalID)
			scope = "fractal_id = $2"
		}

		query := fmt.Sprintf("SELECT id::text, name FROM %s WHERE id = ANY($1) AND enabled = true AND %s", kind.table, scope)
		rows, err := m.pg.Query(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("validate %s actions: %w", kind.kind, err)
		}

		present := make(map[string]string, len(ids))
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err != nil {
				rows.Close()
				return nil, err
			}
			present[id] = name
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()

		var unresolved []string
		for _, id := range ids {
			if _, ok := present[id]; !ok {
				unresolved = append(unresolved, id)
			}
		}
		if len(unresolved) == 0 {
			continue
		}

		// An action that was disabled or moved to another scope still has a row, so
		// name it: "webhook action notify-soc" beats a bare UUID in the confirmation.
		names, err := m.actionNames(ctx, kind.table, unresolved)
		if err != nil {
			return nil, err
		}
		for _, id := range unresolved {
			missing = append(missing, MissingActionRef{Kind: kind.kind, ID: id, Name: names[id]})
		}
	}

	return missing, nil
}

// actionNames resolves whatever names still exist for the given action IDs. A deleted
// action has none, which is itself the answer.
func (m *Manager) actionNames(ctx context.Context, table string, ids []string) (map[string]string, error) {
	rows, err := m.pg.Query(ctx, fmt.Sprintf("SELECT id::text, name FROM %s WHERE id = ANY($1)", table), pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("resolve %s names: %w", table, err)
	}
	defer rows.Close()

	names := make(map[string]string, len(ids))
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}
	return names, rows.Err()
}

// ListRevisions returns an alert's stored history, newest first. Content is included:
// a definition is small, the list is capped by retention, and it lets the diff view
// render without a request per revision.
func (m *Manager) ListRevisions(ctx context.Context, alertID string) ([]Revision, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT r.revision, r.summary, COALESCE(r.author, ''), r.author_label, r.content_hash, r.created_at, r.content
		  FROM alert_revisions r
		 WHERE r.alert_id = $1
		 ORDER BY r.revision DESC`, alertID)
	if err != nil {
		return nil, fmt.Errorf("list alert revisions: %w", err)
	}
	defer rows.Close()

	revisions := []Revision{}
	for rows.Next() {
		var rev Revision
		var raw []byte
		if err := rows.Scan(&rev.Revision, &rev.Summary, &rev.Author, &rev.AuthorLabel, &rev.ContentHash, &rev.CreatedAt, &raw); err != nil {
			return nil, err
		}
		var content RevisionContent
		if err := json.Unmarshal(raw, &content); err != nil {
			return nil, fmt.Errorf("decode revision %d: %w", rev.Revision, err)
		}
		rev.Content = &content
		revisions = append(revisions, rev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(revisions) > 0 {
		revisions[0].IsHead = true
	}
	return revisions, nil
}

// GetRevision returns one stored revision.
func (m *Manager) GetRevision(ctx context.Context, alertID string, revision int) (*Revision, error) {
	var rev Revision
	var raw []byte
	err := m.pg.QueryRow(ctx, `
		SELECT revision, summary, COALESCE(author, ''), author_label, content_hash, created_at, content
		  FROM alert_revisions WHERE alert_id = $1 AND revision = $2`, alertID, revision,
	).Scan(&rev.Revision, &rev.Summary, &rev.Author, &rev.AuthorLabel, &rev.ContentHash, &rev.CreatedAt, &raw)
	if err == sql.ErrNoRows {
		return nil, ErrRevisionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load alert revision: %w", err)
	}

	var content RevisionContent
	if err := json.Unmarshal(raw, &content); err != nil {
		return nil, fmt.Errorf("decode revision %d: %w", revision, err)
	}
	rev.Content = &content
	return &rev, nil
}

// RestoreRevision re-applies a stored definition as a new revision at the head. It is
// never a destructive reset: the restore goes through UpdateAlert, so query parsing,
// the window contract, and action scoping are all validated exactly as for a hand edit.
//
// dropMissing re-applies the definition without action references that no longer
// resolve; without it, such a revision is refused rather than silently unwired.
func (m *Manager) RestoreRevision(ctx context.Context, alertID string, revision int, username string, dropMissing bool) (*Alert, error) {
	rev, err := m.GetRevision(ctx, alertID, revision)
	if err != nil {
		return nil, err
	}

	var enabled bool
	var fractalID, prismID string
	if err := m.pg.QueryRow(ctx,
		`SELECT enabled, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alerts WHERE id = $1`,
		alertID,
	).Scan(&enabled, &fractalID, &prismID); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrAlertNotFound
		}
		return nil, fmt.Errorf("load alert state: %w", err)
	}

	missing, err := m.missingActionRefs(ctx, rev.Content, fractalID, prismID)
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		if !dropMissing {
			return nil, &RestoreBlockedError{Missing: missing}
		}
		dropped := make(map[string]bool, len(missing))
		for _, ref := range missing {
			dropped[ref.Kind+":"+ref.ID] = true
		}
		for _, kind := range actionKinds {
			ids := kind.get(rev.Content)
			kept := make([]string, 0, len(*ids))
			for _, id := range *ids {
				if !dropped[kind.kind+":"+id] {
					kept = append(kept, id)
				}
			}
			*ids = kept
		}
	}

	// Enabled is carried from the live alert: it is operational state and not part of
	// a revision, so restoring a definition must not silently re-enable anything.
	return m.UpdateAlert(ctx, alertID, rev.Content.ToUpdateRequest(enabled), username)
}

// feedAuthorLabel marks revisions written by feed sync. The author column still holds
// the account that owns the feed, so accountability survives; the label just keeps the
// history readable when upstream rewrites a rule.
const feedAuthorLabel = "feed sync"

// feedRevisionFields are the parts of a definition a feed owns. Everything else on the
// alert (throttle, window, schedule, action wiring) is set in Bifract and carries over.
type feedRevisionFields struct {
	name        string
	description string
	queryString string
	alertType   string
	severity    string
	labels      []string
	references  []string
}

func (f feedRevisionFields) applyTo(c RevisionContent) RevisionContent {
	c.Name = f.name
	c.Description = f.description
	c.QueryString = f.queryString
	c.AlertType = f.alertType
	c.Severity = f.severity
	c.Labels = f.labels
	c.References = f.references
	c.canonicalize()
	return c
}

// alertRevisionContent projects a loaded alert onto its versioned definition, for
// callers that judge an existing alert rather than an incoming request.
//
// Action IDs come from the resolved action objects, which GetAlert and ListAlerts
// filter to enabled ones. That is the right reading here: a policy asking whether an
// alert has an action means one that would actually run.
func alertRevisionContent(a *Alert) RevisionContent {
	c := RevisionContent{
		Name:                a.Name,
		Description:         a.Description,
		QueryString:         a.QueryString,
		AlertType:           a.AlertType,
		Severity:            a.Severity,
		ThrottleTimeSeconds: a.ThrottleTimeSeconds,
		ThrottleField:       a.ThrottleField,
		Labels:              a.Labels,
		References:          a.References,
		WindowDuration:      a.WindowDuration,
		ScheduleCron:        a.ScheduleCron,
		QueryWindowSeconds:  a.QueryWindowSeconds,
		DictionaryActionIDs: a.DictionaryActionIDs,
		EmailActionIDs:      a.EmailActionIDs,
	}
	for _, w := range a.WebhookActions {
		c.WebhookActionIDs = append(c.WebhookActionIDs, w.ID)
	}
	for _, f := range a.FractalActions {
		c.FractalActionIDs = append(c.FractalActionIDs, f.ID)
	}
	c.canonicalize()
	return c
}

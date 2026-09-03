// Drafts: the editor's work, saved a few seconds after every change.
//
// An alert takes real time to write, and until now all of it lived in the browser
// until Save. A draft is a proposal row with status "draft", private to its author, so
// leaving the editor loses nothing and the review gate only decides what finishing
// means: apply the draft, or open it for review.
const AlertDrafts = {
    SAVE_DELAY_MS: 2500,

    _draft: null,        // the server row, once one exists
    _alertId: null,
    _timer: null,
    _saving: false,
    _dirty: false,
    _suspended: true,    // no autosave until the editor has finished loading
    _lastSavedAt: null,

    // ---- lifecycle ----

    // Called when the editor opens. Looks for an existing draft of this alert and, if
    // there is one, offers to resume it before the user types over it.
    async start(alertId) {
        this.stop();
        this._alertId = alertId || null;
        this._suspended = true;
        this._dirty = false;
        this._lastSavedAt = null;
        this.renderStatus();

        if (!alertId) {
            this._suspended = false;
            return;
        }

        try {
            const res = await fetch(`/api/v1/alerts/${alertId}/draft`, { credentials: 'include' });
            const payload = await res.json().catch(() => ({}));
            const draft = res.ok ? payload.data : null;
            if (draft) this.offerResume(draft);
        } catch (e) {
            // No draft is the common case; a failed lookup is treated the same way.
        }
        this._suspended = false;
    },

    // Opens the editor directly onto a draft, from the drafts list.
    async open(draft) {
        document.getElementById('alertDraftResume')?.remove();
        this._offered = null;
        this._draft = draft;
        this._alertId = draft.alert_id || null;
        this._suspended = true;
        this.applyToEditor(draft);
        this._dirty = false;
        this._lastSavedAt = draft.updated_at ? new Date(draft.updated_at) : new Date();
        this._suspended = false;
        this.renderStatus();
    },

    stop() {
        clearTimeout(this._timer);
        this._timer = null;
        this._draft = null;
        this._alertId = null;
        this._dirty = false;
        this._suspended = true;
        document.getElementById('alertDraftResume')?.remove();
    },

    // ---- autosave ----

    // Called on any edit. Debounced so a typing burst is one save.
    touch() {
        if (this._suspended) return;
        this._dirty = true;
        this.renderStatus();
        clearTimeout(this._timer);
        // An unanswered offer holds autosave: saving now would overwrite the very
        // draft the user has not yet chosen to resume or discard.
        if (this._offered) return;
        this._timer = setTimeout(() => this.save(), this.SAVE_DELAY_MS);
    },

    async save() {
        if (this._suspended || this._saving || !this._dirty) return;

        const subject = window.Alerts?.getPolicySubject?.();
        if (!subject) return;
        // Nothing worth keeping yet: an empty editor is not a draft.
        if (!subject.name && !subject.query_string && !subject.description) return;

        this._saving = true;
        try {
            const res = await fetch('/api/v1/alert-drafts', {
                method: 'PUT',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    id: this._draft?.id || '',
                    alert_id: this._alertId || '',
                    title: subject.name || 'Untitled',
                    content: this.contentFrom(subject),
                    tests: subject.tests || []
                })
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);

            this._draft = payload.data;
            this._dirty = false;
            this._lastSavedAt = new Date();
        } catch (e) {
            // Left dirty: the next edit retries. Nothing is shown for a transient
            // failure; the status reads "unsaved" until it succeeds.
        } finally {
            this._saving = false;
            this.renderStatus();
        }
    },

    contentFrom(s) {
        return {
            name: s.name, description: s.description, query_string: s.query_string,
            alert_type: s.alert_type, severity: s.severity,
            throttle_time_seconds: s.throttle_time_seconds, throttle_field: s.throttle_field,
            labels: s.labels, references: s.references,
            window_duration: s.window_duration, schedule_cron: s.schedule_cron,
            query_window_seconds: s.query_window_seconds,
            webhook_action_ids: s.webhook_action_ids, fractal_action_ids: s.fractal_action_ids,
            dictionary_action_ids: s.dictionary_action_ids, email_action_ids: s.email_action_ids
        };
    },

    // The work has been saved or proposed for real, so the draft is consumed.
    async finished() {
        const id = this._draft?.id;
        this.stop();
        if (!id) return;
        try {
            await fetch(`/api/v1/alert-drafts/${id}`, { method: 'DELETE', credentials: 'include' });
        } catch (e) {
            // A stale draft is harmless; it lists under drafts until deleted.
        }
    },

    // Submits the draft for review. The server re-validates everything a draft was
    // allowed to leave incomplete.
    async submit() {
        await this.save();
        if (!this._draft?.id) return null;
        const res = await fetch(`/api/v1/alert-drafts/${this._draft.id}/submit`, { method: 'POST', credentials: 'include' });
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);
        this.stop();
        return payload.data;
    },

    // ---- resume ----

    // A quiet notice beside the status pill, not a banner over the work.
    offerResume(draft) {
        document.getElementById('alertDraftResume')?.remove();
        const pill = document.getElementById('alertEditorStatus');
        if (!pill) return;

        pill.insertAdjacentHTML('afterend', `
            <span id="alertDraftResume" class="ae-resume">
                <span class="ae-resume-dot"></span>
                <span>Draft from ${Utils.escapeHtml(Utils.timeAgo(draft.updated_at))}</span>
                <button type="button" class="ae-resume-btn" onclick="AlertDrafts.resumeOffered()">Resume</button>
                <button type="button" class="ae-resume-btn ae-resume-discard" onclick="AlertDrafts.discardOffered()">Discard</button>
            </span>
        `);
        this._offered = draft;
    },

    resumeOffered() {
        const draft = this._offered;
        document.getElementById('alertDraftResume')?.remove();
        this._offered = null;
        if (draft) this.open(draft);
    },

    async discardOffered() {
        const draft = this._offered;
        document.getElementById('alertDraftResume')?.remove();
        this._offered = null;
        if (draft?.id) {
            try {
                await fetch(`/api/v1/alert-drafts/${draft.id}`, { method: 'DELETE', credentials: 'include' });
            } catch (e) { /* listed under drafts until deleted */ }
        }
        // Edits made while the offer stood are drafted now, as a fresh row.
        if (this._dirty) this.touch();
    },

    // Fills the editor from a draft's content. Field IDs are the editor's own.
    applyToEditor(draft) {
        const c = draft.content || {};
        const set = (id, v) => { const el = document.getElementById(id); if (el && v !== undefined && v !== null) el.value = v; };
        set('editorAlertName', c.name);
        window.Alerts?.sizeNameInput?.();
        set('editorQueryInput', c.query_string);
        set('editorAlertDescription', c.description);
        set('editorThrottleTime', c.throttle_time_seconds);
        set('editorThrottleField', c.throttle_field);

        if (c.alert_type) {
            const sel = document.getElementById('alertTypeSelect');
            if (sel) { sel.value = c.alert_type; sel.dispatchEvent(new Event('change')); }
            window.Alerts?.setAlertTypeCard?.(c.alert_type);
        }
        if (c.severity && window.Alerts?.setSeverity) Alerts.setSeverity(c.severity);
        if (Array.isArray(c.labels) && window.Alerts?.setLabelsFromArray) Alerts.setLabelsFromArray(c.labels);
        if (Array.isArray(c.references)) {
            const refs = document.getElementById('editorAlertReferences');
            if (refs) refs.value = c.references.join('\n');
            window.Alerts?.loadReferencesFromTextarea?.();
        }

        if (Array.isArray(draft.tests) && window.AlertTests) AlertTests.adopt(draft.tests);

        document.getElementById('editorQueryInput')?.dispatchEvent(new Event('input'));
    },

    // ---- status ----

    renderStatus() {
        const text = document.querySelector('#alertEditorStatus .ae-status-text');
        const pill = document.getElementById('alertEditorStatus');
        if (!text || !pill) return;

        pill.className = 'ae-status';
        if (this._dirty || this._saving) {
            text.textContent = this._saving ? 'Saving draft' : 'Unsaved changes';
            pill.classList.add('ae-status-dirty');
        } else if (this._lastSavedAt) {
            text.textContent = `Draft saved ${Utils.timeAgo(this._lastSavedAt)}`;
            pill.classList.add('ae-status-saved');
        } else if (this._alertId) {
            text.textContent = 'Saved';
        } else {
            text.textContent = 'New';
        }
    }
};

window.AlertDrafts = AlertDrafts;

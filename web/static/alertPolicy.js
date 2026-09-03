// Alert policy checks: assertions about the definition, evaluated as the author types
// and again on the server when the alert is saved.
//
// A rule is field/operator/value, so evaluation needs no query and can run on the same
// debounce as everything else in the editor.
const AlertPolicy = {
    _result: null,
    _running: false,
    _hasPolicies: false,
    _pending: null,
    _timer: null,

    // ---- Editor ----

    async load() {
        this._result = null;
        this._hasPolicies = false;

        try {
            const res = await fetch('/api/v1/alert-policies', { credentials: 'include' });
            if (res.ok) {
                const payload = await res.json();
                this._hasPolicies = (payload.data || []).some(p => p.enabled);
            }
        } catch (e) {
            this._hasPolicies = false;
        }

        this.render();
        this.updateChip();
        this.watch();
        // The saved tests arrive on their own fetch, so a first pass before they land
        // would count zero of them.
        this.schedule();
    },

    reset() {
        clearTimeout(this._timer);
        this._result = null;
        this._hasPolicies = false;
        this.updateChip();
    },

    // A fractal with no rules shows no tab at all, so nothing changes for a team that
    // never turns this on.
    active() {
        return this._hasPolicies;
    },

    // Checks assert about the whole definition, not just the query, so they re-run on
    // any edit that could change a verdict. Evaluating only when the query ran left a
    // description or a test added afterwards showing a stale violation forever.
    schedule() {
        if (!this._hasPolicies) return;
        clearTimeout(this._timer);
        this._timer = setTimeout(() => this.evaluate(), 250);
    },

    // Re-evaluates on any change to the fields a rule can read. Bound once per editor.
    watch() {
        const panel = document.getElementById('alertEditorView');
        if (!panel || panel.dataset.policyWatched) return;
        panel.dataset.policyWatched = '1';

        panel.addEventListener('input', (e) => {
            if (e.target.closest('.alert-panel, #alertQueryHighlight, .query-input-row')) this.schedule();
        });
        panel.addEventListener('change', (e) => {
            if (e.target.closest('.alert-panel')) this.schedule();
        });
    },

    // Evaluated from whatever is on screen, so violations track the edit rather than
    // the last saved version.
    async evaluate() {
        if (!this._hasPolicies) return;

        const form = window.Alerts?.getPolicySubject?.();
        if (!form) return;

        this._running = true;
        this.updateChip();

        try {
            const res = await fetch('/api/v1/alert-policies/evaluate', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(form)
            });
            const payload = await res.json().catch(() => ({}));
            this._result = res.ok ? (payload.data || null) : null;
        } catch (e) {
            this._result = null;
        } finally {
            this._running = false;
            this.updateChip();
            this.render();
            this.paintFields();
        }
    },

    updateChip() {
        const tab = document.querySelector('#alertResultTabs .ert-tab[data-pane="checks"]');
        if (tab) tab.hidden = !this._hasPolicies;

        const chip = document.getElementById('alertChecksChip');
        if (!chip) return;

        if (!this._hasPolicies || this._running || !this._result) {
            chip.hidden = true;
            return;
        }

        const { blocking = 0, warnings = 0, passed = 0, checks = [] } = this._result;
        const total = checks.length;

        chip.hidden = total === 0;
        chip.className = 'ert-chip';
        chip.textContent = `${passed}/${total}`;
        if (blocking > 0) chip.classList.add('fail');
        else if (warnings > 0) chip.classList.add('warn');
        else chip.classList.add('pass');
    },

    // A violation is shown against the field it concerns as well as in the tab, since
    // that is where the author is already looking.
    paintFields() {
        document.querySelectorAll('.ap-field-note').forEach(el => el.remove());
        document.querySelectorAll('.ap-field-bad').forEach(el => el.classList.remove('ap-field-bad'));

        const violations = this._result?.violations || [];
        for (const violation of violations) {
            const input = this.inputFor(violation.field);
            if (!input) continue;

            input.classList.add('ap-field-bad');
            const note = document.createElement('div');
            note.className = `ap-field-note ap-${violation.severity}`;
            note.textContent = violation.message;
            input.insertAdjacentElement('afterend', note);
        }
    },

    // Maps a policy field onto the editor control that owns it. Fields with no control
    // of their own (tests, actions) are reported in the tab only.
    inputFor(field) {
        const ids = {
            name: 'editorAlertName',
            description: 'editorAlertDescription',
            query_string: 'editorQueryInput',
            severity: 'editorAlertSeverity',
            throttle_time_seconds: 'editorThrottleTime',
            throttle_field: 'editorThrottleField'
        };
        return ids[field] ? document.getElementById(ids[field]) : null;
    },

    render() {
        const pane = document.getElementById('alertChecksPane');
        if (!pane) return;

        if (!this._hasPolicies || !this._result) {
            pane.innerHTML = '<div class="ap-empty">Not checked yet.</div>';
            return;
        }

        const { checks = [], blocking = 0, warnings = 0, passed = 0 } = this._result;
        if (checks.length === 0) {
            pane.innerHTML = '<div class="ap-empty">No rules apply here.</div>';
            return;
        }

        // Failures first: the reader is looking for what to fix, and the passing rules
        // are reassurance rather than work.
        const order = { fail: 0, deferred: 1, pass: 2 };
        const sorted = [...checks].sort((a, b) => order[this.stateOf(a)] - order[this.stateOf(b)]);

        pane.innerHTML = `
            <div class="ap-head">${this.renderSummary(blocking, warnings, passed, checks.length)}</div>
            <div class="ap-list">${sorted.map(c => this.renderCheck(c)).join('')}</div>
        `;
    },

    stateOf(check) {
        if (check.deferred) return 'deferred';
        return check.passed ? 'pass' : 'fail';
    },

    renderSummary(blocking, warnings, passed, total) {
        const counts = `<span class="ap-count">${passed}/${total} passing</span>`;
        if (blocking > 0) {
            return `<span class="ap-status ap-status-block">${blocking} blocking</span>${counts}`;
        }
        if (warnings > 0) {
            return `<span class="ap-status ap-status-warn">${warnings} suggested</span>${counts}`;
        }
        return `<span class="ap-status ap-status-pass">All checks pass</span>${counts}`;
    },

    // One row per rule. A passing rule shows what it asserts; a failing one shows what
    // to do about it and what was actually found.
    renderCheck(check) {
        const state = this.stateOf(check);
        const badge = { pass: 'Pass', fail: check.severity === 'block' ? 'Blocking' : 'Suggested', deferred: 'On save' };

        return `
            <div class="ap-check ap-check-${state}">
                <span class="ap-check-mark"></span>
                <div class="ap-check-body">
                    <div class="ap-check-label">${Utils.escapeHtml(check.label || check.field)}</div>
                    ${state === 'pass' ? '' : `
                        <div class="ap-message">${Utils.escapeHtml(check.message || '')}</div>
                        ${check.detail ? `<div class="ap-detail">${Utils.escapeHtml(check.detail)}</div>` : ''}`}
                </div>
                <span class="ap-badge ap-badge-${state}">${badge[state]}</span>
            </div>
        `;
    },

    renderViolation(violation) {
        const label = violation.severity === 'block' ? 'Blocking' : 'Suggested';
        return `
            <div class="ap-violation ap-${violation.severity}">
                <div class="ap-violation-head">
                    <span class="ap-badge">${label}</span>
                    <code class="ap-field">${Utils.escapeHtml(violation.field)}</code>
                </div>
                <div class="ap-message">${Utils.escapeHtml(violation.message)}</div>
                ${violation.detail ? `<div class="ap-detail">${Utils.escapeHtml(violation.detail)}</div>` : ''}
            </div>
        `;
    },

    // Called when a save is refused, so the reasons land in the tab the author will
    // open rather than only in a toast that scrolls away.
    showBlocked(violations) {
        // The server just proved this scope has rules, whatever the earlier probe said.
        // Without this the tab renders "Not checked yet" over the real reasons.
        this._hasPolicies = true;
        this._result = {
            violations: violations || [],
            blocking: (violations || []).length,
            warnings: 0,
            deferred: []
        };
        this.updateChip();
        this.render();
        this.paintFields();
        document.querySelector('#alertResultTabs .ert-tab[data-pane="checks"]')?.click();
    }
};

window.AlertPolicy = AlertPolicy;

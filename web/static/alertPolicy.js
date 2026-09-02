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
    },

    reset() {
        this._result = null;
        this._hasPolicies = false;
        this.updateChip();
    },

    // A fractal with no rules shows no tab at all, so nothing changes for a team that
    // never turns this on.
    active() {
        return this._hasPolicies;
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

        const { blocking = 0, warnings = 0 } = this._result;
        chip.hidden = false;
        chip.className = 'ert-chip';
        if (blocking > 0) {
            chip.textContent = String(blocking);
            chip.classList.add('fail');
        } else if (warnings > 0) {
            chip.textContent = String(warnings);
            chip.classList.add('warn');
        } else {
            chip.textContent = 'ok';
            chip.classList.add('pass');
        }
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

        const { violations = [], blocking = 0, warnings = 0, deferred = [] } = this._result;

        pane.innerHTML = `
            <div class="ap-head">${this.renderSummary(blocking, warnings)}</div>
            ${violations.length === 0 ? '' : `
                <div class="ap-list">
                    ${violations.map(v => this.renderViolation(v)).join('')}
                </div>`}
            ${deferred.length > 0
                ? `<div class="ap-deferred">${deferred.length} check${deferred.length === 1 ? '' : 's'} run on save</div>`
                : ''}
        `;
    },

    renderSummary(blocking, warnings) {
        if (blocking > 0) {
            return `<span class="ap-status ap-status-block">${blocking} blocking</span>`;
        }
        if (warnings > 0) {
            return `<span class="ap-status ap-status-warn">${warnings} suggested</span>`;
        }
        return '<span class="ap-status ap-status-pass">All checks pass</span>';
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

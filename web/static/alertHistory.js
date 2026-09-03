// Alert definition history: revision list, diff against the current definition, and
// rollback. Renders into the History tab of the alert detail drawer, so it stays part
// of the panel already scoped to that alert rather than stacking a second one over it.
const AlertHistory = {
    // Fields rendered in a diff, in the order an author reads them.
    FIELDS: [
        { key: 'name', label: 'Name' },
        { key: 'description', label: 'Description' },
        { key: 'query_string', label: 'Query', multiline: true, code: true },
        { key: 'alert_type', label: 'Type' },
        { key: 'severity', label: 'Severity' },
        { key: 'throttle_time_seconds', label: 'Throttle (seconds)' },
        { key: 'throttle_field', label: 'Throttle field' },
        { key: 'labels', label: 'Labels', list: true },
        { key: 'references', label: 'References', list: true },
        { key: 'window_duration', label: 'Window (seconds)' },
        { key: 'schedule_cron', label: 'Schedule' },
        { key: 'query_window_seconds', label: 'Query window (seconds)' },
        { key: 'webhook_action_ids', label: 'Webhook actions', list: true },
        { key: 'fractal_action_ids', label: 'Fractal actions', list: true },
        { key: 'dictionary_action_ids', label: 'Dictionary actions', list: true },
        { key: 'email_action_ids', label: 'Email actions', list: true }
    ],

    _container: null,
    _alertId: null,
    _revisions: [],
    _expanded: null,

    async renderInto(container, alertId, alertName) {
        this._container = container;
        this._alertId = alertId;
        this._alertName = alertName || '';
        this._revisions = [];
        this._expanded = null;

        container.innerHTML = '<div class="ahd-empty">Loading history...</div>';

        try {
            const res = await fetch(`/api/v1/alerts/${alertId}/revisions`, { credentials: 'include' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const payload = await res.json();
            this._revisions = payload.data || [];
        } catch (e) {
            container.innerHTML = `<div class="ahd-empty">Failed to load history: ${Utils.escapeHtml(e.message)}</div>`;
            return;
        }

        // Another alert's drawer may have opened while this was in flight.
        if (this._alertId !== alertId) return;

        if (this._revisions.length === 0) {
            container.innerHTML = `
                <div class="ahd-empty">
                    <p>No revisions recorded yet.</p>
                    <p class="ahd-empty-hint">This alert predates revision history. Its next edit records the current definition alongside the change.</p>
                </div>`;
            return;
        }

        this.render();
    },

    render() {
        const container = this._container;
        if (!container) return;

        container.innerHTML = `<div class="ahd-list">${this._revisions.map(r => this.renderRow(r)).join('')}</div>`;

        container.querySelectorAll('.ahd-rev-head').forEach(el => {
            el.addEventListener('click', () => {
                const revision = parseInt(el.dataset.revision, 10);
                this._expanded = this._expanded === revision ? null : revision;
                this.render();
            });
        });

        container.querySelectorAll('.ahd-restore').forEach(el => {
            el.addEventListener('click', () => this.restore(parseInt(el.dataset.revision, 10), false));
        });
    },

    // One revision as a row that expands its own diff underneath, which reads better
    // in a narrow panel than a two-pane split.
    renderRow(rev) {
        const open = rev.revision === this._expanded;
        const author = rev.author_label || rev.author || 'unknown';
        return `
            <div class="ahd-rev${open ? ' open' : ''}">
                <button type="button" class="ahd-rev-head" data-revision="${rev.revision}">
                    <svg class="ahd-caret" width="10" height="10" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="6,3 11,8 6,13"/>
                    </svg>
                    <span class="ahd-rev-num">r${rev.revision}</span>
                    <span class="ahd-rev-summary">${Utils.escapeHtml(rev.summary || '')}</span>
                    ${rev.is_head ? '<span class="ahd-rev-badge">Current</span>' : ''}
                    <span class="ahd-rev-time" title="${Utils.escapeHtml(TZ.format(rev.created_at, 'datetime'))}">${Utils.escapeHtml(Utils.timeAgo(rev.created_at))}</span>
                </button>
                <div class="ahd-rev-meta">${Utils.escapeHtml(author)}</div>
                ${open ? `<div class="ahd-rev-body">${this.renderBody(rev)}</div>` : ''}
            </div>
        `;
    },

    renderBody(rev) {
        if (rev.is_head) {
            return `
                <div class="ahd-detail-note">The alert's current definition.</div>
                ${this.renderSnapshot(rev.content)}
            `;
        }

        const changes = this.diff(this._revisions[0].content, rev.content);
        if (changes.length === 0) {
            return '<div class="ahd-detail-note">Identical to the current definition.</div>';
        }

        return `
            <div class="ahd-detail-note">Restoring this would change ${changes.length} field${changes.length === 1 ? '' : 's'}.</div>
            <div class="ahd-changes">${changes.map(c => this.renderChange(c)).join('')}</div>
            <div class="ahd-detail-actions">
                <button type="button" class="btn-secondary ahd-restore" data-revision="${rev.revision}">Restore r${rev.revision}</button>
            </div>
        `;
    },

    // Snapshot of a definition, for the current revision where there is nothing to
    // compare against.
    renderSnapshot(content) {
        const rows = this.FIELDS
            .map(f => ({ f, value: this.format(content?.[f.key], f) }))
            .filter(r => r.value !== '');

        return `<div class="ahd-changes">${rows.map(({ f, value }) => `
            <div class="ahd-change">
                <div class="ahd-change-field">${Utils.escapeHtml(f.label)}</div>
                <div class="ahd-value${f.code ? ' ahd-code' : ''}">${f.multiline ? this.renderLines(value, f) : Utils.escapeHtml(value)}</div>
            </div>
        `).join('')}</div>`;
    },

    renderChange({ field, before, after }) {
        if (field.multiline) {
            return `
                <div class="ahd-change">
                    <div class="ahd-change-field">${Utils.escapeHtml(field.label)}</div>
                    <div class="ahd-linediff${field.code ? ' ahd-code' : ''}">${this.renderLineDiff(before, after, field)}</div>
                </div>
            `;
        }
        return `
            <div class="ahd-change">
                <div class="ahd-change-field">${Utils.escapeHtml(field.label)}</div>
                <div class="ahd-change-values">
                    <div class="ahd-value ahd-before">${Utils.escapeHtml(before || 'not set')}</div>
                    <div class="ahd-value ahd-after">${Utils.escapeHtml(after || 'not set')}</div>
                </div>
            </div>
        `;
    },

    renderLines(text, field) {
        return String(text).split('\n')
            .map(l => `<div class="ahd-line"><span class="ahd-line-text">${this.renderCell(l, field)}</span></div>`)
            .join('');
    },

    // A query reads as a query here, exactly as it does in the alert's own panel.
    // Everything else is plain text and is escaped.
    renderCell(line, field) {
        if (field?.code && window.SyntaxHighlight) return SyntaxHighlight.highlight(line);
        return Utils.escapeHtml(line);
    },

    // Longest common subsequence over lines, so an edit inside a long query shows the
    // changed lines rather than repainting the whole block.
    renderLineDiff(before, after, field) {
        const a = String(before || '').split('\n');
        const b = String(after || '').split('\n');
        const n = a.length, m = b.length;

        const lcs = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
        for (let i = n - 1; i >= 0; i--) {
            for (let j = m - 1; j >= 0; j--) {
                lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
            }
        }

        const out = [];
        let i = 0, j = 0;
        while (i < n && j < m) {
            if (a[i] === b[j]) {
                out.push({ kind: 'same', text: a[i] });
                i++; j++;
            } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
                out.push({ kind: 'del', text: a[i++] });
            } else {
                out.push({ kind: 'add', text: b[j++] });
            }
        }
        while (i < n) out.push({ kind: 'del', text: a[i++] });
        while (j < m) out.push({ kind: 'add', text: b[j++] });

        const marker = { same: ' ', del: '-', add: '+' };
        return out.map(l =>
            `<div class="ahd-line ahd-line-${l.kind}"><span class="ahd-line-marker">${marker[l.kind]}</span>` +
            `<span class="ahd-line-text">${this.renderCell(l.text, field)}</span></div>`
        ).join('');
    },

    // Fields where the revision differs from the current definition.
    diff(current, revision) {
        const changes = [];
        for (const field of this.FIELDS) {
            const before = this.format(current?.[field.key], field);
            const after = this.format(revision?.[field.key], field);
            if (before !== after) changes.push({ field, before, after });
        }
        return changes;
    },

    format(value, field) {
        if (value === null || value === undefined) return '';
        if (field.list) return (value || []).join(', ');
        return String(value);
    },

    async restore(revision, dropMissingActions) {
        // The wording has to cover both outcomes: where the scope reviews changes this
        // opens a proposal rather than restoring, and promising a restore would be a lie.
        if (!dropMissingActions && !confirm(
            `Restore revision ${revision}? This is recorded as a new revision; nothing is overwritten. ` +
            `Where changes are reviewed, it opens a proposal instead.`)) {
            return;
        }

        const alertId = this._alertId;
        try {
            const res = await fetch(`/api/v1/alerts/${alertId}/revisions/${revision}/restore`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ drop_missing_actions: !!dropMissingActions })
            });

            const payload409 = res.status === 409 || res.status === 422
                ? await res.json().catch(() => ({}))
                : null;

            // Restore is an update, so it meets the gate and the policy rules too. Only
            // a 409 carrying an array is the missing-actions case; the others were being
            // fed to promptDropMissing, which then called .map on an object.
            if (payload409) {
                const refusal = window.Alerts?.classifyRefusal(res, payload409);
                if (refusal === 'gate') {
                    this.proposeRestore(revision);
                    return;
                }
                if (refusal === 'policy') {
                    Toast.error('Blocked by policy',
                        (payload409.data || []).map(v => v.message).filter(Boolean).join(' ') || payload409.error || '');
                    return;
                }
                if (res.status === 409 && Array.isArray(payload409.data)) {
                    this.promptDropMissing(revision, payload409.data);
                    return;
                }
                throw new Error(payload409.error || `HTTP ${res.status}`);
            }
            if (!res.ok) {
                const payload = await res.json().catch(() => ({}));
                throw new Error(payload.error || payload.message || `HTTP ${res.status}`);
            }

            Toast.success('Revision restored', `Alert reverted to revision ${revision}.`);
            await this.renderInto(this._container, alertId, this._alertName);
            if (window.Alerts?.loadAlerts) Alerts.loadAlerts();
        } catch (e) {
            Toast.error('Restore failed', e.message);
        }
    },

    // Under review, restoring is a change like any other, so it goes to the queue with
    // a summary that says what it is.
    async proposeRestore(revision) {
        const rev = this._revisions.find(r => r.revision === revision);
        if (!rev?.content) return;

        try {
            const res = await fetch('/api/v1/alert-changes', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    kind: 'update',
                    alert_id: this._alertId,
                    title: `Restore revision ${revision}`,
                    summary: `Restore the definition from revision ${revision}.`,
                    content: rev.content
                })
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);
            Toast.success('Restore proposed', 'It appears under Changes for review.');
        } catch (e) {
            Toast.error('Could not propose the restore', e.message);
        }
    },

    // A revision can point at actions that were since deleted or disabled. Restoring
    // would drop that wiring, so it is the operator's call rather than a silent edit.
    promptDropMissing(revision, missing) {
        const list = missing.map(m => `${m.kind} action ${m.name || m.id}`).join('\n');
        if (confirm(
            `Revision ${revision} refers to actions that are no longer available:\n\n${list}\n\n` +
            `Restore the definition without them?`
        )) {
            this.restore(revision, true);
        }
    }
};

window.AlertHistory = AlertHistory;

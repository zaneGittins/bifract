// Proposed alert changes: the review queue, and the screen a reviewer decides on.
//
// The diff is rendered by AlertHistory, so a proposal and a past revision read
// identically. Nothing here re-implements what the history view already draws.
const AlertChanges = {
    KIND_LABEL: { create: 'New', update: 'Edit', delete: 'Delete' },
    STATUS_LABEL: {
        draft: 'Draft',
        open: 'In review',
        changes_requested: 'Changes requested',
        merged: 'Merged',
        discarded: 'Withdrawn'
    },

    _config: null,
    _list: [],
    _openOnly: true,
    _showDrafts: false,
    _selected: null,
    _detail: null,
    _busy: false,
    _composing: false,

    // ---- Entry ----

    // The sub-tab badge is the only signal that work is waiting for a reviewer, so it
    // is refreshed whenever the alerts area is entered, not just when this tab is open.
    async refreshBadge() {
        const badge = document.getElementById('alertChangesBadge');
        if (!badge) return;

        try {
            const gate = await this.gateConfig();
            if (!gate?.enabled) {
                badge.hidden = true;
                return;
            }
            const open = (await this.api('/api/v1/alert-changes?open=true')) || [];
            badge.hidden = open.length === 0;
            badge.textContent = String(open.length);
        } catch (e) {
            badge.hidden = true;
        }
    },

    async show() {
        const view = document.getElementById('alertChangesView');
        if (!view) return;
        view.innerHTML = '<div class="ac-empty">Loading...</div>';

        try {
            [this._config, this._list] = await Promise.all([this.gateConfig(), this.fetchList()]);
        } catch (e) {
            view.innerHTML = `<div class="ac-empty">Failed to load: ${Utils.escapeHtml(e.message)}</div>`;
            return;
        }
        this.render();
        this.refreshBadge();
    },

    async gateConfig() {
        try {
            return await this.api('/api/v1/alert-gate');
        } catch (e) {
            return { enabled: false, min_approvals: 1, allow_self_approval: true };
        }
    },

    async fetchList() {
        if (this._showDrafts) return (await this.api('/api/v1/alert-drafts')) || [];
        return (await this.api(`/api/v1/alert-changes${this._openOnly ? '?open=true' : ''}`)) || [];
    },

    async api(url, options) {
        const res = await fetch(url, { credentials: 'include', ...options });
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) {
            const err = new Error(payload.error || `HTTP ${res.status}`);
            err.data = payload.data;
            throw err;
        }
        return payload.data;
    },

    // ---- List ----

    render() {
        const view = document.getElementById('alertChangesView');
        if (!view) return;

        view.innerHTML = `
            <section class="ac-view">
                <div class="ac-head">
                    <h2 class="ac-title">Changes${this._list.length ? `<span class="ac-count">${this._list.length}</span>` : ''}</h2>
                    <div class="ac-head-actions">
                        <div class="ac-filter">
                            <button type="button" class="ac-filter-btn${!this._showDrafts && this._openOnly ? ' active' : ''}" onclick="AlertChanges.setFilter('open')">Open</button>
                            <button type="button" class="ac-filter-btn${!this._showDrafts && !this._openOnly ? ' active' : ''}" onclick="AlertChanges.setFilter('all')">All</button>
                            <button type="button" class="ac-filter-btn${this._showDrafts ? ' active' : ''}" onclick="AlertChanges.setFilter('drafts')">My drafts</button>
                        </div>
                        ${this._config?.enabled ? '' : '<span class="ac-off">Review is off for this scope</span>'}
                    </div>
                </div>
                ${this._list.length === 0
                    ? `<div class="ac-empty">${this._showDrafts ? 'No drafts.' : (this._openOnly ? 'Nothing awaiting review.' : 'No proposals.')}</div>`
                    : `<div class="ac-rows">${this._list.map(cr => this.renderRow(cr)).join('')}</div>`}
            </section>
            <div id="alertChangeDrawer" class="ac-drawer"></div>
        `;

        view.querySelectorAll('.ac-row').forEach(el => {
            el.addEventListener('click', () => {
                if (this._showDrafts) {
                    const draft = this._list.find(cr => cr.id === el.dataset.id);
                    if (draft && window.Alerts?.openDraft) Alerts.openDraft(draft);
                    return;
                }
                this.open(el.dataset.id);
            });
        });
        if (this._selected) this.renderDrawer();
    },

    setFilter(which) {
        this._showDrafts = which === 'drafts';
        this._openOnly = which === 'open';
        this.show();
    },

    renderRow(cr) {
        const approvals = this.approvalCount(cr);
        const min = this._config?.min_approvals || 1;
        const target = cr.alert_name || cr.content?.name || 'Untitled alert';

        return `
            <button type="button" class="ac-row${this._selected === cr.id ? ' selected' : ''}" data-id="${Utils.escapeAttr(cr.id)}">
                <span class="ac-kind ac-kind-${cr.kind}">${this.KIND_LABEL[cr.kind] || cr.kind}</span>
                <span class="ac-row-main">
                    <span class="ac-row-target">${Utils.escapeHtml(target)}</span>
                    <span class="ac-row-title">${Utils.escapeHtml(cr.title || cr.summary || '')}</span>
                </span>
                ${this.renderApprovalPips(approvals, min)}
                <span class="ac-status ac-status-${cr.status}">${this.STATUS_LABEL[cr.status] || cr.status}</span>
                <span class="ac-row-author">${Utils.escapeHtml(cr.author_label || cr.author || 'unknown')}</span>
                <span class="ac-row-age">${Utils.escapeHtml(Utils.timeAgo(cr.updated_at))}</span>
            </button>
        `;
    },

    // Mirrors the server: one vote per reviewer, latest decision wins. Counting rows
    // instead would let a double click read as two approvals.
    approvalCount(cr) {
        const latest = new Map();
        for (const r of cr.reviews || []) {
            if (r.stale) continue;
            latest.set(r.reviewer || `review:${r.id}`, r.decision);
        }
        return [...latest.values()].filter(d => d === 'approve').length;
    },

    // Approvals read faster as filled pips than as a fraction.
    renderApprovalPips(count, min) {
        const pips = [];
        for (let i = 0; i < min; i++) {
            pips.push(`<span class="ac-pip${i < count ? ' filled' : ''}"></span>`);
        }
        return `<span class="ac-pips" title="${count} of ${min} approvals">${pips.join('')}</span>`;
    },

    // ---- Drawer ----

    async open(id, runTests = false) {
        this._selected = id;
        this._detail = null;
        this._composing = false;
        this.render();

        try {
            this._detail = await this.api(`/api/v1/alert-changes/${id}${runTests ? '?run_tests=true' : ''}`);
        } catch (e) {
            Toast.error('Could not load proposal', e.message);
            this._selected = null;
            this.render();
            return;
        }
        this.renderDrawer();
    },

    runTests() {
        if (this._selected) this.open(this._selected, true);
    },

    close() {
        this._selected = null;
        this._detail = null;
        this.render();
    },

    renderDrawer() {
        const drawer = document.getElementById('alertChangeDrawer');
        if (!drawer) return;

        drawer.classList.add('open');
        if (!this._detail) {
            drawer.innerHTML = '<div class="ac-empty">Loading proposal...</div>';
            return;
        }

        const cr = this._detail;
        const target = cr.alert_name || cr.content?.name || 'Untitled alert';

        drawer.innerHTML = `
            <div class="ac-drawer-head">
                <div class="ac-drawer-title">
                    <span class="ac-kind ac-kind-${cr.kind}">${this.KIND_LABEL[cr.kind] || cr.kind}</span>
                    <span class="ac-drawer-target">${Utils.escapeHtml(target)}</span>
                </div>
                <button class="ac-drawer-close" onclick="AlertChanges.close()" aria-label="Close">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                </button>
            </div>

            <div class="ac-drawer-body">
                ${cr.title ? `<div class="ac-proposal-title">${Utils.escapeHtml(cr.title)}</div>` : ''}
                ${cr.summary ? `<div class="ac-proposal-summary">${Utils.escapeHtml(cr.summary)}</div>` : ''}
                <div class="ac-proposal-meta">
                    ${Utils.escapeHtml(cr.author_label || cr.author || 'unknown')} &middot;
                    ${Utils.escapeHtml(Utils.timeAgo(cr.updated_at))} &middot;
                    <span class="ac-status ac-status-${cr.status}">${this.STATUS_LABEL[cr.status] || cr.status}</span>
                </div>

                ${this.renderReadiness(cr)}
                ${this.renderDiff(cr)}
                ${this.renderTests(cr)}
                ${this.renderPolicy(cr)}
                ${this.renderReviews(cr)}
            </div>

            <div class="ac-drawer-actions">${this.renderActions(cr)}</div>
        `;
    },

    // Three facts a reviewer needs before reading anything: approvals, checks, tests.
    renderReadiness(cr) {
        const rd = cr.readiness || {};
        const card = (label, value, state) =>
            `<div class="ac-stat ac-stat-${state}"><span class="ac-stat-value">${value}</span><span class="ac-stat-label">${label}</span></div>`;

        const approvals = rd.approvals || 0;
        const min = rd.min_approvals || 1;
        const cards = [card('Approvals', `${approvals}/${min}`, approvals >= min ? 'pass' : 'pending')];

        // Checks and tests always appear, so a missing card cannot be read as "nothing
        // to check here".
        const policy = rd.policy;
        if (policy && (policy.checks || []).length) {
            const blocking = policy.blocking || 0;
            cards.push(card('Checks', `${policy.passed || 0}/${policy.checks.length}`,
                blocking ? 'fail' : ((policy.warnings || 0) ? 'warn' : 'pass')));
        } else {
            cards.push(card('Checks', 'none', 'pending'));
        }

        if (rd.tests) {
            const failed = rd.tests.failed || 0;
            const total = (rd.tests.passed || 0) + failed;
            cards.push(card('Tests', total ? `${rd.tests.passed || 0}/${total}` : 'none', failed ? 'fail' : (total ? 'pass' : 'pending')));
        } else {
            cards.push(card('Tests', 'not run', 'pending'));
        }

        return `
            <div class="ac-stats">${cards.join('')}</div>
            ${rd.blocker ? `<div class="ac-blocker">${Utils.escapeHtml(rd.blocker)}</div>` : ''}
            ${cr.tests?.length ? `<button type="button" class="btn-secondary btn-sm ac-run-tests" onclick="AlertChanges.runTests()">${rd.tests ? 'Run tests again' : 'Run tests'}</button>` : ''}
        `;
    },

    renderDiff(cr) {
        if (cr.kind === 'delete') {
            return '<div class="ac-section"><div class="ac-section-head">Change</div><div class="ac-delete-note">This alert would be deleted.</div></div>';
        }
        if (!cr.content || !window.AlertHistory) return '';

        // A create has nothing to diff against, so it shows the definition itself.
        const body = cr.current
            ? (() => {
                const changes = AlertHistory.diff(cr.current, cr.content);
                return changes.length === 0
                    ? '<div class="ac-empty">No change to the definition.</div>'
                    : `<div class="ahd-changes">${changes.map(c => AlertHistory.renderChange(c)).join('')}</div>`;
              })()
            : AlertHistory.renderSnapshot(cr.content);

        return `<div class="ac-section"><div class="ac-section-head">${cr.current ? 'Diff' : 'Definition'}</div>${body}</div>`;
    },

    renderTests(cr) {
        const tests = cr.readiness?.tests;
        if (!tests || !(tests.outcomes || []).length) return '';

        return `
            <div class="ac-section">
                <div class="ac-section-head">Tests</div>
                <div class="ac-tests">
                    ${tests.outcomes.map(o => `
                        <div class="ac-test ac-test-${o.passed ? 'pass' : 'fail'}">
                            <span class="ac-test-dot"></span>
                            <span class="ac-test-name">${Utils.escapeHtml(o.name)}</span>
                            <span class="ac-test-expect">${o.expectation === 'match' ? 'should match' : 'should not match'}</span>
                            ${o.passed ? '' : `<span class="ac-test-reason">${Utils.escapeHtml(o.reason || '')}</span>`}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    },

    renderPolicy(cr) {
        const policy = cr.readiness?.policy;
        const checks = policy?.checks || [];
        if (!checks.length) return '';

        // Same ordering as the editor: what needs fixing first, what is satisfied after.
        const order = { fail: 0, deferred: 1, pass: 2 };
        const stateOf = c => (c.deferred ? 'deferred' : (c.passed ? 'pass' : 'fail'));
        const sorted = [...checks].sort((a, b) => order[stateOf(a)] - order[stateOf(b)]);
        const badge = { pass: 'Pass', fail: 'Fail', deferred: 'On merge' };

        return `
            <div class="ac-section">
                <div class="ac-section-head">
                    Checks
                    <span class="ac-section-note">${policy.passed || 0}/${checks.length} passing</span>
                </div>
                <div class="ac-policy">
                    ${sorted.map(c => {
                        const state = stateOf(c);
                        return `
                        <div class="ac-check ac-check-${state}">
                            <span class="ac-check-mark"></span>
                            <span class="ac-check-body">
                                <span class="ac-check-label">${Utils.escapeHtml(c.label || c.field)}</span>
                                ${state === 'pass' ? '' : `
                                    <span class="ac-violation-text">${Utils.escapeHtml(c.message || '')}</span>
                                    ${c.detail ? `<span class="ac-violation-detail">${Utils.escapeHtml(c.detail)}</span>` : ''}`}
                            </span>
                            <span class="ap-badge ap-badge-${state}">${badge[state]}</span>
                        </div>`;
                    }).join('')}
                </div>
            </div>
        `;
    },

    renderReviews(cr) {
        if (!(cr.reviews || []).length) return '';

        return `
            <div class="ac-section">
                <div class="ac-section-head">Reviews</div>
                <div class="ac-reviews">
                    ${cr.reviews.map(r => `
                        <div class="ac-review${r.stale ? ' ac-review-stale' : ''}">
                            <span class="ac-review-mark ac-review-${r.decision}"></span>
                            <span class="ac-review-who">${Utils.escapeHtml(r.reviewer_label || r.reviewer || 'unknown')}</span>
                            <span class="ac-review-verb">${r.decision === 'approve' ? 'approved' : 'requested changes'}</span>
                            ${r.stale ? '<span class="ac-review-tag">superseded</span>' : ''}
                            <span class="ac-review-age">${Utils.escapeHtml(Utils.timeAgo(r.created_at))}</span>
                            ${r.comment ? `<div class="ac-review-comment">${Utils.escapeHtml(r.comment)}</div>` : ''}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    },

    renderActions(cr) {
        if (!cr.status || cr.status === 'merged' || cr.status === 'discarded') {
            return `<span class="ac-actions-note">This proposal is ${this.STATUS_LABEL[cr.status] || cr.status}.</span>`;
        }

        // Asking for changes needs a sentence from the reviewer, composed in place
        // rather than in a browser dialog that discards it on a stray Escape.
        if (this._composing) {
            return `
                <div class="ac-compose">
                    <textarea id="acRejectComment" class="ac-compose-input" spellcheck="false"
                              placeholder="What should the author change?"></textarea>
                    <div class="ac-compose-actions">
                        <button class="btn-secondary btn-sm" onclick="AlertChanges.cancelCompose()">Cancel</button>
                        <button class="btn-primary btn-sm" onclick="AlertChanges.submitReject()">Request changes</button>
                    </div>
                </div>
            `;
        }

        const buttons = [];
        if (cr.can_review) {
            buttons.push('<button class="btn-secondary btn-sm" onclick="AlertChanges.review(\'reject\')">Request changes</button>');
            buttons.push('<button class="btn-secondary btn-sm" onclick="AlertChanges.review(\'approve\')">Approve</button>');
        }
        if (cr.is_author) {
            buttons.push('<button class="btn-secondary btn-sm" onclick="AlertChanges.discard()">Withdraw</button>');
        }
        buttons.push(`<button class="btn-primary btn-sm"${cr.can_merge ? '' : ' disabled'} onclick="AlertChanges.merge()">Merge</button>`);

        return `<div class="ac-actions-row">${buttons.join('')}</div>`;
    },

    // ---- Actions ----

    async review(decision) {
        if (this._busy || !this._selected) return;

        if (decision === 'reject') {
            this._composing = true;
            this.renderDrawer();
            document.getElementById('acRejectComment')?.focus();
            return;
        }
        await this.send('approve', '');
    },

    cancelCompose() {
        this._composing = false;
        this.renderDrawer();
    },

    submitReject() {
        const comment = (document.getElementById('acRejectComment')?.value || '').trim();
        if (!comment) {
            document.getElementById('acRejectComment')?.focus();
            return;
        }
        this._composing = false;
        this.send('reject', comment);
    },

    async send(decision, comment) {
        await this.act(() => this.api(`/api/v1/alert-changes/${this._selected}/review`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ decision, comment })
        }), decision === 'approve' ? 'Approved' : 'Sent back for changes');
    },

    async merge() {
        if (this._busy || !this._selected) return;
        await this.act(() => this.api(`/api/v1/alert-changes/${this._selected}/merge`, { method: 'POST' }), 'Merged');
    },

    async discard() {
        if (this._busy || !this._selected) return;
        // Kept deliberately, unlike approve and merge. Withdrawing is terminal: a
        // discarded proposal is closed to review, revision and merge, and nothing
        // reopens it. The button also sits inches from Approve.
        if (!confirm('Withdraw this proposal? It cannot be reopened.')) return;
        await this.act(() => this.api(`/api/v1/alert-changes/${this._selected}/discard`, { method: 'POST' }), 'Withdrawn');
    },

    async act(fn, successMessage) {
        this._busy = true;
        try {
            await fn();
            Toast.success(successMessage);
            const id = this._selected;
            this._list = await this.fetchList();
            this.render();
            if (this._list.some(cr => cr.id === id)) await this.open(id);
            else this.close();
            this.refreshBadge();
            if (window.Alerts?.loadAlerts) Alerts.loadAlerts();
        } catch (e) {
            Toast.error('Could not complete', e.message);
        } finally {
            this._busy = false;
        }
    }
};

window.AlertChanges = AlertChanges;

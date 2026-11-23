// Bulk edits on the alerts list: selection, the action picker, the review step and undo.
//
// Selection is by id over the filtered set the browser already holds, so "select all
// matching" names exactly the alerts on screen. The server works out each alert's
// change itself: adding an action an alert already runs, or removing one it lacks,
// leaves it alone.
const AlertBulk = {
    KIND_LABEL: { webhook: 'Webhook', email: 'Email', fractal: 'Fractal', dictionary: 'Lookup' },

    selected: new Set(),
    _anchor: null,
    _catalog: null,
    _picker: null,
    _review: null,

    // ---- Action identity ----

    key(kind, id) {
        return `${kind}:${id}`;
    },

    parseKey(key) {
        const i = key.indexOf(':');
        return { kind: key.slice(0, i), id: key.slice(i + 1) };
    },

    // The list endpoint carries webhook and fractal actions as objects, and lookup and
    // email actions as ids.
    actionKeys(alert) {
        const keys = new Set();
        for (const a of alert.webhook_actions || []) keys.add(this.key('webhook', a.id));
        for (const a of alert.fractal_actions || []) keys.add(this.key('fractal', a.id));
        for (const id of alert.dictionary_action_ids || []) keys.add(this.key('dictionary', id));
        for (const id of alert.email_action_ids || []) keys.add(this.key('email', id));
        return keys;
    },

    // Every action in scope, across the four kinds, sorted by name.
    async loadCatalog(force = false) {
        if (this._catalog && !force) return this._catalog;
        const sources = [
            ['webhook', '/api/v1/webhooks', a => a.url],
            ['email', '/api/v1/email-actions', a => (a.recipients || []).join(', ')],
            ['fractal', '/api/v1/fractal-actions', a => a.description || ''],
            ['dictionary', '/api/v1/dictionary-actions', a => a.dictionary_name || '']
        ];
        const lists = await Promise.all(sources.map(async ([kind, url, detail]) => {
            const res = await fetch(url, { credentials: 'include' });
            const body = await res.json().catch(() => ({}));
            return ((body.success ? body.data : null) || []).map(a => ({
                ...a, kind, key: this.key(kind, a.id), detail: detail(a)
            }));
        }));
        this._catalog = lists.flat().sort((a, b) => a.name.localeCompare(b.name));
        return this._catalog;
    },

    actionName(ref) {
        const hit = (this._catalog || []).find(a => a.kind === ref.kind && a.id === ref.id);
        return hit ? hit.name : `${this.KIND_LABEL[ref.kind] || ref.kind} action`;
    },

    // ---- Selection ----

    isSelected(id) {
        return this.selected.has(id);
    },

    // Selection never outlives what is on screen: an alert filtered away is not acted on.
    prune(visibleAlerts) {
        const visible = new Set(visibleAlerts.map(a => a.id));
        for (const id of [...this.selected]) {
            if (!visible.has(id)) this.selected.delete(id);
        }
        if (this._anchor && !visible.has(this._anchor)) this._anchor = null;
    },

    clear() {
        this.selected.clear();
        this._anchor = null;
        this.refresh();
    },

    // Shift-click selects the run between the last clicked row and this one.
    onRowCheck(event, id) {
        event.stopPropagation();
        const checked = event.target.checked;
        const page = Alerts.getCurrentPageAlerts().map(a => a.id);
        if (event.shiftKey && this._anchor && page.includes(this._anchor)) {
            const [a, b] = [page.indexOf(this._anchor), page.indexOf(id)].sort((x, y) => x - y);
            for (const rid of page.slice(a, b + 1)) {
                checked ? this.selected.add(rid) : this.selected.delete(rid);
            }
        } else {
            checked ? this.selected.add(id) : this.selected.delete(id);
        }
        this._anchor = id;
        this.refresh();
    },

    togglePage(checked) {
        for (const a of Alerts.getCurrentPageAlerts()) {
            checked ? this.selected.add(a.id) : this.selected.delete(a.id);
        }
        this.refresh();
    },

    selectAllMatching() {
        for (const a of Alerts.filteredAlerts) this.selected.add(a.id);
        this.refresh();
    },

    // ---- Table chrome ----

    headerCell() {
        return '<th class="ab-check-cell"><input type="checkbox" id="abPageCheck" aria-label="Select this page" onclick="event.stopPropagation(); AlertBulk.togglePage(this.checked)"></th>';
    },

    rowCell(alert) {
        const id = Utils.escapeAttr(alert.id);
        return `<td class="ab-check-cell" onclick="event.stopPropagation()"><input type="checkbox" class="ab-row-check" data-id="${id}" aria-label="Select ${Utils.escapeAttr(alert.name)}" ${this.isSelected(alert.id) ? 'checked' : ''} onclick="AlertBulk.onRowCheck(event, '${Utils.escapeJs(alert.id)}')"></td>`;
    },

    // Updates checkboxes and the bar in place, so selecting does not rebuild the table.
    refresh() {
        const page = Alerts.getCurrentPageAlerts();
        document.querySelectorAll('#alertsList .ab-row-check').forEach(cb => {
            const on = this.selected.has(cb.dataset.id);
            cb.checked = on;
            cb.closest('tr')?.classList.toggle('ab-selected', on);
        });

        const pageCheck = document.getElementById('abPageCheck');
        if (pageCheck) {
            const onPage = page.filter(a => this.selected.has(a.id)).length;
            pageCheck.checked = page.length > 0 && onPage === page.length;
            pageCheck.indeterminate = onPage > 0 && onPage < page.length;
        }

        const bar = document.getElementById('alertsBulkBar');
        if (bar) bar.innerHTML = this.renderBar();
    },

    // The count line, or the bulk bar once anything is selected.
    renderBar() {
        const total = Alerts.filteredAlerts.length;
        const n = this.selected.size;
        if (n === 0) {
            const page = Alerts.getCurrentPageAlerts();
            return AlertList.renderTableHeader({
                shown: page.length,
                total,
                unfiltered: Alerts.allAlerts?.length,
                pageSize: Alerts.alertsPageSize,
                onPageSize: 'Alerts.changePageSize'
            });
        }

        const page = Alerts.getCurrentPageAlerts();
        const pageAll = page.length > 0 && page.every(a => this.selected.has(a.id));
        let scope = '';
        if (n === total && total > page.length) {
            scope = `<span class="ab-scope">All ${total} matching alerts are selected.</span>`;
        } else if (pageAll && total > n) {
            scope = `<span class="ab-scope">All ${n} on this page are selected.
                <button type="button" class="ab-link" onclick="AlertBulk.selectAllMatching()">Select all ${total} matching alerts</button></span>`;
        }

        return `
            <div class="ab-bar" role="toolbar" aria-label="Bulk actions">
                <span class="ab-count">${n} selected</span>
                ${scope}
                <span class="ab-spacer"></span>
                <button type="button" class="btn-secondary btn-sm ab-menu-btn" id="abActionsBtn" onclick="AlertBulk.openPicker(this)">
                    Actions
                    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><polyline points="6 9 12 15 18 9"/></svg>
                </button>
                <button type="button" class="btn-secondary btn-sm" onclick="AlertBulk.toggleEnabled(true)">Enable</button>
                <button type="button" class="btn-secondary btn-sm" onclick="AlertBulk.toggleEnabled(false)">Disable</button>
                <button type="button" class="ab-link ab-clear" onclick="AlertBulk.clear()">Clear</button>
            </div>`;
    },

    selectedAlerts() {
        return (Alerts.allAlerts || []).filter(a => this.selected.has(a.id));
    },

    // Enabling and disabling is undone by putting each alert back as it was, not by
    // flipping the whole set.
    async toggleEnabled(enabled) {
        const alerts = this.selectedAlerts();
        const flipped = alerts.filter(a => a.enabled !== enabled).map(a => a.id);
        if (flipped.length === 0) {
            Toast.show('info', `All ${alerts.length} selected alerts are already ${enabled ? 'enabled' : 'disabled'}`);
            return;
        }
        try {
            await this.post('/api/v1/alerts/batch-toggle', { alert_ids: flipped, enabled });
        } catch (e) {
            Toast.show('error', 'Could not update alerts', e.message);
            return;
        }
        await Alerts.loadAlerts();
        Toast.show('success', `${this.plural(flipped.length)} ${enabled ? 'enabled' : 'disabled'}`, null, {
            action: {
                label: 'Undo',
                onClick: async () => {
                    try {
                        await this.post('/api/v1/alerts/batch-toggle', { alert_ids: flipped, enabled: !enabled });
                        await Alerts.loadAlerts();
                        Toast.show('success', 'Change undone');
                    } catch (e) {
                        Toast.show('error', 'Could not undo', e.message);
                    }
                }
            }
        });
    },

    // ---- Action picker ----

    // Each action shows where it stands across the selection: on all, some or none.
    // Clicking cycles it between adding to all, removing from all and, for a mixed
    // action, leaving it as it is.
    async openPicker(button) {
        if (this._picker) {
            this.closePicker();
            return;
        }
        let catalog;
        try {
            catalog = await this.loadCatalog(true);
        } catch (e) {
            Toast.show('error', 'Could not load actions', e.message);
            return;
        }

        const alerts = this.selectedAlerts();
        const counts = new Map();
        for (const a of alerts) {
            for (const k of this.actionKeys(a)) counts.set(k, (counts.get(k) || 0) + 1);
        }
        const rows = catalog.filter(a => a.enabled || counts.get(a.key)).map(a => {
            const have = counts.get(a.key) || 0;
            const initial = have === 0 ? 'none' : (have === alerts.length ? 'all' : 'some');
            return { ...a, have, initial, state: initial };
        });

        this._picker = { rows, total: alerts.length, query: '', anchor: button };
        const el = document.createElement('div');
        el.id = 'abPicker';
        el.className = 'ab-picker';
        el.setAttribute('role', 'dialog');
        el.setAttribute('aria-label', 'Add or remove actions');
        document.body.appendChild(el);
        this.renderPicker();
        this.placePicker();
        el.querySelector('.ab-picker-search')?.focus();

        this._outside = e => {
            if (!el.contains(e.target) && !button.contains(e.target)) this.closePicker();
        };
        this._keys = e => {
            if (e.key === 'Escape') this.closePicker();
        };
        setTimeout(() => {
            document.addEventListener('mousedown', this._outside);
            document.addEventListener('keydown', this._keys);
        });
        window.addEventListener('resize', this._place = () => this.placePicker());
    },

    placePicker() {
        const el = document.getElementById('abPicker');
        const anchor = this._picker?.anchor;
        if (!el || !anchor?.isConnected) return;
        const r = anchor.getBoundingClientRect();
        const width = el.offsetWidth;
        el.style.top = `${r.bottom + 6}px`;
        el.style.left = `${Math.max(8, Math.min(r.left, window.innerWidth - width - 8))}px`;
    },

    closePicker() {
        document.getElementById('abPicker')?.remove();
        document.removeEventListener('mousedown', this._outside);
        document.removeEventListener('keydown', this._keys);
        window.removeEventListener('resize', this._place);
        this._picker = null;
    },

    cycle(key) {
        const row = this._picker?.rows.find(r => r.key === key);
        if (!row) return;
        const order = row.initial === 'some' ? ['some', 'all', 'none'] : ['all', 'none'];
        const next = order[(order.indexOf(row.state) + 1) % order.length];
        // A disabled action can be taken off alerts but not put on more.
        row.state = next === 'all' && !row.enabled ? (row.initial === 'some' ? 'none' : row.state) : next;
        this.renderPicker();
    },

    filterPicker(query) {
        this._picker.query = query;
        this.renderPicker(true);
    },

    pickerChanges() {
        const add = [];
        const remove = [];
        for (const r of this._picker?.rows || []) {
            if (r.state === r.initial) continue;
            if (r.state === 'all') add.push({ kind: r.kind, id: r.id });
            if (r.state === 'none') remove.push({ kind: r.kind, id: r.id });
        }
        return { add, remove };
    },

    renderPicker(keepSearch = false) {
        const el = document.getElementById('abPicker');
        const p = this._picker;
        if (!el || !p) return;

        const q = p.query.trim().toLowerCase();
        const shown = p.rows.filter(r => !q || r.name.toLowerCase().includes(q) || (this.KIND_LABEL[r.kind] || '').toLowerCase().includes(q));
        const { add, remove } = this.pickerChanges();
        const changes = add.length + remove.length;

        const list = shown.length === 0
            ? `<div class="ab-picker-empty">${p.rows.length ? 'No actions match' : 'No actions yet. Create one under the Actions tab.'}</div>`
            : shown.map(r => this.renderPickerRow(r, p.total)).join('');

        const body = `
            <div class="ab-picker-list" role="listbox" aria-multiselectable="true">${list}</div>
            <div class="ab-picker-foot">
                <span class="ab-picker-hint">${changes ? `${changes} change${changes === 1 ? '' : 's'}` : 'Click an action to add or remove it'}</span>
                <button type="button" class="btn-secondary btn-sm" onclick="AlertBulk.closePicker()">Cancel</button>
                <button type="button" class="btn-primary btn-sm" ${changes ? '' : 'disabled'} onclick="AlertBulk.reviewPicker()">Review</button>
            </div>`;

        if (keepSearch && el.querySelector('.ab-picker-search')) {
            el.querySelector('.ab-picker-body').innerHTML = body;
            return;
        }
        el.innerHTML = `
            <div class="ab-picker-head">
                <input type="text" class="ab-picker-search" placeholder="Search actions" value="${Utils.escapeAttr(p.query)}"
                       oninput="AlertBulk.filterPicker(this.value)" spellcheck="false" autocomplete="off">
            </div>
            <div class="ab-picker-body">${body}</div>`;
    },

    renderPickerRow(r, total) {
        const changed = r.state !== r.initial;
        let note;
        if (r.state === 'all') {
            note = changed ? `<span class="ab-delta ab-delta-add">+${total - r.have}</span>` : `all ${total}`;
        } else if (r.state === 'none') {
            note = changed ? `<span class="ab-delta ab-delta-remove">&minus;${r.have}</span>` : 'none';
        } else {
            note = `${r.have} of ${total}`;
        }
        const box = r.state === 'all' ? 'checked' : (r.state === 'some' ? 'mixed' : 'unchecked');
        return `
            <button type="button" class="ab-picker-row${changed ? ' changed' : ''}" role="option"
                    aria-checked="${r.state === 'all' ? 'true' : (r.state === 'some' ? 'mixed' : 'false')}"
                    onclick="AlertBulk.cycle('${Utils.escapeJs(r.key)}')">
                <span class="ab-box ab-box-${box}"></span>
                <span class="ab-picker-name">${Utils.escapeHtml(r.name)}</span>
                ${r.enabled ? '' : '<span class="ab-picker-off">disabled</span>'}
                <span class="action-type-badge ${r.kind}">${this.KIND_LABEL[r.kind] || r.kind}</span>
                <span class="ab-picker-note">${note}</span>
            </button>`;
    },

    reviewPicker() {
        const { add, remove } = this.pickerChanges();
        const ids = [...this.selected];
        this.closePicker();
        this.review([{ alert_ids: ids, add, remove }]);
    },

    // ---- Review ----

    // The server works out the edit against the alerts as they are now, so the review
    // shows what will actually happen, including alerts a policy rule would block.
    async review(changes, opts = {}) {
        changes = changes.filter(c => c.alert_ids.length && (c.add?.length || c.remove?.length));
        if (changes.length === 0) return;
        let plan;
        try {
            [plan] = await Promise.all([
                this.post('/api/v1/alerts/batch-actions/preview', { changes }),
                this.loadCatalog()
            ]);
        } catch (e) {
            Toast.show('error', 'Could not prepare the change', e.message);
            return;
        }
        this._review = { changes, plan, onDone: opts.onDone };
        this.renderReview();
    },

    renderReview() {
        const { plan } = this._review;
        let modal = document.getElementById('abReviewModal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'abReviewModal';
            modal.className = 'modal ab-review';
            modal.addEventListener('mousedown', e => { if (e.target === modal) this.closeReview(); });
            document.body.appendChild(modal);
            this._reviewKeys = e => { if (e.key === 'Escape') this.closeReview(); };
            document.addEventListener('keydown', this._reviewKeys);
        }

        const lines = [];
        for (const t of plan.actions || []) {
            const name = Utils.escapeHtml(t.name || this.actionName(t));
            const badge = `<span class="action-type-badge ${t.kind}">${this.KIND_LABEL[t.kind] || t.kind}</span>`;
            const adding = this.asked(t, 'add');
            const removing = this.asked(t, 'remove');
            // Skipped is only attributable when the action went one way.
            const skipped = adding !== removing ? t.skipped : 0;
            if (adding) {
                lines.push(this.renderLine('add', `Add ${badge} <strong>${name}</strong> to ${this.plural(t.added)}`,
                    skipped ? `${skipped} already ${skipped === 1 ? 'has' : 'have'} it` : ''));
            }
            if (removing) {
                lines.push(this.renderLine('remove', `Remove ${badge} <strong>${name}</strong> from ${this.plural(t.removed)}`,
                    skipped ? `${skipped} ${skipped === 1 ? 'does' : 'do'} not have it` : ''));
            }
        }

        const byName = (a, b) => (a.alert_name || '').localeCompare(b.alert_name || '');
        const blocked = (plan.alerts || []).filter(a => (a.blocking || []).length).sort(byName);
        const changing = (plan.alerts || []).filter(a => !(a.blocking || []).length).sort(byName);
        const canApply = plan.changed > 0 && plan.blocked === 0;
        const applyLabel = plan.gated ? 'Propose change' : `Apply to ${this.plural(plan.changed)}`;

        modal.innerHTML = `
            <div class="modal-content ab-review-content">
                <div class="modal-header">
                    <h2>Review changes</h2>
                    <button class="close-btn" onclick="AlertBulk.closeReview()" aria-label="Close">&times;</button>
                </div>
                <div class="modal-body">
                    <div class="ab-review-lead">${this.plural(plan.changed)} will change${plan.unchanged ? `, ${plan.unchanged} already ${plan.unchanged === 1 ? 'matches' : 'match'}` : ''}</div>
                    <div class="ab-review-lines">${lines.join('')}</div>
                    ${plan.changed === 0 ? '<div class="ab-review-note">Nothing to change: every selected alert already matches.</div>' : ''}
                    ${blocked.length ? this.renderBlocked(blocked) : ''}
                    ${changing.length ? `
                        <details class="ab-review-alerts">
                            <summary>Show the ${this.plural(changing.length)} that change</summary>
                            <ul>${changing.map(a => this.renderAlertDelta(a)).join('')}</ul>
                        </details>` : ''}
                    ${plan.gated && plan.changed > 0 ? `
                        <div class="ab-review-gate">
                            <div>Changes in this scope are reviewed, so this opens one proposal covering every alert instead of applying now.</div>
                            <textarea id="abReviewSummary" class="ab-review-summary" rows="2" placeholder="Note for reviewers (optional)"></textarea>
                        </div>` : ''}
                </div>
                <div class="modal-footer ab-review-foot">
                    <button class="btn-secondary" onclick="AlertBulk.closeReview()">Cancel</button>
                    <button class="btn-primary" id="abApplyBtn" ${canApply ? '' : 'disabled'} onclick="AlertBulk.apply()">${applyLabel}</button>
                </div>
            </div>`;
        modal.style.display = 'flex';
        modal.querySelector('#abApplyBtn')?.focus();
    },

    // Which way an action was asked to go, read from the request rather than the
    // tally, so a line still shows when every alert already matched.
    asked(ref, dir) {
        return (this._review?.changes || []).some(c => (c[dir] || []).some(r => r.kind === ref.kind && r.id === ref.id));
    },

    renderLine(kind, text, note) {
        return `
            <div class="ab-line ab-line-${kind}">
                <span class="ab-line-mark">${kind === 'add' ? '+' : '&minus;'}</span>
                <span class="ab-line-text">${text}</span>
                ${note ? `<span class="ab-line-note">${note}</span>` : ''}
            </div>`;
    },

    renderBlocked(blocked) {
        const max = 5;
        const items = blocked.slice(0, max).map(a => `
            <li><span class="ab-blocked-name">${Utils.escapeHtml(a.alert_name)}</span>
                <span class="ab-blocked-why">${Utils.escapeHtml(a.blocking.map(v => v.message).join('; '))}</span></li>`).join('');
        return `
            <div class="ab-review-blocked">
                <div class="ab-blocked-head">${this.plural(blocked.length)} would break a policy rule</div>
                <ul>${items}</ul>
                ${blocked.length > max ? `<div class="ab-blocked-more">and ${blocked.length - max} more</div>` : ''}
                <button type="button" class="btn-secondary btn-sm" onclick="AlertBulk.leaveOutBlocked()">Leave ${blocked.length === 1 ? 'it' : 'these'} out</button>
            </div>`;
    },

    renderAlertDelta(a) {
        const chip = (ref, kind) => `<span class="ab-chip ab-chip-${kind}">${kind === 'add' ? '+' : '&minus;'} ${Utils.escapeHtml(this.actionName(ref))}</span>`;
        return `<li><span class="ab-alert-name">${Utils.escapeHtml(a.alert_name)}</span>
            ${(a.added || []).map(r => chip(r, 'add')).join('')}${(a.removed || []).map(r => chip(r, 'remove')).join('')}</li>`;
    },

    async leaveOutBlocked() {
        const out = new Set((this._review.plan.alerts || []).filter(a => (a.blocking || []).length).map(a => a.alert_id));
        for (const id of out) this.selected.delete(id);
        this.refresh();
        const changes = this._review.changes.map(c => ({ ...c, alert_ids: c.alert_ids.filter(id => !out.has(id)) }));
        const onDone = this._review.onDone;
        if (!changes.some(c => c.alert_ids.length)) {
            this.closeReview();
            return;
        }
        await this.review(changes, { onDone });
    },

    closeReview() {
        document.getElementById('abReviewModal')?.remove();
        document.removeEventListener('keydown', this._reviewKeys);
        this._review = null;
    },

    async apply() {
        const r = this._review;
        if (!r) return;
        const btn = document.getElementById('abApplyBtn');
        if (btn) btn.disabled = true;
        const summary = document.getElementById('abReviewSummary')?.value || '';

        let plan;
        try {
            plan = await this.post('/api/v1/alerts/batch-actions', { changes: r.changes, summary });
        } catch (e) {
            // A refusal with a plan attached (a rule that started failing since the
            // preview) re-renders the review rather than losing the user's place.
            if (e.data?.alerts) {
                r.plan = e.data;
                this.renderReview();
                return;
            }
            Toast.show('error', 'Could not apply the change', e.message);
            if (btn) btn.disabled = false;
            return;
        }

        const onDone = r.onDone;
        this.closeReview();
        await Alerts.loadAlerts();
        if (onDone) onDone(plan);

        if (plan.proposal) {
            Toast.show('success', 'Proposal opened', plan.summary, {
                action: { label: 'View', onClick: () => AlertFeeds.showChangesTab?.() }
            });
            window.AlertChanges?.refreshBadge();
            return;
        }
        Toast.show('success', plan.summary, null, {
            duration: 10000,
            action: { label: 'Undo', onClick: () => this.undo(plan.undo || []) }
        });
    },

    async undo(changes) {
        if (!changes.length) return;
        try {
            await this.post('/api/v1/alerts/batch-actions', { changes });
            await Alerts.loadAlerts();
            Toast.show('success', 'Change undone');
        } catch (e) {
            Toast.show('error', 'Could not undo', e.message);
        }
    },

    // ---- From an action: which alerts use it ----

    // The same edit from the other side: one action, every alert, checked where it is
    // attached. Saving reviews the difference like any other bulk edit.
    async manageAction(kind, id) {
        try {
            await Promise.all([this.loadCatalog(true), Alerts.allAlerts?.length ? null : Alerts.loadAlerts()]);
        } catch (e) {
            Toast.show('error', 'Could not load alerts', e.message);
            return;
        }
        const action = this._catalog.find(a => a.kind === kind && a.id === id);
        if (!action) return;
        const key = this.key(kind, id);
        const alerts = (Alerts.allAlerts || []).slice().sort((a, b) => a.name.localeCompare(b.name));
        const using = new Set(alerts.filter(a => this.actionKeys(a).has(key)).map(a => a.id));
        this._usage = { action, alerts, using, checked: new Set(using), query: '', show: 'all' };
        this.renderUsage();
    },

    usageShown() {
        const u = this._usage;
        const q = u.query.trim().toLowerCase();
        return u.alerts.filter(a => {
            if (q && !a.name.toLowerCase().includes(q) && !(a.labels || []).some(l => l.toLowerCase().includes(q))) return false;
            if (u.show === 'using') return u.checked.has(a.id);
            if (u.show === 'unused') return !u.checked.has(a.id);
            return true;
        });
    },

    usageDiff() {
        const u = this._usage;
        return {
            add: [...u.checked].filter(id => !u.using.has(id)),
            remove: [...u.using].filter(id => !u.checked.has(id))
        };
    },

    renderUsage(listOnly = false) {
        const u = this._usage;
        let modal = document.getElementById('abUsageModal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'abUsageModal';
            modal.className = 'modal ab-review';
            modal.addEventListener('mousedown', e => { if (e.target === modal) this.closeUsage(); });
            document.body.appendChild(modal);
            this._usageKeys = e => { if (e.key === 'Escape' && !this._review) this.closeUsage(); };
            document.addEventListener('keydown', this._usageKeys);
        }

        const shown = this.usageShown();
        const { add, remove } = this.usageDiff();
        const rows = shown.length === 0
            ? '<div class="ab-picker-empty">No alerts match</div>'
            : shown.map(a => `
                <label class="ab-usage-row${u.checked.has(a.id) !== u.using.has(a.id) ? ' changed' : ''}">
                    <input type="checkbox" ${u.checked.has(a.id) ? 'checked' : ''} onchange="AlertBulk.toggleUsage('${Utils.escapeJs(a.id)}', this.checked)">
                    <span class="status-dot status-${a.enabled ? 'enabled' : 'disabled'}"></span>
                    <span class="ab-usage-name">${Utils.escapeHtml(a.name)}</span>
                    <span class="severity-pill severity-${Utils.escapeAttr((a.severity || 'medium').toLowerCase())}">${Utils.escapeHtml((a.severity || 'medium').replace(/^./, c => c.toUpperCase()))}</span>
                </label>`).join('');
        const diff = [add.length ? `<span class="ab-delta ab-delta-add">+${add.length}</span>` : '',
            remove.length ? `<span class="ab-delta ab-delta-remove">&minus;${remove.length}</span>` : ''].join(' ');
        const listHTML = `
            <div class="ab-usage-tools">
                <span class="ab-usage-count">Used by ${u.checked.size} of ${u.alerts.length} alerts</span>
                <span class="ab-spacer"></span>
                <button type="button" class="ab-link" onclick="AlertBulk.checkShown(true)">Check all shown</button>
                <button type="button" class="ab-link" onclick="AlertBulk.checkShown(false)">Uncheck all shown</button>
            </div>
            <div class="ab-usage-list">${rows}</div>`;

        if (listOnly && modal.querySelector('#abUsageList')) {
            modal.querySelector('#abUsageList').innerHTML = listHTML;
            modal.querySelector('#abUsageDiff').innerHTML = diff;
            modal.querySelector('#abUsageSave').disabled = !(add.length || remove.length);
            return;
        }

        const tab = (key, label) => `<button type="button" class="ac-filter-btn${u.show === key ? ' active' : ''}" onclick="AlertBulk.setUsageShow('${key}')">${label}</button>`;
        modal.innerHTML = `
            <div class="modal-content ab-review-content ab-usage-content">
                <div class="modal-header">
                    <h2>Alerts using <span class="ab-usage-action">${Utils.escapeHtml(u.action.name)}</span></h2>
                    <button class="close-btn" onclick="AlertBulk.closeUsage()" aria-label="Close">&times;</button>
                </div>
                <div class="modal-body">
                    <div class="ab-usage-filter">
                        <input type="text" class="ab-picker-search" placeholder="Search alerts or labels" value="${Utils.escapeAttr(u.query)}"
                               oninput="AlertBulk.setUsageQuery(this.value)" spellcheck="false" autocomplete="off">
                        <div class="ac-filter">${tab('all', 'All')}${tab('using', 'Using')}${tab('unused', 'Not using')}</div>
                    </div>
                    <div id="abUsageList">${listHTML}</div>
                </div>
                <div class="modal-footer ab-review-foot">
                    <span id="abUsageDiff" class="ab-usage-diff">${diff}</span>
                    <button class="btn-secondary" onclick="AlertBulk.closeUsage()">Cancel</button>
                    <button class="btn-primary" id="abUsageSave" ${add.length || remove.length ? '' : 'disabled'} onclick="AlertBulk.reviewUsage()">Review</button>
                </div>
            </div>`;
        modal.style.display = 'flex';
        modal.querySelector('.ab-picker-search')?.focus();
    },

    toggleUsage(id, on) {
        on ? this._usage.checked.add(id) : this._usage.checked.delete(id);
        this.renderUsage(true);
    },

    checkShown(on) {
        for (const a of this.usageShown()) on ? this._usage.checked.add(a.id) : this._usage.checked.delete(a.id);
        this.renderUsage(true);
    },

    setUsageQuery(q) {
        this._usage.query = q;
        this.renderUsage(true);
    },

    setUsageShow(show) {
        this._usage.show = show;
        this.renderUsage();
    },

    reviewUsage() {
        const { action } = this._usage;
        const ref = { kind: action.kind, id: action.id };
        const { add, remove } = this.usageDiff();
        this.review([
            { alert_ids: add, add: [ref] },
            { alert_ids: remove, remove: [ref] }
        ], {
            onDone: () => {
                this.closeUsage();
                Alerts.loadAllActions?.();
            }
        });
    },

    closeUsage() {
        document.getElementById('abUsageModal')?.remove();
        document.removeEventListener('keydown', this._usageKeys);
        this._usage = null;
    },

    // ---- Helpers ----

    plural(n) {
        return `${n} alert${n === 1 ? '' : 's'}`;
    },

    async post(url, body) {
        const res = await fetch(url, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
            body: JSON.stringify(body)
        });
        const payload = await res.json().catch(() => ({}));
        if (!res.ok || payload.success === false) {
            const err = new Error(payload.error || `HTTP ${res.status}`);
            err.data = payload.data;
            throw err;
        }
        return payload.data;
    }
};

window.AlertBulk = AlertBulk;

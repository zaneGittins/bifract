// Shared alert detail panel: content, activity sparkline, geometry, and list
// keyboard navigation. Both the manual alerts table and the feed alerts table
// render through here so the two panels cannot drift apart again.
const AlertDetail = {
    WIDTH_KEY: 'bifract.alertPanelWidth',
    WIDTH_DEFAULT: 600,
    WIDTH_MIN: 480,
    WIDTH_MAX: 960,

    _activityToken: 0,
    _keyHandler: null,
    _insetHandler: null,
    _insetFrame: null,
    _insetPanel: null,

    // ---- Content ----

    // Details and History share the drawer rather than stacking a second panel over
    // it: history is about the alert this drawer is already scoped to.
    //
    // Tabs are rebuilt on every open, so moving between alerts always lands on Details.
    installTabs(panel, alert, detailsHtml) {
        const content = panel?.querySelector('.alert-details-content');
        if (!content) return;

        content.innerHTML = `
            <div class="ad-tabs">
                <button type="button" class="ad-tab active" data-tab="details">Details</button>
                <button type="button" class="ad-tab" data-tab="history">History</button>
            </div>
            <div class="ad-pane" data-pane="details">${detailsHtml}</div>
            <div class="ad-pane" data-pane="history" hidden></div>
        `;

        const panes = content.querySelectorAll('.ad-pane');
        content.querySelectorAll('.ad-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                content.querySelectorAll('.ad-tab').forEach(t => t.classList.toggle('active', t === tab));
                panes.forEach(p => { p.hidden = p.dataset.pane !== tab.dataset.tab; });
                content.scrollTop = 0;

                // Loaded on first view so opening a drawer stays one request.
                if (tab.dataset.tab === 'history') {
                    const pane = content.querySelector('.ad-pane[data-pane="history"]');
                    if (pane && !pane.dataset.loaded) {
                        pane.dataset.loaded = '1';
                        window.AlertHistory?.renderInto(pane, alert.id, alert.name);
                    }
                }
            });
        });
    },

    // opts: { metaExtra: [{label, html}], blocks: [{label, html}], labels: [],
    //         renderLabel: (label) => html }
    renderBody(alert, opts = {}) {
        const isAutoDisabled = !alert.enabled && alert.disabled_reason;
        const statusClass = isAutoDisabled ? 'auto-disabled' : (alert.enabled ? 'enabled' : 'disabled');
        const statusText = isAutoDisabled ? 'Auto-disabled' : (alert.enabled ? 'Enabled' : 'Disabled');
        const sev = (alert.severity || 'medium').toLowerCase();
        const sevClass = sev === 'info' ? 'informational' : sev;
        const highlighted = window.SyntaxHighlight
            ? SyntaxHighlight.highlight(alert.query_string || '')
            : Utils.escapeHtml(alert.query_string || '');

        let typeDetail = Utils.escapeHtml(alert.alert_type || 'event');
        if (alert.alert_type === 'compound' && alert.window_duration) {
            typeDetail += ` over ${this.formatWindow(alert.window_duration)}`;
        } else if (alert.alert_type === 'scheduled' && alert.schedule_cron) {
            typeDetail += ` ${this.formatCron(alert.schedule_cron)}`;
        }

        let throttle = 'None';
        if (alert.throttle_time_seconds > 0) {
            throttle = this.formatThrottle(alert.throttle_time_seconds);
            if (alert.throttle_field) throttle += ` per ${Utils.escapeHtml(alert.throttle_field)}`;
        }

        const labels = opts.labels || alert.labels || [];
        const renderLabel = opts.renderLabel
            || (l => `<span class="label" style="--chip-color:${Utils.tagColorFor(l)}">${Utils.escapeHtml(l)}</span>`);

        return `
            ${isAutoDisabled ? `
                <div class="alert-auto-disabled-banner">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
                        <line x1="12" y1="9" x2="12" y2="13"/>
                        <line x1="12" y1="17" x2="12.01" y2="17"/>
                    </svg>
                    <span>${Utils.escapeHtml(alert.disabled_reason)}</span>
                </div>
            ` : ''}

            <div class="alert-detail-block">
                <div class="alert-detail-block-head">
                    <span class="alert-detail-label">Activity</span>
                    <span class="alert-activity-readout"></span>
                </div>
                <div class="alert-activity-body">
                    <div class="alert-activity-loading">Loading activity</div>
                </div>
            </div>

            <div class="alert-detail-meta">
                <div class="alert-meta-cell">
                    <span class="alert-detail-label">Status</span>
                    <span class="status-badge status-${statusClass}">${statusText}</span>
                </div>
                <div class="alert-meta-cell">
                    <span class="alert-detail-label">Severity</span>
                    <span class="severity-pill severity-${sevClass}" style="cursor:default">${Utils.escapeHtml(sev)}</span>
                </div>
                <div class="alert-meta-cell">
                    <span class="alert-detail-label">Type</span>
                    <span class="alert-meta-value">${typeDetail}</span>
                </div>
                <div class="alert-meta-cell">
                    <span class="alert-detail-label">Throttle</span>
                    <span class="alert-meta-value">${throttle}</span>
                </div>
                ${(opts.metaExtra || []).map(f => `
                    <div class="alert-meta-cell">
                        <span class="alert-detail-label">${Utils.escapeHtml(f.label)}</span>
                        ${f.html}
                    </div>
                `).join('')}
            </div>

            <div class="alert-detail-block">
                <div class="alert-detail-block-head">
                    <span class="alert-detail-label">Query</span>
                    <button class="alert-query-copy" title="Copy query">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                        </svg>
                    </button>
                </div>
                <pre class="alert-query-display alert-query-highlight"><code>${highlighted}</code></pre>
            </div>

            ${alert.description ? `
                <div class="alert-detail-block">
                    <span class="alert-detail-label">Description</span>
                    <p class="alert-detail-prose">${Utils.escapeHtml(alert.description)}</p>
                </div>
            ` : ''}

            ${this.renderActions(alert)}

            ${labels.length > 0 ? `
                <div class="alert-detail-block">
                    <span class="alert-detail-label">Labels</span>
                    <div class="alert-labels">${labels.map(renderLabel).join('')}</div>
                </div>
            ` : ''}

            ${alert.references && alert.references.length > 0 ? `
                <div class="alert-detail-block">
                    <span class="alert-detail-label">References</span>
                    <ul class="alert-references">
                        ${alert.references.map(ref => `<li><a href="${Utils.escapeHtml(ref)}" target="_blank" rel="noopener noreferrer" class="alert-reference-link">${Utils.escapeHtml(ref)}</a></li>`).join('')}
                    </ul>
                </div>
            ` : ''}

            ${(opts.blocks || []).map(b => `
                <div class="alert-detail-block">
                    <span class="alert-detail-label">${Utils.escapeHtml(b.label)}</span>
                    ${b.html}
                </div>
            `).join('')}

            <div class="alert-detail-provenance">
                <div class="alert-provenance-row">
                    <span>Modified</span>
                    <span>${alert.updated_by ? Utils.escapeHtml(alert.updated_by) : 'unknown'} &middot; ${TZ.format(alert.updated_at || alert.created_at, 'friendly')}</span>
                </div>
                <div class="alert-provenance-row">
                    <span>Created</span>
                    <span>${alert.created_by ? Utils.escapeHtml(alert.created_by) : 'unknown'} &middot; ${TZ.format(alert.created_at, 'friendly')}</span>
                </div>
            </div>
        `;
    },

    // Actions listed by name and target. Everything here already arrives with
    // the alert, so opening a panel costs no extra fetch.
    renderActions(alert) {
        const rows = [];
        (alert.webhook_actions || []).forEach(a => rows.push({ kind: 'Webhook', name: a.name, target: a.url }));
        (alert.fractal_actions || []).forEach(a => rows.push({ kind: 'Fractal', name: a.name, target: a.description || '' }));
        (alert.dictionary_actions || []).forEach(a => rows.push({ kind: 'Dictionary', name: a.name, target: '' }));
        (alert.email_actions || []).forEach(a => rows.push({ kind: 'Email', name: a.name, target: (a.recipients || []).join(', ') }));

        if (rows.length === 0) {
            return `
                <div class="alert-detail-block">
                    <span class="alert-detail-label">Actions</span>
                    <p class="alert-detail-empty">No actions attached. This alert records triggers but notifies nothing.</p>
                </div>
            `;
        }

        return `
            <div class="alert-detail-block">
                <span class="alert-detail-label">Actions</span>
                <ul class="alert-action-list">
                    ${rows.map(r => `
                        <li class="alert-action-item">
                            <span class="alert-action-kind alert-action-kind-${r.kind.toLowerCase()}">${r.kind}</span>
                            <span class="alert-action-name">${Utils.escapeHtml(r.name || 'unnamed')}</span>
                            ${r.target ? `<span class="alert-action-target" title="${Utils.escapeHtml(r.target)}">${Utils.escapeHtml(r.target)}</span>` : ''}
                        </li>
                    `).join('')}
                </ul>
            </div>
        `;
    },

    bindCopy(panel, alert) {
        const btn = panel?.querySelector('.alert-query-copy');
        if (!btn) return;
        btn.onclick = () => {
            navigator.clipboard.writeText(alert.query_string || '').then(() => {
                btn.classList.add('copied');
                setTimeout(() => btn.classList.remove('copied'), 1200);
            }).catch(() => Toast.show('Copy failed', 'error'));
        };
    },

    // ---- Activity sparkline ----

    // Guarded by a token so a slow response for a previously selected alert
    // cannot paint over the panel after the user has moved on.
    async loadActivity(panel, alertId) {
        const token = ++this._activityToken;
        try {
            const res = await fetch(`/api/v1/alerts/${alertId}/activity`, { credentials: 'include' });
            const data = await res.json();
            if (token !== this._activityToken) return;
            if (!data.success) throw new Error(data.error || 'failed');
            this.renderSparkline(panel, data.data);
        } catch (err) {
            if (token !== this._activityToken) return;
            const body = panel?.querySelector('.alert-activity-body');
            if (body) body.innerHTML = '<div class="alert-activity-loading">Activity unavailable</div>';
        }
    },

    renderSparkline(panel, activity) {
        const body = panel?.querySelector('.alert-activity-body');
        const readout = panel?.querySelector('.alert-activity-readout');
        if (!body) return;

        const days = activity?.days || [];
        const total = activity?.total_executions || 0;
        const max = days.reduce((m, d) => Math.max(m, d.executions), 0);

        // A silent alert draws a flatline rather than swapping in a text block,
        // so the panel keeps its shape whether or not the alert has ever fired.
        const bars = days.map((d, i) => {
            // Linear scale keeps a spike looking like a spike; the 3% floor
            // stops a quiet day from being indistinguishable from a silent one.
            const pct = d.executions > 0 ? Math.max(3, (d.executions / max) * 100) : 0;
            return `<div class="alert-spark-slot" data-i="${i}"><div class="alert-spark-bar${d.executions === 0 ? ' is-zero' : ''}" style="height:${pct}%"></div></div>`;
        }).join('');

        const summary = total > 0 ? `${total.toLocaleString()} triggers` : 'Never triggered';

        body.innerHTML = `
            <div class="alert-spark${total === 0 ? ' is-silent' : ''}">${bars}</div>
            <div class="alert-spark-axis">
                <span>${activity.window_days}d ago</span>
                <span>${total > 0 ? `peak ${max.toLocaleString()}/day` : 'no activity'}</span>
                <span>today</span>
            </div>
        `;
        if (readout) readout.textContent = summary;

        const spark = body.querySelector('.alert-spark');
        if (!spark || !readout) return;
        spark.addEventListener('mousemove', (e) => {
            const slot = e.target.closest('.alert-spark-slot');
            if (!slot) return;
            const d = days[Number(slot.dataset.i)];
            if (!d) return;
            readout.textContent = `${this.formatBucketDate(d.date)} - ${d.executions.toLocaleString()} triggers, ${d.logs.toLocaleString()} logs`;
        });
        spark.addEventListener('mouseleave', () => { readout.textContent = summary; });
    },

    // Buckets are server-side calendar days, so they are formatted as written
    // rather than passed through TZ, which would shift them a day.
    formatBucketDate(iso) {
        const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || '');
        if (!m) return iso || '';
        return `${TZ.MONTHS[Number(m[2]) - 1]} ${Number(m[3])}`;
    },

    // ---- Geometry ----

    width() {
        const stored = parseInt(localStorage.getItem(this.WIDTH_KEY), 10);
        if (Number.isFinite(stored)) {
            return Math.min(this.WIDTH_MAX, Math.max(this.WIDTH_MIN, stored));
        }
        return this.WIDTH_DEFAULT;
    },

    applyWidth(panel, px) {
        if (!panel) return;
        // A custom property rather than an inline width, so the narrow-viewport
        // rule can still take the panel full-bleed without fighting a style attr.
        panel.style.setProperty('--alert-panel-w', `${px != null ? px : this.width()}px`);
    },

    setupResize(panel) {
        const handle = panel?.querySelector('.alert-details-resize');
        if (!handle || handle.dataset.bound) return;
        handle.dataset.bound = '1';

        let current = this.width();
        const onMove = (e) => {
            current = Math.min(this.WIDTH_MAX, Math.max(this.WIDTH_MIN, window.innerWidth - e.clientX));
            this.applyWidth(panel, current);
        };
        const onUp = () => {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.classList.remove('alert-panel-resizing');
            localStorage.setItem(this.WIDTH_KEY, String(current));
        };
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            document.body.classList.add('alert-panel-resizing');
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        });
    },

    // The app header is in normal flow rather than fixed, so the panel's top
    // edge tracks the header's live bottom and clamps to 0 once it scrolls away.
    startInset(panel) {
        this._insetPanel = panel;
        if (this._insetHandler) {
            this._insetHandler();
            return;
        }
        const update = () => {
            this._insetFrame = null;
            const p = this._insetPanel;
            const header = document.querySelector('.header');
            if (!p || !header) return;
            p.style.top = `${Math.max(0, Math.round(header.getBoundingClientRect().bottom))}px`;
        };
        this._insetHandler = () => {
            if (this._insetFrame) return;
            this._insetFrame = requestAnimationFrame(update);
        };
        window.addEventListener('scroll', this._insetHandler, { passive: true });
        window.addEventListener('resize', this._insetHandler);
        update();
    },

    stopInset() {
        this._insetPanel = null;
        if (!this._insetHandler) return;
        window.removeEventListener('scroll', this._insetHandler);
        window.removeEventListener('resize', this._insetHandler);
        if (this._insetFrame) cancelAnimationFrame(this._insetFrame);
        this._insetFrame = null;
        this._insetHandler = null;
    },

    // ---- Selection and keyboard ----

    markSelectedRow(alertId, scope) {
        const root = scope || document;
        root.querySelectorAll('.alert-row.selected').forEach(r => r.classList.remove('selected'));
        if (!alertId) return;
        const row = root.querySelector(`.alert-row[data-alert-id="${CSS.escape(alertId)}"]`);
        if (!row) return;
        row.classList.add('selected');
        row.scrollIntoView({ block: 'nearest' });
    },

    // Escape closes; up/down walk the list with the panel following.
    bindKeys({ onClose, onMove }) {
        this.unbindKeys();
        this._keyHandler = (e) => {
            if (e.key === 'Escape') {
                onClose();
                return;
            }
            if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
            if (e.altKey || e.ctrlKey || e.metaKey) return;

            const t = e.target;
            const tag = t?.tagName;
            if (t?.isContentEditable || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

            e.preventDefault();
            onMove(e.key === 'ArrowDown' ? 1 : -1);
        };
        document.addEventListener('keydown', this._keyHandler);
    },

    unbindKeys() {
        if (!this._keyHandler) return;
        document.removeEventListener('keydown', this._keyHandler);
        this._keyHandler = null;
    },

    // ---- Formatting shared by both panels ----

    formatWindow(seconds) {
        if (!seconds) return '';
        if (seconds % 86400 === 0) return `${seconds / 86400}d`;
        if (seconds % 3600 === 0) return `${seconds / 3600}h`;
        return `${Math.round(seconds / 60)}m`;
    },

    formatThrottle(seconds) {
        if (seconds < 60) return `${seconds}s`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
        if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
        return `${Math.floor(seconds / 86400)}d`;
    },

    formatCron(cronExpr) {
        const presets = {
            '0 * * * *': 'hourly',
            '0 0 * * *': 'daily',
            '0 0 * * 1': 'weekly',
            '0 0 1 * *': 'monthly'
        };
        return presets[cronExpr] || cronExpr;
    }
};

window.AlertDetail = AlertDetail;

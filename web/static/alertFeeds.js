// Alert Feeds module - manages feed-sourced alerts and feed configuration
const AlertFeeds = {
    feeds: [],
    feedAlerts: [],
    filteredAlerts: [],
    currentSubTab: 'feed-alerts',
    alertsPage: 1,
    alertsPerPage: 25,
    sortColumn: null,   // current sort column key
    sortDirection: null, // 'asc' or 'desc'

    // Known severity levels in priority order (used for extraction from labels)
    severityLevels: ['critical', 'high', 'medium', 'low', 'informational'],
    // Numeric severity for sorting (higher = more severe)
    severityRank: { critical: 5, high: 4, medium: 3, low: 2, informational: 1 },

    init() {
        this.setupEventListeners();
    },

    showManualAlerts() {
        this.closeDetailsPanel();
        document.querySelectorAll('.alerts-sub-tab').forEach(b => b.classList.remove('active'));
        document.querySelector('.alerts-sub-tab[data-subtab="manual"]')?.classList.add('active');

        const alertsView = document.getElementById('alertsView');
        const feedAlertsView = document.getElementById('feedAlertsView');
        const alertEditorView = document.getElementById('alertEditorView');
        const actionsManageView = document.getElementById('actionsManageView');
        if (alertsView) alertsView.style.display = 'block';
        if (feedAlertsView) feedAlertsView.style.display = 'none';
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (actionsManageView) actionsManageView.style.display = 'none';

        if (window.Alerts) {
            Alerts.closeAlertPanel();
            Alerts.editingFeedAlert = false;
            Alerts.show();
        }
    },

    showFeedAlertsTab() {
        document.querySelectorAll('.alerts-sub-tab').forEach(b => b.classList.remove('active'));
        document.querySelector('.alerts-sub-tab[data-subtab="feeds"]')?.classList.add('active');

        const alertsView = document.getElementById('alertsView');
        const feedAlertsView = document.getElementById('feedAlertsView');
        const alertEditorView = document.getElementById('alertEditorView');
        const actionsManageView = document.getElementById('actionsManageView');
        if (alertsView) alertsView.style.display = 'none';
        if (feedAlertsView) feedAlertsView.style.display = 'block';
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (actionsManageView) actionsManageView.style.display = 'none';

        if (window.Alerts) {
            Alerts.closeAlertPanel();
            Alerts.editingFeedAlert = false;
        }

        this.show();
    },

    setupEventListeners() {
        // Sub-tab switching is handled via onclick attributes in the HTML
    },

    async show() {
        this.closeDetailsPanel();
        await this.loadFeeds();
        if (this.currentSubTab === 'feed-alerts') {
            await this.loadFeedAlerts();
        } else {
            this.renderFeedsManagement();
        }
    },

    switchSubTab(tab) {
        this.closeDetailsPanel();
        this.currentSubTab = tab;
        document.querySelectorAll('.feed-sub-tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll(`.feed-sub-tab[data-tab="${tab}"]`).forEach(b => b.classList.add('active'));

        const alertsView = document.getElementById('feedAlertsListView');
        const manageView = document.getElementById('feedManageView');

        if (tab === 'feed-alerts') {
            if (alertsView) alertsView.style.display = 'block';
            if (manageView) manageView.style.display = 'none';
            this.loadFeedAlerts();
        } else {
            if (alertsView) alertsView.style.display = 'none';
            if (manageView) manageView.style.display = 'block';
            this.renderFeedsManagement();
        }
    },

    // ============================
    // Label / Severity helpers
    // ============================

    // Extract severity from an alert's labels (e.g. "sigma:high" -> "high")
    getAlertSeverity(alert) {
        if (!alert.labels) return '';
        for (const l of alert.labels) {
            if (l.startsWith('sigma:')) {
                const val = l.substring(6).toLowerCase();
                if (this.severityLevels.includes(val)) return val;
            }
        }
        return '';
    },

    // Get display labels (exclude the synthetic "feed:" prefix and "sigma:level" labels shown as severity)
    getDisplayLabels(alert) {
        if (!alert.labels) return [];
        return alert.labels.filter(l => {
            if (l.startsWith('feed:')) return false;
            if (l.startsWith('sigma:') && this.severityLevels.includes(l.substring(6).toLowerCase())) return false;
            return true;
        });
    },

    // Build sorted unique label list from all feed alerts (for the filter dropdown)
    buildLabelOptions() {
        const labels = new Set();
        for (const a of this.feedAlerts) {
            for (const l of this.getDisplayLabels(a)) {
                labels.add(l);
            }
        }
        return [...labels].sort();
    },

    // ============================
    // Feed CRUD
    // ============================

    async loadFeeds() {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/feeds');
            this.feeds = data.data || [];
        } catch (err) {
            console.error('[AlertFeeds] Failed to load feeds:', err);
            this.feeds = [];
        }
    },

    async loadFeedAlerts() {
        const container = document.getElementById('feedAlertsList');
        if (!container) return;

        container.innerHTML = '<div class="loading">Loading feed alerts...</div>';

        try {
            const data = await HttpUtils.safeFetch('/api/v1/alerts/feed');
            this.feedAlerts = data.data || [];
            this.filteredAlerts = this.feedAlerts;
            this.alertsPage = 1;
            this.populateLabelFilter();
            this.filterFeedAlerts();
        } catch (err) {
            console.error('[AlertFeeds] Failed to load feed alerts:', err);
            container.innerHTML = '<div class="error">Failed to load feed alerts: ' + Utils.escapeHtml(err.message) + '</div>';
        }
    },

    // ============================
    // Filtering
    // ============================

    filterFeedAlerts() {
        const search = (document.getElementById('feedAlertSearch')?.value || '').toLowerCase();
        const statusFilter = document.getElementById('feedAlertStatusFilter')?.value || 'all';
        const feedFilter = document.getElementById('feedAlertFeedFilter')?.value || 'all';
        const severityFilter = document.getElementById('feedAlertSeverityFilter')?.value || 'all';
        const labelFilter = document.getElementById('feedAlertLabelFilter')?.value || 'all';

        this.filteredAlerts = this.feedAlerts.filter(a => {
            // Text search
            if (search && !a.name.toLowerCase().includes(search)
                && !(a.feed_rule_path || '').toLowerCase().includes(search)
                && !(a.description || '').toLowerCase().includes(search)
                && !(a.query_string || '').toLowerCase().includes(search)) return false;

            // Status
            if (statusFilter === 'enabled' && !a.enabled) return false;
            if (statusFilter === 'disabled' && a.enabled) return false;

            // Feed
            if (feedFilter !== 'all' && a.feed_id !== feedFilter) return false;

            // Severity
            if (severityFilter !== 'all') {
                const sev = this.getAlertSeverity(a);
                if (sev !== severityFilter) return false;
            }

            // Label
            if (labelFilter !== 'all') {
                const labels = this.getDisplayLabels(a);
                if (!labels.includes(labelFilter)) return false;
            }

            return true;
        });

        this.alertsPage = 1;
        this.applySorting();
        this.populateFeedFilter();
        this.renderFeedAlerts();
        this.updateBulkButtons();
    },

    // ============================
    // Sorting
    // ============================

    toggleSort(column) {
        if (this.sortColumn === column) {
            // Cycle: asc -> desc -> none
            if (this.sortDirection === 'asc') {
                this.sortDirection = 'desc';
            } else {
                this.sortColumn = null;
                this.sortDirection = null;
            }
        } else {
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }
        this.applySorting();
        this.renderFeedAlerts();
    },

    applySorting() {
        if (!this.sortColumn) return;

        const col = this.sortColumn;
        const dir = this.sortDirection === 'asc' ? 1 : -1;

        this.filteredAlerts.sort((a, b) => {
            let va, vb;
            switch (col) {
                case 'name':
                    va = a.name.toLowerCase();
                    vb = b.name.toLowerCase();
                    return va < vb ? -dir : va > vb ? dir : 0;
                case 'severity':
                    va = this.severityRank[this.getAlertSeverity(a)] || 0;
                    vb = this.severityRank[this.getAlertSeverity(b)] || 0;
                    return (va - vb) * dir;
                case 'exec_time':
                    va = a.last_execution_time_ms ?? -1;
                    vb = b.last_execution_time_ms ?? -1;
                    return (va - vb) * dir;
                case 'last_triggered':
                    va = a.last_triggered ? new Date(a.last_triggered).getTime() : 0;
                    vb = b.last_triggered ? new Date(b.last_triggered).getTime() : 0;
                    return (va - vb) * dir;
                default:
                    return 0;
            }
        });
    },

    sortIndicator(column) {
        if (this.sortColumn !== column) return '';
        return this.sortDirection === 'asc' ? ' &#9650;' : ' &#9660;';
    },

    // Set a label filter programmatically (called when clicking a label pill)
    setLabelFilter(label) {
        const select = document.getElementById('feedAlertLabelFilter');
        if (select) {
            // Ensure the option exists
            let found = false;
            for (const opt of select.options) {
                if (opt.value === label) { found = true; break; }
            }
            if (!found) {
                const opt = document.createElement('option');
                opt.value = label;
                opt.textContent = label;
                select.appendChild(opt);
            }
            select.value = label;
        }
        this.filterFeedAlerts();
    },

    // Set severity filter programmatically (called when clicking a severity badge)
    setSeverityFilter(level) {
        const select = document.getElementById('feedAlertSeverityFilter');
        if (select) select.value = level;
        this.filterFeedAlerts();
    },

    // Show/hide bulk buttons when any filter is active
    updateBulkButtons() {
        const search = (document.getElementById('feedAlertSearch')?.value || '').trim();
        const statusFilter = document.getElementById('feedAlertStatusFilter')?.value || 'all';
        const feedFilter = document.getElementById('feedAlertFeedFilter')?.value || 'all';
        const severityFilter = document.getElementById('feedAlertSeverityFilter')?.value || 'all';
        const labelFilter = document.getElementById('feedAlertLabelFilter')?.value || 'all';

        const hasFilter = search || statusFilter !== 'all' || feedFilter !== 'all'
            || severityFilter !== 'all' || labelFilter !== 'all';

        const enableBtn = document.getElementById('feedBulkEnableBtn');
        const disableBtn = document.getElementById('feedBulkDisableBtn');
        if (enableBtn) enableBtn.style.display = hasFilter ? '' : 'none';
        if (disableBtn) disableBtn.style.display = hasFilter ? '' : 'none';
    },

    // ============================
    // Filter dropdown population
    // ============================

    populateFeedFilter() {
        const select = document.getElementById('feedAlertFeedFilter');
        if (!select) return;

        const currentVal = select.value;
        const feedIds = new Set(this.feedAlerts.map(a => a.feed_id));

        if (select.dataset.feedCount === String(feedIds.size)) return;
        select.dataset.feedCount = String(feedIds.size);

        let html = '<option value="all">All Feeds</option>';
        for (const feed of this.feeds) {
            if (feedIds.has(feed.id)) {
                html += `<option value="${feed.id}">${Utils.escapeHtml(feed.name)}</option>`;
            }
        }
        select.innerHTML = html;
        select.value = currentVal || 'all';
    },

    populateLabelFilter() {
        const select = document.getElementById('feedAlertLabelFilter');
        if (!select) return;

        const currentVal = select.value;
        const labels = this.buildLabelOptions();

        let html = '<option value="all">All Labels</option>';
        for (const label of labels) {
            html += `<option value="${Utils.escapeHtml(label)}">${Utils.escapeHtml(label)}</option>`;
        }
        select.innerHTML = html;
        select.value = currentVal || 'all';
    },

    // ============================
    // Rendering
    // ============================

    renderFeedAlerts() {
        const container = document.getElementById('feedAlertsList');
        if (!container) return;

        if (this.filteredAlerts.length === 0) {
            container.innerHTML = `
                <div class="alerts-table-container">
                    <div class="empty-state">
                        <p>No feed alerts found.</p>
                        <p class="empty-hint">Configure feeds in the "Manage Feeds" tab to sync alerts from git repositories.</p>
                    </div>
                </div>`;
            return;
        }

        const totalPages = Math.max(1, Math.ceil(this.filteredAlerts.length / this.alertsPerPage));
        if (this.alertsPage > totalPages) this.alertsPage = totalPages;
        const start = (this.alertsPage - 1) * this.alertsPerPage;
        const pageAlerts = this.filteredAlerts.slice(start, start + this.alertsPerPage);

        // Build feed name lookup
        const feedNames = {};
        for (const f of this.feeds) feedNames[f.id] = f.name;

        let html = `
            <div class="alerts-table-container">
                <div class="alerts-table-header">
                    <div class="alerts-count">
                        Showing ${pageAlerts.length} of ${this.filteredAlerts.length} alerts
                        ${this.filteredAlerts.length !== this.feedAlerts.length ? ` (filtered from ${this.feedAlerts.length} total)` : ''}
                    </div>
                    <div class="alerts-page-size">
                        <label>Show:</label>
                        <select onchange="AlertFeeds.changePageSize(this.value)">
                            <option value="10" ${this.alertsPerPage === 10 ? 'selected' : ''}>10</option>
                            <option value="25" ${this.alertsPerPage === 25 ? 'selected' : ''}>25</option>
                            <option value="50" ${this.alertsPerPage === 50 ? 'selected' : ''}>50</option>
                            <option value="100" ${this.alertsPerPage === 100 ? 'selected' : ''}>100</option>
                        </select>
                    </div>
                </div>
                <table class="alerts-table">
                    <thead>
                        <tr>
                            <th class="sortable-th" onclick="AlertFeeds.toggleSort('name')">Name${this.sortIndicator('name')}</th>
                            <th>Feed</th>
                            <th class="sortable-th" onclick="AlertFeeds.toggleSort('severity')">Severity${this.sortIndicator('severity')}</th>
                            <th>Labels</th>
                            <th class="sortable-th" onclick="AlertFeeds.toggleSort('exec_time')">Exec Time${this.sortIndicator('exec_time')}</th>
                            <th class="sortable-th" onclick="AlertFeeds.toggleSort('last_triggered')">Last Triggered${this.sortIndicator('last_triggered')}</th>
                        </tr>
                    </thead>
                    <tbody>`;

        for (const alert of pageAlerts) {
            const isAutoDisabled = !alert.enabled && alert.disabled_reason;
            const statusClass = isAutoDisabled ? 'auto-disabled' : (alert.enabled ? 'enabled' : 'disabled');
            const statusText = isAutoDisabled ? 'Auto-disabled' : (alert.enabled ? 'Enabled' : 'Disabled');
            const lastTriggered = alert.last_triggered
                ? new Date(alert.last_triggered).toLocaleString()
                : 'Never';
            const lastRunTime = alert.last_execution_time_ms != null
                ? (alert.last_execution_time_ms >= 1000
                    ? `${(alert.last_execution_time_ms / 1000).toFixed(1)}s`
                    : `${alert.last_execution_time_ms}ms`)
                : '-';
            const runTimeClass = alert.last_execution_time_ms != null && alert.last_execution_time_ms >= 3000
                ? 'alert-run-time-slow' : '';
            const feedName = feedNames[alert.feed_id] || alert.feed_name || 'Unknown';
            const severity = this.getAlertSeverity(alert);
            const displayLabels = this.getDisplayLabels(alert);

            // Severity badge
            const severityHtml = severity
                ? `<span class="severity-pill severity-${severity}" onclick="event.stopPropagation(); AlertFeeds.setSeverityFilter('${severity}')" title="Filter by ${severity}">${severity}</span>`
                : '<span class="text-muted">-</span>';

            // Label pills (show max 3, with overflow count)
            const maxLabels = 3;
            let labelsHtml = '';
            if (displayLabels.length > 0) {
                const shown = displayLabels.slice(0, maxLabels);
                labelsHtml = shown.map(l =>
                    `<span class="label-pill" onclick="event.stopPropagation(); AlertFeeds.setLabelFilter('${Utils.escapeHtml(l).replace(/'/g, "\\'")}')" title="${Utils.escapeHtml(l)}">${Utils.escapeHtml(this.truncateLabel(l))}</span>`
                ).join('');
                if (displayLabels.length > maxLabels) {
                    labelsHtml += `<span class="label-pill label-pill-more" title="${Utils.escapeHtml(displayLabels.slice(maxLabels).join(', '))}">+${displayLabels.length - maxLabels}</span>`;
                }
            } else {
                labelsHtml = '<span class="text-muted">-</span>';
            }

            html += `
                        <tr class="alert-row ${statusClass}" data-alert-id="${alert.id}">
                            <td class="alert-name">
                                <div class="alert-name-row">
                                    <span class="status-dot status-${statusClass}" title="${statusText}"></span>
                                    <strong>${Utils.escapeHtml(alert.name)}</strong>
                                    ${isAutoDisabled ? '<span class="alert-auto-disabled-badge">timeout</span>' : ''}
                                </div>
                                ${alert.description ? `<div class="alert-description-preview">${Utils.escapeHtml(alert.description.substring(0, 60))}${alert.description.length > 60 ? '...' : ''}</div>` : ''}
                            </td>
                            <td><span class="feed-badge-sm">${Utils.escapeHtml(feedName)}</span></td>
                            <td class="alert-severity-cell">${severityHtml}</td>
                            <td class="alert-labels-cell">${labelsHtml}</td>
                            <td class="alert-run-time ${runTimeClass}">${lastRunTime}</td>
                            <td class="alert-triggered">${lastTriggered}</td>
                        </tr>`;
        }

        html += `
                    </tbody>
                </table>
                ${this.renderPagination()}
            </div>`;

        container.innerHTML = html;
        this.addRowClickHandlers();
    },

    truncateLabel(label) {
        return label.length > 24 ? label.substring(0, 22) + '..' : label;
    },

    addRowClickHandlers() {
        const rows = document.querySelectorAll('#feedAlertsList .alert-row');
        rows.forEach(row => {
            row.addEventListener('click', () => {
                const alertId = row.dataset.alertId;
                if (alertId) this.viewFeedAlert(alertId);
            });
        });
    },

    renderPagination() {
        const total = this.filteredAlerts.length;
        const totalPages = Math.max(1, Math.ceil(total / this.alertsPerPage));

        return `
            <div class="alerts-pagination">
                <button class="btn-secondary btn-sm" ${this.alertsPage <= 1 ? 'disabled' : ''} onclick="AlertFeeds.feedAlertsPrevPage()">Previous</button>
                <span class="pagination-info">Page ${this.alertsPage} of ${totalPages} (${total} alerts)</span>
                <button class="btn-secondary btn-sm" ${this.alertsPage >= totalPages ? 'disabled' : ''} onclick="AlertFeeds.feedAlertsNextPage()">Next</button>
            </div>`;
    },

    changePageSize(size) {
        this.alertsPerPage = parseInt(size, 10) || 25;
        this.alertsPage = 1;
        this.renderFeedAlerts();
    },

    feedAlertsPrevPage() {
        if (this.alertsPage > 1) {
            this.alertsPage--;
            this.renderFeedAlerts();
        }
    },

    feedAlertsNextPage() {
        const totalPages = Math.ceil(this.filteredAlerts.length / this.alertsPerPage);
        if (this.alertsPage < totalPages) {
            this.alertsPage++;
            this.renderFeedAlerts();
        }
    },

    // ============================
    // Single alert toggle
    // ============================

    async toggleFeedAlert(alertId, enabled) {
        try {
            await HttpUtils.safeFetch(`/api/v1/alerts/${alertId}/toggle-feed`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled })
            });
        } catch (err) {
            console.error('[AlertFeeds] Toggle failed:', err);
            Toast.show('Failed to toggle alert: ' + err.message, 'error');
            this.loadFeedAlerts();
        }
    },

    // ============================
    // Bulk enable/disable (filtered)
    // ============================

    async bulkEnableFiltered() {
        const ids = this.filteredAlerts.map(a => a.id);
        if (ids.length === 0) return;
        if (!confirm(`Enable ${ids.length} filtered alert${ids.length !== 1 ? 's' : ''}?`)) return;
        await this.batchToggle(ids, true);
    },

    async bulkDisableFiltered() {
        const ids = this.filteredAlerts.map(a => a.id);
        if (ids.length === 0) return;
        if (!confirm(`Disable ${ids.length} filtered alert${ids.length !== 1 ? 's' : ''}?`)) return;
        await this.batchToggle(ids, false);
    },

    async batchToggle(alertIds, enabled) {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/alerts/feed/batch-toggle', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ alert_ids: alertIds, enabled })
            });
            const count = data.data?.toggled || alertIds.length;
            Toast.show(`${count} alert${count !== 1 ? 's' : ''} ${enabled ? 'enabled' : 'disabled'}`, 'success');
            await this.loadFeedAlerts();
        } catch (err) {
            console.error('[AlertFeeds] Batch toggle failed:', err);
            Toast.show('Failed: ' + err.message, 'error');
        }
    },

    // ============================
    // Details panel
    // ============================

    viewFeedAlert(alertId) {
        const alert = this.feedAlerts.find(a => a.id === alertId);
        if (!alert) return;
        this.showDetailsPanel(alert);
    },

    showDetailsPanel(alert) {
        const panel = document.getElementById('feedAlertDetailsPanel');
        const title = document.getElementById('feedAlertDetailsTitle');
        const content = document.getElementById('feedAlertDetailsContent');
        if (!panel || !title || !content) return;

        title.textContent = alert.name;
        this.currentDetailAlert = alert;

        const feedNames = {};
        for (const f of this.feeds) feedNames[f.id] = f.name;
        const feedName = feedNames[alert.feed_id] || 'Unknown';

        const isAutoDisabled = !alert.enabled && alert.disabled_reason;
        const statusClass = isAutoDisabled ? 'auto-disabled' : (alert.enabled ? 'enabled' : 'disabled');
        const statusText = isAutoDisabled ? 'Auto-disabled' : (alert.enabled ? 'Enabled' : 'Disabled');
        const lastTriggered = alert.last_triggered
            ? new Date(alert.last_triggered).toLocaleString()
            : 'Never';

        const severity = this.getAlertSeverity(alert);
        const displayLabels = this.getDisplayLabels(alert);

        content.innerHTML = `
            <div class="alert-details-section">
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

                <div class="alert-detail-field">
                    <label>Status:</label>
                    <span class="status-badge status-${statusClass}">${statusText}</span>
                </div>

                <div class="alert-detail-field">
                    <label>Feed:</label>
                    <span class="feed-badge-sm">${Utils.escapeHtml(feedName)}</span>
                </div>

                ${severity ? `
                    <div class="alert-detail-field">
                        <label>Severity:</label>
                        <span class="severity-pill severity-${severity}">${severity}</span>
                    </div>
                ` : ''}

                <div class="alert-detail-field">
                    <label>Query:</label>
                    <pre class="alert-query-display"><code>${Utils.escapeHtml(alert.query_string)}</code></pre>
                </div>

                ${alert.description ? `
                    <div class="alert-detail-field">
                        <label>Description:</label>
                        <p>${Utils.escapeHtml(alert.description)}</p>
                    </div>
                ` : ''}

                ${displayLabels.length > 0 ? `
                    <div class="alert-detail-field">
                        <label>Labels:</label>
                        <div class="alert-labels">
                            ${displayLabels.map(l => `<span class="label-pill label-pill-detail" onclick="AlertFeeds.closeDetailsPanel(); AlertFeeds.setLabelFilter('${Utils.escapeHtml(l).replace(/'/g, "\\'")}')" title="Filter by this label">${Utils.escapeHtml(l)}</span>`).join('')}
                        </div>
                    </div>
                ` : ''}

                ${alert.references && alert.references.length > 0 ? `
                    <div class="alert-detail-field">
                        <label>References:</label>
                        <ul class="alert-references">
                            ${alert.references.map(ref => `<li><a href="${Utils.escapeHtml(ref)}" target="_blank" rel="noopener noreferrer" class="alert-reference-link">${Utils.escapeHtml(ref)}</a></li>`).join('')}
                        </ul>
                    </div>
                ` : ''}

                ${alert.feed_rule_path ? `
                    <div class="alert-detail-field">
                        <label>Rule Path:</label>
                        <span style="font-family: var(--font-mono); font-size: 0.8rem;">${Utils.escapeHtml(alert.feed_rule_path)}</span>
                    </div>
                ` : ''}

                <div class="alert-detail-field">
                    <label>Created:</label>
                    <span>${new Date(alert.created_at).toLocaleString()}</span>
                </div>

                <div class="alert-detail-field">
                    <label>Last Triggered:</label>
                    <span>${lastTriggered}</span>
                </div>
            </div>

            <div class="alert-details-actions">
                <button onclick="AlertFeeds.editFromPanel()" class="btn-primary">
                    Edit Alert
                </button>
                <button onclick="AlertFeeds.toggleCurrentAlert()" class="btn-secondary">
                    ${alert.enabled ? 'Disable' : 'Enable'}
                </button>
            </div>
        `;

        panel.classList.add('open');

        // Close on Escape
        this._detailEscHandler = (e) => {
            if (e.key === 'Escape') this.closeDetailsPanel();
        };
        document.addEventListener('keydown', this._detailEscHandler);
    },

    closeDetailsPanel() {
        const panel = document.getElementById('feedAlertDetailsPanel');
        if (panel) panel.classList.remove('open');
        this.currentDetailAlert = null;
        if (this._detailEscHandler) {
            document.removeEventListener('keydown', this._detailEscHandler);
            this._detailEscHandler = null;
        }
    },

    editFromPanel() {
        const alert = this.currentDetailAlert;
        if (!alert) return;
        this.closeDetailsPanel();

        const feedAlertsView = document.getElementById('feedAlertsView');
        if (feedAlertsView) feedAlertsView.style.display = 'none';

        if (window.Alerts) {
            Alerts.showAlertEditor(alert.id, { fromFeed: true });
        }
    },

    async toggleCurrentAlert() {
        const alert = this.currentDetailAlert;
        if (!alert) return;
        const newState = !alert.enabled;
        await this.toggleFeedAlert(alert.id, newState);
        alert.enabled = newState;
        this.showDetailsPanel(alert);
        this.renderFeedAlerts();
    },

    // ============================
    // Feeds Management
    // ============================

    renderFeedsManagement() {
        const container = document.getElementById('feedManageList');
        if (!container) return;

        if (this.feeds.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <p>No feeds configured.</p>
                    <p class="empty-hint">Create a feed to sync alerts from a git repository.</p>
                </div>`;
            return;
        }

        let html = '<div class="feeds-table-container"><table class="alerts-table feeds-table"><thead><tr>';
        html += '<th>Name</th><th>Repository</th><th>Schedule</th><th>Last Sync</th><th>Rules</th><th>Status</th><th>Actions</th>';
        html += '</tr></thead><tbody>';

        for (const feed of this.feeds) {
            const syncStatus = this.getSyncStatusBadge(feed);
            const lastSync = feed.last_synced_at ? this.timeAgo(new Date(feed.last_synced_at)) : 'Never';
            const repoDisplay = this.formatRepoUrl(feed.repo_url);

            html += `<tr class="feed-row" data-feed-id="${feed.id}">
                <td>
                    <div class="feed-name-cell">
                        <span class="feed-name">${Utils.escapeHtml(feed.name)}</span>
                        ${feed.min_level ? `<span class="feed-min-level-badge">&ge; ${Utils.escapeHtml(feed.min_level)}</span>` : ''}
                        ${feed.min_status ? `<span class="feed-min-level-badge">&ge; ${Utils.escapeHtml(feed.min_status)}</span>` : ''}
                        ${feed.description ? `<span class="feed-desc">${Utils.escapeHtml(feed.description)}</span>` : ''}
                    </div>
                </td>
                <td class="feed-repo" title="${Utils.escapeHtml(feed.repo_url)}">
                    <code>${Utils.escapeHtml(repoDisplay)}</code>
                    ${feed.path ? `<span class="feed-path-badge" title="Path: ${Utils.escapeHtml(feed.path)}">${Utils.escapeHtml(feed.path)}</span>` : ''}
                </td>
                <td><span class="schedule-badge schedule-${feed.sync_schedule}">${feed.sync_schedule}</span></td>
                <td class="feed-last-sync">${lastSync}</td>
                <td>${feed.last_sync_rule_count || 0}</td>
                <td>${syncStatus}</td>
                <td class="feed-actions-cell">
                    <button class="btn-icon" title="Sync Now" onclick="AlertFeeds.syncFeed('${feed.id}')">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M2.5 22v-6h6"/><path d="M2.5 12a10 10 0 0 1 16.5-5.5L21.5 8M21.5 12a10 10 0 0 1-16.5 5.5L2.5 16"/></svg>
                    </button>
                    <button class="btn-icon" title="Enable All Alerts" onclick="AlertFeeds.toggleAllFeedAlerts('${feed.id}', true)">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
                    </button>
                    <button class="btn-icon" title="Disable All Alerts" onclick="AlertFeeds.toggleAllFeedAlerts('${feed.id}', false)">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18.36 6.64A9 9 0 0 1 20.77 15M2 12a10 10 0 0 0 18.36 3.64"/><line x1="1" y1="1" x2="23" y2="23"/></svg>
                    </button>
                    <button class="btn-icon" title="Edit Feed" onclick="AlertFeeds.editFeed('${feed.id}')">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.12 2.12 0 0 1 3 3L12 15l-4 1 1-4Z"/></svg>
                    </button>
                    <button class="btn-icon btn-danger" title="Delete Feed" onclick="AlertFeeds.deleteFeed('${feed.id}')">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m3 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6h14ZM10 11v6M14 11v6"/></svg>
                    </button>
                </td>
            </tr>`;
        }

        html += '</tbody></table></div>';
        container.innerHTML = html;
    },

    getSyncStatusBadge(feed) {
        if (!feed.enabled) {
            return '<span class="sync-status sync-disabled">Disabled</span>';
        }
        if (!feed.last_synced_at) {
            return '<span class="sync-status sync-pending">Pending</span>';
        }
        if (feed.last_sync_status === 'success') {
            return '<span class="sync-status sync-success">OK</span>';
        }
        return `<span class="sync-status sync-error" title="${Utils.escapeHtml(feed.last_sync_status || 'Error')}">Error</span>`;
    },

    // Feed Form
    showCreateFeedForm() {
        this.showFeedForm(null);
    },

    editFeed(feedId) {
        const feed = this.feeds.find(f => f.id === feedId);
        if (feed) this.showFeedForm(feed);
    },

    async showFeedForm(feed) {
        const container = document.getElementById('feedFormContainer');
        if (!container) return;

        let normalizers = [];
        try {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers');
            normalizers = data.data?.normalizers || [];
        } catch (e) { /* ignore */ }

        const isEdit = !!feed;
        const title = isEdit ? 'Edit Feed' : 'Create Feed';

        container.innerHTML = `
            <div class="feed-form-panel">
                <div class="feed-form-header">
                    <h3>${title}</h3>
                    <button class="btn-secondary btn-sm" onclick="AlertFeeds.closeFeedForm()">Cancel</button>
                </div>
                <div class="feed-form-body">
                    <div class="form-row-2col">
                        <div class="form-group">
                            <label for="feedFormName">Name *</label>
                            <input type="text" id="feedFormName" placeholder="e.g. SigmaHQ Windows Rules" value="${Utils.escapeHtml(feed?.name || '')}">
                        </div>
                        <div class="form-group">
                            <label for="feedFormSchedule">Sync Schedule</label>
                            <select id="feedFormSchedule">
                                <option value="never" ${feed?.sync_schedule === 'never' ? 'selected' : ''}>Never</option>
                                <option value="hourly" ${feed?.sync_schedule === 'hourly' ? 'selected' : ''}>Hourly</option>
                                <option value="daily" ${(!feed || feed?.sync_schedule === 'daily') ? 'selected' : ''}>Daily</option>
                                <option value="weekly" ${feed?.sync_schedule === 'weekly' ? 'selected' : ''}>Weekly</option>
                                <option value="monthly" ${feed?.sync_schedule === 'monthly' ? 'selected' : ''}>Monthly</option>
                            </select>
                        </div>
                    </div>
                    <div class="form-group">
                        <label for="feedFormDescription">Description</label>
                        <input type="text" id="feedFormDescription" placeholder="Optional description" value="${Utils.escapeHtml(feed?.description || '')}">
                    </div>
                    <div class="form-row-2col">
                        <div class="form-group">
                            <label for="feedFormRepoURL">Repository URL *</label>
                            <input type="text" id="feedFormRepoURL" placeholder="https://github.com/owner/repo" value="${Utils.escapeHtml(feed?.repo_url || '')}">
                        </div>
                        <div class="form-group">
                            <label for="feedFormBranch">Branch</label>
                            <input type="text" id="feedFormBranch" placeholder="main" value="${Utils.escapeHtml(feed?.branch || 'main')}">
                        </div>
                    </div>
                    <div class="form-row-2col">
                        <div class="form-group">
                            <label for="feedFormPath">Path (optional)</label>
                            <input type="text" id="feedFormPath" placeholder="e.g. rules/windows (empty = whole repo)" value="${Utils.escapeHtml(feed?.path || '')}">
                        </div>
                        <div class="form-group">
                            <label for="feedFormNormalizer">Normalizer</label>
                            <select id="feedFormNormalizer">
                                <option value="">Default</option>
                                ${normalizers.map(n => `<option value="${n.id}" ${feed?.normalizer_id === n.id ? 'selected' : ''}>${Utils.escapeHtml(n.name)}</option>`).join('')}
                            </select>
                        </div>
                    </div>
                    <div class="form-row-2col">
                        <div class="form-group">
                            <label for="feedFormMinLevel">Minimum Severity</label>
                            <select id="feedFormMinLevel">
                                <option value="" ${!feed?.min_level ? 'selected' : ''}>All Levels</option>
                                <option value="informational" ${feed?.min_level === 'informational' ? 'selected' : ''}>Informational</option>
                                <option value="low" ${feed?.min_level === 'low' ? 'selected' : ''}>Low</option>
                                <option value="medium" ${feed?.min_level === 'medium' ? 'selected' : ''}>Medium</option>
                                <option value="high" ${feed?.min_level === 'high' ? 'selected' : ''}>High</option>
                                <option value="critical" ${feed?.min_level === 'critical' ? 'selected' : ''}>Critical</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="feedFormMinStatus">Minimum Status</label>
                            <select id="feedFormMinStatus">
                                <option value="" ${!feed?.min_status ? 'selected' : ''}>All Statuses</option>
                                <option value="unsupported" ${feed?.min_status === 'unsupported' ? 'selected' : ''}>Unsupported</option>
                                <option value="deprecated" ${feed?.min_status === 'deprecated' ? 'selected' : ''}>Deprecated</option>
                                <option value="experimental" ${feed?.min_status === 'experimental' ? 'selected' : ''}>Experimental</option>
                                <option value="test" ${feed?.min_status === 'test' ? 'selected' : ''}>Test</option>
                                <option value="stable" ${feed?.min_status === 'stable' ? 'selected' : ''}>Stable</option>
                            </select>
                        </div>
                    </div>
                    <div class="form-group">
                        <label for="feedFormAuthToken">Auth Token (PAT) ${isEdit && feed.has_auth_token ? '<span class="form-hint">Leave empty to keep current token</span>' : ''}</label>
                        <input type="password" id="feedFormAuthToken" placeholder="${isEdit && feed.has_auth_token ? 'Token is set (leave empty to keep)' : 'For private repos only'}" autocomplete="off">
                        ${isEdit && feed.has_auth_token ? '<label class="checkbox-label" style="margin-top:0.25rem;"><input type="checkbox" id="feedFormClearToken"> Clear token</label>' : ''}
                    </div>
                    <div class="form-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="feedFormEnabled" ${(!feed || feed.enabled) ? 'checked' : ''}>
                            Enabled
                        </label>
                    </div>
                    <div class="form-actions">
                        <button class="btn-primary" onclick="AlertFeeds.saveFeed(${isEdit ? `'${feed.id}'` : 'null'})">${isEdit ? 'Update Feed' : 'Create Feed'}</button>
                        <button class="btn-secondary" onclick="AlertFeeds.closeFeedForm()">Cancel</button>
                    </div>
                    <div id="feedFormError" class="error-message" style="display:none;"></div>
                </div>
            </div>`;

        container.style.display = 'block';
    },

    closeFeedForm() {
        const container = document.getElementById('feedFormContainer');
        if (container) {
            container.style.display = 'none';
            container.innerHTML = '';
        }
    },

    async saveFeed(feedId) {
        const name = document.getElementById('feedFormName')?.value.trim();
        const repoUrl = document.getElementById('feedFormRepoURL')?.value.trim();

        if (!name || !repoUrl) {
            const errEl = document.getElementById('feedFormError');
            if (errEl) {
                errEl.textContent = 'Name and Repository URL are required.';
                errEl.style.display = 'block';
            }
            return;
        }

        const payload = {
            name,
            description: document.getElementById('feedFormDescription')?.value.trim() || '',
            repo_url: repoUrl,
            branch: document.getElementById('feedFormBranch')?.value.trim() || 'main',
            path: document.getElementById('feedFormPath')?.value.trim() || '',
            auth_token: document.getElementById('feedFormAuthToken')?.value || '',
            normalizer_id: document.getElementById('feedFormNormalizer')?.value || '',
            sync_schedule: document.getElementById('feedFormSchedule')?.value || 'daily',
            min_level: document.getElementById('feedFormMinLevel')?.value || '',
            min_status: document.getElementById('feedFormMinStatus')?.value || '',
            enabled: document.getElementById('feedFormEnabled')?.checked ?? true,
        };

        if (feedId) {
            payload.clear_token = document.getElementById('feedFormClearToken')?.checked ?? false;
        }

        try {
            const method = feedId ? 'PUT' : 'POST';
            const url = feedId ? `/api/v1/feeds/${feedId}` : '/api/v1/feeds';
            await HttpUtils.safeFetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            Toast.show(feedId ? 'Feed updated' : 'Feed created', 'success');
            this.closeFeedForm();
            await this.loadFeeds();
            this.renderFeedsManagement();
        } catch (err) {
            console.error('[AlertFeeds] Save feed failed:', err);
            const errEl = document.getElementById('feedFormError');
            if (errEl) {
                errEl.textContent = err.message;
                errEl.style.display = 'block';
            }
        }
    },

    async deleteFeed(feedId) {
        const feed = this.feeds.find(f => f.id === feedId);
        if (!confirm(`Delete feed "${feed?.name || feedId}"? All alerts from this feed will also be deleted.`)) return;

        try {
            await HttpUtils.safeFetch(`/api/v1/feeds/${feedId}`, { method: 'DELETE' });
            Toast.show('Feed deleted', 'success');
            await this.loadFeeds();
            this.renderFeedsManagement();
        } catch (err) {
            console.error('[AlertFeeds] Delete feed failed:', err);
            Toast.show('Failed to delete feed: ' + err.message, 'error');
        }
    },

    async syncFeed(feedId) {
        const btn = document.querySelector(`tr[data-feed-id="${feedId}"] .btn-icon[title="Sync Now"]`);
        if (btn) {
            btn.disabled = true;
            btn.classList.add('spinning');
        }

        try {
            const data = await HttpUtils.safeFetch(`/api/v1/feeds/${feedId}/sync`, { method: 'POST' });
            const result = data.data;
            let msg = 'Sync complete';
            if (result) {
                msg += `: ${result.added || 0} added, ${result.updated || 0} updated, ${result.deleted || 0} deleted`;
                if (result.skipped) msg += `, ${result.skipped} skipped`;
            }
            Toast.show(msg, 'success');
            await this.loadFeeds();
            this.renderFeedsManagement();
            this.loadFeedAlerts();
        } catch (err) {
            console.error('[AlertFeeds] Sync failed:', err);
            Toast.show('Sync failed: ' + err.message, 'error');
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.classList.remove('spinning');
            }
        }
    },

    async toggleAllFeedAlerts(feedId, enable) {
        const action = enable ? 'enable' : 'disable';
        try {
            await HttpUtils.safeFetch(`/api/v1/feeds/${feedId}/alerts/${action}-all`, { method: 'POST' });
            Toast.show(`All alerts ${action}d`, 'success');
            this.loadFeedAlerts();
        } catch (err) {
            console.error('[AlertFeeds] Toggle all failed:', err);
            Toast.show('Failed: ' + err.message, 'error');
        }
    },

    // Utility
    formatRepoUrl(url) {
        try {
            const u = new URL(url);
            return u.hostname + u.pathname.replace(/\.git$/, '');
        } catch {
            return url;
        }
    },

    timeAgo(date) {
        const seconds = Math.floor((new Date() - date) / 1000);
        if (seconds < 60) return 'just now';
        const minutes = Math.floor(seconds / 60);
        if (minutes < 60) return minutes + 'm ago';
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return hours + 'h ago';
        const days = Math.floor(hours / 24);
        return days + 'd ago';
    }
};

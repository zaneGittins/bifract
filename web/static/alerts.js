// Alert management module for Bifract
const Alerts = {
    pressureInterval: null,
    currentAlert: null,
    currentWebhook: null,
    editingFeedAlert: false,
    feedAlertOriginalId: null,
    queryHistory: { states: [''], currentFractal: 0, maxSize: 50 },
    isUndoRedoing: false,
    historyTimer: null,
    // Alert editor state (identical to QueryExecutor)
    currentResults: [],
    fieldOrder: null,
    sortColumn: null,
    sortDirection: null,
    columnWidths: {},
    columnOrder: null,
    isAggregated: false,
    // Alert editor pagination state
    alertCurrentPage: 1,
    alertPageSize: 50,

    // Sorting state for manual alerts table
    alertsSortColumn: null,
    alertsSortDirection: null,

    init() {
        this.setupEventListeners();

        // Initialize pagination variables for alerts table
        this.alertsCurrentPage = 1;
        this.alertsPageSize = 25;
        this.filteredAlerts = [];

        // Initialize variables for query testing in alert editor
        this.currentResults = [];
        this.fieldOrder = [];
        this.isAggregated = false;
        this.alertCurrentPage = 1;
        this.alertPageSize = 10;

        // Initialize variables for alert editor
        this.currentAlert = null;
        this.currentTestRequest = null;

        // Initialize variables for webhook management
        this.currentWebhook = null;

        // Initialize variables for fractal action management
        this.currentFractalAction = null;
        this.selectedFractalActionIds = [];

        // Initialize variables for dictionary action management
        this.currentDictAction = null;
        this.selectedDictActionIds = [];
    },

    setupEventListeners() {
        // Navigation is handled by App.js showFractalViewTab()
        // No need to add event listener for alertsTabBtn here

        // Main control buttons
        const importBtn = document.getElementById('importYamlBtn');
        const createBtn = document.getElementById('createAlertBtn');
        const webhooksBtn = document.getElementById('manageWebhooksBtn');
        const refreshBtn = document.getElementById('alertsRefreshBtn');

        if (importBtn) {
            importBtn.addEventListener('click', () => this.showImportModal());
        }
        if (createBtn) {
            createBtn.addEventListener('click', () => this.showAlertEditor());
        }
        if (webhooksBtn) {
            webhooksBtn.addEventListener('click', () => this.showActionsManageView());
        }
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.loadAlerts());
        }

        // Alert type dropdown
        const alertTypeSelect = document.getElementById('alertTypeSelect');
        if (alertTypeSelect) {
            alertTypeSelect.addEventListener('change', () => {
                const type = alertTypeSelect.value;
                const windowGroup = document.getElementById('windowDurationGroup');
                const scheduledGroup = document.getElementById('scheduledConfigGroup');
                const helpText = document.getElementById('alertTypeHelp');
                if (windowGroup) windowGroup.style.display = type === 'compound' ? 'block' : 'none';
                if (scheduledGroup) scheduledGroup.style.display = type === 'scheduled' ? 'block' : 'none';
                if (helpText) {
                    const descriptions = {
                        event: 'Event alerts match individual logs in real-time.',
                        compound: 'Compound alerts aggregate over a time window.',
                        scheduled: 'Scheduled queries run on a cron schedule and look back a configurable window.'
                    };
                    helpText.textContent = descriptions[type] || '';
                }
            });
        }

        // Alert type card buttons (sync with hidden select)
        document.querySelectorAll('.alert-type-card').forEach(card => {
            card.addEventListener('click', () => {
                document.querySelectorAll('.alert-type-card').forEach(c => c.classList.remove('active'));
                card.classList.add('active');
                const type = card.dataset.type;
                if (alertTypeSelect) {
                    alertTypeSelect.value = type;
                    alertTypeSelect.dispatchEvent(new Event('change'));
                }
            });
        });

        // Severity dropdown
        const severityTrigger = document.getElementById('severityDropdownTrigger');
        const severityDropdown = document.getElementById('severityDropdown');
        if (severityTrigger && severityDropdown) {
            severityTrigger.addEventListener('click', (e) => {
                e.stopPropagation();
                severityDropdown.classList.toggle('open');
            });
            document.querySelectorAll('.severity-dropdown-item').forEach(item => {
                item.addEventListener('click', () => {
                    this.setSeverity(item.dataset.severity);
                    severityDropdown.classList.remove('open');
                });
            });
            document.addEventListener('click', (e) => {
                if (!severityDropdown.contains(e.target)) {
                    severityDropdown.classList.remove('open');
                }
            });
        }

        // Template hint tooltip (JS-based to avoid overflow clipping)
        const templateHint = document.querySelector('.alert-name-template-hint');
        if (templateHint) {
            let tooltipEl = null;
            templateHint.addEventListener('mouseenter', () => {
                const text = templateHint.getAttribute('data-tooltip');
                if (!text) return;
                tooltipEl = document.createElement('div');
                tooltipEl.className = 'alert-template-tooltip';
                tooltipEl.textContent = text;
                document.body.appendChild(tooltipEl);
                const rect = templateHint.getBoundingClientRect();
                tooltipEl.style.top = (rect.top - tooltipEl.offsetHeight - 8) + 'px';
                tooltipEl.style.left = Math.max(8, rect.right - tooltipEl.offsetWidth) + 'px';
            });
            templateHint.addEventListener('mouseleave', () => {
                if (tooltipEl) {
                    tooltipEl.remove();
                    tooltipEl = null;
                }
            });
        }

        // Label chip input
        const labelInput = document.getElementById('editorLabelInput');
        if (labelInput) {
            labelInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ',') {
                    e.preventDefault();
                    const val = labelInput.value.replace(',', '').trim();
                    if (val) {
                        this.addLabelChip(val);
                        labelInput.value = '';
                    }
                } else if (e.key === 'Backspace' && !labelInput.value) {
                    const chips = document.querySelectorAll('#editorLabelsChips .alert-label-chip');
                    if (chips.length > 0) {
                        chips[chips.length - 1].remove();
                        this.syncLabelsToHidden();
                    }
                }
            });
            // Also handle paste with commas
            labelInput.addEventListener('blur', () => {
                const val = labelInput.value.trim();
                if (val) {
                    val.split(',').forEach(v => this.addLabelChip(v));
                    labelInput.value = '';
                }
            });
            // Click on container focuses input
            const chipsContainer = document.getElementById('editorLabelsChips');
            if (chipsContainer) {
                chipsContainer.addEventListener('click', () => labelInput.focus());
            }
        }

        // Panel resize
        const resizeHandle = document.getElementById('alertPanelResizeHandle');
        if (resizeHandle) {
            let startX, startWidth;
            const onMouseMove = (e) => {
                const panel = document.getElementById('alertConfigPanel');
                if (!panel) return;
                const newWidth = Math.max(320, Math.min(window.innerWidth * 0.6, startWidth + (startX - e.clientX)));
                panel.style.width = newWidth + 'px';
                const editorView = document.getElementById('alertEditorView');
                const mainContent = editorView ? editorView.querySelector('.main-content') : null;
                if (mainContent) mainContent.style.marginRight = newWidth + 'px';
            };
            const onMouseUp = () => {
                resizeHandle.classList.remove('dragging');
                document.removeEventListener('mousemove', onMouseMove);
                document.removeEventListener('mouseup', onMouseUp);
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            };
            resizeHandle.addEventListener('mousedown', (e) => {
                e.preventDefault();
                const panel = document.getElementById('alertConfigPanel');
                startX = e.clientX;
                startWidth = panel ? panel.offsetWidth : 420;
                resizeHandle.classList.add('dragging');
                document.body.style.cursor = 'col-resize';
                document.body.style.userSelect = 'none';
                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });
        }

        // Collapsible section headers
        document.querySelectorAll('.alert-section-header[data-toggle]').forEach(header => {
            header.addEventListener('click', () => {
                const targetId = header.dataset.toggle;
                const body = document.getElementById(targetId);
                if (!body) return;
                const isCollapsed = header.classList.toggle('collapsed');
                if (isCollapsed) {
                    body.classList.add('collapsed');
                } else {
                    body.classList.remove('collapsed');
                }
            });
        });

        // Schedule preset dropdown (custom cron toggle)
        const schedulePreset = document.getElementById('editorSchedulePreset');
        if (schedulePreset) {
            schedulePreset.addEventListener('change', () => {
                const customGroup = document.getElementById('customCronGroup');
                if (customGroup) {
                    customGroup.style.display = schedulePreset.value === 'custom' ? 'block' : 'none';
                }
            });
        }

        // Search and filters
        const searchInput = document.getElementById('alertSearchInput');
        const statusFilter = document.getElementById('alertStatusFilter');

        if (searchInput) {
            searchInput.addEventListener('input', Utils.debounce(() => {
                this.filterAlerts();
            }, 300));
        }
        if (statusFilter) {
            statusFilter.addEventListener('change', () => this.filterAlerts());
        }

        const actionFilter = document.getElementById('alertActionFilter');
        if (actionFilter) {
            actionFilter.addEventListener('change', () => this.filterAlerts());
        }

        // Unified actions view filters
        const actionTypeFilter = document.getElementById('actionTypeFilter');
        const actionStatusFilter = document.getElementById('actionStatusFilter');
        const actionSearchInput = document.getElementById('actionSearchInput');
        if (actionTypeFilter) actionTypeFilter.addEventListener('change', () => this.filterUnifiedActions());
        if (actionStatusFilter) actionStatusFilter.addEventListener('change', () => this.filterUnifiedActions());
        if (actionSearchInput) actionSearchInput.addEventListener('input', Utils.debounce(() => this.filterUnifiedActions(), 300));

        // Modal form submissions
        document.addEventListener('click', (e) => {
            if (e.target.id === 'importYamlBtn') {
                this.importYAML();
            } else if (e.target.id === 'saveAlertBtn') {
                this.saveAlertFromEditor();
            } else if (e.target.id === 'saveWebhookBtn') {
                this.saveWebhook();
            } else if (e.target.id === 'testWebhookBtn') {
                this.testWebhook();
            } else if (e.target.id === 'saveFractalActionBtn') {
                this.saveFractalAction();
            } else if (e.target.id === 'saveDictActionBtn') {
                this.saveDictAction();
            }
        });

        // Action tabs switching (for actions manage view)
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('action-tab')) {
                this.switchActionTab(e.target.dataset.tab);
            }
        });

        // Remove selected action from alert editor
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('selected-action-remove')) {
                const id = e.target.dataset.id;
                const type = e.target.dataset.type;
                const item = e.target.closest('.selected-action-item');
                const name = item ? item.querySelector('.selected-action-name')?.textContent : '';
                this.removeSelectedAction(id, type, name);
            }
        });

        // Add action from unified dropdown
        document.addEventListener('change', (e) => {
            if (e.target.id === 'editorActionsSelect') {
                const selected = e.target.options[e.target.selectedIndex];
                if (!selected.value) return;
                const id = selected.value;
                const name = selected.textContent;
                const type = selected.dataset.type;
                const selectedList = document.getElementById('editorSelectedActions');
                if (selectedList) {
                    this._appendSelectedAction(selectedList, id, name, type);
                }
                selected.remove();
                e.target.value = '';
            }
        });

        // Modal backdrop click to close
        document.addEventListener('click', (e) => {
            // Check if clicked element is a modal backdrop
            const modalIds = ['alertModal', 'importYamlModal'];

            for (const modalId of modalIds) {
                const modal = document.getElementById(modalId);
                if (modal && e.target === modal && modal.style.display === 'flex') {
                    this.closeModal(modalId);
                    break;
                }
            }
        });
    },

    async show() {
        // Ensure alert editor and actions views are hidden when showing alerts list
        const alertEditorView = document.getElementById('alertEditorView');
        const actionsManageView = document.getElementById('actionsManageView');
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (actionsManageView) actionsManageView.style.display = 'none';
        this.closeAlertPanel();

        await this.loadAlerts();
        this.updateAlertCount();
        this.startPressurePolling();
    },

    startPressurePolling() {
        this.stopPressurePolling();
        this.checkPressure();
        this.pressureInterval = setInterval(() => this.checkPressure(), 10000);
    },

    stopPressurePolling() {
        if (this.pressureInterval) {
            clearInterval(this.pressureInterval);
            this.pressureInterval = null;
        }
        const existing = document.getElementById('alertsPressureBanner');
        if (existing) existing.remove();
    },

    async checkPressure() {
        try {
            const res = await fetch('/api/v1/system/pressure', { credentials: 'include' });
            const data = await res.json();
            this.renderPressureBanner(data);
        } catch (err) {
            // Non-critical, silently ignore
        }
    },

    renderPressureBanner(data) {
        const existing = document.getElementById('alertsPressureBanner');
        if (!data || !data.alerts_deferred) {
            if (existing) existing.remove();
            return;
        }
        if (existing) return;

        const banner = document.createElement('div');
        banner.id = 'alertsPressureBanner';
        banner.className = 'system-pressure-banner';
        banner.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
                <line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
            </svg>
            Alert evaluation is temporarily deferred due to high ingestion load. Cursors are preserved and alerts will catch up automatically.
        `;

        const header = document.querySelector('.alerts-header');
        if (header && header.parentNode) {
            header.parentNode.insertBefore(banner, header.nextSibling);
        }
    },

    async loadAlerts() {
        const alertsList = document.getElementById('alertsList');
        if (!alertsList) return;

        try {
            const response = await fetch('/api/v1/alerts', {
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load alerts');
            }

            this.renderAlerts(data.data.alerts || []);
            this.updateAlertCount(data.data.count || 0);
        } catch (error) {
            console.error('Failed to load alerts:', error);
            alertsList.innerHTML = '<div class="error">Failed to load alerts: ' + Utils.escapeHtml(error.message) + '</div>';
            Toast.show('Failed to load alerts', 'error');
        }
    },

    renderAlerts(alerts) {
        const alertsList = document.getElementById('alertsList');
        if (!alertsList) return;

        if (alerts.length === 0) {
            alertsList.innerHTML = `
                <div class="empty-state">
                    <div class="empty-text">No Alerts Configured</div>
                    <div class="empty-actions">
                        <button onclick="Alerts.showAlertEditor()" class="btn-primary">Create Your First Alert</button>
                        <button onclick="Alerts.showImportModal()" class="btn-secondary">Import from YAML</button>
                    </div>
                </div>
            `;
            return;
        }

        const alertsHTML = this.renderAlertsTable(alerts);
        alertsList.innerHTML = alertsHTML;

        // Store alerts and initialize pagination
        this.allAlerts = alerts;
        this.filteredAlerts = alerts;
        this.alertsCurrentPage = 1;
        this.buildActionFilterOptions();
        this.addAlertTableClickHandlers();
        this.filterAlerts();
    },

    renderAlertCard(alert) {
        const statusClass = alert.enabled ? 'enabled' : 'disabled';
        const statusText = alert.enabled ? 'Enabled' : 'Disabled';
        const lastTriggered = alert.last_triggered
            ? new Date(alert.last_triggered).toLocaleString()
            : 'Never';

        return `
            <div class="alert-card ${statusClass}" data-alert-id="${alert.id}">
                <div class="alert-header">
                    <div class="alert-title">
                        <h3>${Utils.escapeHtml(alert.name)}</h3>
                        <span class="alert-status status-${statusClass}">
                            ${statusText}
                        </span>
                    </div>
                    <div class="alert-actions">
                        <button onclick="Alerts.editAlert('${alert.id}')" class="btn-icon" title="Edit">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                                <path d="M18.5 2.5a2.12 2.12 0 0 1 3 3L12 15l-4 1 1-4Z"/>
                            </svg>
                        </button>
                        <button onclick="Alerts.toggleAlert('${alert.id}', ${!alert.enabled})" class="btn-icon" title="${alert.enabled ? 'Disable' : 'Enable'}">
                            ${alert.enabled ?
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 9V5a2 2 0 0 1 4 0v4"/><rect width="14" height="11" x="5" y="9" rx="2" ry="2"/><circle cx="12" cy="15" r="1"/></svg>' :
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="14" height="11" x="5" y="9" rx="2" ry="2"/><path d="M7 9V5a2 2 0 0 1 4 0"/></svg>'
                            }
                        </button>
                        <button onclick="Alerts.exportYAML('${alert.id}')" class="btn-icon" title="Export YAML">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                                <polyline points="7,10 12,15 17,10"/>
                                <line x1="12" y1="15" x2="12" y2="3"/>
                            </svg>
                        </button>
                        <button onclick="Alerts.deleteAlert('${alert.id}')" class="btn-icon btn-danger" title="Delete">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m3 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6h14ZM10 11v6M14 11v6"/>
                            </svg>
                        </button>
                    </div>
                </div>

                ${alert.description ? `<p class="alert-description">${Utils.escapeHtml(alert.description)}</p>` : ''}

                <div class="alert-query">
                    <strong>Query:</strong>
                    <code>${Utils.escapeHtml(alert.query_string)}</code>
                </div>

                ${alert.labels && alert.labels.length > 0 ? `
                    <div class="alert-labels">
                        ${alert.labels.map(label => `<span class="label">${Utils.escapeHtml(label)}</span>`).join('')}
                    </div>
                ` : ''}

                <div class="alert-meta">
                    <div class="alert-meta-item">
                        <strong>Actions:</strong> ${alert.webhook_actions?.length || 0}
                    </div>
                    ${alert.throttle_time_seconds > 0 ? `
                        <div class="alert-meta-item">
                            <strong>Throttle:</strong> ${this.formatThrottleTime(alert.throttle_time_seconds)}
                        </div>
                    ` : ''}
                    <div class="alert-meta-item">
                        <strong>Created:</strong> ${new Date(alert.created_at).toLocaleDateString()}
                    </div>
                    <div class="alert-meta-item">
                        <strong>Last Triggered:</strong> ${lastTriggered}
                    </div>
                </div>
            </div>
        `;
    },

    formatThrottleTime(seconds) {
        if (seconds < 60) return `${seconds}s`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
        if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
        return `${Math.floor(seconds / 86400)}d`;
    },

    formatWindowDuration(seconds) {
        if (!seconds) return '';
        if (seconds % 86400 === 0) return `${seconds / 86400}d`;
        if (seconds % 3600 === 0) return `${seconds / 3600}h`;
        return `${Math.round(seconds / 60)}m`;
    },

    formatCronSchedule(cronExpr) {
        const presets = {
            '0 * * * *': 'hourly',
            '0 0 * * *': 'daily',
            '0 0 * * 1': 'weekly',
            '0 0 1 * *': 'monthly'
        };
        return presets[cronExpr] || cronExpr;
    },

    renderAlertsTable(alerts) {
        // Get current page of alerts
        const currentPageAlerts = this.getCurrentPageAlerts();

        return `
            <div class="alerts-table-container">
                <div class="alerts-table-header">
                    <div class="alerts-count">
                        Showing ${currentPageAlerts.length} of ${this.filteredAlerts.length} alerts
                        ${this.filteredAlerts.length !== this.allAlerts?.length ? ` (filtered from ${this.allAlerts?.length} total)` : ''}
                    </div>
                    <div class="alerts-page-size">
                        <label>Show:</label>
                        <select id="alertsPageSizeSelect" onchange="Alerts.changePageSize(this.value)">
                            <option value="10" ${this.alertsPageSize === 10 ? 'selected' : ''}>10</option>
                            <option value="25" ${this.alertsPageSize === 25 ? 'selected' : ''}>25</option>
                            <option value="50" ${this.alertsPageSize === 50 ? 'selected' : ''}>50</option>
                            <option value="100" ${this.alertsPageSize === 100 ? 'selected' : ''}>100</option>
                        </select>
                    </div>
                </div>
                <table class="alerts-table">
                    <thead>
                        <tr>
                            <th class="sortable-th" onclick="Alerts.toggleAlertsSort('name')">Name${this.alertsSortIndicator('name')}</th>
                            <th>Type</th>
                            <th>Actions</th>
                            <th>Last Modified By</th>
                            <th class="sortable-th" onclick="Alerts.toggleAlertsSort('exec_time')">Exec Time${this.alertsSortIndicator('exec_time')}</th>
                            <th class="sortable-th" onclick="Alerts.toggleAlertsSort('last_triggered')">Last Triggered${this.alertsSortIndicator('last_triggered')}</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${currentPageAlerts.map(alert => this.renderAlertTableRow(alert)).join('')}
                    </tbody>
                </table>
                ${this.renderAlertsPagination()}
            </div>

            <!-- Alert Details Panel -->
            <div id="alertDetailsPanel" class="alert-details-panel">
                <div class="alert-details-header">
                    <h3 id="alertDetailsTitle">Alert Details</h3>
                    <button onclick="Alerts.closeAlertDetailsPanel()" class="btn-icon">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div id="alertDetailsContent" class="alert-details-content">
                    <!-- Alert details will be populated here -->
                </div>
            </div>
        `;
    },

    renderAlertTableRow(alert) {
        const isAutoDisabled = !alert.enabled && alert.disabled_reason;
        const statusClass = isAutoDisabled ? 'auto-disabled' : (alert.enabled ? 'enabled' : 'disabled');
        const statusText = isAutoDisabled ? 'Auto-disabled' : (alert.enabled ? 'Enabled' : 'Disabled');
        const lastTriggered = alert.last_triggered
            ? new Date(alert.last_triggered).toLocaleString()
            : 'Never';

        const modifiedBy = alert.updated_by || alert.created_by || '-';
        const actionCount = (alert.webhook_actions?.length || 0)
            + (alert.fractal_actions?.length || 0)
            + (alert.dictionary_actions?.length || 0);

        const lastRunTime = alert.last_execution_time_ms != null
            ? (alert.last_execution_time_ms >= 1000
                ? `${(alert.last_execution_time_ms / 1000).toFixed(1)}s`
                : `${alert.last_execution_time_ms}ms`)
            : '-';
        const runTimeClass = alert.last_execution_time_ms != null && alert.last_execution_time_ms >= 3000
            ? 'alert-run-time-slow' : '';

        return `
            <tr class="alert-row ${statusClass}" data-alert-id="${alert.id}">
                <td class="alert-name">
                    <div class="alert-name-row">
                        <span class="status-dot status-${statusClass}" title="${statusText}"></span>
                        <strong>${Utils.escapeHtml(alert.name)}</strong>
                        ${isAutoDisabled ? '<span class="alert-auto-disabled-badge">timeout</span>' : ''}
                    </div>
                    ${alert.description ? `<div class="alert-description-preview">${Utils.escapeHtml(alert.description.substring(0, 60))}${alert.description.length > 60 ? '...' : ''}</div>` : ''}
                </td>
                <td class="alert-type">${Utils.escapeHtml(alert.alert_type || 'event')}${alert.alert_type === 'compound' && alert.window_duration ? ` <span class="alert-window-badge">${this.formatWindowDuration(alert.window_duration)}</span>` : ''}${alert.alert_type === 'scheduled' && alert.schedule_cron ? ` <span class="alert-window-badge">${this.formatCronSchedule(alert.schedule_cron)}</span>` : ''}</td>
                <td class="alert-action-count">${actionCount}</td>
                <td class="alert-modified-by">${Utils.escapeHtml(modifiedBy)}</td>
                <td class="alert-run-time ${runTimeClass}">${lastRunTime}</td>
                <td class="alert-triggered">${lastTriggered}</td>
            </tr>
        `;
    },

    addAlertTableClickHandlers() {
        const alertRows = document.querySelectorAll('.alert-row');
        alertRows.forEach(row => {
            row.addEventListener('click', (e) => {
                const alertId = row.dataset.alertId;
                const alert = this.allAlerts.find(a => a.id === alertId);
                if (alert) {
                    this.showAlertDetailsPanel(alert);
                }
            });
        });
    },

    showAlertDetailsPanel(alert) {
        const panel = document.getElementById('alertDetailsPanel');
        const title = document.getElementById('alertDetailsTitle');
        const content = document.getElementById('alertDetailsContent');

        if (!panel || !title || !content) return;

        title.textContent = alert.name;
        content.innerHTML = this.renderAlertDetails(alert);

        panel.classList.add('open');

        // Store current alert for actions
        this.currentDetailAlert = alert;

        // Close on Escape
        this._detailEscHandler = (e) => {
            if (e.key === 'Escape') this.closeAlertDetailsPanel();
        };
        document.addEventListener('keydown', this._detailEscHandler);
    },

    closeAlertDetailsPanel() {
        const panel = document.getElementById('alertDetailsPanel');
        if (panel) {
            panel.classList.remove('open');
        }
        this.currentDetailAlert = null;
        if (this._detailEscHandler) {
            document.removeEventListener('keydown', this._detailEscHandler);
            this._detailEscHandler = null;
        }
    },

    renderAlertDetails(alert) {
        const isAutoDisabled = !alert.enabled && alert.disabled_reason;
        const statusClass = isAutoDisabled ? 'auto-disabled' : (alert.enabled ? 'enabled' : 'disabled');
        const statusText = isAutoDisabled ? 'Auto-disabled' : (alert.enabled ? 'Enabled' : 'Disabled');
        const lastTriggered = alert.last_triggered
            ? new Date(alert.last_triggered).toLocaleString()
            : 'Never';

        return `
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
                    <label>Query:</label>
                    <pre class="alert-query-display"><code>${Utils.escapeHtml(alert.query_string)}</code></pre>
                </div>

                ${alert.description ? `
                    <div class="alert-detail-field">
                        <label>Description:</label>
                        <p>${Utils.escapeHtml(alert.description)}</p>
                    </div>
                ` : ''}

                ${alert.labels && alert.labels.length > 0 ? `
                    <div class="alert-detail-field">
                        <label>Labels:</label>
                        <div class="alert-labels">
                            ${alert.labels.map(label => `<span class="label">${Utils.escapeHtml(label)}</span>`).join('')}
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

                <div class="alert-detail-field">
                    <label>Webhook Actions:</label>
                    <span>${alert.webhook_actions?.length || 0}</span>
                </div>

                ${alert.throttle_time_seconds > 0 ? `
                    <div class="alert-detail-field">
                        <label>Throttle Time:</label>
                        <span>${this.formatThrottleTime(alert.throttle_time_seconds)}</span>
                    </div>
                ` : ''}

                ${alert.throttle_field ? `
                    <div class="alert-detail-field">
                        <label>Throttle Field:</label>
                        <span>${Utils.escapeHtml(alert.throttle_field)}</span>
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
                <button onclick="Alerts.editAlert('${alert.id}')" class="btn-primary">
                    Edit Alert
                </button>
                <button onclick="Alerts.toggleAlert('${alert.id}', ${!alert.enabled})" class="btn-secondary">
                    ${alert.enabled ? 'Disable' : 'Enable'}
                </button>
                <button onclick="Alerts.exportYAML('${alert.id}')" class="btn-secondary">
                    Export YAML
                </button>
                <button onclick="Alerts.deleteAlert('${alert.id}')" class="btn-danger">
                    Delete
                </button>
            </div>
        `;
    },

    buildActionFilterOptions() {
        const select = document.getElementById('alertActionFilter');
        if (!select || !this.allAlerts) return;

        const currentValue = select.value;

        // Collect unique actions by type
        const webhooks = new Map();
        const fractals = new Map();
        const dictionaries = new Map();

        for (const alert of this.allAlerts) {
            for (const wa of (alert.webhook_actions || [])) {
                if (wa.id && wa.name) webhooks.set(wa.id, wa.name);
            }
            for (const fa of (alert.fractal_actions || [])) {
                if (fa.id && fa.name) fractals.set(fa.id, fa.name);
            }
            for (const da of (alert.dictionary_actions || [])) {
                if (da.id && da.name) dictionaries.set(da.id, da.name);
            }
        }

        let html = '<option value="all">All Actions</option>';
        html += '<option value="none">No Actions</option>';

        if (webhooks.size > 0) {
            html += '<optgroup label="Webhooks">';
            html += '<option value="webhook:*">Any Webhook</option>';
            for (const [id, name] of [...webhooks.entries()].sort((a, b) => a[1].localeCompare(b[1]))) {
                html += `<option value="webhook:${id}">${Utils.escapeHtml(name)}</option>`;
            }
            html += '</optgroup>';
        }

        if (fractals.size > 0) {
            html += '<optgroup label="Fractal Actions">';
            html += '<option value="fractal:*">Any Fractal Action</option>';
            for (const [id, name] of [...fractals.entries()].sort((a, b) => a[1].localeCompare(b[1]))) {
                html += `<option value="fractal:${id}">${Utils.escapeHtml(name)}</option>`;
            }
            html += '</optgroup>';
        }

        if (dictionaries.size > 0) {
            html += '<optgroup label="Dictionary Actions">';
            html += '<option value="dictionary:*">Any Dictionary Action</option>';
            for (const [id, name] of [...dictionaries.entries()].sort((a, b) => a[1].localeCompare(b[1]))) {
                html += `<option value="dictionary:${id}">${Utils.escapeHtml(name)}</option>`;
            }
            html += '</optgroup>';
        }

        select.innerHTML = html;

        // Restore previous selection if still valid
        if (currentValue && select.querySelector(`option[value="${CSS.escape(currentValue)}"]`)) {
            select.value = currentValue;
        } else {
            select.value = 'all';
        }
    },

    matchesActionFilter(alert, actionFilter) {
        if (actionFilter === 'all') return true;

        const webhookCount = alert.webhook_actions?.length || 0;
        const fractalCount = alert.fractal_actions?.length || 0;
        const dictCount = alert.dictionary_actions?.length || 0;
        const totalActions = webhookCount + fractalCount + dictCount;

        if (actionFilter === 'none') return totalActions === 0;

        const [type, id] = actionFilter.split(':');
        if (id === '*') {
            if (type === 'webhook') return webhookCount > 0;
            if (type === 'fractal') return fractalCount > 0;
            if (type === 'dictionary') return dictCount > 0;
            return false;
        }

        if (type === 'webhook') return alert.webhook_actions?.some(a => a.id === id);
        if (type === 'fractal') return alert.fractal_actions?.some(a => a.id === id);
        if (type === 'dictionary') return alert.dictionary_actions?.some(a => a.id === id);
        return false;
    },

    filterAlerts() {
        if (!this.allAlerts) return;

        const searchTerm = document.getElementById('alertSearchInput')?.value.toLowerCase() || '';
        const statusFilter = document.getElementById('alertStatusFilter')?.value || 'all';
        const actionFilter = document.getElementById('alertActionFilter')?.value || 'all';

        this.filteredAlerts = this.allAlerts.filter(alert => {
            const matchesSearch = searchTerm === '' ||
                alert.name.toLowerCase().includes(searchTerm) ||
                (alert.description && alert.description.toLowerCase().includes(searchTerm)) ||
                alert.query_string.toLowerCase().includes(searchTerm) ||
                (alert.labels && alert.labels.some(label => label.toLowerCase().includes(searchTerm)));

            const matchesStatus = statusFilter === 'all' ||
                (statusFilter === 'enabled' && alert.enabled) ||
                (statusFilter === 'disabled' && !alert.enabled);

            const matchesAction = this.matchesActionFilter(alert, actionFilter);

            return matchesSearch && matchesStatus && matchesAction;
        });

        // Reset to first page when filters change
        this.alertsCurrentPage = 1;

        this.applyAlertsSort();

        // Re-render the table with pagination
        this.updateAlertsTable();
        this.updateBulkButtons();
    },

    updateBulkButtons() {
        const search = (document.getElementById('alertSearchInput')?.value || '').trim();
        const statusFilter = document.getElementById('alertStatusFilter')?.value || 'all';
        const actionFilter = document.getElementById('alertActionFilter')?.value || 'all';

        const hasFilter = search || statusFilter !== 'all' || actionFilter !== 'all';

        const enableBtn = document.getElementById('alertsBulkEnableBtn');
        const disableBtn = document.getElementById('alertsBulkDisableBtn');
        if (enableBtn) enableBtn.style.display = hasFilter ? '' : 'none';
        if (disableBtn) disableBtn.style.display = hasFilter ? '' : 'none';
    },

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
            const response = await fetch('/api/v1/alerts/batch-toggle', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify({ alert_ids: alertIds, enabled })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to update alerts');
            const count = data.data?.toggled || alertIds.length;
            Toast.show(`${count} alert${count !== 1 ? 's' : ''} ${enabled ? 'enabled' : 'disabled'}`, 'success');
            await this.loadAlerts();
        } catch (err) {
            console.error('Batch toggle failed:', err);
            Toast.show('Failed: ' + err.message, 'error');
        }
    },

    toggleAlertsSort(column) {
        if (this.alertsSortColumn === column) {
            if (this.alertsSortDirection === 'asc') {
                this.alertsSortDirection = 'desc';
            } else {
                this.alertsSortColumn = null;
                this.alertsSortDirection = null;
            }
        } else {
            this.alertsSortColumn = column;
            this.alertsSortDirection = 'asc';
        }
        this.applyAlertsSort();
        this.updateAlertsTable();
    },

    applyAlertsSort() {
        if (!this.alertsSortColumn || !this.filteredAlerts) return;

        const col = this.alertsSortColumn;
        const dir = this.alertsSortDirection === 'asc' ? 1 : -1;

        this.filteredAlerts.sort((a, b) => {
            let va, vb;
            switch (col) {
                case 'name':
                    va = a.name.toLowerCase();
                    vb = b.name.toLowerCase();
                    return va < vb ? -dir : va > vb ? dir : 0;
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

    alertsSortIndicator(column) {
        if (this.alertsSortColumn !== column) return '';
        return this.alertsSortDirection === 'asc' ? ' &#9650;' : ' &#9660;';
    },

    updateAlertsTable() {
        const alertsList = document.getElementById('alertsList');
        if (!alertsList) return;

        if (this.filteredAlerts.length === 0 && this.allAlerts.length > 0) {
            alertsList.innerHTML = `
                <div class="empty-state">
                    <div class="empty-text">No alerts match your filters</div>
                    <div class="empty-actions">
                        <button onclick="document.getElementById('alertSearchInput').value=''; document.getElementById('alertStatusFilter').value='all'; document.getElementById('alertActionFilter').value='all'; Alerts.filterAlerts();" class="btn-secondary">Clear Filters</button>
                    </div>
                </div>
            `;
        } else {
            const alertsHTML = this.renderAlertsTable(this.filteredAlerts);
            alertsList.innerHTML = alertsHTML;
            this.addAlertTableClickHandlers();
        }
    },

    getCurrentPageAlerts() {
        const start = (this.alertsCurrentPage - 1) * this.alertsPageSize;
        const end = start + this.alertsPageSize;
        return this.filteredAlerts.slice(start, end);
    },

    getTotalPages() {
        return Math.ceil(this.filteredAlerts.length / this.alertsPageSize);
    },

    renderAlertsPagination() {
        const totalPages = this.getTotalPages();

        if (totalPages <= 1) {
            return '<div class="alerts-pagination" style="display: none;"></div>';
        }

        const current = this.alertsCurrentPage;
        const maxVisible = 5;

        let startPage = Math.max(1, current - Math.floor(maxVisible / 2));
        let endPage = Math.min(totalPages, startPage + maxVisible - 1);

        // Adjust start if we're near the end
        if (endPage - startPage + 1 < maxVisible) {
            startPage = Math.max(1, endPage - maxVisible + 1);
        }

        let paginationHTML = '<div class="alerts-pagination">';

        // Previous button
        if (current > 1) {
            paginationHTML += `<button onclick="Alerts.goToPage(${current - 1})" class="pagination-btn">‹</button>`;
        } else {
            paginationHTML += `<button class="pagination-btn disabled">‹</button>`;
        }

        // First page and ellipsis
        if (startPage > 1) {
            paginationHTML += `<button onclick="Alerts.goToPage(1)" class="pagination-btn">1</button>`;
            if (startPage > 2) {
                paginationHTML += `<span class="pagination-ellipsis">...</span>`;
            }
        }

        // Page numbers
        for (let i = startPage; i <= endPage; i++) {
            if (i === current) {
                paginationHTML += `<button class="pagination-btn active">${i}</button>`;
            } else {
                paginationHTML += `<button onclick="Alerts.goToPage(${i})" class="pagination-btn">${i}</button>`;
            }
        }

        // Last page and ellipsis
        if (endPage < totalPages) {
            if (endPage < totalPages - 1) {
                paginationHTML += `<span class="pagination-ellipsis">...</span>`;
            }
            paginationHTML += `<button onclick="Alerts.goToPage(${totalPages})" class="pagination-btn">${totalPages}</button>`;
        }

        // Next button
        if (current < totalPages) {
            paginationHTML += `<button onclick="Alerts.goToPage(${current + 1})" class="pagination-btn">›</button>`;
        } else {
            paginationHTML += `<button class="pagination-btn disabled">›</button>`;
        }

        paginationHTML += '</div>';
        return paginationHTML;
    },

    goToPage(page) {
        const totalPages = this.getTotalPages();
        if (page < 1 || page > totalPages) return;

        this.alertsCurrentPage = page;
        this.updateAlertsTable();
    },

    changePageSize(newSize) {
        this.alertsPageSize = parseInt(newSize);
        this.alertsCurrentPage = 1; // Reset to first page
        this.updateAlertsTable();
    },

    updateAlertCount(count) {
        // Badge disabled as per user request - counts are distracting
        const badge = document.getElementById('alertsCountBadge');
        if (badge) {
            badge.style.display = 'none';
        }
    },

    // Modal Management
    showImportModal() {
        const modal = document.getElementById('importYamlModal');
        if (modal) {
            modal.style.display = 'flex';
            const yamlTextarea = document.getElementById('yamlContent');
            if (yamlTextarea) {
                yamlTextarea.value = '';

                // Apply simple YAML syntax highlighting with immediate setup
                this.setupYAMLHighlightingForModal(yamlTextarea);

                // Add Sigma detection listener
                if (!yamlTextarea._sigmaListenerAdded) {
                    yamlTextarea._sigmaListenerAdded = true;
                    let debounceTimer = null;
                    yamlTextarea.addEventListener('input', () => {
                        clearTimeout(debounceTimer);
                        debounceTimer = setTimeout(() => {
                            this.detectSigmaRule(yamlTextarea.value);
                        }, 400);
                    });
                }
            }

            // Reset Sigma UI state
            const sigmaInfo = document.getElementById('sigmaDetectedInfo');
            const normalizerGroup = document.getElementById('importNormalizerGroup');
            if (sigmaInfo) sigmaInfo.style.display = 'none';
            if (normalizerGroup) normalizerGroup.style.display = 'none';

            const errorDiv = document.getElementById('importError');
            if (errorDiv) errorDiv.style.display = 'none';

            // Pre-load normalizers for Sigma import
            this.loadNormalizersForImport();
        }
    },

    detectSigmaRule(yamlContent) {
        const hasSigmaDetection = /^detection:/m.test(yamlContent) && /^\s+condition:/m.test(yamlContent);
        const hasBifractQuery = /^queryString:/m.test(yamlContent);
        const isSigma = hasSigmaDetection && !hasBifractQuery;

        const sigmaInfo = document.getElementById('sigmaDetectedInfo');
        const normalizerGroup = document.getElementById('importNormalizerGroup');

        if (sigmaInfo) sigmaInfo.style.display = isSigma ? 'block' : 'none';
        if (normalizerGroup) normalizerGroup.style.display = isSigma ? 'block' : 'none';
    },

    async loadNormalizersForImport() {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers');
            const normalizersList = (data.data && data.data.normalizers) || [];
            const select = document.getElementById('importNormalizerSelect');
            if (!select) return;

            select.innerHTML = '<option value="">None (use Sigma field names as-is)</option>';
            for (const n of normalizersList) {
                const opt = document.createElement('option');
                opt.value = n.id;
                opt.textContent = n.name + (n.is_default ? ' (default)' : '');
                if (n.is_default) opt.selected = true;
                select.appendChild(opt);
            }
        } catch (err) {
            console.error('Failed to load normalizers for import:', err);
        }
    },

    setupYAMLHighlightingForModal(textarea) {
        if (!textarea || textarea.yamlSetup) return;

        // Mark as set up to prevent duplicate setup
        textarea.yamlSetup = true;

        // Add CSS classes for YAML highlighting
        textarea.classList.add('yaml-highlighted-input');

        // Create a preview element for syntax highlighting
        const wrapper = document.createElement('div');
        wrapper.className = 'yaml-input-wrapper';
        wrapper.style.cssText = 'position: relative; display: block;';

        const preview = document.createElement('div');
        preview.className = 'yaml-syntax-preview';
        preview.style.cssText = `
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            padding: ${getComputedStyle(textarea).padding};
            font-family: ${getComputedStyle(textarea).fontFamily};
            font-size: ${getComputedStyle(textarea).fontSize};
            line-height: ${getComputedStyle(textarea).lineHeight};
            white-space: pre-wrap;
            word-wrap: break-word;
            color: transparent;
            background: transparent;
            pointer-events: none;
            z-index: 1;
            overflow: hidden;
            border: none;
            margin: 0;
        `;

        // Insert wrapper before textarea
        textarea.parentNode.insertBefore(wrapper, textarea);
        wrapper.appendChild(preview);
        wrapper.appendChild(textarea);

        // Style textarea to be transparent on top
        textarea.style.cssText += `
            position: relative;
            z-index: 2;
            background: transparent;
            color: var(--text-primary);
        `;

        // Update highlighting function
        const updateHighlighting = () => {
            const yamlText = textarea.value;
            if (yamlText && window.Utils && Utils.highlightYAML) {
                preview.innerHTML = Utils.highlightYAML(yamlText);
            } else {
                preview.innerHTML = Utils.escapeHtml(yamlText);
            }
        };

        // Sync scrolling
        const syncScroll = () => {
            preview.scrollTop = textarea.scrollTop;
            preview.scrollLeft = textarea.scrollLeft;
        };

        // Event listeners
        textarea.addEventListener('input', updateHighlighting);
        textarea.addEventListener('scroll', syncScroll);
        textarea.addEventListener('paste', () => {
            // Delay to allow paste content to be processed
            setTimeout(updateHighlighting, 10);
        });

        // Initial update
        updateHighlighting();
    },

    showAlertModal(alertId = null) {
        const modal = document.getElementById('alertModal');
        const title = document.getElementById('alertModalTitle');

        if (!modal) return;

        // Reset form
        this.resetAlertForm();

        if (alertId) {
            title.textContent = 'Edit Alert';
            this.loadAlertForEdit(alertId);
        } else {
            title.textContent = 'Create Alert';
            this.currentAlert = null;
        }

        this.loadWebhooksForAlert();
        modal.style.display = 'flex';
    },

    showInlineWebhookCreate() {
        document.getElementById('newActionMenu')?.style && (document.getElementById('newActionMenu').style.display = 'none');
        this.closeAllInlineForms();
        this.inlineWebhookForm = 'create';
        this.currentWebhook = null;
        this.renderWebhookInlineForm();
    },

    async showInlineWebhookEdit(webhookId) {
        this.inlineWebhookForm = webhookId;
        this.closeInlineFractalActionForm();
        try {
            const response = await fetch(`/api/v1/webhooks/${webhookId}`, { credentials: 'include' });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to load webhook');
            this.currentWebhook = data.data.webhook;
            this.renderWebhookInlineForm();
            this.populateWebhookFormFields();
        } catch (error) {
            console.error('Load webhook error:', error);
            Toast.show('Failed to load webhook for editing', 'error');
        }
    },

    renderWebhookInlineForm() {
        const container = document.getElementById('webhookInlineFormContainer');
        if (!container) return;

        const isEdit = this.inlineWebhookForm !== 'create';
        const panelClass = isEdit ? 'actions-edit-panel' : 'actions-create-panel';
        const title = isEdit ? 'Edit Webhook' : 'Create Webhook';

        container.innerHTML = `
            <div class="${panelClass}">
                <div class="actions-panel-header">
                    <h3>${title}</h3>
                    <button class="btn-icon" onclick="Alerts.closeInlineWebhookForm()" title="Close">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div class="actions-form-grid">
                    <div class="actions-form-group">
                        <label for="webhookName">Name *</label>
                        <input type="text" id="webhookName" placeholder="Enter webhook name" required>
                    </div>
                    <div class="actions-form-group">
                        <label for="webhookUrl">URL *</label>
                        <input type="url" id="webhookUrl" placeholder="https://example.com/webhook" required>
                    </div>
                    <div class="actions-form-group">
                        <label for="webhookMethod">HTTP Method</label>
                        <select id="webhookMethod">
                            <option value="POST">POST</option>
                            <option value="PUT">PUT</option>
                            <option value="PATCH">PATCH</option>
                        </select>
                    </div>
                    <div class="actions-form-group">
                        <label for="webhookTimeout">Timeout (seconds)</label>
                        <input type="number" id="webhookTimeout" value="30" min="5" max="300">
                    </div>
                    <div class="actions-form-group">
                        <label for="webhookRetries">Retry Count</label>
                        <input type="number" id="webhookRetries" value="3" min="0" max="10">
                    </div>
                    <div class="actions-form-group">
                        <label for="webhookAuthType">Authentication</label>
                        <select id="webhookAuthType" onchange="Alerts.handleAuthTypeChange(this.value)">
                            <option value="none">None</option>
                            <option value="bearer">Bearer Token</option>
                            <option value="basic">Basic Auth</option>
                        </select>
                    </div>
                    <div class="actions-form-group full-width" id="webhookAuthConfig" style="display: none;">
                        <div id="bearerConfig" style="display: none;">
                            <label for="bearerToken">Bearer Token</label>
                            <input type="password" id="bearerToken" placeholder="Enter bearer token">
                        </div>
                        <div id="basicConfig" style="display: none;">
                            <div class="actions-form-grid">
                                <div class="actions-form-group">
                                    <label for="basicUsername">Username</label>
                                    <input type="text" id="basicUsername" placeholder="Enter username">
                                </div>
                                <div class="actions-form-group">
                                    <label for="basicPassword">Password</label>
                                    <input type="password" id="basicPassword" placeholder="Enter password">
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="actions-form-group full-width">
                        <label>Custom Headers</label>
                        <div id="customHeaders">
                            <div class="header-row">
                                <input type="text" placeholder="Header name" class="header-name">
                                <input type="text" placeholder="Header value" class="header-value">
                                <button type="button" class="btn-sm btn-secondary" onclick="Alerts.addHeaderRow()">+</button>
                            </div>
                        </div>
                    </div>
                    <div class="actions-form-group full-width">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="webhookIncludeAlertLink" checked>
                            Include alert link in payload
                        </label>
                    </div>
                    <div class="actions-form-group full-width">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="webhookEnabled" checked>
                            Enabled
                        </label>
                    </div>
                </div>
                <div class="actions-form-actions">
                    <button class="btn-secondary" onclick="Alerts.closeInlineWebhookForm()">Cancel</button>
                    ${isEdit ? '<button id="testWebhookBtn" class="btn-secondary" onclick="Alerts.testWebhook()">Test</button>' : ''}
                    <button id="saveWebhookBtn" class="btn-primary" onclick="Alerts.saveWebhook()">Save Webhook</button>
                </div>
                <div id="webhookError" class="error-message" style="display: none;"></div>
            </div>
        `;

        document.getElementById('webhookName')?.focus();
    },

    populateWebhookFormFields() {
        const webhook = this.currentWebhook;
        if (!webhook) return;

        document.getElementById('webhookName').value = webhook.name || '';
        document.getElementById('webhookUrl').value = webhook.url || '';
        document.getElementById('webhookMethod').value = webhook.method || 'POST';
        document.getElementById('webhookTimeout').value = webhook.timeout_seconds || 30;
        document.getElementById('webhookRetries').value = webhook.retry_count || 3;
        document.getElementById('webhookAuthType').value = webhook.auth_type || 'none';
        document.getElementById('webhookEnabled').checked = webhook.enabled;
        document.getElementById('webhookIncludeAlertLink').checked = webhook.include_alert_link !== false;

        this.handleAuthTypeChange(webhook.auth_type);
        if (webhook.auth_type === 'bearer' && webhook.auth_config?.token) {
            document.getElementById('bearerToken').value = webhook.auth_config.token;
        } else if (webhook.auth_type === 'basic') {
            if (webhook.auth_config?.username) document.getElementById('basicUsername').value = webhook.auth_config.username;
            if (webhook.auth_config?.password) document.getElementById('basicPassword').value = webhook.auth_config.password;
        }

        // Populate custom headers
        const headersContainer = document.getElementById('customHeaders');
        if (headersContainer && webhook.headers) {
            const existingRows = headersContainer.querySelectorAll('.header-row');
            const headerEntries = Object.entries(webhook.headers);
            headerEntries.forEach(([key, value], index) => {
                let row;
                if (index === 0 && existingRows.length > 0) {
                    row = existingRows[0];
                } else {
                    this.addHeaderRow();
                    row = headersContainer.lastElementChild;
                }
                if (row) {
                    const nameInput = row.querySelector('.header-name');
                    const valueInput = row.querySelector('.header-value');
                    if (nameInput) nameInput.value = key;
                    if (valueInput) valueInput.value = value;
                }
            });
        }
    },

    closeInlineWebhookForm() {
        this.inlineWebhookForm = null;
        this.currentWebhook = null;
        const container = document.getElementById('webhookInlineFormContainer');
        if (container) container.innerHTML = '';
    },

    showActionsManageView() {
        const alertsView = document.getElementById('alertsView');
        const alertEditorView = document.getElementById('alertEditorView');
        const actionsView = document.getElementById('actionsManageView');

        if (alertsView) alertsView.style.display = 'none';
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (actionsView) actionsView.style.display = 'block';

        // Close the alert editor panel if open
        this.closeAlertPanel();

        this.loadAllActions();
    },

    backToAlertsFromActions() {
        const alertsView = document.getElementById('alertsView');
        const actionsView = document.getElementById('actionsManageView');

        if (actionsView) actionsView.style.display = 'none';
        if (alertsView) alertsView.style.display = 'block';

        this.inlineWebhookForm = null;
        this.inlineFractalActionForm = null;
        this.inlineDictActionForm = null;
        this.closeInlineWebhookForm();
        this.closeInlineFractalActionForm();
        this.closeInlineDictActionForm();
    },

    closeModal(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.style.display = 'none';
        }
    },

    // YAML Import/Export
    async importYAML() {
        const yamlContent = document.getElementById('yamlContent')?.value.trim();
        const errorDiv = document.getElementById('importError');

        if (!yamlContent) {
            this.showError(errorDiv, 'Please enter YAML content');
            return;
        }

        try {
            const normalizerGroup = document.getElementById('importNormalizerGroup');
            const normalizerSelect = document.getElementById('importNormalizerSelect');
            const isSigmaVisible = normalizerGroup && normalizerGroup.style.display !== 'none';
            const normalizerID = isSigmaVisible && normalizerSelect ? normalizerSelect.value : '';

            let response;
            if (normalizerID) {
                response = await fetch('/api/v1/alerts/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ yaml_content: yamlContent, normalizer_id: normalizerID })
                });
            } else {
                response = await fetch('/api/v1/alerts/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'text/plain' },
                    credentials: 'include',
                    body: yamlContent
                });
            }

            const data = await response.json();

            if (data.success) {
                this.closeModal('importYamlModal');
                this.loadAlerts();
                const msg = isSigmaVisible ? 'Sigma rule imported successfully' : 'Alert imported successfully';
                Toast.show(msg, 'success');
            } else {
                this.showError(errorDiv, data.error || 'Import failed');
            }
        } catch (error) {
            console.error('YAML import error:', error);
            this.showError(errorDiv, 'Network error: ' + error.message);
        }
    },

    async exportYAML(alertId) {
        try {
            const response = await fetch(`/api/v1/alerts/${alertId}`, {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to get alert');
            }

            const alert = data.data.alert;
            const yamlContent = this.alertToYAML(alert);

            // Download as file
            const blob = new Blob([yamlContent], { type: 'text/yaml' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${alert.name.replace(/[^a-zA-Z0-9]/g, '_')}.yaml`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            Toast.show('Alert exported successfully', 'success');
        } catch (error) {
            console.error('Export error:', error);
            Toast.show('Failed to export alert', 'error');
        }
    },

    alertToYAML(alert) {
        const webhookNames = alert.webhook_actions ? alert.webhook_actions.map(wh => wh.name) : [];

        const alertType = alert.alert_type || 'event';
        let yaml = `name: ${alert.name}
description: |-
  ${alert.description || ''}
queryString: |-
  ${alert.query_string}
alertType: ${alertType}
severity: ${alert.severity || 'medium'}
actionNames:
${webhookNames.map(name => `- ${name}`).join('\n')}
labels:
${(alert.labels || []).map(label => `- ${label}`).join('\n')}${(alert.references && alert.references.length > 0) ? `
references:
${alert.references.map(ref => `- ${ref}`).join('\n')}` : ''}
enabled: ${alert.enabled}
throttleTimeSeconds: ${alert.throttle_time_seconds || 0}${alert.throttle_field ? `
throttleField: ${alert.throttle_field}` : ''}`;
        if (alertType === 'compound' && alert.window_duration) {
            yaml += `\nwindowDuration: ${alert.window_duration}`;
        }
        if (alertType === 'scheduled') {
            if (alert.schedule_cron) yaml += `\nscheduleCron: "${alert.schedule_cron}"`;
            if (alert.query_window_seconds) yaml += `\nqueryWindowSeconds: ${alert.query_window_seconds}`;
        }
        return yaml;
    },

    // Alert CRUD Operations
    async editAlert(alertId) {
        this.showAlertEditor(alertId);
    },

    async toggleAlert(alertId, enabled) {
        try {
            // First get the current alert data
            const getResponse = await fetch(`/api/v1/alerts/${alertId}`, {
                method: 'GET',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include'
            });

            if (!getResponse.ok) {
                throw new Error('Failed to get alert data');
            }

            const alertData = await getResponse.json();
            if (!alertData.success) {
                throw new Error(alertData.error || 'Failed to get alert data');
            }

            const alert = alertData.data.alert;

            // Now send complete update with only enabled field changed
            const updateResponse = await fetch(`/api/v1/alerts/${alertId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify({
                    name: alert.name,
                    description: alert.description || '',
                    query_string: alert.query_string,
                    alert_type: alert.alert_type || 'event',
                    webhook_action_ids: alert.webhook_actions ? alert.webhook_actions.map(wa => wa.id) : [],
                    fractal_action_ids: alert.fractal_actions ? alert.fractal_actions.map(fa => fa.id) : [],
                    dictionary_action_ids: alert.dictionary_actions ? alert.dictionary_actions.map(da => da.id) : [],
                    labels: alert.labels || [],
                    enabled: enabled,
                    throttle_time_seconds: alert.throttle_time_seconds || 0,
                    throttle_field: alert.throttle_field || '',
                    window_duration: alert.window_duration || null,
                    schedule_cron: alert.schedule_cron || null,
                    query_window_seconds: alert.query_window_seconds || null
                })
            });

            const data = await updateResponse.json();
            if (data.success) {
                this.loadAlerts();
                Toast.show(`Alert ${enabled ? 'enabled' : 'disabled'}`, 'success');
            } else {
                throw new Error(data.error || 'Failed to toggle alert');
            }
        } catch (error) {
            console.error('Toggle alert error:', error);
            Toast.show('Failed to toggle alert: ' + error.message, 'error');
        }
    },

    async deleteAlert(alertId) {
        if (!confirm('Are you sure you want to delete this alert? This cannot be undone.')) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/alerts/${alertId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();
            if (data.success) {
                this.loadAlerts();
                Toast.show('Alert deleted successfully', 'success');
            } else {
                throw new Error(data.error || 'Failed to delete alert');
            }
        } catch (error) {
            console.error('Delete alert error:', error);
            Toast.show('Failed to delete alert', 'error');
        }
    },

    async saveAlert() {
        const formData = this.getAlertFormData();
        if (!formData) return;

        try {
            const url = this.currentAlert
                ? `/api/v1/alerts/${this.currentAlert.id}`
                : '/api/v1/alerts';

            const method = this.currentAlert ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(formData)
            });

            const data = await response.json();
            if (data.success) {
                this.closeModal('alertModal');
                this.loadAlerts();
                Toast.show(`Alert ${this.currentAlert ? 'updated' : 'created'} successfully`, 'success');
            } else {
                this.showError(document.getElementById('alertError'), data.error || 'Failed to save alert');
            }
        } catch (error) {
            console.error('Save alert error:', error);
            this.showError(document.getElementById('alertError'), 'Network error: ' + error.message);
        }
    },

    getAlertFormData() {
        const name = document.getElementById('alertName')?.value.trim();
        const description = document.getElementById('alertDescription')?.value.trim();
        const queryString = document.getElementById('editorQueryInput')?.value.trim();
        const labels = document.getElementById('alertLabels')?.value.split(',').map(s => s.trim()).filter(s => s);
        const throttleTime = parseInt(document.getElementById('alertThrottleTime')?.value) || 0;
        const throttleField = document.getElementById('alertThrottleField')?.value.trim();
        const enabled = document.getElementById('alertEnabled')?.checked || false;

        // Get selected webhooks
        const webhookCheckboxes = document.querySelectorAll('#alertWebhooksList input[type="checkbox"]:checked');
        const webhookActionIDs = Array.from(webhookCheckboxes).map(cb => cb.value);

        // Validation
        const errorDiv = document.getElementById('alertError');
        if (!name) {
            this.showError(errorDiv, 'Alert name is required');
            return null;
        }
        if (!queryString) {
            this.showError(errorDiv, 'Query string is required');
            return null;
        }

        this.hideError(errorDiv);

        return {
            name,
            description,
            query_string: queryString,
            labels,
            throttle_time_seconds: throttleTime,
            throttle_field: throttleField,
            enabled,
            webhook_action_ids: webhookActionIDs
        };
    },

    async loadAlertForEdit(alertId) {
        try {
            const response = await fetch(`/api/v1/alerts/${alertId}`, {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load alert');
            }

            const alert = data.data.alert;
            this.currentAlert = alert;

            // Populate form
            document.getElementById('alertName').value = alert.name || '';
            document.getElementById('alertDescription').value = alert.description || '';

            const alertQueryTextarea = document.getElementById('editorQueryInput');
            alertQueryTextarea.value = alert.query_string || '';

            // Trigger syntax highlighting manually
            setTimeout(() => {
                if (window.SyntaxHighlight) {
                    SyntaxHighlight.updateHighlight('editorQueryInput', 'alertQueryHighlight');
                }
            }, 200);

            document.getElementById('alertLabels').value = (alert.labels || []).join(', ');
            document.getElementById('alertThrottleTime').value = alert.throttle_time_seconds || 0;
            document.getElementById('alertThrottleField').value = alert.throttle_field || '';
            document.getElementById('alertEnabled').checked = alert.enabled;

            // Update webhook selections after loading webhooks
            this.selectedWebhookIds = (alert.webhook_actions || []).map(wh => wh.id);

            // Update fractal action selections
            this.selectedFractalActionIds = (alert.fractal_actions || []).map(fa => fa.id);

        } catch (error) {
            console.error('Load alert error:', error);
            Toast.show('Failed to load alert for editing', 'error');
        }
    },

    resetAlertForm() {
        const inputs = ['alertName', 'alertDescription', 'editorQueryInput', 'alertLabels', 'alertThrottleField'];
        inputs.forEach(id => {
            const element = document.getElementById(id);
            if (element) element.value = '';
        });

        const throttleTime = document.getElementById('alertThrottleTime');
        if (throttleTime) throttleTime.value = '0';

        const enabled = document.getElementById('alertEnabled');
        if (enabled) enabled.checked = true;

        this.hideError(document.getElementById('alertError'));
        this.selectedWebhookIds = [];
        this.selectedFractalActionIds = [];
    },

    // Webhook Operations
    async loadWebhooks() {
        const webhooksList = document.getElementById('webhooksList');
        if (!webhooksList) return;

        try {
            const response = await fetch('/api/v1/webhooks', {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load webhooks');
            }

            this.renderWebhooks(data.data.webhooks || []);
        } catch (error) {
            console.error('Load webhooks error:', error);
            webhooksList.innerHTML = '<div class="error">Failed to load webhooks</div>';
        }
    },

    async loadWebhooksForAlert() {
        const container = document.getElementById('alertWebhooksList');
        if (!container) return;

        try {
            const response = await fetch('/api/v1/webhooks', {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load webhooks');
            }

            const webhooks = data.data.webhooks || [];
            if (webhooks.length === 0) {
                container.innerHTML = '<div class="empty-text">No webhooks configured. <a href="#" onclick="Alerts.showInlineWebhookCreate()">Create one</a>?</div>';
                return;
            }

            const webhooksHTML = webhooks.map(webhook => {
                const isSelected = this.selectedWebhookIds && this.selectedWebhookIds.includes(webhook.id);
                return `
                    <label class="webhook-checkbox">
                        <input type="checkbox" value="${webhook.id}" ${isSelected ? 'checked' : ''}>
                        <span class="webhook-name">${Utils.escapeHtml(webhook.name)}</span>
                        <span class="webhook-url">${Utils.escapeHtml(webhook.url)}</span>
                    </label>
                `;
            }).join('');

            container.innerHTML = webhooksHTML;
        } catch (error) {
            console.error('Load webhooks for alert error:', error);
            container.innerHTML = '<div class="error">Failed to load webhooks</div>';
        }
    },

    renderWebhooks(webhooks) {
        const container = document.getElementById('webhooksList');
        if (!container) return;

        if (webhooks.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-text">No webhooks configured</div>
                    <button onclick="Alerts.showInlineWebhookCreate()" class="btn-primary">Create Your First Webhook</button>
                </div>
            `;
            return;
        }

        const webhooksHTML = webhooks.map(webhook => `
            <div class="webhook-card ${webhook.enabled ? 'enabled' : 'disabled'}">
                <div class="webhook-header">
                    <h4>${Utils.escapeHtml(webhook.name)}</h4>
                    <div class="webhook-actions">
                        <button onclick="Alerts.editWebhook('${webhook.id}')" class="btn-sm btn-secondary">Edit</button>
                        <button onclick="Alerts.testWebhookDirect('${webhook.id}')" class="btn-sm btn-secondary">Test</button>
                        <button onclick="Alerts.deleteWebhook('${webhook.id}')" class="btn-sm btn-danger">Delete</button>
                    </div>
                </div>
                <div class="webhook-details">
                    <div><strong>URL:</strong> ${Utils.escapeHtml(webhook.url)}</div>
                    <div><strong>Method:</strong> ${webhook.method}</div>
                    <div><strong>Status:</strong> ${webhook.enabled ? 'Enabled' : 'Disabled'}</div>
                </div>
            </div>
        `).join('');

        container.innerHTML = webhooksHTML;
    },

    async editWebhook(webhookId) {
        this.showInlineWebhookEdit(webhookId);
    },

    async deleteWebhook(webhookId) {
        if (!confirm('Are you sure you want to delete this webhook? This cannot be undone.')) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/webhooks/${webhookId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();
            if (data.success) {
                this.loadAllActions();
                Toast.show('Webhook deleted successfully', 'success');
            } else {
                throw new Error(data.error || 'Failed to delete webhook');
            }
        } catch (error) {
            console.error('Delete webhook error:', error);
            Toast.show('Failed to delete webhook: ' + error.message, 'error');
        }
    },

    async testWebhookDirect(webhookId) {
        try {
            const response = await fetch(`/api/v1/webhooks/${webhookId}/test`, {
                method: 'POST',
                credentials: 'include'
            });

            const data = await response.json();
            if (data.success) {
                const result = data.data.test_result;
                if (result.success) {
                    Toast.show('Webhook test successful', 'success');
                } else {
                    Toast.show(`Webhook test failed: ${result.error}`, 'error');
                }
            } else {
                throw new Error(data.error || 'Test failed');
            }
        } catch (error) {
            console.error('Test webhook error:', error);
            Toast.show('Failed to test webhook: ' + error.message, 'error');
        }
    },

    async saveWebhook() {
        const formData = this.getWebhookFormData();
        if (!formData) return;

        try {
            const url = this.currentWebhook
                ? `/api/v1/webhooks/${this.currentWebhook.id}`
                : '/api/v1/webhooks';

            const method = this.currentWebhook ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(formData)
            });

            const data = await response.json();
            if (data.success) {
                this.closeInlineWebhookForm();
                this.loadAllActions();
                Toast.show(`Webhook ${this.currentWebhook ? 'updated' : 'created'} successfully`, 'success');
            } else {
                this.showError(document.getElementById('webhookError'), data.error || 'Failed to save webhook');
            }
        } catch (error) {
            console.error('Save webhook error:', error);
            this.showError(document.getElementById('webhookError'), 'Network error: ' + error.message);
        }
    },

    async testWebhook() {
        if (this.currentWebhook) {
            // Test existing webhook
            this.testWebhookDirect(this.currentWebhook.id);
        } else {
            Toast.show('Please save the webhook first before testing', 'warning');
        }
    },

    getWebhookFormData() {
        const name = document.getElementById('webhookName')?.value.trim();
        const url = document.getElementById('webhookUrl')?.value.trim();
        const method = document.getElementById('webhookMethod')?.value || 'POST';
        const timeout = parseInt(document.getElementById('webhookTimeout')?.value) || 30;
        const retries = parseInt(document.getElementById('webhookRetries')?.value) || 3;
        const authType = document.getElementById('webhookAuthType')?.value || 'none';
        const enabled = document.getElementById('webhookEnabled')?.checked || false;

        // Get custom headers
        const headerRows = document.querySelectorAll('#customHeaders .header-row');
        const headers = {};
        headerRows.forEach(row => {
            const nameInput = row.querySelector('.header-name');
            const valueInput = row.querySelector('.header-value');
            if (nameInput?.value.trim() && valueInput?.value.trim()) {
                headers[nameInput.value.trim()] = valueInput.value.trim();
            }
        });

        // Get auth config
        const authConfig = {};
        if (authType === 'bearer') {
            const token = document.getElementById('bearerToken')?.value.trim();
            if (token) authConfig.token = token;
        } else if (authType === 'basic') {
            const username = document.getElementById('basicUsername')?.value.trim();
            const password = document.getElementById('basicPassword')?.value.trim();
            if (username) authConfig.username = username;
            if (password) authConfig.password = password;
        }

        // Validation
        const errorDiv = document.getElementById('webhookError');
        if (!name) {
            this.showError(errorDiv, 'Webhook name is required');
            return null;
        }
        if (!url) {
            this.showError(errorDiv, 'Webhook URL is required');
            return null;
        }

        // Basic URL validation
        try {
            new URL(url);
        } catch (e) {
            this.showError(errorDiv, 'Invalid webhook URL');
            return null;
        }

        this.hideError(errorDiv);

        const includeAlertLink = document.getElementById('webhookIncludeAlertLink')?.checked ?? true;

        return {
            name,
            url,
            method,
            headers,
            auth_type: authType,
            auth_config: authConfig,
            timeout_seconds: timeout,
            retry_count: retries,
            include_alert_link: includeAlertLink,
            enabled
        };
    },

    // loadWebhookForEdit and resetWebhookForm removed - replaced by inline form rendering

    // Utility Methods
    showError(element, message) {
        if (element) {
            element.textContent = message;
            element.style.display = 'block';
        }
    },

    hideError(element) {
        if (element) {
            element.style.display = 'none';
        }
    },

    handleAuthTypeChange(authType) {
        const authConfig = document.getElementById('webhookAuthConfig');
        const bearerConfig = document.getElementById('bearerConfig');
        const basicConfig = document.getElementById('basicConfig');

        if (!authConfig) return;

        if (authType === 'none') {
            authConfig.style.display = 'none';
        } else {
            authConfig.style.display = 'block';
            bearerConfig.style.display = authType === 'bearer' ? 'block' : 'none';
            basicConfig.style.display = authType === 'basic' ? 'block' : 'none';
        }
    },

    addHeaderRow() {
        const container = document.getElementById('customHeaders');
        if (!container) return;

        const headerRow = document.createElement('div');
        headerRow.className = 'header-row';
        headerRow.innerHTML = `
            <input type="text" placeholder="Header name" class="header-name">
            <input type="text" placeholder="Header value" class="header-value">
            <button type="button" class="btn-sm btn-danger" onclick="this.parentElement.remove()">-</button>
        `;

        container.appendChild(headerRow);
    },

    // ============================
    // Alert Editor Functions
    // ============================

    showAlertEditor(alertId = null, opts = {}) {
        // Track if we're editing a feed alert
        this.editingFeedAlert = opts.fromFeed || false;
        this.feedAlertOriginalId = this.editingFeedAlert ? alertId : null;

        // Hide alerts tab content (sub-tabs + list views) and show editor
        const alertsTabContent = document.getElementById('fractalAlertsTabContent');
        const alertEditorView = document.getElementById('alertEditorView');

        if (alertsTabContent) alertsTabContent.style.display = 'none';
        if (alertEditorView) alertEditorView.style.display = 'block';

        // Set up editor for create vs edit mode
        const saveBtn = document.getElementById('saveAlertBtn');

        if (alertId && !this.editingFeedAlert) {
            // Edit mode (manual alert)
            if (saveBtn) saveBtn.textContent = 'Update Alert';
        } else if (this.editingFeedAlert) {
            // Feed alert mode: will create a copy on save
            if (saveBtn) saveBtn.textContent = 'Save as Manual Alert';
        } else {
            // Create mode
            if (saveBtn) saveBtn.textContent = 'Create Alert';
            this.clearAlertEditor();
        }

        // Set up query input with debounced testing FIRST
        this.setupQueryTesting();

        // Set up alert editor pagination
        this.setupAlertPagination();

        // Set up SQL toggle functionality
        this.setupAlertSqlToggle();

        if (alertId) {
            // Load alert data (which also loads actions with pre-selected state)
            this.loadAlertIntoEditor(alertId);
        } else {
            // Load available actions for new alert
            this.loadActionsIntoEditor();
        }

        // Automatically open the configuration panel for immediate access
        setTimeout(() => {
            this.openAlertPanel();
        }, 100);
    },

    backToAlerts() {
        const wasFromFeed = this.editingFeedAlert;
        this.editingFeedAlert = false;
        this.feedAlertOriginalId = null;

        // Remove feed alert banner if present
        const banner = document.getElementById('feedAlertBanner');
        if (banner) banner.remove();

        // Hide alert editor and restore alerts tab content
        const alertEditorView = document.getElementById('alertEditorView');
        const alertsTabContent = document.getElementById('fractalAlertsTabContent');
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (alertsTabContent) alertsTabContent.style.display = 'block';

        // Return to the correct view
        if (wasFromFeed) {
            const feedAlertsView = document.getElementById('feedAlertsView');
            if (feedAlertsView) feedAlertsView.style.display = 'block';
            if (window.AlertFeeds) AlertFeeds.show();
        } else {
            const alertsView = document.getElementById('alertsView');
            if (alertsView) alertsView.style.display = 'block';
            this.loadAlerts();
        }

        // Close the right-hand configuration panel
        this.closeAlertPanel();

        // Clear the editor
        this.clearAlertEditor();
    },

    clearAlertEditor() {
        // Clear all form fields
        const nameField = document.getElementById('editorAlertName');
        const descField = document.getElementById('editorAlertDescription');
        const queryField = document.getElementById('editorQueryInput');
        const labelsField = document.getElementById('editorAlertLabels');
        const referencesField = document.getElementById('editorAlertReferences');
        const throttleTimeField = document.getElementById('editorThrottleTime');
        const throttleFieldField = document.getElementById('editorThrottleField');
        const enabledField = document.getElementById('editorAlertEnabled');

        if (nameField) nameField.value = '';
        if (descField) descField.value = '';
        if (queryField) queryField.value = '';
        if (labelsField) labelsField.value = '';
        this.setLabelsFromArray([]);
        if (referencesField) referencesField.value = '';
        if (throttleTimeField) throttleTimeField.value = '0';
        if (throttleFieldField) throttleFieldField.value = '';
        if (enabledField) enabledField.checked = true;

        // Reset severity
        this.setSeverity('medium');

        // Reset references container
        const refsContainer = document.getElementById('editorReferencesContainer');
        if (refsContainer) refsContainer.innerHTML = '';

        // Reset alert type to event
        const alertTypeSelect = document.getElementById('alertTypeSelect');
        if (alertTypeSelect) alertTypeSelect.value = 'event';
        this.setAlertTypeCard('event');
        const windowGroup = document.getElementById('windowDurationGroup');
        if (windowGroup) windowGroup.style.display = 'none';
        const scheduledGroup = document.getElementById('scheduledConfigGroup');
        if (scheduledGroup) scheduledGroup.style.display = 'none';
        const windowDur = document.getElementById('editorWindowDuration');
        if (windowDur) windowDur.value = '15';
        const windowUnit = document.getElementById('editorWindowUnit');
        if (windowUnit) windowUnit.value = '60';
        // Reset scheduled fields
        const schedPreset = document.getElementById('editorSchedulePreset');
        if (schedPreset) schedPreset.value = '0 0 * * *';
        const customCronGroup = document.getElementById('customCronGroup');
        if (customCronGroup) customCronGroup.style.display = 'none';
        const schedCron = document.getElementById('editorScheduleCron');
        if (schedCron) schedCron.value = '';
        const qwValue = document.getElementById('editorQueryWindowValue');
        if (qwValue) qwValue.value = '1';
        const qwUnit = document.getElementById('editorQueryWindowUnit');
        if (qwUnit) qwUnit.value = '86400';

        // Clear actions selection
        const actionsSelect = document.getElementById('editorActionsSelect');
        const selectedActions = document.getElementById('editorSelectedActions');
        if (actionsSelect) actionsSelect.innerHTML = '<option value="">Add action...</option>';
        if (selectedActions) selectedActions.innerHTML = '';
        this.selectedWebhookIds = [];
        this.selectedFractalActionIds = [];
        this.selectedEmailActionIds = [];
        this.updateActionCountBadge();

        // Clear results
        const resultsDiv = document.getElementById('queryResults');
        if (resultsDiv) {
            resultsDiv.innerHTML = '<div class="no-results"><p>Enter a query above to see live results</p></div>';
        }

        this.currentAlert = null;
    },

    async testQuery() {
        const queryInput = document.getElementById('editorQueryInput');
        const resultsDiv = document.getElementById('queryResults');
        const countDiv = document.getElementById('alertResultsCount');

        if (!queryInput || !resultsDiv) return;

        const rawQuery = queryInput.value.trim();
        if (!rawQuery) {
            resultsDiv.innerHTML = '<div class="no-results"><p>Enter a query above to see live results</p></div>';
            if (countDiv) countDiv.textContent = '0 results';
            return;
        }

        // Strip comment lines (lines starting with //)
        const query = this.stripComments(rawQuery);
        if (!query) {
            resultsDiv.innerHTML = '<div class="no-results"><p>Enter a query above to see live results</p></div>';
            if (countDiv) countDiv.textContent = '0 results';
            return;
        }

        // Cancel previous request if still running
        if (this.currentTestRequest) {
            this.currentTestRequest.abort();
        }

        // Show loading
        resultsDiv.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        if (countDiv) countDiv.textContent = 'Testing...';

        try {
            // Get time range like the main search view does
            const timeRange = this.getTimeRange();


            // Create abort controller for cancellation
            const controller = new AbortController();
            this.currentTestRequest = controller;

            // Get currently selected fractal for context
            let requestBody = {
                query: query,
                start: timeRange.start,
                end: timeRange.end
            };

            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            // Use the same API structure as main search view (no limit parameter)
            const response = await fetch('/api/v1/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                signal: controller.signal,
                body: JSON.stringify(requestBody)
            });


            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            const data = await response.json();


            // Display SQL output using QueryExecutor's method
            const sqlOutput = document.getElementById('alertSqlOutput');
            if (data.sql && sqlOutput && window.QueryExecutor) {
                sqlOutput.innerHTML = QueryExecutor.highlightSQL(data.sql);
                // Show SQL preview if user has it enabled
                const alertSqlPreview = document.querySelector('#alertEditorView .sql-preview');
                if (alertSqlPreview && window.UserPrefs && UserPrefs.showSQL()) {
                    alertSqlPreview.style.display = 'block';
                }
            }

            if (!data.success) {
                throw new Error(data.error || 'Query failed');
            }

            // Store data exactly like the main search view does
            const results = data.results || [];
            this.currentResults = results;
            this.fieldOrder = data.field_order || null;
            this.isAggregated = data.is_aggregated || false;

            if (countDiv) {
                countDiv.textContent = `${results.length} results`;
            }

            if (results.length === 0) {
                resultsDiv.innerHTML = '<div class="no-results"><p>No results found for this query in the selected time range</p></div>';
                this.hideAlertPagination();
            } else {
                // Use pagination and shared rendering
                this.alertCurrentPage = 1; // Reset to first page
                this.showAlertPagination();
                this.updateAlertPagination();

                // Use QueryExecutor's shared rendering method
                const targetElement = document.getElementById('queryResults');
                const pageResults = this.getCurrentAlertPageResults();
                if (window.QueryExecutor) {
                    QueryExecutor.renderResultsToElement(pageResults, targetElement, this.fieldOrder, {
                        allResults: this.currentResults,
                        isAggregated: this.isAggregated,
                        disableDetailView: true
                    });
                }

                // Show export CSV button when results are available
                const alertExportBtn = document.getElementById('alertExportCsvBtn');
                if (alertExportBtn && this.currentResults && this.currentResults.length > 0) {
                    alertExportBtn.style.display = 'inline-block';
                }
            }

            // Clear the request reference
            this.currentTestRequest = null;

        } catch (error) {
            this.currentTestRequest = null;

            // Don't show error if request was aborted (user typed while request was running)
            if (error.name === 'AbortError') {
                return;
            }

            console.error('Query test error:', error);
            resultsDiv.innerHTML = `<div class="query-error"><p>Query Error: ${Utils.escapeHtml(error.message)}</p></div>`;
            if (countDiv) countDiv.textContent = 'Error';

            // Hide export CSV button when there's an error
            const alertExportBtn = document.getElementById('alertExportCsvBtn');
            if (alertExportBtn) {
                alertExportBtn.style.display = 'none';
            }
        }
    },

    getTimeRange() {
        const timeRangeSelect = document.getElementById('alertTimeRange');
        const customStart = document.getElementById('alertCustomStart');
        const customEnd = document.getElementById('alertCustomEnd');

        if (!timeRangeSelect) {
            return {
                start: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
                end: new Date().toISOString()
            };
        }

        const value = timeRangeSelect.value;
        const now = new Date();
        let start, end = now;

        switch (value) {
            case '1h':
                start = new Date(now - 60 * 60 * 1000);
                break;
            case '6h':
                start = new Date(now - 6 * 60 * 60 * 1000);
                break;
            case '24h':
                start = new Date(now - 24 * 60 * 60 * 1000);
                break;
            case '7d':
                start = new Date(now - 7 * 24 * 60 * 60 * 1000);
                break;
            case '30d':
                start = new Date(now - 30 * 24 * 60 * 60 * 1000);
                break;
            case 'custom':
                if (customStart && customEnd && customStart.value && customEnd.value) {
                    const startDate = new Date(customStart.value);
                    const endDate = new Date(customEnd.value);

                    // Validate that start is before end
                    if (startDate < endDate) {
                        return {
                            start: startDate.toISOString(),
                            end: endDate.toISOString()
                        };
                    }
                }
                // Fallback to default if custom range is invalid
                start = new Date(now - 24 * 60 * 60 * 1000);
                break;
            default:
                start = new Date(now - 24 * 60 * 60 * 1000);
        }

        return {
            start: start.toISOString(),
            end: end.toISOString()
        };
    },

    setupQueryTesting() {
        const queryInput = document.getElementById('editorQueryInput');
        if (queryInput) {
            // Remove automatic testing - user will manually click "Test Query"
            // Only clear results if query becomes empty
            queryInput.addEventListener('input', () => {
                // Save to history (unless we're in undo/redo operation)
                if (!this.isUndoRedoing) {
                    setTimeout(() => {
                        this.saveToHistory(queryInput.value);
                    }, 0);
                }

                if (!queryInput.value.trim()) {
                    const resultsDiv = document.getElementById('queryResults');
                    const countDiv = document.getElementById('alertResultsCount');
                    if (resultsDiv) {
                        resultsDiv.innerHTML = '<div class="no-results"><p>Enter a query above to see live results</p></div>';
                    }
                    if (countDiv) {
                        countDiv.textContent = '0 results';
                    }
                    this.currentResults = [];
                    this.hideAlertPagination();
                }
            });

            // Add keyboard handling for alert editor query input
            queryInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.testQuery();
                } else if (e.key === 'Enter' && e.shiftKey) {
                    // Allow new line (default behavior)
                } else if (e.key === 'Tab') {
                    e.preventDefault();
                    const start = queryInput.selectionStart;
                    const end = queryInput.selectionEnd;
                    const value = queryInput.value;

                    // Insert tab character at cursor position
                    queryInput.value = value.substring(0, start) + '\t' + value.substring(end);

                    // Move cursor after the inserted tab
                    queryInput.selectionStart = queryInput.selectionEnd = start + 1;

                    // Trigger input event to maintain consistency
                    queryInput.dispatchEvent(new Event('input'));
                } else if (e.key === '/' && e.ctrlKey) {
                    e.preventDefault();
                    this.toggleLineComment(queryInput);
                } else if (e.key === 'z' && e.ctrlKey && !e.shiftKey) {
                    e.preventDefault();
                    this.undo(queryInput);
                } else if ((e.key === 'y' && e.ctrlKey) || (e.key === 'z' && e.ctrlKey && e.shiftKey)) {
                    e.preventDefault();
                    this.redo(queryInput);
                }
            });
        }

        // Set up time range controls
        const timeRangeSelect = document.getElementById('alertTimeRange');
        const customTimeInputs = document.getElementById('alertCustomTimeInputs');
        const customStart = document.getElementById('alertCustomStart');
        const customEnd = document.getElementById('alertCustomEnd');

        if (timeRangeSelect && customTimeInputs) {
            timeRangeSelect.addEventListener('change', (e) => {
                if (e.target.value === 'custom') {
                    customTimeInputs.style.display = 'flex';
                    // Initialize custom inputs with default values if empty
                    if (customStart && customEnd) {
                        if (!customStart.value) {
                            const now = new Date();
                            const oneDayAgo = new Date(now - 24 * 60 * 60 * 1000);
                            customStart.value = oneDayAgo.toISOString().slice(0, 16).replace('T', ' ');
                            customEnd.value = now.toISOString().slice(0, 16).replace('T', ' ');
                        }
                    }
                } else {
                    customTimeInputs.style.display = 'none';
                }
                // Time range changed - user can manually re-test if needed
            });

            // Initialize time inputs on load
            if (timeRangeSelect.value === 'custom' && customStart && customEnd) {
                customTimeInputs.style.display = 'flex';
                if (!customStart.value || !customEnd.value) {
                    const now = new Date();
                    const oneDayAgo = new Date(now - 24 * 60 * 60 * 1000);
                    customStart.value = oneDayAgo.toISOString().slice(0, 16).replace('T', ' ');
                    customEnd.value = now.toISOString().slice(0, 16).replace('T', ' ');
                }
            }

            // Time inputs setup - user can manually re-test query if needed
        }
    },

    toggleLineComment(textarea) {
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const value = textarea.value;

        // Find the start and end of the current line(s)
        const beforeStart = value.lastIndexOf('\n', start - 1);
        const lineStart = beforeStart === -1 ? 0 : beforeStart + 1;

        const afterEnd = value.indexOf('\n', end);
        const lineEnd = afterEnd === -1 ? value.length : afterEnd;

        // Get the selected lines
        const selectedText = value.substring(lineStart, lineEnd);
        const lines = selectedText.split('\n');

        // Check if all non-empty lines are commented
        const nonEmptyLines = lines.filter(line => line.trim() !== '');
        const allCommented = nonEmptyLines.length > 0 && nonEmptyLines.every(line => line.trim().startsWith('//'));

        // Toggle comments on all lines
        const modifiedLines = lines.map(line => {
            if (line.trim() === '') return line; // Skip empty lines

            if (allCommented) {
                // Remove comment - find first occurrence of // and remove it
                const commentIndex = line.indexOf('//');
                if (commentIndex !== -1) {
                    return line.substring(0, commentIndex) + line.substring(commentIndex + 2);
                }
                return line;
            } else {
                // Add comment at the beginning of the line (after leading whitespace)
                const match = line.match(/^(\s*)(.*)/);
                if (match) {
                    return match[1] + '//' + match[2];
                }
                return '//' + line;
            }
        });

        const newSelectedText = modifiedLines.join('\n');

        // Replace the text
        const newValue = value.substring(0, lineStart) + newSelectedText + value.substring(lineEnd);
        textarea.value = newValue;

        // Adjust selection to include the modified lines
        const lengthDiff = newSelectedText.length - selectedText.length;
        textarea.selectionStart = lineStart;
        textarea.selectionEnd = lineEnd + lengthDiff;

        // Trigger input event to update syntax highlighting
        textarea.dispatchEvent(new Event('input'));

        // Force save to history after comment toggle
        this.saveToHistoryImmediate(textarea.value, true);
    },

    shouldSaveHistory(oldValue, newValue) {
        // Always save if it's a significant change in length (paste, delete block, etc.)
        const lengthDiff = Math.abs(newValue.length - oldValue.length);
        if (lengthDiff >= 4) return true;

        // Save at word boundaries - when we finish typing a word of 4+ characters
        const oldWords = oldValue.split(/\s+/).filter(w => w.length > 0);
        const newWords = newValue.split(/\s+/).filter(w => w.length > 0);

        // If we added a new word and it's 4+ characters, save
        if (newWords.length > oldWords.length) {
            const lastWord = newWords[newWords.length - 1];
            if (lastWord.length >= 4) return true;
        }

        // If we finished a word (added space or punctuation after 4+ chars)
        if (newValue.length > oldValue.length) {
            const lastChar = newValue[newValue.length - 1];
            if (/[\s|,;.!?(){}[\]]/.test(lastChar)) {
                // Check if the word before this separator is 4+ chars
                const beforeSeparator = newValue.substring(0, newValue.length - 1).split(/[\s|,;.!?(){}[\]]+/).pop();
                if (beforeSeparator && beforeSeparator.length >= 4) return true;
            }
        }

        return false;
    },

    saveToHistoryImmediate(value, force = false) {
        const history = this.queryHistory;
        // Don't save if the value is the same as the current state
        if (!force && history.states[history.currentFractal] === value) {
            return;
        }

        // Remove any states after current index (when we type after undoing)
        history.states = history.states.slice(0, history.currentFractal + 1);

        // Add new state
        history.states.push(value);
        history.currentFractal = history.states.length - 1;

        // Limit history size
        if (history.states.length > history.maxSize) {
            history.states.shift();
            history.currentFractal--;
        }
    },

    saveToHistoryDebounced(value) {
        // Clear existing timer
        if (this.historyTimer) {
            clearTimeout(this.historyTimer);
        }

        // Set new timer to save after 1 second of inactivity
        this.historyTimer = setTimeout(() => {
            this.saveToHistoryImmediate(value);
        }, 1000);
    },

    saveToHistory(value) {
        const history = this.queryHistory;
        const oldValue = history.states[history.currentFractal] || '';

        // Check if we should save immediately
        if (this.shouldSaveHistory(oldValue, value)) {
            this.saveToHistoryImmediate(value);
        } else {
            // Otherwise, use debounced save for pauses in typing
            this.saveToHistoryDebounced(value);
        }
    },

    undo(textarea) {
        const history = this.queryHistory;
        if (history.currentFractal > 0) {
            history.currentFractal--;
            const newValue = history.states[history.currentFractal];
            this.isUndoRedoing = true;
            textarea.value = newValue;

            // Trigger input event to update syntax highlighting
            textarea.dispatchEvent(new Event('input'));
            this.isUndoRedoing = false;
        }
    },

    redo(textarea) {
        const history = this.queryHistory;
        if (history.currentFractal < history.states.length - 1) {
            history.currentFractal++;
            const newValue = history.states[history.currentFractal];
            this.isUndoRedoing = true;
            textarea.value = newValue;

            // Trigger input event to update syntax highlighting
            textarea.dispatchEvent(new Event('input'));
            this.isUndoRedoing = false;
        }
    },

    // Strip comment lines (lines starting with //) from query
    stripComments(query) {
        return query
            .split('\n')
            .filter(line => !line.trim().startsWith('//'))
            .join('\n')
            .trim();
    },

    setupAlertPagination() {
        const alertPageSizeSelect = document.getElementById('alertPageSizeSelect');
        const alertPrevBtn = document.getElementById('alertPrevPageBtn');
        const alertNextBtn = document.getElementById('alertNextPageBtn');

        if (alertPageSizeSelect) {
            alertPageSizeSelect.addEventListener('change', (e) => {
                this.alertPageSizeChanged(e.target.value);
            });
        }

        if (alertPrevBtn) {
            alertPrevBtn.addEventListener('click', () => this.alertPrevPage());
        }

        if (alertNextBtn) {
            alertNextBtn.addEventListener('click', () => this.alertNextPage());
        }
    },

    setupAlertSqlToggle() {
        const toggleSqlBtn = document.getElementById('alertToggleSqlBtn');
        const sqlOutput = document.getElementById('alertSqlOutput');

        if (toggleSqlBtn && sqlOutput) {
            toggleSqlBtn.addEventListener('click', () => {
                const isHidden = sqlOutput.style.display === 'none' || !sqlOutput.style.display;
                sqlOutput.style.display = isHidden ? 'block' : 'none';
                toggleSqlBtn.textContent = isHidden ? 'Hide SQL' : 'Show SQL';
            });
        }
    },

    // Column sorting for alert editor (identical to main search)
    addAlertSortHandlers(table) {
        const headers = table.querySelectorAll('th.sortable');
        headers.forEach(header => {
            header.addEventListener('click', (e) => {
                // Don't trigger sort when clicking resizer or during drag
                if (e.target.classList.contains('column-resizer') || header.classList.contains('dragging')) {
                    return;
                }

                const field = header.dataset.field;
                this.sortAlertResults(field);
            });
        });
    },

    sortAlertResults(field) {
        if (this.sortColumn === field) {
            // Toggle direction if same column
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            // New column, default to ascending
            this.sortColumn = field;
            this.sortDirection = 'asc';
        }

        // Sort the results
        const sorted = [...this.currentResults].sort((a, b) => {
            let aVal = a[field];
            let bVal = b[field];

            // Handle null/undefined values
            if (aVal === null || aVal === undefined) aVal = '';
            if (bVal === null || bVal === undefined) bVal = '';

            // Convert to strings for comparison
            aVal = String(aVal).toLowerCase();
            bVal = String(bVal).toLowerCase();

            if (this.sortDirection === 'asc') {
                return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
            } else {
                return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
            }
        });

        // Update results and re-render
        this.currentResults = sorted;
        this.alertCurrentPage = 1; // Reset to first page after sort
        this.updateAlertPagination();

        // Use shared rendering method
        const targetElement = document.getElementById('queryResults');
        const pageResults = this.getCurrentAlertPageResults();
        if (window.QueryExecutor) {
            QueryExecutor.renderResultsToElement(pageResults, targetElement, this.fieldOrder, {
                allResults: this.currentResults,
                isAggregated: this.isAggregated
            });
        }
    },

    // Panel controls
    toggleAlertPanel() {
        const panel = document.getElementById('alertConfigPanel');
        if (!panel) return;
        const isOpen = panel.classList.contains('open');
        if (isOpen) {
            this.closeAlertPanel();
        } else {
            this.openAlertPanel();
        }
    },

    closeAlertPanel() {
        const panel = document.getElementById('alertConfigPanel');
        const toggleBtn = document.getElementById('toggleAlertPanelBtn');

        if (panel) {
            panel.classList.remove('open');
            panel.style.width = '';
        }

        // Scope to the alert editor container to avoid affecting other views
        const editorView = document.getElementById('alertEditorView');
        const mainContent = editorView ? editorView.querySelector('.main-content') : document.querySelector('.main-content');
        if (mainContent) {
            mainContent.classList.remove('panel-open');
            mainContent.style.marginRight = '';
        }

        if (toggleBtn) toggleBtn.classList.remove('panel-active');
    },

    openAlertPanel() {
        // Only open if the alert editor is actually visible
        const editorView = document.getElementById('alertEditorView');
        if (!editorView || editorView.style.display === 'none') return;

        const panel = document.getElementById('alertConfigPanel');
        const mainContent = editorView.querySelector('.main-content');
        const toggleBtn = document.getElementById('toggleAlertPanelBtn');

        if (panel && mainContent) {
            // Align panel top with the bottom of the header
            const header = document.querySelector('.header');
            if (header) {
                const rect = header.getBoundingClientRect();
                panel.style.top = (rect.bottom) + 'px';
            }
            panel.classList.add('open');
            mainContent.classList.add('panel-open');
            if (toggleBtn) toggleBtn.classList.add('panel-active');
        }
    },

    async loadAlertIntoEditor(alertId) {
        try {
            const response = await fetch(`/api/v1/alerts/${alertId}`, {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load alert');
            }

            const alert = data.data.alert;
            this.currentAlert = alert;

            // Show feed alert banner if applicable
            const existingBanner = document.getElementById('feedAlertBanner');
            if (existingBanner) existingBanner.remove();
            if (this.editingFeedAlert) {
                const banner = document.createElement('div');
                banner.id = 'feedAlertBanner';
                banner.className = 'feed-alert-banner';
                banner.innerHTML = 'Feed-sourced alert. Saving will create an editable manual copy and disable the feed version.';
                const editorContainer = document.querySelector('.alert-editor-container');
                if (editorContainer) editorContainer.prepend(banner);
            }

            // Populate form fields
            const nameField = document.getElementById('editorAlertName');
            const descField = document.getElementById('editorAlertDescription');
            const queryField = document.getElementById('editorQueryInput');
            const labelsField = document.getElementById('editorAlertLabels');
            const referencesField = document.getElementById('editorAlertReferences');
            const throttleTimeField = document.getElementById('editorThrottleTime');
            const throttleFieldField = document.getElementById('editorThrottleField');
            const enabledField = document.getElementById('editorAlertEnabled');

            if (nameField) nameField.value = alert.name || '';
            if (descField) descField.value = alert.description || '';
            if (queryField) {
                queryField.value = alert.query_string || '';
                queryField.dispatchEvent(new Event('input'));
            }
            if (labelsField) labelsField.value = (alert.labels || []).join(', ');
            this.setLabelsFromArray(alert.labels);
            if (referencesField) referencesField.value = (alert.references || []).join('\n');
            this.loadReferencesFromTextarea();
            this.setSeverity(alert.severity || 'medium');
            if (throttleTimeField) throttleTimeField.value = alert.throttle_time_seconds || 0;
            if (throttleFieldField) throttleFieldField.value = alert.throttle_field || '';
            if (enabledField) enabledField.checked = alert.enabled;

            // Set alert type dropdown and card
            const alertType = alert.alert_type || 'event';
            const alertTypeSelect = document.getElementById('alertTypeSelect');
            if (alertTypeSelect) alertTypeSelect.value = alertType;
            this.setAlertTypeCard(alertType);
            const windowGroup = document.getElementById('windowDurationGroup');
            const scheduledGroup = document.getElementById('scheduledConfigGroup');
            if (windowGroup) windowGroup.style.display = alertType === 'compound' ? 'block' : 'none';
            if (scheduledGroup) scheduledGroup.style.display = alertType === 'scheduled' ? 'block' : 'none';

            // Update help text
            const helpText = document.getElementById('alertTypeHelp');
            if (helpText) {
                const descriptions = {
                    event: 'Event alerts match individual logs in real-time.',
                    compound: 'Compound alerts aggregate over a time window.',
                    scheduled: 'Scheduled queries run on a cron schedule and look back a configurable window.'
                };
                helpText.textContent = descriptions[alertType] || '';
            }

            // Set window duration for compound alerts
            if (alertType === 'compound' && alert.window_duration) {
                const totalSeconds = alert.window_duration;
                const windowDur = document.getElementById('editorWindowDuration');
                const windowUnit = document.getElementById('editorWindowUnit');
                if (windowDur && windowUnit) {
                    if (totalSeconds % 86400 === 0) {
                        windowDur.value = totalSeconds / 86400;
                        windowUnit.value = '86400';
                    } else if (totalSeconds % 3600 === 0) {
                        windowDur.value = totalSeconds / 3600;
                        windowUnit.value = '3600';
                    } else {
                        windowDur.value = totalSeconds / 60;
                        windowUnit.value = '60';
                    }
                }
            }

            // Set scheduled alert fields
            if (alertType === 'scheduled') {
                const cronExpr = alert.schedule_cron || '0 0 * * *';
                const presetSelect = document.getElementById('editorSchedulePreset');
                const customCronGrp = document.getElementById('customCronGroup');
                const cronInput = document.getElementById('editorScheduleCron');

                const presets = ['0 * * * *', '0 0 * * *', '0 0 * * 1', '0 0 1 * *'];
                if (presetSelect) {
                    if (presets.includes(cronExpr)) {
                        presetSelect.value = cronExpr;
                        if (customCronGrp) customCronGrp.style.display = 'none';
                    } else {
                        presetSelect.value = 'custom';
                        if (customCronGrp) customCronGrp.style.display = 'block';
                        if (cronInput) cronInput.value = cronExpr;
                    }
                }

                const totalSec = alert.query_window_seconds || 86400;
                const qwVal = document.getElementById('editorQueryWindowValue');
                const qwUn = document.getElementById('editorQueryWindowUnit');
                if (qwVal && qwUn) {
                    if (totalSec % 604800 === 0) {
                        qwVal.value = totalSec / 604800;
                        qwUn.value = '604800';
                    } else if (totalSec % 86400 === 0) {
                        qwVal.value = totalSec / 86400;
                        qwUn.value = '86400';
                    } else {
                        qwVal.value = Math.round(totalSec / 3600);
                        qwUn.value = '3600';
                    }
                }
            }

            // Store selected action IDs then populate the unified dropdown
            this.selectedWebhookIds = (alert.webhook_actions || []).map(wh => wh.id);
            this.selectedFractalActionIds = (alert.fractal_actions || []).map(fa => fa.id);
            this.selectedDictActionIds = (alert.dictionary_actions || []).map(da => da.id);
            this.selectedEmailActionIds = (alert.email_action_ids || []);
            this.loadActionsIntoEditor();

            // Trigger query test to show current results
            this.testQuery();

        } catch (error) {
            console.error('Load alert error:', error);
            Toast.show('Failed to load alert for editing', 'error');
        }
    },

    async loadActionsIntoEditor() {
        const select = document.getElementById('editorActionsSelect');
        const selectedList = document.getElementById('editorSelectedActions');
        if (!select || !selectedList) return;

        try {
            const [webhooksResp, actionsResp, dictActionsResp, emailActionsResp] = await Promise.all([
                fetch('/api/v1/webhooks', { credentials: 'include' }),
                fetch('/api/v1/fractal-actions', { credentials: 'include' }),
                fetch('/api/v1/dictionary-actions', { credentials: 'include' }),
                fetch('/api/v1/email-actions', { credentials: 'include' })
            ]);

            const webhooksData = await webhooksResp.json();
            const actionsData = await actionsResp.json();
            const dictActionsData = await dictActionsResp.json();
            const emailActionsData = await emailActionsResp.json();

            const webhooks = (webhooksData.success ? webhooksData.data.webhooks : null) || [];
            const fractalActions = (actionsData.success ? actionsData.data.fractal_actions : null) || [];
            const dictActions = (dictActionsData.success ? dictActionsData.data.actions : null) || [];
            const emailActions = (emailActionsData.success ? emailActionsData.data.email_actions : null) || [];

            // Populate dropdown with unselected actions
            select.innerHTML = '<option value="">Add action...</option>';
            webhooks.forEach(wh => {
                if ((this.selectedWebhookIds || []).includes(wh.id)) return;
                const opt = document.createElement('option');
                opt.value = wh.id;
                opt.dataset.type = 'webhook';
                opt.textContent = wh.name;
                select.appendChild(opt);
            });
            fractalActions.forEach(fa => {
                if ((this.selectedFractalActionIds || []).includes(fa.id)) return;
                const opt = document.createElement('option');
                opt.value = fa.id;
                opt.dataset.type = 'fractal-action';
                opt.textContent = fa.name;
                select.appendChild(opt);
            });
            dictActions.forEach(da => {
                if ((this.selectedDictActionIds || []).includes(da.id)) return;
                const opt = document.createElement('option');
                opt.value = da.id;
                opt.dataset.type = 'dictionary-action';
                opt.textContent = da.name;
                select.appendChild(opt);
            });
            emailActions.forEach(ea => {
                if ((this.selectedEmailActionIds || []).includes(ea.id)) return;
                const opt = document.createElement('option');
                opt.value = ea.id;
                opt.dataset.type = 'email-action';
                opt.textContent = ea.name;
                select.appendChild(opt);
            });

            // Show already-selected actions
            selectedList.innerHTML = '';
            (this.selectedWebhookIds || []).forEach(id => {
                const wh = webhooks.find(w => w.id === id);
                if (wh) this._appendSelectedAction(selectedList, wh.id, wh.name, 'webhook');
            });
            (this.selectedFractalActionIds || []).forEach(id => {
                const fa = fractalActions.find(f => f.id === id);
                if (fa) this._appendSelectedAction(selectedList, fa.id, fa.name, 'fractal-action');
            });
            (this.selectedDictActionIds || []).forEach(id => {
                const da = dictActions.find(d => d.id === id);
                if (da) this._appendSelectedAction(selectedList, da.id, da.name, 'dictionary-action');
            });
            (this.selectedEmailActionIds || []).forEach(id => {
                const ea = emailActions.find(e => e.id === id);
                if (ea) this._appendSelectedAction(selectedList, ea.id, ea.name, 'email-action');
            });
        } catch (error) {
            console.error('Failed to load actions:', error);
            if (select) select.innerHTML = '<option value="">Failed to load actions</option>';
        }
    },

    _appendSelectedAction(container, id, name, type) {
        const item = document.createElement('div');
        item.className = 'selected-action-item';
        item.dataset.id = id;
        item.dataset.type = type;
        item.innerHTML = `<span class="selected-action-name">${Utils.escapeHtml(name)}</span><button type="button" class="selected-action-remove" data-id="${id}" data-type="${type}">&times;</button>`;
        container.appendChild(item);
        this.updateActionCountBadge();
    },

    updateActionCountBadge() {
        const badge = document.getElementById('actionCountBadge');
        const list = document.getElementById('editorSelectedActions');
        if (!badge || !list) return;
        const count = list.querySelectorAll('.selected-action-item').length;
        badge.textContent = count;
        badge.style.display = count > 0 ? '' : 'none';
    },

    addReferenceField(value = '') {
        const container = document.getElementById('editorReferencesContainer');
        if (!container) return;
        const row = document.createElement('div');
        row.className = 'alert-reference-row';
        row.innerHTML = `
            <input type="url" class="ref-url-input" value="${Utils.escapeHtml(value)}" placeholder="https://..." />
            <button type="button" class="ref-open-btn" title="Open URL">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </button>
            <button type="button" class="ref-remove-btn" title="Remove">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
        `;
        row.querySelector('.ref-open-btn').addEventListener('click', () => {
            const url = row.querySelector('.ref-url-input').value.trim();
            if (url) window.open(url, '_blank', 'noopener');
        });
        row.querySelector('.ref-remove-btn').addEventListener('click', () => {
            row.remove();
            this.syncReferencesToTextarea();
        });
        row.querySelector('.ref-url-input').addEventListener('input', (e) => {
            const input = e.target;
            const val = input.value.trim();
            if (val && !val.match(/^https?:\/\//)) {
                input.classList.add('invalid-url');
            } else {
                input.classList.remove('invalid-url');
            }
            this.syncReferencesToTextarea();
        });
        container.appendChild(row);
        if (!value) row.querySelector('.ref-url-input').focus();
    },

    syncReferencesToTextarea() {
        const container = document.getElementById('editorReferencesContainer');
        const textarea = document.getElementById('editorAlertReferences');
        if (!container || !textarea) return;
        const urls = Array.from(container.querySelectorAll('.ref-url-input'))
            .map(input => input.value.trim())
            .filter(v => v);
        textarea.value = urls.join('\n');
    },

    loadReferencesFromTextarea() {
        const textarea = document.getElementById('editorAlertReferences');
        const container = document.getElementById('editorReferencesContainer');
        if (!container) return;
        container.innerHTML = '';
        const refs = (textarea?.value || '').split('\n').map(s => s.trim()).filter(s => s);
        refs.forEach(ref => this.addReferenceField(ref));
    },

    setSeverity(value) {
        value = value || 'medium';
        const hidden = document.getElementById('editorAlertSeverity');
        if (hidden) hidden.value = value;
        const trigger = document.getElementById('severityDropdownTrigger');
        if (trigger) {
            const dot = trigger.querySelector('.severity-dot');
            const label = trigger.querySelector('.severity-dropdown-label');
            if (dot) {
                dot.className = 'severity-dot severity-dot-' + value;
            }
            if (label) {
                label.textContent = value.charAt(0).toUpperCase() + value.slice(1);
            }
        }
    },

    // Label chip management
    addLabelChip(label) {
        label = label.trim();
        if (!label) return;
        const container = document.getElementById('editorLabelsChips');
        const input = document.getElementById('editorLabelInput');
        if (!container) return;

        // Prevent duplicates
        const existing = container.querySelectorAll('.alert-label-chip');
        for (const chip of existing) {
            if (chip.dataset.label === label) return;
        }

        const chip = document.createElement('span');
        chip.className = 'alert-label-chip';
        chip.dataset.label = label;
        chip.innerHTML = `${Utils.escapeHtml(label)}<button type="button" class="alert-label-chip-remove">&times;</button>`;
        chip.querySelector('.alert-label-chip-remove').addEventListener('click', () => {
            chip.remove();
            this.syncLabelsToHidden();
        });
        container.insertBefore(chip, input);
        this.syncLabelsToHidden();
    },

    syncLabelsToHidden() {
        const container = document.getElementById('editorLabelsChips');
        const hidden = document.getElementById('editorAlertLabels');
        if (!container || !hidden) return;
        const labels = Array.from(container.querySelectorAll('.alert-label-chip'))
            .map(chip => chip.dataset.label);
        hidden.value = labels.join(', ');
    },

    setLabelsFromArray(labels) {
        const container = document.getElementById('editorLabelsChips');
        const input = document.getElementById('editorLabelInput');
        if (!container) return;
        // Remove existing chips
        container.querySelectorAll('.alert-label-chip').forEach(c => c.remove());
        (labels || []).forEach(l => this.addLabelChip(l));
    },

    setAlertTypeCard(type) {
        document.querySelectorAll('.alert-type-card').forEach(card => {
            card.classList.toggle('active', card.dataset.type === type);
        });
    },

    removeSelectedAction(id, type, name) {
        const selectedList = document.getElementById('editorSelectedActions');
        const select = document.getElementById('editorActionsSelect');
        if (!selectedList) return;

        const item = selectedList.querySelector(`[data-id="${CSS.escape(id)}"]`);
        if (!item) return;
        item.remove();

        // Add back to dropdown
        if (select) {
            const opt = document.createElement('option');
            opt.value = id;
            opt.dataset.type = type;
            opt.textContent = name || id;
            select.appendChild(opt);
        }
        this.updateActionCountBadge();
    },

    async saveAlertFromEditor() {
        // Ensure alert panel is open so form fields are accessible
        const panel = document.getElementById('alertConfigPanel');
        if (panel && !panel.classList.contains('open')) {
            Toast.show('Please open the alert configuration panel to save your alert', 'warning');
            this.openAlertPanel();
            return;
        }

        const formData = this.getAlertEditorFormData();
        if (!formData) return;

        // Validate query syntax before saving
        try {
            const query = this.stripComments(formData.query_string);
            const timeRange = this.getTimeRange();
            const validateBody = { query, start: timeRange.start, end: timeRange.end };
            if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
                validateBody.fractal_id = window.FractalContext.currentFractal.id;
            }
            const validateResp = await fetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(validateBody)
            });
            const validateData = await validateResp.json();
            if (!validateData.success) {
                Toast.show('Invalid query syntax: ' + (validateData.error || 'unknown error'), 'error');
                return;
            }
        } catch (err) {
            Toast.show('Could not validate query: ' + err.message, 'error');
            return;
        }

        try {
            // Feed alert: always create a new manual alert (POST)
            const isFeedSave = this.editingFeedAlert;
            const url = (this.currentAlert && !isFeedSave)
                ? `/api/v1/alerts/${this.currentAlert.id}`
                : '/api/v1/alerts';

            const method = (this.currentAlert && !isFeedSave) ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                body: JSON.stringify(formData)
            });

            if (!response.ok) {
                const errorData = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorData}`);
            }

            const data = await response.json();
            if (data.success) {
                // If saving a feed alert as manual, disable the original feed alert
                if (isFeedSave && this.feedAlertOriginalId) {
                    try {
                        await fetch(`/api/v1/alerts/${this.feedAlertOriginalId}/toggle-feed`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            credentials: 'include',
                            body: JSON.stringify({ enabled: false })
                        });
                    } catch (e) {
                        console.error('[Alerts] Failed to disable feed alert:', e);
                    }
                }
                Toast.show(isFeedSave ? 'Manual alert created, feed version disabled' : `Alert ${this.currentAlert ? 'updated' : 'created'} successfully`, 'success');
                this.backToAlerts();
            } else {
                Toast.show(data.error || 'Failed to save alert', 'error');
            }
        } catch (error) {
            console.error('Save alert error:', error);
            Toast.show('Network error: ' + error.message, 'error');
        }
    },

    getAlertEditorFormData() {
        const nameElement = document.getElementById('editorAlertName');
        const descElement = document.getElementById('editorAlertDescription');
        const queryElement = document.getElementById('editorQueryInput');
        const labelsElement = document.getElementById('editorAlertLabels');
        const throttleTimeElement = document.getElementById('editorThrottleTime');
        const throttleFieldElement = document.getElementById('editorThrottleField');
        const enabledElement = document.getElementById('editorAlertEnabled');
        const severityElement = document.getElementById('editorAlertSeverity');

        // Sync chips to hidden fields before reading
        this.syncLabelsToHidden();
        this.syncReferencesToTextarea();
        const referencesElement = document.getElementById('editorAlertReferences');

        const name = nameElement?.value?.trim() || '';
        const description = descElement?.value?.trim() || '';
        const queryString = queryElement?.value?.trim() || '';
        const labels = labelsElement?.value ? labelsElement.value.split(',').map(s => s.trim()).filter(s => s) : [];
        const references = referencesElement?.value ? referencesElement.value.split('\n').map(s => s.trim()).filter(s => s) : [];
        const severity = severityElement?.value || 'medium';
        const throttleTime = parseInt(throttleTimeElement?.value) || 0;
        const throttleField = throttleFieldElement?.value?.trim() || '';
        const enabled = enabledElement?.checked || false;

        // Get selected actions from the unified list
        const webhookActionIDs = [];
        const fractalActionIDs = [];
        const dictActionIDs = [];
        const emailActionIDs = [];
        document.querySelectorAll('#editorSelectedActions .selected-action-item').forEach(item => {
            if (item.dataset.type === 'webhook') {
                webhookActionIDs.push(item.dataset.id);
            } else if (item.dataset.type === 'fractal-action') {
                fractalActionIDs.push(item.dataset.id);
            } else if (item.dataset.type === 'dictionary-action') {
                dictActionIDs.push(item.dataset.id);
            } else if (item.dataset.type === 'email-action') {
                emailActionIDs.push(item.dataset.id);
            }
        });

        // Validation
        if (!name) {
            Toast.show('Alert name is required', 'error');
            if (nameElement) {
                nameElement.focus();
            }
            return null;
        }
        if (!queryString) {
            Toast.show('Query string is required', 'error');
            if (queryElement) {
                queryElement.focus();
            }
            return null;
        }

        // Determine alert type from dropdown
        const alertTypeSelect = document.getElementById('alertTypeSelect');
        const alertType = alertTypeSelect ? alertTypeSelect.value : 'event';

        // Build form data
        const formData = {
            name,
            description,
            query_string: queryString,
            alert_type: alertType,
            severity,
            labels,
            references,
            throttle_time_seconds: throttleTime,
            throttle_field: throttleField,
            enabled,
            webhook_action_ids: webhookActionIDs,
            fractal_action_ids: fractalActionIDs,
            dictionary_action_ids: dictActionIDs,
            email_action_ids: emailActionIDs
        };

        // Add window duration for compound alerts
        if (alertType === 'compound') {
            const windowValue = parseInt(document.getElementById('editorWindowDuration')?.value) || 15;
            const windowUnit = parseInt(document.getElementById('editorWindowUnit')?.value) || 60;
            const windowDuration = windowValue * windowUnit;
            if (windowDuration <= 0) {
                Toast.show('Window duration must be greater than 0', 'error');
                return null;
            }
            formData.window_duration = windowDuration;
        }

        // Add schedule fields for scheduled alerts
        if (alertType === 'scheduled') {
            const presetSelect = document.getElementById('editorSchedulePreset');
            let cronExpr;
            if (presetSelect && presetSelect.value === 'custom') {
                cronExpr = document.getElementById('editorScheduleCron')?.value?.trim();
                if (!cronExpr) {
                    Toast.show('Cron expression is required for custom schedules', 'error');
                    return null;
                }
            } else {
                cronExpr = presetSelect?.value || '0 0 * * *';
            }
            formData.schedule_cron = cronExpr;

            const qwValue = parseInt(document.getElementById('editorQueryWindowValue')?.value) || 1;
            const qwUnit = parseInt(document.getElementById('editorQueryWindowUnit')?.value) || 86400;
            const queryWindowSeconds = qwValue * qwUnit;
            if (queryWindowSeconds <= 0) {
                Toast.show('Query window must be greater than 0', 'error');
                return null;
            }
            formData.query_window_seconds = queryWindowSeconds;
        }

        return formData;
    },


    // Alert Editor Pagination Methods (identical to main search pagination)
    getCurrentAlertPageResults() {
        const start = (this.alertCurrentPage - 1) * this.alertPageSize;
        const end = start + this.alertPageSize;
        return this.currentResults.slice(start, end);
    },

    getAlertTotalPages() {
        return Math.ceil(this.currentResults.length / this.alertPageSize);
    },

    showAlertPagination() {
        const paginationControls = document.getElementById('alertPaginationControls');
        if (paginationControls && this.currentResults.length > this.alertPageSize) {
            paginationControls.style.display = 'flex';
        }
    },

    hideAlertPagination() {
        const paginationControls = document.getElementById('alertPaginationControls');
        if (paginationControls) {
            paginationControls.style.display = 'none';
        }
    },

    updateAlertPagination() {
        const totalPages = this.getAlertTotalPages();
        const pageInfo = document.getElementById('alertPageInfo');
        const prevBtn = document.getElementById('alertPrevPageBtn');
        const nextBtn = document.getElementById('alertNextPageBtn');

        if (pageInfo) {
            pageInfo.textContent = `Page ${this.alertCurrentPage} of ${totalPages}`;
        }

        if (prevBtn) {
            prevBtn.disabled = this.alertCurrentPage <= 1;
        }

        if (nextBtn) {
            nextBtn.disabled = this.alertCurrentPage >= totalPages;
        }
    },

    alertPrevPage() {
        if (this.alertCurrentPage > 1) {
            this.alertCurrentPage--;
            this.updateAlertPagination();

            // Use shared rendering method
            const targetElement = document.getElementById('queryResults');
            const pageResults = this.getCurrentAlertPageResults();
            if (window.QueryExecutor) {
                QueryExecutor.renderResultsToElement(pageResults, targetElement, this.fieldOrder, {
                    allResults: this.currentResults
                });
            }
        }
    },

    alertNextPage() {
        const totalPages = this.getAlertTotalPages();
        if (this.alertCurrentPage < totalPages) {
            this.alertCurrentPage++;
            this.updateAlertPagination();

            // Use shared rendering method
            const targetElement = document.getElementById('queryResults');
            const pageResults = this.getCurrentAlertPageResults();
            if (window.QueryExecutor) {
                QueryExecutor.renderResultsToElement(pageResults, targetElement, this.fieldOrder, {
                    allResults: this.currentResults
                });
            }
        }
    },

    alertPageSizeChanged(newPageSize) {
        this.alertPageSize = parseInt(newPageSize);
        this.alertCurrentPage = 1;
        this.updateAlertPagination();

        // Use shared rendering method
        const targetElement = document.getElementById('queryResults');
        const pageResults = this.getCurrentAlertPageResults();
        if (window.QueryExecutor) {
            QueryExecutor.renderResultsToElement(pageResults, targetElement, this.fieldOrder, {
                allResults: this.currentResults,
                isAggregated: this.isAggregated
            });
        }
    },

    // ============================
    // Fractal Context Management
    // ============================

    onFractalChange() {
        // Clear the rendered list unconditionally so when the user navigates
        // back to this tab after a scope switch they never see the previous
        // scope's alerts flashing before the new load resolves.
        const alertsList = document.getElementById('alertsList');
        if (alertsList) alertsList.innerHTML = '';
        this.filteredAlerts = [];
        this.alertsCurrentPage = 1;

        const alertsView = document.getElementById('alertsView');
        if (alertsView && alertsView.offsetParent !== null) {
            this.loadAlerts();
        }
    },

    // ============================
    // Action Tabs Management
    // ============================

    switchActionTab(tabName) {
        // Close any open inline forms
        this.closeInlineWebhookForm();
        this.closeInlineFractalActionForm();
        this.closeInlineDictActionForm();

        // Update tab buttons
        document.querySelectorAll('.action-tab').forEach(tab => {
            tab.classList.remove('active');
        });
        document.querySelector(`[data-tab="${tabName}"]`)?.classList.add('active');

        // Update tab content
        document.querySelectorAll('.action-tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(`${tabName}-tab`)?.classList.add('active');

        // Toggle header "Add" buttons based on active tab
        const addWebhookBtn = document.getElementById('inlineAddWebhookBtn');
        const addFractalActionBtn = document.getElementById('inlineAddFractalActionBtn');
        const addDictActionBtn = document.getElementById('inlineAddDictActionBtn');
        if (addWebhookBtn) addWebhookBtn.style.display = tabName === 'webhooks' ? '' : 'none';
        if (addFractalActionBtn) addFractalActionBtn.style.display = tabName === 'fractal-actions' ? '' : 'none';
        if (addDictActionBtn) addDictActionBtn.style.display = tabName === 'dictionary-actions' ? '' : 'none';
    },

    // ============================
    // Fractal Action Management
    // ============================

    async loadFractalActionsForManage() {
        const container = document.getElementById('fractalActionsList');
        if (!container) return;

        try {
            const response = await fetch('/api/v1/fractal-actions', {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load fractal actions');
            }

            const fractalActions = data.data.fractal_actions || [];
            if (fractalActions.length === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-text">No fractal actions configured</div>
                        <button onclick="Alerts.showInlineFractalActionCreate()" class="btn-primary">Create Your First Fractal Action</button>
                    </div>
                `;
                return;
            }

            const fractalActionsHTML = fractalActions.map(action => {
                const statusClass = action.enabled ? 'enabled' : 'disabled';
                return `
                    <div class="webhook-card ${statusClass}">
                        <div class="webhook-header">
                            <h4>${Utils.escapeHtml(action.name)}</h4>
                            <div class="webhook-actions">
                                <button onclick="Alerts.editFractalAction('${action.id}')" class="btn-sm btn-secondary">Edit</button>
                                <button onclick="Alerts.deleteFractalAction('${action.id}')" class="btn-sm btn-danger">Delete</button>
                            </div>
                        </div>
                        <div class="webhook-details">
                            <div><strong>Target:</strong> ${Utils.escapeHtml(action.target_fractal_name || action.target_fractal_id)}</div>
                            ${action.description ? `<div><strong>Description:</strong> ${Utils.escapeHtml(action.description)}</div>` : ''}
                            <div><strong>Status:</strong> ${action.enabled ? 'Enabled' : 'Disabled'}</div>
                        </div>
                    </div>
                `;
            }).join('');

            container.innerHTML = fractalActionsHTML;

        } catch (error) {
            console.error('Failed to load fractal actions for manage:', error);
            container.innerHTML = '<div class="error-text">Failed to load fractal actions</div>';
        }
    },


    async showInlineFractalActionCreate() {
        document.getElementById('newActionMenu')?.style && (document.getElementById('newActionMenu').style.display = 'none');
        this.closeAllInlineForms();
        this.inlineFractalActionForm = 'create';
        this.currentFractalAction = null;
        this.renderFractalActionInlineForm();
        await this.loadFractalsForAction();
    },

    async showInlineFractalActionEdit(fractalActionId) {
        this.inlineFractalActionForm = fractalActionId;
        this.closeInlineWebhookForm();
        try {
            const response = await fetch(`/api/v1/fractal-actions/${fractalActionId}`, {
                method: 'GET', credentials: 'include'
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to load fractal action');
            this.currentFractalAction = data.data.fractal_action;
            this.renderFractalActionInlineForm();
            // Must await so select options exist before setting value
            await this.loadFractalsForAction();
            this.populateFractalActionFormFields();
        } catch (error) {
            console.error('Failed to load fractal action for edit:', error);
            Toast.show('Failed to load fractal action: ' + error.message, 'error');
        }
    },

    renderFractalActionInlineForm() {
        const container = document.getElementById('fractalActionInlineFormContainer');
        if (!container) return;

        const isEdit = this.inlineFractalActionForm !== 'create';
        const panelClass = isEdit ? 'actions-edit-panel' : 'actions-create-panel';
        const title = isEdit ? 'Edit Fractal Action' : 'Create Fractal Action';

        // Build the fractal target options from the already-loaded select
        const existingSelect = document.getElementById('fractalActionTarget');
        let fractalOptions = '<option value="">Select target fractal...</option>';
        if (existingSelect) {
            fractalOptions = existingSelect.innerHTML;
        } else {
            // Options will be populated by loadFractalsForAction after render
        }

        container.innerHTML = `
            <div class="${panelClass}">
                <div class="actions-panel-header">
                    <h3>${title}</h3>
                    <button class="btn-icon" onclick="Alerts.closeInlineFractalActionForm()" title="Close">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div class="actions-form-grid">
                    <div class="actions-form-group">
                        <label for="fractalActionName">Name *</label>
                        <input type="text" id="fractalActionName" placeholder="Enter action name" required>
                        <small class="form-help">A descriptive name for this fractal action</small>
                    </div>
                    <div class="actions-form-group">
                        <label for="fractalActionTarget">Target Fractal *</label>
                        <select id="fractalActionTarget" required>
                            ${fractalOptions}
                        </select>
                        <small class="form-help">The fractal where logs will be sent when this action triggers</small>
                    </div>
                    <div class="actions-form-group full-width">
                        <label for="fractalActionDescription">Description</label>
                        <textarea id="fractalActionDescription" placeholder="Optional description" rows="2"></textarea>
                    </div>
                    <div class="actions-form-group">
                        <label for="fractalActionMaxLogs">Max Logs Per Trigger</label>
                        <input type="number" id="fractalActionMaxLogs" value="1000" min="1" max="10000">
                        <small class="form-help">Maximum number of logs to forward when this action triggers</small>
                    </div>
                    <div class="actions-form-group">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="fractalActionPreserveTimestamp" checked>
                            Preserve Original Timestamps
                        </label>
                        <small class="form-help">Keep original log timestamps</small>
                    </div>
                    <div class="actions-form-group">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="fractalActionAddContext" checked>
                            Add Alert Context
                        </label>
                        <small class="form-help">Include alert information in forwarded logs</small>
                    </div>
                    <div class="actions-form-group">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="fractalActionEnabled" checked>
                            Enabled
                        </label>
                    </div>
                </div>
                <div class="actions-form-actions">
                    <button class="btn-secondary" onclick="Alerts.closeInlineFractalActionForm()">Cancel</button>
                    <button id="saveFractalActionBtn" class="btn-primary" onclick="Alerts.saveFractalAction()">Save Fractal Action</button>
                </div>
                <div id="fractalActionError" class="error-message" style="display: none;"></div>
            </div>
        `;

        document.getElementById('fractalActionName')?.focus();
    },

    populateFractalActionFormFields() {
        const action = this.currentFractalAction;
        if (!action) return;

        document.getElementById('fractalActionName').value = action.name || '';
        document.getElementById('fractalActionDescription').value = action.description || '';
        document.getElementById('fractalActionTarget').value = action.target_fractal_id || '';
        document.getElementById('fractalActionMaxLogs').value = action.max_logs_per_trigger || 1000;
        document.getElementById('fractalActionPreserveTimestamp').checked = action.preserve_timestamp !== false;
        document.getElementById('fractalActionAddContext').checked = action.add_alert_context !== false;
        document.getElementById('fractalActionEnabled').checked = action.enabled !== false;
    },

    closeInlineFractalActionForm() {
        this.inlineFractalActionForm = null;
        this.currentFractalAction = null;
        const container = document.getElementById('fractalActionInlineFormContainer');
        if (container) container.innerHTML = '';
    },

    async loadFractalsForAction() {
        try {
            const response = await fetch('/api/v1/fractals', {
                method: 'GET',
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load fractals');
            }

            const select = document.getElementById('fractalActionTarget');
            if (!select) return;

            // Clear existing options except the first placeholder
            select.innerHTML = '<option value="">Select target fractal...</option>';

            // Add fractal options
            data.data.fractals.forEach(fractal => {
                const option = document.createElement('option');
                option.value = fractal.id;
                option.textContent = fractal.name;
                select.appendChild(option);
            });

        } catch (error) {
            console.error('Failed to load fractals for action:', error);
            Toast.show('Failed to load fractals: ' + error.message, 'error');
        }
    },

    // resetFractalActionForm and loadFractalActionForEdit removed - replaced by inline form rendering

    async saveFractalAction() {
        try {
            const name = document.getElementById('fractalActionName').value.trim();
            const description = document.getElementById('fractalActionDescription').value.trim();
            const targetFractalId = document.getElementById('fractalActionTarget').value;
            const maxLogs = parseInt(document.getElementById('fractalActionMaxLogs').value) || 1000;
            const preserveTimestamp = document.getElementById('fractalActionPreserveTimestamp').checked;
            const addContext = document.getElementById('fractalActionAddContext').checked;
            const enabled = document.getElementById('fractalActionEnabled').checked;

            // Hide previous error
            const errorDiv = document.getElementById('fractalActionError');
            if (errorDiv) {
                errorDiv.style.display = 'none';
                errorDiv.textContent = '';
            }

            // Validation
            if (!name) {
                throw new Error('Action name is required');
            }

            if (!targetFractalId) {
                throw new Error('Target fractal is required');
            }

            const requestData = {
                name,
                description,
                target_fractal_id: targetFractalId,
                max_logs_per_trigger: maxLogs,
                preserve_timestamp: preserveTimestamp,
                add_alert_context: addContext,
                enabled
            };

            const url = this.currentFractalAction ?
                `/api/v1/fractal-actions/${this.currentFractalAction.id}` :
                '/api/v1/fractal-actions';
            const method = this.currentFractalAction ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method: method,
                headers: {
                    'Content-Type': 'application/json'
                },
                credentials: 'include',
                body: JSON.stringify(requestData)
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to save fractal action');
            }

            // Success
            const action = this.currentFractalAction ? 'updated' : 'created';
            Toast.show(`Fractal action ${action} successfully`, 'success');

            // Close form and refresh data
            this.closeInlineFractalActionForm();
            this.loadAllActions();

        } catch (error) {
            console.error('Failed to save fractal action:', error);

            // Show error in modal
            const errorDiv = document.getElementById('fractalActionError');
            if (errorDiv) {
                errorDiv.textContent = error.message;
                errorDiv.style.display = 'block';
            } else {
                Toast.show('Failed to save fractal action: ' + error.message, 'error');
            }
        }
    },

    async editFractalAction(fractalActionId) {
        this.showInlineFractalActionEdit(fractalActionId);
    },

    async deleteFractalAction(fractalActionId) {
        if (!confirm('Are you sure you want to delete this fractal action?')) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/fractal-actions/${fractalActionId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to delete fractal action');
            }

            Toast.show('Fractal action deleted successfully', 'success');
            this.loadAllActions(); // Refresh the list

        } catch (error) {
            console.error('Failed to delete fractal action:', error);
            Toast.show('Failed to delete fractal action: ' + error.message, 'error');
        }
    },

    // ============================
    // Dictionary Action Management
    // ============================

    async loadDictActionsForManage() {
        const container = document.getElementById('dictActionsList');
        if (!container) return;

        try {
            const response = await fetch('/api/v1/dictionary-actions', {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load dictionary actions');
            }

            const actions = data.data.actions || [];
            if (actions.length === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-text">No dictionary actions configured</div>
                        <button onclick="Alerts.showInlineDictActionCreate()" class="btn-primary">Create Your First Dictionary Action</button>
                    </div>
                `;
                return;
            }

            const html = actions.map(action => {
                const statusClass = action.enabled ? 'enabled' : 'disabled';
                return `
                    <div class="webhook-card ${statusClass}">
                        <div class="webhook-header">
                            <h4>${Utils.escapeHtml(action.name)}</h4>
                            <div class="webhook-actions">
                                <button onclick="Alerts.editDictAction('${action.id}')" class="btn-sm btn-secondary">Edit</button>
                                <button onclick="Alerts.deleteDictAction('${action.id}')" class="btn-sm btn-danger">Delete</button>
                            </div>
                        </div>
                        <div class="webhook-details">
                            <div><strong>Target Dictionary:</strong> ${Utils.escapeHtml(action.dictionary_name || '')}</div>
                            ${action.description ? `<div><strong>Description:</strong> ${Utils.escapeHtml(action.description)}</div>` : ''}
                            <div><strong>Status:</strong> ${action.enabled ? 'Enabled' : 'Disabled'}</div>
                        </div>
                    </div>
                `;
            }).join('');

            container.innerHTML = html;
        } catch (error) {
            console.error('Failed to load dictionary actions:', error);
            container.innerHTML = '<div class="error-text">Failed to load dictionary actions</div>';
        }
    },

    async showInlineDictActionCreate() {
        document.getElementById('newActionMenu')?.style && (document.getElementById('newActionMenu').style.display = 'none');
        this.closeAllInlineForms();
        this.inlineDictActionForm = 'create';
        this.currentDictAction = null;
        this.renderDictActionInlineForm();
    },

    async showInlineDictActionEdit(dictActionId) {
        this.inlineDictActionForm = dictActionId;
        this.closeInlineWebhookForm();
        this.closeInlineFractalActionForm();
        try {
            const response = await fetch(`/api/v1/dictionary-actions/${dictActionId}`, {
                method: 'GET', credentials: 'include'
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to load dictionary action');
            this.currentDictAction = data.data;
            this.renderDictActionInlineForm();
            this.populateDictActionFormFields();
        } catch (error) {
            console.error('Failed to load dictionary action for edit:', error);
            Toast.show('Failed to load dictionary action: ' + error.message, 'error');
        }
    },

    renderDictActionInlineForm() {
        const container = document.getElementById('dictActionInlineFormContainer');
        if (!container) return;

        const isEdit = this.inlineDictActionForm !== 'create';
        const panelClass = isEdit ? 'actions-edit-panel' : 'actions-create-panel';
        const title = isEdit ? 'Edit Dictionary Action' : 'Create Dictionary Action';

        container.innerHTML = `
            <div class="${panelClass}">
                <div class="actions-panel-header">
                    <h3>${title}</h3>
                    <button class="btn-icon" onclick="Alerts.closeInlineDictActionForm()" title="Close">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div class="actions-form-grid">
                    <div class="actions-form-group">
                        <label for="dictActionName">Name *</label>
                        <input type="text" id="dictActionName" placeholder="Enter action name" required>
                        <small class="form-help">A descriptive name for this dictionary action</small>
                    </div>
                    <div class="actions-form-group">
                        <label for="dictActionDictName">Target Dictionary *</label>
                        <input type="text" id="dictActionDictName" placeholder="e.g. threat_intel, known_hosts" required>
                        <small class="form-help">Dictionary name (auto-created if it doesn't exist). Overwrites all data on each trigger.</small>
                    </div>
                    <div class="actions-form-group full-width">
                        <label for="dictActionDescription">Description</label>
                        <textarea id="dictActionDescription" placeholder="Optional description" rows="2"></textarea>
                    </div>
                    <div class="actions-form-group">
                        <label for="dictActionMaxLogs">Max Logs Per Trigger</label>
                        <input type="number" id="dictActionMaxLogs" value="1000" min="1" max="10000">
                        <small class="form-help">Maximum logs to process per alert trigger</small>
                    </div>
                    <div class="actions-form-group">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="dictActionEnabled" checked>
                            Enabled
                        </label>
                    </div>
                </div>
                <div class="actions-form-actions">
                    <button class="btn-secondary" onclick="Alerts.closeInlineDictActionForm()">Cancel</button>
                    <button id="saveDictActionBtn" class="btn-primary" onclick="Alerts.saveDictAction()">Save Dictionary Action</button>
                </div>
                <div id="dictActionError" class="error-message" style="display: none;"></div>
            </div>
        `;

        document.getElementById('dictActionName')?.focus();
    },

    populateDictActionFormFields() {
        const action = this.currentDictAction;
        if (!action) return;

        document.getElementById('dictActionName').value = action.name || '';
        document.getElementById('dictActionDescription').value = action.description || '';
        document.getElementById('dictActionDictName').value = action.dictionary_name || '';
        document.getElementById('dictActionMaxLogs').value = action.max_logs_per_trigger || 1000;
        document.getElementById('dictActionEnabled').checked = action.enabled !== false;
    },

    closeInlineDictActionForm() {
        this.inlineDictActionForm = null;
        this.currentDictAction = null;
        const container = document.getElementById('dictActionInlineFormContainer');
        if (container) container.innerHTML = '';
    },

    async saveDictAction() {
        try {
            const name = document.getElementById('dictActionName').value.trim();
            const description = document.getElementById('dictActionDescription').value.trim();
            const dictName = document.getElementById('dictActionDictName').value.trim();
            const maxLogs = parseInt(document.getElementById('dictActionMaxLogs').value) || 1000;
            const enabled = document.getElementById('dictActionEnabled').checked;

            const errorDiv = document.getElementById('dictActionError');
            if (errorDiv) {
                errorDiv.style.display = 'none';
                errorDiv.textContent = '';
            }

            if (!name) throw new Error('Action name is required');
            if (!dictName) throw new Error('Target dictionary name is required');

            const requestData = {
                name,
                description,
                dictionary_name: dictName,
                max_logs_per_trigger: maxLogs,
                enabled
            };

            const url = this.currentDictAction
                ? `/api/v1/dictionary-actions/${this.currentDictAction.id}`
                : '/api/v1/dictionary-actions';
            const method = this.currentDictAction ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestData)
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to save dictionary action');
            }

            const action = this.currentDictAction ? 'updated' : 'created';
            Toast.show(`Dictionary action ${action} successfully`, 'success');

            this.closeInlineDictActionForm();
            this.loadAllActions();
        } catch (error) {
            console.error('Failed to save dictionary action:', error);
            const errorDiv = document.getElementById('dictActionError');
            if (errorDiv) {
                errorDiv.textContent = error.message;
                errorDiv.style.display = 'block';
            } else {
                Toast.show('Failed to save dictionary action: ' + error.message, 'error');
            }
        }
    },

    async editDictAction(dictActionId) {
        this.showInlineDictActionEdit(dictActionId);
    },

    async deleteDictAction(dictActionId) {
        if (!confirm('Are you sure you want to delete this dictionary action?')) {
            return;
        }

        try {
            const response = await fetch(`/api/v1/dictionary-actions/${dictActionId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to delete dictionary action');
            }

            Toast.show('Dictionary action deleted successfully', 'success');
            this.loadAllActions();
        } catch (error) {
            console.error('Failed to delete dictionary action:', error);
            Toast.show('Failed to delete dictionary action: ' + error.message, 'error');
        }
    },

    exportToCsv() {
        if (!this.currentResults || this.currentResults.length === 0) {
            Toast.show('No results to export', 'warning');
            return;
        }

        try {
            // Get field order from the last query or use the first result's keys
            const fields = this.fieldOrder || Object.keys(this.currentResults[0]);

            // Create CSV header
            let csvContent = fields.map(field => `"${field}"`).join(',') + '\n';

            // Add CSV rows
            this.currentResults.forEach(row => {
                const values = fields.map(field => {
                    let value = row[field];

                    // Handle different data types
                    if (value === null || value === undefined) {
                        return '""';
                    } else if (typeof value === 'object') {
                        return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
                    } else {
                        return `"${String(value).replace(/"/g, '""')}"`;
                    }
                });
                csvContent += values.join(',') + '\n';
            });

            // Create and download file
            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const link = document.createElement('a');
            const timestamp = new Date().toISOString().slice(0, 19).replace(/[:]/g, '-');
            const filename = `bifract-alert-results-${timestamp}.csv`;

            if (link.download !== undefined) {
                const url = URL.createObjectURL(blob);
                link.setAttribute('href', url);
                link.setAttribute('download', filename);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                URL.revokeObjectURL(url);

                Toast.show(`Exported ${this.currentResults.length} results to ${filename}`, 'success');
            } else {
                Toast.show('CSV export not supported in this browser', 'error');
            }

        } catch (error) {
            console.error('Export error:', error);
            Toast.show('Failed to export CSV: ' + error.message, 'error');
        }
    },
    // ============================
    // Unified Actions View
    // ============================

    allActions: [],

    async loadAllActions() {
        const listEl = document.getElementById('unifiedActionsList');
        if (!listEl) return;
        listEl.innerHTML = '<div class="loading"><span class="spinner"></span></div>';

        try {
            const [webhooksResp, emailResp, fractalResp, dictResp] = await Promise.all([
                fetch('/api/v1/webhooks', { credentials: 'include' }),
                fetch('/api/v1/email-actions', { credentials: 'include' }),
                fetch('/api/v1/fractal-actions', { credentials: 'include' }),
                fetch('/api/v1/dictionary-actions', { credentials: 'include' })
            ]);

            const webhooksData = await webhooksResp.json();
            const emailData = await emailResp.json();
            const fractalData = await fractalResp.json();
            const dictData = await dictResp.json();

            const actions = [];
            ((webhooksData.success ? webhooksData.data.webhooks : null) || []).forEach(w => {
                actions.push({ ...w, actionType: 'webhook', actionLabel: 'Webhook', detail: w.url });
            });
            ((emailData.success ? emailData.data.email_actions : null) || []).forEach(e => {
                actions.push({ ...e, actionType: 'email', actionLabel: 'Email', detail: (e.recipients || []).join(', ') });
            });
            ((fractalData.success ? fractalData.data.fractal_actions : null) || []).forEach(f => {
                actions.push({ ...f, actionType: 'fractal', actionLabel: 'Fractal', detail: f.description || '' });
            });
            ((dictData.success ? dictData.data.actions : null) || []).forEach(d => {
                actions.push({ ...d, actionType: 'dictionary', actionLabel: 'Dictionary', detail: d.dictionary_name || '' });
            });

            actions.sort((a, b) => a.name.localeCompare(b.name));
            this.allActions = actions;
            this.filterUnifiedActions();
        } catch (error) {
            console.error('Failed to load actions:', error);
            listEl.innerHTML = '<div class="error">Failed to load actions</div>';
        }
    },

    filterUnifiedActions() {
        const typeFilter = document.getElementById('actionTypeFilter')?.value || 'all';
        const statusFilter = document.getElementById('actionStatusFilter')?.value || 'all';
        const search = (document.getElementById('actionSearchInput')?.value || '').toLowerCase();

        let filtered = this.allActions;
        if (typeFilter !== 'all') filtered = filtered.filter(a => a.actionType === typeFilter);
        if (statusFilter === 'enabled') filtered = filtered.filter(a => a.enabled);
        if (statusFilter === 'disabled') filtered = filtered.filter(a => !a.enabled);
        if (search) filtered = filtered.filter(a => a.name.toLowerCase().includes(search) || (a.detail || '').toLowerCase().includes(search));

        this.renderUnifiedActionsList(filtered);
    },

    renderUnifiedActionsList(actions) {
        const listEl = document.getElementById('unifiedActionsList');
        if (!listEl) return;

        if (actions.length === 0) {
            listEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-text">No actions configured</div>
                    <div class="empty-actions">
                        <button onclick="Alerts.toggleNewActionMenu()" class="btn-primary">Create Your First Action</button>
                    </div>
                </div>`;
            return;
        }

        listEl.innerHTML = actions.map(a => `
            <div class="unified-action-card" data-id="${a.id}" data-type="${a.actionType}">
                <div class="unified-action-info">
                    <span class="action-type-badge ${a.actionType}">${Utils.escapeHtml(a.actionLabel)}</span>
                    <span class="unified-action-name">${Utils.escapeHtml(a.name)}</span>
                    <span class="unified-action-detail">${Utils.escapeHtml(a.detail || '')}</span>
                </div>
                <div class="unified-action-controls">
                    <span class="unified-action-status ${a.enabled ? 'enabled' : 'disabled'}">${a.enabled ? 'Enabled' : 'Disabled'}</span>
                    ${(a.actionType === 'webhook' || a.actionType === 'email') ? `<button class="btn-xs btn-secondary" onclick="Alerts.testUnifiedAction('${a.id}', '${a.actionType}')">Test</button>` : ''}
                    <button class="btn-xs btn-secondary" onclick="Alerts.editUnifiedAction('${a.id}', '${a.actionType}')">Edit</button>
                    <button class="btn-xs btn-danger" onclick="Alerts.deleteUnifiedAction('${a.id}', '${a.actionType}')">Delete</button>
                </div>
            </div>
        `).join('');
    },

    async testUnifiedAction(id, type) {
        try {
            const endpoint = type === 'webhook' ? `/api/v1/webhooks/${id}/test` : `/api/v1/email-actions/${id}/test`;
            const resp = await fetch(endpoint, { method: 'POST', credentials: 'include' });
            const data = await resp.json();
            if (data.success && data.data.success) {
                Toast.show('Test sent successfully', 'success');
            } else {
                Toast.show('Test failed: ' + (data.data?.error || data.error || 'Unknown error'), 'error');
            }
        } catch (e) {
            Toast.show('Test failed: ' + e.message, 'error');
        }
    },

    editUnifiedAction(id, type) {
        if (type === 'webhook') this.showInlineWebhookEdit(id);
        else if (type === 'email') this.showInlineEmailActionEdit(id);
        else if (type === 'fractal') this.showInlineFractalActionEdit(id);
        else if (type === 'dictionary') this.showInlineDictActionEdit(id);
    },

    async deleteUnifiedAction(id, type) {
        if (!confirm('Delete this action?')) return;
        try {
            const endpoints = { webhook: 'webhooks', email: 'email-actions', fractal: 'fractal-actions', dictionary: 'dictionary-actions' };
            const resp = await fetch(`/api/v1/${endpoints[type]}/${id}`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (data.success) {
                Toast.show('Action deleted', 'success');
                this.loadAllActions();
            } else {
                Toast.show(data.error || 'Failed to delete', 'error');
            }
        } catch (e) {
            Toast.show('Delete failed: ' + e.message, 'error');
        }
    },

    toggleNewActionMenu() {
        const menu = document.getElementById('newActionMenu');
        if (menu) {
            menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
            // Close on outside click
            if (menu.style.display === 'block') {
                const handler = (e) => {
                    if (!document.getElementById('newActionDropdown')?.contains(e.target)) {
                        menu.style.display = 'none';
                        document.removeEventListener('click', handler);
                    }
                };
                setTimeout(() => document.addEventListener('click', handler), 0);
            }
        }
    },

    closeAllInlineForms() {
        this.closeInlineWebhookForm();
        this.closeInlineFractalActionForm();
        this.closeInlineDictActionForm();
        this.closeInlineEmailActionForm();
        const smtp = document.getElementById('smtpSettingsFormContainer');
        if (smtp) smtp.innerHTML = '';
    },

    // Email action inline forms
    showInlineEmailActionCreate() {
        document.getElementById('newActionMenu').style.display = 'none';
        this.closeAllInlineForms();
        this.currentEmailAction = null;
        this.renderEmailActionInlineForm();
    },

    async showInlineEmailActionEdit(id) {
        try {
            const resp = await fetch(`/api/v1/email-actions/${id}`, { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentEmailAction = data.data.email_action;
            this.renderEmailActionInlineForm();
        } catch (e) {
            Toast.show('Failed to load email action: ' + e.message, 'error');
        }
    },

    renderEmailActionInlineForm() {
        const container = document.getElementById('emailActionInlineFormContainer');
        if (!container) return;
        const ea = this.currentEmailAction;
        const isEdit = !!ea;
        const panelClass = isEdit ? 'actions-edit-panel' : 'actions-create-panel';

        container.innerHTML = `
            <div class="${panelClass}">
                <div class="actions-panel-header">
                    <h3>${isEdit ? 'Edit' : 'Create'} Email Action</h3>
                    <button class="btn-icon" onclick="Alerts.closeInlineEmailActionForm()" title="Close">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div class="actions-form-grid">
                    <div class="actions-form-group">
                        <label for="emailActionName">Name *</label>
                        <input type="text" id="emailActionName" value="${Utils.escapeHtml(ea?.name || '')}" placeholder="Security Team Alert" required>
                    </div>
                    <div class="actions-form-group">
                        <label for="emailActionRecipients">Recipients *</label>
                        <input type="text" id="emailActionRecipients" value="${Utils.escapeHtml((ea?.recipients || []).join(', '))}" placeholder="alice@example.com, bob@example.com" required>
                        <p class="form-help">Comma-separated email addresses</p>
                    </div>
                    <div class="actions-form-group">
                        <label for="emailActionSubject">Subject Template</label>
                        <input type="text" id="emailActionSubject" value="${Utils.escapeHtml(ea?.subject_template || '')}" placeholder="[Bifract] {{alert_name}} - {{severity}}">
                        <p class="form-help">Use {{alert_name}}, {{severity}}, {{match_count}}</p>
                    </div>
                    <div class="actions-form-group">
                        <label for="emailActionBody">Body Template (HTML)</label>
                        <textarea id="emailActionBody" rows="4" placeholder="Leave empty for default template">${Utils.escapeHtml(ea?.body_template || '')}</textarea>
                        <p class="form-help">Use {{alert_name}}, {{severity}}, {{match_count}}, {{query}}, {{alert_link}}, {{labels}}</p>
                    </div>
                    <div class="actions-form-group full-width">
                        <label class="actions-checkbox-label">
                            <input type="checkbox" id="emailActionEnabled" ${ea?.enabled !== false ? 'checked' : ''}>
                            Enabled
                        </label>
                    </div>
                </div>
                <div class="actions-form-actions">
                    <button class="btn-secondary" onclick="Alerts.closeInlineEmailActionForm()">Cancel</button>
                    ${isEdit ? '<button class="btn-secondary" onclick="Alerts.testUnifiedAction(\'' + ea.id + '\', \'email\')">Test</button>' : ''}
                    <button class="btn-primary" onclick="Alerts.saveEmailAction()">Save Email Action</button>
                </div>
                <div id="emailActionError" class="error-message" style="display: none;"></div>
            </div>
        `;
        container.querySelector('#emailActionName')?.focus();
    },

    closeInlineEmailActionForm() {
        const container = document.getElementById('emailActionInlineFormContainer');
        if (container) container.innerHTML = '';
        this.currentEmailAction = null;
    },

    async saveEmailAction() {
        const name = document.getElementById('emailActionName')?.value?.trim();
        const recipientsRaw = document.getElementById('emailActionRecipients')?.value?.trim();
        const subjectTemplate = document.getElementById('emailActionSubject')?.value?.trim() || '';
        const bodyTemplate = document.getElementById('emailActionBody')?.value?.trim() || '';
        const enabled = document.getElementById('emailActionEnabled')?.checked ?? true;

        if (!name) { Toast.show('Name is required', 'error'); return; }
        if (!recipientsRaw) { Toast.show('At least one recipient is required', 'error'); return; }

        const recipients = recipientsRaw.split(',').map(s => s.trim()).filter(s => s);
        const body = { name, recipients, subject_template: subjectTemplate, body_template: bodyTemplate, enabled };

        try {
            const url = this.currentEmailAction ? `/api/v1/email-actions/${this.currentEmailAction.id}` : '/api/v1/email-actions';
            const method = this.currentEmailAction ? 'PUT' : 'POST';
            const resp = await fetch(url, {
                method, credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (data.success) {
                Toast.show(`Email action ${this.currentEmailAction ? 'updated' : 'created'}`, 'success');
                this.closeInlineEmailActionForm();
                this.loadAllActions();
            } else {
                Toast.show(data.error || 'Failed to save', 'error');
            }
        } catch (e) {
            Toast.show('Failed to save: ' + e.message, 'error');
        }
    },

    // SMTP Settings
    async showSMTPSettings() {
        const container = document.getElementById('smtpSettingsFormContainer');
        if (!container) return;

        // Toggle
        if (container.innerHTML.trim()) {
            container.innerHTML = '';
            return;
        }

        try {
            const resp = await fetch('/api/v1/smtp-settings', { credentials: 'include' });
            const data = await resp.json();
            const config = data.data?.smtp_config || {};

            container.innerHTML = `
                <div class="actions-create-panel">
                    <div class="actions-panel-header">
                        <h3>SMTP Configuration</h3>
                        <button class="close-panel-btn" onclick="document.getElementById('smtpSettingsFormContainer').innerHTML=''">&times;</button>
                    </div>
                    <div class="actions-form-grid smtp-grid">
                        <div class="form-group">
                            <label>SMTP Host *</label>
                            <input type="text" id="smtpHost" class="form-input" value="${Utils.escapeHtml(config.host || '')}" placeholder="smtp.gmail.com" />
                        </div>
                        <div class="form-group">
                            <label>Port</label>
                            <input type="number" id="smtpPort" class="form-input" value="${config.port || 587}" min="1" max="65535" />
                        </div>
                        <div class="form-group">
                            <label>Username</label>
                            <input type="text" id="smtpUsername" class="form-input" value="${Utils.escapeHtml(config.username || '')}" placeholder="user@example.com" />
                        </div>
                        <div class="form-group">
                            <label>Password</label>
                            <input type="password" id="smtpPassword" class="form-input" value="${Utils.escapeHtml(config.password || '')}" />
                        </div>
                        <div class="form-group">
                            <label>From Address *</label>
                            <input type="email" id="smtpFrom" class="form-input" value="${Utils.escapeHtml(config.from_address || '')}" placeholder="alerts@bifract.io" />
                        </div>
                        <div class="form-group">
                            <label>TLS Mode</label>
                            <select id="smtpTLS" class="form-input">
                                <option value="starttls" ${config.tls_mode === 'starttls' || !config.tls_mode ? 'selected' : ''}>STARTTLS (port 587)</option>
                                <option value="implicit" ${config.tls_mode === 'implicit' ? 'selected' : ''}>Implicit TLS (port 465)</option>
                                <option value="none" ${config.tls_mode === 'none' ? 'selected' : ''}>None (port 25)</option>
                            </select>
                        </div>
                    </div>
                    <div class="actions-form-buttons">
                        <button class="btn-primary btn-sm" onclick="Alerts.saveSMTPSettings()">Save</button>
                        <button class="btn-secondary btn-sm" onclick="document.getElementById('smtpSettingsFormContainer').innerHTML=''">Cancel</button>
                    </div>
                </div>
            `;
        } catch (e) {
            Toast.show('Failed to load SMTP settings: ' + e.message, 'error');
        }
    },

    async saveSMTPSettings() {
        const config = {
            host: document.getElementById('smtpHost')?.value?.trim() || '',
            port: parseInt(document.getElementById('smtpPort')?.value) || 587,
            username: document.getElementById('smtpUsername')?.value?.trim() || '',
            password: document.getElementById('smtpPassword')?.value || '',
            from_address: document.getElementById('smtpFrom')?.value?.trim() || '',
            tls_mode: document.getElementById('smtpTLS')?.value || 'starttls'
        };

        if (!config.host) { Toast.show('SMTP host is required', 'error'); return; }
        if (!config.from_address) { Toast.show('From address is required', 'error'); return; }

        try {
            const resp = await fetch('/api/v1/smtp-settings', {
                method: 'POST', credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            });
            const data = await resp.json();
            if (data.success) {
                Toast.show('SMTP settings saved', 'success');
                document.getElementById('smtpSettingsFormContainer').innerHTML = '';
            } else {
                Toast.show(data.error || 'Failed to save', 'error');
            }
        } catch (e) {
            Toast.show('Failed to save: ' + e.message, 'error');
        }
    }
};

// Make globally available
window.Alerts = Alerts;

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    Alerts.init();
});
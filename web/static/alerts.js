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

    async show(subPath = '') {
        // Ensure alert editor and actions views are hidden when showing alerts list
        const alertEditorView = document.getElementById('alertEditorView');
        const actionsManageView = document.getElementById('actionsManageView');
        if (alertEditorView) alertEditorView.style.display = 'none';
        if (actionsManageView) actionsManageView.style.display = 'none';
        this.closeAlertPanel();

        await this.loadAlerts();
        this.updateAlertCount();
        this.startPressurePolling();

        if (subPath) this.openAlertById(subPath);
    },

    // Opens a panel from outside the table (a deep link), landing on the page
    // that holds the row so the selection is actually visible.
    openAlertById(alertId) {
        const list = this.filteredAlerts || [];
        const idx = list.findIndex(a => a.id === alertId);
        if (idx === -1) return;

        const page = Math.floor(idx / this.alertsPageSize) + 1;
        if (page !== this.alertsCurrentPage) {
            this.currentDetailAlert = list[idx];
            this.alertsCurrentPage = page;
            this.updateAlertsTable();
            return;
        }
        this.showAlertDetailsPanel(list[idx]);
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
            // The screen filters and pages client-side, so fetch the server's
            // ceiling rather than a page; page.total says if it was truncated.
            const response = await fetch('/api/v1/alerts?limit=2000', {
                credentials: 'include'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load alerts');
            }

            this.renderAlerts(data.data || []);
            this.updateAlertCount(data.page?.total ?? (data.data || []).length);
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

        // Replacing the list markup destroys the detail panel with it, so drop
        // the geometry listeners bound to the old node.
        AlertDetail.stopInset();

        const alertsHTML = this.renderAlertsTable(alerts);
        alertsList.innerHTML = alertsHTML;

        // Store alerts and initialize pagination
        this.allAlerts = alerts;
        this.filteredAlerts = alerts;
        this.alertsCurrentPage = 1;
        this.buildActionFilterOptions();
        this.buildLabelFilterOptions();
        this.addAlertTableClickHandlers();
        this.filterAlerts();
    },

    renderAlertCard(alert) {
        const statusClass = alert.enabled ? 'enabled' : 'disabled';
        const statusText = alert.enabled ? 'Enabled' : 'Disabled';
        const lastTriggered = alert.last_triggered
            ? TZ.format(alert.last_triggered, 'friendly')
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
                        ${alert.labels.map(label => `<span class="label" style="--chip-color:${Utils.tagColorFor(label)}">${Utils.escapeHtml(label)}</span>`).join('')}
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
                        <strong>Created:</strong> ${TZ.format(alert.created_at, 'date')}
                    </div>
                    <div class="alert-meta-item">
                        <strong>Last Triggered:</strong> ${lastTriggered}
                    </div>
                </div>
            </div>
        `;
    },

    // Table badges and the detail panel share one set of formatters.
    formatThrottleTime(seconds) { return AlertDetail.formatThrottle(seconds); },
    formatWindowDuration(seconds) { return AlertDetail.formatWindow(seconds); },
    formatCronSchedule(cronExpr) { return AlertDetail.formatCron(cronExpr); },

    renderAlertsTable(alerts) {
        const currentPageAlerts = this.getCurrentPageAlerts();
        const cols = [
            { key: 'name', label: 'Name', sortable: true },
            { key: 'type', label: 'Type' },
            { key: 'severity', label: 'Severity', sortable: true },
            { key: 'labels', label: 'Labels' },
            { key: 'exec_time', label: 'Exec Time', sortable: true },
            { key: 'last_triggered', label: 'Last Triggered', sortable: true }
        ];

        return `
            <div class="alerts-table-container">
                ${AlertList.renderTableHeader({
                    shown: currentPageAlerts.length,
                    total: this.filteredAlerts.length,
                    unfiltered: this.allAlerts?.length,
                    pageSize: this.alertsPageSize,
                    onPageSize: 'Alerts.changePageSize'
                })}
                <table class="alerts-table">
                    <thead>
                        <tr>${AlertList.renderColumns(cols, {
                            onSort: 'Alerts.toggleAlertsSort',
                            sortColumn: this.alertsSortColumn,
                            sortDirection: this.alertsSortDirection
                        })}</tr>
                    </thead>
                    <tbody>
                        ${currentPageAlerts.map(alert => this.renderAlertTableRow(alert)).join('')}
                    </tbody>
                </table>
                ${AlertList.renderPagination({
                    current: this.alertsCurrentPage,
                    totalPages: this.getTotalPages(),
                    onPage: 'Alerts.goToPage'
                })}
            </div>

            <!-- Alert Details Panel -->
            <div id="alertDetailsPanel" class="alert-details-panel">
                <div class="alert-details-resize" title="Drag to resize"></div>
                <div class="alert-details-header">
                    <h3 id="alertDetailsTitle">Alert Details</h3>
                    <span class="alert-details-navhint" title="Move between alerts">
                        <kbd>&uarr;</kbd><kbd>&darr;</kbd>
                    </span>
                    <button onclick="Alerts.closeAlertDetailsPanel()" class="btn-icon" title="Close (Esc)">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div id="alertDetailsContent" class="alert-details-content"></div>
                <div id="alertDetailsFooter" class="alert-details-actions"></div>
            </div>
        `;
    },

    renderAlertTableRow(alert) {
        let type = Utils.escapeHtml(alert.alert_type || 'event');
        if (alert.alert_type === 'compound' && alert.window_duration) {
            type += ` <span class="alert-window-badge">${this.formatWindowDuration(alert.window_duration)}</span>`;
        } else if (alert.alert_type === 'scheduled' && alert.schedule_cron) {
            type += ` <span class="alert-window-badge">${this.formatCronSchedule(alert.schedule_cron)}</span>`;
        }

        return AlertList.renderRow(alert, [
            `<td class="alert-type">${type}</td>`,
            AlertList.severityCell(alert, 'Alerts.setSeverityFilter'),
            AlertList.labelsCell(alert.labels, 'Alerts.setLabelFilter'),
            AlertList.execTimeCell(alert),
            AlertList.lastTriggeredCell(alert)
        ]);
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
        const footer = document.getElementById('alertDetailsFooter');

        if (!panel || !title || !content) return;

        // Re-opening for the same alert (a sort or page re-render rebuilt the
        // table markup, and the panel with it) must not stack history entries.
        if (this.currentDetailAlert?.id !== alert.id) {
            window.App?.pushSubPath(alert.id);
        }
        title.textContent = alert.name;
        title.title = alert.name;
        AlertDetail.installTabs(panel, alert, AlertDetail.renderBody(alert));
        if (footer) footer.innerHTML = this.renderDetailFooter(alert);
        content.scrollTop = 0;

        AlertDetail.applyWidth(panel);
        AlertDetail.startInset(panel);
        AlertDetail.setupResize(panel);
        AlertDetail.bindCopy(panel, alert);
        panel.classList.add('open');

        this.currentDetailAlert = alert;
        AlertDetail.markSelectedRow(alert.id, document.getElementById('alertsList'));
        AlertDetail.loadActivity(panel, alert.id);
        AlertDetail.bindKeys({
            onClose: () => this.closeAlertDetailsPanel(),
            onMove: (d) => this.moveDetailSelection(d)
        });
    },

    // Walks the filtered set, crossing page boundaries so the whole list is
    // reachable from the keyboard.
    moveDetailSelection(delta) {
        if (!this.currentDetailAlert) return;
        const page = this.getCurrentPageAlerts();
        const idx = page.findIndex(a => a.id === this.currentDetailAlert.id);
        if (idx === -1) return;

        const next = idx + delta;
        if (next >= 0 && next < page.length) {
            this.showAlertDetailsPanel(page[next]);
            return;
        }

        if (next < 0 && this.alertsCurrentPage > 1) {
            this.alertsCurrentPage--;
            this._pendingSelectEdge = 'last';
            this.updateAlertsTable();
        } else if (next >= page.length && this.alertsCurrentPage < this.getTotalPages()) {
            this.alertsCurrentPage++;
            this._pendingSelectEdge = 'first';
            this.updateAlertsTable();
        }
    },

    // The table markup owns the panel node, so any re-render destroys it.
    // Re-open it for the same alert when that alert survived the re-render.
    restoreDetailPanel() {
        const edge = this._pendingSelectEdge;
        this._pendingSelectEdge = null;

        if (edge) {
            const page = this.getCurrentPageAlerts();
            const target = edge === 'first' ? page[0] : page[page.length - 1];
            if (target) {
                this.showAlertDetailsPanel(target);
                return;
            }
        }

        if (!this.currentDetailAlert) return;
        const list = this.filteredAlerts || [];
        const idx = list.findIndex(a => a.id === this.currentDetailAlert.id);
        if (idx === -1) {
            // Filtered out from under the panel: drop it rather than leave a
            // detail view for a row that is no longer in the list.
            this.closeAlertDetailsPanel();
            return;
        }

        // A filter or sort can move the open alert off the current page, which
        // would leave the panel open with no highlighted row. Follow it. The
        // page is derived from the index, so this re-renders at most once.
        const page = Math.floor(idx / this.alertsPageSize) + 1;
        if (page !== this.alertsCurrentPage) {
            this.alertsCurrentPage = page;
            this.updateAlertsTable();
            return;
        }

        this.showAlertDetailsPanel(list[idx]);
    },

    // Opens a panel from outside the table (a deep link), landing on the page
    // that holds the row so the selection is actually visible.
    openAlertById(alertId) {
        const list = this.filteredAlerts || [];
        const idx = list.findIndex(a => a.id === alertId);
        if (idx === -1) return;

        const page = Math.floor(idx / this.alertsPageSize) + 1;
        if (page !== this.alertsCurrentPage) {
            this.currentDetailAlert = list[idx];
            this.alertsCurrentPage = page;
            this.updateAlertsTable();
            return;
        }
        this.showAlertDetailsPanel(list[idx]);
    },

    renderDetailFooter(alert) {
        return `
            <button onclick="Alerts.editAlert('${alert.id}')" class="btn-primary">Edit</button>
            <button onclick="Alerts.toggleAlert('${alert.id}', ${!alert.enabled})" class="btn-secondary">${alert.enabled ? 'Disable' : 'Enable'}</button>
            <button onclick="Alerts.exportYAML('${alert.id}')" class="btn-secondary">Export YAML</button>
            <button onclick="Alerts.deleteAlert('${alert.id}')" class="btn-detail-delete">Delete</button>
        `;
    },

    closeAlertDetailsPanel() {
        const panel = document.getElementById('alertDetailsPanel');
        if (panel) panel.classList.remove('open');
        window.App?.pushSubPath('');
        this.currentDetailAlert = null;
        this._pendingSelectEdge = null;
        AlertDetail.markSelectedRow(null, document.getElementById('alertsList'));
        AlertDetail.stopInset();
        AlertDetail.unbindKeys();
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
        const severityFilter = document.getElementById('alertSeverityFilter')?.value || 'all';
        const labelFilter = document.getElementById('alertLabelFilter')?.value || 'all';
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

            const matchesSeverity = severityFilter === 'all'
                || (alert.severity || 'medium').toLowerCase() === severityFilter;

            const matchesLabel = labelFilter === 'all'
                || (alert.labels || []).includes(labelFilter);

            const matchesAction = this.matchesActionFilter(alert, actionFilter);

            return matchesSearch && matchesStatus && matchesSeverity && matchesLabel && matchesAction;
        });

        // Reset to first page when filters change
        this.alertsCurrentPage = 1;

        this.applyAlertsSort();

        // Re-render the table with pagination
        this.updateAlertsTable();
        this.updateBulkButtons();
    },

    hasActiveFilter() {
        const val = id => document.getElementById(id)?.value || 'all';
        return !!(document.getElementById('alertSearchInput')?.value || '').trim()
            || val('alertStatusFilter') !== 'all'
            || val('alertSeverityFilter') !== 'all'
            || val('alertLabelFilter') !== 'all'
            || val('alertActionFilter') !== 'all';
    },

    clearFilters() {
        const search = document.getElementById('alertSearchInput');
        if (search) search.value = '';
        for (const id of ['alertStatusFilter', 'alertSeverityFilter', 'alertLabelFilter', 'alertActionFilter']) {
            const el = document.getElementById(id);
            if (el) el.value = 'all';
        }
        this.filterAlerts();
    },

    setSeverityFilter(severity) {
        const el = document.getElementById('alertSeverityFilter');
        if (!el) return;
        el.value = severity;
        this.filterAlerts();
    },

    setLabelFilter(label) {
        const el = document.getElementById('alertLabelFilter');
        if (!el) return;
        if (![...el.options].some(o => o.value === label)) {
            el.add(new Option(label, label));
        }
        el.value = label;
        this.filterAlerts();
    },

    // Label dropdown options come from the loaded set, like the feed facets.
    buildLabelFilterOptions() {
        const select = document.getElementById('alertLabelFilter');
        if (!select || !this.allAlerts) return;
        const current = select.value;
        const labels = new Set();
        for (const alert of this.allAlerts) {
            for (const l of (alert.labels || [])) labels.add(l);
        }
        select.innerHTML = '<option value="all">All Labels</option>'
            + [...labels].sort((a, b) => a.localeCompare(b))
                .map(l => `<option value="${Utils.escapeHtml(l)}">${Utils.escapeHtml(l)}</option>`).join('');
        if ([...select.options].some(o => o.value === current)) select.value = current;
    },

    updateBulkButtons() {
        const hasFilter = this.hasActiveFilter();

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
                case 'severity':
                    va = AlertList.severityRank(a.severity);
                    vb = AlertList.severityRank(b.severity);
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

    updateAlertsTable() {
        const alertsList = document.getElementById('alertsList');
        if (!alertsList) return;

        if (this.filteredAlerts.length === 0 && this.allAlerts.length > 0) {
            this.closeAlertDetailsPanel();
            alertsList.innerHTML = AlertList.renderEmptyState({
                filtered: this.hasActiveFilter(),
                noun: 'alerts',
                onClear: 'Alerts.clearFilters'
            });
        } else {
            const alertsHTML = this.renderAlertsTable(this.filteredAlerts);
            alertsList.innerHTML = alertsHTML;
            this.addAlertTableClickHandlers();
            this.restoreDetailPanel();
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
            const normalizersList = data.data || [];
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
            this.currentWebhook = data.data;
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
                    <button class="btn-icon" onclick="Alerts.closeActionDrawer()" title="Close">
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
                    <button class="btn-secondary" onclick="Alerts.closeActionDrawer()">Cancel</button>
                    ${isEdit ? '<button id="testWebhookBtn" class="btn-secondary" onclick="Alerts.testWebhook()">Test</button>' : ''}
                    <button id="saveWebhookBtn" class="btn-primary" onclick="Alerts.saveWebhook()">Save Webhook</button>
                </div>
                <div id="webhookError" class="error-message" style="display: none;"></div>
            </div>
        `;

        this.openActionDrawer();
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

            // Import creates or updates alerts, so it meets the same refusals a save
            // does. Reporting them as "Import failed" hides what to do about it.
            const refusal = this.classifyRefusal(response, data);
            if (refusal === 'gate') {
                this.openImportProposal(errorDiv);
                return;
            }
            if (refusal === 'policy') {
                const messages = (data.data || []).map(v => v.message).filter(Boolean);
                this.showError(errorDiv, messages.length ? messages.join(' ') : (data.error || 'Blocked by policy'));
                return;
            }

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

            const alert = data.data;
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

    // Emits `key: value` with a literal block scalar when the value spans lines,
    // otherwise a double-quoted scalar. Quoting is unconditional so values that
    // look like YAML syntax (leading '*', ': ', '#', pure digits) round-trip.
    yamlField(key, value) {
        const text = (value == null ? '' : String(value)).replace(/\r\n?/g, '\n').replace(/\n+$/, '');
        if (text === '') return `${key}: ""`;
        if (!text.includes('\n')) return `${key}: ${this.yamlQuote(text)}`;
        const body = text.split('\n').map(line => line ? `  ${line}` : '').join('\n');
        // `|2-` pins the indent explicitly so a first line that itself starts with
        // whitespace cannot shift the block's detected indentation; `-` chomps the
        // trailing newline the join would otherwise imply.
        return `${key}: |2-\n${body}`;
    },

    yamlList(key, values) {
        const items = values || [];
        if (items.length === 0) return `${key}: []`;
        return `${key}:\n${items.map(v => `- ${this.yamlQuote(v)}`).join('\n')}`;
    },

    // Double-quoted scalar. Control characters are escaped rather than emitted raw,
    // so a stray newline in a label or action name cannot break out of the scalar.
    yamlQuote(value) {
        const escapes = { '\n': '\\n', '\r': '\\r', '\t': '\\t' };
        const text = (value == null ? '' : String(value))
            .replace(/\\/g, '\\\\')
            .replace(/"/g, '\\"')
            .replace(/[\x00-\x1f\x7f]/g, ch => escapes[ch] || `\\x${ch.charCodeAt(0).toString(16).padStart(2, '0')}`);
        return `"${text}"`;
    },

    // Every action the alert runs, of any kind, as one list of names. Import
    // resolves each name back to its kind, so the export does not carry storage
    // layout the way the editor's own action list does not.
    alertActionNames(alert) {
        return [
            alert.webhook_actions,
            alert.fractal_actions,
            alert.dictionary_actions,
            alert.email_actions,
        ].flatMap(list => (list || []).map(a => a && a.name).filter(Boolean));
    },

    alertToYAML(alert) {
        const actionNames = this.alertActionNames(alert);

        const alertType = alert.alert_type || 'event';
        let yaml = `${this.yamlField('name', alert.name)}
${this.yamlField('description', alert.description)}
${this.yamlField('queryString', alert.query_string)}
alertType: ${alertType}
severity: ${alert.severity || 'medium'}
${this.yamlList('actionNames', actionNames)}
${this.yamlList('labels', alert.labels)}${(alert.references && alert.references.length > 0) ? `
${this.yamlList('references', alert.references)}` : ''}
enabled: ${alert.enabled}
throttleTimeSeconds: ${alert.throttle_time_seconds || 0}${alert.throttle_field ? `
${this.yamlField('throttleField', alert.throttle_field)}` : ''}`;
        if (alertType === 'compound' && alert.window_duration) {
            yaml += `\nwindowDuration: ${alert.window_duration}`;
        }
        if (alertType === 'scheduled') {
            if (alert.schedule_cron) yaml += `\n${this.yamlField('scheduleCron', alert.schedule_cron)}`;
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

            const alert = alertData.data;

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

    // The server has three ways to refuse a write, and a client that cannot tell them
    // apart shows "failed" over an instruction the user could have acted on.
    //
    //   gate    409 with {gate:"required"} - the scope reviews changes, so propose
    //   policy  422 with a violation array - a blocking rule, so fix what it names
    //   null    anything else
    // Reads the import modal the same way importYAML does, so a proposal carries the
    // document and normalizer the user actually chose.
    importYamlPayload() {
        const normalizerGroup = document.getElementById('importNormalizerGroup');
        const normalizerSelect = document.getElementById('importNormalizerSelect');
        const isSigmaVisible = normalizerGroup && normalizerGroup.style.display !== 'none';

        return {
            content: document.getElementById('yamlContent')?.value.trim() || '',
            normalizerID: isSigmaVisible && normalizerSelect ? normalizerSelect.value : '',
            errorDiv: document.getElementById('importError')
        };
    },

    // A gated scope refuses the import, so the document goes to the review queue
    // instead. The modal keeps the YAML on screen and asks only for the one thing the
    // reviewer needs that the document cannot supply.
    openImportProposal(errorDiv) {
        if (errorDiv) errorDiv.style.display = 'none';
        document.getElementById('importProposalBlock')?.remove();

        const anchor = document.getElementById('importYamlModal')?.querySelector('.modal-body') || errorDiv?.parentElement;
        if (!anchor) return;

        anchor.insertAdjacentHTML('beforeend', `
            <div id="importProposalBlock" class="alert-propose">
                <label class="alert-propose-label" for="importProposalSummary">This scope reviews changes, so this import becomes a proposal</label>
                <textarea id="importProposalSummary" class="alert-propose-input" spellcheck="false"
                          placeholder="What is this rule, and why import it"></textarea>
                <div class="alert-propose-actions">
                    <button type="button" class="alert-btn alert-btn-ghost" onclick="document.getElementById('importProposalBlock').remove()">Cancel</button>
                    <button type="button" class="alert-btn alert-btn-primary" onclick="Alerts.submitImportProposal()">Open proposal</button>
                </div>
            </div>
        `);
        document.getElementById('importProposalSummary')?.focus();
    },

    async submitImportProposal() {
        const summaryEl = document.getElementById('importProposalSummary');
        const summary = (summaryEl?.value || '').trim();
        if (!summary) {
            summaryEl?.focus();
            return;
        }

        const { content, normalizerID, errorDiv } = this.importYamlPayload();
        if (!content) return;

        try {
            const res = await fetch('/api/v1/alert-changes/from-yaml', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ content, summary, normalizer_id: normalizerID })
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);

            document.getElementById('importProposalBlock')?.remove();
            this.closeModal('importYamlModal');
            Toast.success('Import proposed', 'Ready for review.');
            this.showProposal(payload.data?.id);
        } catch (e) {
            this.showError(errorDiv, e.message);
        }
    },

    classifyRefusal(response, payload) {
        if (response.status === 409 && payload?.data?.gate === 'required') return 'gate';
        if (response.status === 422 && Array.isArray(payload?.data)) return 'policy';
        return null;
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

            // A reviewed scope refuses the direct delete and expects a proposal. The
            // deletion still has to be reachable, so ask for the reason here rather
            // than leaving the button dead.
            if (response.status === 409 && data.data?.gate === 'required') {
                this.openDeleteProposal(alertId);
                return;
            }

            if (data.success) {
                this.loadAlerts();
                Toast.show('Alert deleted successfully', 'success');
            } else {
                throw new Error(data.error || 'Failed to delete alert');
            }
        } catch (error) {
            console.error('Delete alert error:', error);
            Toast.show('Failed to delete alert', error.message);
        }
    },

    // Deleting a detection is gated like any other change, and the reason is the part a
    // reviewer needs most.
    openDeleteProposal(alertId) {
        document.getElementById('alertDeleteProposal')?.remove();

        const alert = (this.alerts || []).find(a => a.id === alertId);
        document.body.insertAdjacentHTML('beforeend', `
            <div id="alertDeleteProposal" class="ac-modal">
                <div class="ac-modal-card">
                    <div class="ac-modal-head">Propose deleting ${Utils.escapeHtml(alert?.name || 'this alert')}</div>
                    <textarea id="alertDeleteReason" class="ac-compose-input" spellcheck="false"
                              placeholder="Why should it go?"></textarea>
                    <div class="ac-compose-actions">
                        <button class="btn-secondary btn-sm" onclick="document.getElementById('alertDeleteProposal').remove()">Cancel</button>
                        <button class="btn-primary btn-sm" onclick="Alerts.submitDeleteProposal('${Utils.escapeAttr(alertId)}')">Propose deletion</button>
                    </div>
                </div>
            </div>
        `);
        document.getElementById('alertDeleteReason')?.focus();
    },

    async submitDeleteProposal(alertId) {
        const reasonEl = document.getElementById('alertDeleteReason');
        const summary = (reasonEl?.value || '').trim();
        if (!summary) {
            reasonEl?.focus();
            return;
        }

        const alert = (this.alerts || []).find(a => a.id === alertId);
        try {
            const res = await fetch('/api/v1/alert-changes', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    kind: 'delete',
                    alert_id: alertId,
                    title: `Delete ${alert?.name || 'alert'}`,
                    summary
                })
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);

            document.getElementById('alertDeleteProposal')?.remove();
            Toast.success('Deletion proposed', 'Ready for review.');
            this.showProposal(payload.data?.id);
        } catch (e) {
            Toast.error('Could not propose deletion', e.message);
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

            const alert = data.data;
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

            this.renderWebhooks(data.data || []);
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

            const webhooks = data.data || [];
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
                const result = data.data;
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
                this.closeActionDrawer();
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

        this.setupResultTabs();
        window.AlertTests?.load(alertId);
        window.AlertPolicy?.load();
        this.loadGateMode(alertId);

        // Set up query input with debounced testing FIRST
        this.setupQueryTesting();
        this.autosizeDescription();

        // Set up alert editor pagination
        this.setupAlertPagination();

        // Set up SQL toggle functionality
        this.setupAlertSqlToggle();

        // Loading an existing alert also loads its actions, pre-selected.
        this._editorReady = alertId ? this.loadAlertIntoEditor(alertId) : this.loadActionsIntoEditor();

        // History and drafts both need a saved alert to hang off.
        const historyTab = document.querySelector('#alertResultTabs .ert-tab[data-pane="history"]');
        if (historyTab) historyTab.hidden = !alertId;
        this._typeInferred = false;
        this.updateTypeBadge();
        this.watchEditorEdits();

        // Drafting starts once the form is filled, so the load is not taken for an edit.
        this.sizeNameInput();
        this.syncEnabledLabel();
        Promise.resolve(this._editorReady).finally(() => {
            this.sizeNameInput();
            this.syncEnabledLabel();
            window.AlertDrafts?.start(alertId);
        });
    },

    // Editor teardown, shared by the post-save return and by tab navigation away
    // from the editor, so no stale state (feed banner, log detail, form values)
    // survives leaving it.
    closeAlertEditor() {
        const alertEditorView = document.getElementById('alertEditorView');
        if (!alertEditorView || alertEditorView.style.display === 'none') return;

        this.editingFeedAlert = false;
        this.feedAlertOriginalId = null;
        window.AlertTests?.release();
        window.AlertPolicy?.reset();
        window.AlertDrafts?.stop();
        document.querySelector('#alertEditorView .ae-body')?.classList.remove('ae-inspecting');
        this.cancelPropose();
        this.cancelTestQuery();
        this._queryChrome().clear();
        document.getElementById('feedAlertBanner')?.remove();
        alertEditorView.style.display = 'none';

        this.closeAlertPanel();
        if (window.LogDetail) LogDetail.close();
        this.clearAlertEditor();
    },

    backToAlerts() {
        const wasFromFeed = this.editingFeedAlert;

        this.closeAlertEditor();

        const alertsTabContent = document.getElementById('fractalAlertsTabContent');
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
    },

    // Grows the description textarea to fit its content, up to the CSS max-height.
    // A height we did not set ourselves means the user dragged the resize handle,
    // and from then on their height wins until the editor is cleared.
    autosizeDescription() {
        const el = document.getElementById('editorAlertDescription');
        if (!el) return;
        if (!el.dataset.autosizeBound) {
            el.dataset.autosizeBound = '1';
            el.addEventListener('input', () => this.autosizeDescription());
        }
        if (el.style.height !== (el.dataset.autoHeight || '')) return;
        el.style.height = 'auto';
        const contentHeight = el.scrollHeight;
        // Zero means the field is not laid out yet; leave the CSS height alone.
        el.style.height = contentHeight > 0 ? `${contentHeight}px` : '';
        el.dataset.autoHeight = el.style.height;
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
        if (descField) {
            descField.value = '';
            descField.style.height = '';
            descField.dataset.autoHeight = '';
        }
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

    // Same deferral policy as the search page, with the editor's own appearance.
    _queryChrome() {
        if (!this._chromeCtl) {
            this._chromeCtl = LoadingChrome.create({
                show: (mode, ctx) => {
                    this._chromeCtx = ctx;
                    this._setTestRunButtonState(true);
                    // Nothing to preserve on a first run, and on a streaming run the
                    // rows replace this the moment they arrive.
                    if (!this._chromeCtl.gotRows && ctx && ctx.resultsDiv) {
                        ctx.resultsDiv.innerHTML =
                            '<div class="loading-spinner"><span class="spinner"></span>' +
                            '<button class="cancel-query-btn" onclick="Alerts.cancelTestQuery()">Cancel</button></div>';
                    }
                },
                hide: () => {},
                // A cancelled query returns on AbortError without touching the results,
                // so anything still showing the spinner would keep it forever. Results,
                // an error or an empty state have all already replaced it by here.
                finish: () => {
                    const el = this._chromeCtx && this._chromeCtx.resultsDiv;
                    if (el && el.querySelector('.loading-spinner')) {
                        el.innerHTML = '<div class="no-results"><p>Query cancelled</p></div>';
                        const count = document.getElementById('alertResultsCount');
                        if (count) count.textContent = '';
                    }
                }
            });
        }
        return this._chromeCtl;
    },

    // Paints one histogram frame. Frames are cumulative, so the latest is the most
    // complete and simply replaces what is drawn.
    _paintAlertHistogram(frame, histTimeRange) {
        if (!window.Timeline || !frame || !Array.isArray(frame.buckets) || !frame.buckets.length) return;
        this._alertHistPainted = true;

        // The frame's own bounds win over the requested range: the server snaps the
        // histogram window out to whole buckets, so stretching the bucket array across
        // the unsnapped request shifts every bar by up to one bucket.
        const range = (frame.bucket_start && frame.bucket_seconds)
            ? {
                start: frame.bucket_start,
                end: new Date(new Date(frame.bucket_start).getTime()
                    + frame.buckets.length * frame.bucket_seconds * 1000).toISOString()
              }
            : histTimeRange;
        if (!range) return;

        // covered_from marks how far the newest-first scan has reached. Without it the
        // unscanned span draws as real zeros, and an analyst reads "no matches" from a
        // window that was never counted.
        Timeline.renderBucketsToEl(frame.buckets, range,
            document.getElementById('alertTimeline'),
            document.getElementById('alertTimelineSection'),
            { coveredFrom: frame.covered_from || 0, pendingLabel: 'not scanned', select: false });
    },

    // Keyed off what the button is actually showing, not off whether a request exists.
    // Keying off the request meant a run started by the typing debounce put the button
    // into Cancel, so the next click cancelled that run instead of starting one.
    runOrCancelQuery() {
        if (this._alertQueryRunning) this.cancelTestQuery();
        else this.testQuery();
    },

    cancelTestQuery() {
        if (this.currentTestRequest) {
            this.currentTestRequest.abort();
            this.currentTestRequest = null;
        }
        this._endQueryChrome();
    },

    // The chrome controller owns the deferral, so the button and the spinner appear
    // together or not at all.
    _endQueryChrome() {
        this._queryChrome().end();
        this._setTestRunButtonState(false);
    },

    _setTestRunButtonState(running) {
        this._alertQueryRunning = running;
        const btn = document.getElementById('testQueryBtn');
        if (!btn) return;
        const text = btn.querySelector('.btn-text');
        const shortcut = btn.querySelector('.btn-shortcut');
        if (running) {
            btn.classList.add('is-running');
            if (text) text.textContent = 'Cancel';
            if (shortcut) shortcut.style.display = 'none';
        } else {
            btn.classList.remove('is-running');
            if (text) text.textContent = 'Run';
            if (shortcut) shortcut.style.display = '';
        }
    },

    async testQuery() {
        const queryInput = document.getElementById('editorQueryInput');
        const resultsDiv = document.getElementById('queryResults');
        const countDiv = document.getElementById('alertResultsCount');
        const execTimeEl = document.getElementById('alertExecutionTime');

        if (!queryInput || !resultsDiv) return;

        const rawQuery = queryInput.value.trim();
        if (!rawQuery) {
            resultsDiv.innerHTML = '<div class="empty-state"><div class="empty-icon">🔍</div><div class="empty-text">Run a query to see results that would trigger this alert</div></div>';
            if (countDiv) countDiv.textContent = '';
            return;
        }

        const query = this.stripComments(rawQuery);
        if (!query) {
            resultsDiv.innerHTML = '<div class="empty-state"><div class="empty-icon">🔍</div><div class="empty-text">Run a query to see results that would trigger this alert</div></div>';
            if (countDiv) countDiv.textContent = '';
            return;
        }

        // Tests evaluate alongside the query rather than behind their own button: the
        // corpus is already loaded server-side, so this is one small query per test.
        window.AlertTests?.run(query);
        window.AlertPolicy?.evaluate();

        // Cancel any in-flight request before starting a new one
        if (this.currentTestRequest) {
            this.currentTestRequest.abort();
        }

        const controller = new AbortController();
        this.currentTestRequest = controller;
        this._alertHistPainted = false;

        // Deferred, like the search page: replacing the results immediately means every
        // debounced run wipes what you were reading and flashes a spinner, even when the
        // query takes 30ms.
        this._queryChrome().begin({ resultsDiv });
        if (countDiv) countDiv.textContent = '—';
        if (execTimeEl) execTimeEl.textContent = '';

        const alertExportWrap = document.getElementById('alertExportMenuWrap');
        if (alertExportWrap) alertExportWrap.style.display = 'none';
        this.hideAlertPagination();

        const timeRange = this.getTimeRange();
        const requestBody = { query, start: timeRange.start, end: timeRange.end, source: 'alert' };
        if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
            requestBody.fractal_id = window.FractalContext.currentFractal.id;
        }

        try {
            const res = await fetch('/api/v1/query/stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                signal: controller.signal,
                body: JSON.stringify(requestBody),
            });

            if (!res.ok) {
                const errData = await res.json().catch(() => ({}));
                throw new Error(errData.error || `HTTP ${res.status}`);
            }

            const contentType = res.headers.get('Content-Type') || '';
            if (!contentType.includes('application/x-ndjson')) {
                // Auth/parse/translate error short-circuited as plain JSON
                const data = await res.json().catch(() => ({}));
                if (!data.success) throw new Error(data.error || 'Query failed');
                this._applyAlertQueryResult({ results: data.results || [], fieldOrder: data.field_order || null,
                    isAggregated: data.is_aggregated || false, sql: data.sql }, resultsDiv, countDiv, execTimeEl, alertExportWrap);
                return;
            }

            await this._consumeAlertStream(res, resultsDiv, countDiv, execTimeEl, alertExportWrap, controller);

        } catch (error) {
            if (error.name === 'AbortError') return;
            resultsDiv.innerHTML = `<div class="query-error"><p>Query Error: ${Utils.escapeHtml(error.message)}</p></div>`;
            if (countDiv) countDiv.textContent = 'Error';
            if (alertExportWrap) alertExportWrap.style.display = 'none';
        } finally {
            if (this.currentTestRequest === controller) {
                this.currentTestRequest = null;
                this._endQueryChrome();
            }
        }
    },

    async _consumeAlertStream(res, resultsDiv, countDiv, execTimeEl, exportBtn, controller) {
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        let rows = [];
        let fieldOrder = null;
        let isAggregated = false;
        let firstRows = true;
        let histogram = null;
        let histTimeRange = null;
        let histFrames = 0;

        try {
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });

                const lines = buf.split('\n');
                buf = lines.pop();

                for (const line of lines) {
                    const trimmed = line.trim();
                    if (!trimmed) continue;
                    let frame;
                    try { frame = JSON.parse(trimmed); } catch { continue; }

                    switch (frame.type) {
                        case 'meta':
                            fieldOrder = frame.field_order || null;
                            isAggregated = frame.is_aggregated || false;
                            this.fieldOrder = fieldOrder;
                            this.isAggregated = isAggregated;
                            histTimeRange = {
                                start: frame.time_start || this.getTimeRange().start,
                                end: frame.time_end || this.getTimeRange().end,
                            };
                            break;
                        case 'histogram':
                            // The server scans the histogram newest-first and keeps
                            // sending frames after 'done'. Painting only at 'done' left
                            // the timeline showing whichever chunks happened to arrive
                            // first, permanently partial on a wide range.
                            histogram = frame.buckets || null;
                            histFrames++;
                            this._paintAlertHistogram(frame, histTimeRange);
                            break;
                        case 'histogram_end':
                            // The scan stopped without covering the range. Keep what was
                            // counted rather than implying more is coming.
                            if (histFrames === 0) {
                                const emptySection = document.getElementById('alertTimelineSection');
                                if (emptySection) emptySection.style.display = 'none';
                            }
                            break;
                        case 'rows': {
                            const incoming = frame.data || [];
                            if (!incoming.length) break;
                            rows = rows.concat(incoming);
                            this._queryChrome().markRows();
                            if (firstRows) {
                                firstRows = false;
                                resultsDiv.innerHTML = '';
                            }
                            this.currentResults = rows;
                            if (countDiv) countDiv.textContent = `${rows.length} result${rows.length === 1 ? '' : 's'}`;
                            if (window.QueryExecutor) {
                                QueryExecutor.renderResultsToElement(rows, resultsDiv, fieldOrder, {
                                    allResults: rows, isAggregated, detailHost: 'alert',
                                    gutter: isAggregated ? null : (window.AlertTests && AlertTests.gutter), comments: false,
                                });
                            }
                            break;
                        }
                        case 'error':
                            throw new Error(frame.error || 'Query error');
                        case 'done':
                            // The stream stays open past this to drain histogram
                            // chunks, so the deferred indicator has to be stood down
                            // here rather than in the finally.
                            this._queryChrome().settle();
                            this._applyAlertQueryResult(
                                { results: rows, fieldOrder, isAggregated, sql: frame.sql, executionMs: frame.execution_ms,
                                  histogram, histTimeRange },
                                resultsDiv, countDiv, execTimeEl, exportBtn
                            );
                            break;
                    }
                }
            }
        } finally {
            reader.releaseLock();
        }
    },

    _applyAlertQueryResult({ results, fieldOrder, isAggregated, sql, executionMs, histogram, histTimeRange }, resultsDiv, countDiv, execTimeEl, exportBtn) {
        this.currentResults = results;
        this.fieldOrder = fieldOrder;
        this.isAggregated = isAggregated;
        this.inferAlertType(isAggregated);

        if (countDiv) countDiv.textContent = `${results.length} result${results.length === 1 ? '' : 's'}`;
        if (execTimeEl && executionMs) execTimeEl.textContent = `${executionMs}ms`;

        const sqlOutput = document.getElementById('alertSqlOutput');
        if (sql && sqlOutput && window.QueryExecutor) {
            sqlOutput.innerHTML = QueryExecutor.highlightSQL(sql);
            const sqlPreview = document.querySelector('#alertEditorView .sql-preview');
            if (sqlPreview && window.UserPrefs && UserPrefs.showSQL()) {
                sqlPreview.style.display = 'block';
            }
        }

        // The timeline belongs to _paintAlertHistogram, which draws each frame as it
        // lands and knows how far the scan has reached. Repainting here would drop
        // covered_from and redraw the unscanned span as real zeros. All that is left to
        // decide is whether there is a timeline at all.
        if (!this._alertHistPainted) {
            const sectionEl = document.getElementById('alertTimelineSection');
            if (sectionEl) sectionEl.style.display = 'none';
        }

        if (results.length === 0) {
            resultsDiv.innerHTML = '<div class="no-results"><p>No results found for this query in the selected time range</p></div>';
            this.hideAlertPagination();
        } else {
            this.alertCurrentPage = 1;
            this.showAlertPagination();
            this.updateAlertPagination();
            const pageResults = this.getCurrentAlertPageResults();
            if (window.QueryExecutor) {
                QueryExecutor.renderResultsToElement(pageResults, resultsDiv, fieldOrder, {
                    allResults: results, isAggregated, detailHost: 'alert',
                    gutter: isAggregated ? null : (window.AlertTests && AlertTests.gutter), comments: false,
                });
            }
            if (exportBtn && results.length > 0) exportBtn.style.display = 'inline-block';
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
                    const startMs = TZ.parseWallClock(customStart.value);
                    const endMs = TZ.parseWallClock(customEnd.value);

                    // Validate that start is before end
                    if (Number.isFinite(startMs) && Number.isFinite(endMs) && startMs < endMs) {
                        return {
                            start: new Date(startMs).toISOString(),
                            end: new Date(endMs).toISOString()
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
                    this.runOrCancelQuery();
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
                    const zoneTag = document.getElementById('alertCustomZone');
                    if (zoneTag) zoneTag.textContent = TZ.abbrev();
                    // Initialize custom inputs with default values if empty
                    if (customStart && customEnd) {
                        if (!customStart.value) {
                            const now = Date.now();
                            customStart.value = TZ.formatInput(now - 24 * 60 * 60 * 1000);
                            customEnd.value = TZ.formatInput(now);
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

    // Results | Tests over the editor's result area. The pagination and export
    // controls belong to results only, so they hide with that pane.
    setupResultTabs() {
        const tabs = document.getElementById('alertResultTabs');
        if (!tabs) return;

        // Every open starts on Results. The listeners are wired once, but the active
        // pane is not: an editor opened after a visit to Tests would otherwise render
        // its query results into a hidden div.
        tabs.querySelector('.ert-tab[data-pane="results"]')?.click();

        if (tabs.dataset.wired) return;
        tabs.dataset.wired = '1';

        tabs.querySelectorAll('.ert-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                tabs.querySelectorAll('.ert-tab').forEach(t => t.classList.toggle('active', t === tab));
                const pane = tab.dataset.pane;
                const panes = {
                    results: document.getElementById('alertResultsPane'),
                    tests: document.getElementById('alertTestsPane'),
                    checks: document.getElementById('alertChecksPane'),
                    history: document.getElementById('alertHistoryPane')
                };
                for (const [name, el] of Object.entries(panes)) {
                    if (el) el.hidden = name !== pane;
                }
                if (pane === 'history' && panes.history && this.currentAlert && window.AlertHistory) {
                    AlertHistory.renderInto(panes.history, this.currentAlert.id, this.currentAlert.name);
                }
                const controls = document.getElementById('alertResultsControls');
                if (controls) controls.style.visibility = pane === 'results' ? '' : 'hidden';
            });
        });
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

    // Panel controls
    // The definition is a rail in the layout now, not a slide-out. These stay as
    // no-ops so the many callers that closed the panel on navigation keep working.
    toggleAlertPanel() {},
    closeAlertPanel() {},
    openAlertPanel() {},

    async loadAlertIntoEditor(alertId) {
        try {
            const response = await fetch(`/api/v1/alerts/${alertId}`, {
                credentials: 'include'
            });

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.error || 'Failed to load alert');
            }

            const alert = data.data;
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
            if (descField) {
                descField.value = alert.description || '';
                this.autosizeDescription();
            }
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

            const webhooks = (webhooksData.success ? webhooksData.data : null) || [];
            const fractalActions = (actionsData.success ? actionsData.data : null) || [];
            const dictActions = (dictActionsData.success ? dictActionsData.data : null) || [];
            const emailActions = (emailActionsData.success ? emailActionsData.data : null) || [];

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
        const formData = this.getAlertEditorFormData();
        if (!formData) return;

        // Validate query syntax before saving
        try {
            const query = this.stripComments(formData.query_string);
            const timeRange = this.getTimeRange();
            const validateBody = { query, start: timeRange.start, end: timeRange.end, source: 'alert' };
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

        if (this.gateEnabled && !this.editingFeedAlert) {
            this.openProposeComposer(formData);
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

            // A policy block is not a failure to report and forget: the reasons go to
            // the Checks tab and onto the fields, where they can be acted on.
            // Neither refusal is a failure to report and forget. A policy block goes to
            // the Checks tab and onto the fields; a gate refusal means the scope started
            // reviewing changes since the editor opened, so switch to proposing.
            if (!response.ok) {
                const payload = await response.json().catch(() => ({}));
                switch (this.classifyRefusal(response, payload)) {
                    case 'policy':
                        window.AlertPolicy?.showBlocked(payload.data || []);
                        Toast.error('Blocked by policy', payload.error || '');
                        return;
                    case 'gate':
                        this.gateEnabled = true;
                        this.applyGateMode(this.currentAlert ? this.currentAlert.id : null);
                        this.openProposeComposer(formData);
                        return;
                }
            }

            if (!response.ok) {
                const errorData = await Utils.errorMessage(response);
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
                await window.AlertDrafts?.finished();
                this.backToAlerts();
            } else {
                Toast.show(data.error || 'Failed to save alert', 'error');
            }
        } catch (error) {
            console.error('Save alert error:', error);
            Toast.show('Network error: ' + error.message, 'error');
        }
    },

    // A gated scope takes proposals, not direct writes, so the editor relabels its save
    // and routes it elsewhere. Loaded per editor open: the gate is per scope, and the
    // scope can change between alerts.
    async loadGateMode(alertId) {
        this.gateEnabled = false;
        this.applyGateMode(alertId);

        try {
            const res = await fetch('/api/v1/alert-gate', { credentials: 'include' });
            if (!res.ok) return;
            const payload = await res.json();
            this.gateEnabled = !!payload.data?.enabled;
        } catch (e) {
            this.gateEnabled = false;
        }
        this.applyGateMode(alertId);
    },

    applyGateMode(alertId) {
        const saveBtn = document.getElementById('saveAlertBtn');
        if (saveBtn && !this.editingFeedAlert) {
            if (this.gateEnabled) saveBtn.textContent = 'Propose change';
            else saveBtn.textContent = alertId ? 'Update Alert' : 'Create Alert';
        }

    },

    // Proposing needs a sentence for the reviewer. It is composed in the editor's own
    // action bar rather than a browser dialog: the author is describing work that is
    // still on screen, and a dialog both hides it and loses the text on a stray Escape.
    openProposeComposer(formData) {
        this._pendingProposal = formData;

        const head = document.querySelector('#alertEditorView .ae-head');
        if (!head) return;
        document.getElementById('alertProposeComposer')?.remove();

        head.insertAdjacentHTML('afterend', `
            <div id="alertProposeComposer" class="alert-propose">
                <label class="alert-propose-label" for="alertProposeSummary">Describe this change for the reviewer</label>
                <textarea id="alertProposeSummary" class="alert-propose-input" spellcheck="false"
                          placeholder="What changed, and why"></textarea>
                <div class="alert-propose-actions">
                    <button type="button" class="alert-btn alert-btn-ghost" onclick="Alerts.cancelPropose()">Cancel</button>
                    <button type="button" class="alert-btn alert-btn-primary" onclick="Alerts.submitProposal()">Open proposal</button>
                </div>
            </div>
        `);
        const saveBtn = document.getElementById('saveAlertBtn');
        if (saveBtn) saveBtn.style.display = 'none';
        document.getElementById('alertProposeSummary')?.focus();
    },

    cancelPropose() {
        this._pendingProposal = null;
        document.getElementById('alertProposeComposer')?.remove();
        const saveBtn = document.getElementById('saveAlertBtn');
        if (saveBtn) saveBtn.style.display = '';
    },

    async submitProposal() {
        const summaryEl = document.getElementById('alertProposeSummary');
        const summary = (summaryEl?.value || '').trim();
        if (!summary) {
            summaryEl?.focus();
            return;
        }

        const formData = this._pendingProposal;
        if (!formData) return;

        const content = {
            name: formData.name,
            description: formData.description,
            query_string: formData.query_string,
            alert_type: formData.alert_type,
            severity: formData.severity,
            throttle_time_seconds: formData.throttle_time_seconds,
            throttle_field: formData.throttle_field,
            labels: formData.labels,
            references: formData.references,
            window_duration: formData.window_duration,
            schedule_cron: formData.schedule_cron,
            query_window_seconds: formData.query_window_seconds,
            webhook_action_ids: formData.webhook_action_ids,
            fractal_action_ids: formData.fractal_action_ids,
            dictionary_action_ids: formData.dictionary_action_ids,
            email_action_ids: formData.email_action_ids
        };

        try {
            const res = await fetch('/api/v1/alert-changes', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    kind: this.currentAlert ? 'update' : 'create',
                    alert_id: this.currentAlert ? this.currentAlert.id : '',
                    title: formData.name,
                    summary,
                    content,
                    tests: formData.tests || []
                })
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);

            this.cancelPropose();
            await window.AlertDrafts?.finished();
            Toast.success('Proposal opened', 'Ready for review.');
            if (payload.data?.id) this.showProposal(payload.data.id);
            else this.backToAlerts();
        } catch (e) {
            Toast.error('Could not open proposal', e.message);
        }
    },

    // Asks the model for ATT&CK labels for the rule as it stands and adds the ones
    // the embedded matrix confirms. Existing labels are left alone.
    async suggestLabels() {
        const btn = document.getElementById('alertLabelsAiBtn');
        if (!btn || btn.classList.contains('busy')) return;

        const subject = this.getPolicySubject();
        if (!subject.name && !subject.query_string && !subject.description) {
            Toast.show('Give the rule a name or a query first', 'warning');
            return;
        }

        btn.classList.add('busy');
        try {
            const res = await fetch('/api/v1/alerts/suggest-labels', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: subject.name, description: subject.description,
                    query_string: subject.query_string, labels: subject.labels || []
                })
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);

            const labels = payload.data?.labels || [];
            if (!labels.length) {
                Toast.show('No ATT&CK mapping found for this rule', 'info');
                return;
            }
            labels.forEach(l => this.addLabelChip(l.label));
            this.syncLabelsToHidden();
            window.AlertDrafts?.touch();
            Toast.show(labels.map(l => `${l.label} (${l.name})`).join(', '), 'success');
        } catch (e) {
            Toast.show(e.message || 'Could not suggest labels', 'error');
        } finally {
            btn.classList.remove('busy');
        }
    },

    // Event versus compound is a property of the query, not a separate choice: an
    // aggregation means compound. Inference applies to a query the user has changed;
    // a saved alert's own type, and a type picked by hand in the rail, are left alone.
    inferAlertType(aggregated) {
        const select = document.getElementById('alertTypeSelect');
        if (!select) return;
        const current = select.value;
        const query = document.getElementById('editorQueryInput')?.value || '';
        const untouched = this.currentAlert && query === (this.currentAlert.query_string || '');

        if (aggregated && current === 'event' && !untouched) {
            select.value = 'compound';
            select.dispatchEvent(new Event('change'));
            this.setAlertTypeCard('compound');
            this._typeInferred = true;
        } else if (!aggregated && current === 'compound' && this._typeInferred) {
            select.value = 'event';
            select.dispatchEvent(new Event('change'));
            this.setAlertTypeCard('event');
            this._typeInferred = false;
        }
        this.updateTypeBadge();
    },

    updateTypeBadge() {
        const select = document.getElementById('alertTypeSelect');
        const value = document.getElementById('alertTypeBadgeValue');
        const hint = document.getElementById('alertTypeBadgeHint');
        if (!select || !value) return;
        const type = select.value || 'event';
        value.textContent = type.charAt(0).toUpperCase() + type.slice(1);
        if (hint) hint.textContent = this._typeInferred ? 'from the query' : '';
    },

    // The name field is as wide as its text, so the status pill reads as part of it.
    sizeNameInput() {
        const input = document.getElementById('editorAlertName');
        if (!input) return;
        let probe = document.getElementById('aeNameProbe');
        if (!probe) {
            probe = document.createElement('span');
            probe.id = 'aeNameProbe';
            probe.className = 'ae-name-probe';
            input.parentElement.appendChild(probe);
        }
        probe.textContent = input.value || input.placeholder || '';
        const width = Math.min(Math.max(probe.offsetWidth + 22, 60), 640);
        input.style.width = width + 'px';
    },

    syncEnabledLabel() {
        const box = document.getElementById('editorAlertEnabled');
        const text = document.getElementById('alertEnabledText');
        if (box && text) text.textContent = box.checked ? 'Enabled' : 'Disabled';
    },

    // One listener for everything the draft and the type badge react to.
    watchEditorEdits() {
        const view = document.getElementById('alertEditorView');
        if (!view || view.dataset.editsWatched) return;
        view.dataset.editsWatched = '1';

        const onEdit = (e) => {
            if (e.target.id === 'editorAlertName') this.sizeNameInput();
            // Only the definition counts: preview range and paging are not edits.
            if (!e.target.closest('.ae-rail, .ae-name-input, #editorQueryInput')) return;
            window.AlertDrafts?.touch();
        };
        view.addEventListener('input', onEdit);
        view.addEventListener('change', onEdit);

        // Inspecting a log wants the width the search page has, so the rail folds
        // away while the detail panel is open and returns when it closes.
        const detail = document.getElementById('alertLogDetailPanel');
        const body = view.querySelector('.ae-body');
        if (detail && body) {
            const sync = () => body.classList.toggle('ae-inspecting', detail.classList.contains('open'));
            new MutationObserver(sync).observe(detail, { attributes: true, attributeFilter: ['class'] });
            sync();
        }

        document.getElementById('alertTypeSelect')?.addEventListener('change', () => this.updateTypeBadge());
        document.getElementById('editorAlertEnabled')?.addEventListener('change', () => this.syncEnabledLabel());
        document.querySelectorAll('.alert-type-card').forEach(card => {
            card.addEventListener('click', () => { this._typeInferred = false; this.updateTypeBadge(); });
        });
    },

    // Lands on the Changes tab with the given proposal open in its drawer. Leaves the
    // editor directly rather than through backToAlerts, whose list reload would reopen
    // the alert on top of the Changes tab.
    async showProposal(id) {
        if (!id || !window.AlertFeeds?.showChangesTab || !window.AlertChanges) return;
        this.closeAlertEditor();
        const tab = document.getElementById('fractalAlertsTabContent');
        if (tab) tab.style.display = 'block';
        AlertFeeds.showChangesTab();
        await AlertChanges.show();
        AlertChanges.open(id);
    },

    // Opens the editor onto a draft from the drafts list.
    async openDraft(draft) {
        if (window.AlertFeeds?.showManualAlerts) AlertFeeds.showManualAlerts();
        this.showAlertEditor(draft.alert_id || null);
        try { await this._editorReady; } catch (e) { /* the editor reported it */ }
        window.AlertDrafts?.open(draft);
    },

    // The definition as it stands in the editor, for a policy check that must judge the
    // edit in progress rather than the last saved version. Deliberately not
    // getAlertEditorFormData: that one validates and toasts, and a check running on a
    // keystroke must do neither.
    getPolicySubject() {
        const value = (id) => document.getElementById(id)?.value || '';
        const listFrom = (id, sep) => {
            const raw = document.getElementById(id)?.value || '';
            return raw ? raw.split(sep).map(s => s.trim()).filter(Boolean) : [];
        };

        this.syncLabelsToHidden();
        this.syncReferencesToTextarea();

        const actionIds = { webhook: [], 'fractal-action': [], 'dictionary-action': [], 'email-action': [] };
        document.querySelectorAll('#editorSelectedActions .selected-action-item').forEach(item => {
            if (actionIds[item.dataset.type]) actionIds[item.dataset.type].push(item.dataset.id);
        });

        const tests = window.AlertTests?.payload() || [];
        const run = window.AlertTests?.lastRun?.() || null;

        return {
            name: value('editorAlertName').trim(),
            description: value('editorAlertDescription').trim(),
            query_string: value('editorQueryInput').trim(),
            alert_type: document.getElementById('alertTypeSelect')?.value || 'event',
            severity: value('editorAlertSeverity') || 'medium',
            throttle_time_seconds: parseInt(value('editorThrottleTime'), 10) || 0,
            throttle_field: value('editorThrottleField').trim(),
            labels: listFrom('editorAlertLabels', ','),
            references: listFrom('editorAlertReferences', '\n'),
            webhook_action_ids: actionIds.webhook,
            fractal_action_ids: actionIds['fractal-action'],
            dictionary_action_ids: actionIds['dictionary-action'],
            email_action_ids: actionIds['email-action'],
            tests,
            tests_run: !!run,
            tests_passing: run ? run.failed === 0 : false
        };
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
            email_action_ids: emailActionIDs,
        };

        // Omitted, not sent as [], when the editor never loaded the saved corpus: the
        // server reads a missing key as "unchanged" and an empty array as "delete them".
        const tests = window.AlertTests?.payload();
        if (tests !== undefined) formData.tests = tests;

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
                    allResults: this.currentResults, detailHost: 'alert',
                    gutter: window.AlertTests && AlertTests.gutter, comments: false,
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
                    allResults: this.currentResults, detailHost: 'alert',
                    gutter: window.AlertTests && AlertTests.gutter, comments: false,
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
                isAggregated: this.isAggregated, detailHost: 'alert',
                gutter: this.isAggregated ? null : (window.AlertTests && AlertTests.gutter), comments: false,
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

        if (FractalContext.shouldReload('alertsView')) this.loadAlerts();
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

            const fractalActions = data.data || [];
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
            this.currentFractalAction = data.data;
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
                    <button class="btn-icon" onclick="Alerts.closeActionDrawer()" title="Close">
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
                    <button class="btn-secondary" onclick="Alerts.closeActionDrawer()">Cancel</button>
                    <button id="saveFractalActionBtn" class="btn-primary" onclick="Alerts.saveFractalAction()">Save Fractal Action</button>
                </div>
                <div id="fractalActionError" class="error-message" style="display: none;"></div>
            </div>
        `;

        this.openActionDrawer();
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
            this.closeActionDrawer();
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

            const actions = data.data || [];
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
                    <button class="btn-icon" onclick="Alerts.closeActionDrawer()" title="Close">
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
                    <button class="btn-secondary" onclick="Alerts.closeActionDrawer()">Cancel</button>
                    <button id="saveDictActionBtn" class="btn-primary" onclick="Alerts.saveDictAction()">Save Dictionary Action</button>
                </div>
                <div id="dictActionError" class="error-message" style="display: none;"></div>
            </div>
        `;

        this.openActionDrawer();
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

            this.closeActionDrawer();
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

            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const filename = this._downloadResults(blob, 'csv');
            if (filename) Toast.show(`Exported ${this.currentResults.length} results to ${filename}`, 'success');
        } catch (error) {
            console.error('Export error:', error);
            Toast.show('Failed to export CSV: ' + error.message, 'error');
        }
    },

    exportToJsonl() {
        if (!this.currentResults || this.currentResults.length === 0) {
            Toast.show('No results to export', 'warning');
            return;
        }
        try {
            const order = this.fieldOrder || [];
            const lines = this.currentResults.map(row => {
                let flat = row;
                if (row._all_fields && typeof row._all_fields === 'object') {
                    const { _all_fields, ...projected } = row;
                    flat = { ..._all_fields, ...projected };
                }
                // Lead with the columns on screen, then everything else.
                const out = {};
                for (const f of order) if (f in flat) out[f] = flat[f];
                for (const k of Object.keys(flat)) if (!(k in out)) out[k] = flat[k];
                return JSON.stringify(out);
            });
            const blob = new Blob([lines.join('\n') + '\n'], { type: 'application/x-ndjson;charset=utf-8;' });
            const filename = this._downloadResults(blob, 'jsonl');
            if (filename) Toast.show(`Exported ${this.currentResults.length} results to ${filename}`, 'success');
        } catch (error) {
            console.error('Export error:', error);
            Toast.show('Failed to export JSON Lines: ' + error.message, 'error');
        }
    },

    _downloadResults(blob, ext) {
        const link = document.createElement('a');
        if (link.download === undefined) {
            Toast.show('Downloads are not supported in this browser', 'error');
            return null;
        }
        const timestamp = new Date().toISOString().slice(0, 19).replace(/[:]/g, '-');
        const filename = `bifract-alert-results-${timestamp}.${ext}`;
        const url = URL.createObjectURL(blob);
        link.href = url;
        link.download = filename;
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        return filename;
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
            ((webhooksData.success ? webhooksData.data : null) || []).forEach(w => {
                actions.push({ ...w, actionType: 'webhook', actionLabel: 'Webhook', detail: w.url });
            });
            ((emailData.success ? emailData.data : null) || []).forEach(e => {
                actions.push({ ...e, actionType: 'email', actionLabel: 'Email', detail: (e.recipients || []).join(', ') });
            });
            ((fractalData.success ? fractalData.data : null) || []).forEach(f => {
                actions.push({ ...f, actionType: 'fractal', actionLabel: 'Fractal', detail: f.description || '' });
            });
            ((dictData.success ? dictData.data : null) || []).forEach(d => {
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
                        <button onclick="Alerts.showNewActionPicker()" class="btn-primary">Create Your First Action</button>
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

    // ---- Action editor drawer (slide-over) ----

    openActionDrawer() {
        const drawer = document.getElementById('actionDrawer');
        const scrim = document.getElementById('actionDrawerScrim');
        if (!drawer || !scrim) return;
        drawer.classList.add('open');
        scrim.classList.add('open');
        document.body.classList.add('drawer-open');
        if (!this._drawerKeyHandler) {
            this._drawerKeyHandler = (e) => {
                if (e.key === 'Escape') this.closeActionDrawer();
            };
            document.addEventListener('keydown', this._drawerKeyHandler);
        }
        // Reset scroll so each form opens at the top.
        const body = document.getElementById('actionDrawerBody');
        if (body) body.scrollTop = 0;
    },

    closeActionDrawer() {
        const drawer = document.getElementById('actionDrawer');
        const scrim = document.getElementById('actionDrawerScrim');
        if (drawer) drawer.classList.remove('open');
        if (scrim) scrim.classList.remove('open');
        document.body.classList.remove('drawer-open');
        if (this._drawerKeyHandler) {
            document.removeEventListener('keydown', this._drawerKeyHandler);
            this._drawerKeyHandler = null;
        }
        this.closeAllInlineForms();
    },

    showNewActionPicker() {
        this.closeAllInlineForms();
        const container = document.getElementById('newActionPickerContainer');
        if (!container) return;
        const tile = (type, name, desc, iconPath) => `
            <button class="action-type-tile" onclick="Alerts.pickNewAction('${type}')">
                <span class="tile-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${iconPath}</svg>
                </span>
                <span class="tile-name">${name}</span>
                <span class="tile-desc">${desc}</span>
            </button>`;
        container.innerHTML = `
            <div class="actions-create-panel">
                <div class="actions-panel-header">
                    <h3>New Action</h3>
                    <button class="btn-icon" onclick="Alerts.closeActionDrawer()" title="Close">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
                    </button>
                </div>
                <div class="action-type-picker">
                    ${tile('webhook', 'Webhook', 'POST matching alerts to an HTTP endpoint.', '<path d="M4 12h16"/><path d="M14 6l6 6-6 6"/>')}
                    ${tile('email', 'Email', 'Send an email to recipients when an alert fires.', '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 7 9 6 9-6"/>')}
                    ${tile('fractal', 'Fractal Action', 'Forward matching logs into another fractal.', '<circle cx="12" cy="12" r="3"/><circle cx="12" cy="12" r="9"/>')}
                    ${tile('dictionary', 'Dictionary Action', 'Write matching logs into a ClickHouse dictionary.', '<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>')}
                </div>
            </div>
        `;
        this.openActionDrawer();
    },

    pickNewAction(type) {
        if (type === 'webhook') this.showInlineWebhookCreate();
        else if (type === 'email') this.showInlineEmailActionCreate();
        else if (type === 'fractal') this.showInlineFractalActionCreate();
        else if (type === 'dictionary') this.showInlineDictActionCreate();
    },

    closeAllInlineForms() {
        this.closeInlineWebhookForm();
        this.closeInlineFractalActionForm();
        this.closeInlineDictActionForm();
        this.closeInlineEmailActionForm();
        const picker = document.getElementById('newActionPickerContainer');
        if (picker) picker.innerHTML = '';
        const smtp = document.getElementById('smtpSettingsFormContainer');
        if (smtp) smtp.innerHTML = '';
    },

    // Email action inline forms
    showInlineEmailActionCreate() {
        this.closeAllInlineForms();
        this.currentEmailAction = null;
        this.renderEmailActionInlineForm();
    },

    async showInlineEmailActionEdit(id) {
        try {
            const resp = await fetch(`/api/v1/email-actions/${id}`, { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentEmailAction = data.data;
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
                    <button class="btn-icon" onclick="Alerts.closeActionDrawer()" title="Close">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                        </svg>
                    </button>
                </div>
                <div id="emailSmtpHint" class="drawer-inline-hint" style="display: none;"></div>
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
                    <button class="btn-secondary" onclick="Alerts.closeActionDrawer()">Cancel</button>
                    ${isEdit ? '<button class="btn-secondary" onclick="Alerts.testUnifiedAction(\'' + ea.id + '\', \'email\')">Test</button>' : ''}
                    <button class="btn-primary" onclick="Alerts.saveEmailAction()">Save Email Action</button>
                </div>
                <div id="emailActionError" class="error-message" style="display: none;"></div>
            </div>
        `;
        this.openActionDrawer();
        container.querySelector('#emailActionName')?.focus();
        this.checkSMTPForEmailForm();
    },

    async checkSMTPForEmailForm() {
        const hint = document.getElementById('emailSmtpHint');
        if (!hint) return;
        try {
            const resp = await fetch('/api/v1/smtp-settings', { credentials: 'include' });
            const data = await resp.json();
            const host = data.data?.host;
            if (!host) {
                hint.innerHTML = `
                    <span>SMTP isn't configured, so emails won't be delivered.</span>
                    <button class="drawer-inline-hint-action" onclick="Alerts.showSMTPSettings()">Configure SMTP</button>`;
                hint.style.display = 'flex';
            } else {
                hint.style.display = 'none';
            }
        } catch (e) {
            // Non-fatal: leave the hint hidden if the check fails.
        }
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
                this.closeActionDrawer();
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
        this.closeAllInlineForms();
        const container = document.getElementById('smtpSettingsFormContainer');
        if (!container) return;

        try {
            const resp = await fetch('/api/v1/smtp-settings', { credentials: 'include' });
            const data = await resp.json();
            const config = data.data || {};

            container.innerHTML = `
                <div class="actions-create-panel">
                    <div class="actions-panel-header">
                        <h3>SMTP Configuration</h3>
                        <button class="btn-icon" onclick="Alerts.closeActionDrawer()" title="Close">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
                        </button>
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
                        <button class="btn-secondary btn-sm" onclick="Alerts.closeActionDrawer()">Cancel</button>
                        <button class="btn-primary btn-sm" onclick="Alerts.saveSMTPSettings()">Save</button>
                    </div>
                </div>
            `;
            this.openActionDrawer();
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
                this.closeActionDrawer();
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
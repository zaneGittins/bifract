/**
 * Dashboards Frontend Module
 * Grid-based dashboards with draggable, resizable query widgets.
 * Opening a dashboard paints the last saved results immediately, then refreshes
 * any widget whose cache has aged past the dashboard's refresh cadence.
 *
 * Viewing and editing are separate: anyone can change the time range and
 * @variable values for their own view (kept in the URL, run through the preview
 * path, never persisted); analysts enter edit mode to change the layout and
 * widgets, and can promote their view to the dashboard defaults.
 */

const Dashboards = {
    currentDashboard: null,
    varManager: null,        // VariableManager for the dashboard's @vars
    currentPage: 0,
    pageSize: 20,
    totalDashboards: 0,
    searchQuery: '',

    editMode: false,
    _op: null,               // active drag/resize
    _undo: [],               // layout snapshots, newest last
    _view: null,             // {range, vars} the viewer picked; null = defaults
    _rangeHistory: [],       // previous view ranges for "back"
    _inflight: new Map(),    // widgetId -> AbortController
    _queueGen: 0,

    presenceInterval: null,
    eventSource: null,
    sseClientId: null,

    // Cache age below which an opened widget is not re-executed when the
    // dashboard has auto-refresh off. Matches the executor's MinInterval floor.
    MIN_CACHE_FRESH_MS: 10000,
    // Widgets executed at once from this client.
    EXEC_CONCURRENCY: 4,

    init() {
        this.currentDashboard = null;
        this.stopDragResize();
        this.bindEvents();
        this.showDashboardListing();
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('Dashboards', () => this.onFractalChange());
        }
        if (!this._kebabCloseHandler) {
            this._kebabCloseHandler = (e) => {
                if (!e.target.closest('.widget-kebab-wrapper')) {
                    document.querySelectorAll('.widget-kebab-menu.open').forEach(m => m.classList.remove('open'));
                }
            };
            document.addEventListener('click', this._kebabCloseHandler);
        }
        if (!this._docKeyHandler) {
            this._docKeyHandler = (e) => this.onDocumentKeyDown(e);
            document.addEventListener('keydown', this._docKeyHandler);
        }
    },

    onFractalChange() {
        this.currentDashboard = null;
        this.resetViewState();
        this.setEditMode(false);
        this.stopDragResize();
        this.stopUpdatedAtTicker();
        this.currentPage = 0;
        this.searchQuery = '';
        const tbody = document.getElementById('dashboardsTableBody');
        if (tbody) tbody.innerHTML = '';

        if (FractalContext.shouldReload('dashboardsView')) this.showDashboardListing();
    },

    bindEvents() {
        this.unbindEvents();

        const on = (id, type, fn) => {
            const el = document.getElementById(id);
            if (!el) return;
            el._dashHandler = fn;
            el._dashEvent = type;
            el.addEventListener(type, fn);
        };

        on('createDashboardBtn', 'click', () => this.showCreateDashboardModal());
        on('dashboardSearchInput', 'input', (e) => {
            this.searchQuery = e.target.value;
            this.currentPage = 0;
            this.loadDashboards();
        });
        on('dashboardsPrevBtn', 'click', () => {
            if (this.currentPage > 0) { this.currentPage--; this.loadDashboards(); }
        });
        on('dashboardsNextBtn', 'click', () => {
            const maxPage = Math.ceil(this.totalDashboards / this.pageSize) - 1;
            if (this.currentPage < maxPage) { this.currentPage++; this.loadDashboards(); }
        });
        on('addWidgetBtn', 'click', () => this.addWidget());
        on('deleteDashboardBtn', 'click', () => this.deleteDashboard());
        on('dashboardShareBtn', 'click', () => this.showShareModal());
        on('dashboardRefreshSelect', 'change', (e) => this.updateRefreshInterval(parseInt(e.target.value, 10)));
        on('dashboardEditBtn', 'click', () => this.setEditMode(true));
        on('dashboardDoneBtn', 'click', () => this.setEditMode(false));
        on('dashboardUndoBtn', 'click', () => this.undoLayout());
        on('dashboardSettingsBtn', 'click', () => this.showSettingsModal());
        on('dashboardTimeBtn', 'click', (e) => { e.stopPropagation(); this.toggleTimePanel(); });
        on('dashboardTimeBackdrop', 'click', () => this.closeTimePanel());
        on('dashboardZoomOutBtn', 'click', () => this.zoomOut());
        on('dashboardTimeBackBtn', 'click', () => this.goBackRange());
        on('dashboardViewResetBtn', 'click', () => this.resetView());
        on('dashboardViewSaveBtn', 'click', () => this.saveViewAsDefault());
    },

    unbindEvents() {
        const ids = [
            'createDashboardBtn', 'dashboardSearchInput', 'dashboardsPrevBtn', 'dashboardsNextBtn',
            'addWidgetBtn', 'deleteDashboardBtn', 'dashboardShareBtn', 'dashboardRefreshSelect',
            'dashboardEditBtn', 'dashboardDoneBtn', 'dashboardUndoBtn', 'dashboardSettingsBtn',
            'dashboardTimeBtn', 'dashboardTimeBackdrop', 'dashboardZoomOutBtn', 'dashboardTimeBackBtn',
            'dashboardViewResetBtn', 'dashboardViewSaveBtn'
        ];
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (el && el._dashHandler) {
                el.removeEventListener(el._dashEvent, el._dashHandler);
                delete el._dashHandler;
                delete el._dashEvent;
            }
        });
    },

    // Analysts edit; viewers only view. The server enforces the same split.
    canEdit() {
        return !!(window.Auth && typeof Auth.hasFractalRole === 'function' && Auth.hasFractalRole('analyst'));
    },

    onDocumentKeyDown(e) {
        if (e.key === 'Escape') {
            if (this._op) { this.endLayoutOp(true); return; }
            this.closeTimePanel();
            return;
        }
        if (!this.editMode || !this.currentDashboard) return;
        const t = e.target;
        const typing = t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName));
        if (!typing && (e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === 'z') {
            e.preventDefault();
            this.undoLayout();
        }
    },

    // =====================
    // Listing
    // =====================

    showDashboardListing() {
        this.stopPresenceTracking();
        this.stopUpdatedAtTicker();
        this.resetViewState();
        this.setEditMode(false);
        this.closeTimePanel();
        const listing = document.getElementById('dashboardListing');
        const editor = document.getElementById('dashboardEditor');
        if (listing) listing.style.display = 'block';
        if (editor) editor.style.display = 'none';
        this.loadDashboards();
    },

    async loadDashboards() {
        const tableContainer = document.querySelector('.dashboards-table-container');
        const emptyEl = document.getElementById('dashboardsEmptyState');
        const paginationEl = document.getElementById('dashboardsPrevBtn')?.parentElement;
        if (tableContainer) tableContainer.style.display = 'none';
        if (emptyEl) emptyEl.style.display = 'none';
        if (paginationEl) paginationEl.style.display = 'none';
        const offset = this.currentPage * this.pageSize;
        const token = window.FractalContext?.scopeToken?.();
        try {
            const response = await fetch(`/api/v1/dashboards?limit=${this.pageSize}&offset=${offset}`, {
                credentials: 'include'
            });
            const data = await response.json();
            if (window.FractalContext?.isScopeStale?.(token)) return;

            if (!data.success) throw new Error(data.error || 'Failed to load dashboards');

            this.totalDashboards = data.page?.total || 0;
            this.renderDashboardTable(data.data || []);
            this.updatePagination();
        } catch (err) {
            if (window.FractalContext?.isScopeStale?.(token)) return;
            console.error('[Dashboards] Failed to load dashboards:', err);
            this.showError('Failed to load dashboards');
        }
    },

    renderDashboardTable(dashboards) {
        const tbody = document.getElementById('dashboardsTableBody');
        if (!tbody) return;
        const tableContainer = tbody.closest('.dashboards-table-container');
        const emptyEl = document.getElementById('dashboardsEmptyState');

        if (dashboards.length === 0) {
            if (tableContainer) tableContainer.style.display = 'none';
            if (emptyEl) emptyEl.style.display = '';
            return;
        }

        if (tableContainer) tableContainer.style.display = '';
        if (emptyEl) emptyEl.style.display = 'none';

        tbody.innerHTML = dashboards.map(d => `
            <tr>
                <td><a href="#" class="dash-link" data-id="${d.id}">${Utils.escapeHtml(d.name)}</a></td>
                <td>${Utils.escapeHtml(d.description || '')}</td>
                <td>${Utils.escapeHtml(this.rangeLabel(d.time_range_type === 'custom' ? { type: 'custom', start: d.time_range_start, end: d.time_range_end } : { type: d.time_range_type }))}</td>
                <td>${this.formatDate(d.created_at)}</td>
                <td>${this.formatDate(d.updated_at)}</td>
                <td class="kebab-cell">
                    <div class="kebab-wrapper">
                        <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">⋮</button>
                        <div class="kebab-menu">
                            <button class="kebab-item" onclick="Dashboards.exportDashboard('${d.id}')">Export</button>
                            <button class="kebab-item danger" onclick="Dashboards.deleteDashboardById('${d.id}')">Delete</button>
                        </div>
                    </div>
                </td>
            </tr>
        `).join('');

        tbody.querySelectorAll('.dash-link').forEach(a => {
            a.addEventListener('click', (e) => {
                e.preventDefault();
                this.openDashboard(a.dataset.id);
            });
        });
    },

    updatePagination() {
        const totalPages = Math.max(1, Math.ceil(this.totalDashboards / this.pageSize));
        const info = document.getElementById('dashboardsPaginationInfo');
        if (info) info.textContent = `Page ${this.currentPage + 1} of ${totalPages}`;

        const prevBtn = document.getElementById('dashboardsPrevBtn');
        const nextBtn = document.getElementById('dashboardsNextBtn');
        if (prevBtn) prevBtn.disabled = this.currentPage === 0;
        if (nextBtn) nextBtn.disabled = this.currentPage >= totalPages - 1;

        const paginationContainer = prevBtn?.parentElement;
        if (paginationContainer) {
            paginationContainer.style.display = totalPages <= 1 ? 'none' : '';
        }
    },

    // =====================
    // Dashboard Editor
    // =====================

    // target is a dashboard id, optionally followed by "?" and view params
    // (the URL hash form), e.g. "<id>?range=last1h&var-host=web1".
    async openDashboard(target) {
        const [id, qs] = String(target || '').split('?');
        if (!id) return;
        try {
            const view = this._pendingView
                || this.viewFromParams(new URLSearchParams(qs || ''))
                || this._readLegacyDrilldown();
            this._pendingView = null;
            if (this.currentDashboard && this.currentDashboard.id !== id) this.setEditMode(false);
            this.resetViewState();
            this._view = view;

            const viewQs = this.viewParams(view).toString();
            window.App?.pushSubPath(viewQs ? `${id}?${viewQs}` : id);
            const response = await fetch(`/api/v1/dashboards/${id}`, { credentials: 'include' });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to load dashboard');

            this.currentDashboard = data.data;

            const listing = document.getElementById('dashboardListing');
            const editor = document.getElementById('dashboardEditor');
            if (listing) listing.style.display = 'none';
            if (editor) editor.style.display = 'block';

            const titleEl = document.getElementById('dashboardTitle');
            if (titleEl) titleEl.textContent = this.currentDashboard.name;

            const canEdit = this.canEdit();
            const editBtn = document.getElementById('dashboardEditBtn');
            if (editBtn) editBtn.style.display = canEdit ? '' : 'none';
            const refreshSelect = document.getElementById('dashboardRefreshSelect');
            if (refreshSelect) {
                refreshSelect.value = String(this.currentDashboard.refresh_interval ?? 0);
                refreshSelect.disabled = !canEdit;
            }

            this.updateShareButtonVisibility();
            this.renderVariablesBar();
            this.renderDashboardGrid();
            this.renderViewState();
            this.paintCachedWidgets();
            this.autoExecuteAllWidgets(true);
            this.startUpdatedAtTicker();
            this.startPresenceTracking();
        } catch (err) {
            console.error('[Dashboards] Failed to open dashboard:', err);
            this.showError('Failed to load dashboard');
        }
    },

    // ---- SSE & Presence ----

    connectSSE() {
        if (!this.currentDashboard) return;
        // Already connected to THIS dashboard: nothing to do. Connected to a
        // different one (a board-to-board drilldown never returns to the listing,
        // so the old stream would otherwise linger): tear it down and resubscribe,
        // so presence and the room we receive broadcasts on track the dashboard
        // actually on screen.
        if (this.eventSource) {
            if (this._sseDashboardId === this.currentDashboard.id) return;
            this.disconnectSSE();
        }
        this._sseDashboardId = this.currentDashboard.id;

        // Immediate presence update and fetch
        fetch(`/api/v1/dashboards/${this.currentDashboard.id}/presence`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({})
        }).catch(() => {});
        this.onPresenceChanged();

        this.eventSource = new EventSource(
            `/api/v1/dashboards/${this.currentDashboard.id}/events`,
            { withCredentials: true }
        );

        this.eventSource.onmessage = (e) => {
            try {
                const event = JSON.parse(e.data);
                this.handleSSEEvent(event);
            } catch (err) {}
        };

        this.eventSource.onerror = () => {};

        // Lightweight DB heartbeat (must be shorter than the 30s DB expiry window)
        this.presenceInterval = setInterval(() => {
            if (this.currentDashboard) {
                fetch(`/api/v1/dashboards/${this.currentDashboard.id}/presence`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({})
                }).catch(() => {});
            }
        }, 15000);
    },

    disconnectSSE() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
            this.sseClientId = null;
            this._sseDashboardId = null;
        }
        if (this.presenceInterval) {
            clearInterval(this.presenceInterval);
            this.presenceInterval = null;
        }
        this.stopViewRefresh();
        const el = document.getElementById('dashboardPresence');
        if (el) el.innerHTML = '';
    },

    startPresenceTracking() { this.connectSSE(); },
    stopPresenceTracking() { this.disconnectSSE(); },

    sseHeaders() {
        const headers = { 'Content-Type': 'application/json' };
        if (this.sseClientId) {
            headers['X-SSE-Client-ID'] = this.sseClientId;
        }
        return headers;
    },

    handleSSEEvent(event) {
        switch (event.type) {
            case 'connected':
                this.sseClientId = event.data.client_id;
                break;
            case 'widget_added':
                this.onRemoteWidgetAdded(event.data);
                break;
            case 'widget_removed':
                this.onRemoteWidgetRemoved(event.data);
                break;
            case 'widget_updated':
                this.onRemoteWidgetUpdated(event.data);
                break;
            case 'widget_results_updated':
                this.onRemoteWidgetResultsUpdated(event.data);
                break;
            case 'dashboard_layout_updated':
                this.onRemoteLayoutUpdated(event.data);
                break;
            case 'presence_joined':
            case 'presence_left':
                this.onPresenceChanged();
                break;
        }
    },

    onRemoteWidgetAdded(widget) {
        if (!this.currentDashboard) return;
        if (!this.currentDashboard.widgets) this.currentDashboard.widgets = [];
        if (this.currentDashboard.widgets.find(w => w.id === widget.id)) return;

        this.currentDashboard.widgets.push(widget);

        const grid = document.getElementById('dashboardGrid');
        if (grid) {
            const el = this.createWidgetElement(widget);
            el.classList.add('remote-added');
            setTimeout(() => el.classList.remove('remote-added'), 1500);
            grid.appendChild(el);
            this.afterLayoutChange();
            this.executeWidget(widget.id);
        }
        this.syncDashboardVariables(false);
    },

    onRemoteWidgetRemoved(data) {
        if (!this.currentDashboard) return;
        const widgetId = data.id;
        this.currentDashboard.widgets = this.currentDashboard.widgets.filter(w => w.id !== widgetId);
        const el = this.widgetEl(widgetId);
        if (el) { this.destroyWidgetVisuals(el); el.remove(); }
        this.afterLayoutChange();
        this.syncDashboardVariables(false);
    },

    onRemoteWidgetUpdated(data) {
        if (!this.currentDashboard) return;
        const widget = this.currentDashboard.widgets.find(w => w.id === data.id);
        if (!widget) return;

        // Skip if user is editing this widget
        const contentEl = document.getElementById(`wc-${data.id}`);
        if (contentEl && contentEl._editingWidget) return;

        // Use != null so a null/omitted field never clobbers existing state
        // (partial updates only carry the fields that changed).
        if (data.title != null) widget.title = data.title;
        if (data.query_content != null) widget.query_content = data.query_content;
        if (data.chart_type != null) widget.chart_type = data.chart_type;
        if (data.chart_config != null) widget.chart_config = data.chart_config;

        // Update title in header
        const widgetEl = document.querySelector(`.dashboard-widget[data-widget-id="${data.id}"]`);
        if (widgetEl) {
            const titleSpan = widgetEl.querySelector('.widget-title');
            if (titleSpan) titleSpan.textContent = widget.title || 'Widget';
        }
        // A remote query change may shift the @variable set; refresh the tray
        // without re-persisting (the remote editor already saved it).
        if (data.query_content != null) this.syncDashboardVariables(false);
    },

    onRemoteWidgetResultsUpdated(data) {
        if (!this.currentDashboard) return;
        const widget = this.currentDashboard.widgets.find(w => w.id === data.id);
        if (!widget) return;

        if (data.last_results) widget.last_results = data.last_results;
        if (data.chart_type) widget.chart_type = data.chart_type;
        if (data.last_executed_at) widget.last_executed_at = data.last_executed_at;

        // A viewer with their own time range or variables keeps seeing their
        // view; the shared cache is kept current for when they reset it.
        if (this.activeView()) return;

        // Skip if user is editing this widget
        const contentEl = document.getElementById(`wc-${data.id}`);
        if (contentEl && contentEl._editingWidget) return;

        try {
            const resultData = JSON.parse(widget.last_results);
            this.renderWidgetResults(data.id, resultData);
            this.renderUpdatedAt();
        } catch (_) {}
    },

    onRemoteLayoutUpdated(data) {
        if (!this.currentDashboard || !data || !Array.isArray(data.widgets)) return;
        // A local drag owns the layout until it is dropped; the drop then saves
        // the merged result for everyone.
        if (this._op) return;
        data.widgets.forEach(it => {
            const widget = this.getWidget(it.id);
            if (!widget) return;
            widget.pos_x = it.pos_x;
            widget.pos_y = it.pos_y;
            widget.width = it.width;
            widget.height = it.height;
            const el = this.widgetEl(it.id);
            if (el) DashboardLayout.place(el, DashboardLayout.fromWidget(widget));
        });
        this.afterLayoutChange();
    },

    async onPresenceChanged() {
        if (!this.currentDashboard) return;
        try {
            const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/presence`, {
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success && data.data) {
                this.renderPresence(data.data);
            }
        } catch (_) {}
    },

    renderPresence(users) {
        const el = document.getElementById('dashboardPresence');
        if (!el) return;
        const escHtml = (s) => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        // Filter out self and deduplicate by username
        const currentUsername = window.Auth && Auth.currentUser ? Auth.currentUser.username : null;
        const seen = new Set();
        const unique = users.filter(u => {
            if (u.username === currentUsername) return false;
            if (seen.has(u.username)) return false;
            seen.add(u.username);
            return true;
        });
        el.innerHTML = unique.map(u => `
            <div class="presence-user" style="background-color: ${u.user_gravatar_color || '#9c6ade'}"
                 title="${escHtml(u.user_display_name || u.username)}">
                ${escHtml(u.user_gravatar_initial || u.username.charAt(0).toUpperCase())}
            </div>
        `).join('');
    },

    renderDashboardGrid() {
        const grid = document.getElementById('dashboardGrid');
        if (!grid) return;

        this.destroyWidgetVisuals(grid);
        grid.innerHTML = '';
        if (!this.currentDashboard) return;

        (this.currentDashboard.widgets || []).forEach(widget => {
            grid.appendChild(this.createWidgetElement(widget));
        });
        this.afterLayoutChange();
        this.initGridInteractions();
    },

    widgetEl(widgetId) {
        return document.querySelector(`#dashboardGrid .dashboard-widget[data-widget-id="${CSS.escape(widgetId)}"]`);
    },

    createWidgetElement(widget) {
        const el = document.createElement('div');
        el.className = 'dashboard-widget';
        el.dataset.widgetId = widget.id;
        DashboardLayout.place(el, DashboardLayout.fromWidget(widget));

        const title = widget.title || 'Widget';

        el.innerHTML = `
            <div class="widget-header" data-widget-id="${widget.id}">
                <span class="widget-title">${Utils.escapeHtml(title)}</span>
                <div class="widget-actions">
                    <button class="widget-btn widget-execute-btn" title="Re-run" onclick="Dashboards.executeWidget('${widget.id}')">&#9654;</button>
                    <div class="widget-kebab-wrapper dash-edit-only">
                        <button class="widget-btn widget-kebab-btn" title="More options" onclick="Dashboards.toggleWidgetKebab('${widget.id}', event)">&#x22EE;</button>
                        <div class="widget-kebab-menu" id="widget-kebab-menu-${widget.id}">
                            <button onclick="Dashboards.showInlineWidgetEdit('${widget.id}')">Edit query</button>
                            <button onclick="Dashboards.openFormatPanel('${widget.id}')">Formatting</button>
                            <button onclick="Dashboards.openPivotConfig('${widget.id}')">Pivots</button>
                            <div class="kebab-divider"></div>
                            <button class="kebab-danger" onclick="Dashboards.deleteWidget('${widget.id}')">Delete</button>
                        </div>
                    </div>
                </div>
            </div>
            <div class="widget-content" id="wc-${widget.id}">
                <div class="widget-loading">Loading...</div>
            </div>
            <div class="widget-resize" data-dir="e" aria-hidden="true"></div>
            <div class="widget-resize" data-dir="s" aria-hidden="true"></div>
            <div class="widget-resize" data-dir="se" aria-hidden="true"></div>
        `;
        this.decorateHeader(el.querySelector('.widget-header'));
        return el;
    },

    // In edit mode a header is the drag handle and a keyboard target.
    decorateHeader(header) {
        if (!header) return;
        if (this.editMode) {
            header.tabIndex = 0;
            header.title = 'Drag to move, double-click to edit. Arrow keys move, Shift+arrows resize.';
            header.setAttribute('aria-roledescription', 'movable widget');
        } else {
            header.removeAttribute('tabindex');
            header.removeAttribute('title');
            header.removeAttribute('aria-roledescription');
        }
    },

    toggleWidgetKebab(widgetId, event) {
        event.stopPropagation();
        const menu = document.getElementById(`widget-kebab-menu-${widgetId}`);
        if (!menu) return;
        const isOpen = menu.classList.contains('open');
        document.querySelectorAll('.widget-kebab-menu.open').forEach(m => m.classList.remove('open'));
        if (!isOpen) menu.classList.add('open');
    },

    // Re-derive everything that depends on the set of placed widgets.
    afterLayoutChange() {
        const grid = document.getElementById('dashboardGrid');
        if (!grid || !this.currentDashboard) return;
        const items = this.layoutItems();
        DashboardLayout.applyOrder(items, id => this.widgetEl(id));
        // Edit mode leaves room below the last widget to drop into.
        grid.style.minHeight = this.editMode
            ? `${(DashboardLayout.bottom(items) + 12) * DashboardLayout.PITCH}px`
            : '';
        this.renderEmptyState(grid, items.length === 0);
    },

    renderEmptyState(grid, empty) {
        let el = grid.querySelector(':scope > .dashboard-empty');
        if (!empty) { if (el) el.remove(); return; }
        if (!el) {
            el = document.createElement('div');
            el.className = 'dashboard-empty';
            grid.appendChild(el);
        }
        const canEdit = this.canEdit();
        el.innerHTML = `
            <div class="dashboard-empty-title">This dashboard has no widgets yet</div>
            <div class="dashboard-empty-hint">${canEdit ? 'Each widget is a BQL query rendered as a table or chart.' : 'An analyst can add widgets to it.'}</div>
            ${canEdit ? '<button class="btn-primary" type="button" onclick="Dashboards.setEditMode(true); Dashboards.addWidget();">+ Add widget</button>' : ''}
        `;
    },

    layoutItems() {
        return DashboardLayout.fromWidgets(this.currentDashboard && this.currentDashboard.widgets);
    },

    // =====================
    // Auto-execute on open
    // =====================

    // Runs widgets top-down a few at a time, so a large board neither floods
    // ClickHouse nor makes the first screen wait on widgets below the fold.
    autoExecuteAllWidgets(skipFresh = false) {
        if (!this.currentDashboard || !this.currentDashboard.widgets) return;
        const ids = DashboardLayout.sorted(this.layoutItems())
            .map(it => this.getWidget(it.id))
            .filter(w => w && !(skipFresh && this.isCacheFresh(w)))
            .map(w => w.id);
        const gen = ++this._queueGen;
        let next = 0;
        const run = () => {
            if (gen !== this._queueGen || next >= ids.length) return;
            this.executeWidget(ids[next++]).finally(run);
        };
        for (let i = 0; i < this.EXEC_CONCURRENCY; i++) run();
    },

    // Age of the results on screen, shown once for the whole dashboard: the
    // executor refreshes every widget in a single pass, so the newest stamp
    // represents the board. A widget that diverges (its own refresh failed)
    // reports that on the widget itself. Blank during a drilldown, whose
    // transient private results are not the shared cache.
    renderUpdatedAt() {
        const el = document.getElementById('dashboardUpdatedAt');
        if (!el) return;
        const clear = () => { el.textContent = ''; el.removeAttribute('title'); };
        if (!this.currentDashboard || !this.currentDashboard.widgets || this.activeView()) return clear();

        let newest = 0;
        this.currentDashboard.widgets.forEach(w => {
            if (!w.last_executed_at) return;
            const t = new Date(w.last_executed_at).getTime();
            if (Number.isFinite(t) && t > newest) newest = t;
        });
        if (!newest) return clear();

        const age = Utils.timeAgo(newest);
        if (!age) return clear();
        el.textContent = `Updated ${age}`;
        el.title = TZ.format(newest, 'friendly');
    },

    startUpdatedAtTicker() {
        this.stopUpdatedAtTicker();
        this.renderUpdatedAt();
        this._updatedAtTimer = setInterval(() => {
            if (!this.currentDashboard) return this.stopUpdatedAtTicker();
            this.renderUpdatedAt();
        }, 15000);
    },

    stopUpdatedAtTicker() {
        if (this._updatedAtTimer) {
            clearInterval(this._updatedAtTimer);
            this._updatedAtTimer = null;
        }
        const el = document.getElementById('dashboardUpdatedAt');
        if (el) { el.textContent = ''; el.removeAttribute('title'); }
    },

    // Re-running a widget on open is wasted ClickHouse work when its saved
    // results are still inside the dashboard's own refresh cadence: the
    // background executor keeps viewed dashboards warm and pushes newer results
    // over SSE. With auto-refresh off, a short floor still absorbs reopens.
    isCacheFresh(widget) {
        if (!widget || !widget.last_results || !widget.last_executed_at) return false;
        if (this.activeView()) return false;
        const age = Date.now() - new Date(widget.last_executed_at).getTime();
        if (!Number.isFinite(age) || age < 0) return false;
        const interval = this.currentDashboard?.refresh_interval || 0;
        return age < (interval > 0 ? interval * 1000 : this.MIN_CACHE_FRESH_MS);
    },

    // Paint the server-persisted results of every widget so an opened dashboard
    // shows data instantly while the live refresh runs underneath. Skipped in a
    // drilldown: the cache holds unfiltered results that would misrepresent it.
    paintCachedWidgets() {
        if (!this.currentDashboard || !this.currentDashboard.widgets) return;
        if (this.activeView()) return;
        this.currentDashboard.widgets.forEach(w => this.renderWidgetFromCache(w.id));
    },

    async executeWidget(widgetId) {
        const widget = this.getWidget(widgetId);
        if (!widget || !this.currentDashboard) return;

        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (contentEl && contentEl._editingWidget) return;

        if (!(widget.query_content || '').trim()) {
            if (contentEl) {
                delete contentEl.dataset.rendered;
                contentEl.innerHTML = `<div class="widget-loading">${this.editMode ? 'No query yet. Use Edit query from the widget menu.' : 'No query yet'}</div>`;
            }
            return;
        }

        // A newer run of this widget supersedes an older one, so a slow result
        // for a stale range or variable never lands over a fresh one.
        const prev = this._inflight.get(widgetId);
        if (prev) prev.abort();
        const ctrl = new AbortController();
        this._inflight.set(widgetId, ctrl);

        // Results already on screen stay visible and are refreshed in place;
        // only an empty widget shows a loading state.
        const widgetEl = this.widgetEl(widgetId);
        const hasRendered = !!(contentEl && contentEl.dataset.rendered === '1');
        if (contentEl && !hasRendered) {
            this.destroyWidgetVisuals(contentEl);
            contentEl.innerHTML = '<div class="widget-loading">Executing...</div>';
        }
        if (widgetEl && hasRendered) widgetEl.classList.add('refreshing');

        const execBtn = widgetEl && widgetEl.querySelector('.widget-execute-btn');
        if (execBtn) { execBtn.innerHTML = '<span class="spinner"></span>'; execBtn.disabled = true; }

        // With no view the run is the shared one: the server persists it as the
        // dashboard cache and pushes it to other viewers. A viewer's own range or
        // variables run as a private preview that is neither stored nor broadcast.
        const view = this.activeView();
        let body;
        if (view) {
            const req = {
                preview: true,
                variables: Object.entries(view.vars).map(([name, value]) => ({ name, value })),
            };
            if (view.range) {
                const r = this.resolveRange(view.range);
                req.time_range_start = r.start;
                req.time_range_end = r.end;
            }
            body = JSON.stringify(req);
        }

        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}/execute`, {
                method: 'POST',
                headers: this.sseHeaders(),
                credentials: 'include',
                body,
                signal: ctrl.signal
            });
            const data = await response.json();
            if (this._inflight.get(widgetId) !== ctrl) return;
            if (!data.success) throw new Error(data.error || 'Query failed');

            const resultData = data.data || {};
            resultData.results = resultData.results || [];
            resultData.chart_type = resultData.chart_type || data.chart_type || 'table';
            resultData.chart_config = resultData.chart_config || {};
            resultData.field_order = resultData.field_order || [];

            if (view) {
                widget._viewResults = resultData;
            } else {
                widget._viewResults = null;
                widget.last_results = JSON.stringify(resultData);
                widget.last_executed_at = new Date().toISOString();
                if (resultData.chart_type) widget.chart_type = resultData.chart_type;
            }

            this.renderWidgetResults(widgetId, resultData);
            this.renderUpdatedAt();
        } catch (err) {
            if (err.name === 'AbortError' || this._inflight.get(widgetId) !== ctrl) return;
            console.error('[Dashboards] Widget execution failed:', err);
            if (contentEl && !contentEl._editingWidget) {
                if (contentEl.dataset.rendered === '1') {
                    // Keep the stale results visible, but say so: silently showing
                    // old data as if it were fresh is worse than a blank widget.
                    this.showStaleNote(contentEl, err.message);
                } else {
                    delete contentEl.dataset.rendered;
                    this.destroyWidgetVisuals(contentEl);
                    contentEl.innerHTML = `<div class="widget-error">Error: ${Utils.escapeHtml(err.message)}</div>`;
                }
            }
        } finally {
            if (this._inflight.get(widgetId) === ctrl) this._inflight.delete(widgetId);
            if (!this._inflight.has(widgetId)) {
                if (widgetEl) widgetEl.classList.remove('refreshing');
                if (execBtn) { execBtn.innerHTML = '&#9654;'; execBtn.disabled = false; }
            }
        }
    },

    abortInflight() {
        this._queueGen++;
        this._inflight.forEach(c => c.abort());
        this._inflight.clear();
    },

    // Chart hosts are inserted as HTML strings; draw once they are laid out.
    afterPaint(fn) {
        requestAnimationFrame(() => fn());
    },

    showStaleNote(contentEl, message) {
        const existing = contentEl.querySelector(':scope > .widget-stale-note');
        if (existing) existing.remove();
        const note = document.createElement('div');
        note.className = 'widget-stale-note';
        note.textContent = `Showing last saved results - refresh failed: ${message}`;
        contentEl.insertBefore(note, contentEl.firstChild);
    },

    renderWidgetResults(widgetId, resultData) {
        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (!contentEl || contentEl._editingWidget) return;

        const chartType = resultData.chart_type || 'table';
        const results = resultData.results || [];

        // Get widget chart_config for row coloring rules
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
        const widgetConfig = widget ? this.parseChartConfig(widget.chart_config) : {};

        this.destroyWidgetVisuals(contentEl);

        if (chartType !== 'table' && results.length > 0) {
            const chartHtml = this.renderQueryChart(resultData, widgetConfig, widgetId);
            contentEl.innerHTML = chartHtml || this.renderResultsTable(results, resultData, widgetConfig, widgetId);
        } else {
            contentEl.innerHTML = this.renderResultsTable(results, resultData, widgetConfig, widgetId);
        }

        // Marks the widget as holding real results: a later refresh then updates
        // it in place instead of wiping it back to a loading state.
        contentEl.dataset.rendered = '1';
    },

    // Chart.js, vis-network and Leaflet each keep their instances alive in an
    // internal registry or a window listener, so dropping the DOM alone leaks
    // them: a dashboard left open refreshing accumulates one per widget per
    // refresh. Tear them down before any content wipe. sharedRender does the
    // same for the wallboard.
    destroyWidgetVisuals(root) {
        if (!root || typeof root.querySelectorAll !== 'function') return;

        if (window.Chart && Chart.getChart) {
            root.querySelectorAll('canvas').forEach(c => {
                const inst = Chart.getChart(c);
                if (inst) inst.destroy();
            });
        }

        root.querySelectorAll('.widget-visual-host').forEach(el => {
            if (el._visNetwork && typeof el._visNetwork.destroy === 'function') el._visNetwork.destroy();
            if (el._leafletMap && typeof el._leafletMap.remove === 'function') el._leafletMap.remove();
            el._visNetwork = null;
            el._leafletMap = null;
        });
    },

    parseChartConfig(config) {
        if (!config) return {};
        if (typeof config === 'string') {
            try { return JSON.parse(config); } catch { return {}; }
        }
        return config;
    },

    renderQueryChart(results, widgetConfig, widgetId) {
        const chartType = results.chart_type || 'table';
        const chartId = `dchart-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;

        if (chartType === 'table' || !results.results || results.results.length === 0) {
            return '';
        }

        if (chartType === 'singleval') {
            return this.renderSingleValWidget(results, widgetConfig);
        }

        if (chartType === 'graph') {
            const graphId = `dgraph-${chartId}`;
            const graphHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                    <div id="${graphId}" class="widget-visual-host" style="width:100%;height:100%;"></div>
                </div>
            `;
            this.afterPaint(() => {
                const el = document.getElementById(graphId);
                if (el) el._visNetwork = BifractCharts.renderGraphSimple(el, {
                    data: results.results || [],
                    fields: results.field_order,
                    config: results.chart_config || {}
                });
            });
            return graphHtml;
        }

        if (chartType === 'mesh') {
            const meshId = `dmesh-${chartId}`;
            const meshHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                    <div id="${meshId}" class="widget-visual-host" style="width:100%;height:100%;"></div>
                </div>
            `;
            this.afterPaint(() => {
                const el = document.getElementById(meshId);
                if (el) el._visNetwork = BifractCharts.renderMeshSimple(el, {
                    data: results.results || [],
                    fields: results.field_order,
                    config: results.chart_config || {},
                    onDataClick: (ctx, ev) => this.onWidgetDataClick(widgetId, ctx, ev)
                });
            });
            return meshHtml;
        }

        if (chartType === 'heatmap') {
            const heatmapId = `dheatmap-${chartId}`;
            const heatmapHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;overflow:auto;">
                    <div id="${heatmapId}" style="width:100%;overflow:auto;"></div>
                </div>
            `;
            this.afterPaint(() => {
                const el = document.getElementById(heatmapId);
                if (el) BifractCharts.renderHeatmap(el, {
                    data: results.results || [],
                    config: results.chart_config || {}
                });
            });
            return heatmapHtml;
        }

        if (chartType === 'mitre') {
            const mitreId = `dmitre-${chartId}`;
            const mitreHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;overflow:auto;">
                    <div id="${mitreId}" class="mtr-host widget-visual-host" style="height:100%;"></div>
                </div>
            `;
            this.afterPaint(() => {
                const el = document.getElementById(mitreId);
                // embedded: a wallboard panel opens on what fired, not on 700 empty cells.
                if (el && window.BifractMitreMatrix) BifractMitreMatrix.render(el, {
                    rows: results.results || [],
                    config: results.chart_config || {},
                    embedded: true
                });
            });
            return mitreHtml;
        }

        if (chartType === 'worldmap') {
            const mapId = `dmap-${chartId}`;
            const mapHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                    <div id="${mapId}" class="worldmap-container widget-visual-host" style="height:100%;"></div>
                </div>
            `;
            this.afterPaint(() => {
                const el = document.getElementById(mapId);
                if (el && window.BifractWorldMap) {
                    const cfg = results.chart_config || {};
                    el._leafletMap = BifractWorldMap.render(el, results.results || [], {
                        latField: cfg.latField || 'latitude',
                        lonField: cfg.lonField || 'longitude',
                        labelField: cfg.labelField || null
                    });
                }
            });
            return mapHtml;
        }

        const chartHtml = `
            <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                <canvas id="${chartId}" style="background:transparent;border-radius:4px;"></canvas>
            </div>
        `;

        this.afterPaint(() => {
            this.renderChartOnCanvas(chartId, results, widgetConfig, widgetId);
        });

        return chartHtml;
    },

    // Query-time config (limit/span/field) lives on results.chart_config; user
    // formatting (colors/unit/legend/stat) lives on the widget's chart_config.
    // Merge so charts receive both; formatting keys win on any overlap.
    mergeChartConfig(results, widgetConfig) {
        return Object.assign({}, results.chart_config || {}, widgetConfig || {});
    },

    renderChartOnCanvas(chartId, results, widgetConfig, widgetId) {
        const canvas = document.getElementById(chartId);
        if (!canvas) return;

        const opts = {
            data: results.results,
            fields: results.field_order,
            config: this.mergeChartConfig(results, widgetConfig),
            maintainAspectRatio: false,
            height: '100%'
        };
        // Time brushing: drag-select a span on a timechart to zoom the whole
        // dashboard to that custom range.
        if (results.chart_type === 'timechart') {
            opts.onBrush = (startISO, endISO) => this.applyBrushTimeRange(startISO, endISO);
        }
        // Pivot drilldown: click a segment/point to pass its row to another
        // dashboard or the search page.
        if (this.widgetHasPivots(widgetId)) {
            opts.onDataClick = (ctx, ev) => this.onWidgetDataClick(widgetId, ctx, ev);
        }

        try {
            BifractCharts.renderOnCanvas(canvas, results.chart_type, opts);
        } catch (err) {
            console.error('[Dashboards] Chart render error:', err);
        }
    },

    // A brushed span on a timechart zooms this viewer's range; "back" undoes it.
    applyBrushTimeRange(startISO, endISO) {
        if (!this.currentDashboard) return;
        this.setViewRange({ type: 'custom', start: startISO, end: endISO });
    },

    renderSingleValWidget(results, widgetConfig) {
        return BifractCharts.renderSingleVal(null, {
            data: results.results,
            fields: results.field_order,
            config: this.mergeChartConfig(results, widgetConfig),
            coloringRules: (widgetConfig && widgetConfig.row_coloring_rules) || [],
            returnHtml: true
        });
    },

    formatSingleValue(num) {
        return BifractCharts.formatSingleValue(num);
    },

    renderResultsTable(results, resultMetadata, widgetConfig, widgetId) {
        if (!results || results.length === 0) {
            return '<div style="padding:20px;text-align:center;color:var(--text-muted);">No results</div>';
        }

        const tableColumns = resultMetadata?.table_columns || resultMetadata?.columns || resultMetadata?.field_order;
        const systemFields = ['_all_fields', 'raw_log', 'log_id', ...Utils.HIDDEN_ROW_FIELDS];
        const headers = (tableColumns && tableColumns.length > 0)
            ? tableColumns
            : Object.keys(results[0]).filter(h => !systemFields.includes(h));

        const rules = (widgetConfig && widgetConfig.row_coloring_rules) || [];
        const pivotable = this.widgetHasPivots(widgetId);

        // Shared core renderer: smart sizing + resize/autofit (global delegation),
        // with dashboard row/cell coloring via hooks. 'dash:' persistence
        // namespace keeps widths independent from search/notebook tables.
        const fractalId = (window.FractalContext && FractalContext.currentFractal && FractalContext.currentFractal.id) || 'default';
        const built = QueryExecutor.buildResultsTable(headers, results, {
            sizingKey: { fractalId: 'dash:' + fractalId, sig: ColumnSizing.signature(headers) },
            features: { resize: true, reorder: false, sort: false },
            maxRows: 100,
            rowClass: pivotable ? () => 'pivotable' : undefined,
            rowStyle: (row) => this.getRowHighlightStyle(row, rules),
            cellStyle: (field, row) => this.getCellHighlightStyle(row, field, rules),
            // Right-click a cell opens a context menu (copy value/row + pivots),
            // cursor-anchored so it is never off-screen for wide/fit-to-data tables.
            // Left-click stays native, so selecting and copying cell text still works.
            onCellContextMenu: pivotable ? (row, field, value, e) => {
                const widget = this.getWidget(widgetId);
                if (widget && window.Pivots) Pivots.showContextMenu(widget, { row, field, value, series: null }, e);
            } : undefined,
            truncatedNote: '<div style="padding:8px;text-align:center;color:var(--text-muted);font-size:0.75rem;">Showing first 100 rows</div>',
        });
        // onRowClick is wired in built.mount(); dashboards otherwise render from the
        // html string alone, so mount only when a pivot needs the listener.
        if (pivotable) {
            requestAnimationFrame(() => {
                const el = document.getElementById(`wc-${widgetId}`);
                if (el) built.mount(el);
            });
        }
        // No extra scroll wrapper: .widget-content already scrolls (and is the
        // height-constrained sticky-header ancestor); wrapping here would force
        // overflow-y:auto and break the sticky thead.
        return built.html;
    },

    evaluateRule(cellVal, rule) {
        if (cellVal === undefined || cellVal === null) return false;
        const op = rule.operator || '=';
        const ruleVal = rule.value;
        if (op === 'contains') {
            return String(cellVal).toLowerCase().includes(String(ruleVal).toLowerCase());
        }
        if (op === '>' || op === '>=' || op === '<' || op === '<=') {
            const numCell = parseFloat(cellVal);
            const numRule = parseFloat(ruleVal);
            if (isNaN(numCell) || isNaN(numRule)) return false;
            if (op === '>') return numCell > numRule;
            if (op === '>=') return numCell >= numRule;
            if (op === '<') return numCell < numRule;
            return numCell <= numRule;
        }
        // Default: exact match
        return String(cellVal) === String(ruleVal);
    },

    getRowHighlightStyle(row, rules) {
        if (!rules || rules.length === 0) return '';
        for (const rule of rules) {
            if (!rule.column) continue;
            if ((rule.target || 'row') !== 'row') continue;
            const cellVal = row[rule.column];
            if (this.evaluateRule(cellVal, rule)) {
                const color = rule.color || '#8b5cf6';
                return `background-color: ${color}26;`;
            }
        }
        return '';
    },

    getCellHighlightStyle(row, column, rules) {
        if (!rules || rules.length === 0) return '';
        for (const rule of rules) {
            if (!rule.column || rule.column !== column) continue;
            if ((rule.target || 'row') !== 'cell') continue;
            const cellVal = row[rule.column];
            if (this.evaluateRule(cellVal, rule)) {
                const color = rule.color || '#8b5cf6';
                return `background-color: ${color}26;`;
            }
        }
        return '';
    },

    // =====================
    // Edit mode, drag and resize
    // =====================

    setEditMode(on) {
        const next = !!on && this.canEdit() && !!this.currentDashboard;
        if (this._op) this.endLayoutOp(true);
        this.editMode = next;
        if (!next) this._undo = [];
        const editor = document.getElementById('dashboardEditor');
        if (editor) editor.classList.toggle('editing', next);
        const grid = document.getElementById('dashboardGrid');
        if (grid) {
            grid.classList.toggle('editing', next);
            grid.querySelectorAll('.widget-header').forEach(h => this.decorateHeader(h));
        }
        document.querySelectorAll('.widget-kebab-menu.open').forEach(m => m.classList.remove('open'));
        this.updateUndoButton();
        this.afterLayoutChange();
    },

    initGridInteractions() {
        this.stopDragResize();
        const grid = document.getElementById('dashboardGrid');
        if (!grid) return;
        this._gridHandlers = {
            pointerdown: (e) => this.onGridPointerDown(e),
            keydown: (e) => this.onGridKeyDown(e),
            dblclick: (e) => {
                if (!this.editMode) return;
                const header = e.target.closest('.widget-header');
                if (!header || e.target.closest('button')) return;
                this.showInlineWidgetEdit(header.dataset.widgetId);
            },
        };
        this._gridEl = grid;
        Object.entries(this._gridHandlers).forEach(([type, fn]) => grid.addEventListener(type, fn));
    },

    stopDragResize() {
        if (this._op) this.endLayoutOp(true);
        if (this._gridEl && this._gridHandlers) {
            Object.entries(this._gridHandlers).forEach(([type, fn]) => this._gridEl.removeEventListener(type, fn));
        }
        this._gridEl = null;
        this._gridHandlers = null;
    },

    onGridPointerDown(e) {
        if (!this.editMode || this._op || (e.pointerType === 'mouse' && e.button !== 0)) return;
        const grid = e.currentTarget;
        const handle = e.target.closest('.widget-resize');
        const header = e.target.closest('.widget-header');
        if (!handle && (!header || e.target.closest('button, input, textarea, select, .widget-kebab-menu'))) return;
        const el = e.target.closest('.dashboard-widget');
        if (!el) return;
        const m = DashboardLayout.metrics(grid);
        if (m.stacked) return;

        const start = this.layoutItems();
        const item = start.find(i => i.id === el.dataset.widgetId);
        if (!item) return;
        e.preventDefault();

        const gridRect = grid.getBoundingClientRect();
        const rect = el.getBoundingClientRect();
        this._op = {
            kind: handle ? 'resize' : 'drag',
            dir: handle ? handle.dataset.dir : '',
            id: item.id, el, grid, m, start, layout: start, item,
            downX: e.clientX, downY: e.clientY, x: e.clientX, y: e.clientY,
            offX: e.clientX - rect.left, offY: e.clientY - rect.top,
            box: { left: rect.left - gridRect.left, top: rect.top - gridRect.top, width: rect.width, height: rect.height },
            lifted: false,
        };
        this._opHandlers = {
            pointermove: (ev) => this.onLayoutPointerMove(ev),
            pointerup: () => this.endLayoutOp(false),
            pointercancel: () => this.endLayoutOp(true),
        };
        Object.entries(this._opHandlers).forEach(([type, fn]) => document.addEventListener(type, fn));
        if (handle) this.liftLayoutOp();
    },

    onLayoutPointerMove(e) {
        const op = this._op;
        if (!op) return;
        op.x = e.clientX;
        op.y = e.clientY;
        // A click or double-click on a header must not turn into a drag.
        if (!op.lifted) {
            if (Math.hypot(op.x - op.downX, op.y - op.downY) < 4) return;
            this.liftLayoutOp();
        }
        this.updateLayoutOp();
    },

    // Take the widget out of the grid flow so it follows the pointer, and show
    // a placeholder where it will land.
    liftLayoutOp() {
        const op = this._op;
        op.lifted = true;
        op.grid.classList.add('layout-active');
        op.grid.style.setProperty('--col-pitch', `${op.m.colPitch}px`);
        document.body.classList.add(op.kind === 'drag' ? 'dash-dragging' : `dash-resizing-${op.dir}`);

        const ph = document.createElement('div');
        ph.className = 'widget-placeholder';
        DashboardLayout.place(ph, op.item);
        op.grid.appendChild(ph);
        op.ph = ph;
        // Live size readout, on the widget being resized so it stays in view.
        if (op.kind === 'resize') {
            op.badge = document.createElement('span');
            op.badge.className = 'widget-size-badge';
            op.el.appendChild(op.badge);
        }

        op.el.classList.add('lifted');
        Object.assign(op.el.style, {
            left: `${op.box.left}px`, top: `${op.box.top}px`,
            width: `${op.box.width}px`, height: `${op.box.height}px`,
        });
        this.updateSizeBadge(op.item);
        this.startAutoScroll(op);
    },

    updateLayoutOp() {
        const op = this._op;
        if (!op || !op.lifted) return;
        const D = DashboardLayout;
        const m = op.m;
        const g = op.grid.getBoundingClientRect();
        let rect;

        if (op.kind === 'drag') {
            const left = Math.min(Math.max(op.x - g.left - op.offX, 0), Math.max(0, m.width - op.box.width));
            const top = Math.max(op.y - g.top - op.offY, 0);
            op.el.style.left = `${left}px`;
            op.el.style.top = `${top}px`;
            rect = { x: Math.round(left / m.colPitch), y: Math.round(top / m.rowPitch) };
        } else {
            let { width, height } = op.box;
            if (op.dir.includes('e')) {
                const minW = D.MIN_W * m.colPitch - D.GAP;
                width = Math.min(Math.max(op.x - g.left - op.box.left, minW), m.width - op.box.left);
            }
            if (op.dir.includes('s')) {
                const minH = D.MIN_H * m.rowPitch - D.GAP;
                height = Math.max(op.y - g.top - op.box.top, minH);
            }
            op.el.style.width = `${width}px`;
            op.el.style.height = `${height}px`;
            rect = {
                w: Math.max(D.MIN_W, Math.round((width + D.GAP) / m.colPitch)),
                h: Math.max(D.MIN_H, Math.round((height + D.GAP) / m.rowPitch)),
            };
        }

        const key = JSON.stringify(rect);
        if (key === op.lastKey) return;
        op.lastKey = key;

        op.layout = D.apply(op.start, op.id, rect);
        op.layout.forEach(it => {
            if (it.id === op.id) return;
            const el = this.widgetEl(it.id);
            if (el) D.place(el, it);
        });
        const mine = op.layout.find(i => i.id === op.id);
        D.place(op.ph, mine);
        this.updateSizeBadge(mine);
        op.grid.style.minHeight = `${(D.bottom(op.layout) + 12) * D.PITCH}px`;
    },

    updateSizeBadge(it) {
        const badge = this._op && this._op.badge;
        if (badge) badge.textContent = `${it.w} × ${it.h}`;
    },

    endLayoutOp(cancelled) {
        const op = this._op;
        if (!op) return;
        this._op = null;
        if (this._opHandlers) {
            Object.entries(this._opHandlers).forEach(([type, fn]) => document.removeEventListener(type, fn));
            this._opHandlers = null;
        }
        if (op.raf) cancelAnimationFrame(op.raf);
        document.body.classList.remove('dash-dragging', 'dash-resizing-e', 'dash-resizing-s', 'dash-resizing-se');
        op.grid.classList.remove('layout-active');
        if (op.ph) op.ph.remove();
        if (op.badge) op.badge.remove();
        if (op.lifted) {
            op.el.classList.remove('lifted');
            Object.assign(op.el.style, { left: '', top: '', width: '', height: '' });
        }
        if (!op.lifted || cancelled) {
            op.start.forEach(it => { const el = this.widgetEl(it.id); if (el) DashboardLayout.place(el, it); });
            this.afterLayoutChange();
            return;
        }
        this.commitLayout(op.start, op.layout);
    },

    // Scroll the page while a drag or resize nears the top or bottom edge.
    startAutoScroll(op) {
        const scroller = this.scrollParent(op.grid);
        const edge = 48;
        const step = () => {
            if (this._op !== op) return;
            const r = scroller === document.scrollingElement
                ? { top: 0, bottom: window.innerHeight }
                : scroller.getBoundingClientRect();
            let dy = 0;
            if (op.y < r.top + edge) dy = -Math.ceil((r.top + edge - op.y) / 3);
            else if (op.y > r.bottom - edge) dy = Math.ceil((op.y - (r.bottom - edge)) / 3);
            if (dy) {
                const before = scroller.scrollTop;
                scroller.scrollTop += dy;
                if (scroller.scrollTop !== before) this.updateLayoutOp();
            }
            op.raf = requestAnimationFrame(step);
        };
        op.raf = requestAnimationFrame(step);
    },

    scrollParent(el) {
        for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
            const oy = getComputedStyle(p).overflowY;
            if ((oy === 'auto' || oy === 'scroll') && p.scrollHeight > p.clientHeight) return p;
        }
        return document.scrollingElement || document.documentElement;
    },

    // Arrow keys move the focused widget one cell (up/down swap with the
    // neighbour, since the grid floats everything up); Shift resizes.
    onGridKeyDown(e) {
        if (!this.editMode || this._op) return;
        const header = e.target;
        if (!header.classList || !header.classList.contains('widget-header')) return;
        const delta = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] }[e.key];
        if (!delta) {
            if (e.key === 'Enter') { e.preventDefault(); this.showInlineWidgetEdit(header.dataset.widgetId); }
            return;
        }
        e.preventDefault();
        const D = DashboardLayout;
        const before = this.layoutItems();
        const it = before.find(i => i.id === header.dataset.widgetId);
        if (!it) return;

        let rect;
        if (e.shiftKey) {
            rect = { w: Math.max(D.MIN_W, it.w + delta[0]), h: Math.max(D.MIN_H, it.h + delta[1]) };
        } else if (delta[1] === 0) {
            rect = { x: it.x + delta[0] };
        } else {
            const overlapX = o => o.id !== it.id && o.x < it.x + it.w && o.x + o.w > it.x;
            if (delta[1] < 0) {
                const above = before.filter(o => overlapX(o) && o.y + o.h <= it.y)
                    .sort((a, b) => (b.y + b.h) - (a.y + a.h))[0];
                rect = { y: above ? above.y : it.y - 1 };
            } else {
                const below = before.filter(o => overlapX(o) && o.y >= it.y + it.h).sort((a, b) => a.y - b.y)[0];
                rect = { y: below ? Math.max(it.y + 1, below.y + below.h - it.h) : it.y + 1 };
            }
        }
        this.commitLayout(before, D.apply(before, it.id, rect));
    },

    // Adopt a new layout locally, then persist only what moved.
    commitLayout(before, after) {
        const changed = DashboardLayout.diff(before, after);
        after.forEach(it => {
            const w = this.getWidget(it.id);
            if (w) { w.pos_x = it.x; w.pos_y = it.y; w.width = it.w; w.height = it.h; }
            const el = this.widgetEl(it.id);
            if (el) DashboardLayout.place(el, it);
        });
        this.afterLayoutChange();
        if (!changed.length) return;
        this._undo.push(before);
        if (this._undo.length > 50) this._undo.shift();
        this.updateUndoButton();
        this.saveLayout(changed);
    },

    undoLayout() {
        const snapshot = this._undo.pop();
        this.updateUndoButton();
        if (!snapshot || !this.currentDashboard) return;
        // Widgets added since the snapshot keep their place; deleted ones drop out.
        const prev = new Map(snapshot.map(i => [i.id, i]));
        const current = this.layoutItems();
        const next = DashboardLayout.compact(current.map(i => ({ ...(prev.get(i.id) || i) })));
        const changed = DashboardLayout.diff(current, next);
        next.forEach(it => {
            const w = this.getWidget(it.id);
            if (w) { w.pos_x = it.x; w.pos_y = it.y; w.width = it.w; w.height = it.h; }
            const el = this.widgetEl(it.id);
            if (el) DashboardLayout.place(el, it);
        });
        this.afterLayoutChange();
        if (changed.length) this.saveLayout(changed);
    },

    updateUndoButton() {
        const btn = document.getElementById('dashboardUndoBtn');
        if (btn) btn.disabled = !this._undo.length;
    },

    async saveLayout(items) {
        if (!this.currentDashboard || !items.length) return;
        try {
            const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/layout`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({
                    widgets: items.map(i => ({ id: i.id, pos_x: i.x, pos_y: i.y, width: i.w, height: i.h }))
                })
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to save layout');
        } catch (err) {
            console.error('[Dashboards] Failed to save layout:', err);
            this.showError('Failed to save layout');
        }
    },

    // =====================
    // Widget CRUD
    // =====================

    async addWidget() {
        if (!this.currentDashboard || !this.editMode) return;

        const w = 12, h = 16;
        const slot = DashboardLayout.findSlot(this.layoutItems(), w, h);

        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets`, {
                method: 'POST',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({
                    title: 'New Widget',
                    query_content: '',
                    chart_type: 'table',
                    pos_x: slot.x,
                    pos_y: slot.y,
                    width: w,
                    height: h
                })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to create widget');

            const widget = data.data;
            if (!this.currentDashboard.widgets) this.currentDashboard.widgets = [];
            this.currentDashboard.widgets.push(widget);

            const grid = document.getElementById('dashboardGrid');
            if (grid) {
                const el = this.createWidgetElement(widget);
                grid.appendChild(el);
                this.afterLayoutChange();
                el.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            }

            // Open inline editor immediately for the new widget
            this.showInlineWidgetEdit(widget.id);
        } catch (err) {
            console.error('[Dashboards] Failed to add widget:', err);
            this.showError('Failed to add widget');
        }
    },

    showInlineWidgetEdit(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId)
            : null;
        if (!widget) return;

        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (!contentEl) return;

        // Don't open a second editor on the same widget
        if (contentEl._editingWidget) return;
        contentEl._editingWidget = true;

        // Save the query text to restore on cancel. The rendered visuals are torn
        // down rather than stashed: a chart's pixels do not survive an innerHTML
        // round-trip, so cancel re-renders from the cache instead.
        contentEl._savedContent = contentEl.innerHTML;
        this.destroyWidgetVisuals(contentEl);

        const hid = `wie-h-${widgetId}`;
        const tid = `wie-q-${widgetId}`;

        contentEl.innerHTML = `
            <div style="display:flex;flex-direction:column;height:100%;padding:8px;box-sizing:border-box;gap:6px;">
                <input type="text" id="wie-title-${widgetId}" class="form-input" value="${Utils.escapeHtml(widget.title || '')}" placeholder="Widget title" style="flex-shrink:0;font-size:0.8rem;padding:5px 8px;">
                <div style="flex:1;position:relative;min-height:60px;">
                    <div id="${hid}" class="query-highlight" style="position:absolute;top:0;left:0;width:100%;height:100%;padding:8px;border:1px solid transparent;border-radius:4px;background:transparent;font-family:var(--font-mono);font-size:0.8rem;line-height:1.5;white-space:pre-wrap;word-wrap:break-word;overflow:hidden;pointer-events:none;z-index:1;box-sizing:border-box;"></div>
                    <textarea id="${tid}" spellcheck="false" autocomplete="off" autocorrect="off" autocapitalize="off" style="position:absolute;top:0;left:0;width:100%;height:100%;padding:8px;border:1px solid var(--border-color);border-radius:4px;background:transparent;color:transparent;caret-color:var(--text-primary);font-family:var(--font-mono);font-size:0.8rem;line-height:1.5;resize:none;box-sizing:border-box;z-index:2;outline:none;">${Utils.escapeHtml(widget.query_content || '')}</textarea>
                </div>
                <div style="display:flex;justify-content:flex-end;gap:6px;flex-shrink:0;">
                    <button class="btn-sm btn-secondary" onclick="Dashboards.cancelInlineWidgetEdit('${widgetId}')">Cancel</button>
                    <button class="btn-sm btn-primary" onclick="Dashboards.saveInlineWidgetEdit('${widgetId}')">Save</button>
                </div>
            </div>
        `;

        const queryEl = document.getElementById(tid);
        const highlightEl = document.getElementById(hid);
        if (queryEl && highlightEl && window.SyntaxHighlight) {
            const doHighlight = () => {
                highlightEl.innerHTML = SyntaxHighlight.highlight(queryEl.value, SyntaxHighlight.errorRanges[tid], SyntaxHighlight.matchRanges[tid]) + '<br/>';
                highlightEl.scrollTop = queryEl.scrollTop;
            };
            doHighlight();
            queryEl.addEventListener('input', doHighlight);
            queryEl.addEventListener('scroll', () => { highlightEl.scrollTop = queryEl.scrollTop; });
            queryEl.focus();
            // Live BQL validation: underline the offending span as the user types.
            if (window.QueryValidate) {
                QueryValidate.attach({
                    inputId: tid,
                    highlightId: hid,
                    getFractalId: () => window.FractalContext?.currentFractal?.id || undefined,
                    getVariables: () => this.editorVariables(queryEl ? queryEl.value : ''),
                    rerender: doHighlight,
                });
            }
        }
    },

    cancelInlineWidgetEdit(widgetId) {
        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (!contentEl) return;
        const saved = contentEl._savedContent;
        delete contentEl._savedContent;
        delete contentEl._editingWidget;

        // Re-render from the cache rather than restoring the saved markup: a
        // chart's canvas comes back blank, since its instance was destroyed when
        // the editor opened. The markup is only a fallback for a widget that has
        // never produced results.
        if (this.renderWidgetFromCache(widgetId)) return;
        delete contentEl.dataset.rendered;
        contentEl.innerHTML = saved || '<div class="widget-loading">No results</div>';
    },

    async saveInlineWidgetEdit(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId)
            : null;
        if (!widget) return;

        const titleEl = document.getElementById(`wie-title-${widgetId}`);
        const queryEl = document.getElementById(`wie-q-${widgetId}`);

        const title = titleEl ? titleEl.value.trim() : widget.title;
        const query = queryEl ? queryEl.value.trim() : widget.query_content;

        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ title, query_content: query })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to update widget');

            widget.title = title;
            widget.query_content = query;
            // A changed query may introduce or remove @variables. Persist the new
            // set BEFORE executing, since the execute endpoint substitutes from
            // the stored variables (a new @var would otherwise hit the parser raw).
            await this.syncDashboardVariables();

            // Update title in widget header
            const widgetEl = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
            if (widgetEl) {
                const titleSpan = widgetEl.querySelector('.widget-title');
                if (titleSpan) titleSpan.textContent = title || 'Widget';
            }

            // Close inline editor
            const contentEl = document.getElementById(`wc-${widgetId}`);
            if (contentEl) {
                delete contentEl._savedContent;
                delete contentEl._editingWidget;
                delete contentEl.dataset.rendered;
                this.destroyWidgetVisuals(contentEl);
                contentEl.innerHTML = '<div class="widget-loading">Executing...</div>';
            }

            await this.executeWidget(widgetId);
        } catch (err) {
            console.error('[Dashboards] Failed to save widget:', err);
            this.showError('Failed to save widget');
        }
    },

    async deleteWidget(widgetId) {
        if (!this.currentDashboard) return;
        if (!confirm('Delete this widget?')) return;

        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}`, {
                method: 'DELETE',
                headers: this.sseHeaders(),
                credentials: 'include'
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to delete widget');

            this.currentDashboard.widgets = this.currentDashboard.widgets.filter(w => w.id !== widgetId);
            // Removing a widget may orphan @variables it referenced.
            this.syncDashboardVariables();

            const widgetEl = this.widgetEl(widgetId);
            if (widgetEl) { this.destroyWidgetVisuals(widgetEl); widgetEl.remove(); }
            this.afterLayoutChange();
        } catch (err) {
            console.error('[Dashboards] Failed to delete widget:', err);
            this.showError('Failed to delete widget');
        }
    },

    // =====================
    // Dashboard CRUD
    // =====================

    // =====================
    // Shared Links (public wallboards)
    // =====================

    // Shows the Share button only when the feature is globally enabled. Any
    // authenticated user can read the flag; the backend still enforces analyst+
    // on link creation.
    async updateShareButtonVisibility() {
        const btn = document.getElementById('dashboardShareBtn');
        if (!btn) return;
        try {
            const res = await fetch('/api/v1/system/shared-links', { credentials: 'include' });
            const d = res.ok ? await res.json() : { enabled: false };
            this._sharedLinksEnabled = !!d.enabled;
        } catch {
            this._sharedLinksEnabled = false;
        }
        btn.style.display = this._sharedLinksEnabled ? '' : 'none';
    },

    buildShareUrl(token) {
        return `${window.location.origin}/shared/${token}`;
    },

    formatShareDate(iso) {
        if (!iso) return 'never';
        try { return TZ.format(iso, 'friendly'); } catch { return iso; }
    },

    showShareModal() {
        if (!this.currentDashboard) return;
        const existing = document.getElementById('shareLinksModal');
        if (existing) existing.remove();

        const esc = (window.Utils && Utils.escapeHtml) ? Utils.escapeHtml : (s => s);
        const modal = document.createElement('div');
        modal.id = 'shareLinksModal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content" style="width:560px;max-width:95vw;">
                <div class="modal-header">
                    <h3>Share "${esc(this.currentDashboard.name)}"</h3>
                    <button class="modal-close" onclick="document.getElementById('shareLinksModal').remove()">&#x2715;</button>
                </div>
                <div class="modal-body">
                    <p class="setting-description" style="margin-top:0;">
                        Anyone with a link can view this dashboard read-only, without signing in.
                        Links show cached results only and stay behind your network controls (mTLS/IP).
                    </p>
                    <div class="form-group">
                        <label>Label (optional)</label>
                        <input type="text" id="shareLinkLabel" class="form-input" placeholder="e.g. Lobby TV" maxlength="200">
                    </div>
                    <div class="form-group">
                        <label>Expires</label>
                        <select id="shareLinkExpiry" class="form-input">
                            <option value="0" selected>Never</option>
                            <option value="86400">In 24 hours</option>
                            <option value="604800">In 7 days</option>
                            <option value="2592000">In 30 days</option>
                            <option value="7776000">In 90 days</option>
                        </select>
                    </div>
                    <div style="margin:4px 0 16px;">
                        <button class="btn-primary" onclick="Dashboards.createSharedLink()">Create link</button>
                    </div>
                    <div id="shareLinkReveal" style="display:none;"></div>
                    <div style="border-top:1px solid var(--border-color);padding-top:12px;">
                        <label class="setting-label" style="display:block;margin-bottom:8px;">Active links</label>
                        <div id="shareLinksList"><div style="color:var(--text-muted);font-size:0.85rem;">Loading…</div></div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-secondary" onclick="document.getElementById('shareLinksModal').remove()">Close</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
        modal.addEventListener('click', (e) => {
            if (e.target === modal) modal.remove();
        });
        this.loadSharedLinks();
    },

    async loadSharedLinks() {
        const listEl = document.getElementById('shareLinksList');
        if (!listEl || !this.currentDashboard) return;
        try {
            const res = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/shared-links`, { credentials: 'include' });
            const data = await res.json();
            if (!data.success) throw new Error(data.error || 'Failed to load links');
            const links = data.data || [];
            listEl.innerHTML = this.renderSharedLinksList(links);
        } catch (err) {
            listEl.innerHTML = `<div style="color:var(--error);font-size:0.85rem;">${err.message}</div>`;
        }
    },

    renderSharedLinksList(links) {
        const esc = (window.Utils && Utils.escapeHtml) ? Utils.escapeHtml : (s => s);
        if (!links.length) {
            return `<div style="color:var(--text-muted);font-size:0.85rem;">No active links.</div>`;
        }
        return links.map(l => {
            const label = l.label ? esc(l.label) : '<span style="color:var(--text-muted);">Untitled</span>';
            const expiry = l.expires_at ? `Expires ${esc(this.formatShareDate(l.expires_at))}` : 'Never expires';
            const last = l.last_accessed_at ? `Last viewed ${esc(this.formatShareDate(l.last_accessed_at))}` : 'Never viewed';
            return `
                <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid var(--border-color);">
                    <div style="min-width:0;">
                        <div style="font-weight:500;">${label}</div>
                        <div style="font-size:0.78rem;color:var(--text-muted);font-family:monospace;">${esc(l.token_prefix)}…</div>
                        <div style="font-size:0.78rem;color:var(--text-muted);">${expiry} &middot; ${last}</div>
                    </div>
                    <button class="btn-secondary" style="color:var(--error);flex-shrink:0;" onclick="Dashboards.revokeSharedLink('${esc(l.id)}')">Revoke</button>
                </div>
            `;
        }).join('');
    },

    async createSharedLink() {
        if (!this.currentDashboard) return;
        const labelEl = document.getElementById('shareLinkLabel');
        const expiryEl = document.getElementById('shareLinkExpiry');
        const label = labelEl ? labelEl.value.trim() : '';
        const expiresInSeconds = expiryEl ? parseInt(expiryEl.value, 10) || 0 : 0;
        try {
            const res = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/shared-links`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ label, expires_in_seconds: expiresInSeconds })
            });
            if (res.status === 403) throw new Error('You need analyst access on this fractal to create links.');
            const data = await res.json();
            if (!data.success || !data.token) throw new Error(data.error || 'Failed to create link');
            this.showSharedLinkReveal(data.token);
            if (labelEl) labelEl.value = '';
            this.loadSharedLinks();
        } catch (err) {
            if (window.Toast) Toast.error('Could not create link', err.message);
        }
    },

    // Reveals the full URL exactly once (the server stores only a hash and can
    // never show it again).
    showSharedLinkReveal(token) {
        const box = document.getElementById('shareLinkReveal');
        if (!box) return;
        const url = this.buildShareUrl(token);
        const esc = (window.Utils && Utils.escapeHtml) ? Utils.escapeHtml : (s => s);
        box.style.display = 'block';
        box.innerHTML = `
            <div style="margin-bottom:16px;padding:12px;border:1px solid var(--accent-primary);border-radius:8px;background:var(--bg-tertiary);">
                <div style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:8px;">
                    Copy this link now &mdash; it will not be shown again.
                </div>
                <div style="display:flex;gap:8px;">
                    <input type="text" readonly value="${esc(url)}" id="shareRevealInput"
                        style="flex:1;min-width:0;padding:8px;border:1px solid var(--border-color);border-radius:4px;background:var(--bg-primary);color:var(--text-primary);font-family:monospace;font-size:0.8rem;">
                    <button class="btn-primary" style="flex-shrink:0;" onclick="Dashboards.copySharedLink()">Copy</button>
                </div>
            </div>
        `;
        const input = document.getElementById('shareRevealInput');
        if (input) { input.focus(); input.select(); }
    },

    copySharedLink() {
        const input = document.getElementById('shareRevealInput');
        if (!input) return;
        const done = () => { if (window.Toast) Toast.success('Copied', 'Share link copied to clipboard.'); };
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(input.value).then(done).catch(() => { input.select(); document.execCommand('copy'); done(); });
        } else {
            input.select();
            document.execCommand('copy');
            done();
        }
    },

    async revokeSharedLink(linkId) {
        if (!this.currentDashboard) return;
        if (!window.confirm('Revoke this link? Anyone using it will immediately lose access.')) return;
        try {
            const res = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/shared-links/${linkId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await res.json();
            if (!data.success) throw new Error(data.error || 'Failed to revoke');
            if (window.Toast) Toast.success('Link revoked', 'The shared link no longer works.');
            this.loadSharedLinks();
        } catch (err) {
            if (window.Toast) Toast.error('Could not revoke', err.message);
        }
    },

    showCreateDashboardModal() {
        const existing = document.getElementById('createDashboardModal');
        if (existing) existing.remove();

        const modal = document.createElement('div');
        modal.id = 'createDashboardModal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content" style="width:480px;max-width:95vw;">
                <div class="modal-header">
                    <h3>New Dashboard</h3>
                    <button class="modal-close" onclick="document.getElementById('createDashboardModal').remove()">&#x2715;</button>
                </div>
                <div class="modal-body">
                    <div class="form-group">
                        <label>Name</label>
                        <input type="text" id="cdName" class="form-input" placeholder="Dashboard name" autofocus>
                    </div>
                    <div class="form-group">
                        <label>Description</label>
                        <input type="text" id="cdDescription" class="form-input" placeholder="Optional description">
                    </div>
                    <div class="form-group">
                        <label>Default Time Range</label>
                        <select id="cdTimeRange" class="form-input">
                            <option value="last1h">Last 1 Hour</option>
                            <option value="last24h" selected>Last 24 Hours</option>
                            <option value="last7d">Last 7 Days</option>
                            <option value="last30d">Last 30 Days</option>
                            <option value="all">All Time</option>
                            <option value="custom">Custom range</option>
                        </select>
                    </div>
                    <div id="cdCustomRange" style="display:none;margin-top:8px;padding:10px;border:1px solid var(--border-color);border-radius:6px;background:var(--bg-tertiary);">
                        <div style="margin-bottom:8px;">
                            <label style="display:block;margin-bottom:4px;font-size:0.85rem;">Start Time</label>
                            <input type="text" placeholder="YYYY-MM-DD HH:mm" id="cdTimeStart" style="width:100%;padding:8px;border:1px solid var(--border-color);border-radius:4px;background:var(--bg-primary);color:var(--text-primary);">
                        </div>
                        <div>
                            <label style="display:block;margin-bottom:4px;font-size:0.85rem;">End Time</label>
                            <input type="text" placeholder="YYYY-MM-DD HH:mm" id="cdTimeEnd" style="width:100%;padding:8px;border:1px solid var(--border-color);border-radius:4px;background:var(--bg-primary);color:var(--text-primary);">
                        </div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-secondary" onclick="document.getElementById('createDashboardModal').remove()">Cancel</button>
                    <button class="btn-primary" onclick="Dashboards.handleCreateDashboard()">Create Dashboard</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        const nameInput = document.getElementById('cdName');
        if (nameInput) {
            nameInput.focus();
            nameInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this.handleCreateDashboard();
            });
        }

        const timeRangeSelect = document.getElementById('cdTimeRange');
        if (timeRangeSelect) {
            timeRangeSelect.addEventListener('change', (e) => {
                const customRange = document.getElementById('cdCustomRange');
                if (!customRange) return;
                const isCustom = e.target.value === 'custom';
                customRange.style.display = isCustom ? 'block' : 'none';
                if (isCustom) {
                    const now = new Date();
                    const pad = (n) => String(n).padStart(2, '0');
                    const fmt = (d) => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
                    const startEl = document.getElementById('cdTimeStart');
                    const endEl = document.getElementById('cdTimeEnd');
                    if (startEl && !startEl.value) startEl.value = fmt(new Date(now - 86400000));
                    if (endEl && !endEl.value) endEl.value = fmt(now);
                }
            });
        }

        modal.addEventListener('click', (e) => {
            if (e.target === modal) modal.remove();
        });
    },

    async handleCreateDashboard() {
        const name = document.getElementById('cdName')?.value.trim();
        const description = document.getElementById('cdDescription')?.value.trim() || '';
        const timeRangeType = document.getElementById('cdTimeRange')?.value || 'last24h';

        if (!name) { this.showError('Name is required'); return; }

        let timeRangeStart = null;
        let timeRangeEnd = null;

        if (timeRangeType === 'custom') {
            const start = document.getElementById('cdTimeStart')?.value;
            const end = document.getElementById('cdTimeEnd')?.value;
            if (!start || !end) { this.showError('Start and end times are required for custom range'); return; }
            const startDate = new Date(start);
            const endDate = new Date(end);
            if (startDate >= endDate) { this.showError('Start time must be before end time'); return; }
            timeRangeStart = startDate.toISOString();
            timeRangeEnd = endDate.toISOString();
        }

        const body = { name, description, time_range_type: timeRangeType, time_range_start: timeRangeStart, time_range_end: timeRangeEnd };

        try {
            const response = await fetch('/api/v1/dashboards', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to create dashboard');

            document.getElementById('createDashboardModal')?.remove();
            this.openDashboard(data.data.id);
        } catch (err) {
            console.error('[Dashboards] Failed to create dashboard:', err);
            this.showError(err.message || 'Failed to create dashboard');
        }
    },

    async deleteDashboard() {
        if (!this.currentDashboard) return;
        if (!confirm(`Delete dashboard "${this.currentDashboard.name}"? This cannot be undone.`)) return;
        await this.deleteDashboardById(this.currentDashboard.id);
    },

    async deleteDashboardById(id) {
        try {
            const response = await fetch(`/api/v1/dashboards/${id}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to delete dashboard');

            if (this.currentDashboard && this.currentDashboard.id === id) {
                this.currentDashboard = null;
                window.App?.pushSubPath('');
            }
            this.showDashboardListing();
        } catch (err) {
            console.error('[Dashboards] Failed to delete dashboard:', err);
            this.showError('Failed to delete dashboard');
        }
    },

    showSettingsModal() {
        if (!this.currentDashboard || !this.canEdit()) return;
        const d = this.currentDashboard;

        const existing = document.getElementById('dashSettingsModal');
        if (existing) existing.remove();

        const modal = document.createElement('div');
        modal.id = 'dashSettingsModal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content" style="width:440px;max-width:95vw;">
                <div class="modal-header">
                    <h3>Dashboard settings</h3>
                    <button class="modal-close" onclick="document.getElementById('dashSettingsModal').remove()">&#x2715;</button>
                </div>
                <div class="modal-body">
                    <div class="form-group">
                        <label for="dsName">Name</label>
                        <input type="text" id="dsName" class="form-input" maxlength="255" value="${Utils.escapeAttr(d.name || '')}">
                    </div>
                    <div class="form-group">
                        <label for="dsDescription">Description</label>
                        <input type="text" id="dsDescription" class="form-input" value="${Utils.escapeAttr(d.description || '')}" placeholder="Optional">
                    </div>
                    <div class="form-group">
                        <label>Default time range</label>
                        <div class="form-hint" style="margin-top:0;">${Utils.escapeHtml(this.rangeLabel(this.defaultRange()))}. Pick a range in the time picker, then use Save as default.</div>
                    </div>
                    <div class="form-group">
                        <label for="dsZone">Bucket timezone</label>
                        <select id="dsZone" class="form-input">${this.zoneOptionsHTML()}</select>
                        <div class="form-hint">Where day, hour and week boundaries fall for bucket() and timechart. It belongs to the dashboard so every viewer reads the same buckets. Individual timestamps still follow each viewer's own zone.</div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-secondary" onclick="document.getElementById('dashSettingsModal').remove()">Cancel</button>
                    <button class="btn-primary" onclick="Dashboards.saveSettings()">Save</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
        modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
        document.getElementById('dsName')?.focus();
    },

    async updateRefreshInterval(seconds) {
        if (!this.currentDashboard) return;
        if (isNaN(seconds)) seconds = 0;
        try {
            const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/refresh-interval`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ refresh_interval: seconds })
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed');
            this.currentDashboard.refresh_interval = seconds;
            this.scheduleViewRefresh();
            if (seconds === 0) {
                this.showSuccess('Auto-refresh disabled');
            } else if (seconds < 0) {
                this.showSuccess('Auto-refresh set to Auto');
            } else {
                this.showSuccess('Auto-refresh updated');
            }
        } catch (err) {
            console.error('[Dashboards] Failed to update refresh interval:', err);
            this.showError('Failed to update auto-refresh');
        }
    },

    // Zone options for the dashboard bucket-timezone select. UTC and the
    // viewer's own zone lead because they are the two anyone reaches for; a
    // zone set from another device stays selectable even if this engine does
    // not enumerate it.
    zoneOptionsHTML() {
        const current = this.currentDashboard?.timezone || 'UTC';
        const opt = (z) => `<option value="${Utils.escapeAttr(z)}"${z === current ? ' selected' : ''}>${Utils.escapeHtml(z)}</option>`;
        const browser = window.TZ ? TZ.browserZone() : 'UTC';
        const lead = ['UTC'];
        if (browser !== 'UTC') lead.push(browser);
        if (!lead.includes(current)) lead.push(current);
        const all = (window.TZ ? TZ.zoneList() : ['UTC']).filter(z => !lead.includes(z));
        return `<optgroup label="Common">${lead.map(opt).join('')}</optgroup>` +
               `<optgroup label="All">${all.map(opt).join('')}</optgroup>`;
    },

    async saveSettings() {
        if (!this.currentDashboard) return;
        const d = this.currentDashboard;
        const name = document.getElementById('dsName')?.value.trim();
        const description = document.getElementById('dsDescription')?.value.trim() ?? '';
        const zone = document.getElementById('dsZone')?.value;
        if (!name) { this.showError('Name is required'); return; }

        const body = {};
        if (name !== d.name) body.name = name;
        if (description !== (d.description || '')) body.description = description;
        if (zone && zone !== (d.timezone || 'UTC')) body.timezone = zone;
        if (!Object.keys(body).length) {
            document.getElementById('dashSettingsModal')?.remove();
            return;
        }

        try {
            const resp = await fetch(`/api/v1/dashboards/${d.id}`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to save');

            Object.assign(d, body);
            const titleEl = document.getElementById('dashboardTitle');
            if (titleEl) titleEl.textContent = d.name;
            document.getElementById('dashSettingsModal')?.remove();

            if (body.timezone) {
                // A zone change re-ran every widget server-side before this
                // response returned, so the shared cache is already fresh.
                if (this.activeView()) this.autoExecuteAllWidgets();
                else await this.reloadCachedResults();
            }
        } catch (err) {
            console.error('[Dashboards] Failed to save settings:', err);
            this.showError(err.message || 'Failed to save settings');
        }
    },

    // Re-read the server-persisted widget results and repaint from them.
    async reloadCachedResults() {
        if (!this.currentDashboard) return;
        try {
            const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}`, { credentials: 'include' });
            const data = await resp.json();
            if (!data.success || !data.data.widgets) return;
            const byId = new Map(data.data.widgets.map(w => [w.id, w]));
            this.currentDashboard.widgets.forEach(w => {
                const fresh = byId.get(w.id);
                if (!fresh) return;
                w.last_results = fresh.last_results;
                w.last_executed_at = fresh.last_executed_at;
            });
            this.paintCachedWidgets();
        } catch (err) {
            console.error('[Dashboards] Failed to reload cached results:', err);
        }
    },

    // =====================
    // Format Panel (type-aware, shared with notebooks via BifractFormat)
    // =====================

    openFormatPanel(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
        if (!widget) return;

        let cached = {};
        if (widget.last_results) {
            try { cached = typeof widget.last_results === 'string' ? JSON.parse(widget.last_results) : widget.last_results; } catch (e) { cached = {}; }
        }
        const chartType = (cached && cached.chart_type) || widget.chart_type || 'table';
        const original = JSON.parse(JSON.stringify(this.parseChartConfig(widget.chart_config) || {}));

        BifractFormat.open({
            chartType,
            config: this.parseChartConfig(widget.chart_config),
            fields: cached.field_order || [],
            results: cached.results || [],
            onPreview: (cfg) => { widget.chart_config = cfg; this.renderWidgetFromCache(widgetId); },
            onCancel: () => { widget.chart_config = original; this.renderWidgetFromCache(widgetId); },
            onSave: (cfg) => this.saveWidgetFormat(widgetId, cfg)
        });
    },

    // Renders the results this viewer is looking at: their private view's
    // results when one is active, else the shared cache. Returns true when
    // something was rendered, so callers can fall back.
    renderWidgetFromCache(widgetId) {
        const widget = this.getWidget(widgetId);
        if (!widget) return false;
        let resultData = this.activeView() ? widget._viewResults : widget.last_results;
        if (!resultData) return false;
        if (typeof resultData === 'string') {
            try { resultData = JSON.parse(resultData); } catch (e) { return false; }
        }
        if (!resultData || typeof resultData !== 'object') return false;
        this.renderWidgetResults(widgetId, resultData);
        return true;
    },

    async saveWidgetFormat(widgetId, cfg) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
        if (!widget) return;
        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ chart_config: cfg })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save');

            widget.chart_config = cfg;
            this.renderWidgetFromCache(widgetId);
            this.showSuccess('Formatting saved');
        } catch (err) {
            console.error('[Dashboards] Failed to save formatting:', err);
            this.showError('Failed to save formatting');
        }
    },

    // =====================
    // Pivots / Drilldown
    // =====================

    getWidget(widgetId) {
        return (this.currentDashboard && this.currentDashboard.widgets)
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
    },

    widgetHasPivots(widgetId) {
        if (!window.Pivots) return false;
        const w = this.getWidget(widgetId);
        return !!(w && Pivots.getPivots(w).length);
    },

    openPivotConfig(widgetId) {
        document.querySelectorAll('.widget-kebab-menu.open').forEach(m => m.classList.remove('open'));
        if (window.Pivots) Pivots.openConfig(widgetId);
    },

    // A data point was clicked on a pivot-enabled widget: route to the pivot
    // runtime (a single pivot fires immediately; multiple show a chooser menu).
    onWidgetDataClick(widgetId, ctx, event) {
        const widget = this.getWidget(widgetId);
        if (!widget || !window.Pivots) return;
        const pivots = Pivots.getPivots(widget);
        if (!pivots.length) return;
        Pivots.handleDataClick(widget, pivots, ctx, event);
    },

    // =====================
    // Per-viewer view state
    // =====================
    // The time range and @variable values a viewer picks belong to them alone.
    // They live in the URL hash ("?range=last1h&var-host=web1", or from/to for an
    // absolute range) so reloads, back/forward and shared links keep them, and
    // they run through the preview path, which the server neither persists nor
    // broadcasts. Pivot drilldowns are views too. Analysts can promote a view to
    // the dashboard defaults with "Save as default".

    activeView() {
        const v = this._view;
        if (!v || !this.currentDashboard) return null;
        return (v.range || Object.keys(v.vars).length) ? v : null;
    },

    ensureView() {
        if (!this._view) this._view = { range: null, vars: {} };
        return this._view;
    },

    resetViewState() {
        this.abortInflight();
        this._view = null;
        this._rangeHistory = [];
        this.stopViewRefresh();
    },

    setViewRange(range, remember = true) {
        const prev = this.currentRange();
        const view = this.ensureView();
        view.range = (range && !this.sameRange(range, this.defaultRange())) ? range : null;
        if (remember && !this.sameRange(prev, this.currentRange())) {
            this._rangeHistory.push(prev);
            if (this._rangeHistory.length > 20) this._rangeHistory.shift();
        }
        this.onViewChanged();
    },

    setViewVar(name, value) {
        const view = this.ensureView();
        const def = this.varManager ? this.varManager.getValue(name) : undefined;
        if (value === def) delete view.vars[name];
        else view.vars[name] = value;
        this.onViewChanged();
    },

    clearViewVar(name) {
        if (!this._view || !(name in this._view.vars)) return;
        delete this._view.vars[name];
        this.onViewChanged();
    },

    resetView() {
        if (!this.activeView()) return;
        this._view = null;
        this._rangeHistory = [];
        this.onViewChanged();
    },

    goBackRange() {
        const prev = this._rangeHistory.pop();
        if (prev) this.setViewRange(prev, false);
    },

    onViewChanged() {
        this.abortInflight();
        this.syncViewToUrl();
        this.renderViewState();
        if (this.activeView()) {
            this.autoExecuteAllWidgets();
        } else {
            // Back on the defaults: the shared cache is what everyone sees.
            this.currentDashboard?.widgets?.forEach(w => { w._viewResults = null; });
            this.paintCachedWidgets();
            this.autoExecuteAllWidgets(true);
        }
    },

    // Enter a pivot drilldown on this or another dashboard. Same-board
    // drilldowns layer onto the current view; cross-board ones open the target.
    enterDrilldown(targetId, dd) {
        const incoming = this.viewFromDrilldown(dd);
        if (targetId && this.currentDashboard && targetId !== this.currentDashboard.id) {
            this._pendingView = incoming;
            this.openDashboard(targetId);
            return;
        }
        const view = this.ensureView();
        Object.assign(view.vars, incoming.vars);
        if (incoming.range) {
            this._rangeHistory.push(this.currentRange());
            view.range = incoming.range;
        }
        this.onViewChanged();
    },

    viewFromDrilldown(dd) {
        const view = { range: null, vars: {} };
        (dd && Array.isArray(dd.vars) ? dd.vars : []).forEach(v => {
            if (v && v.name) view.vars[v.name] = v.value == null ? '' : String(v.value);
        });
        if (dd && dd.start && dd.end) view.range = { type: 'custom', start: dd.start, end: dd.end };
        return view;
    },

    viewParams(view) {
        const p = new URLSearchParams();
        if (!view) return p;
        if (view.range) {
            if (view.range.type === 'custom') {
                p.set('from', view.range.start);
                p.set('to', view.range.end);
            } else {
                p.set('range', view.range.type);
            }
        }
        Object.entries(view.vars || {}).forEach(([name, value]) => p.set(`var-${name}`, value));
        return p;
    },

    viewFromParams(params) {
        const view = { range: null, vars: {} };
        const range = params.get('range');
        const from = params.get('from');
        const to = params.get('to');
        if (range && (range === 'all' || this.parseRelative(range))) {
            view.range = { type: range };
        } else if (from && to) {
            const s = new Date(from), e = new Date(to);
            if (Number.isFinite(s.getTime()) && Number.isFinite(e.getTime()) && s < e) {
                view.range = { type: 'custom', start: s.toISOString(), end: e.toISOString() };
            }
        }
        params.forEach((value, key) => {
            if (key.startsWith('var-') && /^[A-Za-z_][A-Za-z0-9_]*$/.test(key.slice(4))) view.vars[key.slice(4)] = value;
        });
        return (view.range || Object.keys(view.vars).length) ? view : null;
    },

    // Links made before views moved into the hash carried a one-shot ?pv=.
    _readLegacyDrilldown() {
        const params = new URLSearchParams(window.location.search);
        const raw = params.get('pv');
        if (!raw) return null;
        params.delete('pv');
        const qs = params.toString();
        window.history.replaceState(window.history.state, document.title,
            window.location.pathname + (qs ? '?' + qs : '') + window.location.hash);
        try {
            const dd = JSON.parse(decodeURIComponent(atob(raw)));
            return (dd && Array.isArray(dd.vars)) ? this.viewFromDrilldown(dd) : null;
        } catch (e) {
            console.warn('[Dashboards] Invalid drilldown param:', e);
            return null;
        }
    },

    // Mirror the view into the hash without adding a history entry.
    syncViewToUrl() {
        if (!this.currentDashboard) return;
        const [base] = window.location.hash.split('?');
        if (!base.endsWith(`/dashboards/${this.currentDashboard.id}`)) return;
        const qs = this.viewParams(this.activeView()).toString();
        const target = base + (qs ? `?${qs}` : '');
        if (target !== window.location.hash) {
            window.history.replaceState(window.history.state, '', target);
        }
    },

    renderViewState() {
        const view = this.activeView();
        const range = this.currentRange();

        const label = document.getElementById('dashboardTimeLabel');
        if (label) label.textContent = this.rangeLabel(range);
        const timeBtn = document.getElementById('dashboardTimeBtn');
        if (timeBtn) {
            timeBtn.classList.toggle('modified', !!(view && view.range));
            timeBtn.title = view && view.range
                ? `Your range. Dashboard default: ${this.rangeLabel(this.defaultRange())}`
                : 'Dashboard time range';
        }
        const back = document.getElementById('dashboardTimeBackBtn');
        if (back) back.style.display = this._rangeHistory.length ? '' : 'none';
        const zoom = document.getElementById('dashboardZoomOutBtn');
        if (zoom) zoom.disabled = range.type === 'all';

        const chip = document.getElementById('dashboardViewState');
        if (chip) chip.style.display = view ? '' : 'none';
        const save = document.getElementById('dashboardViewSaveBtn');
        if (save) save.style.display = this.canEdit() ? '' : 'none';

        const mgr = this.ensureVarManager();
        if (mgr) {
            const vars = (view && view.vars) || {};
            if (Object.keys(vars).length) mgr.setDisplayOverlay(new Map(Object.entries(vars)));
            else mgr.clearDisplayOverlay();
        }

        this.renderUpdatedAt();
        this.scheduleViewRefresh();
    },

    async saveViewAsDefault() {
        const view = this.activeView();
        if (!view || !this.canEdit() || !this.currentDashboard) return;
        const d = this.currentDashboard;
        try {
            if (view.range) {
                const body = { time_range_type: view.range.type };
                if (view.range.type === 'custom') {
                    body.time_range_start = view.range.start;
                    body.time_range_end = view.range.end;
                }
                const resp = await fetch(`/api/v1/dashboards/${d.id}`, {
                    method: 'PUT',
                    headers: this.sseHeaders(),
                    credentials: 'include',
                    body: JSON.stringify(body)
                });
                const data = await resp.json();
                if (!data.success) throw new Error(data.error || 'Failed to save time range');
                d.time_range_type = body.time_range_type;
                if (body.time_range_start) {
                    d.time_range_start = body.time_range_start;
                    d.time_range_end = body.time_range_end;
                }
            }
            const mgr = this.ensureVarManager();
            if (mgr && Object.keys(view.vars).length) {
                Object.entries(view.vars).forEach(([name, value]) => mgr.setValue(name, value));
                d.variables = mgr.serialize();
                await this.saveVariables();
                mgr.render();
            }

            // The defaults now match the view: run the shared refresh so every
            // viewer and the cache pick them up.
            this.abortInflight();
            this._view = null;
            this._rangeHistory = [];
            this.syncViewToUrl();
            this.renderViewState();
            d.widgets?.forEach(w => { w._viewResults = null; });
            this.autoExecuteAllWidgets();
            this.showSuccess('Saved as the dashboard default');
        } catch (err) {
            console.error('[Dashboards] Failed to save default view:', err);
            this.showError(err.message || 'Failed to save as default');
        }
    },

    // A viewer's own view refreshes from this client at the dashboard cadence;
    // the shared view is refreshed by the server executor and arrives over SSE.
    scheduleViewRefresh() {
        this.stopViewRefresh();
        const view = this.activeView();
        if (!view || !this.currentDashboard) return;
        const range = this.currentRange();
        const interval = this.currentDashboard.refresh_interval || 0;
        if (interval === 0 || range.type === 'custom') return;
        const seconds = Math.max(10, interval > 0 ? interval : this.autoRefreshSeconds(range));
        this._viewTimer = setInterval(() => {
            if (!document.hidden && this.activeView()) this.autoExecuteAllWidgets();
        }, seconds * 1000);
    },

    stopViewRefresh() {
        if (this._viewTimer) {
            clearInterval(this._viewTimer);
            this._viewTimer = null;
        }
    },

    // Mirrors the executor's autoIntervalSeconds.
    autoRefreshSeconds(range) {
        const rel = this.parseRelative(range.type);
        if (!rel) return range.type === 'all' ? 3600 : 300;
        if (rel.ms <= 3600000) return 30;
        if (rel.ms <= 86400000) return 300;
        if (rel.ms <= 604800000) return 1800;
        return 3600;
    },

    // =====================
    // Time picker
    // =====================

    TIME_PRESETS: ['last15m', 'last1h', 'last4h', 'last12h', 'last24h', 'last7d', 'last30d', 'all'],

    toggleTimePanel() {
        const panel = document.getElementById('dashboardTimePanel');
        if (!panel) return;
        if (panel.style.display === 'block') this.closeTimePanel();
        else this.openTimePanel();
    },

    openTimePanel() {
        const panel = document.getElementById('dashboardTimePanel');
        const backdrop = document.getElementById('dashboardTimeBackdrop');
        if (!panel || !this.currentDashboard) return;
        const range = this.currentRange();
        const view = this.activeView();
        const rel = this.parseRelative(range.type);
        const resolved = this.resolveRange(range);
        const fmtIn = (iso) => (window.TZ ? TZ.formatInput(iso) : iso);
        const esc = Utils.escapeHtml;
        const unitOpt = (u, name) => `<option value="${u}"${rel && rel.unit === u ? ' selected' : ''}>${name}</option>`;

        panel.innerHTML = `
            <div class="tp-section-label">Quick ranges</div>
            <div class="tp-presets">
                ${this.TIME_PRESETS.map(t => `<button class="tp-preset${range.type === t ? ' active' : ''}" data-range="${t}" type="button">${t === 'all' ? 'All' : t.slice(4)}</button>`).join('')}
            </div>
            <div class="tp-divider"></div>
            <div class="tp-section-label">Relative</div>
            <div class="tp-relative-row">
                <span class="tp-rel-prefix">Last</span>
                <input type="number" id="dtpRelN" class="tp-num-input" value="${rel ? rel.n : 4}" min="1" max="99999">
                <select id="dtpRelUnit" class="tp-unit-select">
                    ${unitOpt('m', 'minutes')}${unitOpt('h', 'hours')}${unitOpt('d', 'days')}${unitOpt('w', 'weeks')}
                </select>
                <button id="dtpRelApply" class="tp-apply-btn" type="button">Apply</button>
            </div>
            <div class="tp-divider"></div>
            <div class="tp-section-label">Absolute range <span class="tp-zone-tag">${esc(window.TZ ? TZ.abbrev() : 'UTC')}</span></div>
            <div class="tp-absolute-col">
                <div class="tp-abs-row">
                    <span class="tp-abs-label">From</span>
                    <input type="text" id="dtpAbsStart" class="tp-abs-input" placeholder="YYYY-MM-DD HH:MM" spellcheck="false" value="${esc(fmtIn(resolved.start))}">
                </div>
                <div class="tp-abs-row">
                    <span class="tp-abs-label">To</span>
                    <input type="text" id="dtpAbsEnd" class="tp-abs-input" placeholder="YYYY-MM-DD HH:MM" spellcheck="false" value="${esc(fmtIn(resolved.end))}">
                </div>
                <button id="dtpAbsApply" class="tp-apply-btn tp-abs-apply" type="button">Apply</button>
            </div>
            <div class="tp-divider"></div>
            <div class="dtp-default">
                <span>Default: ${esc(this.rangeLabel(this.defaultRange()))}</span>
                ${view && view.range ? '<button id="dtpUseDefault" class="dtp-link" type="button">Use default</button>' : ''}
            </div>
        `;

        const apply = (range) => { this.closeTimePanel(); this.setViewRange(range); };
        panel.querySelectorAll('[data-range]').forEach(btn => {
            btn.addEventListener('click', () => apply({ type: btn.dataset.range }));
        });
        const relApply = () => {
            const n = parseInt(document.getElementById('dtpRelN')?.value, 10);
            const unit = document.getElementById('dtpRelUnit')?.value || 'h';
            if (n > 0 && n <= 99999) apply({ type: `last${n}${unit}` });
        };
        document.getElementById('dtpRelApply')?.addEventListener('click', relApply);
        document.getElementById('dtpRelN')?.addEventListener('keydown', e => { if (e.key === 'Enter') relApply(); });
        const absApply = () => {
            const parse = (id) => {
                const raw = (document.getElementById(id)?.value || '').trim();
                const ms = raw && window.TZ ? TZ.parseWallClock(raw) : NaN;
                return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
            };
            const start = parse('dtpAbsStart');
            const end = parse('dtpAbsEnd');
            if (!start || !end || start >= end) { this.showError('Enter a start before the end, as YYYY-MM-DD HH:MM'); return; }
            apply({ type: 'custom', start, end });
        };
        document.getElementById('dtpAbsApply')?.addEventListener('click', absApply);
        ['dtpAbsStart', 'dtpAbsEnd'].forEach(id => document.getElementById(id)?.addEventListener('keydown', e => { if (e.key === 'Enter') absApply(); }));
        document.getElementById('dtpUseDefault')?.addEventListener('click', () => apply(null));

        panel.style.display = 'block';
        if (backdrop) backdrop.style.display = 'block';
        document.getElementById('dashboardTimeBtn')?.classList.add('active');
    },

    closeTimePanel() {
        const panel = document.getElementById('dashboardTimePanel');
        const backdrop = document.getElementById('dashboardTimeBackdrop');
        if (panel) panel.style.display = 'none';
        if (backdrop) backdrop.style.display = 'none';
        document.getElementById('dashboardTimeBtn')?.classList.remove('active');
    },

    // Double the window. Rolling ranges stay rolling; absolute ones widen
    // around their centre without running past now.
    zoomOut() {
        const range = this.currentRange();
        const rel = this.parseRelative(range.type);
        if (rel) {
            this.setViewRange({ type: this.relativeType(rel.ms * 2) });
            return;
        }
        if (range.type !== 'custom') return;
        const r = this.resolveRange(range);
        const s = new Date(r.start).getTime(), e = new Date(r.end).getTime();
        const span = e - s;
        const end = Math.min(Date.now(), e + span / 2);
        const start = end - span * 2;
        this.setViewRange({ type: 'custom', start: new Date(start).toISOString(), end: new Date(end).toISOString() });
    },

    relativeType(ms) {
        for (const unit of ['w', 'd', 'h', 'm']) {
            const size = this.RANGE_UNITS[unit];
            if (ms % size === 0 && ms / size <= 99999) return `last${ms / size}${unit}`;
        }
        const minutes = Math.round(ms / 60000);
        return minutes <= 99999 ? `last${minutes}m` : 'all';
    },

    // =====================
    // Helpers
    // =====================

    // A range is {type: 'all' | 'lastN{m,h,d,w}'} or {type: 'custom', start, end}.
    RANGE_UNITS: { m: 60000, h: 3600000, d: 86400000, w: 604800000 },

    parseRelative(type) {
        const m = /^last([1-9]\d{0,4})([mhdw])$/.exec(type || '');
        if (!m) return null;
        const n = parseInt(m[1], 10);
        return { n, unit: m[2], ms: n * this.RANGE_UNITS[m[2]] };
    },

    defaultRange() {
        const d = this.currentDashboard || {};
        if (d.time_range_type === 'custom') {
            return { type: 'custom', start: d.time_range_start, end: d.time_range_end };
        }
        return { type: d.time_range_type || 'last24h' };
    },

    currentRange() {
        const view = this.activeView();
        return (view && view.range) || this.defaultRange();
    },

    sameRange(a, b) {
        if (!a || !b || a.type !== b.type) return false;
        if (a.type !== 'custom') return true;
        return new Date(a.start).getTime() === new Date(b.start).getTime() &&
            new Date(a.end).getTime() === new Date(b.end).getTime();
    },

    // Mirrors the executor's computeTimeRange.
    resolveRange(range) {
        const now = Date.now();
        const iso = (ms) => new Date(ms).toISOString();
        if (range && range.type === 'custom' && range.start && range.end) {
            return { start: range.start, end: range.end };
        }
        if (range && range.type === 'all') return { start: '2000-01-01T00:00:00.000Z', end: iso(now) };
        const rel = this.parseRelative(range && range.type);
        return { start: iso(now - (rel ? rel.ms : 86400000)), end: iso(now) };
    },

    // The window the viewer is looking at (pivots forward it).
    getDashboardTimeRange() {
        return this.resolveRange(this.currentRange());
    },

    rangeLabel(range) {
        if (!range) return '';
        if (range.type === 'all') return 'All time';
        if (range.type === 'custom') {
            if (!range.start || !range.end) return 'Custom range';
            return `${this.formatDate(range.start)} to ${this.formatDate(range.end)}`;
        }
        const rel = this.parseRelative(range.type);
        return rel ? `Last ${rel.n}${rel.unit}` : range.type;
    },

    formatDate(dateStr) {
        if (!dateStr) return '';
        try {
            return TZ.format(dateStr, 'friendly');
        } catch {
            return dateStr;
        }
    },

    showError(msg) {
        if (window.Toast) {
            Toast.show(msg, 'error');
        } else {
            console.error('[Dashboards]', msg);
        }
    },

    showSuccess(msg) {
        if (window.Toast) {
            Toast.show(msg, 'success');
        }
    },

    async exportDashboard(dashboardId) {
        try {
            const response = await fetch(`/api/v1/dashboards/${dashboardId}/export`, {
                credentials: 'include'
            });
            if (!response.ok) throw new Error('Failed to export dashboard');

            const blob = await response.blob();
            const disposition = response.headers.get('Content-Disposition') || '';
            const match = disposition.match(/filename="(.+?)"/);
            const filename = match ? match[1] : 'dashboard.yaml';

            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            this.showSuccess('Dashboard exported');
        } catch (err) {
            console.error('[Dashboards] Export failed:', err);
            this.showError('Failed to export dashboard');
        }
    },

    importDashboard() {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.yaml,.yml';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;

            try {
                const text = await file.text();
                const response = await fetch('/api/v1/dashboards/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'text/yaml' },
                    credentials: 'include',
                    body: text
                });

                const data = await response.json();
                if (!data.success) throw new Error(data.error || 'Import failed');

                this.showSuccess('Dashboard imported successfully');
                this.loadDashboards();
            } catch (err) {
                console.error('[Dashboards] Import failed:', err);
                this.showError('Failed to import dashboard: ' + err.message);
            }
        };
        input.click();
    },

    // =====================
    // Variables
    // =====================

    // Variables are auto-detected from widget queries (no manual add). The manager
    // owns the default values; currentDashboard.variables mirrors them for
    // persistence and server-side substitution.
    ensureVarManager() {
        if (this.varManager) return this.varManager;
        if (!window.VariableManager) return null;
        this.varManager = new VariableManager({
            container: 'dashboardVariables',
            // Values typed here are this viewer's own; the stored defaults only
            // change through "Save as default".
            overlayEditable: true,
            onChange: (name, value) => this.setViewVar(name, value),
            onOverlayClear: (name) => this.clearViewVar(name),
        });
        return this.varManager;
    },

    // editorVariables returns the @var bindings referenced in an editor's text,
    // valued from the current manager (default "*"), so live validation of a
    // widget query does not flag a freshly-typed @var as a syntax error.
    editorVariables(text) {
        if (!window.VariableManager) return [];
        const mgr = this.varManager;
        return VariableManager.detectNames(text).map(name => ({
            name,
            value: mgr && mgr.getValue(name) != null ? mgr.getValue(name) : '*'
        }));
    },

    renderVariablesBar() {
        const mgr = this.ensureVarManager();
        if (!mgr) return;
        // Seed remembered values, then reconcile against the current widget set so
        // newly-typed @vars appear and orphaned ones drop.
        mgr.load((this.currentDashboard && this.currentDashboard.variables) || []);
        this.syncDashboardVariables();
    },

    // syncDashboardVariables reconciles the variable set against every widget
    // query. When the set changes it mirrors back to currentDashboard.variables
    // and persists (so the executor substitutes the same set server-side).
    // persist defaults true for local edits; remote (SSE) edits pass false since
    // the originating editor already persisted the set. Returns the persistence
    // promise so callers can await it before executing (the execute endpoint
    // substitutes from the STORED variable set, so the PUT must land first).
    syncDashboardVariables(persist = true) {
        const mgr = this.ensureVarManager();
        if (!mgr || !this.currentDashboard) return Promise.resolve();
        // Guard against an unloaded dashboard: a missing widgets array (vs an
        // explicit []) would otherwise prune every stored variable and PUT [].
        if (!this.currentDashboard.widgets) return Promise.resolve();
        const queries = this.currentDashboard.widgets.map(w => w.query_content || '');
        if (mgr.syncFromText(queries)) {
            this.currentDashboard.variables = mgr.serialize();
            if (persist) return this.saveVariables().catch(err => console.error('[Dashboards] Failed to save variables:', err));
        }
        return Promise.resolve();
    },

    async saveVariables() {
        if (!this.currentDashboard) return;
        const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/variables`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ variables: this.currentDashboard.variables || [] })
        });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || data.success === false) throw new Error(data.error || 'Failed to save variables');
    }
};

window.Dashboards = Dashboards;

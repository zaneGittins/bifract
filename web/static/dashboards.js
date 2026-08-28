/**
 * Dashboards Frontend Module
 * Grid-based dashboards with draggable, resizable query widgets.
 * Opening a dashboard paints the last saved results immediately, then refreshes
 * any widget whose cache has aged past the dashboard's refresh cadence.
 */

const Dashboards = {
    currentDashboard: null,
    varManager: null,        // VariableManager for the dashboard's @vars
    currentPage: 0,
    pageSize: 20,
    totalDashboards: 0,
    searchQuery: '',

    // Drag/resize state
    dragState: null,
    resizeState: null,
    presenceInterval: null,
    eventSource: null,
    sseClientId: null,

    // Grid config: 12 columns, row height in px
    GRID_COLS: 12,
    ROW_HEIGHT: 130,
    MIN_WIDTH: 2,
    MIN_HEIGHT: 2,

    // Cache age below which an opened widget is not re-executed when the
    // dashboard has auto-refresh off. Matches the executor's MinInterval floor.
    MIN_CACHE_FRESH_MS: 10000,

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
    },

    onFractalChange() {
        this._clearStoredDrilldown(this.currentDashboard && this.currentDashboard.id);
        this.currentDashboard = null;
        this._drilldown = null;
        this.stopDragResize();
        this.stopUpdatedAtTicker();
        this.currentPage = 0;
        this.searchQuery = '';
        const tbody = document.getElementById('dashboardsTableBody');
        if (tbody) tbody.innerHTML = '';

        const view = document.getElementById('dashboardsView');
        if (view && view.offsetParent !== null) {
            this.showDashboardListing();
        }
    },

    bindEvents() {
        this.unbindEvents();

        const createBtn = document.getElementById('createDashboardBtn');
        if (createBtn) {
            createBtn._dashHandler = () => this.showCreateDashboardModal();
            createBtn.addEventListener('click', createBtn._dashHandler);
        }


        const searchInput = document.getElementById('dashboardSearchInput');
        if (searchInput) {
            searchInput._dashHandler = (e) => {
                this.searchQuery = e.target.value;
                this.currentPage = 0;
                this.loadDashboards();
            };
            searchInput.addEventListener('input', searchInput._dashHandler);
        }

        const prevBtn = document.getElementById('dashboardsPrevBtn');
        if (prevBtn) {
            prevBtn._dashHandler = () => {
                if (this.currentPage > 0) { this.currentPage--; this.loadDashboards(); }
            };
            prevBtn.addEventListener('click', prevBtn._dashHandler);
        }

        const nextBtn = document.getElementById('dashboardsNextBtn');
        if (nextBtn) {
            nextBtn._dashHandler = () => {
                const maxPage = Math.ceil(this.totalDashboards / this.pageSize) - 1;
                if (this.currentPage < maxPage) { this.currentPage++; this.loadDashboards(); }
            };
            nextBtn.addEventListener('click', nextBtn._dashHandler);
        }

        const addWidgetBtn = document.getElementById('addWidgetBtn');
        if (addWidgetBtn) {
            addWidgetBtn._dashHandler = () => this.addWidget();
            addWidgetBtn.addEventListener('click', addWidgetBtn._dashHandler);
        }

        const deleteDashboardBtn = document.getElementById('deleteDashboardBtn');
        if (deleteDashboardBtn) {
            deleteDashboardBtn._dashHandler = () => this.deleteDashboard();
            deleteDashboardBtn.addEventListener('click', deleteDashboardBtn._dashHandler);
        }

        const timeRangeBtn = document.getElementById('dashboardTimeRangeBtn');
        if (timeRangeBtn) {
            timeRangeBtn._dashHandler = () => this.showTimeRangeModal();
            timeRangeBtn.addEventListener('click', timeRangeBtn._dashHandler);
        }

        const shareBtn = document.getElementById('dashboardShareBtn');
        if (shareBtn) {
            shareBtn._dashHandler = () => this.showShareModal();
            shareBtn.addEventListener('click', shareBtn._dashHandler);
        }

        const refreshSelect = document.getElementById('dashboardRefreshSelect');
        if (refreshSelect) {
            refreshSelect._dashHandler = () => this.updateRefreshInterval(parseInt(refreshSelect.value, 10));
            refreshSelect.addEventListener('change', refreshSelect._dashHandler);
        }
    },

    unbindEvents() {
        const ids = [
            'createDashboardBtn', 'dashboardSearchInput',
            'dashboardsPrevBtn', 'dashboardsNextBtn',
            'addWidgetBtn', 'deleteDashboardBtn', 'dashboardTimeRangeBtn',
            'dashboardShareBtn', 'dashboardRefreshSelect'
        ];
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (el && el._dashHandler) {
                el.removeEventListener('click', el._dashHandler);
                el.removeEventListener('input', el._dashHandler);
                el.removeEventListener('change', el._dashHandler);
                delete el._dashHandler;
            }
        });
    },

    // =====================
    // Listing
    // =====================

    showDashboardListing() {
        this.stopPresenceTracking();
        this.stopUpdatedAtTicker();
        this._clearStoredDrilldown(this.currentDashboard && this.currentDashboard.id);
        this._drilldown = null;
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
                <td>${Utils.escapeHtml(d.time_range_type || '')}</td>
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

    async openDashboard(id) {
        try {
            // Leaving a different dashboard: forget its transient drilldown so it
            // never resurfaces on a later normal open. A reload of the SAME
            // dashboard keeps it (currentDashboard is null on a fresh page load),
            // so a refresh still restores the drilldown view.
            if (this.currentDashboard && this.currentDashboard.id !== id) {
                this._clearStoredDrilldown(this.currentDashboard.id);
            }
            window.App?.pushSubPath(id);
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

            const refreshSelect = document.getElementById('dashboardRefreshSelect');
            if (refreshSelect) refreshSelect.value = String(this.currentDashboard.refresh_interval ?? 0);

            this.updateShareButtonVisibility();
            this.renderVariablesBar();
            this.renderDashboardGrid();
            this._resolveDrilldown();
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
            case 'widget_layout_updated':
                this.onRemoteWidgetLayoutUpdated(event.data);
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
            // Brief highlight
            el.style.transition = 'box-shadow 0.5s ease';
            el.style.boxShadow = '0 0 0 2px var(--accent-primary)';
            setTimeout(() => { el.style.boxShadow = ''; }, 1500);
            grid.appendChild(el);
            this.expandGridIfNeeded();
            this.initDragAndDrop();
            this.executeWidget(widget.id);
        }
        this.syncDashboardVariables(false);
    },

    onRemoteWidgetRemoved(data) {
        if (!this.currentDashboard) return;
        const widgetId = data.id;
        this.currentDashboard.widgets = this.currentDashboard.widgets.filter(w => w.id !== widgetId);
        const el = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
        if (el) { this.destroyWidgetVisuals(el); el.remove(); }
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
        // During a private drilldown, ignore broadcast shared results so they
        // don't clobber this viewer's filtered view. Exiting re-runs the shared
        // queries, so the cache refreshes then. Read through activeDrilldown() so a
        // transiently-nulled in-memory flag cannot let a default-variable broadcast
        // (one per pod in a multi-replica deploy) slip past and revert the view.
        if (this.activeDrilldown()) return;
        const widget = this.currentDashboard.widgets.find(w => w.id === data.id);
        if (!widget) return;

        // Skip if user is editing this widget
        const contentEl = document.getElementById(`wc-${data.id}`);
        if (contentEl && contentEl._editingWidget) return;

        if (data.last_results) widget.last_results = data.last_results;
        if (data.chart_type) widget.chart_type = data.chart_type;
        if (data.last_executed_at) widget.last_executed_at = data.last_executed_at;

        // Re-render results
        try {
            const resultData = JSON.parse(widget.last_results);
            this.renderWidgetResults(data.id, resultData);
            this.renderUpdatedAt();
        } catch (_) {}
    },

    onRemoteWidgetLayoutUpdated(data) {
        if (!this.currentDashboard) return;
        const widget = this.currentDashboard.widgets.find(w => w.id === data.id);
        if (!widget) return;

        // Skip if user is currently dragging/resizing this widget
        if ((this.dragState && this.dragState.widgetId === data.id) ||
            (this.resizeState && this.resizeState.widgetId === data.id)) return;

        widget.pos_x = data.pos_x;
        widget.pos_y = data.pos_y;
        widget.width = data.width;
        widget.height = data.height;

        const el = document.querySelector(`.dashboard-widget[data-widget-id="${data.id}"]`);
        if (el) {
            const grid = document.getElementById('dashboardGrid');
            const containerWidth = grid ? grid.offsetWidth : window.innerWidth - 40;
            const colWidth = containerWidth / this.GRID_COLS;
            el.style.left = `${data.pos_x * colWidth}px`;
            el.style.top = `${data.pos_y * this.ROW_HEIGHT}px`;
            el.style.width = `${data.width * colWidth}px`;
            el.style.height = `${data.height * this.ROW_HEIGHT}px`;
        }

        this.expandGridIfNeeded();
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

        if (!this.currentDashboard || !this.currentDashboard.widgets) return;

        // Calculate grid height needed
        const maxBottom = this.currentDashboard.widgets.reduce((max, w) => {
            return Math.max(max, w.pos_y + w.height);
        }, 6);
        grid.style.minHeight = `${maxBottom * this.ROW_HEIGHT + 40}px`;

        this.currentDashboard.widgets.forEach(widget => {
            const el = this.createWidgetElement(widget);
            grid.appendChild(el);
        });

        this.initDragAndDrop();
    },

    createWidgetElement(widget) {
        const grid = document.getElementById('dashboardGrid');
        const containerWidth = grid ? grid.offsetWidth : window.innerWidth - 40;
        const colWidth = containerWidth / this.GRID_COLS;

        const el = document.createElement('div');
        el.className = 'dashboard-widget';
        el.dataset.widgetId = widget.id;

        el.style.left = `${widget.pos_x * colWidth}px`;
        el.style.top = `${widget.pos_y * this.ROW_HEIGHT}px`;
        el.style.width = `${widget.width * colWidth}px`;
        el.style.height = `${widget.height * this.ROW_HEIGHT}px`;

        const title = widget.title || 'Widget';

        el.innerHTML = `
            <div class="widget-header" data-widget-id="${widget.id}" title="Double-click to edit">
                <span class="widget-title">${Utils.escapeHtml(title)}</span>
                <div class="widget-actions">
                    <button class="widget-btn widget-execute-btn" title="Re-execute" onclick="Dashboards.executeWidget('${widget.id}')">&#9654;</button>
                    <div class="widget-kebab-wrapper">
                        <button class="widget-btn widget-kebab-btn" title="More options" onclick="Dashboards.toggleWidgetKebab('${widget.id}', event)">&#x22EE;</button>
                        <div class="widget-kebab-menu" id="widget-kebab-menu-${widget.id}">
                            <button onclick="Dashboards.showInlineWidgetEdit('${widget.id}')">Edit</button>
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
            <div class="widget-resize-handle" data-widget-id="${widget.id}"></div>
        `;

        return el;
    },

    toggleWidgetKebab(widgetId, event) {
        event.stopPropagation();
        const menu = document.getElementById(`widget-kebab-menu-${widgetId}`);
        if (!menu) return;
        const isOpen = menu.classList.contains('open');
        document.querySelectorAll('.widget-kebab-menu.open').forEach(m => m.classList.remove('open'));
        if (!isOpen) menu.classList.add('open');
    },

    // =====================
    // Auto-execute on open
    // =====================

    autoExecuteAllWidgets(skipFresh = false) {
        if (!this.currentDashboard || !this.currentDashboard.widgets) return;
        this.currentDashboard.widgets.forEach(widget => {
            if (skipFresh && this.isCacheFresh(widget)) return;
            this.executeWidget(widget.id);
        });
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
        if (!this.currentDashboard || !this.currentDashboard.widgets || this.activeDrilldown()) return clear();

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
        if (this.activeDrilldown()) return false;
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
        if (this.activeDrilldown()) return;
        this.currentDashboard.widgets.forEach(w => this.renderWidgetFromCache(w.id));
    },

    async executeWidget(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId)
            : null;
        if (!widget) return;

        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (contentEl && contentEl._editingWidget) return;

        // Results already on screen (cached or from a prior run) stay visible and
        // are refreshed in place; only an empty widget shows a loading state.
        const widgetEl = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
        const hasRendered = !!(contentEl && contentEl.dataset.rendered === '1');
        if (contentEl && !hasRendered) {
            this.destroyWidgetVisuals(contentEl);
            contentEl.innerHTML = '<div class="widget-loading">Executing...</div>';
        }
        if (widgetEl && hasRendered) widgetEl.classList.add('refreshing');

        const execBtn = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"] .widget-execute-btn`);
        if (execBtn) { execBtn.innerHTML = '<span class="spinner"></span>'; execBtn.disabled = true; }

        try {
            // Execution runs server-side against the dashboard's stored scope,
            // time range and variables. The backend persists the results as the
            // authoritative cache and pushes them to other viewers over SSE; the
            // direct response lets this client render immediately.
            // In a pivot drilldown the run is a private, transient view: send the
            // override variables/time so the server executes (but does not persist
            // or broadcast) a filtered result for this viewer only.
            const dd = this.activeDrilldown();
            const body = dd ? JSON.stringify({
                preview: true,
                variables: dd.vars || [],
                time_range_start: dd.start,
                time_range_end: dd.end
            }) : undefined;
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}/execute`, {
                method: 'POST',
                headers: this.sseHeaders(),
                credentials: 'include',
                body
            });
            const data = await response.json();

            if (!data.success) throw new Error(data.error || 'Query failed');

            const resultData = data.data || {};
            resultData.results = resultData.results || [];
            resultData.chart_type = resultData.chart_type || data.chart_type || 'table';
            resultData.chart_config = resultData.chart_config || {};
            resultData.field_order = resultData.field_order || [];

            // Update widget in local state
            widget.last_results = JSON.stringify(resultData);
            widget.last_executed_at = new Date().toISOString();
            if (resultData.chart_type) widget.chart_type = resultData.chart_type;

            this.renderWidgetResults(widgetId, resultData);
            this.renderUpdatedAt();
        } catch (err) {
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
            if (widgetEl) widgetEl.classList.remove('refreshing');
            if (execBtn) { execBtn.innerHTML = '&#9654;'; execBtn.disabled = false; }
        }
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
            setTimeout(() => {
                const el = document.getElementById(graphId);
                if (el) el._visNetwork = BifractCharts.renderGraphSimple(el, {
                    data: results.results || [],
                    fields: results.field_order,
                    config: results.chart_config || {}
                });
            }, 300);
            return graphHtml;
        }

        if (chartType === 'mesh') {
            const meshId = `dmesh-${chartId}`;
            const meshHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                    <div id="${meshId}" class="widget-visual-host" style="width:100%;height:100%;"></div>
                </div>
            `;
            setTimeout(() => {
                const el = document.getElementById(meshId);
                if (el) el._visNetwork = BifractCharts.renderMeshSimple(el, {
                    data: results.results || [],
                    fields: results.field_order,
                    config: results.chart_config || {},
                    onDataClick: (ctx, ev) => this.onWidgetDataClick(widgetId, ctx, ev)
                });
            }, 300);
            return meshHtml;
        }

        if (chartType === 'heatmap') {
            const heatmapId = `dheatmap-${chartId}`;
            const heatmapHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;overflow:auto;">
                    <div id="${heatmapId}" style="width:100%;overflow:auto;"></div>
                </div>
            `;
            setTimeout(() => {
                const el = document.getElementById(heatmapId);
                if (el) BifractCharts.renderHeatmap(el, {
                    data: results.results || [],
                    config: results.chart_config || {}
                });
            }, 300);
            return heatmapHtml;
        }

        if (chartType === 'mitre') {
            const mitreId = `dmitre-${chartId}`;
            const mitreHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;overflow:auto;">
                    <div id="${mitreId}" class="mtr-host widget-visual-host" style="height:100%;"></div>
                </div>
            `;
            setTimeout(() => {
                const el = document.getElementById(mitreId);
                // embedded: a wallboard panel opens on what fired, not on 700 empty cells.
                if (el && window.BifractMitreMatrix) BifractMitreMatrix.render(el, {
                    rows: results.results || [],
                    config: results.chart_config || {},
                    embedded: true
                });
            }, 300);
            return mitreHtml;
        }

        if (chartType === 'worldmap') {
            const mapId = `dmap-${chartId}`;
            const mapHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                    <div id="${mapId}" class="worldmap-container widget-visual-host" style="height:100%;"></div>
                </div>
            `;
            setTimeout(() => {
                const el = document.getElementById(mapId);
                if (el && window.BifractWorldMap) {
                    const cfg = results.chart_config || {};
                    el._leafletMap = BifractWorldMap.render(el, results.results || [], {
                        latField: cfg.latField || 'latitude',
                        lonField: cfg.lonField || 'longitude',
                        labelField: cfg.labelField || null
                    });
                }
            }, 300);
            return mapHtml;
        }

        const chartHtml = `
            <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                <canvas id="${chartId}" style="background:transparent;border-radius:4px;"></canvas>
            </div>
        `;

        setTimeout(() => {
            this.renderChartOnCanvas(chartId, results, widgetConfig, widgetId);
        }, 300);

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

    // Apply a brushed time span as the dashboard's custom range and refresh all
    // widgets. Mirrors saveTimeRange but for an explicit start/end.
    async applyBrushTimeRange(startISO, endISO) {
        if (!this.currentDashboard) return;
        try {
            const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ time_range_type: 'custom', time_range_start: startISO, time_range_end: endISO })
            });
            if (!resp.ok) throw new Error('Failed to save time range');
            this.currentDashboard.time_range_type = 'custom';
            this.currentDashboard.time_range_start = startISO;
            this.currentDashboard.time_range_end = endISO;
            this.autoExecuteAllWidgets();
            this.showSuccess(`Zoomed to ${this.formatDate(startISO)} - ${this.formatDate(endISO)}`);
        } catch (err) {
            console.error('[Dashboards] Failed to apply brushed time range:', err);
            this.showError('Failed to apply time selection');
        }
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
        const systemFields = ['_all_fields', 'raw_log', 'log_id'];
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
    // Drag and Resize
    // =====================

    initDragAndDrop() {
        this.stopDragResize();

        const grid = document.getElementById('dashboardGrid');
        if (!grid) return;

        grid.addEventListener('mousedown', this._onMouseDown = (e) => {
            const header = e.target.closest('.widget-header');
            const resizeHandle = e.target.closest('.widget-resize-handle');
            const btn = e.target.closest('button');

            if (btn) return;

            if (resizeHandle) {
                this.startResize(e, resizeHandle.dataset.widgetId);
            } else if (header) {
                this.startDrag(e, header.dataset.widgetId);
            }
        });

        // Double-click header to edit widget
        grid.addEventListener('dblclick', this._onDblClick = (e) => {
            const header = e.target.closest('.widget-header');
            if (!header) return;
            const btn = e.target.closest('button');
            if (btn) return;
            this.showInlineWidgetEdit(header.dataset.widgetId);
        });
    },

    startDrag(e, widgetId) {
        e.preventDefault();
        const grid = document.getElementById('dashboardGrid');
        const widgetEl = grid.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
        if (!widgetEl) return;

        const rect = widgetEl.getBoundingClientRect();
        const gridRect = grid.getBoundingClientRect();
        const colWidth = grid.offsetWidth / this.GRID_COLS;

        this.dragState = {
            widgetId,
            widgetEl,
            startMouseX: e.clientX,
            startMouseY: e.clientY,
            startLeft: rect.left - gridRect.left,
            startTop: rect.top - gridRect.top,
            colWidth,
            gridRect
        };

        widgetEl.classList.add('dragging');

        document.addEventListener('mousemove', this._onMouseMove = (e) => this.onDragMove(e));
        document.addEventListener('mouseup', this._onMouseUp = (e) => this.onDragEnd(e));
    },

    onDragMove(e) {
        if (!this.dragState) return;
        const ds = this.dragState;
        const dx = e.clientX - ds.startMouseX;
        const dy = e.clientY - ds.startMouseY;

        const newLeft = Math.max(0, ds.startLeft + dx);
        const newTop = Math.max(0, ds.startTop + dy);

        ds.widgetEl.style.left = `${newLeft}px`;
        ds.widgetEl.style.top = `${newTop}px`;
    },

    onDragEnd(_e) {
        if (!this.dragState) return;
        const ds = this.dragState;
        ds.widgetEl.classList.remove('dragging');

        const colWidth = ds.colWidth;
        const left = parseFloat(ds.widgetEl.style.left);
        const top = parseFloat(ds.widgetEl.style.top);

        const widget = this.currentDashboard.widgets.find(w => w.id === ds.widgetId);
        const prevX = widget ? widget.pos_x : 0;
        const prevY = widget ? widget.pos_y : 0;

        const gridX = Math.max(0, Math.round(left / colWidth));
        const gridY = Math.max(0, Math.round(top / this.ROW_HEIGHT));
        const maxX = this.GRID_COLS - (widget ? widget.width : 1);
        let clampedX = Math.min(gridX, maxX);
        let clampedY = gridY;

        // Resolve overlap: push down until no collision
        if (widget) {
            [clampedX, clampedY] = this.resolveOverlap(ds.widgetId, clampedX, clampedY, widget.width, widget.height);
        }

        ds.widgetEl.style.left = `${clampedX * colWidth}px`;
        ds.widgetEl.style.top = `${clampedY * this.ROW_HEIGHT}px`;

        if (widget) {
            widget.pos_x = clampedX;
            widget.pos_y = clampedY;
        }

        this.saveWidgetLayout(ds.widgetId, clampedX, clampedY, widget ? widget.width : 6, widget ? widget.height : 4);

        this.dragState = null;
        document.removeEventListener('mousemove', this._onMouseMove);
        document.removeEventListener('mouseup', this._onMouseUp);

        this.expandGridIfNeeded();

        // Only re-execute if position actually changed (not a click/double-click with no movement)
        if (clampedX !== prevX || clampedY !== prevY) {
            this.executeWidget(ds.widgetId);
        }
    },

    startResize(e, widgetId) {
        e.preventDefault();
        const grid = document.getElementById('dashboardGrid');
        const widgetEl = grid.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
        if (!widgetEl) return;

        const colWidth = grid.offsetWidth / this.GRID_COLS;

        this.resizeState = {
            widgetId,
            widgetEl,
            startMouseX: e.clientX,
            startMouseY: e.clientY,
            startWidth: parseFloat(widgetEl.style.width),
            startHeight: parseFloat(widgetEl.style.height),
            colWidth
        };

        widgetEl.classList.add('resizing');

        document.addEventListener('mousemove', this._onMouseMove = (e) => this.onResizeMove(e));
        document.addEventListener('mouseup', this._onMouseUp = (e) => this.onResizeEnd(e));
    },

    onResizeMove(e) {
        if (!this.resizeState) return;
        const rs = this.resizeState;
        const dx = e.clientX - rs.startMouseX;
        const dy = e.clientY - rs.startMouseY;

        const minW = this.MIN_WIDTH * rs.colWidth;
        const minH = this.MIN_HEIGHT * this.ROW_HEIGHT;

        rs.widgetEl.style.width = `${Math.max(minW, rs.startWidth + dx)}px`;
        rs.widgetEl.style.height = `${Math.max(minH, rs.startHeight + dy)}px`;
    },

    onResizeEnd(_e) {
        if (!this.resizeState) return;
        const rs = this.resizeState;
        rs.widgetEl.classList.remove('resizing');

        const colWidth = rs.colWidth;
        const newWidth = parseFloat(rs.widgetEl.style.width);
        const newHeight = parseFloat(rs.widgetEl.style.height);

        const gridW = Math.max(this.MIN_WIDTH, Math.round(newWidth / colWidth));
        const gridH = Math.max(this.MIN_HEIGHT, Math.round(newHeight / this.ROW_HEIGHT));

        // Get current position and size
        const widget = this.currentDashboard.widgets.find(w => w.id === rs.widgetId);
        const prevW = widget ? widget.width : 6;
        const prevH = widget ? widget.height : 4;
        const maxW = this.GRID_COLS - (widget ? widget.pos_x : 0);
        const clampedW = Math.min(gridW, maxW);

        rs.widgetEl.style.width = `${clampedW * colWidth}px`;
        rs.widgetEl.style.height = `${gridH * this.ROW_HEIGHT}px`;

        if (widget) {
            widget.width = clampedW;
            widget.height = gridH;
        }

        this.saveWidgetLayout(rs.widgetId, widget ? widget.pos_x : 0, widget ? widget.pos_y : 0, clampedW, gridH);

        this.resizeState = null;
        document.removeEventListener('mousemove', this._onMouseMove);
        document.removeEventListener('mouseup', this._onMouseUp);

        this.expandGridIfNeeded();

        // Only re-execute if size actually changed
        if (clampedW !== prevW || gridH !== prevH) {
            this.executeWidget(rs.widgetId);
        }
    },

    // Returns [x, y] adjusted so the widget doesn't overlap any other widget
    resolveOverlap(widgetId, x, y, w, h) {
        const others = this.currentDashboard.widgets.filter(ww => ww.id !== widgetId);
        const overlaps = (ax, ay) => others.some(o =>
            ax < o.pos_x + o.width && ax + w > o.pos_x &&
            ay < o.pos_y + o.height && ay + h > o.pos_y
        );
        // Try the desired position; if blocked, push down row by row
        let ry = y;
        while (overlaps(x, ry)) {
            ry++;
        }
        return [x, ry];
    },

    expandGridIfNeeded() {
        if (!this.currentDashboard || !this.currentDashboard.widgets) return;
        const grid = document.getElementById('dashboardGrid');
        if (!grid) return;

        const maxBottom = this.currentDashboard.widgets.reduce((max, w) => Math.max(max, w.pos_y + w.height), 6);
        grid.style.minHeight = `${maxBottom * this.ROW_HEIGHT + 40}px`;
    },

    stopDragResize() {
        if (this._onMouseMove) document.removeEventListener('mousemove', this._onMouseMove);
        if (this._onMouseUp) document.removeEventListener('mouseup', this._onMouseUp);
        const grid = document.getElementById('dashboardGrid');
        if (grid && this._onDblClick) grid.removeEventListener('dblclick', this._onDblClick);
        if (grid && this._onMouseDown) grid.removeEventListener('mousedown', this._onMouseDown);
        this.dragState = null;
        this.resizeState = null;
    },

    async saveWidgetLayout(widgetId, posX, posY, width, height) {
        if (!this.currentDashboard) return;
        try {
            await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}/layout`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ pos_x: posX, pos_y: posY, width, height })
            });
        } catch (err) {
            console.error('[Dashboards] Failed to save widget layout:', err);
        }
    },

    // =====================
    // Widget CRUD
    // =====================

    async addWidget() {
        if (!this.currentDashboard) return;

        // Find a reasonable default position (below existing widgets)
        const maxBottom = this.currentDashboard.widgets
            ? this.currentDashboard.widgets.reduce((max, w) => Math.max(max, w.pos_y + w.height), 0)
            : 0;

        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets`, {
                method: 'POST',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({
                    title: 'New Widget',
                    query_content: '',
                    chart_type: 'table',
                    pos_x: 0,
                    pos_y: maxBottom,
                    width: 6,
                    height: 4
                })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to create widget');

            const widget = data.data;
            if (!this.currentDashboard.widgets) this.currentDashboard.widgets = [];
            this.currentDashboard.widgets.push(widget);

            // Add widget to grid
            const grid = document.getElementById('dashboardGrid');
            if (grid) {
                const el = this.createWidgetElement(widget);
                grid.appendChild(el);
                this.expandGridIfNeeded();
                this.initDragAndDrop();
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

            const widgetEl = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
            if (widgetEl) { this.destroyWidgetVisuals(widgetEl); widgetEl.remove(); }
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

    showTimeRangeModal() {
        if (!this.currentDashboard) return;

        const existing = document.getElementById('dashTimeRangeModal');
        if (existing) existing.remove();

        const modal = document.createElement('div');
        modal.id = 'dashTimeRangeModal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content" style="width:380px;max-width:95vw;">
                <div class="modal-header">
                    <h3>Time Settings</h3>
                    <button class="modal-close" onclick="document.getElementById('dashTimeRangeModal').remove()">&#x2715;</button>
                </div>
                <div class="modal-body">
                    <div class="form-group">
                        <label>Time Range</label>
                        <select id="dtrSelect" class="form-input">
                            ${this.currentDashboard.time_range_type === 'custom' ? '<option value="custom" selected disabled>Custom (brushed range)</option>' : ''}
                            <option value="last1h" ${this.currentDashboard.time_range_type === 'last1h' ? 'selected' : ''}>Last 1 Hour</option>
                            <option value="last24h" ${this.currentDashboard.time_range_type === 'last24h' ? 'selected' : ''}>Last 24 Hours</option>
                            <option value="last7d" ${this.currentDashboard.time_range_type === 'last7d' ? 'selected' : ''}>Last 7 Days</option>
                            <option value="last30d" ${this.currentDashboard.time_range_type === 'last30d' ? 'selected' : ''}>Last 30 Days</option>
                            <option value="all" ${this.currentDashboard.time_range_type === 'all' ? 'selected' : ''}>All Time</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Bucket Timezone</label>
                        <select id="dtrZone" class="form-input">${this.zoneOptionsHTML()}</select>
                        <div class="form-hint">Where day, hour and week boundaries fall for bucket() and timechart. It belongs to the dashboard so every viewer reads the same buckets. Individual timestamps still follow each viewer's own zone.</div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-secondary" onclick="document.getElementById('dashTimeRangeModal').remove()">Cancel</button>
                    <button class="btn-primary" onclick="Dashboards.saveTimeRange()">Apply &amp; Refresh</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
        modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
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

    async saveTimeRange() {
        if (!this.currentDashboard) return;
        const val = document.getElementById('dtrSelect')?.value;
        const zone = document.getElementById('dtrZone')?.value;

        const body = {};
        // "custom" is the disabled placeholder for a brushed range, so seeing it
        // selected means the range was not touched.
        if (val && val !== 'custom' && val !== this.currentDashboard.time_range_type) {
            body.time_range_type = val;
        }
        if (zone && zone !== (this.currentDashboard.timezone || 'UTC')) {
            body.timezone = zone;
        }
        if (!Object.keys(body).length) {
            document.getElementById('dashTimeRangeModal')?.remove();
            return;
        }

        try {
            const resp = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}`, {
                method: 'PUT',
                headers: this.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to save');

            if (body.time_range_type) this.currentDashboard.time_range_type = body.time_range_type;
            if (body.timezone) this.currentDashboard.timezone = body.timezone;
            document.getElementById('dashTimeRangeModal')?.remove();

            if (body.time_range_type) {
                this.autoExecuteAllWidgets();
            } else {
                // A zone change re-ran every widget server-side before this
                // response returned, so the fresh results are already cached.
                // Reload them instead of paying for the same queries twice.
                await this.reloadCachedResults();
            }
        } catch (err) {
            console.error('[Dashboards] Failed to save time settings:', err);
            this.showError(err.message || 'Failed to save time settings');
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

    // Returns true when cached results were rendered, so callers can fall back.
    renderWidgetFromCache(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
        if (!widget || !widget.last_results) return false;
        let resultData;
        try {
            resultData = typeof widget.last_results === 'string' ? JSON.parse(widget.last_results) : widget.last_results;
        } catch (e) { return false; }
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

    // A drilldown is a transient, per-viewer override that must survive the whole
    // time the user is looking at the target dashboard. It is NOT persisted
    // server-side (the server only ever knows the dashboard's default variables),
    // so its entire lifetime lives client-side. The in-memory flag alone is
    // fragile: a spurious scope notify, a re-entrant route, or an SSE reconnect can
    // null it, and the moment it is null a default-variable result (pushed by the
    // background executor, one per pod in a multi-replica deploy) overwrites the
    // filtered view. To make it authoritative we mirror it into sessionStorage
    // keyed by dashboard id (per-tab, so a new-tab drilldown never collides with or
    // clobbers another tab), and read through activeDrilldown() everywhere the
    // override gates behavior. The stored context is cleared only on an explicit
    // exit, a real scope change, or returning to the listing.
    _drilldownStoreKey(id) { return 'bifract_drilldown_' + id; },

    _storeDrilldown(dd) {
        if (!dd || !dd.dashboardId) return;
        try { sessionStorage.setItem(this._drilldownStoreKey(dd.dashboardId), JSON.stringify(dd)); }
        catch (e) { /* sessionStorage unavailable: fall back to in-memory only */ }
    },

    _loadStoredDrilldown(id) {
        if (!id) return null;
        try {
            const raw = sessionStorage.getItem(this._drilldownStoreKey(id));
            if (!raw) return null;
            const dd = JSON.parse(raw);
            return (dd && Array.isArray(dd.vars) && dd.dashboardId === id) ? dd : null;
        } catch (e) { return null; }
    },

    _clearStoredDrilldown(id) {
        if (!id) return;
        try { sessionStorage.removeItem(this._drilldownStoreKey(id)); } catch (e) { /* ignore */ }
    },

    // The authoritative drilldown for the dashboard currently on screen. Prefers the
    // in-memory context but transparently rehydrates from sessionStorage when a
    // transient reset has nulled it, so default-variable results can never win a
    // race against the override view.
    activeDrilldown() {
        const id = this.currentDashboard && this.currentDashboard.id;
        if (!id) return null;
        if (this._drilldown && this._drilldown.dashboardId === id) return this._drilldown;
        const stored = this._loadStoredDrilldown(id);
        if (stored) { this._drilldown = stored; return stored; }
        return null;
    },

    // Enter a transient drilldown on a (possibly different) dashboard. Same-board
    // drilldowns re-run in place; cross-board ones stash the context and open the
    // target, where _resolveDrilldown picks it up after load.
    enterDrilldown(targetId, dd) {
        if (targetId && this.currentDashboard && targetId !== this.currentDashboard.id) {
            this._pendingDrilldown = dd;
            this.openDashboard(targetId);
            return;
        }
        dd.dashboardId = this.currentDashboard && this.currentDashboard.id;
        this._drilldown = dd;
        this._storeDrilldown(dd);
        this.renderDrilldownBanner();
        this.autoExecuteAllWidgets();
    },

    exitDrilldown() {
        const active = this.activeDrilldown();
        if (!active) return;
        this._clearStoredDrilldown(this.currentDashboard && this.currentDashboard.id);
        this._drilldown = null;
        this.renderDrilldownBanner();
        this.autoExecuteAllWidgets();
    },

    // Resolve a drilldown context on dashboard open: an in-app pending context
    // wins; then a ?pv= URL param (new-tab / shared drilldown link); then a
    // sessionStorage context left by an earlier open of this same dashboard.
    //
    // The overlay is a per-view state, not a one-shot: the ?pv= param is consumed
    // on first read, but opening the same dashboard is re-entrant (routing,
    // presence/SSE, variable-bar reconcile all re-execute widgets). So the overlay
    // is bound to its target dashboard id and preserved across re-entrant opens and
    // reloads; it is only dropped when a fresh drilldown arrives, the user exits, or
    // a DIFFERENT dashboard is opened. Without this, a re-entrant open would null the
    // overlay and the follow-up executes would revert widgets to the stored defaults.
    _resolveDrilldown() {
        const currentId = this.currentDashboard && this.currentDashboard.id;
        let dd = this._pendingDrilldown || null;
        this._pendingDrilldown = null;
        if (!dd) dd = this._readDrilldownFromUrl();
        if (!dd) dd = this._loadStoredDrilldown(currentId);
        if (dd) {
            dd.dashboardId = currentId;
            this._drilldown = dd;
            this._storeDrilldown(dd);
        } else if (!(this._drilldown && this._drilldown.dashboardId === currentId)) {
            this._drilldown = null;
        }
        this.renderDrilldownBanner();
    },

    _readDrilldownFromUrl() {
        const params = new URLSearchParams(window.location.search);
        const raw = params.get('pv');
        if (!raw) return null;
        // Consume the param so a refresh or re-share doesn't silently re-filter.
        params.delete('pv');
        const qs = params.toString();
        window.history.replaceState({}, document.title,
            window.location.pathname + (qs ? '?' + qs : '') + window.location.hash);
        try {
            const dd = JSON.parse(decodeURIComponent(atob(raw)));
            return (dd && Array.isArray(dd.vars)) ? dd : null;
        } catch (e) {
            console.warn('[Dashboards] Invalid drilldown param:', e);
            return null;
        }
    },

    // Reflect the active drilldown directly on the variable pills (the pill shows
    // the drilldown value, styled distinctly, with an (x) to exit). No banner: the
    // overlay is display-only and never rewrites the dashboard's stored defaults.
    renderDrilldownBanner() {
        const mgr = this.ensureVarManager();
        if (!mgr) return;
        // Legacy: remove any banner left over from an older render.
        const stale = document.getElementById('dashboardDrilldownBanner');
        if (stale) stale.remove();
        const active = this.activeDrilldown();
        const vars = (active && active.vars) || [];
        if (!vars.length) { mgr.clearDisplayOverlay(); return; }
        const overlay = new Map();
        vars.forEach(v => { if (v && v.name) overlay.set(v.name, v.value == null ? '' : String(v.value)); });
        mgr.setDisplayOverlay(overlay);
    },

    // =====================
    // Helpers
    // =====================

    getDashboardTimeRange() {
        const type = this.currentDashboard?.time_range_type || 'last24h';
        const now = new Date();
        switch (type) {
            case 'last1h':  return { start: new Date(now - 3600000).toISOString(), end: now.toISOString() };
            case 'last24h': return { start: new Date(now - 86400000).toISOString(), end: now.toISOString() };
            case 'last7d':  return { start: new Date(now - 604800000).toISOString(), end: now.toISOString() };
            case 'last30d': return { start: new Date(now - 2592000000).toISOString(), end: now.toISOString() };
            case 'all':    return { start: new Date('2000-01-01T00:00:00Z').toISOString(), end: now.toISOString() };
            case 'custom':
                if (this.currentDashboard.time_range_start && this.currentDashboard.time_range_end) {
                    return { start: this.currentDashboard.time_range_start, end: this.currentDashboard.time_range_end };
                }
                return { start: new Date(now - 86400000).toISOString(), end: now.toISOString() };
            default:
                return { start: new Date(now - 86400000).toISOString(), end: now.toISOString() };
        }
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
    // owns the displayed values; currentDashboard.variables mirrors it for
    // persistence and server-side substitution.
    ensureVarManager() {
        if (this.varManager) return this.varManager;
        if (!window.VariableManager) return null;
        this.varManager = new VariableManager({
            container: 'dashboardVariables',
            onChange: async () => {
                this.currentDashboard.variables = this.varManager.serialize();
                // Persist before refreshing: server-side execution substitutes
                // from the stored values, so the save must land first.
                await this.saveVariables();
                this.autoExecuteAllWidgets();
            },
            // Clicking the (x) on a drilldown pill exits the drilldown.
            onOverlayClear: () => this.exitDrilldown(),
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
            if (persist) return this.saveVariables();
        }
        return Promise.resolve();
    },

    async saveVariables() {
        if (!this.currentDashboard) return;
        try {
            await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/variables`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ variables: this.currentDashboard.variables || [] })
            });
        } catch (err) {
            console.error('[Dashboards] Failed to save variables:', err);
        }
    }
};

window.Dashboards = Dashboards;

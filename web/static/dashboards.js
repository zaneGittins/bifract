/**
 * Dashboards Frontend Module
 * Grid-based dashboards with draggable, resizable query widgets.
 * All widget queries auto-execute when a dashboard is opened.
 */

const Dashboards = {
    currentDashboard: null,
    currentPage: 0,
    pageSize: 20,
    totalDashboards: 0,
    searchQuery: '',

    // Drag/resize state
    dragState: null,
    resizeState: null,
    presenceInterval: null,

    // Grid config: 12 columns, row height in px
    GRID_COLS: 12,
    ROW_HEIGHT: 130,
    MIN_WIDTH: 2,
    MIN_HEIGHT: 2,

    init() {
        this.currentDashboard = null;
        this.stopDragResize();
        this.bindEvents();
        this.showDashboardListing();
    },

    bindEvents() {
        this.unbindEvents();

        const createBtn = document.getElementById('createDashboardBtn');
        if (createBtn) {
            createBtn._dashHandler = () => this.showCreateDashboardModal();
            createBtn.addEventListener('click', createBtn._dashHandler);
        }

        const refreshBtn = document.getElementById('dashboardsRefreshBtn');
        if (refreshBtn) {
            refreshBtn._dashHandler = () => this.loadDashboards();
            refreshBtn.addEventListener('click', refreshBtn._dashHandler);
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

        const backBtn = document.getElementById('backToDashboardsBtn');
        if (backBtn) {
            backBtn._dashHandler = () => this.showDashboardListing();
            backBtn.addEventListener('click', backBtn._dashHandler);
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
    },

    unbindEvents() {
        const ids = [
            'createDashboardBtn', 'dashboardsRefreshBtn', 'dashboardSearchInput',
            'dashboardsPrevBtn', 'dashboardsNextBtn', 'backToDashboardsBtn',
            'addWidgetBtn', 'deleteDashboardBtn', 'dashboardTimeRangeBtn'
        ];
        ids.forEach(id => {
            const el = document.getElementById(id);
            if (el && el._dashHandler) {
                el.removeEventListener('click', el._dashHandler);
                el.removeEventListener('input', el._dashHandler);
                delete el._dashHandler;
            }
        });
    },

    // =====================
    // Listing
    // =====================

    showDashboardListing() {
        this.stopPresenceTracking();
        const listing = document.getElementById('dashboardListing');
        const editor = document.getElementById('dashboardEditor');
        if (listing) listing.style.display = 'block';
        if (editor) editor.style.display = 'none';
        this.loadDashboards();
    },

    async loadDashboards() {
        const offset = this.currentPage * this.pageSize;
        try {
            const response = await fetch(`/api/v1/dashboards?limit=${this.pageSize}&offset=${offset}`, {
                credentials: 'include'
            });
            const data = await response.json();

            if (!data.success) throw new Error(data.error || 'Failed to load dashboards');

            this.totalDashboards = data.total || 0;
            this.renderDashboardTable(data.data || []);
            this.updatePagination();
        } catch (err) {
            console.error('[Dashboards] Failed to load dashboards:', err);
            this.showError('Failed to load dashboards');
        }
    },

    renderDashboardTable(dashboards) {
        const tbody = document.getElementById('dashboardsTableBody');
        if (!tbody) return;

        if (dashboards.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;padding:40px;color:var(--text-muted);">No dashboards yet. Create one to get started.</td></tr>`;
            return;
        }

        tbody.innerHTML = dashboards.map(d => `
            <tr>
                <td><a href="#" class="dash-link" data-id="${d.id}">${Utils.escapeHtml(d.name)}</a></td>
                <td>${Utils.escapeHtml(d.description || '')}</td>
                <td>${Utils.escapeHtml(d.time_range_type || '')}</td>
                <td>${this.formatDate(d.created_at)}</td>
                <td>${this.formatDate(d.updated_at)}</td>
                <td>
                    <button class="btn-action" onclick="Dashboards.exportDashboard('${d.id}')">Export</button>
                    <button class="btn-action danger" onclick="Dashboards.deleteDashboardById('${d.id}')">Delete</button>
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
    },

    // =====================
    // Dashboard Editor
    // =====================

    async openDashboard(id) {
        try {
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

            this.renderVariablesBar();
            this.renderDashboardGrid();
            this.autoExecuteAllWidgets();
            this.startPresenceTracking();
        } catch (err) {
            console.error('[Dashboards] Failed to open dashboard:', err);
            this.showError('Failed to load dashboard');
        }
    },

    // ---- Presence ----

    startPresenceTracking() {
        if (!this.currentDashboard || this.presenceInterval) return;
        this.updatePresence();
        this.presenceInterval = setInterval(() => {
            this.updatePresence();
            this.refreshPresence();
        }, 5000);
    },

    stopPresenceTracking() {
        if (this.presenceInterval) {
            clearInterval(this.presenceInterval);
            this.presenceInterval = null;
        }
        const el = document.getElementById('dashboardPresence');
        if (el) el.innerHTML = '';
    },

    async updatePresence() {
        if (!this.currentDashboard) return;
        try {
            await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/presence`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({})
            });
        } catch (_) {}
    },

    async refreshPresence() {
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
        // Deduplicate by username
        const seen = new Set();
        const unique = users.filter(u => {
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
                    <button class="widget-btn widget-config-btn" title="Conditional formatting" onclick="Dashboards.showRowColoringPanel('${widget.id}')" style="font-size:0.75rem;">&#9881;</button>
                    <button class="widget-btn widget-edit-btn" title="Edit" onclick="Dashboards.showInlineWidgetEdit('${widget.id}')">&#9998;</button>
                    <button class="widget-btn widget-delete-btn" title="Delete" onclick="Dashboards.deleteWidget('${widget.id}')">&#x2715;</button>
                </div>
            </div>
            <div class="widget-content" id="wc-${widget.id}">
                <div class="widget-loading">Loading...</div>
            </div>
            <div class="widget-resize-handle" data-widget-id="${widget.id}"></div>
        `;

        return el;
    },

    // =====================
    // Auto-execute on open
    // =====================

    autoExecuteAllWidgets() {
        if (!this.currentDashboard || !this.currentDashboard.widgets) return;
        this.currentDashboard.widgets.forEach(widget => {
            this.executeWidget(widget.id);
        });
    },

    async executeWidget(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId)
            : null;
        if (!widget) return;

        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (contentEl && contentEl._editingWidget) return;
        if (contentEl) contentEl.innerHTML = '<div class="widget-loading">Executing...</div>';

        const execBtn = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"] .widget-execute-btn`);
        if (execBtn) { execBtn.innerHTML = '<span class="spinner"></span>'; execBtn.disabled = true; }

        try {
            const timeRange = this.getDashboardTimeRange();
            let query = widget.query_content || '';
            if (this.currentDashboard.variables && this.currentDashboard.variables.length > 0) {
                for (const v of this.currentDashboard.variables) {
                    if (v.name) query = query.replaceAll('@' + v.name, v.value || '*');
                }
            }
            const requestBody = {
                query: query,
                query_type: 'bql',
                start: timeRange.start,
                end: timeRange.end
            };
            if (window.FractalContext && window.FractalContext.currentFractal) {
                requestBody.fractal_id = window.FractalContext.currentFractal.id;
            }

            const response = await fetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(requestBody)
            });
            const data = await response.json();

            if (!data.success) throw new Error(data.error || 'Query failed');

            const resultData = {
                results: data.results || [],
                count: (data.results || []).length,
                execution_ms: data.execution_ms || 0,
                sql: data.sql || '',
                chart_type: data.chart_type || 'table',
                chart_config: data.chart_config || {},
                field_order: data.field_order || [],
                is_aggregated: data.is_aggregated || false
            };

            // Update widget in local state
            widget.last_results = JSON.stringify(resultData);
            widget.last_executed_at = new Date().toISOString();
            if (data.chart_type) widget.chart_type = data.chart_type;

            // Save results to backend (fire-and-forget)
            const chartType = data.chart_type || widget.chart_type || 'table';
            fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}/results`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ last_results: widget.last_results, chart_type: chartType })
            }).catch(() => {});

            this.renderWidgetResults(widgetId, resultData);
        } catch (err) {
            console.error('[Dashboards] Widget execution failed:', err);
            if (contentEl && !contentEl._editingWidget) {
                contentEl.innerHTML = `<div class="widget-error">Error: ${Utils.escapeHtml(err.message)}</div>`;
            }
        } finally {
            if (execBtn) { execBtn.innerHTML = '&#9654;'; execBtn.disabled = false; }
        }
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

        if (chartType !== 'table' && results.length > 0) {
            const chartHtml = this.renderQueryChart(resultData, widgetConfig);
            contentEl.innerHTML = chartHtml || this.renderResultsTable(results, resultData, widgetConfig);
        } else {
            contentEl.innerHTML = this.renderResultsTable(results, resultData, widgetConfig);
        }
    },

    parseChartConfig(config) {
        if (!config) return {};
        if (typeof config === 'string') {
            try { return JSON.parse(config); } catch { return {}; }
        }
        return config;
    },

    renderQueryChart(results, widgetConfig) {
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
                    <div id="${graphId}" style="width:100%;height:100%;"></div>
                </div>
            `;
            setTimeout(() => {
                if (window.Notebooks && Notebooks.renderGraphInNotebook) {
                    Notebooks.renderGraphInNotebook(graphId, results);
                }
            }, 300);
            return graphHtml;
        }

        if (chartType === 'heatmap') {
            const heatmapId = `dheatmap-${chartId}`;
            const heatmapHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;overflow:auto;">
                    <div id="${heatmapId}" style="width:100%;overflow:auto;"></div>
                </div>
            `;
            setTimeout(() => {
                if (window.Notebooks && Notebooks.renderHeatmapInNotebook) {
                    Notebooks.renderHeatmapInNotebook(heatmapId, results);
                }
            }, 300);
            return heatmapHtml;
        }

        if (chartType === 'worldmap') {
            const mapId = `dmap-${chartId}`;
            const mapHtml = `
                <div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">
                    <div id="${mapId}" class="worldmap-container" style="height:100%;"></div>
                </div>
            `;
            setTimeout(() => {
                const el = document.getElementById(mapId);
                if (el && window.BifractWorldMap) {
                    const cfg = results.chart_config || {};
                    BifractWorldMap.render(el, results.results || [], {
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
            this.renderChartOnCanvas(chartId, results);
        }, 300);

        return chartHtml;
    },

    renderChartOnCanvas(chartId, results) {
        const canvas = document.getElementById(chartId);
        if (!canvas) return;

        const existingChart = Chart.getChart(canvas);
        if (existingChart) existingChart.destroy();

        const chartType = results.chart_type;
        const chartData = results.results;

        try {
            if (chartType === 'piechart') {
                this.renderPieChart(canvas, chartData, results);
            } else if (chartType === 'barchart') {
                this.renderBarChart(canvas, chartData, results);
            } else if (chartType === 'timechart') {
                this.renderTimeChart(canvas, chartData, results);
            } else if (chartType === 'histogram') {
                if (window.Notebooks && Notebooks.renderHistogramChart) {
                    Notebooks.renderHistogramChart(canvas, chartData, results);
                }
            }
        } catch (err) {
            console.error('[Dashboards] Chart render error:', err);
        }
    },

    renderPieChart(canvas, data, results) {
        if (!data || data.length === 0) return;

        const fields = results.field_order || Object.keys(data[0] || {});
        const labelField = fields[0];
        const valueField = fields.find(f => f === '_count' || f.includes('count') || f === 'sum' || f === 'avg') || fields[1];

        const chartConfig = results.chart_config || {};
        const limit = chartConfig.limit || data.length;

        let chartData = data.map(row => ({
            label: row[labelField] || 'Unknown',
            value: parseFloat(row[valueField]) || 0
        }));
        chartData.sort((a, b) => b.value - a.value);

        const topItems = chartData.slice(0, limit);
        const remaining = chartData.slice(limit);

        const labels = topItems.map(item => item.label);
        const values = topItems.map(item => item.value);

        if (remaining.length > 0) {
            labels.push(`Others (${remaining.length})`);
            values.push(remaining.reduce((s, i) => s + i.value, 0));
        }

        const colors = [
            '#9c6ade', '#b794f6', '#8b5fbf', '#a855f7', '#7c3aed',
            '#6366f1', '#3b82f6', '#06b6d4', '#10b981', '#f59e0b',
            '#ef4444', '#f97316', '#84cc16', '#22c55e', '#14b8a6'
        ];

        const cv = ThemeManager.getCSSVar;

        // Use a fresh canvas to avoid stale state from Chart.js destroy/recreate
        const parent = canvas.parentElement;
        canvas.style.display = 'none';

        const oldPie = parent.querySelector('.pie-chart-wrapper');
        if (oldPie) oldPie.remove();

        const wrapper = document.createElement('div');
        wrapper.className = 'pie-chart-wrapper';
        wrapper.style.position = 'relative';
        wrapper.style.width = '100%';
        wrapper.style.height = '100%';
        parent.appendChild(wrapper);

        const freshCanvas = document.createElement('canvas');
        wrapper.appendChild(freshCanvas);

        new Chart(freshCanvas, {
            type: 'pie',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors.slice(0, values.length),
                    borderColor: cv('--chart-border'),
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            color: cv('--chart-text'),
                            font: { family: 'Inter', size: 12 },
                            padding: 20
                        }
                    },
                    tooltip: {
                        backgroundColor: cv('--chart-bg'),
                        titleColor: cv('--chart-text'),
                        bodyColor: cv('--chart-text-secondary'),
                        borderColor: cv('--chart-accent'),
                        borderWidth: 1
                    }
                },
                layout: { padding: 20 }
            }
        });
    },

    renderBarChart(canvas, data, results) {
        if (!data || data.length === 0) return;

        const fields = results.field_order || Object.keys(data[0] || {});
        const labelField = fields[0];
        const valueField = fields.find(f => f === '_count' || f.includes('count') || f === 'sum' || f === 'avg') || fields[1];

        const chartConfig = results.chart_config || {};
        const limit = chartConfig.limit || data.length;
        const topItems = data.slice(0, limit);

        const labels = topItems.map(row => String(row[labelField] || 'Unknown'));
        const values = topItems.map(row => parseFloat(row[valueField]) || 0);

        const cv = ThemeManager.getCSSVar;
        new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: valueField.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase()),
                    data: values,
                    backgroundColor: cv('--chart-accent'),
                    borderColor: cv('--chart-accent-dark'),
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: cv('--chart-text'),
                            font: { family: 'Inter', size: 12 }
                        }
                    },
                    tooltip: {
                        backgroundColor: cv('--chart-bg'),
                        titleColor: cv('--chart-text'),
                        bodyColor: cv('--chart-text-secondary'),
                        borderColor: cv('--chart-accent'),
                        borderWidth: 1
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 } },
                        grid: { color: cv('--chart-grid') }
                    },
                    x: {
                        ticks: {
                            color: cv('--chart-text-secondary'),
                            font: { family: 'Inter', size: 11 },
                            maxRotation: 45,
                            minRotation: 0
                        },
                        grid: { color: cv('--chart-grid-half') }
                    }
                }
            }
        });
    },

    renderSingleValWidget(results, widgetConfig) {
        const data = results.results;
        if (!data || data.length === 0) {
            return '<div class="singleval-display"><div class="singleval-value">--</div></div>';
        }

        const fields = results.field_order || Object.keys(data[0] || {});
        const valueField = fields.find(f =>
            f === '_count' || f === 'count' || f.startsWith('sum_') ||
            f.startsWith('avg_') || f.startsWith('min_') || f.startsWith('max_') ||
            f.startsWith('stddev_')
        ) || fields[0];

        const rawValue = data[0][valueField];
        const numValue = parseFloat(rawValue);
        const displayValue = isNaN(numValue) ? String(rawValue) : this.formatSingleValue(numValue);

        const chartConfig = results.chart_config || {};
        const label = chartConfig.label || valueField.replace(/_/g, ' ');

        // Apply conditional formatting to singleval
        let valueStyle = '';
        const rules = (widgetConfig && widgetConfig.row_coloring_rules) || [];
        for (const rule of rules) {
            if (!rule.column) continue;
            if (rule.column === valueField && this.evaluateRule(rawValue, rule)) {
                const color = rule.color || '#8b5cf6';
                valueStyle = `color: ${color};`;
                break;
            }
        }

        return `
            <div class="singleval-display">
                <div class="singleval-value" style="${valueStyle}">${Utils.escapeHtml(displayValue)}</div>
                <div class="singleval-label">${Utils.escapeHtml(label)}</div>
            </div>
        `;
    },

    renderTimeChart(canvas, data, results) {
        if (!data || data.length === 0) return;

        const fields = results.field_order || Object.keys(data[0] || {});
        const timeField = fields.find(f => f === 'time_bucket') || fields[0];
        const valueFields = fields.filter(f =>
            f !== timeField && f !== 'time_bucket' &&
            (f === '_count' || f === 'count' || f.startsWith('sum_') ||
             f.startsWith('avg_') || f.startsWith('min_') || f.startsWith('max_') ||
             f.startsWith('bucket_') || f.startsWith('stddev_'))
        );
        const groupFields = fields.filter(f => f !== timeField && !valueFields.includes(f));

        const cv = ThemeManager.getCSSVar;
        const seriesColors = [
            '#9c6ade', '#3b82f6', '#10b981', '#f59e0b', '#ef4444',
            '#06b6d4', '#8b5fbf', '#f97316', '#84cc16', '#14b8a6'
        ];

        let datasets;
        let labels;
        const valueField = valueFields[0] || fields[1];

        if (groupFields.length > 0) {
            const groupField = groupFields[0];
            const groups = {};
            data.forEach(row => {
                const key = String(row[groupField] || 'Unknown');
                if (!groups[key]) groups[key] = [];
                groups[key].push(row);
            });

            datasets = Object.entries(groups).map(([key, rows], idx) => ({
                label: key,
                data: rows.map(r => parseFloat(r[valueField]) || 0),
                borderColor: seriesColors[idx % seriesColors.length],
                backgroundColor: seriesColors[idx % seriesColors.length] + '20',
                fill: true,
                tension: 0.3,
                pointRadius: 2,
                pointHoverRadius: 5,
                borderWidth: 2
            }));

            labels = Object.values(groups)[0].map(r => String(r[timeField] || ''));
        } else {
            labels = data.map(r => String(r[timeField] || ''));
            datasets = [{
                label: valueField.replace(/_/g, ' '),
                data: data.map(r => parseFloat(r[valueField]) || 0),
                borderColor: cv('--chart-accent'),
                backgroundColor: cv('--chart-accent') + '20',
                fill: true,
                tension: 0.3,
                pointRadius: 2,
                pointHoverRadius: 5,
                borderWidth: 2
            }];
        }

        new Chart(canvas, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        display: datasets.length > 1,
                        position: 'top',
                        labels: {
                            color: cv('--chart-text'),
                            font: { family: 'Inter', size: 11 },
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    },
                    tooltip: {
                        backgroundColor: cv('--chart-bg'),
                        titleColor: cv('--chart-text'),
                        bodyColor: cv('--chart-text-secondary'),
                        borderColor: cv('--chart-accent'),
                        borderWidth: 1
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: cv('--chart-text-secondary'), font: { family: 'Inter', size: 11 } },
                        grid: { color: cv('--chart-grid') }
                    },
                    x: {
                        ticks: {
                            color: cv('--chart-text-secondary'),
                            font: { family: 'Inter', size: 10 },
                            maxRotation: 45,
                            minRotation: 0,
                            maxTicksLimit: 15
                        },
                        grid: { color: cv('--chart-grid') }
                    }
                },
                layout: { padding: 6 }
            }
        });
    },

    formatSingleValue(num) {
        if (num === 0) return '0';
        const abs = Math.abs(num);
        if (abs >= 1e9) return (num / 1e9).toFixed(1) + 'B';
        if (abs >= 1e6) return (num / 1e6).toFixed(1) + 'M';
        if (abs >= 1e4) return (num / 1e3).toFixed(1) + 'K';
        if (Number.isInteger(num)) return num.toLocaleString();
        return num.toFixed(2);
    },

    renderResultsTable(results, resultMetadata, widgetConfig) {
        if (!results || results.length === 0) {
            return '<div style="padding:20px;text-align:center;color:var(--text-muted);">No results</div>';
        }

        const tableColumns = resultMetadata?.table_columns || resultMetadata?.columns || resultMetadata?.field_order;
        const systemFields = ['_all_fields', 'raw_log', 'log_id'];
        const headers = (tableColumns && tableColumns.length > 0)
            ? tableColumns
            : Object.keys(results[0]).filter(h => !systemFields.includes(h));

        const rules = (widgetConfig && widgetConfig.row_coloring_rules) || [];

        return `
            <table class="results-table" style="width:100%;border-collapse:collapse;font-size:0.8rem;">
                <thead>
                    <tr>${headers.map(h => `<th style="padding:6px 8px;border-bottom:1px solid var(--border-color);text-align:left;background:var(--bg-tertiary);">${Utils.escapeHtml(h)}</th>`).join('')}</tr>
                </thead>
                <tbody>
                    ${results.slice(0, 100).map(row => {
                        const rowStyle = this.getRowHighlightStyle(row, rules);
                        return `<tr style="${rowStyle}">${headers.map(h => {
                            const val = row[h];
                            const cellStyle = this.getCellHighlightStyle(row, h, rules);
                            return `<td style="padding:6px 8px;border-bottom:1px solid var(--border-color);${cellStyle}">${Utils.escapeHtml(typeof val === 'object' ? JSON.stringify(val) : String(val ?? ''))}</td>`;
                        }).join('')}</tr>`;
                    }).join('')}
                </tbody>
            </table>
            ${results.length > 100 ? '<div style="padding:8px;text-align:center;color:var(--text-muted);font-size:0.75rem;">Showing first 100 rows</div>' : ''}
        `;
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
                headers: { 'Content-Type': 'application/json' },
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
                headers: { 'Content-Type': 'application/json' },
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

        // Save current content to restore on cancel
        contentEl._savedContent = contentEl.innerHTML;

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
                highlightEl.innerHTML = SyntaxHighlight.highlight(queryEl.value) + '<br/>';
                highlightEl.scrollTop = queryEl.scrollTop;
            };
            doHighlight();
            queryEl.addEventListener('input', doHighlight);
            queryEl.addEventListener('scroll', () => { highlightEl.scrollTop = queryEl.scrollTop; });
            queryEl.focus();
        }
    },

    cancelInlineWidgetEdit(widgetId) {
        const contentEl = document.getElementById(`wc-${widgetId}`);
        if (!contentEl) return;
        contentEl.innerHTML = contentEl._savedContent || '<div class="widget-loading">No results</div>';
        delete contentEl._savedContent;
        delete contentEl._editingWidget;
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
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ title, query_content: query })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to update widget');

            widget.title = title;
            widget.query_content = query;

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
                credentials: 'include'
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to delete widget');

            this.currentDashboard.widgets = this.currentDashboard.widgets.filter(w => w.id !== widgetId);

            const widgetEl = document.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
            if (widgetEl) widgetEl.remove();
        } catch (err) {
            console.error('[Dashboards] Failed to delete widget:', err);
            this.showError('Failed to delete widget');
        }
    },

    // =====================
    // Dashboard CRUD
    // =====================

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
                    <h3>Time Range</h3>
                    <button class="modal-close" onclick="document.getElementById('dashTimeRangeModal').remove()">&#x2715;</button>
                </div>
                <div class="modal-body">
                    <div class="form-group">
                        <label>Time Range</label>
                        <select id="dtrSelect" class="form-input">
                            <option value="last1h" ${this.currentDashboard.time_range_type === 'last1h' ? 'selected' : ''}>Last 1 Hour</option>
                            <option value="last24h" ${this.currentDashboard.time_range_type === 'last24h' ? 'selected' : ''}>Last 24 Hours</option>
                            <option value="last7d" ${this.currentDashboard.time_range_type === 'last7d' ? 'selected' : ''}>Last 7 Days</option>
                            <option value="last30d" ${this.currentDashboard.time_range_type === 'last30d' ? 'selected' : ''}>Last 30 Days</option>
                            <option value="all" ${this.currentDashboard.time_range_type === 'all' ? 'selected' : ''}>All Time</option>
                        </select>
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

    async saveTimeRange() {
        if (!this.currentDashboard) return;
        const val = document.getElementById('dtrSelect')?.value;
        if (!val) return;

        try {
            await fetch(`/api/v1/dashboards/${this.currentDashboard.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ time_range_type: val })
            });
            this.currentDashboard.time_range_type = val;
            document.getElementById('dashTimeRangeModal')?.remove();
            this.autoExecuteAllWidgets();
        } catch (err) {
            console.error('[Dashboards] Failed to save time range:', err);
        }
    },

    // =====================
    // Row Coloring Panel
    // =====================

    showRowColoringPanel(widgetId) {
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
        if (!widget) return;

        // Remove existing panel immediately (not animated) to avoid duplicate DOM IDs
        const oldPanel = document.getElementById('rowColoringPanel');
        const oldOverlay = document.getElementById('rowColoringOverlay');
        if (oldPanel) oldPanel.remove();
        if (oldOverlay) oldOverlay.remove();

        const config = this.parseChartConfig(widget.chart_config);
        const rules = config.row_coloring_rules || [];

        const overlay = document.createElement('div');
        overlay.className = 'row-coloring-overlay';
        overlay.id = 'rowColoringOverlay';
        overlay.addEventListener('click', () => this.closeRowColoringPanel());
        document.body.appendChild(overlay);

        const panel = document.createElement('div');
        panel.className = 'row-coloring-panel';
        panel.id = 'rowColoringPanel';
        panel.dataset.widgetId = widgetId;
        panel.innerHTML = `
            <div class="panel-header">
                <h3>Conditional Formatting</h3>
                <button class="widget-btn" onclick="Dashboards.closeRowColoringPanel()" style="font-size:1.1rem;">&#x2715;</button>
            </div>
            <div class="panel-body">
                <p style="font-size:0.8rem;color:var(--text-secondary);margin:0 0 12px 0;">Highlight cells or rows where a column matches a condition.</p>
                <div id="rowColoringRules">
                    ${rules.length === 0 ? '<div class="row-coloring-empty">No rules configured</div>' : ''}
                </div>
                <button class="btn-sm btn-secondary" onclick="Dashboards.addRowColoringRule()" style="margin-top:8px;width:100%;">+ Add Rule</button>
            </div>
            <div class="panel-footer">
                <button class="btn-secondary" onclick="Dashboards.closeRowColoringPanel()">Cancel</button>
                <button class="btn-primary" onclick="Dashboards.saveRowColoringRules()">Save</button>
            </div>
        `;
        document.body.appendChild(panel);

        // Add existing rules
        rules.forEach(rule => this.addRowColoringRule(rule));

        // Animate open
        requestAnimationFrame(() => {
            overlay.classList.add('open');
            panel.classList.add('open');
        });
    },

    closeRowColoringPanel() {
        const panel = document.getElementById('rowColoringPanel');
        const overlay = document.getElementById('rowColoringOverlay');
        if (panel) {
            panel.classList.remove('open');
            setTimeout(() => panel.remove(), 300);
        }
        if (overlay) {
            overlay.classList.remove('open');
            setTimeout(() => overlay.remove(), 300);
        }
    },

    addRowColoringRule(existing) {
        const container = document.getElementById('rowColoringRules');
        if (!container) return;

        // Remove empty message
        const empty = container.querySelector('.row-coloring-empty');
        if (empty) empty.remove();

        const op = (existing && existing.operator) || '=';
        const target = (existing && existing.target) || 'row';

        const rule = document.createElement('div');
        rule.className = 'row-coloring-rule';
        rule.innerHTML = `
            <div class="rule-row-top">
                <input type="text" class="rule-column" placeholder="Column" value="${Utils.escapeHtml((existing && existing.column) || '')}">
                <select class="rule-operator">
                    <option value="=" ${op === '=' ? 'selected' : ''}>=</option>
                    <option value="contains" ${op === 'contains' ? 'selected' : ''}>contains</option>
                    <option value=">" ${op === '>' ? 'selected' : ''}>&gt;</option>
                    <option value=">=" ${op === '>=' ? 'selected' : ''}>&gt;=</option>
                    <option value="<" ${op === '<' ? 'selected' : ''}>&lt;</option>
                    <option value="<=" ${op === '<=' ? 'selected' : ''}>&lt;=</option>
                </select>
                <input type="text" class="rule-value" placeholder="Value" value="${Utils.escapeHtml((existing && existing.value) || '')}">
                <button class="remove-rule-btn" onclick="this.closest('.row-coloring-rule').remove()">&#x2715;</button>
            </div>
            <div class="rule-row-bottom">
                <div class="rule-target-toggle">
                    <button type="button" class="rule-target-btn ${target === 'cell' ? 'active' : ''}" data-target="cell" onclick="this.parentElement.querySelectorAll('.rule-target-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active')">Cell</button>
                    <button type="button" class="rule-target-btn ${target === 'row' ? 'active' : ''}" data-target="row" onclick="this.parentElement.querySelectorAll('.rule-target-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active')">Row</button>
                </div>
                <div class="rule-color-swatches">
                    ${['#ef4444','#f97316','#eab308','#22c55e','#14b8a6','#3b82f6','#8b5cf6','#ec4899'].map(c =>
                        `<button type="button" class="rule-swatch${(existing && existing.color) === c || (!existing && c === '#8b5cf6') ? ' active' : ''}" data-color="${c}" style="background:${c};" onclick="this.closest('.rule-color-swatches').querySelectorAll('.rule-swatch').forEach(s=>s.classList.remove('active'));this.classList.add('active')"></button>`
                    ).join('')}
                </div>
                <input type="hidden" class="rule-color" value="${(existing && existing.color) || '#8b5cf6'}">
            </div>
        `;
        // Sync swatch clicks to hidden input
        rule.querySelectorAll('.rule-swatch').forEach(sw => {
            sw.addEventListener('click', () => {
                rule.querySelector('.rule-color').value = sw.dataset.color;
            });
        });
        container.appendChild(rule);
    },

    async saveRowColoringRules() {
        const panel = document.getElementById('rowColoringPanel');
        if (!panel) return;

        const widgetId = panel.dataset.widgetId;
        const widget = this.currentDashboard && this.currentDashboard.widgets
            ? this.currentDashboard.widgets.find(w => w.id === widgetId) : null;
        if (!widget) return;

        const ruleEls = panel.querySelectorAll('.row-coloring-rule');
        const rules = [];
        ruleEls.forEach(el => {
            const column = el.querySelector('.rule-column').value.trim();
            const value = el.querySelector('.rule-value').value.trim();
            const operator = el.querySelector('.rule-operator').value;
            const color = el.querySelector('.rule-color').value;
            const activeTarget = el.querySelector('.rule-target-btn.active');
            const target = activeTarget ? activeTarget.dataset.target : 'row';
            if (column) {
                rules.push({ column, operator, value, color, target });
            }
        });

        // Merge with existing chart_config
        const config = this.parseChartConfig(widget.chart_config);
        config.row_coloring_rules = rules;

        try {
            const response = await fetch(`/api/v1/dashboards/${this.currentDashboard.id}/widgets/${widgetId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ chart_config: config })
            });
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Failed to save');

            widget.chart_config = config;
            this.closeRowColoringPanel();

            // Re-render widget if it has cached results
            if (widget.last_results) {
                const resultData = typeof widget.last_results === 'string'
                    ? JSON.parse(widget.last_results) : widget.last_results;
                this.renderWidgetResults(widgetId, resultData);
            }

            this.showSuccess('Row coloring rules saved');
        } catch (err) {
            console.error('[Dashboards] Failed to save row coloring rules:', err);
            this.showError('Failed to save row coloring rules');
        }
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
            return new Date(dateStr).toLocaleString();
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

    renderVariablesBar() {
        const container = document.getElementById('dashboardVariables');
        if (!container) return;

        const vars = (this.currentDashboard && this.currentDashboard.variables) || [];

        if (vars.length === 0) {
            container.innerHTML = `<div class="variables-bar-empty">
                <button class="btn-add-variable" onclick="Dashboards.addVariable()">+ Add Variable</button>
            </div>`;
            return;
        }

        let html = '<div class="variables-bar-items">';
        for (const v of vars) {
            const safeName = Utils.escapeHtml(v.name);
            const safeValue = Utils.escapeHtml(v.value || '*');
            html += `<div class="variable-pill">
                <span class="variable-name">@${safeName}</span>
                <input type="text" class="variable-value-input" value="${safeValue}"
                    data-var-name="${safeName}"
                    onchange="Dashboards.updateVariableValue('${safeName}', this.value)"
                    onkeydown="if(event.key==='Enter'){this.blur();}" />
                <button class="variable-remove-btn" onclick="Dashboards.removeVariable('${safeName}')" title="Remove variable">&times;</button>
            </div>`;
        }
        html += `<button class="btn-add-variable" onclick="Dashboards.addVariable()">+ Variable</button>`;
        html += '</div>';

        container.innerHTML = html;
    },

    addVariable() {
        const name = prompt('Variable name (without @):');
        if (!name || !name.trim()) return;

        const cleanName = name.trim().replace(/[^a-zA-Z0-9_]/g, '');
        if (!cleanName) {
            this.showError('Variable name must contain only letters, numbers, or underscores');
            return;
        }

        if (!this.currentDashboard.variables) {
            this.currentDashboard.variables = [];
        }

        if (this.currentDashboard.variables.some(v => v.name === cleanName)) {
            this.showError('Variable @' + cleanName + ' already exists');
            return;
        }

        this.currentDashboard.variables.push({ name: cleanName, value: '*' });
        this.saveVariables();
        this.renderVariablesBar();
    },

    updateVariableValue(name, value) {
        if (!this.currentDashboard || !this.currentDashboard.variables) return;

        const v = this.currentDashboard.variables.find(v => v.name === name);
        if (v) {
            v.value = value;
            this.saveVariables();
            this.autoExecuteAllWidgets();
        }
    },

    removeVariable(name) {
        if (!this.currentDashboard || !this.currentDashboard.variables) return;

        this.currentDashboard.variables = this.currentDashboard.variables.filter(v => v.name !== name);
        this.saveVariables();
        this.renderVariablesBar();
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

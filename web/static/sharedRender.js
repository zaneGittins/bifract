// SharedRender: standalone renderer for public, no-auth dashboard wallboards.
// It reads a share token from the URL, polls the anonymous API for CACHED widget
// results (the server never executes BQL for this page), and draws them with the
// same standalone chart engine (BifractCharts / BifractWorldMap) the main app
// uses. It deliberately reuses NONE of dashboards.js: no pivots, drilldowns,
// brushing, editing, SSE, or presence exist here.
const SharedRender = {
    GRID_COLS: 12,
    ROW_HEIGHT: 130,

    token: null,
    data: null,
    builtIds: '',            // signature of the current widget shell set
    renderedAt: {},          // widgetId -> last_executed_at last drawn
    pollTimer: null,
    stopped: false,

    init() {
        this.token = this.readToken();
        if (!this.token) {
            this.showState('Invalid link', 'This shared link is missing its token.');
            return;
        }
        window.addEventListener('resize', () => this.relayout());
        this.showState('Loading dashboard…', '');
        this.tick();
    },

    readToken() {
        const m = window.location.pathname.match(/\/shared\/([^/?#]+)/);
        if (m && m[1]) return decodeURIComponent(m[1]);
        const q = new URLSearchParams(window.location.search).get('token');
        return q || null;
    },

    async tick() {
        if (this.stopped) return;
        let pollSeconds = 60;
        try {
            const res = await fetch(`/api/v1/shared/${encodeURIComponent(this.token)}`, { cache: 'no-store' });
            if (res.status === 404) {
                // Permanent: unknown/expired/revoked token, or sharing disabled.
                this.stopped = true;
                this.showState('Not available', 'This dashboard link is invalid, expired, revoked, or sharing has been turned off.');
                return;
            }
            if (!res.ok) throw new Error('Failed to load');
            const data = await res.json();
            this.apply(data);
            pollSeconds = this.pollSeconds(data.refresh_interval);
        } catch (err) {
            // Transient (network/server): keep the last render and retry.
            pollSeconds = 60;
        }
        this.pollTimer = setTimeout(() => this.tick(), pollSeconds * 1000);
    },

    // Client poll cadence. Cheap cached read; floored so a fast dashboard cannot
    // make the wallboard hammer the server, capped so it stays reasonably live.
    pollSeconds(refreshInterval) {
        if (typeof refreshInterval === 'number' && refreshInterval > 0) {
            return Math.min(3600, Math.max(30, refreshInterval));
        }
        return 60;
    },

    apply(data) {
        this.data = data;
        this.hideState();

        const name = data.name || 'Dashboard';
        document.title = `${name} - Bifract`;
        const titleEl = document.getElementById('sharedTitle');
        if (titleEl) titleEl.textContent = name;

        const widgets = data.widgets || [];
        const sig = widgets.map(w => `${w.id}:${w.pos_x},${w.pos_y},${w.width},${w.height}`).join('|');
        if (sig !== this.builtIds) {
            this.buildGrid(widgets);
            this.builtIds = sig;
            this.renderedAt = {};
        }

        let latest = null;
        widgets.forEach(w => {
            const stamp = w.last_executed_at || '';
            if (this.renderedAt[w.id] !== stamp) {
                this.renderWidgetContent(w);
                this.renderedAt[w.id] = stamp;
            }
            if (w.last_executed_at && (!latest || w.last_executed_at > latest)) latest = w.last_executed_at;
        });

        const live = document.getElementById('sharedLive');
        const updated = document.getElementById('sharedUpdated');
        if (live) live.style.display = '';
        if (updated) updated.textContent = latest ? `Updated ${Utils.timeAgo(latest)}` : 'Live';
    },

    buildGrid(widgets) {
        const grid = document.getElementById('sharedGrid');
        if (!grid) return;
        // Destroy any Chart.js instances before wiping to avoid leaking them on a
        // long-running wallboard.
        this.destroyCharts(grid);
        grid.innerHTML = '';

        const maxBottom = widgets.reduce((m, w) => Math.max(m, w.pos_y + w.height), 6);
        grid.style.minHeight = `${maxBottom * this.ROW_HEIGHT + 40}px`;

        widgets.forEach(w => {
            const el = document.createElement('div');
            el.className = 'dashboard-widget';
            el.dataset.widgetId = w.id;
            const esc = (window.Utils && Utils.escapeHtml) ? Utils.escapeHtml : (s => s);
            el.innerHTML = `
                <div class="widget-header">
                    <span class="widget-title">${esc(w.title || 'Widget')}</span>
                </div>
                <div class="widget-content" id="sw-${w.id}">
                    <div class="widget-loading">Loading…</div>
                </div>
            `;
            grid.appendChild(el);
        });
        this.relayout();
    },

    // Re-render every widget's content from the current data (used on theme change
    // so charts pick up the new --chart-* colors). Cheap: no network, cached data.
    rerenderAll() {
        if (!this.data || !this.data.widgets) return;
        this.data.widgets.forEach(w => this.renderWidgetContent(w));
    },

    // Recompute absolute positions from the current grid width (12-col layout).
    relayout() {
        const grid = document.getElementById('sharedGrid');
        if (!grid || !this.data) return;
        const containerWidth = grid.offsetWidth || (window.innerWidth - 40);
        const colWidth = containerWidth / this.GRID_COLS;
        (this.data.widgets || []).forEach(w => {
            const el = grid.querySelector(`.dashboard-widget[data-widget-id="${w.id}"]`);
            if (!el) return;
            el.style.left = `${w.pos_x * colWidth}px`;
            el.style.top = `${w.pos_y * this.ROW_HEIGHT}px`;
            el.style.width = `${w.width * colWidth}px`;
            el.style.height = `${w.height * this.ROW_HEIGHT}px`;
        });
    },

    renderWidgetContent(widget) {
        const el = document.getElementById(`sw-${widget.id}`);
        if (!el) return;
        // Destroy a prior Chart.js instance on this widget's canvas before replacing.
        this.destroyCharts(el);

        const results = widget.results;
        if (!results || !Array.isArray(results.results) || results.results.length === 0) {
            el.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);">No data</div>';
            return;
        }

        // Effective chart type comes from the cached results (matches the app's
        // renderWidgetResults), NOT the widget's configured type.
        const chartType = results.chart_type || 'table';
        const widgetConfig = this.parseConfig(widget.chart_config);
        el.innerHTML = this.renderChart(el, results, chartType, widgetConfig) || this.renderTable(results);
    },

    // Mirrors dashboards.js renderQueryChart, trimmed: no click/brush handlers.
    // Returns HTML for chart types, or '' to fall back to a table.
    renderChart(contentEl, results, chartType, widgetConfig) {
        if (chartType === 'table' || !results.results || results.results.length === 0) return '';

        if (chartType === 'singleval') {
            return BifractCharts.renderSingleVal(null, {
                data: results.results,
                fields: results.field_order,
                config: this.mergeConfig(results, widgetConfig),
                coloringRules: (widgetConfig && widgetConfig.row_coloring_rules) || [],
                returnHtml: true
            });
        }

        const cid = `swc-${Math.random().toString(36).slice(2, 10)}`;
        const wrap = (inner) => `<div class="chart-container" style="margin:0;padding:6px;background:var(--bg-secondary);border-radius:4px;height:calc(100% - 12px);box-sizing:border-box;position:relative;">${inner}</div>`;

        if (chartType === 'graph' || chartType === 'mesh') {
            const html = wrap(`<div id="${cid}" style="width:100%;height:100%;"></div>`);
            setTimeout(() => {
                const el = document.getElementById(cid);
                if (!el) return;
                const opts = { data: results.results, fields: results.field_order, config: results.chart_config || {} };
                if (chartType === 'graph') BifractCharts.renderGraphSimple(el, opts);
                else BifractCharts.renderMeshSimple(el, opts);
            }, 60);
            return html;
        }

        if (chartType === 'heatmap') {
            const html = wrap(`<div id="${cid}" style="width:100%;overflow:auto;"></div>`);
            setTimeout(() => {
                const el = document.getElementById(cid);
                if (el) BifractCharts.renderHeatmap(el, { data: results.results, config: results.chart_config || {} });
            }, 60);
            return html;
        }

        if (chartType === 'worldmap') {
            const html = wrap(`<div id="${cid}" class="worldmap-container" style="height:100%;"></div>`);
            setTimeout(() => {
                const el = document.getElementById(cid);
                // worldmap.js declares BifractWorldMap as a top-level const (a lexical
                // global, not a window property), so reference it by bare name.
                const worldmap = (typeof BifractWorldMap !== 'undefined') ? BifractWorldMap : null;
                if (el && worldmap) {
                    const cfg = results.chart_config || {};
                    worldmap.render(el, results.results || [], {
                        latField: cfg.latField || 'latitude',
                        lonField: cfg.lonField || 'longitude',
                        labelField: cfg.labelField || null
                    });
                }
            }, 60);
            return html;
        }

        // Canvas charts (pie/bar/timechart/etc.)
        const html = wrap(`<canvas id="${cid}" style="background:transparent;border-radius:4px;"></canvas>`);
        setTimeout(() => {
            const canvas = document.getElementById(cid);
            if (!canvas) return;
            try {
                BifractCharts.renderOnCanvas(canvas, chartType, {
                    data: results.results,
                    fields: results.field_order,
                    config: this.mergeConfig(results, widgetConfig),
                    maintainAspectRatio: false,
                    height: '100%'
                });
            } catch (err) { /* leave the container empty on render failure */ }
        }, 60);
        return html;
    },

    renderTable(results) {
        const rows = results.results || [];
        if (!rows.length) return '<div style="padding:20px;text-align:center;color:var(--text-muted);">No results</div>';
        const esc = (window.Utils && Utils.escapeHtml) ? Utils.escapeHtml : (s => String(s));
        const systemFields = ['_all_fields', 'raw_log', 'log_id'];
        const headers = (results.field_order && results.field_order.length)
            ? results.field_order
            : Object.keys(rows[0]).filter(h => !systemFields.includes(h));
        const capped = rows.slice(0, 200);
        const fmt = (v) => {
            if (v === null || v === undefined) return '';
            if (typeof v === 'object') { try { return JSON.stringify(v); } catch { return String(v); } }
            return String(v);
        };
        const thead = headers.map(h => `<th style="text-align:left;padding:6px 10px;position:sticky;top:0;background:var(--bg-secondary);border-bottom:1px solid var(--border-color);">${esc(h)}</th>`).join('');
        const tbody = capped.map(row =>
            `<tr>${headers.map(h => `<td style="padding:6px 10px;border-bottom:1px solid var(--border-color);white-space:nowrap;">${esc(fmt(row[h]))}</td>`).join('')}</tr>`
        ).join('');
        const note = rows.length > capped.length
            ? `<div style="padding:8px;text-align:center;color:var(--text-muted);font-size:0.75rem;">Showing first ${capped.length} rows</div>` : '';
        return `<div style="overflow:auto;height:100%;"><table style="width:100%;border-collapse:collapse;font-size:0.82rem;"><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>${note}</div>`;
    },

    mergeConfig(results, widgetConfig) {
        return Object.assign({}, results.chart_config || {}, widgetConfig || {});
    },

    parseConfig(config) {
        if (!config) return {};
        if (typeof config === 'string') { try { return JSON.parse(config); } catch { return {}; } }
        return config;
    },

    destroyCharts(root) {
        if (!root || !window.Chart || !Chart.getChart) return;
        root.querySelectorAll('canvas').forEach(c => {
            const inst = Chart.getChart(c);
            if (inst) inst.destroy();
        });
    },

    showState(title, msg) {
        const grid = document.getElementById('sharedGrid');
        const state = document.getElementById('sharedState');
        const t = document.getElementById('sharedStateTitle');
        const m = document.getElementById('sharedStateMsg');
        if (grid) grid.style.display = 'none';
        if (state) state.style.display = '';
        if (t) t.textContent = title;
        if (m) m.textContent = msg || '';
        const live = document.getElementById('sharedLive');
        if (live) live.style.display = 'none';
    },

    hideState() {
        const grid = document.getElementById('sharedGrid');
        const state = document.getElementById('sharedState');
        if (grid) grid.style.display = '';
        if (state) state.style.display = 'none';
    }
};

window.SharedRender = SharedRender;

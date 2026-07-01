// Pivots: per-widget drilldown "interactions". Clicking a table cell, chart
// segment, or timechart point passes the clicked row's field values to either
// another dashboard (as a transient, per-user drilldown) or the search page (as
// @variable values substituted into a BQL query). Config is stored on the
// widget's chart_config under `pivots`; the runtime is wired by Dashboards.
window.Pivots = {

    // ---- Accessors ----

    getPivots(widget) {
        if (!widget) return [];
        const cfg = Dashboards.parseChartConfig(widget.chart_config);
        return Array.isArray(cfg.pivots) ? cfg.pivots : [];
    },

    // Result fields available on a widget (from its cached last execution), used
    // to populate field pickers and the "map all fields" shortcut.
    widgetFields(widget) {
        if (!widget || !widget.last_results) return [];
        let rd;
        try { rd = typeof widget.last_results === 'string' ? JSON.parse(widget.last_results) : widget.last_results; }
        catch (e) { return []; }
        if (Array.isArray(rd.field_order) && rd.field_order.length) return rd.field_order.slice();
        if (Array.isArray(rd.results) && rd.results.length) return Object.keys(rd.results[0]);
        return [];
    },

    // ====================================================
    // Runtime: click -> chooser -> navigate
    // ====================================================

    // Chart segment/point click: fire a single pivot, or choose among several.
    handleDataClick(widget, pivots, ctx, event) {
        if (pivots.length === 1) { this.executePivot(pivots[0], ctx); return; }
        this._showMenu(this._pivotItems(pivots, ctx), event, 'Pivot to');
    },

    // Table right-click: a context menu with copy actions plus any pivots. Cursor
    // anchored and viewport-clamped, so it is never off-screen regardless of how
    // wide the table is (unlike a trailing action column).
    showContextMenu(widget, ctx, event) {
        const pivots = this.getPivots(widget);
        const items = [];
        if (ctx.value != null && String(ctx.value) !== '') {
            items.push({ label: 'Copy value', action: () => this._copy(String(ctx.value)) });
        }
        if (ctx.row) {
            items.push({ label: 'Copy row (JSON)', action: () => this._copy(JSON.stringify(ctx.row)) });
        }
        const pivotItems = this._pivotItems(pivots, ctx);
        if (pivotItems.length) {
            if (items.length) items.push({ separator: true });
            pivotItems.forEach(it => items.push(it));
        }
        if (!items.length) return;
        this._showMenu(items, event, 'Actions');
    },

    _pivotItems(pivots, ctx) {
        return pivots.map(p => ({
            label: p.label || this._defaultLabel(p),
            action: () => this.executePivot(p, ctx),
        }));
    },

    // Generic cursor-anchored menu. items: [{label, action} | {separator:true}].
    _showMenu(items, event, header) {
        this._closeMenu();
        const menu = document.createElement('div');
        menu.className = 'pivot-menu';
        menu.id = 'pivotMenu';
        let html = header ? `<div class="pivot-menu-header">${Utils.escapeHtml(header)}</div>` : '';
        items.forEach((it, i) => {
            html += it.separator
                ? '<div class="pivot-menu-sep"></div>'
                : `<button class="pivot-menu-item" data-i="${i}">${Utils.escapeHtml(it.label)}</button>`;
        });
        menu.innerHTML = html;
        document.body.appendChild(menu);

        // Position near the cursor, clamped to the viewport.
        const x = (event && event.clientX) || 120;
        const y = (event && event.clientY) || 120;
        const rect = menu.getBoundingClientRect();
        menu.style.left = Math.max(8, Math.min(x, window.innerWidth - rect.width - 8)) + 'px';
        menu.style.top = Math.max(8, Math.min(y, window.innerHeight - rect.height - 8)) + 'px';

        menu.querySelectorAll('.pivot-menu-item').forEach(btn => {
            btn.addEventListener('click', () => {
                const it = items[parseInt(btn.dataset.i, 10)];
                this._closeMenu();
                if (it && it.action) it.action();
            });
        });

        // Defer outside-click binding so the opening click doesn't close it.
        setTimeout(() => {
            this._menuCloser = (e) => { if (!menu.contains(e.target)) this._closeMenu(); };
            this._menuKey = (e) => { if (e.key === 'Escape') this._closeMenu(); };
            document.addEventListener('mousedown', this._menuCloser, true);
            document.addEventListener('keydown', this._menuKey, true);
        }, 0);
    },

    // Copy to clipboard with a fallback for non-secure contexts.
    _copy(text) {
        const done = () => { if (window.Dashboards && Dashboards.showSuccess) Dashboards.showSuccess('Copied'); };
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(done).catch(() => this._copyFallback(text, done));
        } else {
            this._copyFallback(text, done);
        }
    },

    _copyFallback(text, done) {
        try {
            const ta = document.createElement('textarea');
            ta.value = text;
            ta.style.position = 'fixed';
            ta.style.opacity = '0';
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            document.body.removeChild(ta);
            done();
        } catch (e) { /* clipboard unavailable */ }
    },

    _closeMenu() {
        const m = document.getElementById('pivotMenu');
        if (m) m.remove();
        if (this._menuCloser) { document.removeEventListener('mousedown', this._menuCloser, true); this._menuCloser = null; }
        if (this._menuKey) { document.removeEventListener('keydown', this._menuKey, true); this._menuKey = null; }
    },

    _defaultLabel(p) {
        return p.target === 'search' ? 'Open in Search' : 'Open dashboard';
    },

    // Build [{name, value}] from a pivot's field->variable mappings against the
    // clicked row. A field missing from the row falls back to the clicked value.
    _buildVars(pivot, ctx) {
        const maps = Array.isArray(pivot.params) ? pivot.params : [];
        const out = [];
        maps.forEach(m => {
            if (!m || !m.field) return;
            const name = String(m.variable || m.field).trim();
            if (!name) return;
            let val = (ctx.row && ctx.row[m.field] != null)
                ? ctx.row[m.field]
                : (m.field === ctx.field ? ctx.value : '');
            out.push({ name, value: val == null ? '' : String(val) });
        });
        return out;
    },

    // Forward the clicked bucket window when present (timechart point), else the
    // source dashboard's current range. Null when forwarding is disabled.
    _timeWindow(pivot, ctx) {
        if (pivot.forward_time === false) return null;
        if (ctx.timeStart && ctx.timeEnd) return { start: ctx.timeStart, end: ctx.timeEnd };
        const r = Dashboards.getDashboardTimeRange();
        return { start: r.start, end: r.end };
    },

    executePivot(pivot, ctx) {
        const vars = this._buildVars(pivot, ctx);
        if (pivot.target === 'search') return this._toSearch(pivot, ctx, vars);
        return this._toDashboard(pivot, ctx, vars);
    },

    _scope() {
        const ctxId = window.FractalContext?.currentFractal?.id || '';
        const isPrism = !!(window.FractalContext?.isPrism && window.FractalContext.isPrism());
        return { ctxId, isPrism, key: isPrism ? 'p' : 'f' };
    },

    _toDashboard(pivot, ctx, vars) {
        if (!pivot.dashboard_id) { Dashboards.showError('Pivot has no target dashboard'); return; }
        const win = this._timeWindow(pivot, ctx);
        const dd = { vars, label: pivot.label || '' };
        if (win) { dd.start = win.start; dd.end = win.end; }
        if (pivot.new_tab) {
            window.open(this._dashboardUrl(pivot.dashboard_id, dd), '_blank', 'noopener');
            return;
        }
        Dashboards.enterDrilldown(pivot.dashboard_id, dd);
    },

    _dashboardUrl(targetId, dd) {
        const s = this._scope();
        const params = new URLSearchParams();
        params.set('pv', btoa(encodeURIComponent(JSON.stringify(dd))));
        return `${window.location.origin}${window.location.pathname}?${params.toString()}#${s.key}/${s.ctxId}/dashboards/${targetId}`;
    },

    // Open the search page via the share-link contract (q + tr + f/p + vars).
    // Values flow only as @variable bindings, so substitution stays on the safe
    // server-side bqlvars path; no row data is concatenated into the query.
    _toSearch(pivot, ctx, vars) {
        const s = this._scope();
        if (!s.ctxId) { Dashboards.showError('No fractal selected'); return; }
        const win = this._timeWindow(pivot, ctx);
        const params = new URLSearchParams();
        params.set('q', btoa(encodeURIComponent(pivot.query || '')));
        if (win) { params.set('tr', 'custom'); params.set('ts', win.start); params.set('te', win.end); }
        else { params.set('tr', '24h'); }
        params.set(s.key, s.ctxId);
        if (vars.length) params.set('vars', btoa(encodeURIComponent(JSON.stringify(vars))));
        const url = `${window.location.origin}${window.location.pathname}?${params.toString()}`;
        if (pivot.new_tab) window.open(url, '_blank', 'noopener');
        else window.location.href = url;
    },

    // ====================================================
    // Config panel
    // ====================================================

    async openConfig(widgetId) {
        const widget = Dashboards.getWidget(widgetId);
        if (!widget) return;
        this._removePanel(true);
        this._widgetId = widgetId;
        this._fields = this.widgetFields(widget);
        this._pivots = JSON.parse(JSON.stringify(this.getPivots(widget)));
        this._dashboards = [];

        const overlay = document.createElement('div');
        overlay.className = 'row-coloring-overlay';
        overlay.id = 'pivotOverlay';
        overlay.addEventListener('click', () => this.cancel());
        document.body.appendChild(overlay);

        const panel = document.createElement('div');
        panel.className = 'row-coloring-panel format-panel pivot-panel';
        panel.id = 'pivotPanel';
        panel.innerHTML = `
            <div class="panel-header">
                <h3>Pivots</h3>
                <button class="widget-btn" onclick="Pivots.cancel()" style="background:none;border:none;color:var(--text-primary);cursor:pointer;font-size:1.1rem;">&#x2715;</button>
            </div>
            <div class="panel-body" id="pivotBody">${this._renderBody()}</div>
            <div class="panel-footer">
                <button class="btn-secondary" onclick="Pivots.cancel()">Cancel</button>
                <button class="btn-primary" onclick="Pivots.save()">Save</button>
            </div>
        `;
        document.body.appendChild(panel);
        requestAnimationFrame(() => { overlay.classList.add('open'); panel.classList.add('open'); });

        // Load selectable dashboards in the background, then refresh the target
        // pickers so dashboard pivots can be pointed at a destination.
        try {
            const resp = await fetch('/api/v1/dashboards?limit=500&offset=0', { credentials: 'include' });
            const data = await resp.json();
            if (data.success) { this._dashboards = data.data || []; this._rerender(); }
        } catch (e) { /* picker just stays empty */ }
    },

    _renderBody() {
        const intro = `<div class="pivot-intro">Pass a clicked row's values to another dashboard or the search page. Multiple pivots show a chooser on click.</div>`;
        const cards = this._pivots.map((p, i) => this._renderCard(p, i)).join('');
        const empty = this._pivots.length ? '' : `<div class="fmt-empty">No pivots yet.</div>`;
        return intro + empty + cards +
            `<button class="btn-secondary pivot-add" onclick="Pivots.addPivot()">+ Add pivot</button>`;
    },

    _renderCard(p, i) {
        const esc = Utils.escapeHtml;
        const target = p.target || 'dashboard';
        const dashOptions = this._dashboards.map(d =>
            `<option value="${esc(d.id)}" ${p.dashboard_id === d.id ? 'selected' : ''}>${esc(d.name)}</option>`
        ).join('');

        const targetBlock = target === 'search'
            ? `<label class="fmt-label">Search query (use @variables below)</label>
               <textarea class="pivot-input pivot-query" rows="2" placeholder='e.g. host=@host status>=400'
                 oninput="Pivots.update(${i},'query',this.value)">${esc(p.query || '')}</textarea>`
            : `<label class="fmt-label">Target dashboard</label>
               <select class="fmt-select" onchange="Pivots.update(${i},'dashboard_id',this.value)">
                 <option value="">Select a dashboard...</option>${dashOptions}
               </select>`;

        const maps = (Array.isArray(p.params) ? p.params : []).map((m, j) => this._renderMapRow(m, i, j)).join('');

        return `
        <div class="pivot-card" data-i="${i}">
            <div class="pivot-card-head">
                <input class="pivot-input pivot-label" value="${esc(p.label || '')}" placeholder="Pivot label (shown on click)"
                    oninput="Pivots.update(${i},'label',this.value)">
                <button class="pivot-remove" title="Remove pivot" onclick="Pivots.removePivot(${i})">&#x2715;</button>
            </div>
            <div class="pivot-row">
                <label class="fmt-label">Target</label>
                <select class="fmt-select" onchange="Pivots.update(${i},'target',this.value)">
                    <option value="dashboard" ${target === 'dashboard' ? 'selected' : ''}>Dashboard</option>
                    <option value="search" ${target === 'search' ? 'selected' : ''}>Search page</option>
                </select>
            </div>
            <div class="pivot-row">${targetBlock}</div>
            <div class="pivot-row">
                <div class="pivot-maps-head">
                    <label class="fmt-label">Pass fields as variables</label>
                    <button class="pivot-link-btn" onclick="Pivots.mapAll(${i})">Map all fields</button>
                </div>
                <div class="pivot-maps">${maps || '<div class="pivot-maps-empty">No fields mapped.</div>'}</div>
                <button class="pivot-link-btn" onclick="Pivots.addMap(${i})">+ Add field</button>
            </div>
            <div class="pivot-row pivot-toggles">
                <label class="pivot-check"><input type="checkbox" ${p.forward_time !== false ? 'checked' : ''}
                    onchange="Pivots.update(${i},'forward_time',this.checked)"> Forward time range</label>
                <label class="pivot-check"><input type="checkbox" ${p.new_tab ? 'checked' : ''}
                    onchange="Pivots.update(${i},'new_tab',this.checked)"> Open in new tab</label>
            </div>
        </div>`;
    },

    _renderMapRow(m, i, j) {
        const esc = Utils.escapeHtml;
        const opts = this._fields.map(f =>
            `<option value="${esc(f)}" ${m.field === f ? 'selected' : ''}>${esc(f)}</option>`
        ).join('');
        const customField = m.field && !this._fields.includes(m.field)
            ? `<option value="${esc(m.field)}" selected>${esc(m.field)}</option>` : '';
        return `
        <div class="pivot-map-row">
            <select class="fmt-select pivot-map-field" onchange="Pivots.updateMap(${i},${j},'field',this.value)">
                <option value="">Field...</option>${customField}${opts}
            </select>
            <span class="pivot-map-arrow">&rarr; @</span>
            <input class="pivot-input pivot-map-var" value="${esc(m.variable || '')}" placeholder="${esc(m.field || 'variable')}"
                oninput="Pivots.updateMap(${i},${j},'variable',this.value)">
            <button class="pivot-remove" title="Remove" onclick="Pivots.removeMap(${i},${j})">&#x2715;</button>
        </div>`;
    },

    _rerender() {
        const body = document.getElementById('pivotBody');
        if (body) body.innerHTML = this._renderBody();
    },

    // ---- Config mutations ----

    addPivot() {
        this._pivots.push({ label: '', target: 'dashboard', dashboard_id: '', query: '', params: [], forward_time: true, new_tab: false });
        this._rerender();
    },

    removePivot(i) { this._pivots.splice(i, 1); this._rerender(); },

    // Live field updates do not re-render (preserve focus); target switch does,
    // since it swaps the destination control.
    update(i, key, val) {
        const p = this._pivots[i];
        if (!p) return;
        p[key] = val;
        if (key === 'target') this._rerender();
    },

    addMap(i) {
        const p = this._pivots[i];
        if (!p) return;
        if (!Array.isArray(p.params)) p.params = [];
        p.params.push({ field: '', variable: '' });
        this._rerender();
    },

    mapAll(i) {
        const p = this._pivots[i];
        if (!p) return;
        p.params = this._fields.map(f => ({ field: f, variable: f }));
        this._rerender();
    },

    removeMap(i, j) {
        const p = this._pivots[i];
        if (!p || !Array.isArray(p.params)) return;
        p.params.splice(j, 1);
        this._rerender();
    },

    updateMap(i, j, key, val) {
        const p = this._pivots[i];
        if (!p || !Array.isArray(p.params) || !p.params[j]) return;
        p.params[j][key] = val;
    },

    // ---- Persist ----

    _valid(p) {
        if (!p) return false;
        if (p.target === 'search') return !!(p.query && p.query.trim());
        return !!p.dashboard_id;
    },

    async save() {
        const widgetId = this._widgetId;
        const widget = Dashboards.getWidget(widgetId);
        if (!widget) { this.cancel(); return; }

        const pivots = this._pivots
            .filter(p => this._valid(p))
            .map(p => ({
                label: (p.label || '').trim(),
                target: p.target === 'search' ? 'search' : 'dashboard',
                dashboard_id: p.target === 'search' ? '' : (p.dashboard_id || ''),
                query: p.target === 'search' ? (p.query || '') : '',
                params: (Array.isArray(p.params) ? p.params : []).filter(m => m && m.field),
                forward_time: p.forward_time !== false,
                new_tab: !!p.new_tab
            }));

        const cfg = Dashboards.parseChartConfig(widget.chart_config) || {};
        cfg.pivots = pivots;

        try {
            const resp = await fetch(`/api/v1/dashboards/${Dashboards.currentDashboard.id}/widgets/${widgetId}`, {
                method: 'PUT',
                headers: Dashboards.sseHeaders(),
                credentials: 'include',
                body: JSON.stringify({ chart_config: cfg })
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error || 'Failed to save');
            widget.chart_config = cfg;
            this._removePanel();
            // Re-render so click handlers (or their removal) take effect.
            Dashboards.renderWidgetFromCache(widgetId);
            Dashboards.showSuccess('Pivots saved');
        } catch (err) {
            console.error('[Pivots] Failed to save:', err);
            Dashboards.showError('Failed to save pivots');
        }
    },

    cancel() { this._removePanel(); },

    _removePanel(immediate) {
        const overlay = document.getElementById('pivotOverlay');
        const panel = document.getElementById('pivotPanel');
        if (immediate) {
            if (overlay) overlay.remove();
            if (panel) panel.remove();
            return;
        }
        if (overlay) { overlay.classList.remove('open'); setTimeout(() => overlay.remove(), 200); }
        if (panel) { panel.classList.remove('open'); setTimeout(() => panel.remove(), 200); }
    }
};

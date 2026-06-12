// Analytics Models module — 4-step wizard, listing, and data viewer.
const AnalyticsModels = {
    // ---- State ----
    models: [],
    currentView: 'list',   // 'list' | 'wizard' | 'data'
    selectedModel: null,

    // Wizard state
    wizard: {
        step: 1,
        editId: null,        // set when editing an existing model
        modelType: 'rarity',
        filterRows: [],
        extractions: [],
        partitionKey: '',
        valueKey: '',
        keyFields: [],
        minSample: 5,
        alertMode: 'paused',
        alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true },
        name: '',
        description: '',
    },

    // Data viewer state
    viewer: {
        model: null,
        rows: [],
        total: 0,
        limit: 50,
        offset: 0,
        sortCol: '',
        sortDir: 'desc',
        search: '',
        tab: 'data',     // 'data' | 'config'
        stats: null,
    },

    init() {
        this._render();
    },

    show() {
        this._render();
        if (this.currentView === 'list') this._loadModels();
    },

    // ---- API helpers ----
    async _api(method, path, body) {
        const opts = { method, headers: { 'Content-Type': 'application/json' } };
        if (body !== undefined) opts.body = JSON.stringify(body);
        const data = await HttpUtils.safeFetch('/api/v1' + path, opts);
        return data;
    },

    async _apiRaw(method, path, body, contentType) {
        const opts = { method, headers: { 'Content-Type': contentType } };
        if (body !== undefined) opts.body = body;
        const data = await HttpUtils.safeFetch('/api/v1' + path, opts);
        return data;
    },

    async _loadModels() {
        try {
            const data = await this._api('GET', '/models');
            this.models = data?.data?.models || [];
            this._renderList();
        } catch (e) {
            Toast.error('Failed to load models');
        }
    },

    // ---- Top-level render ----
    _render() {
        const container = document.getElementById('modelsView');
        if (!container) return;
        switch (this.currentView) {
            case 'wizard': this._renderWizardView(container); break;
            case 'data':   this._renderDataViewerView(container); break;
            default:       this._renderListView(container); break;
        }
    },

    // ============================
    // Listing view
    // ============================
    _renderListView(container) {
        container.innerHTML = `
<div class="models-view-section">
    <div class="models-listing">
        <div class="models-filters">
            <input type="text" id="modelsSearchInput" class="models-search" placeholder="Search models...">
            <span class="filters-spacer"></span>
            <button class="btn-secondary" id="modelsImportBtn">Import YAML</button>
            <button class="btn-primary" id="modelsNewBtn">+ New Model</button>
        </div>
        <input type="file" id="modelsImportFile" accept=".yaml,.yml" style="display:none">
        <div id="modelsTableWrap" class="models-table-wrap">
            <div class="models-empty">Loading...</div>
        </div>
    </div>
</div>`;
        document.getElementById('modelsNewBtn').addEventListener('click', () => this._startWizard());
        document.getElementById('modelsImportBtn').addEventListener('click', () => document.getElementById('modelsImportFile').click());
        document.getElementById('modelsImportFile').addEventListener('change', e => this._importModel(e));
        const searchInput = document.getElementById('modelsSearchInput');
        searchInput.addEventListener('input', () => this._renderList());
        this._loadModels();
    },

    _renderList() {
        const wrap = document.getElementById('modelsTableWrap');
        if (!wrap) return;
        const q = (document.getElementById('modelsSearchInput')?.value || '').toLowerCase();
        const filtered = this.models.filter(m =>
            m.name.toLowerCase().includes(q) || m.description.toLowerCase().includes(q)
        );
        if (!filtered.length) {
            wrap.innerHTML = `<div class="models-empty">${this.models.length ? 'No models match your search.' : 'No models yet. Create one to get started.'}</div>`;
            return;
        }
        wrap.innerHTML = `
<table class="models-table">
    <thead><tr>
        <th>Name</th><th>Type</th><th>Status</th><th>Alert</th><th>Updated</th><th></th>
    </tr></thead>
    <tbody>${filtered.map(m => this._modelRow(m)).join('')}</tbody>
</table>`;
        wrap.querySelectorAll('.btn-view-data').forEach(btn => {
            btn.addEventListener('click', () => this._openDataViewer(btn.dataset.id));
        });
        wrap.querySelectorAll('.btn-model-edit').forEach(btn => {
            btn.addEventListener('click', () => this._editModel(btn.dataset.id));
        });
        wrap.querySelectorAll('.btn-model-export').forEach(btn => {
            btn.addEventListener('click', () => this._exportModel(btn.dataset.id, btn.dataset.name));
        });
        wrap.querySelectorAll('.btn-model-delete').forEach(btn => {
            btn.addEventListener('click', () => this._deleteModel(btn.dataset.id, btn.dataset.name, btn));
        });
        wrap.querySelectorAll('.alert-mode-badge[data-id]').forEach(badge => {
            badge.addEventListener('click', () => this._toggleAlertMode(badge.dataset.id, badge.dataset.mode));
        });
    },

    _modelRow(m) {
        const statusClass = { active: 'badge-active', error: 'badge-error', rebuilding: 'badge-rebuilding' }[m.status] || 'badge-none';
        const alertBadge = this._alertModeBadge(m);
        const updated = m.updated_at ? new Date(m.updated_at).toLocaleDateString() : '—';
        const errorTitle = m.status === 'error' && m.error_message ? ` title="${_esc(m.error_message)}"` : '';
        return `
<tr>
    <td><div class="model-name">${_esc(m.name)}</div><div class="model-desc">${_esc(m.description)}</div></td>
    <td>${_esc(m.model_type)}</td>
    <td><span class="model-badge ${statusClass}"${errorTitle}>${_esc(m.status)}</span></td>
    <td>${alertBadge}</td>
    <td>${updated}</td>
    <td>
        <div class="model-actions">
            <button class="btn-view-data" data-id="${m.id}">Data</button>
            <button class="btn-model-edit" data-id="${m.id}">Edit</button>
            <button class="btn-model-export" data-id="${m.id}" data-name="${_esc(m.name)}">Export</button>
            <button class="btn-model-delete btn-danger" data-id="${m.id}" data-name="${_esc(m.name)}">Delete</button>
        </div>
    </td>
</tr>`;
    },

    _alertModeBadge(m) {
        switch (m.alert_mode) {
            case 'active': return `<span class="model-badge badge-alert-active alert-mode-badge" data-id="${m.id}" data-mode="active" title="Click to pause">● Active</span>`;
            case 'paused': return `<span class="model-badge badge-paused alert-mode-badge" data-id="${m.id}" data-mode="paused" title="Click to activate">⏸ Paused</span>`;
            default:       return `<span class="model-badge badge-none">No Alert</span>`;
        }
    },

    async _toggleAlertMode(id, currentMode) {
        const endpoint = currentMode === 'active' ? '/disable-alert' : '/enable-alert';
        try {
            await this._api('POST', `/models/${id}${endpoint}`);
            await this._loadModels();
            Toast.success(currentMode === 'active' ? 'Alert paused' : 'Alert activated');
        } catch (e) {
            Toast.error('Failed to toggle alert');
        }
    },

    _exportModel(id, name) {
        const a = document.createElement('a');
        a.href = `/api/v1/models/${id}/export`;
        a.download = (name || id) + '.yaml';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    },

    async _importModel(e) {
        const file = e.target.files?.[0];
        if (!file) return;
        e.target.value = '';
        const text = await file.text();
        try {
            const data = await this._apiRaw('POST', '/models/import', text, 'application/yaml');
            await this._loadModels();
            Toast.success(`Imported "${data?.data?.model?.name || file.name}"`);
        } catch (err) {
            Toast.error('Import failed: ' + (err.message || 'unknown error'));
        }
    },

    async _deleteModel(id, name, btn) {
        if (!confirm(`Delete model "${name}"? This cannot be undone.`)) return;
        if (btn) { btn.disabled = true; btn.textContent = 'Deleting…'; }
        try {
            await this._api('DELETE', `/models/${id}`);
            await this._loadModels();
            Toast.success('Model deleted');
        } catch (e) {
            console.error('[Models] delete failed:', e);
            Toast.error('Delete failed: ' + (e.message || 'unknown error'));
            if (btn) { btn.disabled = false; btn.textContent = 'Delete'; }
        }
    },

    // ============================
    // Edit existing model (opens wizard pre-populated)
    // ============================
    _editModel(id) {
        const m = this.models.find(m => m.id === id);
        if (!m) return;
        const def = m.definition || {};
        this.wizard = {
            step: 1,
            editId: m.id,
            modelType: m.model_type || 'rarity',
            filterRows: (def.filter || []).map(f => ({ ...f })),
            extractions: (def.extractions || []).map(e => ({ ...e })),
            partitionKey: def.partition_key || '',
            valueKey: def.value_key || '',
            keyFields: (def.key_fields && def.key_fields.length) ? [...def.key_fields] : [''],
            minSample: def.min_sample || 5,
            alertMode: m.alert_mode || 'none',
            alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true },
            name: m.name,
            description: m.description || '',
        };
        this.currentView = 'wizard';
        this._render();
    },

    // ============================
    // Data viewer
    // ============================
    async _openDataViewer(id) {
        const model = this.models.find(m => m.id === id);
        if (!model) return;
        this.viewer = { model, rows: [], total: 0, limit: 50, offset: 0, sortCol: '', sortDir: 'desc', search: '', tab: 'data', stats: null };
        this.currentView = 'data';
        this._render();
        await this._loadViewerData();
        this._loadViewerStats();
    },

    async _loadViewerData() {
        const v = this.viewer;
        const params = new URLSearchParams({
            limit: v.limit, offset: v.offset,
            sort: v.sortCol, order: v.sortDir, search: v.search
        });
        try {
            const data = await this._api('GET', `/models/${v.model.id}/data?${params}`);
            v.rows = data?.data?.rows || [];
            v.total = data?.data?.total || 0;
            this._renderViewerContent();
        } catch (e) {
            console.error('[Models] loadViewerData error:', e);
            Toast.error('Failed to load model data: ' + (e?.message || String(e)));
        }
    },

    async _loadViewerStats() {
        const v = this.viewer;
        try {
            const data = await this._api('GET', `/models/${v.model.id}/stats`);
            v.stats = data?.data?.stats || null;
            this._renderStatsPanel();
        } catch (e) {
            console.error('[Models] loadViewerStats error:', e);
        }
    },

    _renderDataViewerView(container) {
        const m = this.viewer.model;
        const alertBtn = m.alert_mode === 'active'
            ? `<button id="modelDataAlertToggle" class="btn-secondary">Pause Alert</button>`
            : m.alert_mode === 'paused'
                ? `<button id="modelDataAlertToggle" class="btn-primary">Enable Alert</button>`
                : '';

        container.innerHTML = `
<div class="models-view-section model-data-viewer">
    <div class="model-data-header">
        <button class="btn-secondary" id="modelsBackBtn">← Back</button>
        <div class="model-data-title">${_esc(m.name)}</div>
        <span class="model-badge ${m.model_type === 'rarity' ? 'badge-active' : 'badge-paused'}">${_esc(m.model_type)}</span>
        <button class="btn-secondary" id="modelsEditFromViewer">Edit</button>
        ${alertBtn}
        <button class="btn-secondary" id="modelsDataRefresh">↺ Refresh</button>
    </div>
    <div id="modelsStatsPanel" class="model-stats-panel"></div>
    <div class="model-viewer-tabs">
        <button class="viewer-tab ${this.viewer.tab === 'data' ? 'active' : ''}" data-tab="data">Data</button>
        <button class="viewer-tab ${this.viewer.tab === 'config' ? 'active' : ''}" data-tab="config">Configuration</button>
        <div class="viewer-tab-spacer"></div>
        <input type="text" id="modelsDataSearch" class="models-search" placeholder="Search..." value="${_esc(this.viewer.search)}" style="${this.viewer.tab !== 'data' ? 'visibility:hidden' : ''}">
    </div>
    <div class="model-data-table-wrap" id="modelsDataTableWrap">
        <div class="models-empty">Loading...</div>
    </div>
    <div class="model-data-pagination" id="modelsDataPagination"></div>
</div>`;

        document.getElementById('modelsBackBtn').addEventListener('click', () => {
            this.currentView = 'list';
            this._render();
            this._loadModels();
        });
        document.getElementById('modelsDataRefresh').addEventListener('click', () => {
            this._loadViewerData();
            this._loadViewerStats();
        });
        document.getElementById('modelsEditFromViewer').addEventListener('click', () => {
            this._editModel(m.id);
        });
        document.getElementById('modelsDataSearch').addEventListener('input', e => {
            this.viewer.search = e.target.value;
            this.viewer.offset = 0;
            this._loadViewerData();
        });
        container.querySelectorAll('.viewer-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                this.viewer.tab = tab.dataset.tab;
                container.querySelectorAll('.viewer-tab').forEach(t => t.classList.toggle('active', t === tab));
                const searchEl = document.getElementById('modelsDataSearch');
                if (searchEl) searchEl.style.visibility = this.viewer.tab === 'data' ? '' : 'hidden';
                this._renderViewerContent();
            });
        });
        if (document.getElementById('modelDataAlertToggle')) {
            document.getElementById('modelDataAlertToggle').addEventListener('click', () => {
                const endpoint = m.alert_mode === 'active' ? '/disable-alert' : '/enable-alert';
                this._api('POST', `/models/${m.id}${endpoint}`).then(() => {
                    m.alert_mode = m.alert_mode === 'active' ? 'paused' : 'active';
                    this._renderDataViewerView(container);
                    Toast.success(m.alert_mode === 'active' ? 'Alert activated' : 'Alert paused');
                });
            });
        }
    },

    _renderViewerContent() {
        if (this.viewer.tab === 'config') {
            this._renderConfigPanel();
        } else {
            this._renderDataTable();
        }
    },

    _renderStatsPanel() {
        const el = document.getElementById('modelsStatsPanel');
        if (!el) return;
        const v = this.viewer;
        const s = v.stats;
        if (!s) { el.innerHTML = ''; return; }

        if (v.model.model_type === 'rarity') {
            const totalRows = this._fmtNum(s.total_rows);
            const parts = this._fmtNum(s.distinct_partitions);
            const topParts = Array.isArray(s.top_partitions) ? s.top_partitions : [];
            const maxCnt = topParts.reduce((m, r) => Math.max(m, Number(r.cnt || 0)), 1);

            const barsHTML = topParts.map(r => {
                const pct = Math.round(Number(r.cnt || 0) / maxCnt * 100);
                return `<div class="stat-bar-row">
                    <span class="stat-bar-label" title="${_esc(String(r.partition_val || ''))}">${_esc(String(r.partition_val || '').substring(0, 24))}</span>
                    <div class="stat-bar-track"><div class="stat-bar-fill" style="width:${pct}%"></div></div>
                    <span class="stat-bar-val">${this._fmtNum(r.cnt)}</span>
                </div>`;
            }).join('');

            el.innerHTML = `
<div class="model-stats-bar">
    <div class="stat-card">
        <div class="stat-value">${totalRows}</div>
        <div class="stat-label">Total Rows</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">${parts}</div>
        <div class="stat-label">Partitions</div>
    </div>
    ${topParts.length ? `<div class="stat-card stat-card-wide">
        <div class="stat-label">Top Partitions by Event Count</div>
        <div class="stat-bars">${barsHTML}</div>
    </div>` : ''}
</div>`;
        } else {
            // first_seen
            const hasData = Number(s.total_entities) > 0;
            const total = this._fmtNum(s.total_entities);
            const newToday = hasData ? this._fmtNum(s.new_today) : '—';
            const oldest = hasData ? this._fmtDate(s.oldest_seen) : '—';
            const newest = hasData ? this._fmtDate(s.newest_seen) : '—';

            el.innerHTML = `
<div class="model-stats-bar">
    <div class="stat-card">
        <div class="stat-value">${total}</div>
        <div class="stat-label">Total Entities</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">${newToday}</div>
        <div class="stat-label">New Today</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">${oldest}</div>
        <div class="stat-label">First Ever Seen</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">${newest}</div>
        <div class="stat-label">Most Recent</div>
    </div>
</div>`;
        }
    },

    _renderConfigPanel() {
        const wrap = document.getElementById('modelsDataTableWrap');
        if (!wrap) return;
        const m = this.viewer.model;
        const def = m.definition || {};
        const el = document.getElementById('modelsDataPagination');
        if (el) el.innerHTML = '';

        const filterHTML = (def.filter && def.filter.length)
            ? def.filter.map(f => `<div class="config-row"><code>${_esc(f.field)}</code> <span class="config-op">${_esc(f.op)}</span> <code>${_esc(f.value)}</code></div>`).join('')
            : '<div class="config-row config-empty">No filters — processes all logs</div>';

        const extHTML = (def.extractions && def.extractions.length)
            ? def.extractions.map((e, i) => `<div class="config-row">
                <span class="config-num">${i + 1}</span>
                <code>${_esc(e.from_field)}</code>
                <span class="config-op">→</span>
                <code class="config-pattern">${_esc(e.pattern)}</code>
                <span class="config-op">→</span>
                <code>${_esc(e.output_field)}</code>
                ${e.lowercase ? '<span class="config-tag">lowercase</span>' : ''}
                ${e.min_length > 0 ? `<span class="config-tag">min ${e.min_length}</span>` : ''}
            </div>`).join('')
            : '<div class="config-row config-empty">No extractions — uses raw log fields</div>';

        let shapeHTML = '';
        if (m.model_type === 'rarity') {
            shapeHTML = `
<div class="config-row"><span class="config-key">Partition key:</span> <code>${_esc(def.partition_key || '—')}</code></div>
<div class="config-row"><span class="config-key">Value key:</span> <code>${_esc(def.value_key || '—')}</code></div>
<div class="config-row"><span class="config-key">Min sample:</span> <code>${def.min_sample || 5}</code></div>`;
        } else {
            const keys = (def.key_fields || []).join(', ') || '—';
            shapeHTML = `<div class="config-row"><span class="config-key">Key fields:</span> <code>${_esc(keys)}</code></div>`;
        }

        wrap.innerHTML = `
<div class="model-config-panel">
    <div class="config-section">
        <div class="config-section-title">Filters</div>
        ${filterHTML}
    </div>
    <div class="config-section">
        <div class="config-section-title">Extractions</div>
        ${extHTML}
    </div>
    <div class="config-section">
        <div class="config-section-title">Shape</div>
        ${shapeHTML}
    </div>
    <div class="config-section">
        <div class="config-section-title">Alert</div>
        <div class="config-row"><span class="config-key">Mode:</span> <code>${_esc(m.alert_mode || 'none')}</code></div>
    </div>
    <div style="margin-top:16px">
        <button class="btn-primary" id="configEditBtn">Edit Configuration</button>
    </div>
</div>`;
        document.getElementById('configEditBtn')?.addEventListener('click', () => this._editModel(m.id));
    },

    _renderDataTable() {
        const wrap = document.getElementById('modelsDataTableWrap');
        if (!wrap) return;
        const v = this.viewer;
        const m = v.model;
        if (!v.rows.length) {
            wrap.innerHTML = '<div class="models-empty">No data yet — logs matching the model filter will appear here as they are ingested.</div>';
            this._renderPagination();
            return;
        }
        const cols = m.model_type === 'rarity'
            ? ['partition_val', 'value_val', 'model_count', 'percent', 'confidence']
            : ['entity_key', 'first_seen', 'last_seen', 'event_count'];

        const headers = cols.map(c => {
            const active = v.sortCol === c ? (v.sortDir === 'asc' ? 'sort-asc' : 'sort-desc') : '';
            return `<th class="${active}" data-col="${c}">${c} <span class="sort-icon"></span></th>`;
        }).join('');

        const rows = v.rows.map(row => {
            const cells = cols.map(c => {
                let val = row[c] ?? '';
                if (typeof val === 'number') val = val.toLocaleString(undefined, { maximumFractionDigits: 4 });
                return `<td>${_esc(String(val))}</td>`;
            }).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        wrap.innerHTML = `
<table class="model-data-table">
    <thead><tr>${headers}</tr></thead>
    <tbody>${rows}</tbody>
</table>`;

        wrap.querySelectorAll('th[data-col]').forEach(th => {
            th.addEventListener('click', () => {
                const col = th.dataset.col;
                if (v.sortCol === col) {
                    v.sortDir = v.sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    v.sortCol = col;
                    v.sortDir = 'desc';
                }
                v.offset = 0;
                this._loadViewerData();
            });
        });
        this._renderPagination();
    },

    _renderPagination() {
        const el = document.getElementById('modelsDataPagination');
        if (!el) return;
        const v = this.viewer;
        if (v.total === 0) { el.innerHTML = ''; return; }

        const page = Math.floor(v.offset / v.limit) + 1;
        const totalPages = Math.ceil(v.total / v.limit) || 1;

        const pageNums = this._paginationPages(page, totalPages);
        const pageNumsHTML = pageNums.map(p =>
            p === '...'
                ? `<button class="page-num-btn ellipsis" disabled>...</button>`
                : `<button class="page-num-btn models-page-btn${p === page ? ' active' : ''}" data-page="${p}">${p}</button>`
        ).join('');

        const pageSizeHTML = [25, 50, 100].map(s =>
            `<button class="page-size-btn models-size-btn${v.limit === s ? ' active' : ''}" data-size="${s}">${s}</button>`
        ).join('');

        el.innerHTML = `
<span class="pagination-info">${v.total.toLocaleString()} rows</span>
<div class="page-numbers">${pageNumsHTML}</div>
<div class="page-size-options">
    <span class="page-size-label">Per page</span>
    ${pageSizeHTML}
</div>`;

        el.querySelectorAll('.models-page-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                v.offset = (parseInt(btn.dataset.page) - 1) * v.limit;
                this._loadViewerData();
            });
        });
        el.querySelectorAll('.models-size-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                v.limit = parseInt(btn.dataset.size);
                v.offset = 0;
                this._loadViewerData();
            });
        });
    },

    _paginationPages(current, total) {
        if (total <= 9) return Array.from({ length: total }, (_, i) => i + 1);
        const set = new Set([1, total, current]);
        for (let i = Math.max(2, current - 1); i <= Math.min(total - 1, current + 1); i++) set.add(i);
        const sorted = Array.from(set).sort((a, b) => a - b);
        const result = [];
        let prev = 0;
        for (const p of sorted) {
            if (p - prev > 1) result.push('...');
            result.push(p);
            prev = p;
        }
        return result;
    },

    // ---- Formatting helpers ----
    _fmtNum(v) {
        const n = Number(v);
        if (isNaN(n)) return '—';
        if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e4) return (n / 1e3).toFixed(1) + 'K';
        return n.toLocaleString();
    },
    _fmtDate(v) {
        if (!v) return '—';
        try { return new Date(v).toLocaleDateString(); } catch { return String(v); }
    },

    // ============================
    // Wizard
    // ============================
    _startWizard() {
        this.wizard = {
            step: 1,
            editId: null,
            modelType: 'rarity',
            filterRows: [],
            extractions: [],
            partitionKey: '',
            valueKey: '',
            keyFields: [''],
            minSample: 5,
            alertMode: 'paused',
            alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true },
            name: '',
            description: '',
        };
        this.currentView = 'wizard';
        this._render();
    },

    _renderWizardView(container) {
        const w = this.wizard;
        const title = w.editId ? 'Edit Analytics Model' : 'New Analytics Model';
        container.innerHTML = `
<div class="models-view-section">
    <div class="models-wizard">
        <div class="wizard-header">
            <button class="btn-secondary" id="wizardCancelBtn">← Cancel</button>
            <h2>${title}</h2>
            <div id="wizardStepsContainer">${this._wizardStepsHTML()}</div>
        </div>
        <div id="wizardStepContent"></div>
        <div class="wizard-nav" id="wizardNav"></div>
    </div>
</div>`;
        document.getElementById('wizardCancelBtn').addEventListener('click', () => {
            this.currentView = 'list';
            this._render();
            this._loadModels();
        });
        this._renderWizardStep();
    },

    _wizardStepsHTML() {
        const step = this.wizard.step;
        const labels = ['Source', 'Extract', 'Shape', 'Details'];
        const dots = labels.map((label, i) => {
            const n = i + 1;
            const cls = n < step ? 'done' : n === step ? 'active' : '';
            const inner = n < step ? '✓' : n;
            return `<span class="wizard-step-dot ${cls}" title="${label}">${inner}</span>${n < 4 ? '<span class="wizard-step-line"></span>' : ''}`;
        }).join('');
        return `<div class="wizard-steps">${dots}</div>`;
    },

    _renderWizardStep() {
        const content = document.getElementById('wizardStepContent');
        const nav = document.getElementById('wizardNav');
        if (!content) return;

        const stepsContainer = document.getElementById('wizardStepsContainer');
        if (stepsContainer) stepsContainer.innerHTML = this._wizardStepsHTML();

        switch (this.wizard.step) {
            case 1: content.innerHTML = this._step1HTML(); this._bindStep1(); break;
            case 2: content.innerHTML = this._step2HTML(); this._bindStep2(); break;
            case 3: content.innerHTML = this._step3HTML(); this._bindStep3(); break;
            case 4: content.innerHTML = this._step4HTML(); this._bindStep4(); break;
        }

        const createLabel = this.wizard.editId ? 'Update Model' : 'Create Model';
        nav.innerHTML = `
${this.wizard.step > 1 ? '<button class="btn-secondary" id="wizardPrev">← Back</button>' : ''}
<span style="flex:1"></span>
${this.wizard.step < 4
    ? '<button class="btn-primary" id="wizardNext">Next →</button>'
    : `<button class="btn-primary" id="wizardCreate">${createLabel}</button>`}`;

        document.getElementById('wizardPrev')?.addEventListener('click', () => {
            this.wizard.step--;
            this._renderWizardStep();
        });
        document.getElementById('wizardNext')?.addEventListener('click', () => {
            if (this._validateStep()) {
                this.wizard.step++;
                this._renderWizardStep();
            }
        });
        document.getElementById('wizardCreate')?.addEventListener('click', () => this._createModel());
    },

    // --- Step 1: Source (model type + filter) ---
    _step1HTML() {
        const w = this.wizard;
        const isEdit = !!w.editId;
        return `
<div class="wizard-card">
    <h3>Step 1: Source</h3>
    <div class="field-group" style="margin-bottom:16px">
        <label>Model Type${isEdit ? ' <span style="font-weight:400;font-size:11px;color:var(--text-muted)">(cannot change on edit)</span>' : ''}</label>
        <div class="model-type-cards">
            <div class="model-type-card ${w.modelType === 'rarity' ? 'selected' : ''} ${isEdit ? 'model-type-card-locked' : ''}" data-type="rarity">
                <div class="card-title">Rarity</div>
                <div class="card-desc">Score how unusual a value is relative to its partition. Great for download domains, file prefixes, user-agent strings.</div>
            </div>
            <div class="model-type-card ${w.modelType === 'first_seen' ? 'selected' : ''} ${isEdit ? 'model-type-card-locked' : ''}" data-type="first_seen">
                <div class="card-title">First / Last Seen</div>
                <div class="card-desc">Track when an entity was first and last observed. Alert on new entities never seen before.</div>
            </div>
        </div>
    </div>
    <div class="field-group">
        <label>Filter (optional — restrict which logs are fed to this model)</label>
        <div class="filter-rows" id="wizardFilterRows">
            ${w.filterRows.map((f, i) => this._filterRowHTML(f, i)).join('')}
        </div>
        <button class="btn-add-row" id="addFilterRow">+ Add Filter</button>
    </div>
</div>`;
    },

    _filterRowHTML(f, i) {
        return `
<div class="filter-row" data-idx="${i}">
    <input type="text" class="filter-field" placeholder="field" value="${_esc(f.field || '')}">
    <select class="filter-op">
        <option value="=" ${f.op === '=' ? 'selected' : ''}>=</option>
        <option value="!=" ${f.op === '!=' ? 'selected' : ''}>!=</option>
        <option value="~" ${f.op === '~' ? 'selected' : ''}>~</option>
        <option value="!~" ${f.op === '!~' ? 'selected' : ''}>!~</option>
        <option value="cidr" ${f.op === 'cidr' ? 'selected' : ''}>cidr</option>
        <option value="!cidr" ${f.op === '!cidr' ? 'selected' : ''}>!cidr</option>
    </select>
    <input type="text" class="filter-value" placeholder="value" value="${_esc(f.value || '')}">
    <button class="btn-remove-row" data-idx="${i}">×</button>
</div>`;
    },

    _bindStep1() {
        const w = this.wizard;
        if (!w.editId) {
            document.querySelectorAll('.model-type-card').forEach(card => {
                card.addEventListener('click', () => {
                    w.modelType = card.dataset.type;
                    document.querySelectorAll('.model-type-card').forEach(c => c.classList.toggle('selected', c === card));
                });
            });
        }
        document.getElementById('addFilterRow').addEventListener('click', () => {
            w.filterRows.push({ field: '', op: '=', value: '' });
            this._refreshFilterRows();
        });
        this._bindFilterRowEvents();
    },

    _refreshFilterRows() {
        const container = document.getElementById('wizardFilterRows');
        if (!container) return;
        container.innerHTML = this.wizard.filterRows.map((f, i) => this._filterRowHTML(f, i)).join('');
        this._bindFilterRowEvents();
    },

    _bindFilterRowEvents() {
        const w = this.wizard;
        document.querySelectorAll('.filter-row').forEach(row => {
            const i = parseInt(row.dataset.idx);
            row.querySelector('.filter-field').addEventListener('change', e => { w.filterRows[i].field = e.target.value; });
            row.querySelector('.filter-op').addEventListener('change', e => { w.filterRows[i].op = e.target.value; });
            row.querySelector('.filter-value').addEventListener('change', e => { w.filterRows[i].value = e.target.value; });
            row.querySelector('.btn-remove-row').addEventListener('click', () => {
                w.filterRows.splice(i, 1);
                this._refreshFilterRows();
            });
        });
    },

    // --- Step 2: Extractions ---
    _step2HTML() {
        const w = this.wizard;
        return `
<div class="wizard-card">
    <h3>Step 2: Extract</h3>
    <p style="font-size:13px;color:var(--text-muted,#888);margin:0 0 12px 0">
        Use regex extractions to pull fields from raw log content. Each extraction's output becomes available to the next step.
        Leave empty to use raw log fields directly.
    </p>
    <div id="wizardExtractions">${w.extractions.map((e, i) => this._extractionCardHTML(e, i, w)).join('')}</div>
    <button class="btn-add-row" id="addExtraction">+ Add Extraction</button>
    <div id="extractionTestResult" style="margin-top:10px;display:none"></div>
</div>`;
    },

    _extractionCardHTML(ext, i, w) {
        const datalistId = `from-fields-${i}`;
        const fromOptions = this._getAvailableFields(i, w).map(f =>
            `<option value="${_esc(f)}">`
        ).join('');
        return `
<div class="extraction-card" data-idx="${i}">
    <datalist id="${datalistId}">${fromOptions}</datalist>
    <div class="extraction-card-header">
        <span class="ext-num">Extraction ${i + 1}</span>
        <button class="btn-remove-row" data-idx="${i}">× Remove</button>
        <button class="btn-secondary btn-test-ext" data-idx="${i}" style="margin-left:auto;font-size:12px;padding:3px 10px">Test</button>
    </div>
    <div class="extraction-fields">
        <div class="field-group">
            <label>From Field</label>
            <input type="text" class="ext-from field-group-input" list="${datalistId}" value="${_esc(ext.from_field || 'raw_log')}" placeholder="e.g. contents">
        </div>
        <div class="field-group">
            <label>Output Field</label>
            <input type="text" class="ext-output field-group-input" placeholder="e.g. tld" value="${_esc(ext.output_field || '')}">
        </div>
        <div class="field-group" style="grid-column:1/-1">
            <label>Regex Pattern (one capture group)</label>
            <input type="text" class="ext-pattern field-group-input" placeholder='e.g. ([^.]+\\.[^.]+)$' value="${_esc(ext.pattern || '')}" style="font-family:var(--font-mono,'IBM Plex Mono',monospace)">
        </div>
    </div>
    <div class="extraction-toggles">
        <label class="toggle-label">
            <input type="checkbox" class="ext-lowercase themed-checkbox" ${ext.lowercase ? 'checked' : ''}> Lowercase output
        </label>
        <label class="toggle-label" style="margin-left:16px">
            Min length: <input type="number" class="ext-minlen" value="${ext.min_length || 0}" min="0" style="width:50px;margin-left:4px;padding:2px 6px;background:var(--bg-secondary);border:1px solid var(--border-color);border-radius:4px;color:var(--text-primary)">
        </label>
    </div>
    <div class="extraction-test-result" id="extTestResult_${i}" style="display:none"></div>
</div>`;
    },

    _getAvailableFields(beforeIndex, w) {
        const base = ['raw_log', 'contents', 'commandline', 'target_file', 'src_ip', 'dst_ip', 'user', 'image', 'parent_process', 'process_name'];
        const extracted = w.extractions.slice(0, beforeIndex).map(e => e.output_field).filter(Boolean);
        return [...base, ...extracted];
    },

    _bindStep2() {
        const w = this.wizard;
        document.getElementById('addExtraction').addEventListener('click', () => {
            w.extractions.push({ from_field: 'raw_log', pattern: '', output_field: '', lowercase: false, min_length: 0 });
            document.getElementById('wizardExtractions').innerHTML = w.extractions.map((e, i) => this._extractionCardHTML(e, i, w)).join('');
            this._bindExtractionEvents();
        });
        this._bindExtractionEvents();
    },

    _bindExtractionEvents() {
        const w = this.wizard;
        document.querySelectorAll('.extraction-card').forEach(card => {
            const i = parseInt(card.dataset.idx);
            card.querySelector('.ext-from').addEventListener('input', e => { w.extractions[i].from_field = e.target.value; });
            card.querySelector('.ext-output').addEventListener('change', e => { w.extractions[i].output_field = e.target.value; });
            card.querySelector('.ext-pattern').addEventListener('change', e => { w.extractions[i].pattern = e.target.value; });
            card.querySelector('.ext-lowercase').addEventListener('change', e => { w.extractions[i].lowercase = e.target.checked; });
            card.querySelector('.ext-minlen').addEventListener('change', e => { w.extractions[i].min_length = parseInt(e.target.value) || 0; });
            card.querySelector('.btn-remove-row').addEventListener('click', () => {
                w.extractions.splice(i, 1);
                document.getElementById('wizardExtractions').innerHTML = w.extractions.map((ex, idx) => this._extractionCardHTML(ex, idx, w)).join('');
                this._bindExtractionEvents();
            });
            card.querySelector('.btn-test-ext').addEventListener('click', () => this._testExtraction(i));
        });
    },

    async _testExtraction(upToIndex) {
        const w = this.wizard;
        const exts = w.extractions.slice(0, upToIndex + 1).filter(e => e.pattern && e.output_field);
        if (!exts.length) { Toast.warning('Fill in pattern and output field first'); return; }
        const resultEl = document.getElementById(`extTestResult_${upToIndex}`);
        if (resultEl) { resultEl.style.display = 'block'; resultEl.innerHTML = '<em>Testing...</em>'; }
        try {
            const data = await this._api('POST', '/models/test-extraction', {
                filter: w.filterRows.filter(f => f.field && f.value),
                extractions: exts
            });
            const results = data?.data?.results || [];
            const sql = data?.data?.sql || '';
            const outField = exts[exts.length - 1]?.output_field || 'value';
            const sqlBlock = sql ? `<details style="margin-top:6px"><summary style="cursor:pointer;color:var(--text-muted,#888);font-size:11px">Show SQL</summary><pre style="margin:4px 0 0;white-space:pre-wrap;font-size:11px;color:var(--text-muted,#888)">${_esc(sql)}</pre></details>` : '';
            if (!results.length) {
                if (resultEl) resultEl.innerHTML = `No matches found in recent logs.${sqlBlock}`;
                return;
            }
            const preview = results.slice(0, 5).map(r => `${r[outField]} (${r.cnt})`).join(', ');
            if (resultEl) resultEl.innerHTML = `${results.length} distinct values &middot; e.g. ${_esc(preview)}${sqlBlock}`;
        } catch (e) {
            if (resultEl) resultEl.innerHTML = `Error: ${_esc(e.message || 'test failed')}`;
        }
    },

    // --- Step 3: Shape ---
    _step3HTML() {
        const w = this.wizard;
        const allFields = [...this._getAvailableFields(w.extractions.length, w)];
        const fieldOpts = allFields.map(f => `<option value="${_esc(f)}">${_esc(f)}</option>`).join('');

        if (w.modelType === 'rarity') {
            return `
<div class="wizard-card">
    <h3>Step 3: Shape (Rarity)</h3>
    <div class="shape-grid">
        <div class="field-group">
            <label>Partition Key (group by)</label>
            <select id="shapePartKey">${fieldOpts}</select>
        </div>
        <div class="field-group">
            <label>Value Key (rarity of what?)</label>
            <select id="shapeValKey">${fieldOpts}</select>
        </div>
        <div class="field-group">
            <label>Min sample size</label>
            <input type="number" id="shapeMinSample" value="${w.minSample}" min="1" style="padding:5px 8px;background:var(--bg-primary,#0f0f1a);border:1px solid var(--border-color);border-radius:4px;color:var(--text-primary)">
        </div>
    </div>
    <p style="font-size:12px;color:var(--text-muted,#888);margin-top:12px">
        Example: Partition=<em>file_prefix</em>, Value=<em>tld</em> → scores how unusual a TLD is for a given file prefix.
    </p>
</div>`;
        } else {
            return `
<div class="wizard-card">
    <h3>Step 3: Shape (First/Last Seen)</h3>
    <div class="field-group">
        <label>Key Fields (entity to track)</label>
        <div id="keyFieldsList">${w.keyFields.map((kf, i) => `
<div class="filter-row" data-idx="${i}" style="margin-bottom:6px">
    <select class="key-field-sel">${fieldOpts.replace(`value="${_esc(kf)}"`, `value="${_esc(kf)}" selected`)}</select>
    <button class="btn-remove-row" data-idx="${i}">×</button>
</div>`).join('')}</div>
        <button class="btn-add-row" id="addKeyField">+ Add Key Field</button>
    </div>
    <p style="font-size:12px;color:var(--text-muted,#888);margin-top:12px">
        Example: Key=<em>fields.src_ip</em> → tracks when each IP was first and last seen.
        Multiple key fields are concatenated into a composite key.
    </p>
</div>`;
        }
    },

    _bindStep3() {
        const w = this.wizard;
        if (w.modelType === 'rarity') {
            const pSel = document.getElementById('shapePartKey');
            const vSel = document.getElementById('shapeValKey');
            if (w.partitionKey) pSel.value = w.partitionKey;
            if (w.valueKey) vSel.value = w.valueKey;
            pSel?.addEventListener('change', e => { w.partitionKey = e.target.value; });
            vSel?.addEventListener('change', e => { w.valueKey = e.target.value; });
            document.getElementById('shapeMinSample')?.addEventListener('change', e => { w.minSample = parseInt(e.target.value) || 5; });
            if (pSel && !w.partitionKey) w.partitionKey = pSel.value;
            if (vSel && !w.valueKey) w.valueKey = vSel.value;
        } else {
            this._bindKeyFieldEvents();
            document.getElementById('addKeyField')?.addEventListener('click', () => {
                w.keyFields.push('');
                this._refreshKeyFields();
            });
        }
    },

    _refreshKeyFields() {
        const w = this.wizard;
        const allFields = this._getAvailableFields(w.extractions.length, w);
        const fieldOpts = allFields.map(f => `<option value="${_esc(f)}">${_esc(f)}</option>`).join('');
        const list = document.getElementById('keyFieldsList');
        if (!list) return;
        list.innerHTML = w.keyFields.map((kf, i) => `
<div class="filter-row" data-idx="${i}" style="margin-bottom:6px">
    <select class="key-field-sel">${fieldOpts}</select>
    <button class="btn-remove-row" data-idx="${i}">×</button>
</div>`).join('');
        this._bindKeyFieldEvents();
    },

    _bindKeyFieldEvents() {
        const w = this.wizard;
        document.querySelectorAll('.key-field-sel').forEach((sel, i) => {
            if (w.keyFields[i]) sel.value = w.keyFields[i];
            sel.addEventListener('change', e => { w.keyFields[i] = e.target.value; });
            if (!w.keyFields[i]) w.keyFields[i] = sel.value;
        });
        document.querySelectorAll('#keyFieldsList .btn-remove-row').forEach(btn => {
            btn.addEventListener('click', () => {
                w.keyFields.splice(parseInt(btn.dataset.idx), 1);
                this._refreshKeyFields();
            });
        });
    },

    // --- Step 4: Name & Create ---
    _step4HTML() {
        const w = this.wizard;
        return `
<div class="wizard-card">
    <h3>Step 4: ${w.editId ? 'Name &amp; Update' : 'Name &amp; Create'}</h3>
    <div class="form-row">
        <div class="field-group" style="flex:2">
            <label>Model Name</label>
            <input type="text" id="wizardName" class="full-input" placeholder="e.g. download_domain_rarity" value="${_esc(w.name)}">
        </div>
    </div>
    <div class="field-group" style="margin-top:10px">
        <label>Description (optional)</label>
        <textarea id="wizardDesc" class="full-input" rows="2">${_esc(w.description)}</textarea>
    </div>
    <div class="field-group" style="margin-top:16px">
        <label>Alert Mode</label>
        <div class="alert-mode-options">
            <label class="alert-mode-option ${w.alertMode === 'none' ? 'selected' : ''}">
                <input type="radio" name="alertMode" value="none" ${w.alertMode === 'none' ? 'checked' : ''}>
                <div class="alert-mode-text">
                    <div class="mode-title">Collect data only (no alert)</div>
                    <div class="mode-desc">Model runs silently. View data anytime to see what it's learning.</div>
                </div>
            </label>
            <label class="alert-mode-option ${w.alertMode === 'paused' ? 'selected' : ''}">
                <input type="radio" name="alertMode" value="paused" ${w.alertMode === 'paused' ? 'checked' : ''}>
                <div class="alert-mode-text">
                    <div class="mode-title">Create alert — paused <em style="color:var(--accent-primary);font-style:normal">(Recommended)</em></div>
                    <div class="mode-desc">Alert is created but won't fire. Enable it when the model has baked.</div>
                </div>
            </label>
            <label class="alert-mode-option ${w.alertMode === 'active' ? 'selected' : ''}">
                <input type="radio" name="alertMode" value="active" ${w.alertMode === 'active' ? 'checked' : ''}>
                <div class="alert-mode-text">
                    <div class="mode-title">Create alert — active</div>
                    <div class="mode-desc">Alert fires immediately when threshold is exceeded.</div>
                </div>
            </label>
        </div>
    </div>
    ${w.alertMode !== 'none' ? this._alertConfigHTML(w) : '<div id="alertConfigSection"></div>'}
</div>`;
    },

    _alertConfigHTML(w) {
        const c = w.alertConfig;
        const isRarity = w.modelType === 'rarity';
        return `
<div class="alert-config-section" id="alertConfigSection">
    <div class="field-group">
        <label>Severity</label>
        <select id="alertSeverity">
            ${['low','medium','high','critical'].map(s => `<option ${c.severity === s ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
    </div>
    ${isRarity ? `
    <div class="form-row">
        <div class="field-group">
            <label>Min Confidence</label>
            <input type="number" id="alertConfidence" value="${c.confidence_threshold}" min="0" max="1" step="0.05" style="width:80px;padding:5px 8px;background:var(--bg-secondary);border:1px solid var(--border-color);border-radius:4px;color:var(--text-primary)">
        </div>
        <div class="field-group">
            <label>Max % Threshold</label>
            <input type="number" id="alertPercent" value="${c.percent_threshold}" min="0.1" max="100" step="0.5" style="width:80px;padding:5px 8px;background:var(--bg-secondary);border:1px solid var(--border-color);border-radius:4px;color:var(--text-primary)">
        </div>
    </div>` : `
    <label class="toggle-label">
        <input type="checkbox" id="alertOnNew" ${c.alert_on_new ? 'checked' : ''}> Alert on new entities only
    </label>`}
</div>`;
    },

    _bindStep4() {
        const w = this.wizard;
        document.getElementById('wizardName').addEventListener('input', e => { w.name = e.target.value; });
        document.getElementById('wizardDesc').addEventListener('input', e => { w.description = e.target.value; });
        document.querySelectorAll('input[name=alertMode]').forEach(radio => {
            radio.addEventListener('change', e => {
                w.alertMode = e.target.value;
                document.querySelectorAll('.alert-mode-option').forEach(opt => {
                    opt.classList.toggle('selected', opt.querySelector('input').value === w.alertMode);
                });
                const configSection = document.getElementById('alertConfigSection');
                if (configSection) {
                    configSection.outerHTML = w.alertMode !== 'none' ? this._alertConfigHTML(w) : '<div id="alertConfigSection"></div>';
                    this._bindAlertConfigEvents();
                }
            });
        });
        this._bindAlertConfigEvents();
    },

    _bindAlertConfigEvents() {
        const c = this.wizard.alertConfig;
        document.getElementById('alertSeverity')?.addEventListener('change', e => { c.severity = e.target.value; });
        document.getElementById('alertConfidence')?.addEventListener('change', e => { c.confidence_threshold = parseFloat(e.target.value); });
        document.getElementById('alertPercent')?.addEventListener('change', e => { c.percent_threshold = parseFloat(e.target.value); });
        document.getElementById('alertOnNew')?.addEventListener('change', e => { c.alert_on_new = e.target.checked; });
    },

    _validateStep() {
        const w = this.wizard;
        switch (w.step) {
            case 1: return true;
            case 2: return true;
            case 3:
                if (w.modelType === 'rarity' && (!w.partitionKey || !w.valueKey)) {
                    Toast.warning('Select partition key and value key');
                    return false;
                }
                if (w.modelType === 'first_seen' && (!w.keyFields.length || !w.keyFields[0])) {
                    Toast.warning('Add at least one key field');
                    return false;
                }
                return true;
            case 4:
                if (!w.name.trim()) {
                    Toast.warning('Model name is required');
                    return false;
                }
                return true;
        }
        return true;
    },

    async _createModel() {
        const w = this.wizard;
        if (!this._validateStep()) return;

        const def = {
            filter: w.filterRows.filter(f => f.field && f.value),
            extractions: w.extractions.filter(e => e.pattern && e.output_field),
            min_sample: w.minSample,
        };
        if (w.modelType === 'rarity') {
            def.partition_key = w.partitionKey;
            def.value_key = w.valueKey;
        } else {
            def.key_fields = w.keyFields.filter(Boolean);
        }
        if (w.alertMode !== 'none') {
            def.alert = { ...w.alertConfig };
        }

        const btn = document.getElementById('wizardCreate');
        if (btn) btn.disabled = true;

        try {
            if (w.editId) {
                await this._api('PUT', `/models/${w.editId}`, {
                    name: w.name.trim(),
                    description: w.description.trim(),
                    definition: def,
                    alert_mode: w.alertMode,
                });
                Toast.success('Model updated');
            } else {
                await this._api('POST', '/models', {
                    name: w.name.trim(),
                    description: w.description.trim(),
                    model_type: w.modelType,
                    definition: def,
                    alert_mode: w.alertMode,
                });
                Toast.success('Model created');
            }
            this.currentView = 'list';
            this._render();
            await this._loadModels();
        } catch (e) {
            Toast.error(e.message || 'Failed to save model');
            if (btn) btn.disabled = false;
        }
    },
};

window.AnalyticsModels = AnalyticsModels;

// HTML-escape helper (shared with other modules in this codebase)
function _esc(str) {
    if (str === null || str === undefined) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

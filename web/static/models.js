// Analytics Models module — BQL-first split-panel editor, listing, and data viewer.
const AnalyticsModels = {
    // ---- State ----
    models: [],
    currentView: 'list',   // 'list' | 'editor' | 'data'
    selectedModel: null,
    _runSeq: 0,            // monotonic token so only the latest _runQuery renders
    _viewerPoll: null,     // interval: poll the open model while its backfill runs
    _listPoll: null,       // interval: poll the listing while any backfill runs
    BACKFILL_WINDOWS: [['24h', 'Last 24h'], ['7d', 'Last 7 days'], ['30d', 'Last 30 days'], ['90d', 'Last 90 days']],
    EXAMPLE_QUERY: 'level = "dns" | regex(field=raw_log, regex="([a-z]+)$", as=tld) | lowercase(tld)',

    // Editor state (split-panel: BQL source query on the left, shape/alert on the right)
    editor: {
        editId: null,        // set when editing an existing model
        modelType: 'rarity',
        query: '',           // BQL source query (filter + regex extractions)
        parsed: { filter: [], extractions: [], candidate_fields: [], errors: [], warnings: [] },
        partitionKey: '',
        valueKey: '',
        keyFields: [''],
        minSample: 5,
        timeBucket: 'day',
        alertMode: 'paused',
        alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true, z_threshold: 3.5 },
        name: '',
        description: '',
        timeRange: '24h',
        fieldOrder: null,
        resultFields: [],
        results: [],
        ran: false,
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
        // Re-entering the Models tab always returns to the listing. currentView and
        // the data-viewer/editor state are module-level, so without this reset a tab
        // switch would re-render the previously opened model with stale (empty) data.
        this.currentView = 'list';
        this.selectedModel = null;
        this.viewer.model = null;
        this._render();
    },

    // Stop all polling when the models tab is hidden (called from app.js).
    teardown() {
        this._stopViewerPoll();
        this._stopListPoll();
    },

    _backfillPct(m) {
        const total = Number(m.backfill_total || 0);
        if (total <= 0) return 0;
        return Math.min(100, Math.round(Number(m.backfill_done || 0) / total * 100));
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
            case 'editor': this._renderEditorView(container); break;
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
        document.getElementById('modelsNewBtn').addEventListener('click', () => this._startEditor());
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

        // Keep the listing live while any model is seeding.
        if (this.models.some(m => m.backfill_status === 'running')) this._startListPoll();
        else this._stopListPoll();
    },

    _startListPoll() {
        if (this._listPoll) return;
        this._listPoll = setInterval(async () => {
            if (this.currentView !== 'list') { this._stopListPoll(); return; }
            try {
                const data = await this._api('GET', '/models');
                this.models = data?.data?.models || [];
                this._renderList();
            } catch (e) { /* transient; keep polling */ }
        }, 3000);
    },

    _stopListPoll() {
        if (this._listPoll) { clearInterval(this._listPoll); this._listPoll = null; }
    },

    _modelRow(m) {
        const statusClass = { active: 'badge-active', error: 'badge-error', rebuilding: 'badge-rebuilding' }[m.status] || 'badge-none';
        const alertBadge = this._alertModeBadge(m);
        const updated = m.updated_at ? new Date(m.updated_at).toLocaleDateString() : '—';
        const errorTitle = m.status === 'error' && m.error_message ? ` title="${_esc(m.error_message)}"` : '';
        const backfillBadge = m.backfill_status === 'running'
            ? ` <span class="model-badge badge-backfilling" title="Seeding historical data">⟳ Seeding ${this._backfillPct(m)}%</span>`
            : '';
        return `
<tr>
    <td><div class="model-name">${_esc(m.name)}</div><div class="model-desc">${_esc(m.description)}</div></td>
    <td>${_esc(m.model_type)}</td>
    <td><span class="model-badge ${statusClass}"${errorTitle}>${_esc(m.status)}</span>${backfillBadge}</td>
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
    // Edit existing model (opens the editor pre-populated)
    // ============================
    _editModel(id) {
        const m = this.models.find(m => m.id === id);
        if (!m) return;
        const def = m.definition || {};
        const alertCfg = { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true, z_threshold: 3.5 };
        if (def.alert) Object.assign(alertCfg, def.alert);
        this.editor = {
            editId: m.id,
            modelType: m.model_type || 'rarity',
            query: m.source_query || '',
            parsed: { filter: (def.filter || []).map(f => ({ ...f })), extractions: (def.extractions || []).map(e => ({ ...e })), candidate_fields: [], errors: [], warnings: [] },
            partitionKey: def.partition_key || '',
            valueKey: def.value_key || '',
            keyFields: (def.key_fields && def.key_fields.length) ? [...def.key_fields] : [''],
            minSample: def.min_sample || 5,
            timeBucket: def.time_bucket || 'day',
            alertMode: m.alert_mode || 'none',
            alertConfig: alertCfg,
            name: m.name,
            description: m.description || '',
            timeRange: '24h',
            fieldOrder: null,
            resultFields: [],
            results: [],
            ran: false,
        };
        this.currentView = 'editor';
        this._render();
    },

    // ============================
    // Data viewer
    // ============================
    async _openDataViewer(id) {
        const model = this.models.find(m => m.id === id);
        if (!model) return;
        this._stopListPoll();
        this.viewer = { model, rows: [], total: 0, limit: 50, offset: 0, sortCol: '', sortDir: 'desc', search: '', tab: 'data', stats: null, backfillWindow: '7d' };
        this.currentView = 'data';
        this._render();
        await this._loadViewerData();
        this._loadViewerStats();
        if (model.backfill_status === 'running') this._startViewerPoll();
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
    <div id="modelsBackfillBar" class="model-backfill-bar"></div>
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
            this._stopViewerPoll();
            this.currentView = 'list';
            this._render();
            this._loadModels();
        });
        this._renderBackfillBar();
        // Resume progress polling if a backfill is running (e.g. after returning
        // to this view from another tab). _startViewerPoll guards against dupes.
        if (this.viewer.model && this.viewer.model.backfill_status === 'running') this._startViewerPoll();
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

    // ---- Backfill (seed historical data) ----
    _renderBackfillBar() {
        const el = document.getElementById('modelsBackfillBar');
        if (!el) return;
        const m = this.viewer.model;
        const st = m.backfill_status || 'none';

        // Backfill is only available once the model's table+MV exist.
        if (m.status !== 'active' && st !== 'running') {
            el.className = 'model-backfill-bar';
            el.innerHTML = `<div class="backfill-cta"><span class="backfill-note">Model is initializing…</span></div>`;
            return;
        }

        if (st === 'running') {
            const pct = this._backfillPct(m);
            el.className = 'model-backfill-bar active';
            el.innerHTML = `
<div class="backfill-running">
    <div class="backfill-running-head">
        <span class="spinner spinner-inline"></span>
        <span class="backfill-label">Seeding history…</span>
        <span class="backfill-count">${m.backfill_done || 0}/${m.backfill_total || 0} days</span>
        <span class="backfill-spacer"></span>
        <button class="btn-secondary btn-sm" id="backfillCancelBtn">Cancel</button>
    </div>
    <div class="stat-bar-track"><div class="stat-bar-fill" style="width:${pct}%"></div></div>
</div>`;
            document.getElementById('backfillCancelBtn')?.addEventListener('click', () => this._cancelBackfill());
            return;
        }

        el.className = 'model-backfill-bar';

        // Completed: terminal, no re-seed (re-running would double-count). To
        // seed again the user edits the model, which resets and drops data.
        if (st === 'completed') {
            el.innerHTML = `<div class="backfill-cta"><span class="backfill-note ok">✓ Seeded ${_esc(m.backfill_window || 'history')} of history</span></div>`;
            return;
        }

        // Failed / cancelled: offer Resume (continues from the saved cursor; no
        // window picker, no double-count of already-seeded days).
        if (st === 'failed' || st === 'cancelled') {
            const note = st === 'failed'
                ? `<span class="backfill-note err" title="${_esc(m.backfill_error || '')}">Backfill failed at ${m.backfill_done || 0}/${m.backfill_total || 0} days</span>`
                : `<span class="backfill-note">Backfill cancelled at ${m.backfill_done || 0}/${m.backfill_total || 0} days</span>`;
            el.innerHTML = `
<div class="backfill-cta">
    ${note}
    <span class="backfill-spacer"></span>
    <button class="btn-primary btn-sm" id="backfillStartBtn">Resume</button>
</div>`;
            document.getElementById('backfillStartBtn')?.addEventListener('click', () => this._startBackfill());
            return;
        }

        // Fresh: window picker + seed.
        const win = this.viewer.backfillWindow || '7d';
        const pills = this.BACKFILL_WINDOWS.map(([v, l]) =>
            `<button class="backfill-pill ${v === win ? 'selected' : ''}" data-win="${v}">${l.replace('Last ', '')}</button>`
        ).join('');
        el.innerHTML = `
<div class="backfill-cta">
    <span class="backfill-note">This model captures new logs from now on. Seed it with historical data.</span>
    <span class="backfill-spacer"></span>
    <div class="backfill-pills">${pills}</div>
    <button class="btn-primary btn-sm" id="backfillStartBtn">Seed history</button>
</div>`;
        el.querySelectorAll('.backfill-pill').forEach(p => {
            p.addEventListener('click', () => {
                this.viewer.backfillWindow = p.dataset.win;
                el.querySelectorAll('.backfill-pill').forEach(x => x.classList.toggle('selected', x === p));
            });
        });
        document.getElementById('backfillStartBtn')?.addEventListener('click', () => this._startBackfill());
    },

    async _startBackfill() {
        const m = this.viewer.model;
        const window = this.viewer.backfillWindow || '7d';
        const btn = document.getElementById('backfillStartBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Starting…'; }
        try {
            const data = await this._api('POST', `/models/${m.id}/backfill`, { window });
            if (data?.data?.model) this.viewer.model = data.data.model;
            else m.backfill_status = 'running';
            this._renderBackfillBar();
            this._startViewerPoll();
            Toast.success('Backfill started');
        } catch (e) {
            Toast.error('Failed to start backfill: ' + (e?.message || 'error'));
            this._renderBackfillBar();
        }
    },

    async _cancelBackfill() {
        const m = this.viewer.model;
        const btn = document.getElementById('backfillCancelBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Cancelling…'; }
        try {
            await this._api('POST', `/models/${m.id}/backfill/cancel`);
            Toast.success('Backfill cancelling…');
            this._refreshViewerModel();
        } catch (e) {
            Toast.error('Failed to cancel backfill');
            if (btn) { btn.disabled = false; btn.textContent = 'Cancel'; }
        }
    },

    _startViewerPoll() {
        if (this._viewerPoll) return;
        this._viewerPoll = setInterval(() => this._refreshViewerModel(), 3000);
    },

    _stopViewerPoll() {
        if (this._viewerPoll) { clearInterval(this._viewerPoll); this._viewerPoll = null; }
    },

    async _refreshViewerModel() {
        if (this.currentView !== 'data' || !this.viewer.model) { this._stopViewerPoll(); return; }
        const id = this.viewer.model.id;
        let model;
        try {
            const data = await this._api('GET', `/models/${id}`);
            model = data?.data?.model;
        } catch (e) { return; /* transient; keep polling */ }
        if (!model) return;
        const prev = this.viewer.model.backfill_status;
        this.viewer.model = model;
        this._renderBackfillBar();
        if (model.backfill_status !== 'running') {
            this._stopViewerPoll();
            if (prev === 'running') {
                // Just finished: refresh the rows + stats once so seeded data shows.
                this._loadViewerData();
                this._loadViewerStats();
                if (model.backfill_status === 'completed') Toast.success('Backfill complete');
                else if (model.backfill_status === 'failed') Toast.error('Backfill failed: ' + (model.backfill_error || 'error'));
            }
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
        } else if (v.model.model_type === 'volume_baseline') {
            const hasData = Number(s.total_entities) > 0;
            const total = this._fmtNum(s.total_entities);
            const anomalous = hasData ? this._fmtNum(s.anomalous) : '—';
            const maxZ = hasData && s.max_z != null ? Number(s.max_z).toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—';

            el.innerHTML = `
<div class="model-stats-bar">
    <div class="stat-card">
        <div class="stat-value">${total}</div>
        <div class="stat-label">Entities Scored</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">${anomalous}</div>
        <div class="stat-label">Anomalous</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">${maxZ}</div>
        <div class="stat-label">Max |Z-score|</div>
    </div>
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
        } else if (m.model_type === 'volume_baseline') {
            const keys = (def.key_fields || []).join(', ') || '—';
            shapeHTML = `
<div class="config-row"><span class="config-key">Entity fields:</span> <code>${_esc(keys)}</code></div>
<div class="config-row"><span class="config-key">Bucket:</span> <code>${_esc(def.time_bucket || 'day')}</code></div>
<div class="config-row"><span class="config-key">Min history:</span> <code>${def.min_sample || 7}</code></div>`;
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
            const seeding = m.backfill_status === 'running';
            wrap.innerHTML = seeding
                ? '<div class="models-empty">Seeding historical data… rows will appear as each day completes.</div>'
                : '<div class="models-empty">No data yet — seed historical data above, or new matching logs will appear here as they are ingested.</div>';
            this._renderPagination();
            return;
        }
        const cols = m.model_type === 'rarity'
            ? ['partition_val', 'value_val', 'model_count', 'percent', 'confidence']
            : m.model_type === 'volume_baseline'
                ? ['entity_val', 'latest_count', 'baseline_median', 'mad', 'z_score', 'n_buckets', 'latest_bucket']
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
    // Editor (split-panel, BQL-first)
    // ============================
    BASE_FIELDS: ['raw_log', 'contents', 'commandline', 'target_file', 'src_ip', 'dst_ip', 'user', 'image', 'parent_process', 'process_name'],

    _startEditor() {
        this.editor = {
            editId: null,
            modelType: 'rarity',
            query: '',
            parsed: { filter: [], extractions: [], candidate_fields: [], errors: [], warnings: [] },
            partitionKey: '',
            valueKey: '',
            keyFields: [''],
            minSample: 5,
            timeBucket: 'day',
            alertMode: 'paused',
            alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true, z_threshold: 3.5 },
            name: '',
            description: '',
            timeRange: '24h',
            fieldOrder: null,
            resultFields: [],
            results: [],
            ran: false,
        };
        this.currentView = 'editor';
        this._render();
    },

    _renderEditorView(container) {
        const e = this.editor;
        const title = e.editId ? 'Edit Analytics Model' : 'New Analytics Model';
        const saveLabel = e.editId ? 'Update Model' : 'Create Model';
        const ranges = [['1h', 'Last 1h'], ['6h', 'Last 6h'], ['24h', 'Last 24h'], ['7d', 'Last 7d'], ['30d', 'Last 30d']];
        container.innerHTML = `
<div class="models-view-section model-editor">
    <div class="model-editor-header">
        <button class="btn-secondary" id="modelEditorCancel">← Cancel</button>
        <h2>${title}</h2>
        <span style="flex:1"></span>
        <button class="btn-primary" id="modelEditorSave">${saveLabel}</button>
    </div>
    <div class="model-editor-body">
        <div class="model-editor-left">
            <div class="model-editor-toolbar">
                <select id="modelTimeRange" class="model-time-select">
                    ${ranges.map(([v, l]) => `<option value="${v}" ${e.timeRange === v ? 'selected' : ''}>${l}</option>`).join('')}
                </select>
                <button class="btn-primary" id="modelRunBtn">Run</button>
                <span style="flex:1"></span>
                <span class="model-results-count" id="modelResultsCount"></span>
            </div>
            <div class="model-query-wrap">
                <div id="modelQueryHighlight" class="model-query-highlight"></div>
                <textarea id="modelQueryInput" class="model-query-input" spellcheck="false" placeholder='Filter logs in BQL, or leave empty to use all logs'>${_esc(e.query)}</textarea>
            </div>
            <div class="model-query-hint">
                <span>Narrow with a BQL filter and pull fields out with <code>regex()</code>.</span>
                <button type="button" class="model-example-link" id="modelInsertExample">Insert example</button>
            </div>
            <pre id="modelSqlOutput" class="model-sql-output" style="display:${this._showSQL() ? 'block' : 'none'}"></pre>
            <div id="modelTranslation" class="model-translation"></div>
            <div id="modelQueryResults" class="model-query-results"><div class="models-empty">Run the query to preview matching logs and extracted fields.</div></div>
        </div>
        <div class="model-editor-right">
            <div class="config-section">
                <div class="config-section-title">Model Type</div>
                <div class="model-type-cards model-type-cards-compact" id="modelTypeCards">
                    <div class="model-type-card ${e.modelType === 'rarity' ? 'selected' : ''} ${e.editId ? 'model-type-card-locked' : ''}" data-type="rarity">
                        <div class="card-title">Rarity</div>
                        <div class="card-desc">Score how unusual a value is within its partition.</div>
                    </div>
                    <div class="model-type-card ${e.modelType === 'first_seen' ? 'selected' : ''} ${e.editId ? 'model-type-card-locked' : ''}" data-type="first_seen">
                        <div class="card-title">First / Last Seen</div>
                        <div class="card-desc">Track when an entity was first and last observed.</div>
                    </div>
                    <div class="model-type-card ${e.modelType === 'volume_baseline' ? 'selected' : ''} ${e.editId ? 'model-type-card-locked' : ''}" data-type="volume_baseline">
                        <div class="card-title">Volume Baseline</div>
                        <div class="card-desc">Flag entities whose volume deviates from their own history (modified z-score).</div>
                    </div>
                </div>
            </div>
            <div class="config-section">
                <div class="config-section-title">Shape</div>
                <div id="modelShapeConfig">${this._editorShapeHTML()}</div>
            </div>
            <div class="config-section">
                <div class="config-section-title">Details</div>
                <div class="field-group">
                    <label>Model Name</label>
                    <input type="text" id="modelName" class="full-input" placeholder="e.g. download_domain_rarity" value="${_esc(e.name)}">
                </div>
                <div class="field-group" style="margin-top:10px">
                    <label>Description (optional)</label>
                    <textarea id="modelDesc" class="full-input" rows="2">${_esc(e.description)}</textarea>
                </div>
            </div>
            <div class="config-section">
                <div class="config-section-title">Alert</div>
                <div class="alert-mode-options">
                    ${this._editorAlertModeOptionsHTML()}
                </div>
                <div id="modelAlertConfig">${e.alertMode !== 'none' ? this._editorAlertConfigHTML() : ''}</div>
            </div>
        </div>
    </div>
</div>`;

        document.getElementById('modelEditorCancel').addEventListener('click', () => {
            this.currentView = 'list';
            this._render();
            this._loadModels();
        });
        document.getElementById('modelEditorSave').addEventListener('click', () => this._saveModel());
        document.getElementById('modelRunBtn').addEventListener('click', () => this._runQuery());
        const ta = document.getElementById('modelQueryInput');
        ta.addEventListener('input', ev => { e.query = ev.target.value; this._updateQueryHighlight(); });
        ta.addEventListener('scroll', () => this._syncQueryHighlightScroll());
        ta.addEventListener('keydown', ev => {
            if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') { ev.preventDefault(); this._runQuery(); }
        });
        this._updateQueryHighlight();
        document.getElementById('modelInsertExample')?.addEventListener('click', () => {
            ta.value = this.EXAMPLE_QUERY;
            e.query = this.EXAMPLE_QUERY;
            this._updateQueryHighlight();
            ta.focus();
        });
        document.getElementById('modelTimeRange').addEventListener('change', ev => { e.timeRange = ev.target.value; if (e.ran) this._runQuery(); });
        if (!e.editId) {
            document.querySelectorAll('#modelTypeCards .model-type-card').forEach(card => {
                card.addEventListener('click', () => {
                    e.modelType = card.dataset.type;
                    document.querySelectorAll('#modelTypeCards .model-type-card').forEach(c => c.classList.toggle('selected', c === card));
                    this._renderEditorShape();
                    this._renderEditorAlertConfig();
                });
            });
        }
        this._bindEditorDetails();
        this._bindEditorShape();
        this._renderTranslation();

        // Seed an initial run when editing (source query is pre-filled).
        if (e.editId && (e.query || '').trim()) {
            this._runQuery();
        }
    },

    // Whether to surface the translated ClickHouse SQL (driven by the user's
    // "Show Query Debug" profile preference, shared with the main search view).
    _showSQL() {
        return !!(window.UserPrefs && UserPrefs.showSQL());
    },

    // ---- BQL syntax highlighting (overlay over the query textarea) ----
    _updateQueryHighlight() {
        const ta = document.getElementById('modelQueryInput');
        const hl = document.getElementById('modelQueryHighlight');
        if (!ta || !hl || !window.SyntaxHighlight) return;
        hl.innerHTML = SyntaxHighlight.highlight(ta.value) + '<br/>';
        this._syncQueryHighlightScroll();
    },

    _syncQueryHighlightScroll() {
        const ta = document.getElementById('modelQueryInput');
        const hl = document.getElementById('modelQueryHighlight');
        if (!ta || !hl) return;
        hl.scrollTop = ta.scrollTop;
        hl.scrollLeft = ta.scrollLeft;
    },

    // ---- Field option helpers ----
    _editorAllFields(extra) {
        const e = this.editor;
        const seen = new Set();
        const out = [];
        const add = f => { if (f && !seen.has(f)) { seen.add(f); out.push(f); } };
        // Fields discovered in the most recent query results come first: these
        // are the columns actually present in the user's searched data.
        (e.resultFields || []).forEach(add);
        (e.parsed.extractions || []).forEach(x => add(x.output_field));
        (e.parsed.candidate_fields || []).forEach(add);
        this.BASE_FIELDS.forEach(add);
        (e.parsed.filter || []).forEach(f => add(f.field));
        (extra || []).forEach(add);
        return out;
    },

    // Freeform field input backed by a datalist: users can pick a discovered
    // field or type any column name (extracted fields, nested keys, etc).
    _fieldInput(id, value, placeholder) {
        const listId = id + 'List';
        const opts = this._editorAllFields(value ? [value] : [])
            .map(f => `<option value="${_esc(f)}"></option>`).join('');
        return `<input type="text" id="${id}" class="full-input model-field-input" list="${listId}" value="${_esc(value || '')}" placeholder="${_esc(placeholder || 'field name')}" spellcheck="false" autocomplete="off">
<datalist id="${listId}">${opts}</datalist>`;
    },

    // ---- Shape (right panel) ----
    _editorShapeHTML() {
        const e = this.editor;
        if (e.modelType === 'rarity') {
            return `
<div class="field-group">
    <label>Partition Key (group by)</label>
    ${this._fieldInput('shapePartKey', e.partitionKey, 'e.g. file_prefix')}
</div>
<div class="field-group" style="margin-top:10px">
    <label>Value Key (rarity of what?)</label>
    ${this._fieldInput('shapeValKey', e.valueKey, 'e.g. tld')}
</div>
<div class="field-group" style="margin-top:10px">
    <label>Min sample size</label>
    <input type="number" id="shapeMinSample" class="model-num-input" value="${e.minSample}" min="1">
</div>
<p class="config-hint">Example: Partition=<em>file_prefix</em>, Value=<em>tld</em> scores how unusual a TLD is for a given prefix.</p>`;
        }
        if (e.modelType === 'volume_baseline') {
            return `
<div class="field-group">
    <label>Entity Fields (baseline per)</label>
    <div id="keyFieldsList">${e.keyFields.map((kf, i) => `
<div class="key-field-row" data-idx="${i}">
    ${this._fieldInput('keyField' + i, kf, 'e.g. user')}
    <button class="btn-remove-row" data-idx="${i}">×</button>
</div>`).join('')}</div>
    <button class="btn-add-row" id="addKeyField">+ Add Entity Field</button>
</div>
<div class="form-row" style="margin-top:10px">
    <div class="field-group">
        <label>Bucket</label>
        <select id="shapeTimeBucket" class="full-input">
            <option value="day" ${e.timeBucket === 'day' ? 'selected' : ''}>Per day</option>
            <option value="hour" ${e.timeBucket === 'hour' ? 'selected' : ''}>Per hour</option>
        </select>
    </div>
    <div class="field-group">
        <label>Min history (buckets)</label>
        <input type="number" id="shapeMinSample" class="model-num-input" value="${e.minSample}" min="1">
    </div>
</div>
<p class="config-hint">Counts events per <em>${e.timeBucket === 'hour' ? 'hour' : 'day'}</em> per entity, then scores the latest complete bucket against the entity's own median (modified z-score). The current, incomplete bucket is excluded.</p>`;
        }
        return `
<div class="field-group">
    <label>Key Fields (entity to track)</label>
    <div id="keyFieldsList">${e.keyFields.map((kf, i) => `
<div class="key-field-row" data-idx="${i}">
    ${this._fieldInput('keyField' + i, kf, 'e.g. src_ip')}
    <button class="btn-remove-row" data-idx="${i}">×</button>
</div>`).join('')}</div>
    <button class="btn-add-row" id="addKeyField">+ Add Key Field</button>
</div>
<p class="config-hint">Example: Key=<em>src_ip</em> tracks when each IP was first and last seen.</p>`;
    },

    _renderEditorShape() {
        const el = document.getElementById('modelShapeConfig');
        if (el) { el.innerHTML = this._editorShapeHTML(); this._bindEditorShape(); }
    },

    _bindEditorShape() {
        const e = this.editor;
        if (e.modelType === 'rarity') {
            const pSel = document.getElementById('shapePartKey');
            const vSel = document.getElementById('shapeValKey');
            if (pSel) pSel.addEventListener('input', ev => { e.partitionKey = ev.target.value.trim(); });
            if (vSel) vSel.addEventListener('input', ev => { e.valueKey = ev.target.value.trim(); });
            document.getElementById('shapeMinSample')?.addEventListener('change', ev => { e.minSample = parseInt(ev.target.value) || 5; });
        } else {
            this._bindKeyFieldEvents();
            document.getElementById('addKeyField')?.addEventListener('click', () => {
                e.keyFields.push('');
                this._renderEditorShape();
            });
            if (e.modelType === 'volume_baseline') {
                document.getElementById('shapeTimeBucket')?.addEventListener('change', ev => { e.timeBucket = ev.target.value; this._renderEditorShape(); });
                document.getElementById('shapeMinSample')?.addEventListener('change', ev => { e.minSample = parseInt(ev.target.value) || 7; });
            }
        }
    },

    _bindKeyFieldEvents() {
        const e = this.editor;
        document.querySelectorAll('#keyFieldsList .key-field-row').forEach(row => {
            const i = parseInt(row.dataset.idx);
            const sel = row.querySelector('.model-field-input');
            sel.addEventListener('input', ev => { e.keyFields[i] = ev.target.value.trim(); });
            row.querySelector('.btn-remove-row').addEventListener('click', () => {
                e.keyFields.splice(i, 1);
                if (!e.keyFields.length) e.keyFields = [''];
                this._renderEditorShape();
            });
        });
    },

    // ---- Alert config (right panel) ----
    _editorAlertModeOptionsHTML() {
        const e = this.editor;
        const opt = (val, title, desc) => `
<label class="alert-mode-option ${e.alertMode === val ? 'selected' : ''}">
    <input type="radio" name="modelAlertMode" value="${val}" ${e.alertMode === val ? 'checked' : ''}>
    <div class="alert-mode-text">
        <div class="mode-title">${title}</div>
        <div class="mode-desc">${desc}</div>
    </div>
</label>`;
        return opt('none', 'Collect data only', "Model runs silently. View its data anytime.")
            + opt('paused', 'Create alert — paused <em style="color:var(--accent-primary);font-style:normal">(Recommended)</em>', "Alert created but won't fire until enabled.")
            + opt('active', 'Create alert — active', 'Alert fires immediately when the threshold is exceeded.');
    },

    _editorAlertConfigHTML() {
        const c = this.editor.alertConfig;
        const mt = this.editor.modelType;
        let typeFields;
        if (mt === 'rarity') {
            typeFields = `
    <div class="form-row" style="margin-top:10px">
        <div class="field-group">
            <label>Min Confidence</label>
            <input type="number" id="alertConfidence" class="model-num-input" value="${c.confidence_threshold}" min="0" max="1" step="0.05">
        </div>
        <div class="field-group">
            <label>Max % Threshold</label>
            <input type="number" id="alertPercent" class="model-num-input" value="${c.percent_threshold}" min="0.1" max="100" step="0.5">
        </div>
    </div>`;
        } else if (mt === 'volume_baseline') {
            typeFields = `
    <div class="field-group" style="margin-top:10px">
        <label>Z-score threshold</label>
        <input type="number" id="alertZThreshold" class="model-num-input" value="${c.z_threshold}" min="0" step="0.5">
        <p class="config-hint">Alert when an entity's latest bucket has |modified z-score| above this. 3.5 is the standard cutoff.</p>
    </div>`;
        } else {
            typeFields = `
    <label class="toggle-label" style="margin-top:10px">
        <input type="checkbox" class="themed-checkbox" id="alertOnNew" ${c.alert_on_new ? 'checked' : ''}> Alert on new entities only
    </label>`;
        }
        return `
<div class="alert-config-section">
    <div class="field-group">
        <label>Severity</label>
        <select id="alertSeverity">
            ${['low', 'medium', 'high', 'critical'].map(s => `<option ${c.severity === s ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
    </div>
    ${typeFields}
</div>`;
    },

    _renderEditorAlertConfig() {
        const el = document.getElementById('modelAlertConfig');
        if (el) { el.innerHTML = this.editor.alertMode !== 'none' ? this._editorAlertConfigHTML() : ''; this._bindAlertConfigEvents(); }
    },

    _bindEditorDetails() {
        const e = this.editor;
        document.getElementById('modelName').addEventListener('input', ev => { e.name = ev.target.value; });
        document.getElementById('modelDesc').addEventListener('input', ev => { e.description = ev.target.value; });
        document.querySelectorAll('input[name=modelAlertMode]').forEach(radio => {
            radio.addEventListener('change', ev => {
                e.alertMode = ev.target.value;
                document.querySelectorAll('.alert-mode-option').forEach(opt => {
                    opt.classList.toggle('selected', opt.querySelector('input').value === e.alertMode);
                });
                this._renderEditorAlertConfig();
            });
        });
        this._bindAlertConfigEvents();
    },

    _bindAlertConfigEvents() {
        const c = this.editor.alertConfig;
        document.getElementById('alertSeverity')?.addEventListener('change', ev => { c.severity = ev.target.value; });
        document.getElementById('alertConfidence')?.addEventListener('change', ev => { c.confidence_threshold = parseFloat(ev.target.value); });
        document.getElementById('alertPercent')?.addEventListener('change', ev => { c.percent_threshold = parseFloat(ev.target.value); });
        document.getElementById('alertZThreshold')?.addEventListener('change', ev => { c.z_threshold = parseFloat(ev.target.value); });
        document.getElementById('alertOnNew')?.addEventListener('change', ev => { c.alert_on_new = ev.target.checked; });
    },

    // ---- Translation feedback strip (left panel) ----
    _renderTranslation() {
        const el = document.getElementById('modelTranslation');
        if (!el) return;
        const p = this.editor.parsed;

        // Nothing parsed yet (or an empty query): keep the strip hidden rather
        // than showing a noisy "all logs / none" placeholder.
        const hasContent = (p.filter || []).length || (p.extractions || []).length ||
            (p.errors || []).length || (p.warnings || []).length;
        if (!hasContent) {
            el.innerHTML = '';
            el.style.display = 'none';
            return;
        }
        el.style.display = '';

        const parts = [];

        if (p.errors && p.errors.length) {
            parts.push(`<div class="model-trans-errors">${p.errors.map(x => `<div class="model-trans-error">⚠ ${_esc(x)}</div>`).join('')}</div>`);
        }
        if (p.warnings && p.warnings.length) {
            parts.push(`<div class="model-trans-warnings">${p.warnings.map(x => `<div class="model-trans-warn">${_esc(x)}</div>`).join('')}</div>`);
        }

        const filterChips = (p.filter || []).map(f =>
            `<span class="model-chip"><code>${_esc(f.field)}</code> ${_esc(f.op)} <code>${_esc(f.value)}</code></span>`
        ).join('');
        const filterRow = `<div class="model-trans-row"><span class="model-trans-label">Filters</span>${filterChips || '<span class="model-trans-muted">all logs</span>'}</div>`;

        let extRows;
        if ((p.extractions || []).length) {
            extRows = (p.extractions || []).map(x => {
                const badges = [];
                if (x.min_length > 0) badges.push(`<span class="model-ext-badge">min len ${x.min_length}</span>`);
                if (x.lowercase) badges.push(`<span class="model-ext-badge">lowercase</span>`);
                return `
<div class="model-ext-row">
    <span class="model-chip"><code>${_esc(x.output_field)}</code> <span class="model-trans-muted">← regex(${_esc(x.from_field)})</span></span>
    ${badges.join('')}
</div>`;
            }).join('');
        } else {
            extRows = '<span class="model-trans-muted">none</span>';
        }
        const extRow = `<div class="model-trans-row model-trans-row-col"><span class="model-trans-label">Extractions</span><div class="model-ext-list">${extRows}</div></div>`;

        parts.push(`<div class="model-trans-body">${filterRow}${extRow}</div>`);
        el.innerHTML = parts.join('');
    },

    // ---- Time range ----
    _editorTimeRange() {
        const now = Date.now();
        const map = { '1h': 3600e3, '6h': 6 * 3600e3, '24h': 24 * 3600e3, '7d': 7 * 24 * 3600e3, '30d': 30 * 24 * 3600e3 };
        const span = map[this.editor.timeRange] || map['24h'];
        return { start: new Date(now - span).toISOString(), end: new Date(now).toISOString() };
    },

    // ---- Run: live preview + translation (parallel) ----
    async _runQuery() {
        const e = this.editor;
        e.query = (document.getElementById('modelQueryInput')?.value || '').trim();
        e.ran = true;
        // Guard against out-of-order completion: only the latest run may render.
        const seq = ++this._runSeq;
        const resultsEl = document.getElementById('modelQueryResults');
        const countEl = document.getElementById('modelResultsCount');
        if (resultsEl) resultsEl.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        if (countEl) countEl.textContent = 'Running…';

        const { start, end } = this._editorTimeRange();
        const qbody = { query: e.query || '*', start, end };
        if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
            qbody.fractal_id = window.FractalContext.currentFractal.id;
        }

        const queryPromise = e.query
            ? fetch('/api/v1/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'include', body: JSON.stringify(qbody) }).then(r => r.json())
            : Promise.resolve(null);
        const parsePromise = this._api('POST', '/models/parse-query', { query: e.query, model_type: e.modelType }).catch(() => null);

        const [queryData, parseData] = await Promise.all([queryPromise.catch(err => ({ error: err.message })), parsePromise]);

        // A newer run started while this one was in flight; discard stale results.
        if (seq !== this._runSeq) return;

        // Translation result.
        if (parseData?.data) {
            const d = parseData.data;
            e.parsed = {
                filter: d.definition?.filter || [],
                extractions: d.definition?.extractions || [],
                candidate_fields: d.candidate_fields || [],
                errors: d.errors || [],
                warnings: d.warnings || [],
            };
            this._renderTranslation();
            this._renderEditorShape();
        }

        // Live results.
        const sqlEl = document.getElementById('modelSqlOutput');
        if (sqlEl) {
            if (queryData && queryData.sql && window.QueryExecutor) sqlEl.innerHTML = QueryExecutor.highlightSQL(queryData.sql);
            sqlEl.style.display = (this._showSQL() && queryData && queryData.sql) ? 'block' : 'none';
        }
        if (!e.query) {
            if (resultsEl) resultsEl.innerHTML = '<div class="models-empty">No filter — the model will process all logs in this fractal.</div>';
            if (countEl) countEl.textContent = '';
        } else if (queryData && queryData.error) {
            if (resultsEl) resultsEl.innerHTML = `<div class="query-error"><p>Query Error: ${_esc(queryData.error)}</p></div>`;
            if (countEl) countEl.textContent = 'Error';
        } else if (queryData) {
            const results = queryData.results || [];
            e.results = results;
            e.fieldOrder = queryData.field_order || null;
            e.resultFields = this._collectResultFields(queryData);
            // Refresh shape datalists so partition/value keys suggest the fields
            // actually present in the freshly searched data.
            this._renderEditorShape();
            if (countEl) countEl.textContent = `${results.length} result${results.length === 1 ? '' : 's'}`;
            if (!results.length) {
                if (resultsEl) resultsEl.innerHTML = '<div class="models-empty">No matching logs in the selected time range.</div>';
            } else if (window.QueryExecutor && resultsEl) {
                QueryExecutor.renderResultsToElement(results.slice(0, 100), resultsEl, e.fieldOrder, {
                    allResults: results, isAggregated: queryData.is_aggregated || false, disableDetailView: true
                });
            }
        }
    },

    // Collect the column names present in a query response so they can be
    // offered as partition/value/key field suggestions.
    _collectResultFields(queryData) {
        const fields = [];
        const seen = new Set();
        const add = f => { if (f && !seen.has(f)) { seen.add(f); fields.push(f); } };
        // field_order is the authoritative visible-column list; fall back to the
        // union of result keys when it is absent.
        if (queryData.field_order && queryData.field_order.length) {
            queryData.field_order.forEach(add);
        } else {
            (queryData.results || []).slice(0, 50).forEach(r => Object.keys(r || {}).forEach(add));
        }
        return fields;
    },

    // ---- Save ----
    async _saveModel() {
        const e = this.editor;
        e.query = (document.getElementById('modelQueryInput')?.value || '').trim();

        if (!e.name.trim()) { Toast.warning('Model name is required'); return; }

        // Re-parse on save for an authoritative definition + validation.
        let filter = [], extractions = [];
        if (e.query) {
            const parseData = await this._api('POST', '/models/parse-query', { query: e.query, model_type: e.modelType }).catch(() => null);
            const d = parseData?.data;
            if (!d) { Toast.error('Could not validate the source query'); return; }
            if (d.errors && d.errors.length) {
                e.parsed = { filter: d.definition?.filter || [], extractions: d.definition?.extractions || [], candidate_fields: d.candidate_fields || [], errors: d.errors, warnings: d.warnings || [] };
                this._renderTranslation();
                Toast.error(d.errors[0]);
                return;
            }
            filter = d.definition?.filter || [];
            extractions = d.definition?.extractions || [];
            e.parsed.candidate_fields = d.candidate_fields || [];
        }

        if (e.modelType === 'rarity' && (!e.partitionKey || !e.valueKey)) {
            Toast.warning('Select a partition key and a value key');
            return;
        }
        if ((e.modelType === 'first_seen' || e.modelType === 'volume_baseline') && !e.keyFields.filter(Boolean).length) {
            Toast.warning(e.modelType === 'volume_baseline' ? 'Add at least one entity field' : 'Add at least one key field');
            return;
        }

        const def = { filter, extractions };
        if (e.modelType === 'rarity') {
            def.partition_key = e.partitionKey;
            def.value_key = e.valueKey;
            def.min_sample = e.minSample;
        } else if (e.modelType === 'volume_baseline') {
            def.key_fields = e.keyFields.filter(Boolean);
            def.time_bucket = e.timeBucket;
            def.min_sample = e.minSample;
        } else {
            def.key_fields = e.keyFields.filter(Boolean);
        }
        if (e.alertMode !== 'none') def.alert = { ...e.alertConfig };

        const btn = document.getElementById('modelEditorSave');
        if (btn) btn.disabled = true;
        try {
            if (e.editId) {
                await this._api('PUT', `/models/${e.editId}`, {
                    name: e.name.trim(), description: e.description.trim(), definition: def, alert_mode: e.alertMode,
                });
                Toast.success('Model updated');
            } else {
                await this._api('POST', '/models', {
                    name: e.name.trim(), description: e.description.trim(), model_type: e.modelType, definition: def, alert_mode: e.alertMode,
                });
                Toast.success('Model created');
            }
            this.currentView = 'list';
            this._render();
            await this._loadModels();
        } catch (err) {
            Toast.error(err.message || 'Failed to save model');
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

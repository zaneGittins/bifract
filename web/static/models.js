// Analytics Models module — BQL-first split-panel editor, listing, and data viewer.
const AnalyticsModels = {
    // ---- State ----
    models: [],
    currentView: 'list',   // 'list' | 'editor' | 'data'
    selectedModel: null,
    _runSeq: 0,            // monotonic token so only the latest _runQuery renders
    _queryController: null, // AbortController for the in-flight preview fetch
    _viewerPoll: null,     // interval: poll the open model while its backfill runs
    _listPoll: null,       // interval: poll the listing while any backfill runs
    BACKFILL_WINDOWS: [['24h', 'Last 24h'], ['7d', 'Last 7 days'], ['30d', 'Last 30 days'], ['90d', 'Last 90 days']],

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
    },

    init() {
        this._render();
    },

    show(subPath = '') {
        if (subPath === 'new') {
            this._startEditor();
            return;
        }
        if (subPath) {
            const parts = subPath.split('/');
            const modelId = parts[0];
            const isEdit = parts[1] === 'edit';
            // Load model list then navigate; pushSubPath inside these functions is
            // deduplicated against the current URL, so no spurious history entry is created.
            this._api('GET', '/models').then(data => {
                this.models = data?.data?.models || [];
                if (isEdit) this._editModel(modelId);
                else this._openDataViewer(modelId);
            }).catch(() => {
                this.currentView = 'list';
                this.selectedModel = null;
                this.viewer.model = null;
                this._render();
            });
            return;
        }
        // Default: reset to listing.
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
        <th>Name</th><th>Type</th><th>Status</th><th>Alert</th><th>Updated</th>
    </tr></thead>
    <tbody>${filtered.map(m => this._modelRow(m)).join('')}</tbody>
</table>`;
        wrap.querySelectorAll('.model-name-link').forEach(btn => {
            btn.addEventListener('click', () => this._openDataViewer(btn.dataset.id));
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
            ? ` <span class="model-badge badge-backfilling" title="Backfilling historical data">⟳ Backfilling ${this._backfillPct(m)}%</span>`
            : '';
        return `
<tr>
    <td><button class="model-name-link" data-id="${m.id}" title="Open ${_esc(m.name)}">${_esc(m.name)}</button><div class="model-desc">${_esc(m.description)}</div></td>
    <td>${_esc(m.model_type)}</td>
    <td><span class="model-badge ${statusClass}"${errorTitle}>${_esc(m.status)}</span>${backfillBadge}</td>
    <td>${alertBadge}</td>
    <td>${updated}</td>
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

    async _deleteModel(id, name) {
        if (!confirm(`Delete model "${name}"? This permanently removes the model, its data, and its alert. This cannot be undone.`)) return;
        try {
            await this._api('DELETE', `/models/${id}`);
            Toast.success('Model deleted');
            // Deletion is initiated from the model page; return to the listing
            // (_renderListView reloads the models list itself).
            this._stopViewerPoll();
            window.App?.pushSubPath('');
            this.currentView = 'list';
            this._render();
        } catch (e) {
            console.error('[Models] delete failed:', e);
            Toast.error('Delete failed: ' + (e.message || 'unknown error'));
        }
    },

    // ============================
    // Edit existing model (opens the editor pre-populated)
    // ============================
    _editModel(id) {
        const m = this.models.find(m => m.id === id);
        if (!m) return;
        window.App?.pushSubPath(`${id}/edit`);
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
        window.App?.pushSubPath(id);
        this._stopListPoll();
        this.viewer = { model, rows: [], total: 0, limit: 50, offset: 0, sortCol: '', sortDir: 'desc', search: '', tab: 'data', backfillWindow: '7d', histogram: null };
        this.currentView = 'data';
        this._render();
        await this._loadViewerData();
        this._loadHistogram();
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

    _renderDataViewerView(container) {
        const m = this.viewer.model;

        container.innerHTML = `
<div class="models-view-section model-data-viewer">
    <div class="model-data-header">
        <div class="model-data-title">${_esc(m.name)}</div>
        <span class="model-badge badge-${_esc(m.status || 'none')}">${_esc(m.status || 'unknown')}</span>
        <span style="flex:1"></span>
        <button class="btn-secondary" id="modelsBackfillBtn">Backfill</button>
        <button class="btn-secondary" id="modelsExportFromViewer">Export</button>
        <button class="btn-secondary" id="modelsEditFromViewer">Edit</button>
        <div class="model-menu-wrap">
            <button class="btn-secondary model-menu-btn" id="modelsMenuBtn" title="More actions" aria-haspopup="true" aria-label="More actions">&#x22EE;</button>
            <div class="model-menu" id="modelsMenu" hidden>
                <button class="model-menu-item danger" id="modelsDeleteItem">Delete model</button>
            </div>
        </div>
    </div>
    <div id="modelsBackfillBar" class="model-backfill-bar"></div>
    <div id="modelsHistogramPanel" class="model-histogram-panel"></div>
    <div class="model-data-toolbar">
        <input type="text" id="modelsDataSearch" class="models-search" placeholder="Search..." value="${_esc(this.viewer.search)}">
    </div>
    <div class="model-data-table-wrap" id="modelsDataTableWrap">
        <div class="models-empty">Loading...</div>
    </div>
    <div class="model-data-pagination" id="modelsDataPagination"></div>
</div>`;

        this._renderBackfillBar();
        // Resume progress polling if a backfill is running (e.g. after returning
        // to this view). _startViewerPoll guards against dupes.
        if (this.viewer.model && this.viewer.model.backfill_status === 'running') this._startViewerPoll();
        document.getElementById('modelsEditFromViewer').addEventListener('click', () => {
            this._editModel(m.id);
        });
        document.getElementById('modelsExportFromViewer').addEventListener('click', () => {
            this._exportModel(m.id, m.name);
        });
        const menuBtn = document.getElementById('modelsMenuBtn');
        const menu = document.getElementById('modelsMenu');
        menuBtn.addEventListener('click', e => {
            e.stopPropagation();
            menu.hidden = !menu.hidden;
        });
        document.getElementById('modelsDeleteItem').addEventListener('click', () => {
            menu.hidden = true;
            this._deleteModel(m.id, m.name);
        });
        // Close the overflow menu on any outside click or Escape. Bound once on
        // the document so re-renders of this view don't stack listeners.
        if (!this._menuDocBound) {
            this._menuDocBound = true;
            document.addEventListener('click', () => {
                const mm = document.getElementById('modelsMenu');
                if (mm) mm.hidden = true;
            });
            document.addEventListener('keydown', e => {
                if (e.key !== 'Escape') return;
                const mm = document.getElementById('modelsMenu');
                if (mm) mm.hidden = true;
            });
        }
        document.getElementById('modelsDataSearch').addEventListener('input', e => {
            this.viewer.search = e.target.value;
            this.viewer.offset = 0;
            this._loadViewerData();
        });
    },

    // ---- Score distribution histogram ----
    METRIC_LABELS: { confidence: 'Confidence', z_score: 'Anomaly score (|z|)', event_count: 'Event count' },

    async _loadHistogram() {
        const v = this.viewer;
        if (!v.model) return;
        try {
            const data = await this._api('GET', `/models/${v.model.id}/histogram`);
            v.histogram = data?.data?.histogram || null;
        } catch (e) {
            console.error('[Models] loadHistogram error:', e);
            v.histogram = null;
        }
        this._renderHistogram();
    },

    _renderHistogram() {
        const el = document.getElementById('modelsHistogramPanel');
        if (!el) return;
        const h = this.viewer.histogram;
        const buckets = (h && Array.isArray(h.buckets)) ? h.buckets : [];
        const metric = this.METRIC_LABELS[h?.metric] || 'Score';
        const max = buckets.reduce((m, b) => Math.max(m, Number(b.count || 0)), 0);

        if (!buckets.length || max <= 0) {
            el.innerHTML = `
<div class="histogram-head"><span class="histogram-title">${_esc(metric)} distribution</span></div>
<div class="histogram-empty">Not enough data yet to show a distribution.</div>`;
            return;
        }

        const cols = buckets.map(b => {
            const cnt = Number(b.count || 0);
            const pct = max > 0 ? Math.round(cnt / max * 100) : 0;
            return `<div class="histogram-col" title="${_esc(b.label)}: ${cnt.toLocaleString()}">
    <span class="histogram-bar-val">${this._fmtNum(cnt)}</span>
    <div class="histogram-bar-track"><div class="histogram-bar" style="height:${cnt > 0 ? Math.max(pct, 2) : 0}%"></div></div>
    <span class="histogram-bar-label">${_esc(b.label)}</span>
</div>`;
        }).join('');

        let thresholdLine = '';
        if (h?.metric === 'confidence') {
            const t = this.viewer.model?.definition?.alert?.confidence_threshold;
            if (t != null && t >= 0 && t <= 1) {
                thresholdLine = `<div class="histogram-threshold-line" style="left:calc(12px + (100% - 24px) * ${t})" title="Alert threshold: ${Math.round(t * 100)}%"><span class="histogram-threshold-label">${Math.round(t * 100)}%</span></div>`;
            }
        }

        el.innerHTML = `
<div class="histogram-head"><span class="histogram-title">${_esc(metric)} distribution</span></div>
<div class="histogram-chart">${cols}${thresholdLine}</div>`;
    },

    _fmtNum(v) {
        const n = Number(v);
        if (isNaN(n)) return '0';
        if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e4) return (n / 1e3).toFixed(1) + 'K';
        return n.toLocaleString();
    },

    // ---- Backfill (seed historical data) ----
    // The header "Backfill" button starts/resumes; the banner below the header
    // surfaces live progress (with a cancel control) while a backfill runs.
    _renderBackfillBar() {
        this._renderBackfillBanner();
        this._renderBackfillButton();
    },

    // Header Backfill button: label + handler reflect the model's backfill state.
    _renderBackfillButton() {
        const btn = document.getElementById('modelsBackfillBtn');
        if (!btn) return;
        const m = this.viewer.model;
        const st = m.backfill_status || 'none';
        btn.className = 'btn-secondary';
        btn.onclick = null;

        if (m.status !== 'active' && st !== 'running') {
            btn.disabled = true;
            btn.textContent = 'Backfill';
            btn.title = 'Available once the model finishes initializing';
            return;
        }
        if (st === 'running') {
            btn.disabled = true;
            btn.textContent = 'Backfilling…';
            btn.title = 'Backfill in progress';
            return;
        }
        if (st === 'completed') {
            btn.disabled = true;
            btn.textContent = 'Backfilled';
            btn.title = `Backfilled ${m.backfill_window || 'history'} of history`;
            return;
        }
        btn.disabled = false;
        if (st === 'failed' || st === 'cancelled') {
            btn.textContent = 'Resume Backfill';
            btn.title = `Backfill ${st} at ${m.backfill_done || 0}/${m.backfill_total || 0} days`;
            btn.onclick = () => this._startBackfill();
        } else {
            btn.textContent = 'Backfill';
            btn.title = 'Seed this model with historical data';
            btn.onclick = () => this._openBackfillModal();
        }
    },

    // Header banner: running progress only (collapses to nothing otherwise).
    _renderBackfillBanner() {
        const el = document.getElementById('modelsBackfillBar');
        if (!el) return;
        const m = this.viewer.model;
        if ((m.backfill_status || 'none') !== 'running') {
            el.className = 'model-backfill-bar';
            el.innerHTML = '';
            return;
        }
        const pct = this._backfillPct(m);
        el.className = 'model-backfill-bar active';
        el.innerHTML = `
<div class="backfill-running">
    <div class="backfill-running-head">
        <span class="spinner spinner-inline"></span>
        <span class="backfill-label">Backfilling history…</span>
        <span class="backfill-count">${m.backfill_done || 0}/${m.backfill_total || 0} days</span>
        <span class="backfill-spacer"></span>
        <button class="btn-secondary btn-sm" id="backfillCancelBtn">Cancel</button>
    </div>
    <div class="stat-bar-track"><div class="stat-bar-fill" style="width:${pct}%"></div></div>
</div>`;
        document.getElementById('backfillCancelBtn')?.addEventListener('click', () => this._cancelBackfill());
    },

    _openBackfillModal() {
        document.getElementById('backfillModal')?.remove();
        const win = this.viewer.backfillWindow || '7d';
        const opts = this.BACKFILL_WINDOWS.map(([v, l]) =>
            `<option value="${v}" ${v === win ? 'selected' : ''}>${l}</option>`).join('');
        const modal = document.createElement('div');
        modal.id = 'backfillModal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
<div class="modal-content" style="width:440px;max-width:95vw;">
    <div class="modal-header">
        <h3>Backfill historical data</h3>
        <button class="modal-close" onclick="document.getElementById('backfillModal').remove()">&#x2715;</button>
    </div>
    <div class="modal-body">
        <div class="form-group">
            <label>Backfill from</label>
            <select id="backfillWindowSelect" class="form-input">${opts}</select>
        </div>
        <div class="backfill-modal-warning">
            <span class="backfill-warning-icon">⚠</span>
            <span>Backfilling is CPU intensive and may take some time depending on how many historical logs match this model. It runs in the background, so you can keep working while it completes.</span>
        </div>
    </div>
    <div class="modal-footer">
        <button class="btn-secondary" onclick="document.getElementById('backfillModal').remove()">Cancel</button>
        <button class="btn-primary" id="backfillModalStart">Start Backfill</button>
    </div>
</div>`;
        document.body.appendChild(modal);
        modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
        document.getElementById('backfillModalStart')?.addEventListener('click', () => {
            const sel = document.getElementById('backfillWindowSelect');
            this.viewer.backfillWindow = sel ? sel.value : win;
            modal.remove();
            this._startBackfill();
        });
    },

    async _startBackfill() {
        const m = this.viewer.model;
        const window = this.viewer.backfillWindow || '7d';
        const btn = document.getElementById('modelsBackfillBtn');
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
                // Just finished: refresh the rows + distribution so backfilled data shows.
                this._loadViewerData();
                this._loadHistogram();
                if (model.backfill_status === 'completed') Toast.success('Backfill complete');
                else if (model.backfill_status === 'failed') Toast.error('Backfill failed: ' + (model.backfill_error || 'error'));
            }
        }
    },

    // The data viewer is data-only; configuration lives in the editor (Edit).
    _renderViewerContent() {
        this._renderDataTable();
    },

    _renderDataTable() {
        const wrap = document.getElementById('modelsDataTableWrap');
        if (!wrap) return;
        const v = this.viewer;
        const m = v.model;
        if (!v.rows.length) {
            const seeding = m.backfill_status === 'running';
            wrap.innerHTML = seeding
                ? '<div class="models-empty">Backfilling historical data… rows will appear as each day completes.</div>'
                : '<div class="models-empty">No data yet — use the Backfill button to seed historical data, or new matching logs will appear here as they are ingested.</div>';
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
        }).join('') + `<th class="pivot-col-header" title="Pivot to Search">↗</th>`;

        const rows = v.rows.map((row, idx) => {
            const days = Array.isArray(row.days) ? row.days : [];
            const cells = cols.map(c => {
                let val = row[c] ?? '';
                if (typeof val === 'number') val = val.toLocaleString(undefined, { maximumFractionDigits: 4 });
                return `<td>${_esc(String(val))}</td>`;
            }).join('');
            const pivotCell = days.length
                ? `<td class="pivot-cell"><button class="row-pivot-btn" data-row="${idx}" title="Search ${days.length} day${days.length === 1 ? '' : 's'} in logs">${days.length}d</button></td>`
                : `<td class="pivot-cell"></td>`;
            return `<tr class="${days.length ? 'has-pivot' : ''}">${cells}${pivotCell}</tr>`;
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

        wrap.querySelectorAll('.row-pivot-btn').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                const row = v.rows[parseInt(btn.dataset.row)];
                if (row) this._pivotToSearch(row, m);
            });
        });

        this._renderPagination();
    },

    // Build a BQL source query string from a model definition (mirrors GenerateSourceQuery in Go).
    _buildSourceQuery(def) {
        const lines = [];
        const esc = s => `"${String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
        const relit = s => {
            // Wrap as /.../ regex literal, escaping unescaped forward slashes.
            let out = '/';
            for (let i = 0; i < s.length; i++) {
                if (s[i] === '\\' && i + 1 < s.length) { out += s[i] + s[i + 1]; i++; continue; }
                if (s[i] === '/') out += '\\/';
                else out += s[i];
            }
            return out + '/';
        };
        for (const fc of (def.filter || [])) {
            if (fc.op === 'cidr' || fc.op === '!cidr') continue;
            if (fc.op === '=')  lines.push(`${fc.field} = ${esc(fc.value)}`);
            else if (fc.op === '!=') lines.push(`${fc.field} != ${esc(fc.value)}`);
            else if (fc.op === '~')  lines.push(`${fc.field} = ${relit(fc.value)}`);
            else if (fc.op === '!~') lines.push(`NOT ${fc.field} = ${relit(fc.value)}`);
            else lines.push(`${fc.field} = ${esc(fc.value)}`);
        }
        for (const fc of (def.filter || [])) {
            if (fc.op === 'cidr')  lines.push(`| cidr(${fc.field}, ${esc(fc.value)})`);
            else if (fc.op === '!cidr') lines.push(`| !cidr(${fc.field}, ${esc(fc.value)})`);
        }
        for (const ext of (def.extractions || [])) {
            const from = ext.from_field || 'raw_log';
            lines.push(`| regex(field=${from}, regex=${esc(ext.pattern)}, as=${ext.output_field})`);
            if (ext.min_length > 0) {
                lines.push(`| len(${ext.output_field}, as=${ext.output_field}_len) | ${ext.output_field}_len >= ${ext.min_length}`);
            }
            if (ext.lowercase) lines.push(`| lowercase(${ext.output_field})`);
        }
        return lines.join('\n');
    },

    _pivotToSearch(row, model) {
        const days = Array.isArray(row.days) ? row.days : [];
        if (!days.length) { Toast.error('No day data available for this row yet.'); return; }

        const sorted = [...days].map(d => String(d).substring(0, 10)).sort();
        const startISO = sorted[0] + 'T00:00:00Z';
        const endISO = sorted[sorted.length - 1] + 'T23:59:59Z';

        const def = model.definition || {};
        const esc = s => `"${String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;

        let bql = this._buildSourceQuery(def);

        // Append row-specific entity/value filters
        const rowFilters = [];
        const mtype = model.model_type || 'rarity';
        if (mtype === 'rarity') {
            if (def.partition_key && row.partition_val != null) rowFilters.push(`| ${def.partition_key}=${esc(row.partition_val)}`);
            if (def.value_key && row.value_val != null)         rowFilters.push(`| ${def.value_key}=${esc(row.value_val)}`);
        } else {
            // first_seen uses entity_key; volume_baseline uses entity_val
            const entityRaw = mtype === 'first_seen' ? row.entity_key : row.entity_val;
            const fields = Array.isArray(def.key_fields) ? def.key_fields : [];
            if (fields.length && entityRaw != null) {
                const parts = String(entityRaw).split('\x1e');
                fields.forEach((field, i) => rowFilters.push(`| ${field}=${esc(parts[i] ?? '')}`));
            }
        }
        if (rowFilters.length) {
            if (!bql) rowFilters[0] = rowFilters[0].replace(/^\|\s+/, '');
            bql = (bql ? bql + '\n' : '') + rowFilters.join('\n');
        }

        if (window.App) App.showFractalViewTab('search');

        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = bql;
            if (window.SyntaxHighlight) SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
        }

        if (window.TimePicker) {
            TimePicker.setState({ type: 'custom', customStart: startISO, customEnd: endISO }, true);
        }

        if (window.QueryExecutor) {
            QueryExecutor.pendingActiveDays = sorted;
            setTimeout(() => QueryExecutor.execute(), 50);
        }
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

    // ============================
    // Editor (split-panel, BQL-first)
    // ============================
    BASE_FIELDS: ['raw_log', 'contents', 'commandline', 'target_file', 'src_ip', 'dst_ip', 'user', 'image', 'parent_process', 'process_name'],

    _startEditor() {
        window.App?.pushSubPath('new');
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
                <button class="search-btn" id="modelRunBtn">
                    <span class="btn-text">Run</span>
                </button>
                <span style="flex:1"></span>
                <span class="model-results-count" id="modelResultsCount"></span>
            </div>
            <div class="model-query-wrap">
                <div id="modelQueryHighlight" class="model-query-highlight"></div>
                <textarea id="modelQueryInput" class="model-query-input" spellcheck="false" placeholder='Filter logs in BQL, or leave empty to use all logs'>${_esc(e.query)}</textarea>
            </div>
            <pre id="modelSqlOutput" class="model-sql-output" style="display:${this._showSQL() ? 'block' : 'none'}"></pre>
            <div id="modelTranslation" class="model-translation"></div>
            <div id="modelTimelineWrap" class="timeline-inline" style="display:none;"><canvas id="modelTimeline"></canvas></div>
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
                <div class="config-section-title">Detection</div>
                <p class="config-hint">Thresholds that decide when this model flags a result. A paused alert is created with these thresholds${e.editId ? '' : ' on save'} — enable it and configure actions, throttling, and severity from the Alerts page.</p>
                <div id="modelAlertConfig">${this._editorAlertConfigHTML()}</div>
            </div>
        </div>
    </div>
</div>`;

        document.getElementById('modelEditorSave').addEventListener('click', () => this._saveModel());
        document.getElementById('modelRunBtn').addEventListener('click', () => this._runOrCancelModel());
        const ta = document.getElementById('modelQueryInput');
        ta.addEventListener('input', ev => { e.query = ev.target.value; this._updateQueryHighlight(); });
        ta.addEventListener('scroll', () => this._syncQueryHighlightScroll());
        ta.addEventListener('keydown', ev => {
            if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') { ev.preventDefault(); this._runOrCancelModel(); }
        });
        this._updateQueryHighlight();
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
    ${typeFields}
</div>`;
    },

    _renderEditorAlertConfig() {
        const el = document.getElementById('modelAlertConfig');
        if (el) { el.innerHTML = this._editorAlertConfigHTML(); this._bindAlertConfigEvents(); }
    },

    _bindEditorDetails() {
        const e = this.editor;
        document.getElementById('modelName').addEventListener('input', ev => { e.name = ev.target.value; });
        document.getElementById('modelDesc').addEventListener('input', ev => { e.description = ev.target.value; });
        this._bindAlertConfigEvents();
    },

    _bindAlertConfigEvents() {
        const c = this.editor.alertConfig;
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
    _runOrCancelModel() {
        if (this._queryController) {
            this._queryController.abort();
            this._queryController = null;
            this._setModelRunState(false);
        } else {
            this._runQuery();
        }
    },

    _setModelRunState(running) {
        const btn = document.getElementById('modelRunBtn');
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

    async _runQuery() {
        const e = this.editor;
        e.query = (document.getElementById('modelQueryInput')?.value || '').trim();
        e.ran = true;

        // Cancel any prior in-flight fetch before starting fresh.
        if (this._queryController) this._queryController.abort();
        const controller = new AbortController();
        this._queryController = controller;

        // Guard against out-of-order completion: only the latest run may render.
        const seq = ++this._runSeq;
        const resultsEl = document.getElementById('modelQueryResults');
        const countEl = document.getElementById('modelResultsCount');
        if (resultsEl) resultsEl.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        if (countEl) countEl.textContent = 'Running…';
        const timelineWrapEl = document.getElementById('modelTimelineWrap');
        if (timelineWrapEl) timelineWrapEl.style.display = 'none';
        this._setModelRunState(true);

        const { start, end } = this._editorTimeRange();
        const qbody = { query: e.query || '*', start, end };
        if (window.FractalContext && window.FractalContext.currentFractal && !window.FractalContext.isPrism()) {
            qbody.fractal_id = window.FractalContext.currentFractal.id;
        }

        try {
            const queryPromise = e.query
                ? fetch('/api/v1/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'include', signal: controller.signal, body: JSON.stringify(qbody) }).then(r => r.json())
                : Promise.resolve(null);
            const parsePromise = this._api('POST', '/models/parse-query', { query: e.query, model_type: e.modelType }).catch(() => null);

            const [queryData, parseData] = await Promise.all([queryPromise.catch(err => {
                if (err.name === 'AbortError') throw err;
                return { error: err.message };
            }), parsePromise]);

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
            // Histogram (present on both success and some error paths from the buffered endpoint)
            const timelineCanvasEl = document.getElementById('modelTimeline');
            const timelineWrapEl = document.getElementById('modelTimelineWrap');
            if (window.Timeline && queryData && queryData.histogram && queryData.time_start) {
                Timeline.renderBucketsToEl(
                    queryData.histogram,
                    { start: queryData.time_start, end: queryData.time_end },
                    timelineCanvasEl, timelineWrapEl
                );
            } else if (timelineWrapEl) {
                timelineWrapEl.style.display = 'none';
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
        } catch (err) {
            if (err.name === 'AbortError') return;
            if (seq !== this._runSeq) return;
            if (resultsEl) resultsEl.innerHTML = `<div class="query-error"><p>Query Error: ${_esc(err.message)}</p></div>`;
            if (countEl) countEl.textContent = 'Error';
        } finally {
            if (this._queryController === controller) {
                this._queryController = null;
                this._setModelRunState(false);
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
        // Alert thresholds are always configured now; mode is set to paused on
        // create and preserved on edit (changed ad-hoc from the model's data view).
        def.alert = { ...e.alertConfig };

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
            window.App?.pushSubPath('');
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

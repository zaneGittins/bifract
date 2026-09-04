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
        stats: null,     // aggregate summary from /models/:id/stats
        selected: -1,    // index of the row open in the detail drawer
    },

    init() {
        // No render here: the panel is hidden at startup and entering the tab
        // always calls show(). Rendering at init loaded models before any scope
        // was chosen.
        if (window.FractalContext && FractalContext.subscribe) {
            FractalContext.subscribe('AnalyticsModels', () => this.onFractalChange());
        }
    },

    // Models are fractal-scoped server-side and every call here relies on the
    // request scope, so a switch invalidates the whole panel. Without this the
    // previous fractal's models stay on screen while edits and runs land in the
    // new one.
    onFractalChange() {
        this.teardown();
        this.models = [];
        this.currentView = 'list';
        this.selectedModel = null;
        this.viewer.model = null;
        this.viewer.rows = [];
        this.viewer.total = 0;
        this.viewer.offset = 0;
        if (this._queryController) {
            this._queryController.abort();
            this._queryController = null;
        }
        this._runSeq++;

        if (!FractalContext.shouldReload('modelsView')) {
            // Drop the previous scope's markup; re-entry goes through show().
            const view = document.getElementById('modelsView');
            if (view) view.innerHTML = '';
            return;
        }
        if (FractalContext.isPrism()) {
            this._render();
            return;
        }
        this.show('');
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
                this.models = data?.data || [];
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

    // Stop all polling and hand the page height back when the models tab is
    // hidden (called from app.js).
    teardown() {
        this._stopViewerPoll();
        this._stopListPoll();
        this._setWorkspaceChrome(false);
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
            this.models = data?.data || [];
            this._renderList();
        } catch (e) {
            Toast.error('Failed to load models');
        }
    },

    // ---- Top-level render ----
    _render() {
        const container = document.getElementById('modelsView');
        if (!container) return;
        // Models are fractal-scoped, and the API rejects a prism outright. Say so
        // rather than rendering a listing whose load can only fail: onFractalChange
        // renders on every scope switch, including while this view is hidden.
        if (window.FractalContext && FractalContext.isPrism()) {
            this._setWorkspaceChrome(false);
            container.innerHTML = `
<div class="models-view-section">
    <div class="models-empty">Analytics models are scoped to a fractal. Select a fractal to manage them.</div>
</div>`;
            return;
        }
        this._setWorkspaceChrome(this.currentView === 'editor' || this.currentView === 'data');
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
                this.models = data?.data || [];
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
        const updated = m.updated_at ? TZ.format(m.updated_at, 'date') : '—';
        const errorTitle = m.status === 'error' && m.error_message ? ` title="${_esc(m.error_message)}"` : '';
        const backfillBadge = m.backfill_status === 'running'
            ? ` <span class="model-badge badge-backfilling" title="Backfilling historical data"><span class="model-dot"></span>Backfilling ${this._backfillPct(m)}%</span>`
            : '';
        return `
<tr>
    <td><button class="model-name-link" data-id="${m.id}" title="Open ${_esc(m.name)}">${_esc(m.name)}</button><div class="model-desc">${_esc(m.description)}</div></td>
    <td>${_esc(this._typeLabel(m.model_type))}</td>
    <td><span class="model-badge ${statusClass}"${errorTitle}><span class="model-dot"></span>${_esc(this._statusLabel(m.status))}</span>${backfillBadge}</td>
    <td>${alertBadge}</td>
    <td>${updated}</td>
</tr>`;
    },

    _statusLabel(status) {
        const s = String(status || '');
        return s ? s.charAt(0).toUpperCase() + s.slice(1) : 'Unknown';
    },

    _alertModeBadge(m) {
        switch (m.alert_mode) {
            case 'active': return `<span class="model-badge badge-alert-active alert-mode-badge" data-id="${m.id}" data-mode="active" title="Click to pause"><span class="model-dot"></span>Active</span>`;
            case 'paused': return `<span class="model-badge badge-paused alert-mode-badge" data-id="${m.id}" data-mode="paused" title="Click to activate"><span class="model-dot"></span>Paused</span>`;
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
            Toast.success(`Imported "${data?.data?.name || file.name}"`);
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
        const alertCfg = { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true, z_threshold: 3.5, beacon_threshold: 0.8, longconn_threshold: 0.5 };
        if (def.alert) Object.assign(alertCfg, def.alert);
        if (def.beacon && def.beacon.score_threshold != null) alertCfg.beacon_threshold = def.beacon.score_threshold;
        if (def.long_conn && def.long_conn.score_threshold != null) alertCfg.longconn_threshold = def.long_conn.score_threshold;
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
            network: this._networkFromDef(def),
            window: def.window || '1d',
            alertMode: m.alert_mode || 'none',
            alertConfig: alertCfg,
            name: m.name,
            description: m.description || '',
            timeRange: '24h',
            fieldOrder: null,
            resultFields: [],
            results: [],
            resultCount: '',
            hasTimeline: false,
            ran: false,
            resultMode: 'logs',
            previewWindow: '7d',
            preview: null,
            dirty: false,
        };
        this.currentView = 'editor';
        this._render();
    },

    // ============================
    // Data viewer
    // ============================

    // How each model type presents its results. One place decides the column
    // order, the label a stored column carries, how its value is formatted, and
    // whether the backend will actually sort on it. A label given as a function
    // is resolved against the definition, so a column stored as partition_val
    // reads as the field the model was built from. Columns the table omits are
    // still shown in the row drawer.
    VIEW_SPEC: {
        rarity: {
            sortDefault: 'confidence',
            sortable: ['partition_val', 'value_val', 'model_count', 'percent', 'confidence'],
            score: { col: 'confidence', threshold: d => d.alert?.confidence_threshold ?? 0.8 },
            cols: [
                { col: 'confidence', label: 'Confidence', fmt: 'meter', align: 'num' },
                { col: 'partition_val', label: d => d.partition_key || 'Partition' },
                { col: 'value_val', label: d => d.value_key || 'Value' },
                { col: 'model_count', label: 'Seen', fmt: 'int', align: 'num' },
                { col: 'percent', label: 'Share', fmt: 'pct100', align: 'num' },
            ],
        },
        first_seen: {
            sortDefault: 'first_seen',
            sortable: ['entity_key', 'first_seen', 'last_seen', 'event_count'],
            cols: [
                { col: 'entity_key', keys: true },
                { col: 'first_seen', label: 'First seen', fmt: 'ts' },
                { col: 'last_seen', label: 'Last seen', fmt: 'ts' },
                { col: 'event_count', label: 'Events', fmt: 'int', align: 'num' },
            ],
        },
        volume_baseline: {
            sortDefault: 'z_score',
            sortable: ['entity_val', 'latest_count', 'baseline_median', 'mad', 'z_score', 'n_buckets', 'latest_bucket'],
            score: { col: 'z_score', threshold: d => d.alert?.z_threshold || 3.5, abs: true },
            cols: [
                { col: 'z_score', label: 'z-score', fmt: 'score', align: 'num' },
                { col: 'entity_val', keys: true },
                { col: 'latest_count', label: 'Latest', fmt: 'int', align: 'num' },
                { col: 'baseline_median', label: 'Baseline', fmt: 'num', align: 'num' },
                { col: 'mad', label: 'MAD', fmt: 'num', align: 'num' },
                { col: 'n_buckets', label: 'Buckets', fmt: 'int', align: 'num' },
                { col: 'latest_bucket', label: 'Latest bucket', fmt: 'ts' },
            ],
        },
        beacon: {
            sortDefault: 'final_score',
            sortable: ['final_score', 'regularity_score', 'conn_count', 'total_duration', 'prevalence', 'last_seen'],
            score: { col: 'final_score', threshold: d => d.beacon?.score_threshold || 0.8 },
            cols: [
                { col: 'final_score', label: 'Score', fmt: 'score', align: 'num' },
                { col: 'src_ip', label: 'Source' },
                { col: 'dst_ip', label: 'Destination' },
                { col: 'dst_port', label: 'Port', align: 'num' },
                { col: 'regularity_score', label: 'Regularity', fmt: 'score', align: 'num' },
                { col: 'conn_count', label: 'Connections', fmt: 'int', align: 'num' },
                { col: 'prevalence', label: 'Prevalence', fmt: 'pct1', align: 'num' },
                { col: 'last_seen', label: 'Last seen', fmt: 'ts' },
            ],
        },
        long_connection: {
            sortDefault: 'final_score',
            sortable: ['final_score', 'regularity_score', 'conn_count', 'total_duration', 'prevalence', 'last_seen'],
            score: { col: 'final_score', threshold: d => d.long_conn?.score_threshold || 0.5 },
            cols: [
                { col: 'final_score', label: 'Score', fmt: 'score', align: 'num' },
                { col: 'src_ip', label: 'Source' },
                { col: 'dst_ip', label: 'Destination' },
                { col: 'dst_port', label: 'Port', align: 'num' },
                { col: 'total_duration', label: 'Total duration', fmt: 'dur', align: 'num' },
                { col: 'conn_count', label: 'Connections', fmt: 'int', align: 'num' },
                { col: 'prevalence', label: 'Prevalence', fmt: 'pct1', align: 'num' },
                { col: 'last_seen', label: 'Last seen', fmt: 'ts' },
            ],
        },
    },

    // Summary cards, from the /stats endpoint. tone colours the value.
    STAT_SPEC: {
        rarity: [
            { k: 'total_rows', label: 'Pairs', fmt: 'int' },
            { k: 'distinct_partitions', label: d => 'Distinct ' + (d.partition_key || 'partitions'), fmt: 'int' },
        ],
        first_seen: [
            { k: 'total_entities', label: 'Entities', fmt: 'int' },
            { k: 'new_today', label: 'New today', fmt: 'int', tone: 'flag' },
            { k: 'newest_seen', label: 'Newest', fmt: 'ago' },
            { k: 'oldest_seen', label: 'Oldest', fmt: 'ago' },
        ],
        volume_baseline: [
            { k: 'total_entities', label: 'Entities', fmt: 'int' },
            { k: 'anomalous', label: 'Anomalous', fmt: 'int', tone: 'flag' },
            { k: 'max_z', label: 'Max |z|', fmt: 'score' },
        ],
        network: [
            { k: 'total_pairs', label: 'Scored pairs', fmt: 'int' },
            { k: 'flagged', label: 'Flagged', fmt: 'int', tone: 'flag' },
            { k: 'critical', label: 'Critical', fmt: 'int', tone: 'crit' },
            { k: 'max_score', label: 'Max score', fmt: 'score' },
        ],
    },

    _viewSpec(model) {
        return this.VIEW_SPEC[model?.model_type] || this.VIEW_SPEC.rarity;
    },

    // Expands the type's column spec against this model's definition: a key
    // column becomes one column per configured field (entity_key and entity_val
    // pack them with a record separator), and labels resolve to field names.
    _viewColumns(model) {
        const spec = this._viewSpec(model);
        const def = model?.definition || {};
        const out = [];
        for (const c of spec.cols) {
            if (!c.keys) {
                out.push({ ...c, label: typeof c.label === 'function' ? c.label(def) : c.label });
                continue;
            }
            const fields = (Array.isArray(def.key_fields) && def.key_fields.length) ? def.key_fields : ['Entity'];
            // Only the first part carries the sort: the backend orders by the
            // packed key, which is ordering by the leading field.
            const canSort = spec.sortable.includes(c.col);
            fields.forEach((f, i) => out.push({ col: c.col, label: f, part: i, sortable: i === 0 && canSort }));
        }
        return out.map(c => ({ ...c, sortable: c.sortable ?? spec.sortable.includes(c.col) }));
    },

    // Seconds to a two-unit duration. Model durations are session totals, so
    // days and hours are the useful scale, not raw seconds.
    _fmtDuration(v) {
        const s = Number(v);
        if (!isFinite(s) || s <= 0) return '0s';
        const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
        if (d) return `${d}d ${h}h`;
        if (h) return `${h}h ${m}m`;
        if (m) return `${m}m ${Math.floor(s % 60)}s`;
        return `${Math.round(s)}s`;
    },

    // A zero DateTime means the value was never written; rendering the epoch as
    // a date reads as real data.
    _isEpochZero(v) {
        const ms = window.TZ ? TZ.toEpoch(v) : Date.parse(v);
        return !Number.isFinite(ms) || ms < 86400000;
    },

    _fmtTime(v, style) {
        if (!v || this._isEpochZero(v)) return '<span class="mv-none">&mdash;</span>';
        return `<span title="${_esc(Utils.timestampTitle(v))}">${_esc(Utils.formatTimestamp(v, style || 'friendly'))}</span>`;
    },

    // Severity is measured against the model's own alert threshold, so a colour
    // means "this crossed what you configured" rather than a fixed cut.
    _sevClass(value, threshold) {
        const t = Number(threshold), v = Number(value);
        if (!isFinite(t) || t <= 0 || !isFinite(v)) return '';
        const r = v / t;
        if (r >= 1.25) return 'sev-crit';
        if (r >= 1) return 'sev-high';
        if (r >= 0.75) return 'sev-med';
        return 'sev-low';
    },

    _cellText(c, row) {
        const v = row[c.col];
        return c.part !== undefined ? (String(v ?? '').split('\x1e')[c.part] ?? '') : String(v ?? '');
    },

    // Renders one cell. Returns HTML, so every formatter escapes its own value.
    _fmtCell(c, row, threshold) {
        let v = row[c.col];
        if (c.part !== undefined) {
            v = String(v ?? '').split('\x1e')[c.part] ?? '';
        }
        if (v === null || v === undefined || v === '') return '<span class="mv-none">&mdash;</span>';
        switch (c.fmt) {
            case 'int':    return _esc(Number(v).toLocaleString());
            case 'pct1':   return _esc((Number(v) * 100).toFixed(1) + '%');
            case 'pct100': return _esc(Number(v).toFixed(1) + '%');
            case 'score':  return _esc(Number(v).toFixed(3));
            case 'num':    return _esc(Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 }));
            case 'dur':    return _esc(this._fmtDuration(v));
            case 'ts':     return this._fmtTime(v);
            case 'meter': {
                const n = Number(v);
                const pct = Math.max(0, Math.min(100, n * 100));
                const hot = isFinite(threshold) && n >= threshold ? ' hot' : '';
                return `<span class="mv-meter"><span class="mv-meter-track"><span class="mv-meter-fill${hot}" style="width:${pct}%"></span></span>${_esc(n.toFixed(3))}</span>`;
            }
            default:       return _esc(String(v));
        }
    },

    async _openDataViewer(id) {
        const model = this.models.find(m => m.id === id);
        if (!model) return;
        window.App?.pushSubPath(id);
        this._stopListPoll();
        // Seed the sort from the type's server-side default so the header shows
        // the order the first page actually comes back in.
        this.viewer = {
            model, rows: [], total: 0, limit: 50, offset: 0,
            sortCol: this._viewSpec(model).sortDefault, sortDir: 'desc',
            search: '', tab: 'data', backfillWindow: '7d', histogram: null, stats: null, selected: -1,
        };
        this.currentView = 'data';
        this._render();
        await this._loadViewerData();
        this._loadHistogram();
        this._loadStats();
        if (model.backfill_status === 'running') this._startViewerPoll();
    },

    // Aggregate summary. Best-effort: the table is the page, so a failed stats
    // call leaves the strip out rather than blocking the results.
    async _loadStats() {
        const v = this.viewer;
        if (!v.model) return;
        const id = v.model.id;
        try {
            const data = await this._api('GET', `/models/${id}/stats`);
            if (this.viewer.model?.id !== id) return;
            this.viewer.stats = data?.data || null;
        } catch (e) {
            console.error('[Models] loadStats error:', e);
            this.viewer.stats = null;
        }
        this._renderStats();
    },

    async _loadViewerData() {
        const v = this.viewer;
        const params = new URLSearchParams({
            limit: v.limit, offset: v.offset,
            sort: v.sortCol, order: v.sortDir, search: v.search
        });
        try {
            const data = await this._api('GET', `/models/${v.model.id}/data?${params}`);
            v.rows = data?.data || [];
            v.total = data?.page?.total || 0;
            this._hideRowDrawer();
            this._renderViewerContent();
        } catch (e) {
            console.error('[Models] loadViewerData error:', e);
            Toast.error('Failed to load model data: ' + (e?.message || String(e)));
        }
    },

    _renderDataViewerView(container) {
        const m = this.viewer.model;

        container.innerHTML = `
<div class="model-data-viewer">
    <div class="mv-head">
        <div class="mv-title">
            <span class="mv-name">${_esc(m.name)}</span>
            <span class="mv-type-tag">${_esc(this._typeLabel(m.model_type))}</span>
            <span class="model-badge badge-${_esc(m.status || 'none')}"><span class="model-dot"></span>${_esc(this._statusLabel(m.status))}</span>
        </div>
        <div class="mv-actions">
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
    </div>

    <div class="mv-body">
        <div class="mv-main">
            <div id="modelsBackfillBar" class="model-backfill-bar"></div>
            <div id="modelsStats" class="mv-stats"></div>
            <div id="modelsHistogramPanel" class="model-histogram-panel"></div>
            <div class="mv-results">
                <div class="mv-toolbar">
                    <input type="text" id="modelsDataSearch" class="models-search" placeholder="Search results..." value="${_esc(this.viewer.search)}">
                    <span class="mv-toolbar-spacer"></span>
                    <span class="mv-range" id="modelsDataRange"></span>
                </div>
                <div class="model-data-table-wrap" id="modelsDataTableWrap">
                    <div class="models-empty">Loading...</div>
                </div>
                <div class="model-data-pagination" id="modelsDataPagination"></div>
            </div>
        </div>
        <div class="mv-drawer" id="modelsRowDrawer" hidden></div>
        <aside class="mv-rail" id="modelsRail">
            <div class="mv-rail-handle" id="modelsRailHandle" title="Drag to resize"></div>
            <div class="mv-rail-content" id="modelsRailContent"></div>
        </aside>
    </div>
</div>`;

        this._renderRail();
        this._bindRailResize();

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

    // ---- Summary strip ----
    // The counts that answer "did this model find anything", from /stats. The
    // strip stays out of the layout entirely until the call lands.
    _renderStats() {
        const el = document.getElementById('modelsStats');
        if (!el) return;
        const v = this.viewer;
        const st = v.stats;
        if (!st) { el.innerHTML = ''; return; }

        const mt = v.model?.model_type;
        const spec = this.STAT_SPEC[mt === 'beacon' || mt === 'long_connection' ? 'network' : mt] || [];
        const def = v.model?.definition || {};

        const cards = spec.map(c => {
            const raw = st[c.k];
            if (raw === null || raw === undefined) return '';
            let val;
            if (c.fmt === 'int') val = Number(raw).toLocaleString();
            else if (c.fmt === 'score') val = Number(raw).toFixed(3);
            else if (c.fmt === 'ago') val = this._isEpochZero(raw) ? '\u2014' : (Utils.timeAgo(raw) || '\u2014');
            else val = String(raw);
            const label = typeof c.label === 'function' ? c.label(def) : c.label;
            const tone = c.tone && Number(raw) > 0 ? ' mv-stat-' + c.tone : '';
            return `<div class="mv-stat${tone}"><div class="mv-stat-k">${_esc(label)}</div><div class="mv-stat-v">${_esc(val)}</div></div>`;
        }).join('');

        el.innerHTML = cards;
    },

    // ---- Definition rail ----
    // Read-only. Answering "what does this model match" should not require
    // opening the editor, which is a form that can be saved by accident.
    _renderRail() {
        const el = document.getElementById('modelsRailContent');
        if (!el) return;
        const m = this.viewer.model;
        if (!m) { el.innerHTML = ''; return; }
        const def = m.definition || {};
        const mt = m.model_type;

        const rows = [['Type', this._typeLabel(mt)]];
        if (mt === 'rarity') {
            rows.push(['Min sample', def.min_sample || 5]);
            rows.push(['Confidence threshold', (def.alert?.confidence_threshold ?? 0.8).toFixed(2)]);
            rows.push(['Percent threshold', (def.alert?.percent_threshold ?? 5) + '%']);
        } else if (mt === 'volume_baseline') {
            rows.push(['Bucket', def.time_bucket || 'day']);
            rows.push(['z threshold', (def.alert?.z_threshold || 3.5).toFixed(1)]);
        } else if (mt === 'beacon') {
            rows.push(['Window', def.window || '1d']);
            rows.push(['Min connections', def.beacon?.min_connections || 4]);
            rows.push(['Score threshold', (def.beacon?.score_threshold || 0.8).toFixed(2)]);
        } else if (mt === 'long_connection') {
            rows.push(['Window', def.window || '1d']);
            rows.push(['Score threshold', (def.long_conn?.score_threshold || 0.5).toFixed(2)]);
        } else if (mt === 'first_seen') {
            rows.push(['Alert on new', def.alert?.alert_on_new === false ? 'No' : 'Yes']);
        }

        let keyLabel = 'Keys', keys = [];
        if (mt === 'rarity') {
            keys = [def.partition_key, def.value_key].filter(Boolean);
        } else if (mt === 'beacon' || mt === 'long_connection') {
            keyLabel = 'Connection fields';
            const n = def.network || {};
            keys = [n.src_field || 'src_ip', n.dst_field || 'dst_ip', n.port_field || 'dst_port', n.duration_field || 'duration'];
        } else {
            keys = Array.isArray(def.key_fields) ? def.key_fields.filter(Boolean) : [];
        }

        const bql = this._buildSourceQuery(def);
        const alertMode = m.alert_mode || 'paused';

        el.innerHTML = `
<div class="me-sec">
    <div class="me-sec-label">Definition</div>
    <dl class="mv-kv">${rows.map(([k, val]) =>
        `<dt>${_esc(k)}</dt><dd>${_esc(String(val))}</dd>`).join('')}</dl>
</div>
${keys.length ? `<div class="me-sec">
    <div class="me-sec-label">${_esc(keyLabel)}</div>
    <div class="mv-chips">${keys.map(k => `<span class="mv-chip">${_esc(k)}</span>`).join('')}</div>
</div>` : ''}
${bql ? `<div class="me-sec">
    <div class="me-sec-label">Matches</div>
    <pre class="mv-code">${_esc(bql)}</pre>
</div>` : ''}
<div class="me-sec">
    <div class="me-sec-label">Alerting</div>
    <dl class="mv-kv">
        <dt>Mode</dt><dd>${_esc(this._statusLabel(alertMode))}</dd>
        <dt>Severity</dt><dd>${_esc(this._statusLabel(def.alert?.severity || 'medium'))}</dd>
    </dl>
</div>
${m.description ? `<div class="me-sec">
    <div class="me-sec-label">Description</div>
    <div class="mv-desc">${_esc(m.description)}</div>
</div>` : ''}
<div class="mv-rail-foot">Updated ${_esc(Utils.timeAgo(m.updated_at) || 'recently')}</div>`;
    },

    _bindRailResize() {
        window.App?.bindEditorRail?.(document.getElementById('modelsRailHandle'), {
            body: document.querySelector('.mv-body'),
            cssVar: '--mv-rail-w',
            storageKey: 'bifract-model-viewer-rail-width',
        });
    },

    // ---- Row drawer ----
    // A row is an entity with a history, so it opens rather than dead-ending.
    // It also carries the columns the table leaves out (beacon sub-scores, the
    // prevalence totals) which is what makes trimming the table safe.
    _openRowDrawer(idx) {
        const v = this.viewer;
        const row = v.rows[idx];
        const el = document.getElementById('modelsRowDrawer');
        if (!row || !el) return;
        v.selected = idx;

        const m = v.model;
        const spec = this._viewSpec(m);
        const thr = spec.score ? Number(spec.score.threshold(m.definition || {})) : NaN;
        const days = Array.isArray(row.days) ? row.days : [];

        // entity_key and entity_val pack the key fields with a record separator,
        // which prints as a control character when dumped raw.
        const packed = new Set(['entity_key', 'entity_val']);
        const skip = new Set(['days', 'fractal_id']);
        const fields = Object.keys(row).filter(k => !skip.has(k)).map(k => {
            const val = packed.has(k)
                ? _esc(String(row[k] ?? '').split('\x1e').join(' / '))
                : this._fmtCell({ col: k, fmt: this._drawerFmt(k) }, row, thr);
            return `<dt>${_esc(k)}</dt><dd>${val}</dd>`;
        }).join('');

        el.innerHTML = `
<div class="mv-drawer-head">
    <div class="mv-drawer-title">${_esc(this._rowTitle(row, m))}</div>
    <button class="mv-drawer-close" id="modelsDrawerClose" title="Close" aria-label="Close">&times;</button>
</div>
<div class="mv-drawer-body">
    <div class="me-sec">
        <div class="me-sec-label">All columns</div>
        <dl class="mv-kv mv-kv-wide">${fields}</dl>
    </div>
    ${days.length ? `<div class="me-sec">
        <div class="me-sec-label">Active days (${days.length})</div>
        <div class="mv-chips">${days.map(d => `<span class="mv-chip">${_esc(String(d).substring(0, 10))}</span>`).join('')}</div>
    </div>` : ''}
</div>
<div class="mv-drawer-foot">
    ${days.length
        ? `<button class="btn-primary btn-sm" id="modelsDrawerPivot">Search these ${days.length} day${days.length === 1 ? '' : 's'} in logs</button>`
        : `<span class="mv-none">No day history recorded for this row, so there is nothing to pivot to.</span>`}
</div>`;
        el.hidden = false;
        document.querySelector('.mv-body')?.classList.add('mv-inspecting');
        document.getElementById('modelsDrawerClose').addEventListener('click', () => this._closeRowDrawer());
        document.getElementById('modelsDrawerPivot')?.addEventListener('click', () => this._pivotToSearch(row, m));
        this._renderDataTable();
    },

    _closeRowDrawer() {
        this._hideRowDrawer();
        this._renderDataTable();
    },

    _hideRowDrawer() {
        const el = document.getElementById('modelsRowDrawer');
        if (el) { el.hidden = true; el.innerHTML = ''; }
        document.querySelector('.mv-body')?.classList.remove('mv-inspecting');
        this.viewer.selected = -1;
    },

    // Formatter for a column the table does not lay out, keyed by name so the
    // drawer reads the same as the table for the columns they share.
    _drawerFmt(k) {
        if (k === 'total_duration') return 'dur';
        if (k === 'prevalence') return 'pct1';
        if (k === 'percent') return 'pct100';
        if (/_seen$|_at$|_bucket$/.test(k)) return 'ts';
        if (/_score$|^confidence$|^mad$/.test(k)) return 'score';
        if (/_count$|_total$|^n_buckets$/.test(k)) return 'int';
        return '';
    },

    _rowTitle(row, model) {
        const mt = model?.model_type;
        if (mt === 'beacon' || mt === 'long_connection') {
            return `${row.src_ip} \u2192 ${row.dst_ip}:${row.dst_port}`;
        }
        if (mt === 'rarity') return `${row.partition_val} / ${row.value_val}`;
        const raw = mt === 'first_seen' ? row.entity_key : row.entity_val;
        return String(raw ?? '').split('\x1e').join(' / ');
    },

    // ---- Score distribution histogram ----
    METRIC_LABELS: { confidence: 'Confidence', z_score: 'Anomaly score (|z|)', event_count: 'Event count', beacon_score: 'Beacon score', longconn_score: 'Long-connection score', final_score: 'Score' },

    async _loadHistogram() {
        const v = this.viewer;
        if (!v.model) return;
        try {
            const data = await this._api('GET', `/models/${v.model.id}/histogram`);
            v.histogram = data?.data || null;
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
        // The marker is drawn wherever the metric and the threshold share a 0..1
        // scale: confidence and the network final_score do, banded |z| does not.
        let thr = null;
        const spec = this._viewSpec(this.viewer.model);
        if (spec.score && (h?.metric === 'confidence' || h?.metric === 'final_score')) {
            const t = Number(spec.score.threshold(this.viewer.model?.definition || {}));
            if (isFinite(t) && t >= 0 && t <= 1) thr = t;
        }
        el.innerHTML = this._buildHistogramHTML(buckets, h?.metric, thr);
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

        // Scheduled types are seeded by the scorer's rolling window and the API
        // rejects a backfill outright, so the button does not offer one.
        if (m.model_type === 'beacon' || m.model_type === 'long_connection') {
            btn.disabled = true;
            btn.textContent = 'Backfill';
            btn.title = 'Not applicable: this model type is seeded by its rolling window';
            return;
        }

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
            if (data?.data) this.viewer.model = data.data;
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
            model = data?.data;
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
                ? EmptyState.render({ icon: 'list', title: 'Backfilling historical data', detail: 'Rows appear as each day completes.' })
                : v.search
                    ? EmptyState.render({ icon: 'list', title: 'No matching rows', detail: 'No result matches this search.' })
                    : EmptyState.render({
                        icon: 'list',
                        title: 'No data yet',
                        detail: 'Use Backfill to seed history, or new matching logs will appear here as they are ingested.',
                    });
            this._renderPagination();
            return;
        }

        const spec = this._viewSpec(m);
        const cols = this._viewColumns(m);
        const thr = spec.score ? Number(spec.score.threshold(m.definition || {})) : NaN;
        const scoreCol = spec.score?.col;

        const headers = cols.map((c, i) => {
            const active = c.sortable && v.sortCol === c.col ? (v.sortDir === 'asc' ? ' sort-asc' : ' sort-desc') : '';
            const cls = `${c.align === 'num' ? 'num ' : ''}${c.sortable ? 'sortable' : ''}${active}`.trim();
            const attr = c.sortable ? ` data-col="${_esc(c.col)}"` : '';
            const title = c.label !== c.col ? ` title="${_esc(c.col)}"` : '';
            return `<th class="${cls}"${attr}${title}><span class="mv-h">${_esc(c.label)}</span>${
                c.label !== c.col ? `<span class="mv-h-src">${_esc(c.col)}</span>` : ''
            }${c.sortable ? '<span class="sort-icon"></span>' : ''}</th>`;
        }).join('');

        const rows = v.rows.map((row, idx) => {
            const sev = scoreCol ? this._sevClass(spec.score.abs ? Math.abs(row[scoreCol]) : row[scoreCol], thr) : '';
            const cells = cols.map(c => {
                const cls = [c.align === 'num' ? 'num' : '', c.col === scoreCol ? 'mv-score' : ''].filter(Boolean).join(' ');
                const title = c.fmt ? '' : ` title="${_esc(this._cellText(c, row))}"`;
                return `<td${cls ? ` class="${cls}"` : ''}${title}>${this._fmtCell(c, row, thr)}</td>`;
            }).join('');
            const cls = [sev, idx === v.selected ? 'selected' : ''].filter(Boolean).join(' ');
            return `<tr${cls ? ` class="${cls}"` : ''} data-row="${idx}">${cells}</tr>`;
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

        wrap.querySelectorAll('tbody tr').forEach(tr => {
            tr.addEventListener('click', () => {
                const idx = parseInt(tr.dataset.row, 10);
                if (idx === v.selected) this._closeRowDrawer();
                else this._openRowDrawer(idx);
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
            const from = ext.from_field || 'norm_log';
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
            setTimeout(() => QueryExecutor.execute(), 50);
        }
    },

    _renderPagination() {
        const el = document.getElementById('modelsDataPagination');
        const v = this.viewer;
        const range = document.getElementById('modelsDataRange');
        if (range) {
            range.textContent = v.total === 0 ? ''
                : `${(v.offset + 1).toLocaleString()}\u2013${Math.min(v.offset + v.rows.length, v.total).toLocaleString()} of ${v.total.toLocaleString()}`;
        }
        if (!el) return;
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
    MODEL_TYPES: [
        { id: 'rarity', label: 'Rarity', desc: 'Scores how unusual a value is within its partition.' },
        { id: 'first_seen', label: 'First / Last Seen', desc: 'Tracks when an entity was first and last observed.' },
        { id: 'volume_baseline', label: 'Volume Baseline', desc: 'Flags entities whose volume deviates from their own history, by modified z-score.' },
        { id: 'beacon', label: 'Beacon', desc: 'Finds regular, automated check-ins (C2 beaconing) in network connection logs.' },
        { id: 'long_connection', label: 'Long Connection', desc: 'Surfaces unusually long-lived sessions (tunnels, exfil, persistent C2) by total duration.' },
    ],

    TYPE_ICONS: {
        rarity: '<path d="M6 3h12l3 6-9 12L3 9z"/><path d="M3 9h18"/>',
        first_seen: '<circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15.5 14"/>',
        volume_baseline: '<polyline points="3 13 7 13 10 5 14 19 17 13 21 13"/>',
        beacon: '<circle cx="12" cy="12" r="2"/><path d="M7.8 7.8a6 6 0 0 0 0 8.4"/><path d="M16.2 16.2a6 6 0 0 0 0-8.4"/><path d="M4.9 4.9a10 10 0 0 0 0 14.2"/><path d="M19.1 19.1a10 10 0 0 0 0-14.2"/>',
        long_connection: '<path d="M10.5 13.5a4.5 4.5 0 0 0 6.6.4l2.6-2.6a4.5 4.5 0 0 0-6.4-6.4l-1.5 1.5"/><path d="M13.5 10.5a4.5 4.5 0 0 0-6.6-.4l-2.6 2.6a4.5 4.5 0 0 0 6.4 6.4l1.5-1.5"/>',
    },

    BASE_FIELDS: ['norm_log', 'contents', 'commandline', 'target_file', 'src_ip', 'dst_ip', 'user', 'image', 'parent_process', 'process_name'],

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
            network: this._networkFromDef({}),
            window: '1d',
            alertMode: 'paused',
            alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.8, percent_threshold: 5.0, alert_on_new: true, z_threshold: 3.5, beacon_threshold: 0.8, longconn_threshold: 0.5 },
            name: '',
            description: '',
            timeRange: '24h',
            fieldOrder: null,
            resultFields: [],
            results: [],
            resultCount: '',
            hasTimeline: false,
            ran: false,
            resultMode: 'logs',     // 'logs' (matching logs) | 'scores' (score preview)
            previewWindow: '7d',    // lookback for the score preview
            preview: null,          // last PreviewResult
            dirty: false,
        };
        this.currentView = 'editor';
        this._render();
    },

    // The editor is a viewport-height workspace like the search page and the alert
    // editor: the head and the rail stay put, and the bench scrolls inside itself.
    _renderEditorView(container) {
        const e = this.editor;
        const scores = e.resultMode === 'scores';
        const ranges = [['1h', 'Last 1 Hour'], ['6h', 'Last 6 Hours'], ['24h', 'Last 24 Hours'], ['7d', 'Last 7 Days'], ['30d', 'Last 30 Days']];
        const previews = [['1d', 'Last 1 Day'], ['7d', 'Last 7 Days'], ['30d', 'Last 30 Days']];
        const opts = (list, sel) => list.map(([v, l]) => `<option value="${v}" ${sel === v ? 'selected' : ''}>${l}</option>`).join('');
        container.innerHTML = `
<div class="model-editor-container">
    <div class="me-head">
        <div class="me-name">
            <input type="text" id="modelName" class="me-name-input" placeholder="Name this model"
                   spellcheck="false" autocomplete="off" value="${_esc(e.name)}">
            <span id="modelEditorStatus" class="me-status"><span class="me-status-dot"></span><span class="me-status-text">New</span></span>
        </div>
        <div class="me-actions">
            <span class="me-type-tag">Type <b id="modelTypeTagValue">${_esc(this._typeLabel(e.modelType))}</b></span>
            <button class="btn-primary" id="modelEditorSave">${e.editId ? 'Update Model' : 'Create Model'}</button>
        </div>
    </div>

    <div class="me-body">
        <div class="me-bench">
            <section class="search-section">
                <div class="search-toolbar">
                    <select id="modelTimeRange" class="time-range-select" ${scores ? 'hidden' : ''}>${opts(ranges, e.timeRange)}</select>
                    <select id="modelPreviewWindow" class="time-range-select" title="Lookback window for the score preview" ${scores ? '' : 'hidden'}>${opts(previews, e.previewWindow)}</select>
                    <div class="toolbar-spacer"></div>
                    <button class="search-btn" id="modelRunBtn" ${scores ? 'hidden' : ''}>
                        <span class="btn-text">Run</span>
                    </button>
                </div>

                <div class="query-input-row">
                    <div class="query-input-wrapper">
                        <div id="modelQueryHighlight" class="query-highlight"></div>
                        <textarea id="modelQueryInput" class="search-input" rows="1" spellcheck="false" autocomplete="off"
                                  placeholder="Filter logs in BQL, or leave empty to use every log">${_esc(e.query)}</textarea>
                    </div>
                </div>
                <div class="query-resize-handle" data-target="modelQueryInput"></div>
            </section>

            <div class="search-results-split">
                <section class="results-section">
                    <div class="timeline-inline" id="modelTimelineWrap" style="display:none;"><canvas id="modelTimeline"></canvas></div>

                    <div class="results-header">
                        <div class="editor-result-tabs" id="modelResultTabs">
                            <button type="button" class="ert-tab ${scores ? '' : 'active'}" data-mode="logs">Results</button>
                            <button type="button" class="ert-tab ${scores ? 'active' : ''}" data-mode="scores">Scores<span id="modelFlagChip" class="ert-chip" hidden></span></button>
                        </div>
                        <div class="results-controls">
                            <span class="result-count" id="modelResultsCount"></span>
                        </div>
                    </div>

                    <div id="modelResultsPane" ${scores ? 'hidden' : ''}>
                        <div class="sql-preview">
                            <div class="sql-header">
                                <strong>Generated SQL</strong>
                                <button id="modelToggleSqlBtn" class="toggle-sql-btn">Show SQL</button>
                            </div>
                            <code id="modelSqlOutput" style="display:none;"></code>
                        </div>
                        <div id="modelTranslation" class="model-translation"></div>
                        <div id="modelQueryResults" class="results-container">${this._benchEmpty()}</div>
                    </div>

                    <div id="modelScorePreview" class="model-score-pane" ${scores ? '' : 'hidden'}></div>
                </section>

                <div id="modelLogDetailPanel" class="log-detail-panel">
                    <div class="panel-resize-handle"></div>
                    <div class="panel-header">
                        <div class="panel-header-context">
                            <span class="log-level-badge"></span>
                            <span class="panel-timestamp"></span>
                            <span class="panel-source"></span>
                        </div>
                        <div class="panel-header-actions">
                            <button class="panel-nav-btn panel-prev-btn" title="Previous event">&#8249;</button>
                            <button class="panel-nav-btn panel-next-btn" title="Next event">&#8250;</button>
                            <button class="panel-nav-btn panel-search-btn" title="Search for this log" style="display:none;">
                                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
                            </button>
                            <button class="send-to-chat-btn" title="Analyze with AI">
                                <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor"><path d="M4 0C4.2 1.6 4.8 2.8 5.6 3.6 6.4 4.4 7.6 5 9 5.2 7.6 5.4 6.4 6 5.6 6.8 4.8 7.6 4.2 8.8 4 10.4 3.8 8.8 3.2 7.6 2.4 6.8 1.6 6 .4 5.4 0 5.2 .4 5 1.6 4.4 2.4 3.6 3.2 2.8 3.8 1.6 4 0z"/><path d="M11 6c.12 1 .48 1.72 1 2.24.52.52 1.24.88 2.24 1-1 .12-1.72.48-2.24 1-.52.52-.88 1.24-1 2.24-.12-1-.48-1.72-1-2.24-.52-.52-1.24-.88-2.24-1 1-.12 1.72-.48 2.24-1 .52-.52.88-1.24 1-2.24z"/><path d="M6 11c.08.68.32 1.16.68 1.52.36.36.84.6 1.52.68-.68.08-1.16.32-1.52.68-.36.36-.6.84-.68 1.52-.08-.68-.32-1.16-.68-1.52-.36-.36-.84-.6-1.52-.68.68-.08 1.16-.32 1.52-.68.36-.36.6-.84.68-1.52z"/></svg>
                            </button>
                            <button class="close-panel-btn">&times;</button>
                        </div>
                    </div>
                    <div class="panel-body"></div>
                </div>
            </div>
        </div>

        <div class="me-rail" id="modelRail">
            <div class="me-rail-handle" id="modelRailHandle" title="Drag to resize"></div>
            <div class="me-rail-content">
                <div class="me-sec">
                    <div class="me-sec-label">Type</div>
                    <div class="me-type-cards" id="modelTypeCards">${this._typeCardsHTML()}</div>
                    <p class="me-hint" id="modelTypeHelp">${_esc(this._typeDesc(e.modelType))}${e.editId ? ' The type of an existing model cannot be changed.' : ''}</p>
                </div>

                <div class="me-sec">
                    <div class="me-sec-label">Shape</div>
                    <div id="modelShapeConfig">${this._editorShapeHTML()}</div>
                </div>

                <div class="me-sec">
                    <div class="me-sec-label">Detection</div>
                    <div id="modelAlertConfig">${this._editorAlertConfigHTML()}</div>
                    <p class="me-hint">A paused alert is created with these thresholds${e.editId ? '' : ' on save'}. Enable it and set actions, throttling and severity from the Alerts page.</p>
                </div>

                <div class="me-sec">
                    <label class="me-sec-label" for="modelDesc">Description</label>
                    <textarea id="modelDesc" class="full-input" rows="4" placeholder="What this model measures and why">${_esc(e.description)}</textarea>
                </div>
            </div>
        </div>
    </div>
</div>`;

        document.getElementById('modelEditorSave').addEventListener('click', () => this._saveModel());
        document.getElementById('modelRunBtn').addEventListener('click', () => this._runOrCancelModel());
        const ta = document.getElementById('modelQueryInput');
        ta.addEventListener('input', ev => { e.query = ev.target.value; this._updateQueryHighlight(); this._schedulePreview(); });
        ta.addEventListener('scroll', () => this._syncQueryHighlightScroll());
        // The same keyboard contract as the search page and the alert editor.
        window.App?.bindQueryEditorKeys?.(ta, {
            historyKey: 'model',
            seed: e.query || '',
            onRun: () => { if (e.resultMode === 'scores') this._runScorePreview(); else this._runOrCancelModel(); },
        });
        ta.addEventListener('input', ev => {
            if (!window.App?.isUndoRedoing) window.App?.saveToHistory('model', ev.target.value);
        });
        this._updateQueryHighlight();
        // Live BQL validation: underline the offending span as the user types.
        if (window.QueryValidate) {
            this._detachQueryValidate = QueryValidate.attach({
                inputId: 'modelQueryInput',
                highlightId: 'modelQueryHighlight',
                getFractalId: () => e.fractalId || window.FractalContext?.currentFractal?.id || undefined,
                rerender: () => this._updateQueryHighlight(),
            });
        }
        // The query box gets the gutter and the drag handle the search page has.
        window.App?.setupQueryResizeHandles?.();
        window.App?.setupQueryLineNumbers?.();

        this._bindSqlToggle();
        document.getElementById('modelTimeRange').addEventListener('change', ev => { e.timeRange = ev.target.value; if (e.ran) this._runQuery(); });
        document.querySelectorAll('#modelResultTabs .ert-tab').forEach(b => {
            b.addEventListener('click', () => this._setResultMode(b.dataset.mode));
        });
        document.getElementById('modelPreviewWindow').addEventListener('change', ev => { e.previewWindow = ev.target.value; this._runScorePreview(); });
        this._bindTypeCards();
        this._bindEditorDetails();
        this._bindEditorShape();
        this._bindRail();
        this._renderTranslation();
        this._renderEditorStatus();

        // The editor DOM is rebuilt on every render, so (re)register its log
        // detail panel with the shared controller against the fresh element.
        if (window.LogDetail) {
            LogDetail.registerHost('model', '#modelLogDetailPanel', { tableRoot: '#modelQueryResults', storageKey: 'modelLogDetailPanelWidth' });
        }

        // Seed an initial run when editing (source query is pre-filled).
        if (e.editId && (e.query || '').trim()) {
            this._runQuery();
        }
        if (e.resultMode === 'scores') this._runScorePreview();
    },

    // The Results tab's count, remembered so leaving for the Scores tab and coming
    // back restores what is actually on screen rather than a number from two runs ago.
    _setResultCount(text) {
        this.editor.resultCount = text;
        if (this.editor.resultMode === 'scores') return;
        const el = document.getElementById('modelResultsCount');
        if (el) el.textContent = text;
    },

    _benchEmpty(title = 'Nothing run yet', detail = 'Run the query to preview matching logs and the fields you can build a shape from.') {
        return EmptyState.render({ icon: 'list', title, detail });
    },

    // Icon-and-label cards, as in the alert editor's type picker. The description of
    // whichever type is selected reads underneath, so five choices cost one line.
    _typeCardsHTML() {
        const e = this.editor;
        return this.MODEL_TYPES.map(t => `
<button type="button" class="me-type-card ${e.modelType === t.id ? 'active' : ''} ${e.editId ? 'me-type-card-locked' : ''}"
        data-type="${t.id}" title="${_esc(t.desc)}" ${e.editId ? 'disabled' : ''}>
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${this.TYPE_ICONS[t.id] || ''}</svg>
    <span class="me-type-card-label">${_esc(t.label)}</span>
</button>`).join('');
    },

    _bindTypeCards() {
        if (this.editor.editId) return;
        document.querySelectorAll('#modelTypeCards .me-type-card').forEach(card => {
            card.addEventListener('click', () => {
                const e = this.editor;
                e.modelType = card.dataset.type;
                document.querySelectorAll('#modelTypeCards .me-type-card').forEach(c => c.classList.toggle('active', c === card));
                const help = document.getElementById('modelTypeHelp');
                if (help) help.textContent = this._typeDesc(e.modelType);
                const tag = document.getElementById('modelTypeTagValue');
                if (tag) tag.textContent = this._typeLabel(e.modelType);
                this._renderEditorShape();
                this._renderEditorAlertConfig();
                this._markDirty();
                this._schedulePreview();
            });
        });
    },

    // The SQL block follows the "Show Query Debug" profile preference like every
    // other one on the site; this button is the local override while it is shown.
    _bindSqlToggle() {
        const btn = document.getElementById('modelToggleSqlBtn');
        const out = document.getElementById('modelSqlOutput');
        if (!btn || !out) return;
        btn.addEventListener('click', () => {
            const hidden = out.style.display === 'none';
            out.style.display = hidden ? 'block' : 'none';
            btn.textContent = hidden ? 'Hide SQL' : 'Show SQL';
        });
        if (window.UserPrefs && !UserPrefs.showSQL()) {
            const wrap = document.querySelector('#modelResultsPane .sql-preview');
            if (wrap) wrap.style.display = 'none';
        }
    },

    _bindRail() {
        window.App?.bindEditorRail?.(document.getElementById('modelRailHandle'), {
            body: document.querySelector('.me-body'),
            cssVar: '--me-rail-w',
            storageKey: 'bifract-model-rail-width',
            detail: document.getElementById('modelLogDetailPanel'),
            foldClass: 'me-inspecting',
        });
    },

    // Locks the page to the viewport while the editor or the data viewer is open,
    // as the search page and the alert editor do, and hands the height back on
    // the way out.
    // _render also runs while this tab is hidden (a scope switch renders every
    // view), so the lock is conditional on actually being on screen: a body locked
    // to the viewport with the tab hidden would freeze whatever tab is showing.
    _setWorkspaceChrome(on) {
        const tab = document.getElementById('fractalModelsTabContent');
        const view = document.getElementById('modelsView');
        const visible = !!tab && !!view && tab.style.display !== 'none' && view.style.display !== 'none';
        const active = on && visible;
        document.body.classList.toggle('models-workspace', active);
        if (visible) {
            tab.style.display = active ? 'flex' : 'block';
            view.style.display = active ? 'flex' : 'block';
        }
    },

    _markDirty() {
        if (this.currentView !== 'editor' || this.editor.dirty) return;
        this.editor.dirty = true;
        this._renderEditorStatus();
    },

    _renderEditorStatus() {
        const pill = document.getElementById('modelEditorStatus');
        const text = pill?.querySelector('.me-status-text');
        if (!pill || !text) return;
        pill.className = 'me-status';
        if (this.editor.dirty) {
            text.textContent = 'Unsaved changes';
            pill.classList.add('me-status-dirty');
        } else {
            text.textContent = this.editor.editId ? 'Saved' : 'New';
        }
    },

    // ---- BQL syntax highlighting (overlay over the query textarea) ----
    _updateQueryHighlight() {
        const ta = document.getElementById('modelQueryInput');
        const hl = document.getElementById('modelQueryHighlight');
        if (!ta || !hl || !window.SyntaxHighlight) return;
        hl.innerHTML = SyntaxHighlight.highlight(ta.value, SyntaxHighlight.errorRanges['modelQueryInput'], SyntaxHighlight.matchRanges['modelQueryInput']) + '<br/>';
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

    // Normalizes a definition's network field map to the editor shape with defaults.
    _networkFromDef(def) {
        const n = (def && def.network) || {};
        return {
            src_field: n.src_field || 'src_ip',
            dst_field: n.dst_field || 'dst_ip',
            port_field: n.port_field || 'dst_port',
            duration_field: n.duration_field || 'duration',
            bytes_field: n.bytes_field || 'orig_bytes',
        };
    },

    _isNetworkType(mt) { return mt === 'beacon' || mt === 'long_connection'; },

    _typeLabel(mt) {
        return this.MODEL_TYPES.find(t => t.id === mt)?.label || mt;
    },

    _typeDesc(mt) {
        return this.MODEL_TYPES.find(t => t.id === mt)?.desc || '';
    },

    // ---- Shape (right panel) ----
    _editorShapeHTML() {
        const e = this.editor;
        if (this._isNetworkType(e.modelType)) {
            const n = e.network;
            const isBeacon = e.modelType === 'beacon';
            const windows = [['1d', '1 day'], ['7d', '7 days'], ['14d', '14 days']];
            return `
<div class="field-group">
    <label>Source field</label>
    ${this._fieldInput('netSrc', n.src_field, 'src_ip')}
</div>
<div class="field-group" style="margin-top:10px">
    <label>Destination field</label>
    ${this._fieldInput('netDst', n.dst_field, 'dst_ip')}
</div>
<div class="field-group" style="margin-top:10px">
    <label>Port field</label>
    ${this._fieldInput('netPort', n.port_field, 'dst_port')}
</div>
${isBeacon ? `
<div class="field-group" style="margin-top:10px">
    <label>Bytes field (connection size)</label>
    ${this._fieldInput('netBytes', n.bytes_field, 'orig_bytes')}
</div>` : `
<div class="field-group" style="margin-top:10px">
    <label>Duration field (seconds)</label>
    ${this._fieldInput('netDuration', n.duration_field, 'duration')}
</div>`}
<div class="field-group" style="margin-top:10px">
    <label>Rolling window</label>
    <select id="netWindow" class="full-input">
        ${windows.map(([v, l]) => `<option value="${v}" ${e.window === v ? 'selected' : ''}>${l}</option>`).join('')}
    </select>
</div>
<p class="config-hint">${isBeacon
    ? 'Scores the regularity of connection timing and size per (source, destination, port). A longer window catches slower beacons (e.g. daily check-ins).'
    : 'Scores the total connection duration per (source, destination, port). A longer window aggregates recurring long sessions.'}</p>`;
        }
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
        if (this._isNetworkType(e.modelType)) {
            const bindField = (id, key) => document.getElementById(id)?.addEventListener('input', ev => { e.network[key] = ev.target.value.trim(); this._schedulePreview(); });
            bindField('netSrc', 'src_field');
            bindField('netDst', 'dst_field');
            bindField('netPort', 'port_field');
            bindField('netBytes', 'bytes_field');
            bindField('netDuration', 'duration_field');
            document.getElementById('netWindow')?.addEventListener('change', ev => { e.window = ev.target.value; this._schedulePreview(); });
            return;
        }
        if (e.modelType === 'rarity') {
            const pSel = document.getElementById('shapePartKey');
            const vSel = document.getElementById('shapeValKey');
            if (pSel) pSel.addEventListener('input', ev => { e.partitionKey = ev.target.value.trim(); this._schedulePreview(); });
            if (vSel) vSel.addEventListener('input', ev => { e.valueKey = ev.target.value.trim(); this._schedulePreview(); });
            document.getElementById('shapeMinSample')?.addEventListener('change', ev => { e.minSample = parseInt(ev.target.value) || 5; this._schedulePreview(); });
        } else {
            this._bindKeyFieldEvents();
            document.getElementById('addKeyField')?.addEventListener('click', () => {
                e.keyFields.push('');
                this._renderEditorShape();
            });
            if (e.modelType === 'volume_baseline') {
                document.getElementById('shapeTimeBucket')?.addEventListener('change', ev => { e.timeBucket = ev.target.value; this._renderEditorShape(); this._schedulePreview(); });
                document.getElementById('shapeMinSample')?.addEventListener('change', ev => { e.minSample = parseInt(ev.target.value) || 7; this._schedulePreview(); });
            }
        }
    },

    _bindKeyFieldEvents() {
        const e = this.editor;
        document.querySelectorAll('#keyFieldsList .key-field-row').forEach(row => {
            const i = parseInt(row.dataset.idx);
            const sel = row.querySelector('.model-field-input');
            sel.addEventListener('input', ev => { e.keyFields[i] = ev.target.value.trim(); this._schedulePreview(); });
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
        if (mt === 'beacon') {
            typeFields = `
    <div class="field-group" style="margin-top:10px">
        <label>Beacon score threshold</label>
        <input type="number" id="alertBeaconThreshold" class="model-num-input" value="${c.beacon_threshold}" min="0" max="1" step="0.05">
        <p class="config-hint">Alert when a pair's final beacon score (regularity, reranked by prevalence) is at or above this. 0.8 is a strong-signal cutoff.</p>
    </div>`;
        } else if (mt === 'long_connection') {
            typeFields = `
    <div class="field-group" style="margin-top:10px">
        <label>Long-connection score threshold</label>
        <input type="number" id="alertLongConnThreshold" class="model-num-input" value="${c.longconn_threshold}" min="0" max="1" step="0.05">
        <p class="config-hint">Alert when a pair's duration score is at or above this. 0.5 corresponds to the ~8h tier.</p>
    </div>`;
        } else if (mt === 'rarity') {
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
        const name = document.getElementById('modelName');
        name.addEventListener('input', ev => { e.name = ev.target.value; this._sizeNameInput(); });
        this._sizeNameInput();
        document.getElementById('modelDesc').addEventListener('input', ev => { e.description = ev.target.value; });
        this._bindAlertConfigEvents();

        // One listener for the whole definition. The preview range, the result tabs
        // and paging sit outside it, because looking at something is not an edit.
        const root = document.querySelector('.model-editor-container');
        if (root) {
            const onEdit = ev => {
                if (!ev.target.closest('.me-rail, .me-name-input, #modelQueryInput')) return;
                this._markDirty();
            };
            root.addEventListener('input', onEdit);
            root.addEventListener('change', onEdit);
        }
    },

    // The name field is as wide as its text, so the status pill reads as part of it.
    _sizeNameInput() {
        const input = document.getElementById('modelName');
        if (!input) return;
        let probe = document.getElementById('modelNameProbe');
        if (!probe) {
            probe = document.createElement('span');
            probe.id = 'modelNameProbe';
            probe.className = 'me-name-probe';
            input.parentElement.appendChild(probe);
        }
        probe.textContent = input.value || input.placeholder || '';
        input.style.width = Math.min(Math.max(probe.offsetWidth + 22, 60), 640) + 'px';
    },

    _bindAlertConfigEvents() {
        const c = this.editor.alertConfig;
        document.getElementById('alertConfidence')?.addEventListener('change', ev => { c.confidence_threshold = parseFloat(ev.target.value); this._schedulePreview(); });
        document.getElementById('alertPercent')?.addEventListener('change', ev => { c.percent_threshold = parseFloat(ev.target.value); this._schedulePreview(); });
        document.getElementById('alertZThreshold')?.addEventListener('change', ev => { c.z_threshold = parseFloat(ev.target.value); this._schedulePreview(); });
        document.getElementById('alertOnNew')?.addEventListener('change', ev => { c.alert_on_new = ev.target.checked; this._schedulePreview(); });
        document.getElementById('alertBeaconThreshold')?.addEventListener('change', ev => { c.beacon_threshold = parseFloat(ev.target.value); this._schedulePreview(); });
        document.getElementById('alertLongConnThreshold')?.addEventListener('change', ev => { c.longconn_threshold = parseFloat(ev.target.value); this._schedulePreview(); });
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
            parts.push(`<div class="model-trans-errors">${p.errors.map(x => `<div class="model-trans-error">${_esc(x)}</div>`).join('')}</div>`);
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
        const wasCount = e.resultCount;
        const timelineWrapEl = document.getElementById('modelTimelineWrap');
        if (timelineWrapEl) timelineWrapEl.style.display = 'none';
        this._setModelRunState(true);

        const { start, end } = this._editorTimeRange();
        const qbody = { query: e.query || '*', start, end, source: 'model' };
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

            // Live results. The SQL block itself is governed by the profile's
            // "Show Query Debug" preference, so only its content is set here.
            const sqlEl = document.getElementById('modelSqlOutput');
            if (sqlEl) {
                sqlEl.innerHTML = (queryData && queryData.sql && window.QueryExecutor)
                    ? QueryExecutor.highlightSQL(queryData.sql) : '';
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
                e.hasTimeline = true;
            } else {
                e.hasTimeline = false;
                if (timelineWrapEl) timelineWrapEl.style.display = 'none';
            }

            if (!e.query) {
                if (resultsEl) resultsEl.innerHTML = this._benchEmpty('No filter', 'This model will process every log in the fractal. Add a BQL filter to narrow it.');
                this._setResultCount('');
            } else if (queryData && queryData.error) {
                if (resultsEl) resultsEl.innerHTML = `<div class="query-error"><p>Query Error: ${_esc(queryData.error)}</p></div>`;
                this._setResultCount('Error');
            } else if (queryData) {
                const results = queryData.results || [];
                e.results = results;
                e.fieldOrder = queryData.field_order || null;
                e.resultFields = this._collectResultFields(queryData);
                // Refresh shape datalists so partition/value keys suggest the fields
                // actually present in the freshly searched data.
                this._renderEditorShape();
                this._setResultCount(`${results.length} result${results.length === 1 ? '' : 's'}`);
                if (!results.length) {
                    if (resultsEl) resultsEl.innerHTML = this._benchEmpty('No matching logs', 'Nothing matched this filter in the selected time range. Widen the range or loosen the query.');
                } else if (window.QueryExecutor && resultsEl) {
                    QueryExecutor.renderResultsToElement(results.slice(0, 100), resultsEl, e.fieldOrder, {
                        allResults: results, isAggregated: queryData.is_aggregated || false, detailHost: 'model'
                    });
                }
            }
        } catch (err) {
            // Cancelled: put back what the pane last said rather than leaving
            // "Running…" over a spinner that is no longer running. A newer run
            // aborts this one too and owns the pane, so only an untouched
            // sequence means the user pressed Cancel.
            if (err.name === 'AbortError') {
                if (seq === this._runSeq) {
                    this._setResultCount(wasCount || '');
                    if (resultsEl) resultsEl.innerHTML = this._benchEmpty('Run cancelled', 'The query was cancelled before it returned.');
                }
                return;
            }
            if (seq !== this._runSeq) return;
            if (resultsEl) resultsEl.innerHTML = `<div class="query-error"><p>Query Error: ${_esc(err.message)}</p></div>`;
            this._setResultCount('Error');
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

        const shapeErr = this._validateShape();
        if (shapeErr) { Toast.warning(shapeErr); return; }
        const def = this._composeDefinition(filter, extractions);

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
            e.dirty = false;
            window.App?.pushSubPath('');
            this.currentView = 'list';
            this._render();
            await this._loadModels();
        } catch (err) {
            Toast.error(err.message || 'Failed to save model');
            if (btn) btn.disabled = false;
        }
    },

    // ---- Shared definition building (save + preview) ----
    // Returns an error message if the shape is incomplete, else null.
    _validateShape() {
        const e = this.editor;
        if (e.modelType === 'rarity' && (!e.partitionKey || !e.valueKey)) return 'Select a partition key and a value key';
        if ((e.modelType === 'first_seen' || e.modelType === 'volume_baseline') && !e.keyFields.filter(Boolean).length) {
            return e.modelType === 'volume_baseline' ? 'Add at least one entity field' : 'Add at least one key field';
        }
        if (this._isNetworkType(e.modelType) && (!e.network.src_field || !e.network.dst_field)) {
            return 'Set a source and destination field';
        }
        return null;
    },

    // Builds the ModelDefinition payload from the current shape + alert config.
    _composeDefinition(filter, extractions) {
        const e = this.editor;
        const def = { filter: filter || [], extractions: extractions || [] };
        if (e.modelType === 'rarity') {
            def.partition_key = e.partitionKey;
            def.value_key = e.valueKey;
            def.min_sample = e.minSample;
        } else if (e.modelType === 'volume_baseline') {
            def.key_fields = e.keyFields.filter(Boolean);
            def.time_bucket = e.timeBucket;
            def.min_sample = e.minSample;
        } else if (this._isNetworkType(e.modelType)) {
            def.network = { ...e.network };
            def.window = e.window;
            if (e.modelType === 'beacon') {
                def.beacon = { score_threshold: e.alertConfig.beacon_threshold };
            } else {
                def.long_conn = { score_threshold: e.alertConfig.longconn_threshold };
            }
        } else {
            def.key_fields = e.keyFields.filter(Boolean);
        }
        def.alert = { ...e.alertConfig };
        return def;
    },

    // ---- Score preview ----
    _setResultMode(mode) {
        const e = this.editor;
        e.resultMode = mode;
        const scores = mode === 'scores';
        // The detail panel only applies to the matching-logs view.
        if (window.LogDetail) LogDetail.close();
        document.querySelectorAll('#modelResultTabs .ert-tab').forEach(b => b.classList.toggle('active', b.dataset.mode === mode));
        const show = (id, vis) => { const el = document.getElementById(id); if (el) el.hidden = !vis; };
        show('modelResultsPane', !scores);
        show('modelScorePreview', scores);
        show('modelTimeRange', !scores);
        show('modelPreviewWindow', scores);
        show('modelRunBtn', !scores);
        // The count and the timeline describe the pane on screen, so they do not
        // follow the tab out, and they come back with it.
        const countEl = document.getElementById('modelResultsCount');
        if (countEl) countEl.textContent = scores ? '' : (e.resultCount || '');
        const tl = document.getElementById('modelTimelineWrap');
        if (tl) tl.style.display = (!scores && e.hasTimeline) ? '' : 'none';
        if (scores) this._runScorePreview();
    },

    // Debounced re-run so live threshold/shape tweaks update the preview without
    // a request per keystroke. No-op unless the preview tab is active.
    _schedulePreview() {
        if (this.editor.resultMode !== 'scores') return;
        clearTimeout(this._previewTimer);
        this._previewTimer = setTimeout(() => this._runScorePreview(), 450);
    },

    async _runScorePreview() {
        const e = this.editor;
        const panel = document.getElementById('modelScorePreview');
        if (!panel) return;

        // Bump the sequence first so any in-flight response is discarded even when
        // we early-return below (e.g. an edit that makes the shape invalid).
        const seq = (this._previewSeq = (this._previewSeq || 0) + 1);

        const shapeErr = this._validateShape();
        if (shapeErr) {
            const chip = document.getElementById('modelFlagChip');
            if (chip) chip.hidden = true;
            const count = document.getElementById('modelResultsCount');
            if (count) count.textContent = '';
            panel.innerHTML = this._benchEmpty('Shape incomplete', _esc(shapeErr) + ' to preview scores.');
            return;
        }

        panel.innerHTML = '<div class="loading-spinner"><span class="spinner"></span></div>';
        const countEl = document.getElementById('modelResultsCount');
        if (countEl) countEl.textContent = '';

        // Resolve filter/extractions authoritatively from the current query.
        e.query = (document.getElementById('modelQueryInput')?.value || '').trim();
        let filter = [], extractions = [];
        if (e.query) {
            const pd = await this._api('POST', '/models/parse-query', { query: e.query, model_type: e.modelType }).catch(() => null);
            if (seq !== this._previewSeq) return;
            const d = pd?.data;
            if (d?.errors?.length) {
                panel.innerHTML = `<div class="query-error"><p>${_esc(d.errors[0])}</p></div>`;
                return;
            }
            filter = d?.definition?.filter || [];
            extractions = d?.definition?.extractions || [];
        }

        const def = this._composeDefinition(filter, extractions);
        try {
            const data = await this._api('POST', '/models/preview', { model_type: e.modelType, definition: def, window: e.previewWindow });
            if (seq !== this._previewSeq) return;
            e.preview = data?.data || null;
            this._renderScorePreview();
        } catch (err) {
            if (seq !== this._previewSeq) return;
            panel.innerHTML = `<div class="query-error"><p>${_esc(err.message || 'Preview failed')}</p></div>`;
        }
    },

    _renderScorePreview() {
        const panel = document.getElementById('modelScorePreview');
        if (!panel) return;
        const p = this.editor.preview;
        const chip = document.getElementById('modelFlagChip');
        if (!p) {
            if (chip) chip.hidden = true;
            panel.innerHTML = this._benchEmpty('No preview available', 'The model produced no scores for this window.');
            return;
        }

        const s = p.stats || {};
        const num = v => this._fmtNum(Number(v || 0));
        let chips = [];
        if (p.model_type === 'rarity') {
            chips = [
                [num(s.scored_values), 'values'],
                [num(s.partitions), 'partitions'],
                [Number(s.max_confidence || 0).toFixed(2), 'max confidence'],
                [Number(s.avg_confidence || 0).toFixed(2), 'avg confidence'],
            ];
        } else if (p.model_type === 'first_seen') {
            chips = [
                [num(s.entities), 'entities'],
                [num(s.new_recent), 'new (last 24h)'],
            ];
        } else if (p.model_type === 'volume_baseline') {
            chips = [
                [num(s.entities_scored), 'entities scored'],
                [Number(s.max_z || 0).toFixed(2), 'max |z|'],
                [num(s.min_buckets), 'min history'],
            ];
        } else if (this._isNetworkType(p.model_type)) {
            chips = [
                [num(s.pairs_scored), 'pairs scored'],
                [Number(s.max_score || 0).toFixed(2), 'max score'],
                [num(s.network_size), 'hosts'],
            ];
        }

        const scoredTotal = Number(s.scored_values || s.entities || s.entities_scored || s.pairs_scored || 0);
        const flags = Number(p.would_flag || 0);
        // The tab carries the number that matters: how much this model would fire.
        if (chip) {
            chip.textContent = this._fmtNum(flags);
            chip.classList.toggle('warn', flags > 0);
            chip.hidden = false;
        }
        const flagBadge = `<div class="score-flag-badge ${flags > 0 ? 'has-flags' : ''}">
    <span class="score-flag-count">${this._fmtNum(flags)}</span>
    <span class="score-flag-text">would flag</span>
    <span class="score-flag-basis">${_esc(p.flag_basis || '')}</span>
</div>`;

        const chipsHTML = chips.map(([v, l]) => `<div class="score-stat-chip"><span class="score-stat-val">${_esc(String(v))}</span><span class="score-stat-label">${_esc(l)}</span></div>`).join('');

        // Volume baseline needs several complete buckets to score; surface why it
        // may be empty over a short window rather than showing a blank chart.
        let hint = '';
        if (p.model_type === 'volume_baseline' && scoredTotal === 0) {
            hint = `<div class="score-preview-hint">No entity has enough complete buckets in this window to establish a baseline. Try a longer window or the per-hour bucket.</div>`;
        } else if (scoredTotal === 0) {
            hint = `<div class="score-preview-hint">No matching results in the last ${_esc(p.window)}.</div>`;
        }

        const countEl = document.getElementById('modelResultsCount');
        if (countEl) countEl.textContent = `${this._fmtNum(scoredTotal)} scored`;

        const histHTML = this._buildHistogramHTML(p.histogram || [], p.metric, p.model_type === 'rarity' ? this.editor.alertConfig.confidence_threshold : null);
        const topHTML = this._previewTopTableHTML(p.top_columns || [], p.top || []);

        panel.innerHTML = `
<div class="score-preview">
    <div class="score-preview-head">
        <div class="score-preview-stats">${chipsHTML}</div>
        ${flagBadge}
    </div>
    ${hint}
    ${histHTML}
    ${topHTML}
</div>`;
    },

    // Builds the score-distribution chart markup, reusing the model viewer's
    // histogram styles. thresholdFrac (0..1), when provided, draws a marker line.
    _buildHistogramHTML(buckets, metricKey, thresholdFrac) {
        const metric = this.METRIC_LABELS[metricKey] || 'Score';
        const arr = Array.isArray(buckets) ? buckets : [];
        const max = arr.reduce((m, b) => Math.max(m, Number(b.count || 0)), 0);
        if (!arr.length || max <= 0) {
            return `<div class="histogram-head"><span class="histogram-title">${_esc(metric)} distribution</span></div>
<div class="histogram-empty">Not enough data to show a distribution.</div>`;
        }
        const cols = arr.map(b => {
            const cnt = Number(b.count || 0);
            const pct = max > 0 ? Math.round(cnt / max * 100) : 0;
            return `<div class="histogram-col" title="${_esc(b.label)}: ${cnt.toLocaleString()}">
    <span class="histogram-bar-val">${this._fmtNum(cnt)}</span>
    <div class="histogram-bar-track"><div class="histogram-bar" style="height:${cnt > 0 ? Math.max(pct, 2) : 0}%"></div></div>
    <span class="histogram-bar-label">${_esc(b.label)}</span>
</div>`;
        }).join('');
        let thresholdLine = '';
        if (thresholdFrac != null && thresholdFrac >= 0 && thresholdFrac <= 1) {
            const label = _esc(Number(thresholdFrac).toFixed(2)) + ' alert';
            thresholdLine = `<div class="histogram-threshold-line" style="left:calc(12px + (100% - 24px) * ${thresholdFrac})" title="Alert threshold: ${_esc(String(thresholdFrac))}"><span class="histogram-threshold-label">${label}</span></div>`;
        }
        return `<div class="histogram-head"><span class="histogram-title">${_esc(metric)} distribution</span></div>
<div class="histogram-chart">${cols}${thresholdLine}</div>`;
    },

    _previewTopTableHTML(columns, rows) {
        if (!columns.length || !rows.length) return '';
        const head = columns.map(c => `<th>${_esc(this._colLabel(c))}</th>`).join('');
        const body = rows.map(r => `<tr>${columns.map(c => `<td>${this._fmtScoreVal(c, r[c])}</td>`).join('')}</tr>`).join('');
        return `<div class="score-top">
    <div class="score-top-title">Top results</div>
    <div class="score-top-scroll"><table class="score-top-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>
</div>`;
    },

    _colLabel(c) {
        const map = {
            partition_val: 'Partition', value_val: 'Value', model_count: 'Days seen', percent: '%', confidence: 'Confidence',
            entity_key: 'Entity', entity_val: 'Entity', first_seen: 'First seen', last_seen: 'Last seen', event_count: 'Events',
            latest_count: 'Latest', baseline_median: 'Median', mad: 'MAD', n_buckets: 'Buckets', z_score: 'z-score',
            src_ip: 'Source', dst_ip: 'Destination', dst_port: 'Port', final_score: 'Score', score: 'Score',
            regularity: 'Regularity', ts_score: 'Timing', ds_score: 'Size', dur_score: 'Duration', hist_score: 'Histogram',
            prevalence: 'Prevalence', conn_count: 'Conns', total_duration: 'Total dur (s)',
        };
        return map[c] || c.replace(/_/g, ' ');
    },

    _fmtScoreVal(col, v) {
        if (v === null || v === undefined) return '';
        if (col === 'confidence') return _esc(Number(v).toFixed(3));
        if (col === 'percent') return _esc(Number(v).toFixed(2) + '%');
        if (col === 'z_score' || col === 'baseline_median' || col === 'mad') return _esc(Number(v).toFixed(2));
        if (col === 'score' || col === 'final_score' || col === 'regularity' || col === 'ts_score' || col === 'ds_score' || col === 'dur_score' || col === 'hist_score' || col === 'prevalence') return _esc(Number(v).toFixed(3));
        if (col === 'conn_count' || col === 'total_duration') return _esc(this._fmtNum(Number(v)));
        if (col === 'model_count' || col === 'event_count' || col === 'latest_count' || col === 'n_buckets') return _esc(this._fmtNum(Number(v)));
        if (col === 'first_seen' || col === 'last_seen') {
            return _esc(TZ.format(v, 'friendly') || String(v));
        }
        return `<span class="score-cell-val" title="${_esc(String(v))}">${_esc(String(v))}</span>`;
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

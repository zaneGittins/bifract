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
        parsed: { source_bql: '', filter_complete: true, computed_fields: [], filter: [], extractions: [], candidate_fields: [], errors: [], warnings: [] },
        partitionKey: '',
        valueKey: '',
        keyFields: [''],
        minSample: 1,
        timeBucket: 'day',
        alertMode: 'paused',
        alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.9, percent_threshold: 10.0, alert_on_new: true, z_threshold: 3.5 },
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
        view: 'findings', // 'findings' (what the alert would raise) | 'all'
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
        <th>Name</th><th>Type</th><th>State</th>
        <th title="What the model surfaced over the last 7 days">Findings <span class="models-th-sub">7d</span></th>
        <th>Last alert</th><th>Alert</th>
    </tr></thead>
    <tbody>${filtered.map(m => this._modelRow(m)).join('')}</tbody>
</table>`;
        wrap.querySelectorAll('.model-name-link').forEach(btn => {
            btn.addEventListener('click', () => this._openDataViewer(btn.dataset.id));
        });
        wrap.querySelectorAll('.alert-mode-badge[data-id]').forEach(badge => {
            badge.addEventListener('click', () => this._toggleAlertMode(badge.dataset.id, badge.dataset.mode));
        });

        // Keep the listing live while any model is seeding, or while the server is
        // still reading a model's state for its health.
        const pending = m => m.backfill_status === 'running' || m.health?.state === 'checking';
        if (this.models.some(pending)) this._startListPoll();
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
        return `
<tr data-model-id="${_esc(m.id)}">
    <td><button class="model-name-link" data-id="${m.id}" title="Open ${_esc(m.name)}">${_esc(m.name)}</button><div class="model-desc">${_esc(m.description)}</div></td>
    <td>${_esc(this._typeLabel(m.model_type))}</td>
    <td class="model-state-cell">${this._healthBadge(m)}</td>
    <td class="model-findings-cell">${this._findingsCell(m)}</td>
    <td class="model-last-alert-cell">${this._lastAlertCell(m)}</td>
    <td>${this._alertModeBadge(m)}</td>
</tr>`;
    },

    // One state per model, worst first; the server decides which (health.go) and
    // the badge only names it. [class, label]
    HEALTH_BADGES: {
        error:        ['badge-error', 'Error'],
        not_updating: ['badge-stalled', 'Not updating'],
        not_started:  ['badge-stalled', 'Not started'],
        behind:       ['badge-stalled', 'Behind'],
        rebuilding:   ['badge-rebuilding', 'Rebuilding'],
        backfilling:  ['badge-progress', 'Backfill'],
        stale:        ['badge-stale', 'Stale'],
        learning:     ['badge-learning', 'Learning'],
        healthy:      ['badge-healthy', 'Healthy'],
        checking:     ['badge-checking', 'Checking'],
        unknown:      ['badge-none', 'Unknown'],
    },

    _healthBadge(m) {
        const h = m.health;
        if (!h) {
            const cls = { active: 'badge-active', error: 'badge-error', rebuilding: 'badge-rebuilding' }[m.status] || 'badge-none';
            return `<span class="model-badge ${cls}"><span class="model-dot"></span>${_esc(this._statusLabel(m.status))}</span>`;
        }
        const [cls, base] = this.HEALTH_BADGES[h.state] || ['badge-none', this._statusLabel(h.state)];
        let label = base;
        if (h.state === 'behind' && m.state_lag_seconds != null) label += ' ' + this._lagLabel(m.state_lag_seconds);
        if (h.state === 'backfilling') label += ` ${this._backfillPct(m)}%`;
        if (h.state === 'stale' && h.newest_data) label += ' ' + this._lagLabel((Date.now() - TZ.toEpoch(h.newest_data)) / 1000);
        if (h.state === 'learning' && h.history_needed) label += ` ${h.history || 0}/${h.history_needed}${h.history_unit === 'hour' ? 'h' : 'd'}`;
        const lines = [h.detail || ''];
        if (h.newest_data) lines.push(`Newest event: ${TZ.format(h.newest_data, 'friendly')}`);
        return `<span class="model-badge ${cls}" title="${_esc(lines.filter(Boolean).join('\n'))}"><span class="model-dot"></span>${_esc(label)}</span>`;
    },

    // Findings over the last 7 days, with a per-day sparkline where the model
    // keeps a daily history of them.
    _findingsCell(m) {
        const h = m.health;
        if (!h || h.findings == null) {
            const why = m.model_type === 'tlsh' ? 'A TLSH index raises no findings' : 'Not computed yet';
            return `<span class="model-muted" title="${why}">n/a</span>`;
        }
        const n = Number(h.findings) || 0;
        const series = Array.isArray(h.findings_series) ? h.findings_series : [];
        let title = h.findings_basis || '';
        if (series.length) {
            const day = i => new Date(Date.now() - (series.length - 1 - i) * 86400000).toISOString().slice(5, 10);
            title += '\n' + series.map((c, i) => `${day(i)}: ${Number(c).toLocaleString()}`).join('\n');
        }
        return `<span class="model-findings${n > 0 ? ' has-findings' : ''}" title="${_esc(title)}">` +
            `<span class="model-findings-n">${this._fmtNum(n)}</span>${series.length ? this._sparkline(series) : ''}</span>`;
    },

    // A 7-bar sparkline; an empty day keeps a 1px tick so the week reads as a week.
    _sparkline(series) {
        const max = Math.max(1, ...series.map(Number));
        const w = 4, gap = 2, hgt = 14;
        const bars = series.map((v, i) => {
            const n = Number(v) || 0;
            const bh = n > 0 ? Math.max(2, Math.round(n / max * hgt)) : 1;
            return `<rect x="${i * (w + gap)}" y="${hgt - bh}" width="${w}" height="${bh}" rx="1"${n > 0 ? '' : ' class="spark-zero"'}/>`;
        }).join('');
        return `<svg class="model-spark" width="${series.length * (w + gap) - gap}" height="${hgt}" viewBox="0 0 ${series.length * (w + gap) - gap} ${hgt}" aria-hidden="true">${bars}</svg>`;
    },

    _lastAlertCell(m) {
        if (!m.linked_alert_id) return '<span class="model-muted" title="Collect only: no alert">n/a</span>';
        if (!m.last_alert_at) return '<span class="model-muted">Never</span>';
        const secs = (Date.now() - TZ.toEpoch(m.last_alert_at)) / 1000;
        const rel = secs < 86400 ? `${this._lagLabel(Math.max(0, secs))} ago` : (FactChips.ago(m.last_alert_at) || TZ.format(m.last_alert_at, 'date'));
        return `<span class="model-last-alert" title="${_esc(TZ.format(m.last_alert_at, 'friendly'))}">${_esc(rel)}</span>`;
    },

    _lagLabel(seconds) {
        const s = Math.floor(Number(seconds) || 0);
        if (s < 60) return `${s}s`;
        if (s < 3600) return `${Math.floor(s / 60)}m`;
        if (s < 86400) return `${Math.floor(s / 3600)}h`;
        return `${Math.floor(s / 86400)}d`;
    },

    // min_sample is one stored field with two meanings, so its sensible default
    // differs by type. For rarity it is a floor on how many times a value must
    // have been seen before it is scored, and anything above 1 silently gives up
    // first sightings, which is usually the detection you wanted. For volume
    // baseline it is how much history a median and MAD need to mean anything.
    _defaultMinSample(modelType) {
        return modelType === 'volume_baseline' ? 7 : 1;
    },

    _statusLabel(status) {
        const s = String(status || '');
        return s ? s.charAt(0).toUpperCase() + s.slice(1) : 'Unknown';
    },

    // Active is the only mode that can page someone, so it is the only one with
    // weight; paused is an outline, collect-only is plain text.
    _alertModeBadge(m) {
        switch (m.alert_mode) {
            case 'active': return `<span class="model-badge badge-alert-active alert-mode-badge" data-id="${m.id}" data-mode="active" title="Click to pause"><span class="model-dot"></span>Active</span>`;
            case 'paused': return `<span class="model-badge badge-shadow alert-mode-badge" data-id="${m.id}" data-mode="paused" title="Click to activate">Paused</span>`;
            default:       return `<span class="model-collect-only" title="The model records data but raises no alert">Collect only</span>`;
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
        const alertCfg = { severity: 'medium', action_ids: [], confidence_threshold: 0.9, percent_threshold: 10.0, alert_on_new: true, z_threshold: 3.5, beacon_threshold: 0.8, longconn_threshold: 0.5 };
        if (def.alert) Object.assign(alertCfg, def.alert);
        if (def.beacon && def.beacon.score_threshold != null) alertCfg.beacon_threshold = def.beacon.score_threshold;
        if (def.long_conn && def.long_conn.score_threshold != null) alertCfg.longconn_threshold = def.long_conn.score_threshold;
        this.editor = {
            editId: m.id,
            modelType: m.model_type || 'rarity',
            query: m.source_query || '',
            parsed: { source_bql: def.source_bql || '', filter_complete: true, computed_fields: [], filter: (def.filter || []).map(f => ({ ...f })), extractions: (def.extractions || []).map(e => ({ ...e })), candidate_fields: [], errors: [], warnings: [] },
            partitionKey: def.partition_key || '',
            valueKey: def.value_key || '',
            keyFields: (def.key_fields && def.key_fields.length) ? [...def.key_fields] : [''],
            minSample: def.min_sample || this._defaultMinSample(m.model_type || 'rarity'),
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
            previewStart: '',
            previewEnd: '',
            preview: null,
            previewCfg: null,
            showGallery: false,
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
    // still shown in the row drawer. { why: true } places the fact-chip column.
    VIEW_SPEC: {
        rarity: {
            sortDefault: 'confidence',
            sortable: ['partition_val', 'value_val', 'model_count', 'percent', 'confidence'],
            score: { col: 'confidence', threshold: d => d.alert?.confidence_threshold ?? 0.8 },
            cols: [
                { col: 'confidence', label: 'Confidence', fmt: 'meter', align: 'num' },
                { col: 'partition_val', label: d => d.partition_key || 'Partition' },
                { col: 'value_val', label: d => d.value_key || 'Value' },
                { why: true },
                { col: 'percent', label: 'Share of days', fmt: 'pct100', align: 'num' },
            ],
        },
        first_seen: {
            sortDefault: 'first_seen',
            sortable: ['entity_key', 'first_seen', 'last_seen', 'event_count'],
            cols: [
                { col: 'entity_key', keys: true },
                { why: true },
                { col: 'first_seen', label: 'First seen', fmt: 'ts' },
                { col: 'last_seen', label: 'Last seen', fmt: 'ts' },
                { col: 'event_count', label: 'Events', fmt: 'int', align: 'num' },
            ],
        },
        tlsh: {
            sortDefault: 'first_seen',
            sortable: ['digest', 'first_seen', 'last_seen', 'event_count'],
            cols: [
                { col: 'digest', keys: true },
                { why: true },
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
                { why: true },
                { col: 'latest_count', label: 'Latest', fmt: 'int', align: 'num' },
                { col: 'baseline_median', label: 'Baseline', fmt: 'num', align: 'num' },
                { col: 'mad', label: 'MAD', fmt: 'num', align: 'num' },
                { col: 'n_buckets', label: 'History', fmt: 'int', align: 'num' },
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
                { why: true },
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
                { why: true },
                { col: 'total_duration', label: 'Total duration', fmt: 'dur', align: 'num' },
                { col: 'conn_count', label: 'Connections', fmt: 'int', align: 'num' },
                { col: 'prevalence', label: 'Prevalence', fmt: 'pct1', align: 'num' },
                { col: 'last_seen', label: 'Last seen', fmt: 'ts' },
            ],
        },
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
            if (c.why) {
                out.push({ why: true, col: '_why', label: 'Why', sortable: false });
                continue;
            }
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

    // A volume z_score of +/-VOLUME_FLAT_Z means the history was perfectly flat and
    // the latest bucket differs (parser.VolumeFlatZ); the number itself means nothing.
    VOLUME_FLAT_Z: 1000000,
    FLAT_Z_LABEL: 'flat history, any change',

    _isFlatZ(v) {
        return Math.abs(Number(v)) === this.VOLUME_FLAT_Z;
    },

    _fmtTime(v, style) {
        if (!v || this._isEpochZero(v)) return '<span class="mv-none">&mdash;</span>';
        return `<span title="${_esc(Utils.timestampTitle(v))}">${_esc(Utils.formatTimestamp(v, style || 'friendly'))}</span>`;
    },

    // Severity is measured against the model's own alert threshold, so a colour
    // means "this crossed what you configured" rather than a fixed cut.
    // A row is in play only when it meets every condition its alert has. Colouring
    // from the score alone marked rows that can never fire: a rarity value can sit
    // at 0.95 confidence and still be 97% of its partition, which no percent
    // threshold admits.
    _meetsOtherConditions(row, model) {
        if (!model || model.model_type !== 'rarity') return true;
        const def = model.definition || {};
        const pct = Number(def.alert?.percent_threshold);
        if (pct > 0 && !(Number(row.percent) < pct)) return false;
        const min = Number(def.min_sample);
        if (min > 1 && !(Number(row.model_count) >= min)) return false;
        return true;
    },

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
    _fmtCell(c, row, threshold, inPlay = true) {
        let v = row[c.col];
        if (c.part !== undefined) {
            v = String(v ?? '').split('\x1e')[c.part] ?? '';
        }
        if (v === null || v === undefined || v === '') return '<span class="mv-none">&mdash;</span>';
        switch (c.fmt) {
            case 'int':    return _esc(Number(v).toLocaleString());
            case 'pct1':   return _esc((Number(v) * 100).toFixed(1) + '%');
            case 'pct100': return _esc(Number(v).toFixed(1) + '%');
            case 'score':
                if (c.col === 'z_score' && this._isFlatZ(v)) {
                    return `<span title="Every bucket of history had the same count, so any change is maximal">${_esc(this.FLAT_Z_LABEL)}</span>`;
                }
                return _esc(Number(v).toFixed(3));
            case 'num':   return _esc(Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 }));
            case 'dur':    return _esc(this._fmtDuration(v));
            case 'ts':     return this._fmtTime(v);
            case 'meter': {
                const n = Number(v);
                const pct = Math.max(0, Math.min(100, n * 100));
                const hot = inPlay && isFinite(threshold) && n >= threshold ? ' hot' : '';
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
        // Findings first: the rows the alert would raise, most unusual first. A
        // model with no alert (a tlsh index) has only its rows.
        const view = this._hasFindings(model) ? 'findings' : 'all';
        this.viewer = {
            model, rows: [], total: 0, limit: 50, offset: 0,
            sortCol: this._defaultSort(model, view), sortDir: 'desc',
            search: '', view, backfillWindow: '7d', stats: null, selected: -1,
        };
        this.currentView = 'data';
        this._render();
        await this._loadViewerData();
        this._loadStats();
        if (model.backfill_status === 'running') this._startViewerPoll();
    },

    _hasFindings(model) {
        return !!model && model.model_type !== 'tlsh';
    },

    // A findings page comes back in the rule's order, so no header claims the
    // sort; All rows seeds the type's server-side default so the header shows
    // the order the first page actually comes back in.
    _defaultSort(model, view) {
        return view === 'findings' ? '' : this._viewSpec(model).sortDefault;
    },

    _setView(view) {
        const v = this.viewer;
        if (!v.model || v.view === view) return;
        v.view = view;
        v.offset = 0;
        v.sortCol = this._defaultSort(v.model, view);
        v.sortDir = 'desc';
        this._renderTabs();
        this._loadViewerData();
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
            view: v.view, limit: v.limit, offset: v.offset,
            sort: v.sortCol, order: v.sortDir, search: v.search
        });
        const id = v.model.id, view = v.view;
        try {
            const data = await this._api('GET', `/models/${id}/data?${params}`);
            if (this.viewer.model?.id !== id || this.viewer.view !== view) return;
            v.rows = data?.data || [];
            this._pivotRanges = new Map();
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
            <div id="modelsStats" class="mv-summary"></div>
            <div class="mv-results">
                <div class="mv-toolbar">
                    <div class="mv-tabs" id="modelsViewTabs" role="tablist"></div>
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
        this._renderTabs();
        this._renderStats();
        const onPivot = q => this._pivotChip(q);
        FactChips.bind(document.getElementById('modelsDataTableWrap'), onPivot);
        FactChips.bind(document.getElementById('modelsRowDrawer'), onPivot);

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
    // One line that answers "did this model find anything": what its alert would
    // raise now, what turned up recently, and new values per day. The counts come
    // from /stats, which applies the same rule as the Findings tab.
    _renderStats() {
        const el = document.getElementById('modelsStats');
        if (!el) return;
        const v = this.viewer;
        this._renderTabs();
        if (!v.stats || !v.model) { el.innerHTML = ''; return; }
        const s = this._summary(v.stats, v.model);
        const facts = s.facts.filter(Boolean).map(f => {
            const tone = f.tone ? ` mv-sum-${f.tone}` : '';
            const title = f.title ? ` title="${_esc(f.title)}"` : '';
            const val = f.value !== undefined ? `<b>${_esc(f.value)}</b> ` : '';
            return `<span class="mv-sum-fact${tone}"${title}>${val}${_esc(f.label)}</span>`;
        }).join('<span class="mv-sum-sep" aria-hidden="true">·</span>');
        el.innerHTML = `
<div class="mv-sum-main">
    <div class="mv-sum-facts">${facts}</div>
    ${s.rule ? `<div class="mv-sum-rule">${_esc(s.rule)}</div>` : ''}
</div>
${Array.isArray(s.series) && s.series.length ? this._sparkHTML(s.series, s.seriesLabel) : ''}`;
        // An empty findings page rendered before the rule arrived can now say why.
        if (v.view === 'findings' && !v.rows.length) this._renderDataTable();
    },

    // The strip's facts per type. The lead is what the alert would raise; the
    // rest is context, muted so the lead carries the line.
    _summary(st, m) {
        const def = m.definition || {};
        const n = x => Number(x || 0).toLocaleString();
        const findings = Number(st.findings || 0);
        const lead = { value: n(findings), label: 'would alert', tone: findings > 0 ? 'alert' : '' };
        const rule = st.findings_rule ? 'Alert rule: ' + st.findings_rule : '';
        const newest = v => (v && !this._isEpochZero(v)) ? { label: 'newest seen ' + (Utils.timeAgo(v) || ''), tone: 'muted' } : null;
        switch (m.model_type) {
            case 'rarity':
                return {
                    facts: [
                        st.findings_rule ? lead : { label: 'No alert thresholds set', tone: 'muted' },
                        { value: n(st.new_week), label: 'new pairs this week', title: 'Pairs first seen in the last 7 days' },
                        { value: n(st.total_rows), label: 'pairs', tone: 'muted' },
                        { value: n(st.distinct_partitions), label: (def.partition_key || 'partition') + ' values', tone: 'muted' },
                    ],
                    rule, series: st.series, seriesLabel: 'New pairs per day',
                };
            case 'first_seen': {
                const alertOn = def.alert?.alert_on_new !== false;
                return {
                    facts: [
                        { value: n(findings), label: 'new this week', tone: findings > 0 ? 'alert' : '', title: 'First recorded by the model in the last 7 days' },
                        alertOn ? { value: n(st.new_hour), label: 'would alert now', title: 'First recorded in the last hour, which is what the alert fires on' } : null,
                        { value: n(st.total_entities), label: 'entities', tone: 'muted' },
                        newest(st.newest_seen),
                    ],
                    rule: 'New means first recorded by the model. History seeded by a backfill never counts.',
                    series: st.series, seriesLabel: 'New entities per day',
                };
            }
            case 'tlsh':
                return { facts: [{ value: n(st.total_entities), label: 'digests' }, newest(st.newest_seen)] };
            case 'volume_baseline': {
                const lb = st.latest_bucket && !this._isEpochZero(st.latest_bucket)
                    ? { label: 'latest bucket ' + Utils.formatTimestamp(st.latest_bucket, 'friendly'), tone: 'muted' } : null;
                return { facts: [lead, { value: n(st.total_entities), label: 'entities scored', tone: 'muted' }, lb], rule };
            }
            case 'beacon':
            case 'long_connection': {
                const scored = st.scored_at && !this._isEpochZero(st.scored_at)
                    ? { label: 'scored ' + (Utils.timeAgo(st.scored_at) || ''), tone: 'muted', title: Utils.timestampTitle(st.scored_at) } : null;
                return { facts: [lead, { value: n(st.total_pairs), label: 'pairs scored', tone: 'muted' }, scored], rule };
            }
        }
        return { facts: [] };
    },

    // Thirty days of counts as bars: a burst of new values reads at a glance,
    // and every bar names its day and count on hover.
    _sparkHTML(series, label) {
        const max = series.reduce((m, d) => Math.max(m, Number(d.count || 0)), 0);
        const bw = 4, gap = 2, h = 26;
        const total = series.reduce((t, d) => t + Number(d.count || 0), 0);
        const bars = series.map((d, i) => {
            const c = Number(d.count || 0);
            const bh = c > 0 ? Math.max(3, Math.round(c / max * h)) : 1;
            const cls = c > 0 ? 'mv-spark-bar' : 'mv-spark-bar mv-spark-zero';
            return `<rect class="${cls}" x="${i * (bw + gap)}" y="${h - bh}" width="${bw}" height="${bh}" rx="1"><title>${_esc(d.day)}: ${c.toLocaleString()}</title></rect>`;
        }).join('');
        const w = series.length * (bw + gap) - gap;
        return `<div class="mv-spark">
    <svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="${_esc(label)}: ${total.toLocaleString()} in the last ${series.length} days">${bars}</svg>
    <span class="mv-spark-label">${_esc(label)} &middot; ${series.length}d</span>
</div>`;
    },

    // Findings | All rows. Counts come with the stats, so they appear when it lands.
    _renderTabs() {
        const el = document.getElementById('modelsViewTabs');
        if (!el) return;
        const v = this.viewer;
        if (!this._hasFindings(v.model)) { el.hidden = true; el.innerHTML = ''; return; }
        el.hidden = false;
        const st = v.stats;
        const allKey = { rarity: 'total_rows', first_seen: 'total_entities', volume_baseline: 'total_entities', beacon: 'total_pairs', long_connection: 'total_pairs' }[v.model.model_type];
        const count = (k, hot) => st && st[k] != null
            ? `<span class="mv-tab-n${hot && Number(st[k]) > 0 ? ' hot' : ''}">${_esc(this._fmtNum(st[k]))}</span>` : '';
        el.innerHTML = [['findings', 'Findings', count('findings', true)], ['all', 'All rows', count(allKey)]].map(([k, label, c]) =>
            `<button type="button" role="tab" class="mv-tab${v.view === k ? ' active' : ''}" aria-selected="${v.view === k}" data-view="${k}">${label}${c}</button>`
        ).join('');
        el.querySelectorAll('.mv-tab').forEach(b => b.addEventListener('click', () => this._setView(b.dataset.view)));
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
        // How current the state is. The answer to "why has this model not fired"
        // is often that it has not read the logs yet.
        if (m.state_lag_seconds != null) {
            rows.push(['State lag', this._lagLabel(m.state_lag_seconds) + (m.state_behind ? ' (behind)' : '')]);
        }
        if (mt === 'rarity') {
            rows.push(['Min days seen', def.min_sample || this._defaultMinSample(mt)]);
            rows.push(['Confidence threshold', (def.alert?.confidence_threshold ?? 0.8).toFixed(2)]);
            rows.push(['Max share of days', (def.alert?.percent_threshold ?? 5) + '%']);
        } else if (mt === 'volume_baseline') {
            rows.push(['Bucket', def.time_bucket || 'day']);
            rows.push(['Min history', (def.min_sample || this._defaultMinSample(mt)) + ' buckets']);
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
    <div class="me-sec-label mv-code-head">
        <span>Matches</span>
        <button class="mv-code-copy" id="modelsRailCopy" title="Copy query" aria-label="Copy query">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
            </svg>
        </button>
    </div>
    <pre class="mv-code mv-code-wrap"><code>${window.SyntaxHighlight ? SyntaxHighlight.highlight(bql) : _esc(bql)}</code></pre>
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

        const copy = document.getElementById('modelsRailCopy');
        if (copy) {
            copy.addEventListener('click', () => {
                navigator.clipboard.writeText(bql).then(() => {
                    copy.classList.add('copied');
                    setTimeout(() => copy.classList.remove('copied'), 1200);
                }).catch(() => Toast.error('Copy failed'));
            });
        }
    },

    _bindRailResize() {
        window.App?.bindEditorRail?.(document.getElementById('modelsRailHandle'), {
            body: document.querySelector('.mv-body'),
            cssVar: '--mv-rail-w',
            storageKey: 'bifract-model-viewer-rail-width',
        });
    },

    // ---- Row drawer ----
    // A row is an entity with a history, so it opens rather than dead-ending. It
    // leads with why the row stands out, as fact chips; every stored column,
    // including the ones the table leaves out, sits folded under All columns.
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
        const search = this._rowSearch(row, m);

        // entity_key and entity_val pack the key fields with a record separator,
        // which prints as a control character when dumped raw.
        const packed = new Set(['entity_key', 'entity_val']);
        const skip = new Set(['days', 'fractal_id', ...Utils.HIDDEN_ROW_FIELDS]);
        const fields = Object.keys(row).filter(k => !skip.has(k)).map(k => {
            const val = packed.has(k)
                ? _esc(String(row[k] ?? '').split('\x1e').join(' / '))
                : this._fmtCell({ col: k, fmt: this._drawerFmt(k) }, row, thr, this._meetsOtherConditions(row, m));
            return `<dt>${_esc(k)}</dt><dd>${val}</dd>`;
        }).join('');

        el.innerHTML = `
<div class="mv-drawer-head">
    <div class="mv-drawer-title">${_esc(this._rowTitle(row, m))}</div>
    <button class="mv-drawer-close" id="modelsDrawerClose" title="Close" aria-label="Close">&times;</button>
</div>
<div class="mv-drawer-body">
    <div class="me-sec">
        <div class="me-sec-label">Why</div>
        ${FactChips.render(this._whyChips(row, m)) || '<span class="mv-none">Nothing recorded for this row.</span>'}
    </div>
    ${days.length ? `<div class="me-sec">
        <div class="me-sec-label">Active days (${days.length})</div>
        <div class="mv-chips">${days.map(d => {
            const day = String(d).substring(0, 10);
            return `<button type="button" class="mv-chip mv-chip-day" data-day="${_esc(day)}" title="Search this day in logs">${_esc(day)}</button>`;
        }).join('')}</div>
    </div>` : ''}
    <details class="me-sec mv-all-cols">
        <summary class="me-sec-label">All columns</summary>
        <dl class="mv-kv mv-kv-wide">${fields}</dl>
    </details>
</div>
<div class="mv-drawer-foot">
    ${search
        ? `<button class="btn-primary btn-sm" id="modelsDrawerPivot">${days.length ? `Search these ${days.length} day${days.length === 1 ? '' : 's'} in logs` : 'Search this pair in logs'}</button>`
        : `<span class="mv-none">No time range recorded for this row, so there is nothing to pivot to.</span>`}
</div>`;
        el.hidden = false;
        document.querySelector('.mv-body')?.classList.add('mv-inspecting');
        document.getElementById('modelsDrawerClose').addEventListener('click', () => this._closeRowDrawer());
        document.getElementById('modelsDrawerPivot')?.addEventListener('click', () => this._pivotToSearch(row, m));
        el.querySelectorAll('.mv-chip-day').forEach(chip => {
            chip.addEventListener('click', () => this._pivotToSearch(row, m, chip.dataset.day));
        });
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

    // Score axis names for the editor preview's distribution chart.
    METRIC_LABELS: { confidence: 'Confidence', z_score: 'Anomaly score (|z|)', event_count: 'Event count', beacon_score: 'Beacon score', longconn_score: 'Long-connection score', final_score: 'Score' },

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
                // Just finished: refresh the rows and the summary so backfilled data shows.
                this._loadViewerData();
                this._loadStats();
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
                    : v.view === 'findings'
                        ? this._findingsEmptyHTML()
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

        // The stored column name stays in the tooltip, so a sort or a BQL query
        // written against these results still lines up with what is on screen.
        const headers = cols.map(c => {
            const active = c.sortable && v.sortCol === c.col ? (v.sortDir === 'asc' ? ' sort-asc' : ' sort-desc') : '';
            const cls = `${c.align === 'num' ? 'num ' : ''}${c.why ? 'mv-why ' : ''}${c.sortable ? 'sortable' : ''}${active}`.trim();
            const attr = c.sortable ? ` data-col="${_esc(c.col)}"` : '';
            const tip = c.why ? 'What makes this row stand out' : (c.label !== c.col ? c.col : '');
            const title = tip ? ` title="${_esc(tip)}"` : '';
            return `<th class="${cls}"${attr}${title}><span class="mv-h">${_esc(c.label)}</span>${c.sortable ? '<span class="sort-icon"></span>' : ''}</th>`;
        }).join('');

        const rows = v.rows.map((row, idx) => {
            const inPlay = this._meetsOtherConditions(row, m);
            const sev = scoreCol && inPlay ? this._sevClass(spec.score.abs ? Math.abs(row[scoreCol]) : row[scoreCol], thr) : '';
            const cells = cols.map(c => {
                if (c.why) return `<td class="mv-why">${FactChips.render(this._whyChips(row, m, true), { compact: true })}</td>`;
                const cls = [c.align === 'num' ? 'num' : '', c.col === scoreCol ? 'mv-score' : ''].filter(Boolean).join(' ');
                const title = c.fmt ? '' : ` title="${_esc(this._cellText(c, row))}"`;
                return `<td${cls ? ` class="${cls}"` : ''}${title}>${this._fmtCell(c, row, thr, inPlay)}</td>`;
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
            tr.addEventListener('click', e => {
                // A pivot chip searches; it does not also toggle the drawer.
                if (e.target.closest('.fact-chip-pivot')) return;
                const idx = parseInt(tr.dataset.row, 10);
                if (idx === v.selected) this._closeRowDrawer();
                else this._openRowDrawer(idx);
            });
        });

        this._renderPagination();
    },

    // An empty Findings tab says what the rule is, so "nothing" reads as a
    // result rather than a failure, and offers every row instead.
    _findingsEmptyHTML() {
        const v = this.viewer;
        const st = v.stats;
        const mt = v.model?.model_type;
        const action = { label: 'Browse all rows', onclick: "AnalyticsModels._setView('all')" };
        if (!st) return EmptyState.render({ icon: 'list', title: 'Nothing would alert', action });
        if (mt === 'rarity' && !st.findings_rule) {
            return EmptyState.render({
                icon: 'list', title: 'No alert thresholds set', action,
                detail: 'Without a confidence or share-of-days threshold this model singles nothing out. Set them in Edit to see what would alert.',
            });
        }
        if (mt === 'first_seen') {
            return EmptyState.render({
                icon: 'list', title: 'Nothing new this week', action,
                detail: 'No entity was first recorded by the model in the last 7 days. History seeded by a backfill never counts as new.',
            });
        }
        return EmptyState.render({
            icon: 'list', title: 'Nothing would alert', action,
            detail: _esc(`No row meets the alert rule: ${st.findings_rule}.`),
        });
    },

    // Build a BQL source query string from a model definition (mirrors GenerateSourceQuery in Go).
    // The author's own query wins: rendering from def.filter would drop every filter
    // the structured form cannot hold, so the detail view would describe a wider
    // query than the model runs and a pivot would search rows it never counted.
    _buildSourceQuery(def) {
        if (def && typeof def.source_bql === 'string' && def.source_bql.trim()) return def.source_bql.trim();
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

    // The search a row pivots to: the model's source query narrowed to the row's
    // keys, over the span it was seen. range narrows it: a 'YYYY-MM-DD' day, or
    // { from, to } ISO bounds. Null when the row records no time to search.
    _rowSearch(row, model, range) {
        const def = model.definition || {};
        const mt = model.model_type || 'rarity';
        const esc = s => `"${String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
        const rowFilters = [];
        let from = '', to = '';
        if (mt === 'beacon' || mt === 'long_connection') {
            const n = def.network || {};
            rowFilters.push(`| ${n.src_field || 'src_ip'}=${esc(row.src_ip)}`);
            rowFilters.push(`| ${n.dst_field || 'dst_ip'}=${esc(row.dst_ip)}`);
            rowFilters.push(`| ${n.port_field || 'dst_port'}=${esc(row.dst_port)}`);
            if (!this._isEpochZero(row.first_seen) && !this._isEpochZero(row.last_seen)) {
                from = this._isoSec(row.first_seen);
                to = this._isoSec(row.last_seen, 1);
            }
        } else {
            if (mt === 'rarity') {
                if (def.partition_key && row.partition_val != null) rowFilters.push(`| ${def.partition_key}=${esc(row.partition_val)}`);
                if (def.value_key && row.value_val != null)         rowFilters.push(`| ${def.value_key}=${esc(row.value_val)}`);
            } else {
                const entityRaw = { first_seen: row.entity_key, tlsh: row.digest }[mt] ?? row.entity_val;
                const fields = Array.isArray(def.key_fields) ? def.key_fields : [];
                if (fields.length && entityRaw != null) {
                    const parts = String(entityRaw).split('\x1e');
                    fields.forEach((field, i) => rowFilters.push(`| ${field}=${esc(parts[i] ?? '')}`));
                }
            }
            const days = (Array.isArray(row.days) ? row.days : []).map(d => String(d).substring(0, 10)).sort();
            if (days.length) {
                from = days[0] + 'T00:00:00Z';
                to = days[days.length - 1] + 'T23:59:59Z';
            }
        }
        if (typeof range === 'string' && range) {
            from = range + 'T00:00:00Z';
            to = range + 'T23:59:59Z';
        } else if (range && range.from && range.to) {
            ({ from, to } = range);
        }
        if (!from || !to) return null;

        let bql = this._buildSourceQuery(def);
        if (rowFilters.length) {
            if (!bql) rowFilters[0] = rowFilters[0].replace(/^\|\s+/, '');
            bql = (bql ? bql + '\n' : '') + rowFilters.join('\n');
        }
        return { bql, from, to };
    },

    // A stored timestamp as an ISO bound at second precision, plus pad seconds so
    // an end bound keeps the event at that second.
    _isoSec(v, pad = 0) {
        const ms = window.TZ ? TZ.toEpoch(v) : Date.parse(v);
        if (!Number.isFinite(ms)) return '';
        return new Date(Math.floor(ms / 1000) * 1000 + pad * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
    },

    // day, when given, narrows the search to that one active day rather than the
    // whole span the row was seen over.
    _pivotToSearch(row, model, day) {
        const s = this._rowSearch(row, model, day);
        if (!s) { Toast.error('No time range recorded for this row yet.'); return; }
        this._runSearch(s);
    },

    // A pivot chip carries its query; the range it searches was registered with it
    // when the chip was built.
    _pivotChip(query) {
        const s = this._pivotRanges?.get(query);
        if (s) this._runSearch(s);
    },

    _runSearch({ bql, from, to }) {
        if (window.App) App.showFractalViewTab('search');

        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = bql;
            if (window.SyntaxHighlight) SyntaxHighlight.updateHighlight('queryInput', 'queryHighlight');
        }

        if (window.TimePicker) {
            TimePicker.setState({ type: 'custom', customStart: from, customEnd: to }, true);
        }

        if (window.QueryExecutor) {
            setTimeout(() => QueryExecutor.execute(), 50);
        }
    },

    // ---- Why: fact chips ----
    // Each chip states one fact against the baseline it was measured on. compact
    // keeps the one to three that say most for the table; the drawer gets all.
    // The chip that names the row's activity pivots to search for it.
    _whyChips(row, model, compact = false) {
        const def = model.definition || {};
        const mt = model.model_type;
        const int = x => Math.round(Number(x) || 0).toLocaleString();
        const fix = (x, d = 2) => Number(x || 0).toFixed(d);
        const days = Array.isArray(row.days) ? [...row.days].map(d => String(d).substring(0, 10)).sort() : [];
        const pivot = range => {
            const s = this._rowSearch(row, model, range);
            if (!s) return undefined;
            (this._pivotRanges ||= new Map()).set(s.bql, s);
            return s.bql;
        };
        const pick = (all, idx) => compact ? idx.map(i => all[i]).filter(Boolean) : all.filter(Boolean);

        if (mt === 'rarity') {
            const pctThr = Number(def.alert?.percent_threshold) || 0;
            const confThr = Number(def.alert?.confidence_threshold) || 0;
            const pct = Number(row.percent);
            const conf = Number(row.confidence);
            return pick([
                { label: 'Seen on', value: `${FactChips.of(row.model_count, row.model_total)} days`,
                  tone: pctThr > 0 && pct < pctThr ? 'alert' : '',
                  title: `${fix(pct, 1)}% of the days ${row.partition_val} was seen. Click to search them.`, query: pivot() },
                { label: 'Partition confidence', value: fix(conf),
                  tone: confThr > 0 && !(conf > confThr) ? 'muted' : '',
                  title: 'How rarely this partition produces a new value (Good-Turing coverage). Low means new values are routine there.' },
                days.length ? { label: 'First seen', value: FactChips.ago(days[0]) || days[0], title: days[0] } : null,
            ], [0, 2]);
        }
        if (mt === 'volume_baseline') {
            const z = Number(row.z_score);
            const zThr = Number(def.alert?.z_threshold) || 3.5;
            const unit = def.time_bucket === 'hour' ? 'hour' : 'day';
            const flat = this._isFlatZ(z);
            let latestRange;
            if (row.latest_bucket && !this._isEpochZero(row.latest_bucket)) {
                latestRange = { from: this._isoSec(row.latest_bucket), to: this._isoSec(row.latest_bucket, (unit === 'hour' ? 3600 : 86400) - 1) };
            }
            return pick([
                flat ? { value: 'Flat history', tone: 'warn', title: 'Every bucket of history had the same count, so any change is maximal' } : null,
                { label: 'Latest', value: int(row.latest_count), tone: z > zThr ? 'alert' : '',
                  title: `Events in the latest complete ${unit}. Click to search it.`, query: pivot(latestRange) },
                { label: 'Typical', value: Number(row.baseline_median || 0).toLocaleString(undefined, { maximumFractionDigits: 2 }), title: `Median events per ${unit} over its history` },
                { label: 'History', value: `${int(row.n_buckets)} ${unit}s` },
                flat ? null : { label: 'z', value: fix(z), tone: z > zThr ? 'alert' : 'muted', title: `Modified z-score; the alert fires above ${zThr}` },
            ], flat ? [0, 1] : [1, 2]);
        }
        if (mt === 'first_seen' || mt === 'tlsh') {
            const rec = row.recorded_at;
            const recMs = rec && !this._isEpochZero(rec) ? (window.TZ ? TZ.toEpoch(rec) : Date.parse(rec)) : NaN;
            const isNew = Number.isFinite(recMs) && Date.now() - recMs < 7 * 86400000;
            return pick([
                isNew ? { value: 'New to model', tone: 'alert', title: `First recorded by the model ${FactChips.ago(rec)}` } : null,
                { label: 'First seen', value: FactChips.ago(row.first_seen) || '—', title: Utils.timestampTitle(row.first_seen) + '. Click to search its days.', query: pivot() },
                { label: 'Events', value: int(row.event_count) },
                days.length ? { label: 'Active on', value: `${days.length} day${days.length === 1 ? '' : 's'}` } : null,
                { label: 'Last seen', value: FactChips.ago(row.last_seen), title: Utils.timestampTitle(row.last_seen) },
            ], [0, 1]);
        }
        if (mt === 'beacon' || mt === 'long_connection') {
            const final = Number(row.final_score);
            const thr = Number(this._viewSpec(model).score.threshold(def));
            // The stored share is rounded, so the network size cannot be recovered
            // from it exactly; the count and the share are stated as stored.
            const srcs = Number(row.prevalence_total);
            const prevalence = srcs > 0 ? {
                label: 'Sources', value: `${int(srcs)} (${(Number(row.prevalence) * 100).toFixed(1)}%)`,
                tone: srcs <= 1 ? 'warn' : '',
                title: 'Distinct sources that talked to this destination in the window, and their share of all sources',
            } : null;
            const conns = { label: 'Connections', value: int(row.conn_count), title: 'Click to search this pair', query: pivot() };
            const seen = !this._isEpochZero(row.last_seen) ? { label: 'Last seen', value: FactChips.ago(row.last_seen), title: Utils.timestampTitle(row.last_seen) } : null;
            if (mt === 'long_connection') {
                return pick([
                    { label: 'Duration', value: this._fmtDuration(row.total_duration), tone: final > thr ? 'alert' : '' },
                    prevalence, conns, seen,
                ], [1, 3]);
            }
            const subs = [
                ['Timing', 'ts_score', 'Consistency of the gaps between connections'],
                ['Size', 'ds_score', 'Consistency of the bytes per connection'],
                ['Persistence', 'dur_score', 'How much of the window the pair spans and how it recurs across the day'],
                ['Histogram', 'hist_score', 'How evenly connections spread across the hours of the day'],
            ].map(([label, k, title]) => ({ label, value: fix(row[k]), tone: Number(row[k]) >= 0.8 ? '' : 'muted', title, v: Number(row[k]) || 0 }));
            if (compact) {
                // The table already shows the score, regularity and connections;
                // the chips add who else talks to it and what drove the regularity.
                const top = subs.filter(s => s.v > 0).sort((a, b) => b.v - a.v).slice(0, 2);
                return [prevalence, ...top].filter(Boolean);
            }
            return [
                { label: 'Regularity', value: fix(row.regularity_score), tone: final > thr ? 'alert' : '', title: 'Combined timing, size, persistence and histogram regularity' },
                prevalence, conns, ...subs, seen,
            ].filter(Boolean);
        }
        return [];
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
        { id: 'tlsh', label: 'TLSH Index', desc: 'Indexes the distinct fuzzy-hash digests in a field so tlsh() can match similar files. Not a detection on its own.' },
        { id: 'beacon', label: 'Beacon', desc: 'Finds regular, automated check-ins (C2 beaconing) in network connection logs.' },
        { id: 'long_connection', label: 'Long Connection', desc: 'Surfaces unusually long-lived sessions (tunnels, exfil, persistent C2) by total duration.' },
    ],

    TYPE_ICONS: {
        rarity: '<path d="M6 3h12l3 6-9 12L3 9z"/><path d="M3 9h18"/>',
        first_seen: '<circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15.5 14"/>',
        volume_baseline: '<polyline points="3 13 7 13 10 5 14 19 17 13 21 13"/>',
        tlsh: '<circle cx="9" cy="12" r="6"/><circle cx="15" cy="12" r="6"/>',
        beacon: '<circle cx="12" cy="12" r="2"/><path d="M7.8 7.8a6 6 0 0 0 0 8.4"/><path d="M16.2 16.2a6 6 0 0 0 0-8.4"/><path d="M4.9 4.9a10 10 0 0 0 0 14.2"/><path d="M19.1 19.1a10 10 0 0 0 0-14.2"/>',
        long_connection: '<path d="M10.5 13.5a4.5 4.5 0 0 0 6.6.4l2.6-2.6a4.5 4.5 0 0 0-6.4-6.4l-1.5 1.5"/><path d="M13.5 10.5a4.5 4.5 0 0 0-6.6-.4l-2.6 2.6a4.5 4.5 0 0 0 6.4 6.4l1.5-1.5"/>',
    },

    BASE_FIELDS: ['norm_log', 'contents', 'commandline', 'target_file', 'src_ip', 'dst_ip', 'user', 'image', 'parent_process', 'process_name'],

    // Starter models over the normalized schema (bifract_category plus the
    // provenance fields, see docs/features/provenance-graph.md). Each fills the
    // whole editor; the author tunes from there.
    TEMPLATES: [
        {
            id: 'new_programs', title: 'New programs per host',
            blurb: 'A binary running on a host for the first time.',
            name: 'new_programs_per_host', type: 'first_seen',
            description: 'Alerts when a host runs an image it has never run before.',
            query: 'bifract_category=process_creation',
            shape: { keyFields: ['computer_name', 'image'] },
            alert: { alert_on_new: true },
        },
        {
            id: 'office_child', title: 'Rare child of Office apps',
            blurb: 'Office spawning a process it rarely spawns.',
            name: 'rare_office_child', type: 'rarity',
            description: 'Scores how rarely each Office application launches a given child process.',
            query: 'bifract_category=process_creation\n| parent_image=$winword.exe,excel.exe,powerpnt.exe,outlook.exe,onenote.exe,msaccess.exe,mspub.exe',
            shape: { partitionKey: 'parent_image', valueKey: 'image', minSample: 1 },
            alert: { confidence_threshold: 0.9, percent_threshold: 10 },
        },
        {
            id: 'outbound_ports', title: 'New outbound ports per host',
            blurb: 'A host connecting on a port it rarely uses.',
            name: 'rare_ports_per_host', type: 'rarity',
            description: 'Scores how rarely each host connects to a destination port.',
            query: 'bifract_category=network_connect',
            shape: { partitionKey: 'computer_name', valueKey: 'dst_port', minSample: 1 },
            alert: { confidence_threshold: 0.9, percent_threshold: 10 },
        },
        {
            id: 'net_volume', title: 'Network volume spike per host',
            blurb: 'Hourly connection count far above the host\'s norm.',
            name: 'network_volume_per_host', type: 'volume_baseline',
            description: 'Flags hosts whose hourly connection count spikes against their own history.',
            query: 'bifract_category=network_connect',
            shape: { keyFields: ['computer_name'], timeBucket: 'hour', minSample: 24 },
            alert: { z_threshold: 3.5 },
        },
        {
            id: 'beaconing', title: 'Beaconing',
            blurb: 'Regular, automated check-ins to one destination.',
            name: 'beaconing', type: 'beacon',
            description: 'Finds source, destination and port pairs that connect on a regular interval.',
            query: 'bifract_category=network_connect',
            shape: { window: '1d', network: { src_field: 'src_ip', dst_field: 'dst_ip', port_field: 'dst_port', bytes_field: 'orig_bytes' } },
            alert: { beacon_threshold: 0.8 },
        },
    ],

    _startEditor() {
        window.App?.pushSubPath('new');
        this.editor = {
            editId: null,
            modelType: 'rarity',
            query: '',
            parsed: { source_bql: '', filter_complete: true, computed_fields: [], filter: [], extractions: [], candidate_fields: [], errors: [], warnings: [] },
            partitionKey: '',
            valueKey: '',
            keyFields: [''],
            minSample: 1,
            timeBucket: 'day',
            network: this._networkFromDef({}),
            window: '1d',
            alertMode: 'paused',
            alertConfig: { severity: 'medium', action_ids: [], confidence_threshold: 0.9, percent_threshold: 10.0, alert_on_new: true, z_threshold: 3.5, beacon_threshold: 0.8, longconn_threshold: 0.5 },
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
            previewWindow: '7d',    // score preview preset, or 'custom' for previewStart/End
            previewStart: '',       // custom preview range, UTC ISO8601
            previewEnd: '',
            preview: null,          // last PreviewResult
            previewCfg: null,       // alertConfig the preview was scored with
            showGallery: true,      // starter templates until one is picked or the user starts blank
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
        const previews = [['1d', 'Last 1 Day'], ['7d', 'Last 7 Days'], ['30d', 'Last 30 Days'], ['custom', 'Custom range']];
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
                    <select id="modelPreviewWindow" class="time-range-select" title="Window the score preview scans" ${scores ? '' : 'hidden'}>${opts(previews, e.previewWindow)}</select>
                    <div id="modelPreviewRange" class="me-preview-range" ${scores && e.previewWindow === 'custom' ? '' : 'hidden'}>
                        <input type="text" id="modelPreviewStart" class="me-range-input" placeholder="YYYY-MM-DD HH:MM" spellcheck="false" autocomplete="off" value="${_esc(e.previewStart ? TZ.formatInput(e.previewStart) : '')}" aria-label="Preview start">
                        <span class="me-range-sep">to</span>
                        <input type="text" id="modelPreviewEnd" class="me-range-input" placeholder="YYYY-MM-DD HH:MM" spellcheck="false" autocomplete="off" value="${_esc(e.previewEnd ? TZ.formatInput(e.previewEnd) : '')}" aria-label="Preview end">
                        <button type="button" class="btn-secondary me-range-apply" id="modelPreviewApply">Apply</button>
                    </div>
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
                        <div id="modelQueryResults" class="results-container">${!e.editId && e.showGallery ? this._templateGalleryHTML() : this._benchEmpty()}</div>
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
                    <p class="me-hint" id="modelAlertHint"${e.modelType === 'tlsh' ? ' style="display:none"' : ''}>A paused alert is created with these thresholds${e.editId ? '' : ' on save'}. Enable it and set actions, throttling and severity from the Alerts page.</p>
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
        this._bindPreviewRange();
        this._bindTemplateGallery();
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

    // ---- Starter templates (new models only) ----
    _templateGalleryHTML() {
        const keys = t => {
            const sh = t.shape || {};
            if (sh.partitionKey) return `${sh.partitionKey} → ${sh.valueKey}`;
            if (sh.keyFields) return sh.keyFields.join(', ') + (sh.timeBucket ? ` per ${sh.timeBucket}` : '');
            if (sh.network) return `${sh.network.src_field} → ${sh.network.dst_field}:${sh.network.port_field}`;
            return '';
        };
        const cards = this.TEMPLATES.map(t => `
<button type="button" class="me-tpl-card" data-template="${_esc(t.id)}">
    <span class="me-tpl-head">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${this.TYPE_ICONS[t.type] || ''}</svg>
        <span class="me-tpl-title">${_esc(t.title)}</span>
    </span>
    <span class="me-tpl-blurb">${_esc(t.blurb)}</span>
    <span class="me-tpl-meta">${_esc(this._typeLabel(t.type))} · <code>${_esc(keys(t))}</code></span>
</button>`).join('');
        return `
<div class="me-tpl-gallery" id="modelTemplateGallery">
    <div class="me-tpl-intro">
        <div class="me-tpl-heading">Start from a template</div>
        <div class="me-tpl-sub">Each fills the query, shape and thresholds over the normalized schema. Tune from there.</div>
    </div>
    <div class="me-tpl-grid">${cards}</div>
    <button type="button" class="btn-secondary me-tpl-blank" id="modelStartBlank">Start blank</button>
</div>`;
    },

    _bindTemplateGallery() {
        const gallery = document.getElementById('modelTemplateGallery');
        if (!gallery) return;
        gallery.querySelectorAll('.me-tpl-card').forEach(card => {
            card.addEventListener('click', () => this._applyTemplate(card.dataset.template));
        });
        document.getElementById('modelStartBlank')?.addEventListener('click', () => {
            this.editor.showGallery = false;
            const el = document.getElementById('modelQueryResults');
            if (el) el.innerHTML = this._benchEmpty();
            document.getElementById('modelQueryInput')?.focus();
        });
    },

    // Fills the editor from a template, then scores it: the preview is what tells
    // the author whether the template fits their data.
    _applyTemplate(id) {
        const t = this.TEMPLATES.find(x => x.id === id);
        if (!t) return;
        const e = this.editor;
        const sh = t.shape || {};
        e.showGallery = false;
        e.modelType = t.type;
        e.name = t.name;
        e.description = t.description;
        e.query = t.query;
        e.partitionKey = sh.partitionKey || '';
        e.valueKey = sh.valueKey || '';
        e.keyFields = sh.keyFields ? [...sh.keyFields] : [''];
        e.minSample = sh.minSample || this._defaultMinSample(t.type);
        e.timeBucket = sh.timeBucket || 'day';
        e.window = sh.window || '1d';
        e.network = this._networkFromDef({ network: sh.network || {} });
        Object.assign(e.alertConfig, t.alert || {});
        e.resultMode = 'scores';
        e.dirty = true;
        this._render();
        // The Results tab gets the matching logs too, which also feeds the field
        // suggestions; it renders into its hidden pane.
        this._runQuery();
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
                const prevType = e.modelType;
                e.modelType = card.dataset.type;
                // The field means a different thing for the new type, so carrying
                // the old number over would carry the old meaning with it.
                if (e.minSample === this._defaultMinSample(prevType)) {
                    e.minSample = this._defaultMinSample(e.modelType);
                }
                // A tlsh model takes exactly one digest field, and its shape editor
                // renders only the first row with no remove button. Extra fields
                // carried over from another type would be invisible here but still
                // submitted, leaving the form permanently unsavable.
                if (e.modelType === 'tlsh' && e.keyFields.length > 1) {
                    e.keyFields = [e.keyFields.find(Boolean) || ''];
                }
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
        // Columns the source query computes come first: they are the reason the
        // query has them, and the model's scan projects them like any other.
        (e.parsed.computed_fields || []).forEach(add);
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
    ${this._fieldInput('shapePartKey', e.partitionKey, 'e.g. computer_name')}
</div>
<div class="field-group" style="margin-top:10px">
    <label>Value Key (rarity of what?)</label>
    ${this._fieldInput('shapeValKey', e.valueKey, 'e.g. dst_port')}
</div>
<div class="field-group" style="margin-top:10px">
    <label>Min days seen</label>
    <input type="number" id="shapeMinSample" class="model-num-input" value="${e.minSample}" min="1">
    <p class="config-hint">Keep at 1: first sightings are what this model finds.</p>
</div>
<p class="config-hint">Example: Partition=<em>computer_name</em>, Value=<em>dst_port</em> scores how unusual a port is for that host, counted in days.</p>`;
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
<p class="config-hint">Scores each entity's last complete ${e.timeBucket === 'hour' ? 'hour' : 'day'} against its own history (modified z-score). Empty buckets count as zero.</p>`;
        }
        if (e.modelType === 'tlsh') {
            return `
<div class="field-group">
    <label>Digest Field</label>
    <div id="keyFieldsList">
<div class="key-field-row" data-idx="0">
    ${this._fieldInput('keyField0', e.keyFields[0] || '', 'e.g. tlsh')}
</div></div>
</div>
<p class="config-hint">Indexes the distinct TLSH digests in this field so <em>tlsh()</em> can match against them. Only well-formed digests are indexed, so absent and truncated values never enter the index. Seed it with a backfill to cover existing history.</p>`;
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
            document.getElementById('shapeMinSample')?.addEventListener('change', ev => { e.minSample = parseInt(ev.target.value) || 1; this._schedulePreview(); });
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
            sel?.addEventListener('input', ev => { e.keyFields[i] = ev.target.value.trim(); this._schedulePreview(); });
            // A tlsh model has exactly one digest field, so its row carries no
            // remove button. Every other type renders one.
            row.querySelector('.btn-remove-row')?.addEventListener('click', () => {
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
        if (mt === 'tlsh') {
            return `<p class="config-hint">A TLSH index raises no alerts of its own. It records which digests exist; the detection is <em>tlsh()</em> in a query or alert, where the distance threshold decides what counts as a match.</p>`;
        }
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
            <label>Max % of days</label>
            <input type="number" id="alertPercent" class="model-num-input" value="${c.percent_threshold}" min="0.1" max="100" step="0.5">
        </div>
    </div>
    <p class="config-hint">Flags values seen on under this share of their partition's days, where new values are rare. Learns for more than 100 ÷ share days.</p>`;
        } else if (mt === 'volume_baseline') {
            typeFields = `
    <div class="field-group" style="margin-top:10px">
        <label>Z-score threshold</label>
        <input type="number" id="alertZThreshold" class="model-num-input" value="${c.z_threshold}" min="0" step="0.5">
        <p class="config-hint">Flags a spike above this modified z-score. 3.5 is the standard cutoff.</p>
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
        const hint = document.getElementById('modelAlertHint');
        if (hint) hint.style.display = this.editor.modelType === 'tlsh' ? 'none' : '';
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

    // Thresholds never need a new scan: the Scores tab recounts from the
    // distribution it already has (_onThresholdChange).
    _bindAlertConfigEvents() {
        const c = this.editor.alertConfig;
        const num = (id, key) => document.getElementById(id)?.addEventListener('input', ev => {
            const v = parseFloat(ev.target.value);
            if (!Number.isFinite(v)) return;
            c[key] = v;
            this._onThresholdChange(key);
        });
        num('alertConfidence', 'confidence_threshold');
        num('alertPercent', 'percent_threshold');
        num('alertZThreshold', 'z_threshold');
        num('alertBeaconThreshold', 'beacon_threshold');
        num('alertLongConnThreshold', 'longconn_threshold');
        document.getElementById('alertOnNew')?.addEventListener('change', ev => { c.alert_on_new = ev.target.checked; this._onThresholdChange('alert_on_new'); });
    },

    // ---- Translation feedback strip (left panel) ----
    _renderTranslation() {
        const el = document.getElementById('modelTranslation');
        if (!el) return;
        const p = this.editor.parsed;

        // Nothing parsed yet (or an empty query): keep the strip hidden rather
        // than showing a noisy "all logs / none" placeholder.
        const hasContent = (p.filter || []).length || (p.extractions || []).length ||
            (p.errors || []).length || (p.warnings || []).length || p.filter_complete === false;
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

        // Chips only when they describe the whole query. A query with an OR, a
        // group or a command the structured form has no shape for would otherwise
        // read as a narrower filter than the one the model actually applies.
        let filterBody;
        if (p.filter_complete === false) {
            filterBody = '<span class="model-trans-muted">the source query as written</span>';
        } else {
            filterBody = (p.filter || []).map(f =>
                `<span class="model-chip"><code>${_esc(f.field)}</code> ${_esc(f.op)} <code>${_esc(f.value)}</code></span>`
            ).join('') || '<span class="model-trans-muted">all logs</span>';
        }
        const filterRow = `<div class="model-trans-row"><span class="model-trans-label">Filters</span>${filterBody}</div>`;

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
                    source_bql: d.source_bql || '',
                    filter_complete: d.filter_complete !== false,
                    computed_fields: d.computed_fields || [],
                    filter: d.filter || [],
                    extractions: d.extractions || [],
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
            (queryData.results || []).slice(0, 50).forEach(r => Utils.visibleFields(Object.keys(r || {})).forEach(add));
        }
        return fields;
    },

    // ---- Save ----
    async _saveModel() {
        const e = this.editor;
        e.query = (document.getElementById('modelQueryInput')?.value || '').trim();

        if (!e.name.trim()) { Toast.warning('Model name is required'); return; }

        // Re-parse on save for an authoritative definition + validation.
        let parsed = null;
        if (e.query) {
            const parseData = await this._api('POST', '/models/parse-query', { query: e.query, model_type: e.modelType }).catch(() => null);
            const d = parseData?.data;
            if (!d) { Toast.error('Could not validate the source query'); return; }
            if (d.errors && d.errors.length) {
                e.parsed = { source_bql: d.source_bql || '', filter_complete: d.filter_complete !== false, computed_fields: d.computed_fields || [], filter: d.filter || [], extractions: d.extractions || [], candidate_fields: d.candidate_fields || [], errors: d.errors, warnings: d.warnings || [] };
                this._renderTranslation();
                Toast.error(d.errors[0]);
                return;
            }
            parsed = d;
            e.parsed.candidate_fields = d.candidate_fields || [];
        }

        const shapeErr = this._validateShape();
        if (shapeErr) { Toast.warning(shapeErr); return; }
        const def = this._composeDefinition(parsed);

        const btn = document.getElementById('modelEditorSave');
        if (btn) btn.disabled = true;
        // A TLSH index raises no alerts, so it is saved with no alert mode rather
        // than the editor's default of "paused".
        const alertMode = e.modelType === 'tlsh' ? 'none' : e.alertMode;

        try {
            if (e.editId) {
                await this._api('PUT', `/models/${e.editId}`, {
                    name: e.name.trim(), description: e.description.trim(), definition: def, alert_mode: alertMode,
                });
                Toast.success('Model updated');
            } else {
                await this._api('POST', '/models', {
                    name: e.name.trim(), description: e.description.trim(), model_type: e.modelType, definition: def, alert_mode: alertMode,
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
        if (e.modelType === 'tlsh' && e.keyFields.filter(Boolean).length !== 1) {
            return 'Set the log field that holds the TLSH digest';
        }
        if ((e.modelType === 'first_seen' || e.modelType === 'volume_baseline') && !e.keyFields.filter(Boolean).length) {
            return e.modelType === 'volume_baseline' ? 'Add at least one entity field' : 'Add at least one key field';
        }
        if (this._isNetworkType(e.modelType) && (!e.network.src_field || !e.network.dst_field)) {
            return 'Set a source and destination field';
        }
        return null;
    },

    // Builds the ModelDefinition payload from the current shape + alert config.
    // parsed is the parse-query response: source_bql is what the model compiles,
    // filter/extractions are the structured half the editor displays and the
    // extraction columns the model renders itself.
    _composeDefinition(parsed) {
        const e = this.editor;
        const p = parsed || {};
        const def = { source_bql: p.source_bql || '', filter: p.filter || [], extractions: p.extractions || [] };
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
    // The preview's window: a preset, or a custom range typed as wall clock in
    // the display zone (the same "YYYY-MM-DD HH:MM" the time pickers take).
    _bindPreviewRange() {
        const e = this.editor;
        const sel = document.getElementById('modelPreviewWindow');
        const box = document.getElementById('modelPreviewRange');
        if (!sel || !box) return;
        sel.addEventListener('change', ev => {
            e.previewWindow = ev.target.value;
            box.hidden = e.previewWindow !== 'custom';
            if (e.previewWindow === 'custom') {
                // Seed with the last 7 days so there is something to edit.
                if (!e.previewStart || !e.previewEnd) {
                    e.previewEnd = new Date().toISOString();
                    e.previewStart = new Date(Date.now() - 7 * 86400000).toISOString();
                    document.getElementById('modelPreviewStart').value = TZ.formatInput(e.previewStart);
                    document.getElementById('modelPreviewEnd').value = TZ.formatInput(e.previewEnd);
                }
                document.getElementById('modelPreviewStart')?.focus();
            }
            this._runScorePreview();
        });
        const apply = () => {
            const parse = id => {
                const ms = TZ.parseWallClock(String(document.getElementById(id)?.value || '').trim());
                return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
            };
            const start = parse('modelPreviewStart'), end = parse('modelPreviewEnd');
            if (!start || !end) { Toast.warning('Enter the range as YYYY-MM-DD HH:MM'); return; }
            if (Date.parse(end) <= Date.parse(start)) { Toast.warning('The range must end after it starts'); return; }
            if (Date.parse(end) - Date.parse(start) > 90 * 86400000) { Toast.warning('A preview range is limited to 90 days'); return; }
            e.previewStart = start;
            e.previewEnd = end;
            this._runScorePreview();
        };
        document.getElementById('modelPreviewApply')?.addEventListener('click', apply);
        box.querySelectorAll('.me-range-input').forEach(i => i.addEventListener('keydown', ev => {
            if (ev.key === 'Enter') { ev.preventDefault(); apply(); }
        }));
    },

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
        show('modelPreviewRange', scores && e.previewWindow === 'custom');
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
        let parsed = null;
        if (e.query) {
            const pd = await this._api('POST', '/models/parse-query', { query: e.query, model_type: e.modelType }).catch(() => null);
            if (seq !== this._previewSeq) return;
            const d = pd?.data;
            if (d?.errors?.length) {
                panel.innerHTML = `<div class="query-error"><p>${_esc(d.errors[0])}</p></div>`;
                return;
            }
            parsed = d;
        }

        const def = this._composeDefinition(parsed);
        const body = { model_type: e.modelType, definition: def, window: e.previewWindow };
        if (e.previewWindow === 'custom') {
            if (!e.previewStart || !e.previewEnd) {
                panel.innerHTML = this._benchEmpty('Pick a range', 'Enter a start and end (up to 90 days) to score the model over a past window.');
                return;
            }
            body.window = '';
            body.start = e.previewStart;
            body.end = e.previewEnd;
        }
        try {
            const data = await this._api('POST', '/models/preview', body);
            if (seq !== this._previewSeq) return;
            e.preview = data?.data || null;
            e.previewCfg = { ...e.alertConfig };
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
                [num(s.new_recent), 'new (last 24h of range)'],
            ];
        } else if (p.model_type === 'volume_baseline') {
            chips = [
                [num(s.entities_scored), 'entities scored'],
                [this._isFlatZ(s.max_z) ? this.FLAT_Z_LABEL : Number(s.max_z || 0).toFixed(2), 'max |z|'],
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
        const chipsHTML = chips.map(([v, l]) => `<div class="score-stat-chip"><span class="score-stat-val">${_esc(String(v))}</span><span class="score-stat-label">${_esc(l)}</span></div>`).join('');

        // Volume baseline needs several complete buckets to score; surface why it
        // may be empty over a short window rather than showing a blank chart.
        let hint = '';
        const range = this._previewRangeLabel(p);
        if (p.model_type === 'volume_baseline' && scoredTotal === 0) {
            hint = `<div class="score-preview-hint">No entity has enough buckets of history in ${_esc(range)} to establish a baseline. Try a longer window or the per-hour bucket.</div>`;
        } else if (scoredTotal === 0) {
            hint = `<div class="score-preview-hint">No matching results ${_esc(range)}. If the data is older, pick a custom range.</div>`;
        }

        const countEl = document.getElementById('modelResultsCount');
        if (countEl) countEl.textContent = `${this._fmtNum(scoredTotal)} scored`;

        const topHTML = this._previewTopTableHTML(p.top_columns || [], p.top || []);

        panel.innerHTML = `
<div class="score-preview">
    <div class="score-preview-head">
        <div class="score-preview-stats">${chipsHTML}</div>
        <div id="modelFlagBadge"></div>
    </div>
    ${this._scoreThresholdsHTML(p.model_type)}
    ${hint}
    <div id="modelScoreHistogram"></div>
    ${topHTML}
</div>`;
        this._bindScoreThresholds();
        this._renderFlagCount();
    },

    // "over the last 7d", or "between Sep 1 10:00 and Sep 8 10:00" for a custom range.
    _previewRangeLabel(p) {
        if (p.window) return `in the last ${p.window}`;
        if (p.start && p.end) return `between ${TZ.format(p.start, 'friendly')} and ${TZ.format(p.end, 'friendly')}`;
        return 'in this window';
    },

    // The would-flag count, the tab chip and the histogram marker for the current
    // thresholds. Recounted from the preview's score distribution, so moving a
    // threshold costs no scan; the server's own count stands when the thresholds
    // are the ones it scored with.
    _renderFlagCount() {
        const e = this.editor;
        const p = e.preview;
        if (!p) return;
        const counted = this._countFlags(p);
        const asScored = this._thresholdsAsScored(p.model_type);
        const flags = asScored || !counted ? Number(p.would_flag || 0) : counted.n;
        const approx = !asScored && counted && !counted.exact;
        // The criterion wording comes from the server, where the rule lives; once a
        // threshold moves, the controls above say what it is instead.
        const basis = asScored ? (p.flag_basis || '') : (counted ? 'at the thresholds above' : 'rescoring...');

        const chip = document.getElementById('modelFlagChip');
        if (chip) {
            chip.textContent = this._fmtNum(flags);
            chip.classList.toggle('warn', flags > 0);
            chip.hidden = false;
        }
        const badge = document.getElementById('modelFlagBadge');
        if (badge) {
            badge.innerHTML = `<div class="score-flag-badge ${flags > 0 ? 'has-flags' : ''}">
    <span class="score-flag-count">${approx ? '~' : ''}${this._fmtNum(flags)}</span>
    <span class="score-flag-text">would flag</span>
    <span class="score-flag-basis">${_esc(basis)}</span>
</div>`;
        }
        const hist = document.getElementById('modelScoreHistogram');
        if (hist) {
            const c = e.alertConfig;
            const fracKey = { rarity: 'confidence_threshold', beacon: 'beacon_threshold', long_connection: 'longconn_threshold' }[p.model_type];
            let frac = fracKey ? Number(c[fracKey]) : null;
            if (frac != null && !(frac >= 0 && frac <= 1)) frac = null;
            const criterion = p.model_type !== 'rarity' ? '' : (asScored ? (p.flag_basis || '') : 'the thresholds above');
            hist.innerHTML = this._buildHistogramHTML(p.histogram || [], p.metric, frac, criterion, null);
        }
    },

    // Threshold fields per type: [alertConfig key, label, min, max, step, default].
    THRESHOLD_FIELDS: {
        rarity: [['confidence_threshold', 'Min confidence', 0, 1, 0.01, 0.9], ['percent_threshold', 'Max % of days', 0.1, 100, 0.1, 10]],
        volume_baseline: [['z_threshold', 'Z-score above', 0, 20, 0.1, 3.5]],
        beacon: [['beacon_threshold', 'Score at least', 0, 1, 0.01, 0.8]],
        long_connection: [['longconn_threshold', 'Score at least', 0, 1, 0.01, 0.5]],
    },

    _scoreThresholdsHTML(mt) {
        const c = this.editor.alertConfig;
        if (mt === 'first_seen') {
            return `<div class="score-thresholds"><label class="toggle-label"><input type="checkbox" class="themed-checkbox" id="scoreAlertOnNew" ${c.alert_on_new ? 'checked' : ''}> Alert on new entities only</label></div>`;
        }
        const fields = this.THRESHOLD_FIELDS[mt];
        if (!fields) return '';
        return `<div class="score-thresholds">${fields.map(([key, label, min, max, step]) => `
    <label class="score-threshold" data-key="${key}">
        <span class="score-threshold-label">${_esc(label)}</span>
        <input type="range" class="score-threshold-range" data-key="${key}" min="${min}" max="${max}" step="${step}" value="${_esc(String(c[key]))}">
        <input type="number" class="model-num-input model-num-mini score-threshold-num" data-key="${key}" min="${min}" max="${max}" step="${step}" value="${_esc(String(c[key]))}">
    </label>`).join('')}</div>`;
    },

    _bindScoreThresholds() {
        const e = this.editor;
        document.getElementById('scoreAlertOnNew')?.addEventListener('change', ev => {
            e.alertConfig.alert_on_new = ev.target.checked;
            this._onThresholdChange('alert_on_new');
        });
        document.querySelectorAll('#modelScorePreview .score-threshold input').forEach(input => {
            input.addEventListener('input', ev => {
                const key = ev.target.dataset.key;
                const v = parseFloat(ev.target.value);
                if (!Number.isFinite(v)) return;
                e.alertConfig[key] = v;
                // Keep the slider and its number box together.
                document.querySelectorAll(`#modelScorePreview .score-threshold input[data-key="${key}"]`).forEach(o => { if (o !== ev.target) o.value = String(v); });
                this._onThresholdChange(key);
            });
        });
    },

    // A threshold moved, from the Scores tab or the rail. Both controls follow,
    // and the count is redone client-side when the distribution allows it.
    _onThresholdChange(key) {
        const e = this.editor;
        this._markDirty();
        const v = e.alertConfig[key];
        const railIds = { confidence_threshold: 'alertConfidence', percent_threshold: 'alertPercent', z_threshold: 'alertZThreshold', beacon_threshold: 'alertBeaconThreshold', longconn_threshold: 'alertLongConnThreshold', alert_on_new: 'alertOnNew' };
        const rail = document.getElementById(railIds[key]);
        if (rail && document.activeElement !== rail) {
            if (rail.type === 'checkbox') rail.checked = !!v; else rail.value = String(v);
        }
        document.querySelectorAll(`#modelScorePreview .score-threshold input[data-key="${key}"]`).forEach(o => { if (document.activeElement !== o) o.value = String(v); });
        const scoreNew = document.getElementById('scoreAlertOnNew');
        if (scoreNew && key === 'alert_on_new') scoreNew.checked = !!v;

        if (e.resultMode !== 'scores' || !e.preview) return;
        if (e.preview.model_type !== e.modelType) { this._schedulePreview(); return; }
        if (this._countFlags(e.preview) || this._thresholdsAsScored(e.preview.model_type)) this._renderFlagCount();
        else this._schedulePreview();
    },

    // True when the thresholds the count depends on are the ones the preview was
    // scored with, so the server's would_flag and wording apply as they are.
    _thresholdsAsScored(mt) {
        const was = this.editor.previewCfg;
        if (!was) return false;
        const now = this.editor.alertConfig;
        const keys = mt === 'first_seen' ? ['alert_on_new'] : (this.THRESHOLD_FIELDS[mt] || []).map(f => f[0]);
        return keys.every(k => String(was[k]) === String(now[k]));
    },

    // Recounts would_flag from the preview's distribution (see ScoreDistribution
    // in preview.go): { n, exact } or null when it cannot. Each rule mirrors the
    // server's for the type, defaults included.
    _countFlags(p) {
        const c = this.editor.alertConfig;
        if (p.model_type === 'first_seen' || p.model_type === 'tlsh') {
            const s = p.stats || {};
            return { n: Number(c.alert_on_new ? s.new_recent : s.entities) || 0, exact: true };
        }
        const d = p.distribution;
        if (!d || d.truncated || !Array.isArray(d.cells)) return null;
        const onGrid = (v, step) => Math.abs(v / step - Math.round(v / step)) < 1e-6;
        let n = 0, exact = true;
        if (p.model_type === 'rarity') {
            const ct = Number(c.confidence_threshold) || 0, pt = Number(c.percent_threshold) || 0;
            const cj = Math.round(ct * 100), pj = Math.round(pt * 10);
            exact = onGrid(ct, 0.01) && onGrid(pt, 0.1);
            for (const [cb, pb, cnt] of d.cells) {
                if ((ct <= 0 || cb > cj) && (pt <= 0 || pb < pj)) n += cnt;
            }
        } else if (p.model_type === 'volume_baseline') {
            const z = Number(c.z_threshold) > 0 ? Number(c.z_threshold) : 3.5;
            const zj = Math.round(z * 10);
            exact = onGrid(z, 0.1);
            for (const [zb, cnt] of d.cells) if (zb > zj) n += cnt;
        } else if (this._isNetworkType(p.model_type)) {
            const raw = Number(p.model_type === 'beacon' ? c.beacon_threshold : c.longconn_threshold);
            const t = raw > 0 ? raw : (p.model_type === 'beacon' ? 0.8 : 0.5);
            const tj = Math.round(t * 100);
            exact = onGrid(t, 0.01);
            for (const [fb, cnt] of d.cells) if (fb >= tj) n += cnt;
        } else {
            return null;
        }
        return { n, exact };
    },

    // Builds the score-distribution chart markup, reusing the model viewer's
    // histogram styles. thresholdFrac (0..1), when provided, draws a marker line.
    // criterion, when given, says what the alert actually requires. The marker sits
    // on the metric axis alone, so on a rarity model, where the alert also needs a
    // low percent, everything right of the line would otherwise read as flagged.
    _buildHistogramHTML(buckets, metricKey, thresholdFrac, criterion, thresholdValue) {
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
            const shown = thresholdValue != null ? Number(thresholdValue) : Number(thresholdFrac);
            const label = _esc(shown.toFixed(2));
            // No title: the line is pointer-events:none, so the note below carries it.
            thresholdLine = `<div class="histogram-threshold-line" style="left:calc(12px + (100% - 24px) * ${thresholdFrac})"><span class="histogram-threshold-label">${label}</span></div>`;
        }
        const note = criterion ? `<div class="histogram-note">Alerts need ${_esc(criterion)}. This axis shows ${_esc(metric.toLowerCase())} only.</div>` : '';
        return `<div class="histogram-head"><span class="histogram-title">${_esc(metric)} distribution</span></div>
<div class="histogram-chart">${cols}${thresholdLine}</div>${note}`;
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
            latest_count: 'Latest', baseline_median: 'Median', mad: 'MAD', n_buckets: 'History', z_score: 'z-score',
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
        if (col === 'z_score' && this._isFlatZ(v)) return _esc(this.FLAT_Z_LABEL);
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

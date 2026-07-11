// Fields rail: server-side field statistics for the current search.
//
// Statistics are computed by the backend over a bounded, most-recent sample of the
// query's matched events (see pkg/query/fieldstats.go), never in the browser and
// never over the full result payload. Opening the rail issues one request scoped to
// the exact executed query/time/fractal; the response is cached by that signature so
// sort, filter, and expand/collapse are instant and never refetch or recompute.
//
// It also maintains a cheap field-name index from the visible results (names only,
// no value histograms) so query autocomplete keeps working when the rail is closed.
const FieldStats = {
    isOpen: false,
    sortMode: 'cardinality', // 'cardinality' | 'alpha'
    expandedFields: new Set(),
    filterText: '',

    // Server response for the current signature.
    stats: {},          // { field: { present, cardinality, coverage, topValues: [[val,count],...] } }
    sampleSize: 0,
    approximate: false,
    supported: true,

    // Cheap field-name index (for autocomplete) derived from visible results.
    fieldNames: new Set(),

    // Fetch/caching state.
    _sig: null,         // signature of the data currently in `stats`
    _state: 'idle',     // 'idle' | 'loading' | 'ready' | 'error' | 'unsupported' | 'empty'
    _controller: null,

    init() {
        const toggle = document.getElementById('fieldsRailToggle');
        if (toggle) toggle.addEventListener('click', () => this.toggle());

        const closeBtn = document.getElementById('fieldsRailClose');
        if (closeBtn) closeBtn.addEventListener('click', () => this.close());

        const sortBtn = document.getElementById('fieldsRailSort');
        if (sortBtn) sortBtn.addEventListener('click', () => this.toggleSort());

        const filter = document.getElementById('fieldsRailFilter');
        if (filter) filter.addEventListener('input', (e) => this.handleFilter(e.target.value));
    },

    // ---- open/close -------------------------------------------------------

    toggle() {
        if (this.isOpen) this.close();
        else this.open();
    },

    open() {
        this.isOpen = true;
        const rail = document.getElementById('fieldsRail');
        if (rail) rail.classList.add('open');
        this._syncToggle();

        // Mutually exclusive with the detail panel on width-constrained viewports.
        if (window.LogDetail && window.innerWidth < 1200) {
            const panel = document.getElementById('logDetailPanel');
            if (panel && panel.classList.contains('open')) LogDetail.close();
        }
        this.load();
    },

    close() {
        this.isOpen = false;
        const rail = document.getElementById('fieldsRail');
        if (rail) rail.classList.remove('open');
        this._syncToggle();
        if (this._controller) { this._controller.abort(); this._controller = null; }
    },

    _syncToggle() {
        const toggle = document.getElementById('fieldsRailToggle');
        if (toggle) toggle.classList.toggle('active', this.isOpen);
    },

    // ---- results lifecycle -----------------------------------------------

    // Called by QueryExecutor whenever a result set is ready. Always refreshes the
    // cheap name index; refetches stats only when the rail is open.
    onResults() {
        this.indexNames();
        if (!this.isOpen) return;
        this.load();
    },

    // Cheap: collect field names from visible results (no value counting) so
    // autocomplete has field names even when the rail was never opened.
    indexNames() {
        const results = (window.QueryExecutor && QueryExecutor.currentResults) || [];
        const names = new Set();
        const skip = new Set(['raw_log', '_all_fields', 'fields', 'norm_log', 'log_id', 'fractal_id']);
        for (let i = 0; i < results.length && i < 200; i++) {
            const row = results[i];
            for (const key of Object.keys(row)) {
                if (skip.has(key)) {
                    if (key === 'fields' && row.fields && typeof row.fields === 'object' && !Array.isArray(row.fields)) {
                        for (const sub of Object.keys(row.fields)) names.add(sub);
                    }
                    continue;
                }
                names.add(key);
            }
        }
        this.fieldNames = names;
    },

    // Field names for autocomplete: server stats when present, else the cheap index.
    getFieldNames() {
        const set = new Set(this.fieldNames);
        for (const f of Object.keys(this.stats)) set.add(f);
        return set;
    },

    // ---- fetch + cache ----------------------------------------------------

    _signature() {
        const qe = window.QueryExecutor;
        if (!qe) return null;
        const tr = qe.currentTimeRange || {};
        const fractalId = (window.FractalContext && FractalContext.currentFractal && !FractalContext.isPrism())
            ? FractalContext.currentFractal.id : '';
        return JSON.stringify({
            q: qe.currentQuery || '',
            s: tr.start || '', e: tr.end || '',
            f: fractalId,
            v: qe.variablesPayload() || null
        });
    },

    load() {
        const qe = window.QueryExecutor;
        if (!qe || !qe.currentQuery) { this._state = 'empty'; this.render(); return; }

        // Field distributions describe raw events; they are meaningless over an
        // already-aggregated table or a chart.
        if (qe.isAggregated) { this._state = 'unsupported'; this.render(); return; }

        const sig = this._signature();
        if (sig && sig === this._sig && this._state === 'ready') {
            this.render(); // cache hit
            return;
        }
        this.fetch(sig);
    },

    async fetch(sig) {
        const qe = window.QueryExecutor;
        if (this._controller) this._controller.abort();
        this._controller = new AbortController();

        this._state = 'loading';
        this.render();

        const tr = qe.currentTimeRange || {};
        const body = { query: qe.currentQuery, start: tr.start, end: tr.end };
        const vars = qe.variablesPayload();
        if (vars) body.variables = vars;
        if (window.FractalContext && FractalContext.currentFractal && !FractalContext.isPrism()) {
            body.fractal_id = FractalContext.currentFractal.id;
        }

        try {
            const res = await fetch('/api/v1/query/fieldstats', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body),
                signal: this._controller.signal
            });
            const data = await res.json();
            if (!res.ok || !data.success) {
                this._state = 'error';
                this.render();
                return;
            }

            if (data.supported === false) {
                this.stats = {}; this._sig = sig; this._state = 'unsupported';
                this.render();
                return;
            }

            // Normalize into the shape autocomplete + render expect.
            const stats = {};
            for (const f of (data.fields || [])) {
                stats[f.name] = {
                    present: f.present || 0,
                    cardinality: f.cardinality || 0,
                    coverage: data.sample_size ? (f.present || 0) / data.sample_size : 0,
                    topValues: (f.top || []).map(t => [t.value, t.count])
                };
            }
            this.stats = stats;
            this.sampleSize = data.sample_size || 0;
            this.approximate = !!data.approximate;
            this._sig = sig;
            this._state = Object.keys(stats).length ? 'ready' : 'empty';

            // Auto-expand a small result set; keep collapsed when there are many
            // fields so the list stays scannable.
            if (Object.keys(stats).length <= 8) {
                this.expandedFields = new Set(Object.keys(stats));
            }
            this.render();
        } catch (err) {
            if (err.name === 'AbortError') return;
            this._state = 'error';
            this.render();
        } finally {
            this._controller = null;
        }
    },

    // ---- rendering --------------------------------------------------------

    getFieldsSorted() {
        const fields = Object.keys(this.stats);
        if (this.sortMode === 'alpha') return fields.sort((a, b) => a.localeCompare(b));
        return fields.sort((a, b) => this.stats[b].cardinality - this.stats[a].cardinality);
    },

    render() {
        const body = document.getElementById('fieldsRailBody');
        if (!body) return;

        // Header count + sample note.
        const countEl = document.getElementById('fieldsRailCount');
        const noteEl = document.getElementById('fieldsRailNote');
        const fieldCount = Object.keys(this.stats).length;
        if (countEl) countEl.textContent = this._state === 'ready' ? String(fieldCount) : '';
        if (noteEl) {
            if (this._state === 'ready' && this.sampleSize) {
                noteEl.textContent = this.approximate
                    ? `~${this.fmtNum(this.sampleSize)} events sampled`
                    : `${this.fmtNum(this.sampleSize)} events`;
                noteEl.title = this.approximate
                    ? 'The match set is larger than the sample; the distribution is an approximation of the most recent events.'
                    : 'The full match set fit within the sample; these figures are exact.';
                noteEl.style.display = '';
            } else {
                noteEl.style.display = 'none';
            }
        }

        if (this._state === 'loading') { body.innerHTML = this._skeleton(); return; }
        if (this._state === 'error') { body.innerHTML = this._msg('Could not load field statistics'); return; }
        if (this._state === 'unsupported') { body.innerHTML = this._msg('Field statistics apply to raw event results, not aggregations'); return; }
        if (this._state === 'empty' || fieldCount === 0) { body.innerHTML = this._msg('Run a search to explore its fields'); return; }

        const filterLower = this.filterText.toLowerCase();
        let html = '';
        let shown = 0;
        for (const field of this.getFieldsSorted()) {
            if (filterLower && !field.toLowerCase().includes(filterLower)) continue;
            shown++;
            const stat = this.stats[field];
            const expanded = this.expandedFields.has(field);
            const coverPct = Math.round(stat.coverage * 100);

            html += `<div class="fr-field${expanded ? ' expanded' : ''}" data-field="${this.escAttr(field)}">`;
            html += `<div class="fr-field-head" data-field="${this.escAttr(field)}">`;
            html += `<span class="fr-arrow">${expanded ? '▾' : '▸'}</span>`;
            html += `<span class="fr-name" title="${this.escAttr(field)}">${this.escHtml(field)}</span>`;
            html += `<span class="fr-card" title="${this.fmtNum(stat.cardinality)} distinct value${stat.cardinality === 1 ? '' : 's'}">${this.fmtNum(stat.cardinality)}</span>`;
            html += `</div>`;

            if (expanded) {
                html += `<div class="fr-detail">`;
                html += `<div class="fr-coverage"><span class="fr-coverage-bar"><span style="width:${coverPct}%"></span></span><span class="fr-coverage-pct">${coverPct}% coverage</span></div>`;
                const top = stat.topValues || [];
                const maxCount = top.length ? top[0][1] : 1;
                for (const [val, count] of top) {
                    const barW = maxCount ? (count / maxCount) * 100 : 0;
                    const pct = stat.present ? ((count / stat.present) * 100).toFixed(1) : '0.0';
                    const display = val.length > 48 ? val.substring(0, 48) + '…' : val;
                    html += `<div class="fr-val" data-field="${this.escAttr(field)}" data-value="${this.escAttr(val)}">`;
                    html += `<span class="fr-val-bar" style="width:${barW}%"></span>`;
                    html += `<span class="fr-val-text" title="${this.escAttr(val)}">${val === '' ? '<em>(empty)</em>' : this.escHtml(display)}</span>`;
                    html += `<span class="fr-val-actions">`;
                    html += `<button class="fr-val-btn fr-in" title="Filter to this value">+</button>`;
                    html += `<button class="fr-val-btn fr-out" title="Exclude this value">&minus;</button>`;
                    html += `</span>`;
                    html += `<span class="fr-val-count">${this.fmtNum(count)} · ${pct}%</span>`;
                    html += `</div>`;
                }
                if (stat.cardinality > top.length) {
                    html += `<div class="fr-more">+${this.fmtNum(stat.cardinality - top.length)} more value${stat.cardinality - top.length === 1 ? '' : 's'}</div>`;
                }
                html += `</div>`;
            }
            html += `</div>`;
        }

        if (shown === 0) html = this._msg('No fields match the filter');
        body.innerHTML = html;
        this._bind(body);
    },

    _bind(body) {
        body.onclick = (e) => {
            const head = e.target.closest('.fr-field-head');
            if (head) {
                const f = head.dataset.field;
                if (this.expandedFields.has(f)) this.expandedFields.delete(f);
                else this.expandedFields.add(f);
                this.render();
                return;
            }
            const inBtn = e.target.closest('.fr-in');
            const outBtn = e.target.closest('.fr-out');
            if (inBtn || outBtn) {
                const row = e.target.closest('.fr-val');
                if (row) this.addFilter(row.dataset.field, row.dataset.value, !!outBtn);
            }
        };
    },

    _skeleton() {
        const widths = [70, 50, 62, 45, 68, 55, 40, 60];
        return widths.map(w =>
            `<div class="fr-skel"><span class="fr-skel-name" style="width:${w}%"></span><span class="fr-skel-badge"></span></div>`
        ).join('');
    },

    _msg(text) {
        return `<div class="fr-empty">${this.escHtml(text)}</div>`;
    },

    // ---- actions ----------------------------------------------------------

    addFilter(field, value, exclude = false) {
        const input = document.getElementById('queryInput');
        if (!input) return;
        const op = exclude ? '!=' : '=';
        const filter = /^-?\d+(\.\d+)?$/.test(value)
            ? `${field}${op}${value}`
            : `${field}${op}"${value.replace(/"/g, '\\"')}"`;
        const current = input.value.trim();
        input.value = current ? `${current} ${filter}` : filter;
        input.dispatchEvent(new Event('input', { bubbles: true }));
    },

    handleFilter(value) {
        this.filterText = value;
        this.render();
    },

    toggleSort() {
        this.sortMode = this.sortMode === 'cardinality' ? 'alpha' : 'cardinality';
        const btn = document.getElementById('fieldsRailSort');
        if (btn) {
            btn.textContent = this.sortMode === 'cardinality' ? '#' : 'Az';
            btn.title = this.sortMode === 'cardinality' ? 'Sort alphabetically' : 'Sort by cardinality';
        }
        this.render();
    },

    // ---- helpers ----------------------------------------------------------

    fmtNum(n) {
        n = Number(n) || 0;
        if (n >= 1e9) return (n / 1e9).toFixed(n % 1e9 === 0 ? 0 : 1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(n % 1e6 === 0 ? 0 : 1) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(n % 1e3 === 0 ? 0 : 1) + 'k';
        return String(n);
    },

    escHtml(str) {
        if (window.Utils) return Utils.escapeHtml(str);
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    escAttr(str) {
        if (window.Utils) return Utils.escapeAttr(str);
        return String(str).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
};

window.FieldStats = FieldStats;

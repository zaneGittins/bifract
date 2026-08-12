// ATT&CK matrix over query results (BQL: `... | mitre()`).
//
// The Alerts coverage map answers "what can we detect"; this answers "what did we
// see", from the attack.* tags the matched events already carry. The server does
// the counting (one row per tag), so this module only has to resolve tags to
// techniques against the embedded matrix and paint the grid.
//
// Tag resolution mirrors pkg/attack.ParseLabel: sub-technique hits roll up to
// their parent as inherited, a retired ID is followed to its replacement, and a
// tactic-only tag is counted but never colours a technique cell, because it does
// not say which technique fired.
//
// The grid reuses the coverage map's atk-* styles so both read as one feature.

const BifractMitreMatrix = {
    matrix: null,
    _matrixPromise: null,

    TACTIC_ALIASES: {
        'defense-evasion': 'stealth',
        'stealth': 'defense-evasion',
    },

    // ============================
    // Matrix data
    // ============================

    // The matrix only changes when the binary does and is served with a strong
    // ETag, so one fetch per session is enough. An anonymous shared wallboard
    // cannot call the endpoint at all (viewer+), so it passes the matrix in with
    // its payload instead.
    loadMatrix(provided) {
        if (provided && !this.matrix) this.matrix = this._index(provided);
        if (this.matrix) return Promise.resolve(this.matrix);
        if (!this._matrixPromise) {
            this._matrixPromise = fetch('/api/v1/attack/matrix', { credentials: 'same-origin' })
                .then(r => r.json())
                .then(res => {
                    if (!res.success) throw new Error(res.error || 'Failed to load ATT&CK matrix');
                    this.matrix = this._index(res.data);
                    return this.matrix;
                })
                .catch(err => { this._matrixPromise = null; throw err; });
        }
        return this._matrixPromise;
    },

    _index(raw) {
        const m = raw || {};
        m.byId = new Map();
        m.subsOf = new Map();
        m.byTactic = new Map();
        m.tacticByKey = new Map();

        for (const t of m.tactics || []) {
            m.tacticByKey.set(t.short, t);
            m.tacticByKey.set(String(t.id).toLowerCase(), t);
        }
        for (const tech of m.techniques || []) {
            m.byId.set(tech.id, tech);
            if (tech.deprecated) continue;
            if (tech.sub) {
                if (!m.subsOf.has(tech.parent)) m.subsOf.set(tech.parent, []);
                m.subsOf.get(tech.parent).push(tech);
                continue;
            }
            for (const short of tech.tactics || []) {
                if (!m.byTactic.has(short)) m.byTactic.set(short, []);
                m.byTactic.get(short).push(tech);
            }
        }
        for (const list of m.byTactic.values()) list.sort((a, b) => a.name.localeCompare(b.name));
        for (const list of m.subsOf.values()) list.sort((a, b) => a.id.localeCompare(b.id));
        return m;
    },

    // ============================
    // Tag resolution
    // ============================

    // Returns {kind, id} where kind is technique | tactic | retired | other,
    // or null for a label that is not an ATT&CK reference at all.
    resolveTag(tag) {
        let s = String(tag || '').toLowerCase().trim();
        const prefixed = s.match(/^(?:attack|mitre)\.(.+)$/);
        if (prefixed) s = prefixed[1];
        s = s.replace(/_/g, '-').replace(/[.\-]+$/, '');
        if (!s) return null;

        if (/^ta\d+$/.test(s)) {
            const tactic = this.matrix.tacticByKey.get(s);
            return tactic ? { kind: 'tactic', id: tactic.short } : null;
        }
        if (/^t\d+(\.\d+)?$/.test(s)) return this._resolveTechnique(s);
        if (/^[gsc]\d+$/.test(s)) return { kind: 'other', id: s.toUpperCase() };

        const tactic = this.matrix.tacticByKey.get(s) ||
            this.matrix.tacticByKey.get(this.TACTIC_ALIASES[s]);
        return tactic ? { kind: 'tactic', id: tactic.short } : null;
    },

    // ATT&CK revokes and replaces IDs; a tag carrying the old one still means the
    // new one. The chain is walked a few steps only, so bad data cannot hang.
    //
    // Only an ID that has a cell in the grid counts as a technique. A deprecated
    // one does not, and calling it a technique would put its events in the
    // headline count while no cell on screen carries them.
    _resolveTechnique(slug) {
        let id = slug.toUpperCase();
        if (this._renderable(this.matrix.byId.get(id))) return { kind: 'technique', id };
        const revoked = this.matrix.revoked_by || {};
        for (let i = 0; i < 4; i++) {
            const next = revoked[id];
            if (!next) break;
            id = next;
            if (this._renderable(this.matrix.byId.get(id))) return { kind: 'technique', id };
        }
        return { kind: 'retired', id: slug.toUpperCase() };
    },

    // A technique has a cell only if it and (for a sub-technique) its parent
    // survive into the grid.
    _renderable(tech) {
        if (!tech || tech.deprecated) return false;
        if (!tech.sub) return true;
        const parent = this.matrix.byId.get(tech.parent);
        return !!parent && !parent.deprecated;
    },

    // ============================
    // Aggregation
    // ============================

    // Folds the server's (tag, [by], count) rows into per-technique tallies.
    aggregate(rows, byField) {
        const state = {
            techniques: new Map(),   // id -> {direct, inherited, total, tags:Set, by:Map}
            tactics: new Map(),      // short -> events attributed by technique
            tacticOnly: new Map(),   // short -> events from tactic-only tags
            retired: new Map(),
            other: new Map(),
            unresolved: new Map(),
        };

        const cell = (id) => {
            if (!state.techniques.has(id)) {
                state.techniques.set(id, { direct: 0, inherited: 0, total: 0, tags: new Set(), by: new Map() });
            }
            return state.techniques.get(id);
        };

        for (const row of rows || []) {
            const tag = row.attack_tag;
            if (!tag) continue;
            const count = Number(row._count) || 0;
            const byValue = byField ? row[byField] : null;
            const hit = this.resolveTag(tag);

            if (!hit) { this._bump(state.unresolved, tag, count); continue; }
            if (hit.kind === 'tactic') { this._bump(state.tacticOnly, hit.id, count); continue; }
            if (hit.kind === 'retired') { this._bump(state.retired, hit.id, count); continue; }
            if (hit.kind === 'other') { this._bump(state.other, hit.id, count); continue; }

            const c = cell(hit.id);
            c.direct += count;
            c.total += count;
            c.tags.add(tag);
            if (byValue !== null && byValue !== undefined && byValue !== '') {
                this._bump(c.by, String(byValue), count);
            }
        }

        // Sub-technique activity rolls up to the parent, kept separate from direct
        // hits: "we saw T1059.004" and "we saw something under T1059" differ.
        for (const [id, c] of [...state.techniques]) {
            const tech = this.matrix.byId.get(id);
            if (!tech || !tech.sub || !tech.parent) continue;
            const parent = cell(tech.parent);
            parent.inherited += c.direct;
            parent.total += c.direct;
            for (const tag of c.tags) parent.tags.add(tag);
            for (const [k, v] of c.by) this._bump(parent.by, k, v);
        }

        for (const [id, c] of state.techniques) {
            const tech = this.matrix.byId.get(id);
            if (!tech) continue;
            const top = tech.sub ? this.matrix.byId.get(tech.parent) : tech;
            for (const short of (top && top.tactics) || []) {
                this._bump(state.tactics, short, c.direct);
            }
        }
        return state;
    },

    _bump(map, key, n) {
        map.set(key, (map.get(key) || 0) + n);
    },

    // ============================
    // Render
    // ============================

    // host: element to render into (its contents are replaced).
    // opts: {
    //   rows, config: {tagField, byField, limit},
    //   embedded: true for a dashboard/notebook panel (starts on observed-only and
    //             drops the controls that only make sense while hunting),
    //   onDrill(tags, technique): omit where there is no query bar to drill with.
    // }
    //
    // Several matrices can be live at once (a dashboard of panels), so all state
    // lives on the returned view, never on the module.
    async render(host, opts = {}) {
        if (!host) return null;
        const view = {
            host,
            rows: opts.rows || [],
            config: opts.config || {},
            embedded: !!opts.embedded,
            onDrill: typeof opts.onDrill === 'function' ? opts.onDrill : null,
            cells: new Map(),
            autoExpanded: new Set(),
            showSubs: false,
            observedOnly: !!opts.embedded,
            colorBy: 'events',
            search: '',
            selectedId: null,
        };
        view.byField = view.config.byField || '';

        // A panel that refreshes replaces its own render; the superseded one must
        // not paint over the newer grid or leave its observer attached.
        this._release(host);
        const token = {};
        host._mtrToken = token;

        host.innerHTML = '<div class="atk-empty">Loading ATT&CK matrix...</div>';
        try {
            await this.loadMatrix(opts.matrix);
        } catch (err) {
            if (host._mtrToken !== token) return null;
            host.innerHTML = `<div class="atk-empty">${Utils.escapeHtml(err.message || 'Failed to load ATT&CK matrix')}</div>`;
            return null;
        }
        if (host._mtrToken !== token) return null;

        view.state = this.aggregate(view.rows, view.byField);
        this._buildShell(view);
        this._buildGrid(view);
        this._paint(view);
        this._renderSummary(view);
        this._renderLegend(view);
        host._mtrView = view;
        return view;
    },

    // Drops a host's previous view without touching any other live matrix.
    _release(host) {
        const prev = host && host._mtrView;
        if (!prev) return;
        prev.resizeObserver?.disconnect();
        if (this._drawerView === prev) this._closeDrawer();
        host._mtrView = null;
    },

    // Tears down anything the matrix parked outside its host (drawer, tooltip).
    destroy(host) {
        if (host) this._release(host);
        document.getElementById('mtrDrawerHost')?.remove();
        document.getElementById('mtrTip')?.remove();
        document.body.classList.remove('atk-drawer-open');
        this._drawerView = null;
    },

    _buildShell(view) {
        const tagField = view.config.tagField || 'rule_tags';
        const scanNote = tagField === 'norm_log'
            ? 'Scanning the whole event for attack.* tags'
            : `Tags read from ${tagField}`;

        const byOption = view.byField
            ? `<option value="by">Distinct ${Utils.escapeHtml(view.byField)}</option>` : '';

        view.host.innerHTML = `
            <section class="atk-view mtr-view${view.embedded ? ' mtr-embedded' : ''}">
                <div class="atk-summary" data-mtr-summary></div>

                <div class="atk-controls">
                    ${view.embedded ? '' : '<input type="text" class="atk-search" data-mtr-search placeholder="Search technique or ID..." />'}
                    <select class="atk-select" data-mtr-scope title="Show the whole matrix or only what these events touched">
                        <option value="all"${view.observedOnly ? '' : ' selected'}>Full matrix</option>
                        <option value="observed"${view.observedOnly ? ' selected' : ''}>Observed only</option>
                    </select>
                    ${byOption ? `<select class="atk-select" data-mtr-colorby title="What the cell colour encodes">
                        <option value="events">Event count</option>
                        ${byOption}
                    </select>` : ''}
                    <label class="atk-toggle"><input type="checkbox" data-mtr-subs /> Sub-techniques</label>
                    <div class="atk-controls-end">
                        ${view.embedded ? '' : `<span class="mtr-scan-note" title="${Utils.escapeHtml(scanNote)}">${Utils.escapeHtml(scanNote)}</span>`}
                        <div class="atk-legend" data-mtr-legend></div>
                        <button class="btn-secondary btn-sm" data-mtr-export title="Download what these events touched as an ATT&amp;CK Navigator layer (.json)">Export layer</button>
                    </div>
                </div>

                <div class="atk-empty" data-mtr-status style="display:none;"></div>
                <div class="atk-matrix-wrap mtr-matrix-wrap"><div class="atk-matrix" data-mtr-matrix></div></div>
            </section>
        `;

        const q = (sel) => view.host.querySelector(sel);
        q('[data-mtr-search]')?.addEventListener('input', (e) => {
            view.search = e.target.value.trim().toLowerCase();
            this._applyFilters(view);
        });
        q('[data-mtr-scope]').addEventListener('change', (e) => {
            view.observedOnly = e.target.value === 'observed';
            this._applyFilters(view);
        });
        q('[data-mtr-colorby]')?.addEventListener('change', (e) => {
            view.colorBy = e.target.value;
            this._paint(view);
            this._renderLegend(view);
        });
        q('[data-mtr-subs]').addEventListener('change', (e) => {
            view.showSubs = e.target.checked;
            view.host.querySelectorAll('.atk-subs').forEach(wrap => {
                wrap.classList.toggle('atk-open', e.target.checked);
                wrap.previousElementSibling?.classList.toggle('atk-open', e.target.checked);
            });
        });
        q('[data-mtr-export]').addEventListener('click', () => this._exportLayer(view));

        const status = q('[data-mtr-status]');
        if (!view.state.techniques.size) {
            // Naming the field the search actually read is the difference between
            // "these events are untagged" and "you read the wrong field".
            status.style.display = 'block';
            status.textContent = tagField === 'norm_log'
                ? 'No ATT&CK tags found anywhere in these events.'
                : `No ATT&CK tags found in ${tagField}. If this source keeps them elsewhere, name it (mitre(tags=detect_mtd_tags)) or scan the whole event with mitre(tags=norm_log).`;
        } else if (view.config.limit && view.rows.length >= view.config.limit) {
            // A truncated tail makes every count below a lower bound, which a report
            // must not present as a total.
            status.style.display = 'block';
            status.className = 'atk-note mtr-note';
            status.textContent = `Only the top ${view.config.limit} ${view.byField ? 'tag/' + view.byField + ' combinations' : 'tags'} were returned, so counts here are lower bounds. Raise limit= or drop by= for exact totals.`;
        }
    },

    _buildGrid(view) {
        const grid = view.host.querySelector('[data-mtr-matrix]');
        const frag = document.createDocumentFragment();

        for (const tactic of this.matrix.tactics || []) {
            const column = document.createElement('div');
            column.className = 'atk-column';

            const head = document.createElement('div');
            head.className = 'atk-col-head';
            head.innerHTML = `
                <div class="atk-col-name">${Utils.escapeHtml(tactic.name)}</div>
                <div class="atk-col-count" data-tactic-count="${Utils.escapeHtml(tactic.short)}">-</div>
                <div class="atk-col-sub" data-tactic-tech="${Utils.escapeHtml(tactic.short)}"></div>
                <div class="atk-meter"><div class="atk-meter-fill" data-tactic-meter="${Utils.escapeHtml(tactic.short)}" style="width:0%"></div></div>
            `;
            column.appendChild(head);

            const body = document.createElement('div');
            body.className = 'atk-col-body';
            for (const tech of this.matrix.byTactic.get(tactic.short) || []) {
                const subs = this.matrix.subsOf.get(tech.id) || [];
                body.appendChild(this._buildCell(view, tech, subs));
                if (subs.length) {
                    const wrap = document.createElement('div');
                    wrap.className = 'atk-subs';
                    for (const sub of subs) wrap.appendChild(this._buildCell(view, sub, []));
                    body.appendChild(wrap);
                }
            }
            column.appendChild(body);
            frag.appendChild(column);
        }

        grid.innerHTML = '';
        grid.appendChild(frag);
        this._fitColumns(view);

        if (view.resizeObserver) view.resizeObserver.disconnect();
        view.resizeObserver = new ResizeObserver(() => this._fitColumns(view));
        const wrap = view.host.querySelector('.atk-matrix-wrap');
        if (wrap) view.resizeObserver.observe(wrap);
    },

    // Fit the whole kill chain across the available width, down to a legibility
    // floor below which the grid scrolls instead of shrinking.
    _fitColumns(view) {
        const wrap = view.host.querySelector('.atk-matrix-wrap');
        const grid = view.host.querySelector('[data-mtr-matrix]');
        const columns = (this.matrix.tactics || []).length;
        if (!wrap || !grid || !columns || !wrap.clientWidth) return;
        const available = wrap.clientWidth - (columns - 1) - 2;
        grid.style.setProperty('--atk-col-min', Math.max(90, Math.floor(available / columns)) + 'px');
    },

    _buildCell(view, tech, subs) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'atk-cell';
        btn.dataset.tid = tech.id;
        btn.dataset.name = tech.name.toLowerCase();

        const chevron = subs.length
            ? `<span class="atk-sub-toggle" role="button" tabindex="-1" aria-label="Toggle sub-techniques">
                   <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><path d="M9 6l6 6-6 6"/></svg>
               </span>`
            : '';

        btn.innerHTML = `
            <div class="atk-cell-top">
                ${chevron}
                <span class="atk-cell-name">${Utils.escapeHtml(tech.name)}<span class="atk-cell-subs" data-meta></span></span>
                <span class="atk-badge" data-badge></span>
            </div>
        `;

        btn.addEventListener('click', (e) => {
            if (e.target.closest('.atk-sub-toggle')) {
                e.stopPropagation();
                const wrap = btn.nextElementSibling;
                if (wrap && wrap.classList.contains('atk-subs')) {
                    const open = wrap.classList.toggle('atk-open');
                    btn.classList.toggle('atk-open', open);
                }
                return;
            }
            this._openDrawer(view, tech.id);
        });
        btn.addEventListener('mouseenter', (e) => this._showTip(view, e, tech));
        btn.addEventListener('mousemove', (e) => this._moveTip(e));
        btn.addEventListener('mouseleave', () => this._hideTip());

        if (!view.cells.has(tech.id)) view.cells.set(tech.id, []);
        view.cells.get(tech.id).push(btn);
        return btn;
    },

    // ============================
    // Painting
    // ============================

    // Event volumes span orders of magnitude, so the ramp is logarithmic and
    // relative to this result set. An absolute scale would leave every cell on
    // the same step for one query and saturated for the next.
    _heat(value, max) {
        if (!value || value <= 0) return 0;
        if (max <= 1) return 5;
        const ratio = Math.log(value) / Math.log(max);
        return Math.max(1, Math.min(5, 1 + Math.floor(ratio * 4.999)));
    },

    _cellValue(view, c) {
        if (!c) return 0;
        return view.colorBy === 'by' ? c.by.size : c.total;
    },

    _maxValue(view) {
        let max = 0;
        for (const c of view.state.techniques.values()) {
            max = Math.max(max, this._cellValue(view, c));
        }
        return max;
    },

    _paint(view) {
        const max = this._maxValue(view);
        view.max = max;

        for (const [id, els] of view.cells) {
            const c = view.state.techniques.get(id);
            const value = this._cellValue(view, c);
            const heat = this._heat(value, max);
            const subs = this.matrix.subsOf.get(id) || [];
            const subsHit = subs.filter(s => view.state.techniques.has(s.id)).length;

            for (const el of els) {
                if (heat > 0) el.dataset.heat = String(heat);
                else delete el.dataset.heat;

                const badge = el.querySelector('[data-badge]');
                if (badge) badge.textContent = value > 0 ? this._fmt(value) : '';

                const meta = el.querySelector('[data-meta]');
                if (meta) meta.textContent = subs.length && subsHit ? ` (${subsHit}/${subs.length})` : '';
            }
        }

        this._paintTacticHeaders(view);
        this._applyFilters(view);
    },

    _paintTacticHeaders(view) {
        for (const tactic of this.matrix.tactics || []) {
            const techs = this.matrix.byTactic.get(tactic.short) || [];
            let hit = 0;
            for (const tech of techs) {
                const c = view.state.techniques.get(tech.id);
                if (c && c.total > 0) hit++;
            }
            const events = view.state.tactics.get(tactic.short) || 0;
            const pct = techs.length ? Math.round((hit / techs.length) * 100) : 0;

            const count = view.host.querySelector(`[data-tactic-count="${tactic.short}"]`);
            if (count) count.textContent = hit ? `${hit} technique${hit === 1 ? '' : 's'}` : '-';

            const sub = view.host.querySelector(`[data-tactic-tech="${tactic.short}"]`);
            if (sub) sub.textContent = events ? `${this._fmt(events)} events` : '';

            const meter = view.host.querySelector(`[data-tactic-meter="${tactic.short}"]`);
            if (meter) meter.style.width = pct + '%';

            const head = count?.closest('.atk-col-head');
            if (head) {
                head.title = `${tactic.name}\n` +
                    `${hit}/${techs.length} of this tactic's techniques appear in these events.\n` +
                    `${this._fmt(events)} events, summed across those techniques: an event tagged with two of them counts once for each.`;
            }
        }
    },

    // Non-matching cells dim rather than disappear so the kill chain keeps its
    // shape. "Observed only" is the exception: once narrowed to what fired, a
    // screen of empty cells is not what was asked for, so those are removed and
    // the search hides rather than dims on top of that.
    _applyFilters(view) {
        const term = view.search;
        const narrowing = Boolean(term) || view.observedOnly;
        const expand = new Set();

        for (const [id, els] of view.cells) {
            const c = view.state.techniques.get(id);
            const observed = !!(c && c.total > 0);
            const match = !term || id.toLowerCase().includes(term) ||
                (els[0]?.dataset.name || '').includes(term);
            const hidden = view.observedOnly && (!observed || !match);

            for (const el of els) {
                el.classList.toggle('mtr-hidden', hidden);
                el.classList.toggle('atk-dim', !view.observedOnly && !match);
                if (match && !hidden) {
                    const wrap = el.closest('.atk-subs');
                    if (wrap && narrowing) expand.add(wrap);
                }
            }
        }

        // Only groups this opened are closed again when the filter clears, so a
        // group the operator expanded by hand survives.
        for (const wrap of view.autoExpanded) {
            if (narrowing && expand.has(wrap)) continue;
            wrap.classList.remove('atk-open');
            wrap.previousElementSibling?.classList.remove('atk-open');
        }
        view.autoExpanded = narrowing ? expand : new Set();
        for (const wrap of view.autoExpanded) {
            wrap.classList.add('atk-open');
            wrap.previousElementSibling?.classList.add('atk-open');
        }

        // An "observed only" column with nothing left in it is noise in a report.
        view.host.querySelectorAll('.atk-column').forEach(col => {
            const anyVisible = col.querySelector('.atk-cell:not(.mtr-hidden)');
            col.classList.toggle('mtr-hidden', view.observedOnly && !anyVisible);
        });
    },

    _fmt(n) {
        if (n >= 1000000) return (n / 1000000).toFixed(n >= 10000000 ? 0 : 1) + 'M';
        if (n >= 1000) return (n / 1000).toFixed(n >= 10000 ? 0 : 1) + 'k';
        return String(n);
    },

    // ============================
    // Summary and legend
    // ============================

    _renderSummary(view) {
        const el = view.host.querySelector('[data-mtr-summary]');
        if (!el) return;
        const s = view.state;

        let subCount = 0;
        let topId = '';
        let topCount = 0;
        for (const [id, c] of s.techniques) {
            if (this.matrix.byId.get(id)?.sub) subCount++;
            if (c.direct > topCount) { topCount = c.direct; topId = id; }
        }
        const tacticsHit = [...s.tactics].filter(([, v]) => v > 0).length;
        const totalTactics = (this.matrix.tactics || []).length;
        const top = topId ? this.matrix.byId.get(topId) : null;

        // Counted as tags, not events. The rows are already grouped by tag, so
        // whether an event ALSO carried a technique tag is not knowable here, and
        // an event total would read as "activity missing from the map" when most
        // of it is on the map twice over.
        const tacticTags = [...s.tacticOnly.entries()].sort((a, b) => b[1] - a[1]);
        const badTags = [...s.retired.entries(), ...s.unresolved.entries()].sort((a, b) => b[1] - a[1]);
        const tacticNote = tacticTags.length
            ? this.matrix.tacticByKey.get(tacticTags[0][0])?.name || tacticTags[0][0] : '';

        const stat = (label, value, note, tip, warn) => `
            <span class="atk-stat" title="${Utils.escapeHtml(tip || '')}">
                <span class="atk-stat-label">${Utils.escapeHtml(label)}</span>
                <span class="atk-stat-value${warn ? ' atk-warn' : ''}">${Utils.escapeHtml(value)}</span>
                ${note ? `<span class="atk-stat-note">${Utils.escapeHtml(note)}</span>` : ''}
            </span>`;

        el.innerHTML = `
            ${stat('Techniques', String(s.techniques.size), subCount ? `${subCount} sub-techniques` : '',
                'Distinct ATT&CK techniques seen in these events. Parents credited by a sub-technique hit are included.')}
            ${stat('Tactics', `${tacticsHit}/${totalTactics}`, '',
                'Kill-chain stages these events touched.')}
            ${stat('Top technique', top ? top.name : '-', top ? `${this._fmt(topCount)} events` : '',
                top ? `${topId} accounts for the most events in this result set.` : 'No technique tags in this result set.')}
            ${tacticTags.length ? stat('Tactic tags', String(tacticTags.length), tacticNote,
                'Tactic-level tags (attack.execution) seen on these events. A tactic does not say which technique fired, so it colours no cell; the same events usually carry a technique tag as well.') : ''}
            ${badTags.length ? stat('Unresolved', `${badTags.length} tag${badTags.length === 1 ? '' : 's'}`,
                badTags.slice(0, 4).map(([tag]) => tag).join(', '),
                'Tags naming a technique ID that does not exist in this ATT&CK version and has no replacement, usually a typo or a technique MITRE removed outright. Retired IDs that DO have a replacement are followed automatically and are not counted here.', true) : ''}
            <span class="atk-stat-version" title="Embedded ATT&CK Enterprise matrix version">ATT&CK v${Utils.escapeHtml(this.matrix.version || '')}</span>
        `;
    },

    _renderLegend(view) {
        const el = view.host.querySelector('[data-mtr-legend]');
        if (!el) return;
        const max = view.max || 0;
        const unit = view.colorBy === 'by' ? view.byField : 'events';
        el.innerHTML = `<span>1</span><div class="atk-legend-swatches">` +
            [1, 2, 3, 4, 5].map(i => `<div class="atk-legend-swatch" data-heat="${i}"></div>`).join('') +
            `</div><span title="Log scale, relative to the busiest technique in this result set">${Utils.escapeHtml(this._fmt(max) + ' ' + unit)}</span>`;
    },

    // ============================
    // Tooltip
    // ============================

    _tipEl() {
        let tip = document.getElementById('mtrTip');
        if (!tip) {
            tip = document.createElement('div');
            tip.id = 'mtrTip';
            tip.className = 'atk-tip';
            document.body.appendChild(tip);
        }
        return tip;
    },

    _showTip(view, event, tech) {
        const tip = this._tipEl();
        const c = view.state.techniques.get(tech.id);
        let detail = 'Not seen in these events';
        if (c && c.total > 0) {
            detail = `${this._fmt(c.total)} events`;
            if (c.inherited > 0) detail += ` (${this._fmt(c.direct)} direct, ${this._fmt(c.inherited)} via sub-techniques)`;
        }
        const byLine = c && view.byField && c.by.size
            ? `<div class="atk-tip-line">${c.by.size} distinct ${Utils.escapeHtml(view.byField)}</div>` : '';

        tip.innerHTML = `
            <div class="atk-tip-id">${Utils.escapeHtml(tech.id)}</div>
            <div>${Utils.escapeHtml(tech.name)}</div>
            <div class="atk-tip-line">${Utils.escapeHtml(detail)}</div>
            ${byLine}
        `;
        tip.classList.add('open');
        this._moveTip(event);
    },

    _moveTip(event) {
        const tip = document.getElementById('mtrTip');
        if (!tip || !tip.classList.contains('open')) return;
        const pad = 14;
        const rect = tip.getBoundingClientRect();
        let left = (event.clientX ?? 0) + pad;
        let top = (event.clientY ?? 0) + pad;
        if (left + rect.width > window.innerWidth - 8) left = (event.clientX ?? 0) - rect.width - pad;
        if (top + rect.height > window.innerHeight - 8) top = (event.clientY ?? 0) - rect.height - pad;
        tip.style.left = Math.max(8, left) + 'px';
        tip.style.top = Math.max(8, top) + 'px';
    },

    _hideTip() {
        document.getElementById('mtrTip')?.classList.remove('open');
    },

    // ============================
    // Drawer
    // ============================

    _drawerEls() {
        let host = document.getElementById('mtrDrawerHost');
        if (!host) {
            host = document.createElement('div');
            host.id = 'mtrDrawerHost';
            host.innerHTML = `
                <div class="atk-drawer-scrim" data-scrim></div>
                <aside class="atk-drawer" role="dialog" aria-modal="true" aria-label="Technique detail">
                    <div class="atk-drawer-head">
                        <div class="atk-drawer-title">
                            <div class="atk-tip-id" data-id></div>
                            <h3 data-name></h3>
                        </div>
                        <button class="atk-drawer-close" data-close aria-label="Close">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M18 6L6 18M6 6l12 12"/>
                            </svg>
                        </button>
                    </div>
                    <div class="atk-drawer-body" data-body></div>
                </aside>`;
            document.body.appendChild(host);
            host.querySelector('[data-scrim]').addEventListener('click', () => this._closeDrawer());
            host.querySelector('[data-close]').addEventListener('click', () => this._closeDrawer());
        }
        // Bound to the document once for the module's lifetime: the drawer host is
        // recreated whenever a query page tears it down, and re-binding here would
        // stack a listener per teardown.
        if (!this._escBound) {
            this._escBound = true;
            document.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') this._closeDrawer();
            });
        }
        return {
            host,
            drawer: host.querySelector('.atk-drawer'),
            scrim: host.querySelector('.atk-drawer-scrim'),
            id: host.querySelector('[data-id]'),
            name: host.querySelector('[data-name]'),
            body: host.querySelector('[data-body]'),
        };
    },

    _openDrawer(view, techniqueId) {
        const tech = this.matrix.byId.get(techniqueId);
        if (!tech) return;
        const els = this._drawerEls();
        this._hideTip();
        // The drawer is shared by every matrix on the page, so it remembers which
        // one owns the open selection rather than assuming the last rendered.
        if (this._drawerView && this._drawerView !== view) this._setSelected(this._drawerView, null);
        this._drawerView = view;
        this._setSelected(view, techniqueId);

        els.drawer.classList.add('open');
        els.scrim.classList.add('open');
        document.body.classList.add('atk-drawer-open');
        els.id.textContent = tech.id;
        els.name.textContent = tech.name;

        const c = view.state.techniques.get(techniqueId);
        const chips = (values) => (values || []).length
            ? `<div class="atk-chips">${values.map(v => `<span class="atk-chip">${Utils.escapeHtml(v)}</span>`).join('')}</div>`
            : '<div class="atk-empty">Not specified</div>';

        const tacticNames = (tech.sub ? (this.matrix.byId.get(tech.parent)?.tactics || []) : (tech.tactics || []))
            .map(short => this.matrix.tacticByKey.get(short)?.name || short);

        const platforms = (tech.platforms || [])
            .map(i => (this.matrix.platforms || [])[i]).filter(Boolean);

        els.body.innerHTML = `
            ${this._drawerActivity(view, tech, c)}
            ${this._drawerBreakdown(view, c)}
            ${this._drawerSubs(view, tech)}
            <div class="atk-drawer-section">
                <h4>Tactics</h4>
                ${chips(tacticNames)}
            </div>
            <div class="atk-drawer-section">
                <h4>Platforms</h4>
                ${chips(platforms)}
            </div>
            <div class="atk-drawer-section">
                <a class="atk-link" href="https://attack.mitre.org/techniques/${encodeURIComponent(tech.id).replace(/\./g, '/')}/"
                   target="_blank" rel="noopener noreferrer">View ${Utils.escapeHtml(tech.id)} on attack.mitre.org</a>
            </div>
        `;

        els.body.querySelector('[data-drill]')?.addEventListener('click', () => {
            const tags = [...(c?.tags || [])];
            if (!tags.length || !view.onDrill) return;
            this._closeDrawer();
            view.onDrill(tags, tech);
        });
    },

    _drawerActivity(view, tech, c) {
        if (!c || c.total === 0) {
            return `<div class="atk-drawer-section">
                <div class="atk-note mtr-note">No event in this result set carries ${Utils.escapeHtml(tech.id)}.</div>
            </div>`;
        }
        const split = c.inherited > 0
            ? `<div class="atk-note mtr-note">${this._fmt(c.direct)} tagged with ${Utils.escapeHtml(tech.id)} directly, ${this._fmt(c.inherited)} via its sub-techniques.</div>`
            : '';
        const drill = view.onDrill
            ? `<button class="btn-secondary btn-sm" data-drill>View matching events</button>` : '';

        return `
            <div class="atk-drawer-section">
                <div class="mtr-drawer-metric">
                    <span class="mtr-metric-value">${Utils.escapeHtml(this._fmt(c.total))}</span>
                    <span class="mtr-metric-label">events</span>
                    ${drill}
                </div>
                ${split}
                <div class="atk-chips">${[...c.tags].map(t => `<span class="atk-chip">${Utils.escapeHtml(t)}</span>`).join('')}</div>
            </div>`;
    },

    // The by= breakdown is what turns "T1059.004 fired" into "on these hosts".
    _drawerBreakdown(view, c) {
        if (!view.byField || !c || !c.by.size) return '';
        const rows = [...c.by].sort((a, b) => b[1] - a[1]);
        const max = rows[0][1] || 1;
        const shown = rows.slice(0, 15);
        const more = rows.length - shown.length;

        return `
            <div class="atk-drawer-section">
                <h4>By ${Utils.escapeHtml(view.byField)} (${rows.length})</h4>
                ${shown.map(([value, count]) => `
                    <div class="mtr-bar-row" title="${Utils.escapeHtml(value)}">
                        <span class="mtr-bar-label">${Utils.escapeHtml(value)}</span>
                        <span class="mtr-bar-track"><span class="mtr-bar-fill" style="width:${Math.max(2, Math.round((count / max) * 100))}%"></span></span>
                        <span class="mtr-bar-count">${Utils.escapeHtml(this._fmt(count))}</span>
                    </div>`).join('')}
                ${more > 0 ? `<div class="atk-empty">and ${more} more</div>` : ''}
            </div>`;
    },

    _drawerSubs(view, tech) {
        if (tech.sub) return '';
        const subs = this.matrix.subsOf.get(tech.id) || [];
        const hit = subs.map(s => [s, view.state.techniques.get(s.id)]).filter(([, c]) => c && c.direct > 0);
        if (!hit.length) return '';

        return `
            <div class="atk-drawer-section">
                <h4>Sub-techniques seen (${hit.length}/${subs.length})</h4>
                ${hit.sort((a, b) => b[1].direct - a[1].direct).map(([s, c]) => `
                    <div class="mtr-bar-row">
                        <span class="mtr-bar-label">${Utils.escapeHtml(s.id)} ${Utils.escapeHtml(s.name)}</span>
                        <span class="mtr-bar-count">${Utils.escapeHtml(this._fmt(c.direct))}</span>
                    </div>`).join('')}
            </div>`;
    },

    _setSelected(view, techniqueId) {
        if (view.selectedId) {
            (view.cells.get(view.selectedId) || []).forEach(el => el.classList.remove('atk-selected'));
        }
        view.selectedId = techniqueId;
        (view.cells.get(techniqueId) || []).forEach(el => el.classList.add('atk-selected'));
    },

    _closeDrawer() {
        const host = document.getElementById('mtrDrawerHost');
        host?.querySelector('.atk-drawer')?.classList.remove('open');
        host?.querySelector('.atk-drawer-scrim')?.classList.remove('open');
        document.body.classList.remove('atk-drawer-open');
        if (this._drawerView) {
            this._setSelected(this._drawerView, null);
            this._drawerView = null;
        }
    },

    // ============================
    // Export
    // ============================

    // ATT&CK Navigator layer (v4.5) of what these events touched, so the same
    // picture can be opened in MITRE's own tool or dropped into a report.
    _exportLayer(view) {
        const scope = window.FractalContext?.getCurrentFractal?.()?.name || 'Bifract';
        const techniques = [];
        for (const [id, c] of view.state.techniques) {
            if (!c.total) continue;
            techniques.push({
                techniqueID: id,
                score: c.total,
                comment: c.inherited > 0
                    ? `${c.direct} direct, ${c.inherited} via sub-techniques`
                    : `${c.direct} events`,
                enabled: true,
            });
        }
        if (!techniques.length) {
            // The anonymous wallboard has no Toast; the button is simply inert there.
            window.Toast?.show('No ATT&CK techniques in these results to export', 'warning');
            return;
        }

        const layer = {
            name: `${scope} observed ATT&CK activity`,
            description: `Bifract query results, ${techniques.length} techniques observed`,
            domain: this.matrix.domain || 'enterprise-attack',
            versions: {
                attack: String(this.matrix.version || '').split('.')[0],
                navigator: '5.1.0',
                layer: '4.5',
            },
            techniques,
            gradient: {
                colors: ['#2d2d4a', '#9c6ade'],
                minValue: 0,
                maxValue: Math.max(1, ...techniques.map(t => t.score)),
            },
            legendItems: [{ label: 'Events observed', color: '#9c6ade' }],
            showSubtechniques: true,
            sorting: 0,
        };

        const blob = new Blob([JSON.stringify(layer, null, 2)], { type: 'application/json' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `${String(scope).toLowerCase().replace(/[^a-z0-9]+/g, '-')}-attack-observed.json`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(a.href);
    },
};

window.BifractMitreMatrix = BifractMitreMatrix;

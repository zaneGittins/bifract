// MITRE ATT&CK coverage map (Alerts -> Coverage).
//
// Sigma rules already carry attack.* tags in alerts.labels; the server reads them
// back against the embedded ATT&CK matrix. This module renders the result as the
// familiar tactic-column grid, heat-mapped by how many rules cover each technique,
// so gaps are visible at a glance rather than buried in a label filter.
//
// The grid is built once from the matrix and then only recoloured. Rebuilding
// ~700 cells on every filter keystroke is what makes coverage maps feel sluggish.

const AttackCoverage = {
    matrix: null,
    coverage: null,
    cells: new Map(),      // techniqueID -> cell button element
    built: false,
    loadGen: 0,
    reloadTimer: null,
    selectedId: null,

    filters: {
        // Server-side: these change the denominators, so they need a refetch.
        enabledOnly: false,
        severity: '',
        feedId: '',
        platform: '',
        // Client-side only.
        colorBy: 'count',
        coverage: 'all',
        showSubs: false,
        search: '',
    },

    init() {
        if (window.FractalContext) {
            FractalContext.subscribe('AttackCoverage', () => this.onFractalChange());
        }
    },

    onFractalChange() {
        // Coverage is per-fractal. Drop everything so the previous scope's grid
        // never flashes into view on tab re-entry.
        this.coverage = null;
        this.built = false;
        this.cells.clear();
        this.closeDrawer();
        const grid = document.getElementById('atkMatrix');
        if (grid) grid.innerHTML = '';
        const view = document.getElementById('attackCoverageView');
        if (view && view.style.display !== 'none') this.show();
    },

    async show() {
        const view = document.getElementById('attackCoverageView');
        if (!view) return;
        this.renderShell();
        await this.load();
    },

    hide() {
        this.closeDrawer();
        this.hideTip();
    },

    // ============================
    // Data
    // ============================

    // Loads are superseded rather than dropped. An in-flight guard looks safer but
    // is worse: a fractal switch that lands mid-load would be discarded and leave
    // the grid empty with nothing to retrigger it.
    async load() {
        const gen = ++this.loadGen;
        const stale = () => gen !== this.loadGen;

        this.setStatus('Loading coverage...');
        try {
            if (!this.matrix) {
                const res = await HttpUtils.safeFetch('/api/v1/attack/matrix');
                if (stale()) return;
                if (!res.success) throw new Error(res.error || 'Failed to load ATT&CK matrix');
                this.matrix = res.data;
                this.populatePlatformFilter();
            }
            await this.populateFeedFilter();
            if (stale()) return;

            const res = await HttpUtils.safeFetch('/api/v1/attack/coverage?' + this.queryString());
            if (stale()) return;
            if (!res.success) throw new Error(res.error || 'Failed to load coverage');
            this.coverage = res.data;

            if (!this.built) this.buildGrid();
            this.paint();
            this.renderSummary();
            this.setStatus('');

            // Gaps are a second, heavier query. Loading them after the grid is
            // painted keeps the matrix from waiting on them.
            const gapRes = await HttpUtils.safeFetch('/api/v1/attack/gaps?' + this.queryString());
            if (stale()) return;
            if (gapRes.success) this.renderGaps(gapRes.data);
        } catch (err) {
            if (!stale()) this.setStatus(err.message || 'Failed to load coverage');
        }
    },

    queryString() {
        const p = new URLSearchParams();
        if (this.filters.enabledOnly) p.set('enabled_only', 'true');
        if (this.filters.severity) p.set('severity', this.filters.severity);
        if (this.filters.feedId) p.set('feed_id', this.filters.feedId);
        if (this.filters.platform) p.set('platform', this.filters.platform);
        return p.toString();
    },

    // Server-side filters change denominators, so they refetch. Debounced because
    // the severity/platform selects are easy to scrub through.
    scheduleReload() {
        clearTimeout(this.reloadTimer);
        this.reloadTimer = setTimeout(() => this.load(), 150);
    },

    // ============================
    // Shell
    // ============================

    renderShell() {
        const view = document.getElementById('attackCoverageView');
        if (view.dataset.built === 'true') return;
        view.dataset.built = 'true';

        view.innerHTML = `
            <section class="atk-view">
                <div class="atk-summary" id="atkSummary"></div>

                <div class="atk-controls">
                    <input type="text" id="atkSearch" class="atk-search" placeholder="Search technique or ID..." />
                    <select id="atkCoverage" class="atk-select" title="Show everything, only what is covered, or only the gaps">
                        <option value="all">All techniques</option>
                        <option value="gaps">Gaps only</option>
                        <option value="covered">Covered only</option>
                    </select>
                    <select id="atkColorBy" class="atk-select" title="What the cell colour encodes">
                        <option value="count">Colour: rule count</option>
                        <option value="enabled">Colour: enabled rules</option>
                        <option value="severity">Colour: highest severity</option>
                    </select>
                    <select id="atkSeverity" class="atk-select">
                        <option value="">All severities</option>
                        <option value="critical">Critical</option>
                        <option value="high">High</option>
                        <option value="medium">Medium</option>
                        <option value="low">Low</option>
                        <option value="info">Info</option>
                    </select>
                    <select id="atkPlatform" class="atk-select">
                        <option value="">All platforms</option>
                    </select>
                    <select id="atkFeed" class="atk-select">
                        <option value="">All rules</option>
                        <option value="none">Manual rules only</option>
                    </select>
                    <label class="atk-toggle"><input type="checkbox" id="atkEnabledOnly" /> Enabled only</label>
                    <label class="atk-toggle"><input type="checkbox" id="atkShowSubs" /> Sub-techniques</label>
                    <div class="atk-controls-end">
                        <div class="atk-legend" id="atkLegend"></div>
                        <button class="btn-secondary btn-sm" id="atkExportBtn" title="Download as an ATT&amp;CK Navigator layer">Export layer</button>
                    </div>
                </div>

                <div class="atk-empty" id="atkStatus"></div>
                <div class="atk-matrix-wrap"><div class="atk-matrix" id="atkMatrix"></div></div>

                <div class="atk-gaps" id="atkGaps"></div>
            </section>

            <div class="atk-drawer-scrim" id="atkDrawerScrim"></div>
            <aside class="atk-drawer" id="atkDrawer" role="dialog" aria-modal="true" aria-label="Technique detail">
                <div class="atk-drawer-head">
                    <div class="atk-drawer-title">
                        <div class="atk-tip-id" id="atkDrawerId"></div>
                        <h3 id="atkDrawerName"></h3>
                    </div>
                    <button class="atk-drawer-close" id="atkDrawerClose" aria-label="Close">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M18 6L6 18M6 6l12 12"/>
                        </svg>
                    </button>
                </div>
                <div class="atk-drawer-body" id="atkDrawerBody"></div>
            </aside>
            <div class="atk-tip" id="atkTip"></div>
        `;

        this.bindControls();
        this.renderLegend();
    },

    // The legend follows the colour mode: a magnitude ramp and a status palette
    // are different encodings and must not share a key.
    renderLegend() {
        const el = document.getElementById('atkLegend');
        if (!el) return;

        if (this.filters.colorBy === 'severity') {
            const levels = [
                ['critical', 'Critical'], ['high', 'High'], ['medium', 'Medium'],
                ['low', 'Low'], ['info', 'Info'],
            ];
            el.innerHTML = '<span>highest severity</span><div class="atk-legend-swatches">' +
                levels.map(([key, label]) =>
                    `<div class="atk-legend-swatch"><span class="severity-dot severity-dot-${key}" title="${label}"></span></div>`).join('') +
                '</div>';
            return;
        }

        const buckets = ['No rules', '1', '2-3', '4-7', '8-15', '16+'];
        el.innerHTML = '<span>none</span><div class="atk-legend-swatches">' +
            buckets.map((label, i) =>
                `<div class="atk-legend-swatch" data-heat="${i}" title="${Utils.escapeHtml(label)}"></div>`).join('') +
            '</div><span>more</span>';
    },

    bindControls() {
        const on = (id, evt, fn) => document.getElementById(id)?.addEventListener(evt, fn);

        on('atkSearch', 'input', (e) => {
            this.filters.search = e.target.value.trim().toLowerCase();
            this.applyClientFilters();
        });
        on('atkColorBy', 'change', (e) => {
            this.filters.colorBy = e.target.value;
            this.renderLegend();
            this.paint();
        });
        on('atkCoverage', 'change', (e) => {
            this.filters.coverage = e.target.value;
            this.applyClientFilters();
        });
        on('atkSeverity', 'change', (e) => {
            this.filters.severity = e.target.value;
            this.scheduleReload();
        });
        on('atkPlatform', 'change', (e) => {
            this.filters.platform = e.target.value;
            this.scheduleReload();
        });
        on('atkFeed', 'change', (e) => {
            this.filters.feedId = e.target.value;
            this.scheduleReload();
        });
        on('atkEnabledOnly', 'change', (e) => {
            this.filters.enabledOnly = e.target.checked;
            this.scheduleReload();
        });
        on('atkShowSubs', 'change', (e) => {
            this.filters.showSubs = e.target.checked;
            this.toggleAllSubs(e.target.checked);
        });
        on('atkExportBtn', 'click', () => this.exportLayer());
        on('atkDrawerClose', 'click', () => this.closeDrawer());
        on('atkDrawerScrim', 'click', () => this.closeDrawer());

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && document.getElementById('atkDrawer')?.classList.contains('open')) {
                this.closeDrawer();
            }
        });
    },

    populatePlatformFilter() {
        const select = document.getElementById('atkPlatform');
        if (!select || !this.matrix?.platforms) return;
        const options = [...this.matrix.platforms].sort();
        select.innerHTML = '<option value="">All platforms</option>' +
            options.map(p => `<option value="${Utils.escapeHtml(p)}">${Utils.escapeHtml(p)}</option>`).join('');
    },

    // Feeds are scope-dependent, so this refreshes with the fractal rather than
    // being cached alongside the matrix.
    async populateFeedFilter() {
        const select = document.getElementById('atkFeed');
        if (!select) return;
        try {
            const res = await HttpUtils.safeFetch('/api/v1/feeds');
            const feeds = res?.data?.feeds || res?.data || [];
            const current = this.filters.feedId;
            select.innerHTML = '<option value="">All rules</option><option value="none">Manual rules only</option>' +
                feeds.map(f => `<option value="${Utils.escapeHtml(f.id)}">${Utils.escapeHtml(f.name)}</option>`).join('');
            select.value = feeds.some(f => f.id === current) || current === 'none' ? current : '';
            this.filters.feedId = select.value;
        } catch {
            // A missing feed list is not fatal: the rest of the map still works.
        }
    },

    setStatus(text) {
        const el = document.getElementById('atkStatus');
        if (!el) return;
        el.textContent = text || '';
        el.style.display = text ? 'block' : 'none';
    },

    // ============================
    // Grid
    // ============================

    buildGrid() {
        const grid = document.getElementById('atkMatrix');
        if (!grid || !this.matrix) return;

        const subsOf = new Map();
        const byTactic = new Map();
        for (const tech of this.matrix.techniques) {
            if (tech.deprecated) continue;
            if (tech.sub) {
                if (!subsOf.has(tech.parent)) subsOf.set(tech.parent, []);
                subsOf.get(tech.parent).push(tech);
                continue;
            }
            for (const short of tech.tactics || []) {
                if (!byTactic.has(short)) byTactic.set(short, []);
                byTactic.get(short).push(tech);
            }
        }
        for (const list of byTactic.values()) list.sort((a, b) => a.name.localeCompare(b.name));
        for (const list of subsOf.values()) list.sort((a, b) => a.id.localeCompare(b.id));

        const frag = document.createDocumentFragment();
        this.cells.clear();

        for (const tactic of this.matrix.tactics) {
            const column = document.createElement('div');
            column.className = 'atk-column';
            column.dataset.tactic = tactic.short;

            const head = document.createElement('div');
            head.className = 'atk-col-head';
            head.innerHTML = `
                <div class="atk-col-name">${Utils.escapeHtml(tactic.name)}</div>
                <div class="atk-col-count" data-tactic-count="${Utils.escapeHtml(tactic.short)}">-</div>
                <div class="atk-meter"><div class="atk-meter-fill" data-tactic-meter="${Utils.escapeHtml(tactic.short)}" style="width:0%"></div></div>
            `;
            column.appendChild(head);

            const body = document.createElement('div');
            body.className = 'atk-col-body';
            for (const tech of byTactic.get(tactic.short) || []) {
                const subs = subsOf.get(tech.id) || [];
                body.appendChild(this.buildCell(tech, subs));
                if (subs.length) {
                    const wrap = document.createElement('div');
                    wrap.className = 'atk-subs';
                    wrap.dataset.subsOf = `${tactic.short}:${tech.id}`;
                    for (const sub of subs) wrap.appendChild(this.buildCell(sub, []));
                    body.appendChild(wrap);
                }
            }
            column.appendChild(body);
            frag.appendChild(column);
        }

        grid.innerHTML = '';
        grid.appendChild(frag);
        this.built = true;
    },

    // A technique can sit in more than one tactic column, so cells are tracked as
    // a list per ID and painted together.
    buildCell(tech, subs) {
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
                <span class="atk-cell-name">${Utils.escapeHtml(tech.name)}</span>
                <span class="atk-badge" data-badge></span>
            </div>
            <div class="atk-cell-meta" data-meta>${Utils.escapeHtml(tech.id)}</div>
        `;

        btn.addEventListener('click', (e) => {
            if (e.target.closest('.atk-sub-toggle')) {
                e.stopPropagation();
                this.toggleSubs(btn);
                return;
            }
            this.openDrawer(tech.id);
        });
        btn.addEventListener('mouseenter', (e) => this.showTip(e, tech));
        btn.addEventListener('mousemove', (e) => this.moveTip(e));
        btn.addEventListener('mouseleave', () => this.hideTip());
        btn.addEventListener('focus', (e) => this.showTip(e, tech));
        btn.addEventListener('blur', () => this.hideTip());

        if (!this.cells.has(tech.id)) this.cells.set(tech.id, []);
        this.cells.get(tech.id).push(btn);
        return btn;
    },

    toggleSubs(cellEl) {
        const wrap = cellEl.nextElementSibling;
        if (!wrap || !wrap.classList.contains('atk-subs')) return;
        const open = wrap.classList.toggle('atk-open');
        cellEl.classList.toggle('atk-open', open);
    },

    toggleAllSubs(open) {
        document.querySelectorAll('#atkMatrix .atk-subs').forEach(wrap => {
            wrap.classList.toggle('atk-open', open);
            wrap.previousElementSibling?.classList.toggle('atk-open', open);
        });
    },

    // ============================
    // Painting
    // ============================

    // Fixed thresholds, not a relative max: a scale that rescales on every filter
    // change makes two screenshots of the same deployment incomparable.
    heatLevel(count) {
        if (count <= 0) return 0;
        if (count === 1) return 1;
        if (count <= 3) return 2;
        if (count <= 7) return 3;
        if (count <= 15) return 4;
        return 5;
    },

    paint() {
        if (!this.coverage) return;
        const byId = this.coverage.techniques || {};
        const mode = this.filters.colorBy;

        for (const [id, els] of this.cells) {
            const cell = byId[id];
            const count = !cell ? 0 : (mode === 'enabled' ? cell.enabled : cell.total || 0);
            const heat = this.heatLevel(count);
            const sev = mode === 'severity' && cell && cell.total > 0 ? (cell.max_severity || '') : '';

            for (const el of els) {
                if (sev) {
                    el.dataset.sev = sev;
                    delete el.dataset.heat;
                } else {
                    delete el.dataset.sev;
                    if (heat > 0) el.dataset.heat = String(heat);
                    else delete el.dataset.heat;
                }

                const badge = el.querySelector('[data-badge]');
                if (badge) badge.textContent = count > 0 ? String(count) : '';

                const meta = el.querySelector('[data-meta]');
                if (meta) {
                    let text = id;
                    if (cell && cell.subs_total > 0) text += ` · ${cell.subs_covered || 0}/${cell.subs_total} sub`;
                    meta.textContent = text;
                }
            }
        }

        this.paintTacticHeaders();
        this.applyClientFilters();
    },

    paintTacticHeaders() {
        const per = this.coverage?.summary?.per_tactic || {};
        for (const tactic of this.matrix.tactics) {
            const stats = per[tactic.short] || { total: 0, covered: 0 };
            const pct = stats.total ? Math.round((stats.covered / stats.total) * 100) : 0;
            const label = document.querySelector(`[data-tactic-count="${tactic.short}"]`);
            if (label) label.textContent = `${stats.covered}/${stats.total} · ${pct}%`;
            const meter = document.querySelector(`[data-tactic-meter="${tactic.short}"]`);
            if (meter) meter.style.width = pct + '%';
        }
    },

    // Non-matching cells are dimmed rather than removed, so the column geometry
    // holds still and the eye keeps its sense of where matches sit in the kill
    // chain. A search that only matches a sub-technique expands its parent, or the
    // match would be hidden inside a collapsed group.
    applyClientFilters() {
        const term = this.filters.search;
        const mode = this.filters.coverage;
        const byId = this.coverage?.techniques || {};
        const expand = new Set();

        for (const [id, els] of this.cells) {
            const cell = byId[id];
            const covered = (cell?.total || 0) > 0;

            let match = !term || id.toLowerCase().includes(term) ||
                (els[0]?.dataset.name || '').includes(term);
            if (match && mode === 'gaps' && covered) match = false;
            if (match && mode === 'covered' && !covered) match = false;

            for (const el of els) {
                el.classList.toggle('atk-dim', !match);
                if (match) {
                    const wrap = el.closest('.atk-subs');
                    if (wrap) expand.add(wrap);
                }
            }
        }

        if (term || mode !== 'all') {
            for (const wrap of expand) {
                wrap.classList.add('atk-open');
                wrap.previousElementSibling?.classList.add('atk-open');
            }
        } else if (!this.filters.showSubs) {
            this.toggleAllSubs(false);
        }
    },

    // ============================
    // Summary
    // ============================

    renderSummary() {
        const el = document.getElementById('atkSummary');
        const s = this.coverage?.summary;
        if (!el || !s) return;

        const pct = s.techniques_total ? Math.round((s.techniques_covered / s.techniques_total) * 100) : 0;
        const subPct = s.subtechniques_total ? Math.round((s.subtechniques_covered / s.subtechniques_total) * 100) : 0;
        const tacticName = (short) => this.matrix.tactics.find(t => t.short === short)?.name || short;
        const weak = (s.weakest_tactics || []).map(tacticName);
        const weakStats = s.per_tactic?.[(s.weakest_tactics || [])[0]];
        const weakSub = weak.length > 1 ? 'then ' + weak.slice(1).join(', ') : 'No tactic data';

        el.innerHTML = `
            ${this.statCard('Technique coverage', `${s.techniques_covered}/${s.techniques_total}`, `${pct}% of ATT&CK v${s.matrix_version || ''}`, pct)}
            ${this.statCard('Sub-technique coverage', `${s.subtechniques_covered}/${s.subtechniques_total}`, `${subPct}% covered directly`, subPct)}
            ${this.statCard('Weakest tactic', weak[0] || '-', weakStats ? `${weakStats.covered}/${weakStats.total} covered · ${weakSub}` : weakSub)}
            ${this.statCard('Rules mapped', `${s.rules_mapped}/${s.rules_total}`, `${s.rules_unmapped} rule(s) carry no technique tag`, null, s.rules_unmapped > 0)}
            ${this.statCard('Retired tags', String(s.rules_retired_tag || 0), (s.retired_tags || []).slice(0, 4).join(', ') || 'None', null, (s.rules_retired_tag || 0) > 0)}
        `;
    },

    statCard(label, value, sub, meterPct = null, warn = false) {
        const meter = meterPct === null ? '' :
            `<div class="atk-meter"><div class="atk-meter-fill" style="width:${meterPct}%"></div></div>`;
        return `
            <div class="atk-stat">
                <div class="atk-stat-label">${Utils.escapeHtml(label)}</div>
                <div class="atk-stat-value${warn ? ' atk-warn' : ''}">${Utils.escapeHtml(value)}</div>
                <div class="atk-stat-sub" title="${Utils.escapeHtml(sub)}">${Utils.escapeHtml(sub)}</div>
                ${meter}
            </div>
        `;
    },

    // ============================
    // Gaps
    // ============================

    SKIP_REASONS: {
        min_level: 'below the feed severity threshold',
        min_status: 'below the feed maturity threshold',
        translate_error: 'cannot be translated to BQL',
        parse_error: 'cannot be parsed',
        create_error: 'failed to import',
    },

    // Ranks uncovered techniques by what can actually be done about them today.
    // "No coverage" is a fact; "SigmaHQ has 12 rules for this, all filtered out by
    // min_level" is a decision.
    renderGaps(data) {
        const el = document.getElementById('atkGaps');
        if (!el) return;

        const gaps = data.gaps || [];
        if (!gaps.length) {
            el.innerHTML = '';
            return;
        }

        const hint = data.catalog_populated
            ? `${data.candidate_rules} rule(s) in your feeds are not imported.`
            : 'Sync a feed to see which of its rules could close these gaps.';

        el.innerHTML = `
            <div class="atk-gaps-head">
                <h3>Top gaps</h3>
                <span class="atk-gaps-hint">${data.uncovered_total} uncovered technique(s). ${Utils.escapeHtml(hint)}</span>
            </div>
            <div class="atk-gaps-list">
                ${gaps.map(g => this.gapRow(g)).join('')}
            </div>
        `;

        el.querySelectorAll('.atk-gap').forEach(row => {
            row.addEventListener('click', () => this.openDrawer(row.dataset.tid));
        });
    },

    gapRow(gap) {
        const tacticNames = (gap.tactics || [])
            .map(short => this.matrix.tactics.find(t => t.short === short)?.name || short)
            .join(', ');

        const reasons = Object.entries(gap.by_reason || {})
            .map(([reason, n]) => `${n} ${this.SKIP_REASONS[reason] || reason}`)
            .join(' · ');

        const action = gap.available > 0
            ? `<span class="atk-gap-action">${gap.available} rule(s) available</span>`
            : '<span class="atk-gap-action atk-gap-build">Needs a new rule</span>';

        const detail = gap.available > 0
            ? reasons
            : ((gap.log_sources || []).slice(0, 3).join(', ') || 'No telemetry guidance from MITRE');

        return `
            <button class="atk-gap" data-tid="${Utils.escapeHtml(gap.technique_id)}">
                <span class="atk-gap-id">${Utils.escapeHtml(gap.technique_id)}</span>
                <span class="atk-gap-main">
                    <span class="atk-gap-name">${Utils.escapeHtml(gap.name)}</span>
                    <span class="atk-gap-detail">${Utils.escapeHtml(detail)}</span>
                </span>
                <span class="atk-gap-tactics">${Utils.escapeHtml(tacticNames)}</span>
                ${action}
            </button>
        `;
    },

    // ============================
    // Tooltip
    // ============================

    showTip(event, tech) {
        const tip = document.getElementById('atkTip');
        if (!tip) return;
        const cell = this.coverage?.techniques?.[tech.id];
        const total = cell?.total || 0;

        let detail = 'No rules cover this technique';
        if (total > 0) {
            detail = `${total} rule(s), ${cell.enabled} enabled`;
            if (cell.inherited > 0) detail += ` (${cell.direct} direct, ${cell.inherited} via sub-techniques)`;
        }
        const subs = cell && cell.subs_total > 0
            ? `<div class="atk-tip-line">${cell.subs_covered || 0}/${cell.subs_total} sub-techniques covered</div>` : '';

        tip.innerHTML = `
            <div class="atk-tip-id">${Utils.escapeHtml(tech.id)}</div>
            <div>${Utils.escapeHtml(tech.name)}</div>
            <div class="atk-tip-line">${Utils.escapeHtml(detail)}</div>
            ${subs}
        `;
        tip.classList.add('open');
        this.moveTip(event);
    },

    moveTip(event) {
        const tip = document.getElementById('atkTip');
        if (!tip || !tip.classList.contains('open')) return;
        const pad = 14;
        const rect = tip.getBoundingClientRect();
        const x = event.clientX ?? 0;
        const y = event.clientY ?? 0;
        let left = x + pad;
        let top = y + pad;
        if (left + rect.width > window.innerWidth - 8) left = x - rect.width - pad;
        if (top + rect.height > window.innerHeight - 8) top = y - rect.height - pad;
        tip.style.left = Math.max(8, left) + 'px';
        tip.style.top = Math.max(8, top) + 'px';
    },

    hideTip() {
        document.getElementById('atkTip')?.classList.remove('open');
    },

    // ============================
    // Drawer
    // ============================

    async openDrawer(techniqueId) {
        const drawer = document.getElementById('atkDrawer');
        const scrim = document.getElementById('atkDrawerScrim');
        const body = document.getElementById('atkDrawerBody');
        if (!drawer || !body) return;

        this.hideTip();
        this.setSelected(techniqueId);
        drawer.classList.add('open');
        scrim?.classList.add('open');
        document.body.classList.add('atk-drawer-open');
        body.innerHTML = '<div class="atk-empty">Loading...</div>';

        const params = new URLSearchParams(this.queryString());
        params.set('include_sub', 'true');

        try {
            const res = await HttpUtils.safeFetch(
                `/api/v1/attack/techniques/${encodeURIComponent(techniqueId)}/rules?${params.toString()}`);
            if (!res.success) throw new Error(res.error || 'Failed to load technique');

            // Only an uncovered technique needs the gap section, so it costs a
            // second request only when it will actually be shown.
            let gap = null;
            if (!(res.data.rules || []).length) {
                const gapRes = await HttpUtils.safeFetch(
                    `/api/v1/attack/techniques/${encodeURIComponent(techniqueId)}/gap`);
                if (gapRes.success) gap = gapRes.data;
            }
            this.renderDrawer(res.data, gap);
        } catch (err) {
            body.innerHTML = `<div class="atk-empty">${Utils.escapeHtml(err.message || 'Failed to load technique')}</div>`;
        }
    },

    renderDrawer(data, gap) {
        const tech = data.technique || {};
        document.getElementById('atkDrawerId').textContent = tech.id || '';
        document.getElementById('atkDrawerName').textContent = tech.name || '';

        const chips = (values) => (values || []).length
            ? `<div class="atk-chips">${values.map(v => `<span class="atk-chip">${Utils.escapeHtml(v)}</span>`).join('')}</div>`
            : '<div class="atk-empty">Not specified</div>';

        const tacticNames = (tech.tactics || []).map(short =>
            this.matrix.tactics.find(t => t.short === short)?.name || short);

        const rules = data.rules || [];
        const rulesHtml = rules.length
            ? rules.map(r => `
                <button class="atk-rule${r.enabled ? '' : ' atk-rule-off'}" data-rule-id="${Utils.escapeHtml(r.id)}">
                    <span class="severity-dot severity-dot-${Utils.escapeHtml(r.severity || 'medium')}"></span>
                    <span class="atk-rule-name" title="${Utils.escapeHtml(r.name)}">${Utils.escapeHtml(r.name)}</span>
                    <span class="atk-rule-meta">${r.enabled ? 'on' : 'off'}${r.feed_name ? ' · ' + Utils.escapeHtml(r.feed_name) : ''}</span>
                </button>`).join('')
            : `<div class="atk-note">No rule in this fractal maps to ${Utils.escapeHtml(tech.id || '')}.
                   MITRE expects this technique to show up in the telemetry listed below, so that is where a
                   new detection would start.</div>`;

        document.getElementById('atkDrawerBody').innerHTML = `
            <div class="atk-drawer-section">
                <h4>Tactics</h4>
                ${chips(tacticNames)}
            </div>
            <div class="atk-drawer-section">
                <h4>Covering rules (${rules.length})</h4>
                ${rulesHtml}
            </div>
            ${this.gapSection(gap)}
            <div class="atk-drawer-section">
                <h4>Platforms</h4>
                ${chips(data.platforms)}
            </div>
            <div class="atk-drawer-section">
                <h4>Expected telemetry</h4>
                ${chips(data.log_sources)}
            </div>
            <div class="atk-drawer-section">
                <a class="atk-link" href="${Utils.escapeHtml(data.url || '#')}" target="_blank" rel="noopener noreferrer">
                    View ${Utils.escapeHtml(tech.id || '')} on attack.mitre.org
                </a>
            </div>
        `;

        document.querySelectorAll('#atkDrawerBody .atk-rule').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.dataset.ruleId;
                this.closeDrawer();
                if (window.Alerts) Alerts.showAlertEditor(id);
            });
        });
    },

    // Lists the rules a configured feed already offers for an uncovered technique,
    // with the reason each one is not running. Nothing is imported behind the
    // operator's back: a threshold they set is a decision, not an accident.
    gapSection(gap) {
        if (!gap || !(gap.candidates || []).length) return '';

        const rows = gap.candidates.map(c => `
            <div class="atk-candidate">
                <div class="atk-candidate-top">
                    <span class="atk-candidate-title" title="${Utils.escapeHtml(c.title || c.path)}">${Utils.escapeHtml(c.title || c.path)}</span>
                    <span class="atk-chip">${Utils.escapeHtml(c.level || 'unrated')}</span>
                </div>
                <div class="atk-candidate-meta">
                    ${Utils.escapeHtml(c.feed_name)} · ${Utils.escapeHtml(this.SKIP_REASONS[c.skip_reason] || c.skip_reason)}
                </div>
            </div>
        `).join('');

        const more = gap.available > gap.candidates.length
            ? `<div class="atk-empty">and ${gap.available - gap.candidates.length} more</div>` : '';

        return `
            <div class="atk-drawer-section">
                <h4>Available in your feeds (${gap.available})</h4>
                <div class="atk-note">These rules exist in a configured feed but were not imported.
                    Lower the feed's severity or maturity threshold to pull them in, or fix the
                    translation for the ones Bifract cannot express.</div>
                ${rows}
                ${more}
            </div>
        `;
    },

    setSelected(techniqueId) {
        if (this.selectedId) {
            (this.cells.get(this.selectedId) || []).forEach(el => el.classList.remove('atk-selected'));
        }
        this.selectedId = techniqueId;
        (this.cells.get(techniqueId) || []).forEach(el => el.classList.add('atk-selected'));
    },

    closeDrawer() {
        document.getElementById('atkDrawer')?.classList.remove('open');
        document.getElementById('atkDrawerScrim')?.classList.remove('open');
        document.body.classList.remove('atk-drawer-open');
        if (this.selectedId) {
            (this.cells.get(this.selectedId) || []).forEach(el => el.classList.remove('atk-selected'));
            this.selectedId = null;
        }
    },

    // ============================
    // Export
    // ============================

    // Downloads an ATT&CK Navigator layer so coverage can be opened in MITRE's own
    // tool and diffed against other sources.
    exportLayer() {
        const params = new URLSearchParams(this.queryString());
        const scope = window.FractalContext?.getCurrentFractal?.()?.name
            || window.FractalContext?.currentFractalName
            || 'Bifract';
        params.set('scope_name', scope);

        // An anchor click keeps the SPA on its current hash; assigning
        // location.href would leave the app mid-navigation if the download stalls.
        const a = document.createElement('a');
        a.href = '/api/v1/attack/layer?' + params.toString();
        a.download = '';
        document.body.appendChild(a);
        a.click();
        a.remove();
    },
};

window.AttackCoverage = AttackCoverage;

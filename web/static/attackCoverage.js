// MITRE ATT&CK coverage map (Alerts -> Coverage).
//
// Sigma rules already carry attack.* tags in alerts.labels; the server reads them
// back against the embedded ATT&CK matrix. This module renders the result as the
// familiar tactic-column grid, heat-mapped by how many rules cover each technique,
// so gaps are visible at a glance rather than buried in a label filter.
//
// The grid is built once from the matrix and then only recoloured. Rebuilding
// ~700 cells on every filter keystroke is what makes coverage maps feel sluggish.
//
// Cells are coloured by status by default: covered and live, covered but every
// rule disabled, uncovered with rules waiting in a synced feed, or uncovered.
// That last distinction is what a ranked gap list used to say in prose, and it
// reads better on the map, where the operator is already looking.

const AttackCoverage = {
    matrix: null,
    coverage: null,
    cells: new Map(),      // techniqueID -> cell button element
    built: false,
    loadGen: 0,
    reloadTimer: null,
    autoExpanded: new Set(),
    selectedId: null,

    filters: {
        // Server-side: these change the denominators, so they need a refetch.
        enabledOnly: false,
        severity: '',
        feedId: '',
        platform: '',
        // Client-side only.
        colorBy: 'status',
        coverage: 'all',
        only: '',            // '', 'close' or 'off', set by the attention chips
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
        this.autoExpanded.clear();
        this.closeDrawer();
        const grid = document.getElementById('atkMatrix');
        if (grid) grid.innerHTML = '';
        if (FractalContext.shouldReload('attackCoverageView')) this.show();
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
            this.renderSummary();
            this.paint();
            this.setStatus('');
            // The summary strip above the grid is only final now, and it decides
            // where the grid starts.
            this.fitGrid();
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
                    <select id="atkColorBy" class="atk-select" title="What the cell color encodes">
                        <option value="status">Color: status</option>
                        <option value="count">Color: rule count</option>
                        <option value="severity">Color: highest severity</option>
                    </select>

                    <!-- Scope filters are occasional, so they fold into one control
                         rather than spending five slots in the toolbar. -->
                    <div class="atk-filters">
                        <button type="button" class="atk-select atk-filters-btn" id="atkFiltersBtn" aria-expanded="false">
                            Filters<span class="atk-filters-count" id="atkFiltersCount"></span>
                        </button>
                        <div class="atk-filters-menu" id="atkFiltersMenu">
                            <label class="atk-field">Show
                                <select id="atkCoverage" class="atk-select">
                                    <option value="all">All techniques</option>
                                    <option value="gaps">Gaps only</option>
                                    <option value="covered">Covered only</option>
                                </select>
                            </label>
                            <label class="atk-field">Severity
                                <select id="atkSeverity" class="atk-select">
                                    <option value="">All severities</option>
                                    <option value="critical">Critical</option>
                                    <option value="high">High</option>
                                    <option value="medium">Medium</option>
                                    <option value="low">Low</option>
                                    <option value="info">Info</option>
                                </select>
                            </label>
                            <label class="atk-field">Platform
                                <select id="atkPlatform" class="atk-select">
                                    <option value="">All platforms</option>
                                </select>
                            </label>
                            <label class="atk-field">Source
                                <select id="atkFeed" class="atk-select">
                                    <option value="">All rules</option>
                                    <option value="none">Manual rules only</option>
                                </select>
                            </label>
                            <label class="atk-toggle"><input type="checkbox" id="atkEnabledOnly" /> Enabled rules only</label>
                            <button type="button" class="atk-filters-clear" id="atkFiltersClear">Clear filters</button>
                        </div>
                    </div>

                    <label class="atk-toggle"><input type="checkbox" id="atkShowSubs" /> Sub-techniques</label>
                    <div class="atk-controls-end">
                        <div class="atk-legend" id="atkLegend"></div>
                        <button class="btn-secondary btn-sm" id="atkExportBtn" title="Download this coverage as an ATT&amp;CK Navigator layer (.json) to open at mitre-attack.github.io/attack-navigator, share with people who do not have Bifract access, or diff against another tool's coverage">Export layer</button>
                    </div>
                </div>

                <div class="atk-empty" id="atkStatus"></div>
                <div class="atk-matrix-wrap"><div class="atk-matrix" id="atkMatrix"></div></div>
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
                <div class="atk-drawer-actions">
                    <button class="btn-primary btn-sm" id="atkDrawerWrite">Write a detection</button>
                    <a class="btn-secondary btn-sm" id="atkDrawerMitre" target="_blank" rel="noopener noreferrer">attack.mitre.org</a>
                </div>
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

        if (this.filters.colorBy === 'status') {
            const states = [
                ['live', 'Live', 'At least one enabled rule maps here'],
                ['off', 'Disabled only', 'Every rule mapped here is switched off'],
                ['close', 'Feed can close', 'No rule here, but a synced feed carries one that was not imported'],
                ['none', 'No coverage', 'Nothing here, and nothing waiting in a feed'],
            ];
            el.innerHTML = states.map(([key, label, tip]) =>
                `<span class="atk-legend-key" title="${Utils.escapeHtml(tip)}"><i class="atk-legend-swatch" data-state="${key}"></i>${Utils.escapeHtml(label)}</span>`).join('');
            return;
        }

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
            this.updateFilterCount();
            this.applyClientFilters();
        });
        on('atkSeverity', 'change', (e) => {
            this.filters.severity = e.target.value;
            this.updateFilterCount();
            this.scheduleReload();
        });
        on('atkPlatform', 'change', (e) => {
            this.filters.platform = e.target.value;
            this.updateFilterCount();
            this.scheduleReload();
        });
        on('atkFeed', 'change', (e) => {
            this.filters.feedId = e.target.value;
            this.updateFilterCount();
            this.scheduleReload();
        });
        on('atkEnabledOnly', 'change', (e) => {
            this.filters.enabledOnly = e.target.checked;
            this.updateFilterCount();
            this.scheduleReload();
        });
        on('atkShowSubs', 'change', (e) => {
            this.filters.showSubs = e.target.checked;
            this.toggleAllSubs(e.target.checked);
        });
        on('atkExportBtn', 'click', () => this.exportLayer());
        on('atkDrawerWrite', 'click', () => this.writeDetection());
        on('atkDrawerClose', 'click', () => this.closeDrawer());
        on('atkDrawerScrim', 'click', () => this.closeDrawer());

        on('atkFiltersBtn', 'click', (e) => {
            e.stopPropagation();
            this.toggleFilters();
        });
        on('atkFiltersMenu', 'click', (e) => e.stopPropagation());
        on('atkFiltersClear', 'click', () => {
            Object.assign(this.filters, { severity: '', platform: '', feedId: '', enabledOnly: false, coverage: 'all' });
            document.getElementById('atkSeverity').value = '';
            document.getElementById('atkPlatform').value = '';
            document.getElementById('atkFeed').value = '';
            document.getElementById('atkCoverage').value = 'all';
            document.getElementById('atkEnabledOnly').checked = false;
            this.updateFilterCount();
            this.applyClientFilters();
            this.scheduleReload();
        });
        document.addEventListener('click', () => this.toggleFilters(false));

        document.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape') return;
            if (document.getElementById('atkFiltersMenu')?.classList.contains('open')) {
                this.toggleFilters(false);
                return;
            }
            if (document.getElementById('atkDrawer')?.classList.contains('open')) {
                this.closeDrawer();
            }
        });
    },

    toggleFilters(force) {
        const menu = document.getElementById('atkFiltersMenu');
        const btn = document.getElementById('atkFiltersBtn');
        if (!menu || !btn) return;
        const open = force === undefined ? !menu.classList.contains('open') : force;
        menu.classList.toggle('open', open);
        btn.classList.toggle('atk-active', open);
        btn.setAttribute('aria-expanded', String(open));
    },

    // The badge is the only sign a hidden filter is narrowing the numbers, so it
    // must never be out of step with the actual state.
    updateFilterCount() {
        const n = ['severity', 'platform', 'feedId'].filter(k => this.filters[k]).length
            + (this.filters.enabledOnly ? 1 : 0)
            + (this.filters.coverage !== 'all' ? 1 : 0);
        const badge = document.getElementById('atkFiltersCount');
        if (badge) badge.textContent = n ? String(n) : '';
        document.getElementById('atkFiltersBtn')?.classList.toggle('atk-filtered', n > 0);
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
            this.updateFilterCount();
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
                <div class="atk-meter atk-meter-stacked">
                    <div class="atk-meter-fill" data-tactic-meter="${Utils.escapeHtml(tactic.short)}" style="width:0%"></div>
                    <div class="atk-meter-close" data-tactic-close="${Utils.escapeHtml(tactic.short)}" style="width:0%"></div>
                </div>
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

        this.fitGrid();
        if (!this.resizeObserver) {
            this.resizeObserver = new ResizeObserver(() => this.fitGrid());
            const wrap = document.querySelector('.atk-matrix-wrap');
            if (wrap) this.resizeObserver.observe(wrap);
            // A window resized only vertically leaves the grid's own box alone, so
            // the observer never fires and the height would stay stale.
            window.addEventListener('resize', () => this.fitGrid());
        }
    },

    // Both axes: columns spread across the width, and the grid runs down to the
    // bottom bar instead of stopping wherever a fixed viewport budget guessed.
    fitGrid() {
        this.fitColumns();
        Utils.fitBelow(document.querySelector('.atk-matrix-wrap'), 260);
    },

    // Columns get a width names can live in and the matrix scrolls sideways past
    // it. Dividing the viewport by fifteen tactics is what used to break
    // "Administration Command" mid-word, and an unreadable column that fits is
    // worse than a readable one that scrolls.
    COL_WIDTH: 162,

    fitColumns() {
        const wrap = document.querySelector('.atk-matrix-wrap');
        const grid = document.getElementById('atkMatrix');
        const columns = this.matrix?.tactics?.length;
        if (!wrap || !grid || !columns || !wrap.clientWidth) return;

        // Only widen: a viewport with room to spare spends it on the columns
        // rather than on empty gutter, but nothing ever shrinks below the floor.
        const available = wrap.clientWidth - (columns - 1) - 2;
        const width = Math.max(this.COL_WIDTH, Math.floor(available / columns));
        grid.style.setProperty('--atk-col-min', width + 'px');
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

        // No ID row: it costs a full line on every one of ~700 cells, which is most
        // of why the matrix would not fit on screen. The ID lives in the tooltip,
        // the drawer, and the search index instead.
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

    // A cell's status is the question the page exists to answer, so it is what the
    // colour says unless the operator asks for depth (count) or weight (severity).
    cellState(id) {
        const cell = this.coverage?.techniques?.[id];
        const total = cell?.total || 0;
        const available = this.coverage?.candidates?.[id] || 0;
        if (total === 0) return available > 0 ? 'close' : 'none';
        return (cell.enabled || 0) === 0 ? 'off' : 'live';
    },

    paint() {
        if (!this.coverage) return;
        const byId = this.coverage.techniques || {};
        const candidates = this.coverage.candidates || {};
        const mode = this.filters.colorBy;

        for (const [id, els] of this.cells) {
            const cell = byId[id];
            const state = this.cellState(id);
            const count = cell?.total || 0;
            const heat = this.heatLevel(count);
            const sev = mode === 'severity' && cell && cell.total > 0 ? (cell.max_severity || '') : '';

            for (const el of els) {
                delete el.dataset.heat;
                delete el.dataset.sev;
                el.classList.remove('atk-off', 'atk-close');

                let badge = count > 0 ? String(count) : '';
                if (sev) {
                    el.dataset.sev = sev;
                } else if (mode === 'status') {
                    if (state === 'live') el.dataset.heat = String(heat);
                    else if (state === 'off') el.classList.add('atk-off');
                    else if (state === 'close') {
                        el.classList.add('atk-close');
                        badge = String(candidates[id] || 0);
                    }
                } else if (heat > 0) {
                    el.dataset.heat = String(heat);
                }

                const badgeEl = el.querySelector('[data-badge]');
                if (badgeEl) badgeEl.textContent = badge;

                const meta = el.querySelector('[data-meta]');
                if (meta) {
                    meta.textContent = cell && cell.subs_covered > 0
                        ? ` (${cell.subs_covered}/${cell.subs_total})` : '';
                }
            }
        }

        this.paintTacticHeaders();
        this.applyClientFilters();
    },

    // The header leads with leaf coverage: every sub-technique, plus every technique
    // that has none. The generous top-level count, which credits a whole technique
    // for one covered child, stays in the tooltip where it cannot mislead a glance.
    paintTacticHeaders() {
        const per = this.coverage?.summary?.per_tactic || {};
        const closable = this.closableByTactic();

        for (const tactic of this.matrix.tactics) {
            const s = per[tactic.short] || { total: 0, covered: 0, leaf_total: 0, leaf_covered: 0 };
            const leafPct = s.leaf_total ? Math.round((s.leaf_covered / s.leaf_total) * 100) : 0;
            const techPct = s.total ? Math.round((s.covered / s.total) * 100) : 0;
            const close = closable[tactic.short] || 0;
            const closePct = s.leaf_total ? Math.round((close / s.leaf_total) * 100) : 0;

            const label = document.querySelector(`[data-tactic-count="${tactic.short}"]`);
            if (label) label.textContent = `${s.leaf_covered}/${s.leaf_total} · ${leafPct}%`;

            const meter = document.querySelector(`[data-tactic-meter="${tactic.short}"]`);
            if (meter) meter.style.width = leafPct + '%';

            const closeMeter = document.querySelector(`[data-tactic-close="${tactic.short}"]`);
            if (closeMeter) closeMeter.style.width = Math.min(closePct, 100 - leafPct) + '%';

            const head = label?.closest('.atk-col-head');
            if (head) {
                head.title =
                    `${tactic.name}\n` +
                    `${s.leaf_covered}/${s.leaf_total} (${leafPct}%) detectable units covered — every sub-technique, plus techniques that have none. Nothing is inherited.\n` +
                    `${s.covered}/${s.total} (${techPct}%) top-level techniques have at least one rule somewhere under them.` +
                    (close ? `\n${close} uncovered unit(s) have rules waiting in a synced feed.` : '');
            }
        }
    },

    // Counts the leaf techniques per tactic that no rule covers but a synced feed
    // could. Leaves only: a parent lit by one covered child would double-count.
    closableByTactic() {
        const candidates = this.coverage?.candidates || {};
        const out = {};
        for (const tech of this.matrix.techniques || []) {
            if (tech.deprecated || !candidates[tech.id]) continue;
            if (!tech.sub && this.hasSubs(tech.id)) continue;
            for (const short of tech.tactics || []) out[short] = (out[short] || 0) + 1;
        }
        return out;
    },

    hasSubs(id) {
        if (!this._subParents) {
            this._subParents = new Set();
            for (const tech of this.matrix.techniques || []) {
                if (tech.sub && tech.parent) this._subParents.add(tech.parent);
            }
        }
        return this._subParents.has(id);
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
            if (match && this.filters.only) match = this.cellState(id) === this.filters.only;

            for (const el of els) {
                el.classList.toggle('atk-dim', !match);
                if (match) {
                    const wrap = el.closest('.atk-subs');
                    if (wrap) expand.add(wrap);
                }
            }
        }

        // Only groups this function opened are closed again when the filter clears.
        // Collapsing everything would undo whatever the operator expanded by hand.
        const narrowing = Boolean(term) || mode !== 'all' || Boolean(this.filters.only);
        for (const wrap of this.autoExpanded) {
            if (narrowing && expand.has(wrap)) continue;
            wrap.classList.remove('atk-open');
            wrap.previousElementSibling?.classList.remove('atk-open');
        }
        this.autoExpanded = narrowing ? expand : new Set();

        for (const wrap of this.autoExpanded) {
            wrap.classList.add('atk-open');
            wrap.previousElementSibling?.classList.add('atk-open');
        }
    },

    // ============================
    // Summary
    // ============================

    // One headline number, the weakest tactic, and a chip per thing that needs
    // doing. A chip only exists when its count does, so a chip on screen always
    // means work: an operator never has to read five stats to find the zeroes.
    renderSummary() {
        const el = document.getElementById('atkSummary');
        const s = this.coverage?.summary;
        if (!el || !s) return;

        const leafPct = s.leaf_total ? (s.leaf_covered / s.leaf_total) * 100 : 0;
        const techPct = s.techniques_total ? Math.round((s.techniques_covered / s.techniques_total) * 100) : 0;
        const weakShort = (s.weakest_tactics || [])[0];
        const weakName = this.matrix.tactics.find(t => t.short === weakShort)?.name;
        const weakStats = s.per_tactic?.[weakShort];

        const coverTip =
            `Detectable units covered: every sub-technique, plus every technique that has none. ` +
            `Nothing is inherited, so a rule on a parent does not credit its sub-techniques.\n` +
            `${s.techniques_covered}/${s.techniques_total} (${techPct}%) top-level techniques have a rule somewhere under them.`;

        el.innerHTML = `
            <span class="atk-headline" title="${Utils.escapeAttr(coverTip)}">
                <span class="atk-stat-label">Coverage</span>
                <span class="atk-headline-value">${s.leaf_covered}</span>
                <span class="atk-headline-of">/${s.leaf_total}</span>
                <span class="atk-meter atk-headline-meter"><span class="atk-meter-fill" style="width:${leafPct > 0 ? Math.max(leafPct, 1) : 0}%"></span></span>
                <span class="atk-headline-pct">${leafPct > 0 && leafPct < 10 ? leafPct.toFixed(1) : Math.round(leafPct)}%</span>
            </span>
            ${weakName ? `<span class="atk-weakest" title="Lowest share of its detectable units covered.">Weakest
                <b>${Utils.escapeHtml(weakName)}</b>
                ${weakStats ? `${weakStats.leaf_covered}/${weakStats.leaf_total}` : ''}</span>` : ''}
            <span class="atk-attn" id="atkAttn"></span>
            <span class="atk-stat-version" title="Embedded ATT&CK Enterprise matrix version">ATT&CK v${Utils.escapeHtml(s.matrix_version || '')}</span>
        `;
        this.renderAttention();
    },

    // The chips are filters, not decoration: clicking one narrows the map to the
    // cells it counted, which is the only reason to put a number on screen.
    renderAttention() {
        const el = document.getElementById('atkAttn');
        const s = this.coverage?.summary;
        if (!el || !s) return;

        const techniques = this.coverage.techniques || {};
        const candidates = this.coverage.candidates || {};
        const closable = Object.keys(candidates).length;
        const disabled = Object.values(techniques).filter(c => c.total > 0 && !c.enabled).length;
        const broken = s.rules_retired_tag || 0;

        const chips = [];
        if (closable) {
            chips.push({ only: 'close', kind: 'close', label: `<b>${closable}</b> gaps your feeds can close`,
                tip: "Uncovered techniques a synced feed already carries rules for. Lower that feed's severity or maturity threshold to pull them in." });
        }
        if (disabled) {
            chips.push({ only: 'off', kind: 'off', label: `<b>${disabled}</b> covered but disabled`,
                tip: 'Techniques whose every mapped rule is switched off. They count as covered on paper and detect nothing.' });
        }
        if (s.rules_unmapped) {
            chips.push({ kind: 'muted', label: `<b>${s.rules_unmapped}</b> rules untagged`,
                tip: 'Rules with no attack.tNNNN tag, or with only a tactic tag. They are invisible to this map.' });
        }
        if (broken) {
            chips.push({ kind: 'broken', label: `<b>${broken}</b> broken tags`,
                tip: `Rules tagged with a technique ID this ATT&CK version does not know and has no replacement for: ${(s.retired_tags || []).slice(0, 4).join(', ')}` });
        }

        // A chip whose count went to zero takes its filter with it, or the map stays
        // narrowed by something the operator can no longer see, let alone clear.
        const stale = this.filters.only && !chips.some(c => c.only === this.filters.only);
        if (stale) this.filters.only = '';

        el.innerHTML = chips.map(c => this.chip(c)).join('');
        el.querySelectorAll('.atk-chip-btn[data-only]').forEach(btn => {
            btn.addEventListener('click', () => {
                this.filters.only = this.filters.only === btn.dataset.only ? '' : btn.dataset.only;
                this.renderAttention();
                this.applyClientFilters();
            });
        });

        if (stale) this.applyClientFilters();
    },

    // A chip with a filter behind it is a button; one that only reports a number
    // stays a span, so nothing looks clickable that is not.
    chip({ only, kind, label, tip }) {
        const on = Boolean(only) && this.filters.only === only;
        const tag = only ? 'button' : 'span';
        const attrs = only ? `type="button" data-only="${only}" aria-pressed="${on}"` : '';
        return `<${tag} class="atk-chip-btn atk-chip-${kind}${on ? ' atk-chip-on' : ''}" ${attrs}
            title="${Utils.escapeAttr(tip)}"><i class="atk-chip-dot"></i>${label}</${tag}>`;
    },

    // Why a rule a feed carries is not running. The drawer names the reason so an
    // operator can tell a threshold they set from a translation Bifract cannot do.
    SKIP_REASONS: {
        min_level: 'below the feed severity threshold',
        min_status: 'below the feed maturity threshold',
        translate_error: 'cannot be translated to BQL',
        parse_error: 'cannot be parsed',
        create_error: 'failed to import',
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
        this._drawerTechnique = null;
        this.setSelected(techniqueId);
        const mitre = document.getElementById('atkDrawerMitre');
        if (mitre) mitre.href = 'https://attack.mitre.org/techniques/' + techniqueId.replaceAll('.', '/') + '/';
        const write = document.getElementById('atkDrawerWrite');
        if (write) write.disabled = true;
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
        `;

        this._drawerTechnique = { id: tech.id, name: tech.name, logSources: data.log_sources || [] };
        const write = document.getElementById('atkDrawerWrite');
        if (write) {
            write.textContent = rules.length ? 'Write another detection' : 'Write a detection';
            write.disabled = false;
        }
        const mitre = document.getElementById('atkDrawerMitre');
        if (mitre) mitre.href = data.url || `https://attack.mitre.org/techniques/${encodeURIComponent(tech.id || '')}/`;

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
                <div class="atk-note">These rules exist in a synced feed but were not imported.
                    Lower the feed's severity or maturity threshold to pull them in, or fix the
                    translation for the ones Bifract cannot express.</div>
                ${rows}
                ${more}
            </div>
        `;
    },

    // Starts a rule from the technique in the drawer, prefilled with the label the
    // map reads back, so closing a gap does not depend on remembering to tag it.
    writeDetection() {
        const tech = this._drawerTechnique;
        if (!tech || !window.Alerts) return;

        const telemetry = (tech.logSources || []).slice(0, 4).join(', ');
        this.closeDrawer();
        Alerts.showAlertEditor(null, {
            prefill: {
                name: tech.name,
                description: `Detects ${tech.name} (${tech.id}).` +
                    (telemetry ? ` MITRE expects this in: ${telemetry}.` : ''),
                labels: [`attack.${tech.id.toLowerCase()}`],
            },
        });
    },

    setSelected(techniqueId) {
        if (this.selectedId) {
            (this.cells.get(this.selectedId) || []).forEach(el => el.classList.remove('atk-selected'));
        }
        this.selectedId = techniqueId;
        (this.cells.get(techniqueId) || []).forEach(el => el.classList.add('atk-selected'));
    },

    closeDrawer() {
        this._drawerTechnique = null;
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

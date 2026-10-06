// Go to palette: jump to any page, sub-page, fractal, prism or object in the current scope.

const GoTo = {
    // Sub-tabs reachable as "Page > Sub-tab", read from their own markup so labels
    // and role gating stay in one place.
    SUBTABS: [
        { parent: 'alerts', level: 'fractal', selector: '#alertsSubTabs .alerts-sub-tab', route: b => b.dataset.subtab },
        { parent: 'notebooks', level: 'fractal', selector: '[data-investigation-subtab="evidence"]', route: () => 'evidence' },
        { parent: 'manage', level: 'fractal', selector: '#manageSubTabs .alerts-sub-tab', prism: false, route: b => b.dataset.subtab },
        { parent: 'manage', level: 'fractal', selector: '#prismManageSubTabs .alerts-sub-tab', prism: true, route: b => b.dataset.subtab },
        { parent: 'performance', level: 'main', selector: '#perfSubTabs .alerts-sub-tab', route: b => b.dataset.subtab },
        { parent: 'settings', level: 'main', selector: '#settingsSubTabs .alerts-sub-tab', route: b => b.dataset.subtab },
    ],

    // Objects in the current scope. Searched sources are queried per keystroke so
    // large scopes stay complete; listed ones are small and filtered locally.
    SOURCES: [
        { group: 'Dashboards', tab: 'dashboards', url: '/api/v1/dashboards?limit=8', search: true },
        { group: 'Notebooks', tab: 'notebooks', url: '/api/v1/notebooks?limit=8', search: true },
        { group: 'Alert rules', tab: 'alerts', url: '/api/v1/alerts?limit=8', search: true },
        { group: 'Lookups', tab: 'dictionaries', url: '/api/v1/dictionaries' },
        { group: 'Analytics models', tab: 'models', url: '/api/v1/models', fractalOnly: true },
    ],

    PER_GROUP: 6,
    SEARCH_DELAY_MS: 150,
    LIST_CACHE_MS: 60000,

    _open: false,
    _results: [],
    _active: 0,
    _returnFocus: null,
    _lists: null,       // { token, at, items } listed sources for the current scope
    _searched: null,    // { token, q, items } searched sources for the last query
    _searchTimer: null,
    _searchSeq: 0,
    _searchPending: false,

    init() {
        const overlay = document.getElementById('goToPalette');
        const input = document.getElementById('goToInput');
        const list = document.getElementById('goToList');
        if (!overlay || !input || !list) return;

        const kbd = document.querySelector('#sidebarGoTo .sb-kbd');
        if (kbd && !Utils.isMac) kbd.textContent = 'Ctrl K';

        document.getElementById('sidebarGoTo')?.addEventListener('click', () => this.open());
        // Keep focus in the input: clicks elsewhere in the dialog must not blur it.
        overlay.addEventListener('mousedown', (e) => {
            if (e.target === overlay) this.close();
            else if (e.target !== input) e.preventDefault();
        });
        input.addEventListener('input', () => this._update());
        input.addEventListener('keydown', (e) => this._onKey(e));
        list.addEventListener('mousemove', (e) => {
            const row = e.target.closest('[data-index]');
            if (row) this._setActive(Number(row.dataset.index), false);
        });
        list.addEventListener('click', (e) => {
            const row = e.target.closest('[data-index]');
            if (row) this._run(Number(row.dataset.index));
        });

        // Capture phase so an open palette owns the key before page handlers.
        // Closed, Cmd/Ctrl+K opens it everywhere except Query and Recall, which
        // keep their own palettes (Recall handles the key itself while active).
        document.addEventListener('keydown', (e) => {
            if (this._open && e.key === 'Escape') {
                e.preventDefault();
                e.stopImmediatePropagation();
                this.close();
                return;
            }
            const mod = Utils.isMac ? e.metaKey : e.ctrlKey;
            if (!mod || e.shiftKey || e.altKey || !e.key || e.key.toLowerCase() !== 'k') return;
            if (this._open) {
                e.preventDefault();
                e.stopImmediatePropagation();
                this.close();
                return;
            }
            if (document.body.classList.contains('recall-active')) return;
            e.preventDefault();
            if (window.App && App.currentViewLevel === 'fractal' && App.currentView === 'search' && window.QueryPalette) {
                QueryPalette.toggle();
            } else {
                this.open();
            }
        }, true);

        if (window.FractalContext) {
            FractalContext.subscribe('GoTo', () => {
                this._lists = null;
                this._searched = null;
            });
        }
    },

    // A page modal keeps the keyboard; navigating under it would strand it.
    _modalOpen() {
        return [...document.querySelectorAll('.modal, .modal-overlay')].some(m => getComputedStyle(m).display !== 'none');
    },

    open() {
        if (this._open || this._modalOpen()) return;
        if (window.QueryPalette && QueryPalette.isOpen) QueryPalette.close();
        this._open = true;
        this._returnFocus = document.activeElement;
        const input = document.getElementById('goToInput');
        input.value = '';
        document.getElementById('goToPalette').hidden = false;
        input.focus();
        this._update();
    },

    close(restoreFocus = true) {
        if (!this._open) return;
        this._open = false;
        clearTimeout(this._searchTimer);
        this._searchSeq++;
        this._searchPending = false;
        document.getElementById('goToPalette').hidden = true;
        const back = this._returnFocus;
        this._returnFocus = null;
        if (restoreFocus && back && document.contains(back)) back.focus();
    },

    _onKey(e) {
        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
            e.preventDefault();
            e.stopPropagation();
            if (!this._results.length) return;
            const step = e.key === 'ArrowDown' ? 1 : -1;
            this._setActive((this._active + step + this._results.length) % this._results.length, true);
        } else if (e.key === 'Enter') {
            e.preventDefault();
            e.stopPropagation();
            this._run(this._active);
        } else if (e.key === 'Tab') {
            e.preventDefault();
        }
    },

    // ---- Candidates ----

    _pages() {
        const out = [];
        document.querySelectorAll('#sidebar a[data-nav]').forEach(a => {
            if (a.hidden || a.closest('[hidden]')) return;
            out.push({ group: 'Pages', label: a.dataset.tip, level: a.dataset.level, tab: a.dataset.nav, sub: '' });
        });
        const parents = new Map(out.map(p => [`${p.level}:${p.tab}`, p.label]));
        const isPrism = !!(window.FractalContext && FractalContext.isPrism());
        for (const s of this.SUBTABS) {
            const parent = parents.get(`${s.level}:${s.parent}`);
            if (!parent || (s.prism !== undefined && s.prism !== isPrism)) continue;
            document.querySelectorAll(s.selector).forEach(b => {
                if (b.classList.contains('rbac-hidden') || b.hidden) return;
                // Own text only: some sub-tabs carry a count badge.
                const text = [...b.childNodes].filter(n => n.nodeType === Node.TEXT_NODE).map(n => n.textContent).join('').trim();
                out.push({ group: 'Pages', label: `${parent} > ${text}`, level: s.level, tab: s.parent, sub: s.route(b), isSub: true });
            });
        }
        return out;
    },

    _scopes() {
        if (!window.FractalSelector) return [];
        const current = window.FractalContext && FractalContext.hasScope() ? FractalContext.currentFractal.id : null;
        return [
            ...FractalSelector.availableFractals.map(f => ({ group: 'Fractals', label: f.name, hint: 'Fractal', scope: f.id, prism: false })),
            ...FractalSelector.availablePrisms.map(p => ({ group: 'Fractals', label: p.name, hint: 'Prism', scope: p.id, prism: true })),
        ].filter(s => s.scope !== current);
    },

    _sources() {
        const isPrism = window.FractalContext && FractalContext.isPrism();
        return this.SOURCES.filter(s => !(s.fractalOnly && isPrism));
    },

    async _fetchObjects(source, q) {
        const url = q ? `${source.url}&search=${encodeURIComponent(q)}` : source.url;
        const res = await fetch(url, { credentials: 'include' });
        const body = res.ok ? await res.json() : null;
        const rows = body && body.success && Array.isArray(body.data) ? body.data : [];
        return rows.filter(o => o && o.id && o.name)
            .map(o => ({ group: source.group, label: o.name, level: 'fractal', tab: source.tab, sub: String(o.id) }));
    },

    // Listed sources load once per scope and are reused for a minute.
    _ensureLists() {
        const ctx = window.FractalContext;
        if (!ctx || !ctx.hasScope()) return;
        const token = ctx.scopeToken();
        if (this._lists && this._lists.token === token &&
            (this._lists.pending || Date.now() - this._lists.at < this.LIST_CACHE_MS)) return;
        this._lists = { token, at: Date.now(), items: [], pending: true };
        const listed = this._sources().filter(s => !s.search);
        Promise.allSettled(listed.map(s => this._fetchObjects(s, ''))).then(results => {
            if (!this._lists || this._lists.token !== token || ctx.isScopeStale(token)) return;
            this._lists = { token, at: Date.now(), items: results.flatMap(r => (r.status === 'fulfilled' ? r.value : [])) };
            if (this._open) this._update();
        });
    },

    // Searched sources are debounced and only the newest query's results apply.
    _scheduleSearch(q) {
        clearTimeout(this._searchTimer);
        const ctx = window.FractalContext;
        if (!ctx || !ctx.hasScope()) return;
        const token = ctx.scopeToken();
        if (this._searched && this._searched.token === token && this._searched.q === q) return;
        this._searchPending = true;
        this._searchTimer = setTimeout(() => {
            const seq = ++this._searchSeq;
            const searched = this._sources().filter(s => s.search);
            Promise.allSettled(searched.map(s => this._fetchObjects(s, q))).then(results => {
                if (seq !== this._searchSeq || ctx.isScopeStale(token)) return;
                this._searchPending = false;
                this._searched = { token, q, items: results.flatMap(r => (r.status === 'fulfilled' ? r.value : [])) };
                if (this._open) this._update();
            });
        }, this.SEARCH_DELAY_MS);
    },

    // ---- Matching ----

    _score(label, q) {
        const l = label.toLowerCase();
        const i = l.indexOf(q);
        if (i === 0) return 4;
        if (i > 0) return /[\s>_\-./]/.test(l[i - 1]) ? 3 : 2;
        let j = 0;
        for (let k = 0; k < l.length && j < q.length; k++) if (l[k] === q[j]) j++;
        return j === q.length ? 1 : 0;
    },

    _key(r) {
        return r ? `${r.group}\u0000${r.label}\u0000${r.tab || ''}\u0000${r.sub || r.scope || ''}` : '';
    },

    _update() {
        const q = document.getElementById('goToInput').value.trim().toLowerCase();
        const token = window.FractalContext ? FractalContext.scopeToken() : null;
        let candidates = this._pages();
        if (q) {
            this._ensureLists();
            this._scheduleSearch(q);
            const lists = this._lists && this._lists.token === token ? this._lists.items : [];
            // The last search's rows stay until the next lands; scoring drops any that no longer match.
            const searched = this._searched && this._searched.token === token ? this._searched.items : [];
            candidates = candidates.concat(this._scopes(), lists, searched);
        } else {
            candidates = candidates.filter(c => !c.isSub).concat(this._scopes().slice(0, this.PER_GROUP));
        }

        const groups = new Map();
        for (const c of candidates) {
            const score = q ? this._score(c.label, q) : 1;
            if (!score) continue;
            if (!groups.has(c.group)) groups.set(c.group, []);
            groups.get(c.group).push({ ...c, score });
        }
        const keep = this._key(this._results[this._active]);
        this._results = [];
        for (const [group, items] of groups) {
            if (q) items.sort((a, b) => b.score - a.score || a.label.localeCompare(b.label));
            this._results.push(...(q || group !== 'Pages' ? items.slice(0, this.PER_GROUP) : items));
        }
        // Results arriving later must not move the selection out from under the user.
        const kept = this._results.findIndex(r => this._key(r) === keep);
        this._render(q, kept >= 0 ? kept : 0);
    },

    _render(q, active) {
        const list = document.getElementById('goToList');
        const input = document.getElementById('goToInput');
        const status = document.getElementById('goToStatus');
        input.setAttribute('aria-expanded', String(this._results.length > 0));
        if (!this._results.length) {
            const pending = q && this._searchPending;
            list.innerHTML = '';
            status.textContent = pending ? 'Searching...' : 'No matches';
            status.classList.add('visible');
            input.removeAttribute('aria-activedescendant');
            return;
        }
        status.textContent = `${this._results.length} result${this._results.length === 1 ? '' : 's'}`;
        status.classList.remove('visible');

        let html = '';
        let group = null;
        let groupIndex = 0;
        this._results.forEach((r, i) => {
            if (r.group !== group) {
                if (group !== null) html += '</div>';
                group = r.group;
                groupIndex++;
                html += `<div role="group" aria-labelledby="goToGroup${groupIndex}">` +
                    `<div class="goto-group" id="goToGroup${groupIndex}">${Utils.escapeHtml(group)}</div>`;
            }
            html += `<div class="goto-item" id="goToOpt${i}" role="option" data-index="${i}" aria-selected="false">
                <span class="goto-label">${this._highlight(r.label, q)}</span>
                ${r.hint ? `<span class="goto-hint">${Utils.escapeHtml(r.hint)}</span>` : ''}
            </div>`;
        });
        list.innerHTML = html + '</div>';
        this._setActive(active, true);
    },

    _highlight(label, q) {
        const i = q ? label.toLowerCase().indexOf(q) : -1;
        if (i < 0) return Utils.escapeHtml(label);
        return Utils.escapeHtml(label.slice(0, i)) + '<mark>' + Utils.escapeHtml(label.slice(i, i + q.length)) +
            '</mark>' + Utils.escapeHtml(label.slice(i + q.length));
    },

    _setActive(i, scroll) {
        const list = document.getElementById('goToList');
        const prev = list.querySelector('.goto-item.active');
        if (prev) {
            prev.classList.remove('active');
            prev.setAttribute('aria-selected', 'false');
        }
        this._active = i;
        const row = document.getElementById(`goToOpt${i}`);
        if (!row) return;
        row.classList.add('active');
        row.setAttribute('aria-selected', 'true');
        document.getElementById('goToInput').setAttribute('aria-activedescendant', row.id);
        if (scroll) row.scrollIntoView({ block: 'nearest' });
    },

    _run(i) {
        const r = this._results[i];
        if (!r || !window.App) return;
        this.close(false);
        if (r.scope) {
            if (r.prism) FractalSelector.selectPrism(r.scope);
            else FractalSelector.selectFractal(r.scope);
        } else if (r.level === 'main') {
            App.showMainView(r.tab, r.sub);
        } else {
            App.showFractalView(r.tab, r.sub);
        }
    },
};

window.GoTo = GoTo;

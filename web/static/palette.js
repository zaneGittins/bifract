// Command palette: the one Cmd/Ctrl+K surface on every page. Sources register
// the rows they offer and the page in view decides which lead, so Query shows
// its history first and still reaches every page, fractal and object. A row of
// tabs narrows it to one kind of row, so each list is a click away.

const Palette = {
    PER_GROUP: 6,
    SCOPED_LIMIT: 50,
    SEARCH_DELAY_MS: 150,
    LIST_CACHE_MS: 60000,
    WORKSPACE: new Set(['search', 'recall']),

    // Tabs after All, in order, shown when one of their sources is enabled. A
    // prefix typed first opens the tab; count shows how many rows it holds.
    TABS: {
        recall: { label: 'Recall', placeholder: 'Search recalls...' },
        saved: { label: 'Saved', prefix: '#', count: true, placeholder: 'Search saved queries...' },
        recent: { label: 'Recent', count: true, placeholder: 'Search recent queries...' },
        functions: { label: 'Functions', prefix: '/', count: true, placeholder: 'Search functions and operators...' },
        goto: { label: 'Go to', placeholder: 'Search pages, fractals, objects...' },
        actions: { label: 'Actions', prefix: '>', placeholder: 'Search actions...' },
        ai: { label: 'Ask AI', prefix: '?', placeholder: 'Describe what you want to find...' },
    },

    _sources: [],
    _open: false,
    _tab: null,         // null is All
    _counts: new Map(), // tab -> rows it holds for the current text
    _results: [],
    _active: 0,
    _moved: false,      // the user picked a row, so late results must not move it
    _returnFocus: null,
    _data: new Map(),   // source id -> { token, q, items, pending, at }
    _seq: 0,
    _panel: null,       // { onSubmit } while a form replaces the preview

    register(source) {
        this._sources.push(source);
        this._sources.sort((a, b) => a.order - b.order);
    },

    init() {
        const overlay = document.getElementById('palette');
        const input = document.getElementById('paletteInput');
        const list = document.getElementById('paletteList');
        if (!overlay || !input || !list) return;

        const shortcut = Utils.isMac ? '⌘K' : 'Ctrl K';
        document.querySelectorAll('[data-palette-kbd]').forEach(k => { k.textContent = shortcut; });

        document.getElementById('sidebarGoTo')?.addEventListener('click', () => this.open());
        overlay.addEventListener('mousedown', (e) => {
            if (e.target === overlay) this.close();
            // Keep focus in the input unless a form field is clicked, or code is
            // being selected to copy.
            else if (!e.target.closest('input, textarea, label, select, .pal-code')) e.preventDefault();
        });
        input.addEventListener('input', () => this._onInput());
        input.addEventListener('keydown', (e) => this._onKey(e));
        list.addEventListener('mousemove', (e) => {
            const row = e.target.closest('[data-index]');
            if (row && Number(row.dataset.index) !== this._active) {
                this._moved = true;
                this._setActive(Number(row.dataset.index), false);
            }
        });
        list.addEventListener('click', (e) => {
            const see = e.target.closest('[data-tab]');
            if (see) return this.setTab(see.dataset.tab);
            const row = e.target.closest('[data-index]');
            if (!row) return;
            const r = this._results[Number(row.dataset.index)];
            const btn = e.target.closest('button[data-act]');
            if (btn && r) this._runAction(this._actions(r)[Number(btn.dataset.act)], r.item);
            else this.run(Number(row.dataset.index), 0);
        });
        document.getElementById('paletteTabs').addEventListener('click', (e) => {
            const tab = e.target.closest('[data-tab]');
            if (tab) this.setTab(tab.dataset.tab || null);
        });
        document.getElementById('palettePreview').addEventListener('click', (e) => this._onPreviewClick(e));
        document.getElementById('palettePreview').addEventListener('keydown', (e) => this._onPanelKey(e));

        // Capture phase so an open palette owns the keys before page handlers.
        document.addEventListener('keydown', (e) => {
            if (this._open && e.key === 'Escape') {
                e.preventDefault();
                e.stopImmediatePropagation();
                if (this._panel) this.closePanel();
                else this.close();
                return;
            }
            const mod = Utils.isMac ? e.metaKey : e.ctrlKey;
            if (!mod || e.shiftKey || e.altKey || !e.key || e.key.toLowerCase() !== 'k') return;
            e.preventDefault();
            e.stopImmediatePropagation();
            this.toggle();
        }, true);

        // Back/forward moves the page out from under the palette.
        window.addEventListener('hashchange', () => this.close(false));

        if (window.FractalContext) {
            FractalContext.subscribe('Palette', () => {
                this._data.clear();
                if (this._open) this._update();
            });
        }
    },

    // ---- Open / close ----

    toggle(opts) { this._open ? this.close() : this.open(opts); },

    // A page modal keeps the keyboard; navigating under it would strand it.
    _modalOpen() {
        return [...document.querySelectorAll('.modal, .modal-overlay')].some(m => getComputedStyle(m).display !== 'none');
    },

    // opts.tab opens on one tab instead of All; opts.text pre-fills the input.
    open(opts = {}) {
        if (this._open || this._modalOpen()) return;
        this._open = true;
        this._returnFocus = document.activeElement;
        this._panel = null;
        this._tab = opts.tab || null;
        this._active = 0;
        this._moved = false;
        this._results = [];
        // Fresh sources (history, saved) reload on every open, showing the last
        // rows until the new ones land.
        for (const s of this._sources) {
            const d = s.fresh && this._data.get(s.id);
            if (d && !d.pending) d.at = 0;
        }
        const input = document.getElementById('paletteInput');
        input.value = opts.text || '';
        document.getElementById('palette').hidden = false;
        input.focus();
        this._sources.forEach(s => s.onOpen && s.onOpen());
        this._update();
    },

    close(restoreFocus = true) {
        if (!this._open) return;
        this._open = false;
        this._panel = null;
        this._sources.forEach(s => clearTimeout(s._timer));
        this._seq++;
        // Fetches still in flight are now ignored, so their rows must load again.
        for (const [id, d] of this._data) if (d.pending) this._data.delete(id);
        document.getElementById('palette').hidden = true;
        this._sources.forEach(s => s.onClose && s.onClose());
        const back = this._returnFocus;
        this._returnFocus = null;
        if (restoreFocus && back && document.contains(back)) back.focus();
    },

    get isOpen() { return this._open; },

    setTab(tab, text) {
        this._tab = tab;
        this._panel = null;
        const input = document.getElementById('paletteInput');
        if (text !== undefined) input.value = text;
        input.focus();
        this._active = 0;
        this._moved = false;
        this._update();
    },

    // The tabs this page offers, All (null) first.
    _tabs(ctx) {
        return [null, ...Object.keys(this.TABS).filter(t => this._sources.some(s => s.tab === t && this._enabled(s, ctx)))];
    },

    _stepTab(step) {
        const tabs = this._tabs(this.context());
        const i = Math.max(0, tabs.indexOf(this._tab));
        this.setTab(tabs[(i + step + tabs.length) % tabs.length]);
    },

    // A tab whose every source draws full rows (saved, recent) has no preview.
    _rich() {
        const ctx = this.context();
        const own = this._sources.filter(s => this._tab && s.tab === this._tab && this._enabled(s, ctx));
        return own.length > 0 && own.every(s => s.row);
    },

    // Re-render after a source's data changed underneath an open palette.
    refresh() {
        if (this._open) this._update();
    },

    // Refetch a source's rows, keeping the current ones on screen meanwhile so
    // the list does not flicker and the selection stays put.
    invalidate(id) {
        const d = this._data.get(id);
        if (d && !d.pending) d.at = 0;
        this.refresh();
    },

    context() {
        const app = window.App || {};
        const fc = window.FractalContext;
        return {
            view: app.currentViewLevel === 'fractal' ? app.currentView : null,
            level: app.currentViewLevel,
            hasScope: !!(fc && fc.hasScope()),
            isPrism: !!(fc && fc.isPrism && fc.isPrism()),
            tab: this._tab,
            workspace: app.currentViewLevel === 'fractal' && this.WORKSPACE.has(app.currentView),
        };
    },

    query() {
        return document.getElementById('paletteInput').value.trim();
    },

    // ---- Input ----

    _onInput() {
        const input = document.getElementById('paletteInput');
        if (!this._tab) {
            const tab = this._tabs(this.context()).find(t => t && this.TABS[t].prefix && input.value.startsWith(this.TABS[t].prefix));
            if (tab) {
                this._tab = tab;
                input.value = input.value.slice(this.TABS[tab].prefix.length).replace(/^\s+/, '');
            }
        }
        this._panel = null;
        this._moved = false;
        this._update();
    },

    _onKey(e) {
        if (e.isComposing) return;
        const takeover = this._takeover();
        const mod = e.metaKey || e.ctrlKey;
        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
            e.preventDefault();
            if (takeover || !this._results.length) return;
            const step = e.key === 'ArrowDown' ? 1 : -1;
            this._moved = true;
            this._setActive((this._active + step + this._results.length) % this._results.length, true);
        } else if (e.key === 'Enter') {
            e.preventDefault();
            if (takeover) takeover.enter(this.query(), { secondary: mod, force: e.shiftKey });
            else this.run(this._active, mod ? 1 : 0);
        } else if (e.key === 'Tab') {
            e.preventDefault();
            this._stepTab(e.shiftKey ? -1 : 1);
        } else if (e.key === 'Backspace' && this._tab && !e.target.value) {
            e.preventDefault();
            this.setTab(null);
        } else if (mod && !e.shiftKey && !e.altKey && e.key.length === 1 && !takeover) {
            const hit = this._shortcut(e.key.toLowerCase());
            if (hit) {
                e.preventDefault();
                this._runAction(hit.source.actions(hit.item, this.context())[0], hit.item);
            }
        }
    },

    // An item a source binds to Cmd/Ctrl+<key> while the palette is open.
    _shortcut(key) {
        const ctx = this.context();
        for (const s of this._sources) {
            if (!s.items || !this._enabled(s, ctx)) continue;
            const item = s.items(ctx, '').find(i => i.shortcut === key);
            if (item) return { source: s, item };
        }
        return null;
    },

    // A tab whose source draws the whole body itself (Ask AI).
    _takeover() {
        if (!this._tab) return null;
        return this._sources.find(s => s.tab === this._tab && s.takeover) || null;
    },

    _enabled(s, ctx) {
        return !s.when || s.when(ctx);
    },

    // ---- Data ----

    // Async sources: listed ones load once per scope and are reused for a
    // minute; searched ones are debounced and only the newest query applies.
    _load(source, q, ctx) {
        const fc = window.FractalContext;
        const token = fc ? fc.scopeToken() : null;
        const key = source.search ? q : '';
        const d = this._data.get(source.id);
        if (d && d.token === token && d.q === key && (d.pending || Date.now() - d.at < this.LIST_CACHE_MS)) return;
        const prev = d && d.token === token ? d.items : (source.cached ? source.cached(ctx) : []);
        this._data.set(source.id, { token, q: key, items: prev, pending: true, at: 0 });
        const seq = this._seq;
        const go = () => source.fetch(key, ctx).then(items => {
            const cur = this._data.get(source.id);
            if (seq !== this._seq || !cur || cur.q !== key || cur.token !== token) return;
            if (fc && fc.isScopeStale && fc.isScopeStale(token)) return;
            this._data.set(source.id, { token, q: key, items: items || [], pending: false, at: Date.now() });
            if (this._open) this._update();
        }).catch(() => {
            // Kept for the cache window like a success, so a failing endpoint is
            // not refetched on every keystroke.
            const cur = this._data.get(source.id);
            if (seq !== this._seq || !cur || cur.q !== key || cur.token !== token) return;
            cur.pending = false;
            cur.at = Date.now();
            if (this._open) this._update();
        });
        if (source.search && key) {
            clearTimeout(source._timer);
            source._timer = setTimeout(go, this.SEARCH_DELAY_MS);
        } else {
            go();
        }
    },

    _items(source, q, ctx) {
        if (source.items) return source.items(ctx, q);
        this._load(source, q, ctx);
        return this._data.get(source.id)?.items || [];
    },

    // A source's rows for q, best first.
    _rows(source, q, ctx) {
        const items = this._items(source, q, ctx);
        const ql = q.toLowerCase();
        let rows = items.map(item => ({ source, item, score: 0 }));
        if (ql && !source.unscored) {
            rows = rows
                .map(r => ({ ...r, score: this._scoreItem(r.item, ql) }))
                .filter(r => r.score)
                .sort((a, b) => b.score - a.score);
        }
        return rows;
    },

    _pending() {
        for (const d of this._data.values()) if (d.pending) return true;
        return false;
    },

    // ---- Matching ----

    _score(text, q) {
        const l = text.toLowerCase();
        const i = l.indexOf(q);
        if (i === 0) return 4;
        if (i > 0) return /[\s>_\-./|=(]/.test(l[i - 1]) ? 3 : 2;
        let j = 0;
        for (let k = 0; k < l.length && j < q.length; k++) if (l[k] === q[j]) j++;
        return j === q.length ? 1 : 0;
    },

    // The label matches loosely; the longer search text (descriptions, query
    // text) only as written, since scattered letters match almost any paragraph.
    _scoreItem(item, q) {
        const label = this._score(item.label, q);
        return label > 1 ? label : Math.max(label, item.search && item.search.toLowerCase().includes(q) ? 1.5 : 0);
    },

    // Sections keep their order, so the first row is not always the best match:
    // "coverage" should pick Alerts > Coverage over a query that merely contains
    // those letters in order.
    _best() {
        let best = 0;
        this._results.forEach((r, i) => { if (r.score > this._results[best].score) best = i; });
        return best;
    },

    _key(r) {
        return r ? `${r.source.id}\u0000${r.item.key || r.item.label}` : '';
    },

    _update() {
        const ctx = this.context();
        const q = this.query();
        const takeover = this._takeover();

        const keep = this._moved ? this._key(this._results[this._active]) : null;
        this._results = [];
        this._counts = new Map();
        for (const source of this._sources) {
            if (!this._enabled(source, ctx)) continue;
            const shown = !takeover && (!this._tab || source.tab === this._tab);
            const limit = !shown ? 0
                : this._tab ? this.SCOPED_LIMIT
                    : q ? (!source.inAll || source.inAll(ctx) ? this.PER_GROUP : 0)
                        : (source.idle ? source.idle(ctx) : 0);
            // Counted tabs load even when hidden, so every tab says what it holds.
            const counted = source.tab && this.TABS[source.tab].count;
            if (!limit && !counted) continue;
            const rows = this._rows(source, q, ctx);
            // A first load has no rows yet, which is not the same as none.
            const loading = !rows.length && this._data.get(source.id)?.pending;
            if (counted && !loading) this._counts.set(source.tab, (this._counts.get(source.tab) || 0) + rows.length);
            this._results.push(...rows.slice(0, limit));
        }
        this._renderChrome(ctx);

        if (takeover) {
            this._results = [];
            const view = takeover.render(q, ctx);
            const input = document.getElementById('paletteInput');
            input.setAttribute('aria-expanded', 'false');
            input.removeAttribute('aria-activedescendant');
            document.getElementById('paletteList').innerHTML = `<div class="pal-takeover">${view.list}</div>`;
            document.getElementById('palettePreview').innerHTML = view.preview;
            document.getElementById('paletteStatus').classList.remove('visible');
            this._renderFoot(view.foot);
            return;
        }

        const kept = this._results.findIndex(r => this._key(r) === keep);
        this._renderList(q, kept >= 0 ? kept : this._best());
    },

    // ---- Rendering ----

    _renderChrome(ctx) {
        const tabs = this._tabs(ctx);
        const input = document.getElementById('paletteInput');
        input.placeholder = this._tab ? this.TABS[this._tab].placeholder
            : `Search ${ctx.view === 'search' ? 'queries, ' : ''}pages, fractals, objects${tabs.includes('ai') ? ', or ask AI' : ''}...`;
        document.querySelector('#palette .pal-dialog').classList.toggle('rich', this._rich());
        document.getElementById('palettePreview').classList.toggle('has-panel', !!this._panel);
        this._renderTabs(tabs);
        const ctxLabel = document.getElementById('paletteContext');
        const active = document.querySelector('#sidebar a[data-nav][aria-current="page"]');
        ctxLabel.textContent = active ? active.dataset.tip : '';
        ctxLabel.hidden = !active;
    },

    _renderTabs(tabs) {
        document.getElementById('paletteTabs').innerHTML = tabs.map(t => {
            const tab = t ? this.TABS[t] : { label: 'All' };
            const n = this._counts.get(t);
            const tip = tab.prefix ? ` title="Type ${Utils.escapeHtml(tab.prefix)} to open"` : '';
            return `<button type="button" role="tab" class="pal-tab${t === 'ai' ? ' ai' : ''}" data-tab="${t || ''}" aria-selected="${t === this._tab}" tabindex="-1"${tip}>` +
                `${t === 'ai' ? this.ICONS.spark : ''}${Utils.escapeHtml(tab.label)}${n !== undefined ? `<span>${n}</span>` : ''}</button>`;
        }).join('');
    },

    _renderList(q, active) {
        const list = document.getElementById('paletteList');
        const input = document.getElementById('paletteInput');
        const status = document.getElementById('paletteStatus');
        input.setAttribute('aria-expanded', String(this._results.length > 0));
        if (!this._results.length) {
            list.innerHTML = '';
            status.textContent = this._pending() ? 'Searching...' : q ? 'No matches' : 'Nothing here yet';
            status.classList.add('visible');
            input.removeAttribute('aria-activedescendant');
            if (!this._panel) {
                document.getElementById('palettePreview').innerHTML = '';
                this._renderFoot();
            }
            return;
        }
        status.textContent = `${this._results.length} result${this._results.length === 1 ? '' : 's'}`;
        status.classList.remove('visible');

        const ql = q.toLowerCase();
        const rich = this._rich();
        const linked = new Set();
        let html = '';
        let group = null;
        let gi = 0;
        this._results.forEach((r, i) => {
            if (rich) {
                html += this._richRow(r, i, ql);
                return;
            }
            if (r.source !== group) {
                if (group !== null) html += '</div>';
                group = r.source;
                gi++;
                const hint = typeof r.source.hint === 'function' ? r.source.hint() : r.source.hint;
                // In All, the first section of each tab links to that tab.
                const t = r.source.tab;
                let see = '';
                if (!this._tab && t && !r.source.takeover && !linked.has(t)) {
                    linked.add(t);
                    const n = this._counts.get(t);
                    see = `<button type="button" class="pal-see" data-tab="${t}" tabindex="-1">See all${n !== undefined ? ` ${n}` : ''}</button>`;
                }
                html += `<div role="group" aria-labelledby="palGroup${gi}">` +
                    `<div class="pal-group"><span id="palGroup${gi}">${Utils.escapeHtml(r.source.group)}</span>` +
                    `<span class="pal-group-end">${hint ? `<em>${Utils.escapeHtml(hint)}</em>` : ''}${see}</span></div>`;
            }
            const it = r.item;
            const label = it.labelHtml !== undefined ? it.labelHtml : this._highlight(it.label, ql);
            html += `<div class="pal-item${r.source.tab === 'ai' ? ' ai' : ''}" id="palOpt${i}" role="option" data-index="${i}" aria-selected="false">
                <span class="pal-ic" aria-hidden="true">${it.icon || r.source.icon || ''}</span>
                <span class="pal-label${it.mono ? ' mono pal-hl' : ''}">${label}</span>
                ${it.hintHtml ? `<span class="pal-hint">${it.hintHtml}</span>` : it.hint ? `<span class="pal-hint">${Utils.escapeHtml(it.hint)}</span>` : ''}
            </div>`;
        });
        list.innerHTML = rich ? html : html + '</div>';
        this._setActive(Math.min(active, this._results.length - 1), true);
    },

    // A browse row: the source's own multi-line markup and every action as a
    // button, since there is no preview to hold them.
    _richRow(r, i, ql) {
        const it = r.item;
        const body = r.source.row ? r.source.row(it, ql) : `<span class="pal-label">${this._highlight(it.label, ql)}</span>`;
        const acts = this._actions(r).map((a, j) =>
            `<button type="button" class="pal-tool${a.danger ? ' danger' : ''}" data-act="${j}">${Utils.escapeHtml(a.short || a.label)}</button>`).join('');
        return `<div class="pal-item rich" id="palOpt${i}" role="option" data-index="${i}" aria-selected="false">
            <span class="pal-ic" aria-hidden="true">${it.icon || r.source.icon || ''}</span>
            <div class="pal-rich">${body}</div>
            <span class="pal-row-acts">${acts}</span>
        </div>`;
    },

    _highlight(label, q) {
        const i = q ? label.toLowerCase().indexOf(q) : -1;
        if (i < 0) return Utils.escapeHtml(label);
        return Utils.escapeHtml(label.slice(0, i)) + '<mark>' + Utils.escapeHtml(label.slice(i, i + q.length)) +
            '</mark>' + Utils.escapeHtml(label.slice(i + q.length));
    },

    _setActive(i, scroll) {
        const list = document.getElementById('paletteList');
        const prev = list.querySelector('.pal-item.active');
        if (prev) {
            prev.classList.remove('active');
            prev.setAttribute('aria-selected', 'false');
        }
        this._active = i;
        const row = document.getElementById(`palOpt${i}`);
        if (row) {
            row.classList.add('active');
            row.setAttribute('aria-selected', 'true');
            document.getElementById('paletteInput').setAttribute('aria-activedescendant', row.id);
            if (scroll) row.scrollIntoView({ block: 'nearest' });
        }
        if (!this._panel) {
            if (!this._rich()) this._renderPreview();
            this._renderFoot();
        }
    },

    _actions(r) {
        return r ? r.source.actions(r.item, this.context()) : [];
    },

    // The actions Enter and Cmd/Ctrl+Enter run, in that order. An action marked
    // tool is only ever a button, so a reflex keystroke cannot reach it.
    _keyed(acts) {
        return acts.filter(a => !a.tool).slice(0, 2);
    },

    _renderPreview() {
        const pane = document.getElementById('palettePreview');
        const r = this._results[this._active];
        if (!r) { pane.innerHTML = ''; return; }
        const body = r.source.preview
            ? r.source.preview(r.item, this.context())
            : `<div class="pal-pv-kind">${Utils.escapeHtml(r.item.kind || r.source.group)}</div>` +
              `<div class="pal-pv-title">${Utils.escapeHtml(r.item.label)}</div>`;
        // Enter and Cmd/Ctrl+Enter are buttons at the foot; the rest are quieter
        // tools in the corner.
        const all = this._actions(r);
        const keyed = this._keyed(all);
        const mod = Utils.isMac ? '⌘' : 'Ctrl';
        const main = keyed.map((a, j) =>
            `<button type="button" class="pal-btn${j === 0 ? ' primary' : ''}" data-act="${all.indexOf(a)}">${Utils.escapeHtml(a.label)}${j === 0 ? '<kbd>↵</kbd>' : `<kbd>${mod}</kbd><kbd>↵</kbd>`}</button>`).join('');
        const tools = all.filter(a => !keyed.includes(a)).map(a =>
            `<button type="button" class="pal-tool${a.danger ? ' danger' : ''}" data-act="${all.indexOf(a)}">${Utils.escapeHtml(a.label)}</button>`).join('');
        pane.innerHTML = (tools ? `<div class="pal-pv-tools">${tools}</div>` : '') +
            `<div class="pal-pv-body">${body}</div>${main ? `<div class="pal-pv-actions">${main}</div>` : ''}`;
    },

    _renderFoot(custom) {
        const foot = document.getElementById('paletteFoot');
        const mod = Utils.isMac ? '⌘' : 'Ctrl';
        let parts = custom;
        if (!parts) {
            const acts = this._keyed(this._actions(this._results[this._active]));
            parts = ['<kbd>↑</kbd><kbd>↓</kbd> navigate'];
            if (acts[0]) parts.push(`<kbd>↵</kbd> ${Utils.escapeHtml(acts[0].label.toLowerCase())}`);
            if (acts[1]) parts.push(`<kbd>${mod}</kbd><kbd>↵</kbd> ${Utils.escapeHtml(acts[1].label.toLowerCase())}`);
            parts.push('<kbd>Tab</kbd> next tab');
            if (this._tab) parts.push('<kbd>⌫</kbd> all');
            parts.push('|', '<kbd>esc</kbd> close');
        }
        foot.innerHTML = parts.map(p => (p === '|' ? '<span class="pal-sp"></span>' : `<span>${p}</span>`)).join('');
    },

    // ---- Running ----

    // Runs the Enter (key 0) or Cmd/Ctrl+Enter (key 1) action of row i.
    run(i, key) {
        const r = this._results[i];
        if (r) this._runAction(this._keyed(this._actions(r))[key], r.item);
    },

    _runAction(act, item) {
        if (!act) return;
        if (act.keepOpen) {
            // Whatever the action reloads, the row it acted on stays selected.
            this._moved = true;
            act.run(item);
            return;
        }
        this.close(false);
        act.run(item);
    },

    _onPreviewClick(e) {
        const btn = e.target.closest('button');
        if (!btn) return;
        if (btn.dataset.act !== undefined) {
            const r = this._results[this._active];
            if (r) this._runAction(this._actions(r)[Number(btn.dataset.act)], r.item);
        } else if (btn.dataset.panel === 'cancel') {
            this.closePanel();
        } else if (btn.dataset.panel === 'submit') {
            this._submitPanel();
        } else if (btn.dataset.ai !== undefined) {
            const t = this._takeover();
            if (t) t.button(btn.dataset.ai);
        }
    },

    // ---- Panels: a small form that replaces the preview (save, edit) ----

    showPanel({ title, html, submitLabel, onSubmit }) {
        this._panel = { onSubmit, busy: false };
        const pane = document.getElementById('palettePreview');
        pane.classList.add('has-panel');
        pane.innerHTML = `<form class="pal-panel" onsubmit="return false">
            <div class="pal-pv-kind">${Utils.escapeHtml(title)}</div>
            ${html}
            <div class="pal-pv-actions">
                <button type="button" class="pal-btn primary" data-panel="submit">${Utils.escapeHtml(submitLabel)}<kbd>↵</kbd></button>
                <button type="button" class="pal-btn" data-panel="cancel">Cancel<kbd>esc</kbd></button>
            </div>
        </form>`;
        const first = pane.querySelector('input, textarea');
        if (first) first.focus();
        this._renderFoot(['<kbd>↵</kbd> ' + Utils.escapeHtml(submitLabel.toLowerCase()), '<kbd>esc</kbd> cancel']);
    },

    closePanel() {
        if (!this._panel) return;
        this._panel = null;
        document.getElementById('paletteInput').focus();
        this._update();
    },

    async _submitPanel() {
        const panel = this._panel;
        // One request per submit, however fast Enter is pressed.
        if (!panel || panel.busy) return;
        panel.busy = true;
        try {
            const done = await panel.onSubmit(document.querySelector('#palettePreview .pal-panel'));
            if (done && this._panel === panel) this.closePanel();
        } finally {
            panel.busy = false;
        }
    },

    _onPanelKey(e) {
        // Buttons answer Enter themselves; a checkbox toggles.
        if (!this._panel || e.key !== 'Enter' || e.isComposing || ['TEXTAREA', 'BUTTON'].includes(e.target.tagName) || e.target.type === 'checkbox') return;
        e.preventDefault();
        this._submitPanel();
    },

    ICONS: {
        spark: '<svg width="13" height="13" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M8 1l1.6 4.4L14 7l-4.4 1.6L8 13l-1.6-4.4L2 7l4.4-1.6L8 1z"/></svg>',
        bolt: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"><path d="M9 1L3 9h4l-1 6 6-8H8l1-6z"/></svg>',
        clock: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><circle cx="8" cy="8" r="6"/><path d="M8 5v3l2 1.5" stroke-linecap="round"/></svg>',
        star: '<svg width="13" height="13" viewBox="0 0 16 16" fill="currentColor"><path d="M8 1.5l1.9 4 4.4.5-3.3 3 .9 4.3L8 11.1 4.1 13.3l.9-4.3-3.3-3 4.4-.5L8 1.5z"/></svg>',
        bookmark: '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><path d="M4 1h8a1 1 0 0 1 1 1v12l-5-3-5 3V2a1 1 0 0 1 1-1z" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>',
        page: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M3 8h10M9 4l4 4-4 4"/></svg>',
        scope: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><circle cx="8" cy="8" r="5.5"/><circle cx="8" cy="8" r="2"/></svg>',
        object: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><rect x="2.5" y="2.5" width="11" height="11" rx="2"/></svg>',
    },
};

window.Palette = Palette;

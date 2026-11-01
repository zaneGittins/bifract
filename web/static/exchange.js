// Exchange: browse shared content and add it through each kind's own import
// endpoint. The list on the right holds bundles, their items and the items in no
// bundle; searching flattens it. The selection fills the main pane on the left.
// Adding an item adds its requirements first; a bundle adds its members. A dataset
// is different: adding one creates a fractal of its own and imports into it in the
// background, with progress streamed over SSE.
const Exchange = {
    KINDS: {
        dataset: { label: 'Dataset', hint: 'Creates a fractal named after it and imports its logs there in the background. Needs a tenant admin.' },
        bundle: { label: 'Bundle', hint: 'Adds each item below that is not here yet, requirements first.' },
        lookup: { label: 'Lookup', hint: 'Added to this fractal. Rows refresh from the upstream URL. Needs a tenant admin.' },
        model: { label: 'Model', hint: 'Added to this fractal. It builds from new logs; run a backfill under Analytics for stored ones. Needs an analyst.' },
        detection: { label: 'Detection', hint: 'Added as an alert in this fractal, or as a draft when changes are reviewed.' },
        search: { label: 'Saved search', hint: 'Saved for everyone in this fractal.' },
        dashboard: { label: 'Dashboard', hint: 'Added to this fractal. Needs an analyst.' },
        normalizer: { label: 'Normalizer', hint: 'Added instance-wide. Needs a tenant admin.' },
        policy: { label: 'Policy', hint: 'Appends its rules to this fractal\'s alert policies, skipping any already there. Needs a fractal admin.' },
    },

    items: [],
    installed: new Set(),
    _loaded: false,
    _detail: null,
    _confirm: null,
    _busy: new Map(),
    _files: {},
    _loading: {},
    _errors: {},
    _normalizers: null,
    _bound: false,
    // Each dataset's latest import, by dataset id. Only tenant admins can read them.
    imports: new Map(),
    _events: null,
    _eventsRetry: null,
    _eventsBackoff: 0,
    _renderQueued: false,

    init() {
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('Exchange', () => {
                this._confirm = null;
                this.installed = new Set();
                if (FractalContext.shouldReload('exchangeView')) this.load();
            });
        }
    },

    show(subPath = '') {
        this.bindEvents();
        this._detail = subPath || null;
        this._confirm = null;
        this.load();
    },

    bindEvents() {
        if (this._bound) return;
        this._bound = true;
        document.getElementById('exchangeSearchInput')?.addEventListener('input', () => this.render());
        document.getElementById('exchangeKindFilter')?.addEventListener('change', () => this.render());
        document.getElementById('exchangeView')?.addEventListener('click', (e) => this.onClick(e));
    },

    scopeToken() {
        return window.FractalContext?.scopeToken?.();
    },

    isStale(token) {
        return !!window.FractalContext?.isScopeStale?.(token);
    },

    async load() {
        const token = this.scopeToken();
        if (!this._loaded) this.setStatus('Loading the Exchange...');
        try {
            const listing = (await HttpUtils.safeFetch('/api/v1/exchange')).data || {};
            if (this.isStale(token)) return;
            if (!listing.enabled) {
                this.items = [];
                this.setStatus('The Exchange is turned off on this server.');
                return;
            }
            const items = listing.items || [];
            const installed = await this.loadInstalled(items);
            if (this.isStale(token)) return;
            this.items = items;
            this.installed = installed;
            this._loaded = true;
            const mirror = document.getElementById('exchangeMirror');
            mirror.hidden = !listing.source;
            mirror.textContent = listing.source ? `Read from a mirror: ${listing.source}` : '';
            this.render();
        } catch (e) {
            if (this.isStale(token)) return;
            console.error('Failed to load the Exchange:', e);
            this.items = [];
            this.setStatus(`The Exchange could not be loaded: ${e.message}`);
        }
    },

    setStatus(text) {
        document.getElementById('exchangeMain').innerHTML = `<div class="admin-empty"><p>${Utils.escapeHtml(text)}</p></div>`;
        document.getElementById('exchangeList').innerHTML = '';
    },

    // What is already present in this scope, keyed as installedKey() keys an item.
    // Each list is read with the caller's own access, so one the caller cannot read
    // just shows nothing. Dashboards are paged, so each is looked up by name.
    async loadInstalled(items) {
        const keys = new Set();
        const read = async (url, kind, match) => {
            try {
                const data = await HttpUtils.safeFetch(url);
                for (const row of data.data || []) {
                    if (!match || row.name === match) keys.add(`${kind}:${row.name}`);
                }
            } catch (_) { /* not readable at this access level */ }
        };
        const dashboards = items.filter(it => it.kind === 'dashboard')
            .map(it => read(`/api/v1/dashboards?limit=100&search=${encodeURIComponent(it.name)}`, 'dashboard', it.name));
        await Promise.all([
            this.loadImports(items),
            read('/api/v1/dictionaries', 'lookup'),
            read('/api/v1/normalizers', 'normalizer'),
            read('/api/v1/alerts?limit=2000', 'detection'),
            read('/api/v1/models', 'model'),
            read('/api/v1/saved-queries', 'search'),
            ...dashboards,
        ]);
        this._normalizers = null;
        return keys;
    },

    // Datasets create fractals, so only tenant admins add them or see their imports.
    canAddDatasets() {
        return !!window.Auth?.currentUser?.is_admin;
    },

    async loadImports(items = this.items) {
        if (!this.canAddDatasets() || !items.some(it => it.kind === 'dataset')) return;
        try {
            const data = await HttpUtils.safeFetch('/api/v1/exchange/datasets');
            // Newest first, so the first per dataset is its latest.
            const seen = new Set();
            for (const imp of data.data || []) {
                if (seen.has(imp.dataset_id)) continue;
                seen.add(imp.dataset_id);
                this.onImport(imp, false);
            }
            this.watchImports();
        } catch (e) {
            console.error('Failed to load dataset imports:', e);
        }
    },

    importOpen(imp) {
        return !!imp && ['pending', 'downloading', 'importing'].includes(imp.status);
    },

    // Progress arrives over SSE while any import is running. The browser reconnects
    // on its own after a dropped connection; one it gives up on is reopened with
    // backoff. Either way the list is read again, since events were missed.
    watchImports() {
        const open = [...this.imports.values()].some(imp => this.importOpen(imp));
        if (!open) {
            this._events?.close();
            this._events = null;
            clearTimeout(this._eventsRetry);
            this._eventsRetry = null;
            return;
        }
        if (this._events || this._eventsRetry) return;
        const es = new EventSource('/api/v1/exchange/datasets/events', { withCredentials: true });
        this._events = es;
        let connects = 0;
        es.onmessage = (e) => {
            let event;
            try { event = JSON.parse(e.data); } catch (_) { return; }
            if (event.type === 'connected') {
                this._eventsBackoff = 0;
                if (connects++ > 0) this.loadImports().then(() => this.renderSoon());
            } else if (event.type === 'dataset_import' && event.data) {
                this.onImport(event.data);
            }
        };
        es.onerror = () => {
            if (es.readyState !== EventSource.CLOSED || this._events !== es) return;
            this._events = null;
            this._eventsBackoff = Math.min((this._eventsBackoff || 2500) * 2, 60000);
            this._eventsRetry = setTimeout(() => {
                this._eventsRetry = null;
                this.loadImports().then(() => this.renderSoon());
            }, this._eventsBackoff);
        };
    },

    // Keeps the newest state of each dataset's latest import. A list read and the
    // stream can cross, so an older state of the same import is dropped.
    onImport(imp, announce = true) {
        const prev = this.imports.get(imp.dataset_id);
        if (prev && (prev.id > imp.id || (prev.id === imp.id && Date.parse(prev.updated_at) > Date.parse(imp.updated_at)))) return;
        this.imports.set(imp.dataset_id, imp);
        if (announce && this.importOpen(prev) && !this.importOpen(imp)) {
            const name = this.find(imp.dataset_id)?.name || imp.dataset_id;
            if (imp.status === 'succeeded') Toast.success(`Imported ${name}`, `${this.count(imp.logs_done)} logs are in fractal ${imp.fractal_name}.`);
            else Toast.error(`Could not import ${name}`, imp.error || 'The import failed.');
        }
        if (!announce) return;
        this.watchImports();
        if (prev?.status === imp.status && this.importOpen(imp)) this.showProgress(imp);
        else this.renderSoon();
    },

    // Renders on the next frame, once however many changes arrive, and not at all
    // while the Exchange is hidden: showing it renders anyway.
    renderSoon() {
        if (this._renderQueued) return;
        this._renderQueued = true;
        requestAnimationFrame(() => {
            this._renderQueued = false;
            if (document.getElementById('exchangeView')?.offsetParent) this.render();
        });
    },

    // Progress within one status updates its marks in place rather than rebuilding
    // the panes, which would drop scroll and hover.
    showProgress(imp) {
        const id = CSS.escape(imp.dataset_id);
        const { label, pct } = this.importProgress(imp);
        document.querySelectorAll(`#exchangeView [data-progress="${id}"]`).forEach(el => {
            if (el.dataset.part === 'mark') el.textContent = `${pct}%`;
            else if (el.dataset.part === 'status') el.textContent = `${label} ${pct}%`;
            else if (el.dataset.part === 'detail') el.textContent = this.importDetail(imp);
            else if (el.dataset.part === 'bar') {
                el.setAttribute('aria-valuenow', pct);
                el.setAttribute('aria-valuetext', this.importDetail(imp));
                el.firstElementChild.style.width = `${pct}%`;
            }
        });
    },

    importDetail(imp) {
        const { label } = this.importProgress(imp);
        if (imp.status === 'importing') return `${label}: ${this.count(imp.logs_done)} of ${this.count(imp.logs_total)} logs`;
        if (imp.status === 'downloading') return `${label}: ${Utils.formatBytes(imp.bytes_done)} of ${Utils.formatBytes(imp.bytes_total)}`;
        return 'Waiting to start';
    },

    count(n) {
        return Number(n || 0).toLocaleString();
    },

    // Where a running import has got to, as a whole percentage.
    importProgress(imp) {
        if (imp.status === 'downloading') return { label: 'Downloading', pct: imp.bytes_total ? Math.floor(imp.bytes_done * 100 / imp.bytes_total) : 0 };
        if (imp.status === 'importing') return { label: 'Importing', pct: imp.logs_total ? Math.floor(imp.logs_done * 100 / imp.logs_total) : 0 };
        return { label: 'Queued', pct: 0 };
    },

    // The name an item is installed under: lookups and models by id, the rest by
    // name. Policies have no name, so they are never shown as installed; adding one
    // again skips the rules already present.
    installedKey(item) {
        if (item.kind === 'policy' || item.kind === 'bundle') return null;
        return `${item.kind}:${item.kind === 'lookup' || item.kind === 'model' ? item.id : item.name}`;
    },

    find(id) {
        return this.items.find(it => it.id === id);
    },

    members(bundle) {
        return bundle.items.map(id => this.find(id)).filter(Boolean);
    },

    isInstalled(item) {
        if (item.kind === 'dataset') return this.imports.get(item.id)?.status === 'succeeded';
        if (item.kind === 'bundle') return this.members(item).every(m => this.isInstalled(m));
        const key = this.installedKey(item);
        return !!key && this.installed.has(key);
    },

    // Everything adding an item involves, in order: requirements first, each once.
    // A bundle covers its members that way.
    order(item) {
        const order = [];
        const seen = new Set();
        const visit = (it) => {
            if (!it || seen.has(it.id)) return;
            seen.add(it.id);
            (it.requires || []).forEach(id => visit(this.find(id)));
            if (it.kind === 'bundle') it.items.forEach(id => visit(this.find(id)));
            else order.push(it);
        };
        visit(item);
        return order;
    },

    pending(item) {
        return this.order(item).filter(it => !this.isInstalled(it));
    },

    bundlesOf(item) {
        return this.items.filter(b => b.kind === 'bundle' && b.items.includes(item.id));
    },

    // ---- Views ----

    // The entry the main pane shows: the one picked, else the first bundle. Narrow
    // screens show one pane at a time, so there the default leaves the list showing.
    selected() {
        return (this._detail && this.find(this._detail))
            || this.items.find(it => it.kind === 'dataset')
            || this.items.find(it => it.kind === 'bundle') || this.items[0] || null;
    },

    render() {
        if (!this._loaded) return;
        // Rebuilt panes carry keyboard focus over to the same control.
        const focused = document.activeElement?.closest?.('#exchangeView [data-action]');
        const sel = this.selected();
        document.getElementById('exchangeShell').classList.toggle('exchange-picked', !!this._detail);
        this.renderList(sel);
        document.getElementById('exchangeMain').innerHTML = sel ? this.mainHtml(sel) : '';
        if (focused) {
            const q = `[data-action="${focused.dataset.action}"]` + (focused.dataset.id ? `[data-id="${CSS.escape(focused.dataset.id)}"]` : '');
            const where = focused.closest('#exchangeList') ? '#exchangeList' : '#exchangeMain';
            document.querySelector(`${where} ${q}`)?.focus();
        }
    },

    matches(it, terms, kind) {
        if (kind && it.kind !== kind) return false;
        return terms.every(t => [it.name, it.description, it.author, this.KINDS[it.kind].label, ...(it.tags || [])]
            .some(v => (v || '').toLowerCase().includes(t)));
    },

    // Browsing shows each bundle with its items under it, then the items in none.
    // A search or kind filter looks for items, so it lists every match once.
    renderList(sel) {
        const q = (document.getElementById('exchangeSearchInput')?.value || '').toLowerCase().trim();
        const kind = document.getElementById('exchangeKindFilter')?.value || '';
        const terms = q.split(/\s+/).filter(Boolean);
        const row = (it, nested) => this.listRowHtml(it, sel, nested);
        let html;
        if (!terms.length && !kind) {
            const datasets = this.items.filter(it => it.kind === 'dataset');
            const bundles = this.items.filter(it => it.kind === 'bundle');
            const loose = this.items.filter(it => it.kind !== 'bundle' && it.kind !== 'dataset' && !this.bundlesOf(it).length);
            html = (datasets.length ? `<div class="exchange-list-label">Datasets</div>` + datasets.map(it => row(it, false)).join('') : '')
                + `<div class="exchange-list-label">Bundles</div>`
                + bundles.map(b => row(b, false) + this.members(b).map(m => row(m, true)).join('')).join('')
                + (loose.length ? `<div class="exchange-list-label">Other items</div>` + loose.map(it => row(it, false)).join('') : '');
        } else {
            const found = this.items.filter(it => this.matches(it, terms, kind));
            html = found.length
                ? `<div class="exchange-list-label">${found.length} match${found.length === 1 ? '' : 'es'}</div>` + found.map(it => row(it, false)).join('')
                : '<p class="exchange-list-empty">No items match.</p>';
        }
        document.getElementById('exchangeList').innerHTML = html;
    },

    listRowHtml(it, sel, nested) {
        const esc = Utils.escapeHtml;
        const current = sel && sel.id === it.id;
        return `
<button type="button" class="exchange-row${nested ? ' nested' : ''}${it.kind === 'bundle' ? ' bundle' : ''}" data-action="select" data-id="${Utils.escapeAttr(it.id)}"${current ? ' aria-current="true"' : ''}>
    <span class="exchange-row-kind">${esc(this.KINDS[it.kind].label)}</span>
    <span class="exchange-row-name">${esc(it.name)}</span>
    ${this.statusMark(it)}
</button>`;
    },

    // A compact status for the list: a tick when present, a count for a bundle.
    statusMark(it) {
        const imp = it.kind === 'dataset' && this.imports.get(it.id);
        if (this.importOpen(imp)) {
            const { label, pct } = this.importProgress(imp);
            return `<span class="exchange-row-status" title="${label}" data-progress="${Utils.escapeAttr(it.id)}" data-part="mark">${pct}%</span>`;
        }
        if (imp && imp.status === 'failed') return '<span class="exchange-row-status failed" title="Import failed">!</span>';
        if (it.kind === 'bundle') {
            const members = this.members(it);
            const done = members.filter(m => this.isInstalled(m)).length;
            if (!done) return '';
            const all = done === members.length;
            return `<span class="exchange-row-status${all ? ' done' : ''}" title="${done} of ${members.length} added">${all ? '&#10003;' : `${done}/${members.length}`}</span>`;
        }
        return this.isInstalled(it) ? '<span class="exchange-row-status done" title="Installed">&#10003;</span>' : '';
    },

    statusHtml(it) {
        const imp = it.kind === 'dataset' && this.imports.get(it.id);
        if (this.importOpen(imp)) {
            const { label, pct } = this.importProgress(imp);
            return `<span class="exchange-partial" data-progress="${Utils.escapeAttr(it.id)}" data-part="status">${label} ${pct}%</span>`;
        }
        if (imp && imp.status === 'failed') return '<span class="exchange-failed">Import failed</span>';
        if (it.kind === 'bundle') {
            const members = this.members(it);
            const done = members.filter(m => this.isInstalled(m)).length;
            if (done === members.length) return '<span class="exchange-installed">Installed</span>';
            if (done) return `<span class="exchange-partial">${done} of ${members.length} added</span>`;
            return '';
        }
        return this.isInstalled(it) ? '<span class="exchange-installed">Installed</span>' : '';
    },

    kindHtml(it) {
        return `<span class="exchange-kind exchange-kind-${it.kind}">${Utils.escapeHtml(this.KINDS[it.kind].label)}</span>`;
    },

    tagsHtml(it) {
        return (it.tags || []).map(t => `<span class="exchange-tag">${Utils.escapeHtml(t)}</span>`).join('');
    },

    mainHtml(it) {
        const esc = Utils.escapeHtml, attr = Utils.escapeAttr;
        const author = esc(it.author || 'Bifract');
        const by = Utils.isHttpUrl(it.homepage)
            ? `<a href="${attr(it.homepage)}" target="_blank" rel="noopener noreferrer">${author}</a>`
            : author;
        const links = (ids) => ids.map(id => this.find(id)).filter(Boolean)
            .map(r => `<button type="button" class="exchange-link" data-action="select" data-id="${attr(r.id)}">${esc(r.name)}</button>`).join(', ');
        const facts = [];
        const bundles = this.bundlesOf(it).map(b => b.id);
        if (bundles.length) facts.push(`<dt>Part of</dt><dd>${links(bundles)}</dd>`);
        if (it.requires?.length) facts.push(`<dt>Requires</dt><dd>${links(it.requires)}</dd>`);
        return `
<button type="button" class="exchange-back" data-action="back">&larr; All items</button>
<div class="exchange-detail-head">
    <div class="exchange-detail-title">
        <div class="exchange-card-head">${this.kindHtml(it)}${this.statusHtml(it)}</div>
        <h2>${esc(it.name)}</h2>
        <div class="exchange-card-meta"><span>${by}</span>${this.tagsHtml(it)}</div>
    </div>
    <div class="exchange-detail-action">${this._confirm === it.id ? '' : this.actionHtml(it)}</div>
</div>
${this.confirmHtml(it)}
<p class="exchange-detail-desc">${esc(it.description || '')}</p>
<p class="exchange-hint">${esc(this.KINDS[it.kind].hint)}</p>
${facts.length ? `<dl class="exchange-facts">${facts.join('')}</dl>` : ''}
${it.kind === 'bundle' ? this.contentsHtml(it) : `<h3 class="exchange-section">Definition</h3>${this.definitionHtml(it)}`}`;
    },

    // The header's button: Add, Add all, or its progress.
    actionHtml(it) {
        const busy = this._busy.get(it.id);
        if (busy) return `<button type="button" class="btn-primary" disabled>${Utils.escapeHtml(busy)}</button>`;
        if (it.kind === 'dataset') return this.datasetActionHtml(it);
        if (this.isInstalled(it)) return '';
        const steps = this.pending(it);
        let label = 'Add';
        if (it.kind === 'bundle') label = steps.length === this.order(it).length ? `Add all (${steps.length})` : `Add ${steps.length} more`;
        return `<button type="button" class="btn-primary" data-action="install" data-id="${Utils.escapeAttr(it.id)}">${label}</button>`;
    },

    // A dataset is added once, then opened, while it fills too; a failed import can
    // be run again.
    datasetActionHtml(it) {
        if (!this.canAddDatasets()) return '';
        const imp = this.imports.get(it.id);
        const attr = Utils.escapeAttr;
        if (!imp) return `<button type="button" class="btn-primary" data-action="install" data-id="${attr(it.id)}">Add</button>`;
        if (imp.status === 'failed') return `<button type="button" class="btn-primary" data-action="retry-import" data-id="${attr(it.id)}">Retry</button>`;
        return `<button type="button" class="btn-primary" data-action="open-fractal" data-id="${attr(it.id)}">Open</button>`;
    },

    // Adding more than one thing says what it adds and skips before doing it, and
    // adding a dataset what it creates and downloads.
    confirmHtml(it) {
        if (this._confirm !== it.id) return '';
        const esc = Utils.escapeHtml;
        if (it.kind === 'dataset') {
            return `
<div class="exchange-confirm" role="region" aria-label="Confirm adding">
    <p>Creates fractal <strong>${esc(it.id)}</strong> and imports ${this.count(it.dataset.logs)} logs into it, downloading ${esc(Utils.formatBytes(it.dataset.bytes))}. It runs in the background.</p>
    <div class="exchange-confirm-actions">
        <button type="button" class="btn-secondary btn-sm" data-action="cancel">Cancel</button>
        <button type="button" class="btn-primary btn-sm" data-action="confirm" data-id="${Utils.escapeAttr(it.id)}">Add dataset</button>
    </div>
</div>`;
        }
        const steps = this.pending(it);
        const skipped = this.order(it).filter(s => this.isInstalled(s));
        const list = (items) => items.map(s => `<li>${this.kindHtml(s)}<span>${esc(s.name)}</span></li>`).join('');
        return `
<div class="exchange-confirm" role="region" aria-label="Confirm adding">
    <p>Adds ${steps.length} item${steps.length === 1 ? '' : 's'} to this fractal, in this order:</p>
    <ol class="exchange-confirm-list">${list(steps)}</ol>
    ${skipped.length ? `<p class="exchange-hint">Already here, so skipped: ${skipped.map(s => esc(s.name)).join(', ')}.</p>` : ''}
    <div class="exchange-confirm-actions">
        <button type="button" class="btn-secondary btn-sm" data-action="cancel">Cancel</button>
        <button type="button" class="btn-primary btn-sm" data-action="confirm" data-id="${Utils.escapeAttr(it.id)}">Add ${steps.length} item${steps.length === 1 ? '' : 's'}</button>
    </div>
</div>`;
    },

    // A bundle's items, each with its status and a menu to view or add it alone.
    contentsHtml(bundle) {
        const esc = Utils.escapeHtml, attr = Utils.escapeAttr;
        const rows = this.members(bundle).map(m => {
            const first = this.pending(m).filter(s => s.id !== m.id);
            const busy = this._busy.get(m.id);
            const add = this.isInstalled(m) ? ''
                : `<button type="button" class="kebab-item" data-action="install" data-id="${attr(m.id)}">${first.length ? `Add with ${esc(first.map(s => s.name).join(', '))}` : 'Add'}</button>`;
            return `
<tr>
    <td class="exchange-cell-kind">${this.kindHtml(m)}</td>
    <td><button type="button" class="exchange-link exchange-member-name" data-action="select" data-id="${attr(m.id)}">${esc(m.name)}</button>
        <div class="exchange-member-desc">${esc(m.description || '')}</div></td>
    <td class="exchange-cell-status">${busy ? `<span class="exchange-partial">${esc(busy)}</span>` : this.statusHtml(m)}</td>
    <td class="kebab-cell"><div class="kebab-wrapper">
        <button type="button" class="kebab-btn" data-action="kebab" aria-label="Actions for ${attr(m.name)}">&#8942;</button>
        <div class="kebab-menu">
            <button type="button" class="kebab-item" data-action="select" data-id="${attr(m.id)}">View definition</button>
            ${add}
        </div>
    </div></td>
</tr>`;
        }).join('');
        return `<h3 class="exchange-section">Contents</h3><table class="exchange-contents"><tbody>${rows}</tbody></table>`;
    },

    // What an item adds: a lookup's source settings, or the file it imports.
    definitionHtml(it) {
        const esc = Utils.escapeHtml;
        if (it.kind === 'dataset') return this.datasetHtml(it);
        if (it.kind === 'lookup') {
            const s = it.lookup.source;
            const rows = [
                ['Lookup name', it.id],
                ['Kind', it.lookup.list_kind || 'value'],
                ['URL', s.url],
                ['Key column', s.key_field],
                ['Other columns', (s.columns || []).join(', ') || 'all'],
                ['Split key cells on', s.split_key || ''],
                ['Keys are globs', s.key_format === 'glob' ? 'yes' : ''],
                ['Refresh every', this.every(s.refresh_minutes || 60)],
            ].filter(([, v]) => v);
            return `<dl class="exchange-source">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
        }
        if (this._errors[it.id]) {
            return `<p class="exchange-error">${esc(this._errors[it.id])} <button type="button" class="exchange-link" data-action="retry" data-id="${Utils.escapeAttr(it.id)}">Try again</button></p>`;
        }
        const file = this._files[it.id];
        if (!file) {
            if (!this._loading[it.id]) {
                this.fetchFile(it).then(() => this.render(), err => { this._errors[it.id] = err.message; this.render(); });
            }
            return '<p class="exchange-hint">Loading...</p>';
        }
        return `<pre class="exchange-content yaml-code">${Utils.highlightYAML(file.content)}</pre>`;
    },

    // What a dataset imports, and how its import is going.
    datasetHtml(it) {
        const esc = Utils.escapeHtml;
        const d = it.dataset;
        const fmt = v => (window.TZ ? TZ.formatInput(v, true) : v);
        const rows = [
            ['Fractal', it.id],
            ['Logs', this.count(d.logs)],
            ['Time range', `${fmt(d.start)} to ${fmt(d.end)}`],
            ['Size', `${Utils.formatBytes(d.bytes)} compressed`],
            ['File', d.url],
            ['SHA-256', d.sha256],
        ];
        let html = `<dl class="exchange-source">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
        const imp = this.imports.get(it.id);
        if (this.importOpen(imp)) {
            const { pct } = this.importProgress(imp);
            const id = Utils.escapeAttr(it.id);
            const detail = esc(this.importDetail(imp));
            html += `<h3 class="exchange-section">Import</h3>
<div class="exchange-progress" role="progressbar" aria-label="Import progress" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}" aria-valuetext="${detail}" data-progress="${id}" data-part="bar"><span style="width:${pct}%"></span></div>
<p class="exchange-hint"><span data-progress="${id}" data-part="detail">${detail}</span>. Fractal ${esc(imp.fractal_name)} can be searched while it fills.</p>`;
        } else if (imp?.status === 'failed') {
            html += `<h3 class="exchange-section">Import</h3><p class="exchange-error">${esc(imp.error || 'The import failed.')}</p>
<p class="exchange-hint">Retry empties fractal ${esc(imp.fractal_name)} and imports the dataset again.</p>`;
        }
        return html;
    },

    every(minutes) {
        for (const [size, unit] of [[1440, 'day'], [60, 'hour'], [1, 'minute']]) {
            if (minutes % size === 0) {
                const n = minutes / size;
                return `${n} ${unit}${n === 1 ? '' : 's'}`;
            }
        }
    },

    // ---- Actions ----

    onClick(e) {
        const el = e.target.closest('[data-action]');
        if (!el || !document.getElementById('exchangeView').contains(el)) return;
        const item = el.dataset.id ? this.find(el.dataset.id) : null;
        switch (el.dataset.action) {
            case 'kebab': KebabMenu.toggle(e, el); break;
            case 'select': if (item) this.select(item); break;
            case 'back': this.back(); break;
            case 'retry':
                if (!item) break;
                delete this._errors[item.id];
                this.render();
                break;
            case 'install':
                if (!item) break;
                if (item.kind === 'dataset' || this.pending(item).length > 1) {
                    // A multi-step add from a row's menu confirms on that item's own view.
                    if (this.selected()?.id !== item.id) this.select(item);
                    this._confirm = item.id;
                    this.render();
                    document.querySelector('#exchangeMain [data-action="confirm"]')?.focus();
                } else {
                    this.install(item);
                }
                break;
            case 'confirm':
                this._confirm = null;
                if (item) this.install(item);
                break;
            case 'cancel':
                this._confirm = null;
                this.render();
                break;
            case 'retry-import':
                if (item) this.retryImport(item);
                break;
            case 'open-fractal':
                if (item) this.openFractal(item);
                break;
        }
    },

    async retryImport(item) {
        const imp = this.imports.get(item.id);
        if (!imp || this._busy.has(item.id)) return;
        this._busy.set(item.id, 'Retrying...');
        this.render();
        try {
            const data = await this.expectOK(this.post(null, `/api/v1/exchange/datasets/imports/${imp.id}/retry`, {}));
            this.onImport(data.data);
        } catch (err) {
            Toast.error(`Could not retry ${item.name}`, err.message);
        } finally {
            this._busy.delete(item.id);
            this.render();
        }
    },

    async openFractal(item) {
        const imp = this.imports.get(item.id);
        if (!imp || !window.FractalContext) return;
        // A fractal added this session may not be in the selector's list yet.
        await window.FractalSelector?.loadAvailableFractals?.();
        if (await FractalContext.setCurrentFractal({ id: imp.fractal_id, name: imp.fractal_name })) {
            window.App?.showFractalView('search');
        }
    },

    select(item) {
        this._detail = item.id;
        this._confirm = null;
        window.App?.pushSubPath(item.id);
        this.render();
        document.getElementById('exchangeMain')?.scrollTo?.(0, 0);
    },

    // Narrow screens only: back from the main pane to the list.
    back() {
        this._detail = null;
        this._confirm = null;
        window.App?.pushSubPath('');
        this.render();
    },

    // An item's file, and for a search the parsed search the server sends with it.
    // Concurrent callers share one request.
    fetchFile(item) {
        if (this._files[item.id]) return Promise.resolve(this._files[item.id]);
        this._loading[item.id] ??= HttpUtils.safeFetch(`/api/v1/exchange/items/${encodeURIComponent(item.id)}`)
            .then(resp => (this._files[item.id] = { content: resp.data?.content ?? '', search: resp.data?.search }))
            .finally(() => delete this._loading[item.id]);
        return this._loading[item.id];
    },

    // Adds an item's pending steps in order. A step that fails is reported, and so
    // is anything after it that requires it, which is skipped; the rest still go
    // ahead. Every step acts on the fractal the session has selected, so a switch
    // part way stops before writing to the new one.
    async install(item) {
        if (this._busy.has(item.id)) return;
        const token = this.scopeToken();
        const steps = this.pending(item);
        const added = [], failed = new Map(), skipped = [];
        let message = '';
        for (const [i, step] of steps.entries()) {
            const blocker = this.order(step).find(r => r.id !== step.id && failed.has(r.id));
            if (blocker) {
                skipped.push(`${step.name} (needs ${blocker.name})`);
                continue;
            }
            this._busy.set(item.id, steps.length > 1 ? `Adding ${i + 1} of ${steps.length}...` : 'Adding...');
            this.render();
            try {
                const result = await this.installers[step.kind].call(this, step, token);
                if (step.kind === 'normalizer') this._normalizers = null;
                if (result.installed && !this.isStale(token)) this.installed.add(this.installedKey(step));
                added.push(step.name);
                message = result.message;
            } catch (err) {
                failed.set(step.id, `${step.name}: ${err.message.replace(/[.\s]*$/, '.')}`);
                if (this.isStale(token)) break;
            }
        }
        this._busy.delete(item.id);
        this.render();
        if (!failed.size) {
            Toast.success(`Added ${item.name}`, added.length > 1 ? `Added ${added.join(', ')}.` : message);
            return;
        }
        const parts = [...failed.values()];
        if (skipped.length) parts.push(`Skipped: ${skipped.join(', ')}.`);
        if (added.length) parts.push(`Added: ${added.join(', ')}.`);
        Toast.error(failed.size === steps.length ? `Could not add ${item.name}` : `Added ${added.length} of ${steps.length} for ${item.name}`, parts.join(' '));
    },

    // A null token posts without a scope check, for what is not added to a fractal.
    async post(token, url, body, contentType = 'application/json') {
        if (token !== null && this.isStale(token)) throw new Error('The fractal changed before this was added. Nothing more was added.');
        const resp = await fetch(url, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': contentType },
            body: contentType === 'application/json' ? JSON.stringify(body) : body,
        });
        const data = await resp.json().catch(() => ({}));
        return { resp, data };
    },

    async expectOK(request) {
        const { resp, data } = await request;
        if (!resp.ok || data.success === false) throw new Error(data.error || `HTTP ${resp.status}`);
        return data;
    },

    async postYAML(token, url, item) {
        const { content } = await this.fetchFile(item);
        return this.expectOK(this.post(token, url, content, 'application/yaml'));
    },

    installers: {
        // Not scoped: the dataset gets a fractal of its own.
        async dataset(item) {
            const data = await this.expectOK(this.post(null, `/api/v1/exchange/datasets/${encodeURIComponent(item.id)}/import`, {}));
            this.onImport(data.data);
            await window.FractalSelector?.loadAvailableFractals?.();
            return { installed: false, message: `Importing into fractal ${data.data.fractal_name}. Progress shows here.` };
        },

        async lookup(item, token) {
            await this.expectOK(this.post(token, '/api/v1/dictionaries', {
                name: item.id,
                description: item.description,
                kind: item.lookup.list_kind || 'value',
                source: item.lookup.source,
            }));
            return { installed: true, message: `Lookup ${item.id} is loading from ${new URL(item.lookup.source.url).host}.` };
        },

        async normalizer(item, token) {
            await this.postYAML(token, '/api/v1/normalizers/import', item);
            return { installed: true, message: 'Assign it to a fractal or ingest token to use it.' };
        },

        async model(item, token) {
            await this.postYAML(token, '/api/v1/models/import', item);
            return { installed: true, message: 'It builds from new logs. Run a backfill under Analytics for stored ones.' };
        },

        async dashboard(item, token) {
            await this.postYAML(token, '/api/v1/dashboards/import', item);
            return { installed: true, message: 'Find it under Dashboards.' };
        },

        async search(item, token) {
            const { search } = await this.fetchFile(item);
            await this.expectOK(this.post(token, '/api/v1/saved-queries', search));
            return { installed: true, message: 'Find it under Saved queries on the Query page.' };
        },

        async policy(item, token) {
            const { content } = await this.fetchFile(item);
            const data = await this.expectOK(this.post(token, '/api/v1/alert-policies/import', { content, replace: false }));
            return { installed: false, message: `${(data.data || []).length} policy rules in this scope now.` };
        },

        async detection(item, token) {
            const { content } = await this.fetchFile(item);
            const normalizerID = await this.normalizerFor(item);
            const { resp, data } = await this.post(token, '/api/v1/alerts/import', { yaml_content: content, normalizer_id: normalizerID });
            // A scope that reviews changes takes the rule as the caller's draft instead.
            if (resp.status === 409 && data?.data?.gate === 'required') {
                await this.expectOK(this.post(token, '/api/v1/alert-drafts/from-yaml', { content, normalizer_id: normalizerID }));
                return { installed: false, message: 'This scope reviews changes, so it was saved to your drafts.' };
            }
            if (resp.status === 422 && Array.isArray(data?.data)) {
                throw new Error(data.data.map(v => v.message).filter(Boolean).join(' ') || data.error || 'Blocked by policy');
            }
            if (!resp.ok || !data.success) throw new Error(data.error || `HTTP ${resp.status}`);
            return { installed: true, message: 'Find it under Alerts.' };
        },
    },

    // A Sigma detection maps its fields through the normalizer it requires, which is
    // added before it. Without one the instance default applies, as in a manual import.
    async normalizerFor(item) {
        if (!this._normalizers) {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers');
            this._normalizers = data.data || [];
        }
        const wanted = (item.requires || []).map(id => this.find(id)).find(it => it?.kind === 'normalizer');
        if (!wanted) return this._normalizers.find(n => n.is_default)?.id || '';
        const match = this._normalizers.find(n => n.name === wanted.name);
        if (!match) throw new Error(`Add the ${wanted.name} normalizer first.`);
        return match.id;
    },
};

window.Exchange = Exchange;

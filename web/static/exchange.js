// Exchange: browse shared content and add it through each kind's own import
// endpoint. The list on the right holds bundles, their items and the items in no
// bundle; searching flattens it. The selection fills the main pane on the left.
// Adding an item adds its requirements first; a bundle adds its members.
const Exchange = {
    KINDS: {
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
            const bundles = this.items.filter(it => it.kind === 'bundle');
            const loose = this.items.filter(it => it.kind !== 'bundle' && !this.bundlesOf(it).length);
            html = `<div class="exchange-list-label">Bundles</div>`
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
        const by = /^https?:\/\//i.test(it.homepage || '')
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
        if (this.isInstalled(it)) return '';
        const steps = this.pending(it);
        let label = 'Add';
        if (it.kind === 'bundle') label = steps.length === this.order(it).length ? `Add all (${steps.length})` : `Add ${steps.length} more`;
        return `<button type="button" class="btn-primary" data-action="install" data-id="${Utils.escapeAttr(it.id)}">${label}</button>`;
    },

    // Adding more than one thing says what it adds and skips before doing it.
    confirmHtml(it) {
        if (this._confirm !== it.id) return '';
        const esc = Utils.escapeHtml;
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
                if (this.pending(item).length > 1) {
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

    async post(token, url, body, contentType = 'application/json') {
        if (this.isStale(token)) throw new Error('The fractal changed before this was added. Nothing more was added.');
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

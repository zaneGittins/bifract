// Exchange: browse shared content and add it through each kind's own import
// endpoint. Adding an item adds its requirements first; a bundle adds its members.
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
    _busy: new Map(),
    _open: null,
    _files: {},
    _errors: {},
    _normalizers: null,
    _bound: false,

    init() {
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('Exchange', () => {
                this._open = null;
                this.installed = new Set();
                if (FractalContext.shouldReload('exchangeView')) this.load();
            });
        }
    },

    show() {
        this.bindEvents();
        this.load();
    },

    bindEvents() {
        if (this._bound) return;
        this._bound = true;
        document.getElementById('exchangeSearchInput')?.addEventListener('input', () => this.render());
        document.getElementById('exchangeKindFilter')?.addEventListener('change', () => this.render());
        document.getElementById('exchangeTableBody')?.addEventListener('click', (e) => this.onTableClick(e));
    },

    scopeToken() {
        return window.FractalContext?.scopeToken?.();
    },

    isStale(token) {
        return !!window.FractalContext?.isScopeStale?.(token);
    },

    async load() {
        const token = this.scopeToken();
        this.setStatus('Loading the Exchange...');
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
            this.setSource(listing.source);
            document.getElementById('exchangeStatus').hidden = true;
            this.render();
        } catch (e) {
            if (this.isStale(token)) return;
            console.error('Failed to load the Exchange:', e);
            this.items = [];
            this.setStatus(`The Exchange could not be loaded: ${e.message}`);
        }
    },

    setStatus(text) {
        const status = document.getElementById('exchangeStatus');
        if (!status) return;
        status.hidden = false;
        status.querySelector('p').textContent = text;
        document.getElementById('exchangeTableContainer').hidden = true;
        document.getElementById('exchangeEmpty').hidden = true;
    },

    setSource(url) {
        const el = document.getElementById('exchangeSource');
        if (!el) return;
        el.textContent = url ? `Source: ${url}` : '';
        el.title = url || '';
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

    isInstalled(item) {
        if (item.kind === 'bundle') return item.items.every(id => this.isInstalled(this.find(id)));
        const key = this.installedKey(item);
        return !!key && this.installed.has(key);
    },

    // What adding an item adds, in order: requirements first, each once, skipping
    // what is already here. A bundle adds its members that way.
    pending(item) {
        const order = [];
        const seen = new Set();
        const visit = (it) => {
            if (!it || seen.has(it.id)) return;
            seen.add(it.id);
            (it.requires || []).forEach(id => visit(this.find(id)));
            if (it.kind === 'bundle') it.items.forEach(id => visit(this.find(id)));
            else if (!this.isInstalled(it)) order.push(it);
        };
        visit(item);
        return order;
    },

    bundlesOf(item) {
        return this.items.filter(b => b.kind === 'bundle' && b.items.includes(item.id));
    },

    filtered() {
        const q = (document.getElementById('exchangeSearchInput')?.value || '').toLowerCase().trim();
        const kind = document.getElementById('exchangeKindFilter')?.value || '';
        const matches = this.items.filter(it => {
            if (kind && it.kind !== kind) return false;
            if (!q) return true;
            return [it.name, it.description, it.author, ...(it.tags || [])]
                .some(v => (v || '').toLowerCase().includes(q));
        });
        // Bundles lead: they are the quickest way to a working setup.
        return [...matches.filter(it => it.kind === 'bundle'), ...matches.filter(it => it.kind !== 'bundle')];
    },

    render() {
        const tbody = document.getElementById('exchangeTableBody');
        if (!tbody) return;
        // The rows are rebuilt, so keyboard focus is carried over to the same control.
        const focused = document.activeElement?.closest?.('#exchangeTableBody [data-action]');
        const items = this.filtered();
        document.getElementById('exchangeTableContainer').hidden = items.length === 0;
        document.getElementById('exchangeEmpty').hidden = items.length > 0 || this.items.length === 0;
        tbody.innerHTML = items.map(it => this.rowHtml(it)).join('');
        if (focused) {
            tbody.querySelector(`[data-action="${focused.dataset.action}"][data-id="${CSS.escape(focused.dataset.id)}"]`)?.focus();
        }
    },

    rowHtml(it) {
        const esc = Utils.escapeHtml, attr = Utils.escapeAttr;
        const kind = this.KINDS[it.kind];
        const open = this._open === it.id;
        const tags = (it.tags || []).map(t => `<span class="exchange-tag">${esc(t)}</span>`).join('');
        const bundles = this.bundlesOf(it).map(b => `<span class="exchange-bundle-chip">${esc(b.name)}</span>`).join('');
        const author = esc(it.author || 'Bifract');
        const by = /^https?:\/\//i.test(it.homepage || '')
            ? `<a href="${attr(it.homepage)}" target="_blank" rel="noopener noreferrer">${author}</a>`
            : author;
        let row = `
<tr class="exchange-row${open ? ' open' : ''}${it.kind === 'bundle' ? ' exchange-bundle-row' : ''}">
    <td class="exchange-col-name">
        <button type="button" class="exchange-name" data-action="toggle" data-id="${attr(it.id)}" aria-expanded="${open}"${open ? ` aria-controls="exchange-preview-${attr(it.id)}"` : ''}>${esc(it.name)}</button>
        <div class="exchange-desc">${esc(it.description || '')}</div>
        ${tags || bundles ? `<div class="exchange-tags">${bundles}${tags}</div>` : ''}
    </td>
    <td class="exchange-col-meta" title="${attr(kind.hint)}">${esc(kind.label)}</td>
    <td class="exchange-col-meta">${by}</td>
    <td class="exchange-col-action">${this.actionHtml(it)}</td>
</tr>`;
        if (open) {
            row += `<tr class="exchange-preview-row" id="exchange-preview-${attr(it.id)}"><td colspan="4">${this.previewHtml(it)}</td></tr>`;
        }
        return row;
    },

    actionHtml(it) {
        const attr = Utils.escapeAttr;
        const busy = this._busy.get(it.id);
        if (busy) return `<button type="button" class="btn-primary btn-sm" disabled>${Utils.escapeHtml(busy)}</button>`;
        if (this.isInstalled(it)) return '<span class="exchange-installed">Installed</span>';
        const steps = this.pending(it);
        let label = 'Add';
        if (it.kind === 'bundle') label = steps.length === it.items.length ? 'Add all' : `Add ${steps.length} more`;
        else if (steps.length > 1) label = `Add with ${steps.length - 1} more`;
        const title = steps.length > 1 ? ` title="${attr('Adds ' + steps.map(s => s.name).join(', '))}"` : '';
        return `<button type="button" class="btn-primary btn-sm" data-action="install" data-id="${attr(it.id)}"${title}>${Utils.escapeHtml(label)}</button>`;
    },

    previewHtml(it) {
        const esc = Utils.escapeHtml;
        const hint = `<p class="exchange-preview-hint">${esc(this.KINDS[it.kind].hint)}</p>`;
        if (it.kind === 'bundle') {
            const rows = it.items.map(id => this.find(id)).filter(Boolean).map(m => `
                <li><span class="exchange-member-kind">${esc(this.KINDS[m.kind].label)}</span>
                    <span class="exchange-member-name">${esc(m.name)}</span>
                    ${this.isInstalled(m) ? '<span class="exchange-installed">Installed</span>' : ''}</li>`).join('');
            return hint + `<ul class="exchange-members">${rows}</ul>`;
        }
        const steps = this.pending(it).filter(s => s.id !== it.id);
        const first = steps.length
            ? `<p class="exchange-preview-hint">Adds first: ${steps.map(s => esc(s.name)).join(', ')}.</p>`
            : '';
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
            return hint + first + `<dl class="exchange-source">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
        }
        if (this._errors[it.id]) return hint + first + `<p class="exchange-error">${esc(this._errors[it.id])}</p>`;
        const file = this._files[it.id];
        if (!file) return hint + first + '<p class="exchange-preview-hint">Loading...</p>';
        return hint + first + `<pre class="exchange-content yaml-code">${Utils.highlightYAML(file.content)}</pre>`;
    },

    every(minutes) {
        for (const [size, unit] of [[1440, 'day'], [60, 'hour'], [1, 'minute']]) {
            if (minutes % size === 0) {
                const n = minutes / size;
                return `${n} ${unit}${n === 1 ? '' : 's'}`;
            }
        }
    },

    onTableClick(e) {
        const el = e.target.closest('[data-action]');
        if (!el) return;
        const item = this.find(el.dataset.id);
        if (!item) return;
        if (el.dataset.action === 'toggle') this.toggle(item);
        else if (el.dataset.action === 'install') this.install(item);
    },

    async toggle(item) {
        this._open = this._open === item.id ? null : item.id;
        // A failed preview is tried again each time it is opened.
        delete this._errors[item.id];
        this.render();
        if (this._open !== item.id || item.kind === 'lookup' || item.kind === 'bundle' || this._files[item.id]) return;
        try {
            await this.fetchFile(item);
        } catch (err) {
            this._errors[item.id] = err.message;
        }
        this.render();
    },

    // An item's file, and for a search the parsed search the server sends with it.
    async fetchFile(item) {
        if (!this._files[item.id]) {
            const data = (await HttpUtils.safeFetch(`/api/v1/exchange/items/${encodeURIComponent(item.id)}`)).data || {};
            this._files[item.id] = { content: data.content ?? '', search: data.search };
        }
        return this._files[item.id];
    },

    // Adds an item's pending steps in order and stops at the first failure, since
    // what follows may need it. Every step acts on the fractal the session has
    // selected, so a switch part way stops before writing to the new one.
    async install(item) {
        if (this._busy.has(item.id)) return;
        const token = this.scopeToken();
        const steps = this.pending(item);
        const added = [];
        let message = '';
        try {
            for (const [i, step] of steps.entries()) {
                this._busy.set(item.id, steps.length > 1 ? `Adding ${i + 1} of ${steps.length}...` : 'Adding...');
                this.render();
                const result = await this.installers[step.kind].call(this, step, token);
                if (step.kind === 'normalizer') this._normalizers = null;
                if (result.installed && !this.isStale(token)) this.installed.add(this.installedKey(step));
                added.push(step.name);
                message = result.message;
            }
            Toast.success(`Added ${item.name}`, added.length > 1 ? `Added ${added.join(', ')}.` : message);
        } catch (err) {
            const failed = steps[added.length];
            const done = added.length ? ` Added before it: ${added.join(', ')}.` : '';
            Toast.error(`Could not add ${failed ? failed.name : item.name}`, err.message + done);
        } finally {
            this._busy.delete(item.id);
            this.render();
        }
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

// Exchange: browse shared lookups, detections, normalizers and policies, and add
// them through each kind's own import endpoint.
const Exchange = {
    KINDS: {
        lookup: { label: 'Lookup', hint: 'Added to this fractal. Rows refresh from the upstream URL. Needs a tenant admin.' },
        detection: { label: 'Detection', hint: 'Added as an alert in this fractal, or as a draft when changes are reviewed.' },
        normalizer: { label: 'Normalizer', hint: 'Added instance-wide. Needs a tenant admin.' },
        policy: { label: 'Policy', hint: 'Appends its rules to this fractal\'s alert policies, skipping any already there. Needs a fractal admin.' },
    },

    items: [],
    installed: new Set(),
    _busy: new Set(),
    _open: null,
    _content: {},
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
            const installed = await this.loadInstalled();
            if (this.isStale(token)) return;
            this.items = listing.items || [];
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

    // Names already present in this scope, keyed "kind:name". Each list is read with
    // the caller's own access, so one the caller cannot read just shows nothing.
    // Policies have no names, so they are never shown as installed; adding one again
    // skips the rules already present.
    async loadInstalled() {
        const names = new Set();
        const read = async (url, kind, key) => {
            try {
                const data = await HttpUtils.safeFetch(url);
                for (const row of data.data || []) names.add(`${kind}:${row[key]}`);
            } catch (_) { /* not readable at this access level */ }
        };
        await Promise.all([
            read('/api/v1/dictionaries', 'lookup', 'name'),
            read('/api/v1/normalizers', 'normalizer', 'name'),
            read('/api/v1/alerts?limit=2000', 'detection', 'name'),
        ]);
        this._normalizers = null;
        return names;
    },

    // The name an item is installed under: a lookup by its id, the rest by name.
    installedKey(item) {
        return `${item.kind}:${item.kind === 'lookup' ? item.id : item.name}`;
    },

    filtered() {
        const q = (document.getElementById('exchangeSearchInput')?.value || '').toLowerCase().trim();
        const kind = document.getElementById('exchangeKindFilter')?.value || '';
        return this.items.filter(it => {
            if (kind && it.kind !== kind) return false;
            if (!q) return true;
            return [it.name, it.description, it.author, ...(it.tags || [])]
                .some(v => (v || '').toLowerCase().includes(q));
        });
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
        const installed = this.installed.has(this.installedKey(it));
        const busy = this._busy.has(it.id);
        const open = this._open === it.id;
        const tags = (it.tags || []).map(t => `<span class="exchange-tag">${esc(t)}</span>`).join('');
        const author = esc(it.author || 'Bifract');
        const by = /^https?:\/\//i.test(it.homepage || '')
            ? `<a href="${attr(it.homepage)}" target="_blank" rel="noopener noreferrer">${author}</a>`
            : author;
        const action = installed
            ? '<span class="exchange-installed">Installed</span>'
            : `<button type="button" class="btn-primary btn-sm" data-action="install" data-id="${attr(it.id)}"${busy ? ' disabled' : ''}>${busy ? 'Adding...' : 'Add'}</button>`;
        let row = `
<tr class="exchange-row${open ? ' open' : ''}">
    <td class="exchange-col-name">
        <button type="button" class="exchange-name" data-action="toggle" data-id="${attr(it.id)}" aria-expanded="${open}"${open ? ` aria-controls="exchange-preview-${attr(it.id)}"` : ''}>${esc(it.name)}</button>
        <div class="exchange-desc">${esc(it.description || '')}</div>
        ${tags ? `<div class="exchange-tags">${tags}</div>` : ''}
    </td>
    <td class="exchange-col-meta" title="${attr(kind.hint)}">${esc(kind.label)}</td>
    <td class="exchange-col-meta">${by}</td>
    <td class="exchange-col-action">${action}</td>
</tr>`;
        if (open) {
            row += `<tr class="exchange-preview-row" id="exchange-preview-${attr(it.id)}"><td colspan="4">${this.previewHtml(it)}</td></tr>`;
        }
        return row;
    },

    previewHtml(it) {
        const esc = Utils.escapeHtml;
        const hint = `<p class="exchange-preview-hint">${esc(this.KINDS[it.kind].hint)}</p>`;
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
            return hint + `<dl class="exchange-source">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
        }
        if (this._errors[it.id]) return hint + `<p class="exchange-error">${esc(this._errors[it.id])}</p>`;
        const content = this._content[it.id];
        if (content === undefined) return hint + '<p class="exchange-preview-hint">Loading...</p>';
        return hint + `<pre class="exchange-content yaml-code">${Utils.highlightYAML(content)}</pre>`;
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
        const item = this.items.find(it => it.id === el.dataset.id);
        if (!item) return;
        if (el.dataset.action === 'toggle') this.toggle(item);
        else if (el.dataset.action === 'install') this.install(item);
    },

    async toggle(item) {
        this._open = this._open === item.id ? null : item.id;
        // A failed preview is tried again each time it is opened.
        delete this._errors[item.id];
        this.render();
        if (this._open !== item.id || item.kind === 'lookup' || this._content[item.id] !== undefined) return;
        try {
            await this.fetchContent(item);
        } catch (err) {
            this._errors[item.id] = err.message;
        }
        this.render();
    },

    async fetchContent(item) {
        if (this._content[item.id] === undefined) {
            const data = await HttpUtils.safeFetch(`/api/v1/exchange/items/${encodeURIComponent(item.id)}`);
            this._content[item.id] = data.data?.content ?? '';
        }
        return this._content[item.id];
    },

    // Installs act on the fractal the session has selected, so one started in another
    // fractal stops before writing anything and is never marked installed here.
    async install(item) {
        if (this._busy.has(item.id)) return;
        const token = this.scopeToken();
        this._busy.add(item.id);
        this.render();
        try {
            const { message, installed } = await this.installers[item.kind].call(this, item, token);
            if (item.kind === 'normalizer') this._normalizers = null;
            if (installed && !this.isStale(token)) this.installed.add(this.installedKey(item));
            Toast.success(`Added ${item.name}`, message);
        } catch (err) {
            Toast.error(`Could not add ${item.name}`, err.message);
        } finally {
            this._busy.delete(item.id);
            this.render();
        }
    },

    async post(token, url, body, contentType = 'application/json') {
        if (this.isStale(token)) throw new Error('The fractal changed before this was added. Nothing was added.');
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
            const content = await this.fetchContent(item);
            await this.expectOK(this.post(token, '/api/v1/normalizers/import', content, 'application/yaml'));
            return { installed: true, message: 'Assign it to a fractal or ingest token to use it.' };
        },

        async policy(item, token) {
            const content = await this.fetchContent(item);
            const data = await this.expectOK(this.post(token, '/api/v1/alert-policies/import', { content, replace: false }));
            return { installed: false, message: `${(data.data || []).length} policy rules in this scope now.` };
        },

        async detection(item, token) {
            const content = await this.fetchContent(item);
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

    // A Sigma detection names the Exchange normalizer its fields map through; it must
    // be installed first. Otherwise the instance default applies, as in a manual import.
    async normalizerFor(item) {
        if (!this._normalizers) {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers');
            this._normalizers = data.data || [];
        }
        if (!item.normalizer) return this._normalizers.find(n => n.is_default)?.id || '';
        const wanted = this.items.find(it => it.id === item.normalizer);
        const match = this._normalizers.find(n => n.name === wanted?.name);
        if (!match) throw new Error(`Add the ${wanted?.name || item.normalizer} normalizer first.`);
        return match.id;
    },
};

window.Exchange = Exchange;

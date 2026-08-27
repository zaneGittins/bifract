// The API explorer. It reads the description the running server generates from
// its own router, so what this page shows is what this build serves: there is no
// second copy of the contract to drift.
const APIExplorer = {
    doc: null,
    ops: [],
    selected: null,
    loaded: false,
    scopes: [],
    scope: '',

    // The explorer is reached outside any fractal, so FractalContext is empty
    // here and cannot supply a scope. The choice lives with the page instead.
    SCOPE_KEY: 'bifract.apiExplorer.scope',

    async show() {
        this._bind();
        if (!this.loaded) {
            await Promise.all([this.load(), this.loadScopes()]);
        }
    },

    _bind() {
        const search = document.getElementById('apiExplorerSearch');
        if (search && !search.dataset.bound) {
            search.dataset.bound = '1';
            search.addEventListener('input', () => this.renderList());
        }
        const scope = document.getElementById('apiExplorerScopeSelect');
        if (scope && !scope.dataset.bound) {
            scope.dataset.bound = '1';
            scope.addEventListener('change', () => {
                this.scope = scope.value;
                try { localStorage.setItem(this.SCOPE_KEY, this.scope); } catch (_) { /* private mode */ }
                this._markScope();
                if (this.selected) this.renderDetail();
            });
        }
    },

    // Fractals and prisms the caller may act on, for the scope selector.
    async loadScopes() {
        try {
            const res = await fetch('/api/v1/fractals', { credentials: 'include' });
            if (!res.ok) return;
            const body = await res.json();
            const data = body.data || {};
            const fractals = (data.fractals || []).map(f => ({ value: `fractal:${f.id}`, label: `fractal · ${f.name}` }));
            const prisms = (data.prisms || []).map(p => ({ value: `prism:${p.id}`, label: `prism · ${p.name}` }));
            this.scopes = [...fractals, ...prisms];
        } catch (_) {
            this.scopes = [];
        }

        try { this.scope = localStorage.getItem(this.SCOPE_KEY) || ''; } catch (_) { this.scope = ''; }
        if (this.scope && !this.scopes.some(s => s.value === this.scope)) this.scope = '';
        if (!this.scope && this.scopes.length) this.scope = this.scopes[0].value;

        const sel = document.getElementById('apiExplorerScopeSelect');
        if (sel) {
            sel.innerHTML = [`<option value="">no scope header</option>`]
                .concat(this.scopes.map(s =>
                    `<option value="${Utils.escapeHtml(s.value)}"${s.value === this.scope ? ' selected' : ''}>${Utils.escapeHtml(s.label)}</option>`))
                .join('');
        }
        this._markScope();
    },

    // A missing scope is the usual cause of a confusing 400, so make it visible.
    _markScope() {
        const box = document.getElementById('apiExplorerScope');
        if (box) box.classList.toggle('api-scope-missing', !this.scope && this.scopes.length > 0);
    },

    async load() {
        const list = document.getElementById('apiExplorerList');
        try {
            const res = await fetch('/api/v1/openapi.json', { credentials: 'include' });
            if (!res.ok) throw new Error(await Utils.errorMessage(res, 'Failed to load the API description'));
            this.doc = await res.json();
            this.ops = this._index(this.doc);
            this.loaded = true;

            this.renderList();
            this.renderEmpty();
        } catch (err) {
            if (list) list.innerHTML = `<div class="error">${Utils.escapeHtml(err.message)}</div>`;
        }
    },

    _index(doc) {
        const out = [];
        for (const [path, item] of Object.entries(doc.paths || {})) {
            for (const method of ['get', 'post', 'put', 'patch', 'delete']) {
                const op = item[method];
                if (!op) continue;
                out.push({
                    method: method.toUpperCase(),
                    path,
                    op,
                    tag: (op.tags && op.tags[0]) || 'other',
                    id: op.operationId,
                });
            }
        }
        return out.sort((a, b) =>
            a.tag.localeCompare(b.tag) || a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
    },

    filtered() {
        const term = (document.getElementById('apiExplorerSearch')?.value || '').toLowerCase().trim();
        if (!term) return this.ops;
        return this.ops.filter(o =>
            o.path.toLowerCase().includes(term) ||
            o.method.toLowerCase().includes(term) ||
            o.tag.toLowerCase().includes(term) ||
            (o.op.summary || '').toLowerCase().includes(term));
    },

    renderList() {
        const list = document.getElementById('apiExplorerList');
        if (!list) return;

        const ops = this.filtered();
        if (!ops.length) {
            list.innerHTML = '<div class="api-explorer-empty">Nothing matches.</div>';
            return;
        }

        let html = '';
        let tag = null;
        for (const o of ops) {
            if (o.tag !== tag) {
                tag = o.tag;
                html += `<div class="api-tag-head">${Utils.escapeHtml(tag)}</div>`;
            }
            const active = this.selected === o.id ? ' active' : '';
            html += `
                <button class="api-op${active}" data-op="${Utils.escapeHtml(o.id)}"
                        onclick="APIExplorer.select('${Utils.escapeHtml(o.id)}')">
                    <span class="api-method api-method-${o.method.toLowerCase()}">${o.method}</span>
                    <span class="api-op-path">${Utils.escapeHtml(o.path.replace('/api/v1', ''))}</span>
                </button>`;
        }
        list.innerHTML = html;
    },

    select(id) {
        this.selected = id;
        this._remember(id);
        this.renderList();
        this.renderDetail();
    },

    RECENT_KEY: 'bifract.apiExplorer.recent',

    _remember(id) {
        try {
            const seen = [id, ...this.recent().filter(x => x !== id)].slice(0, 5);
            localStorage.setItem(this.RECENT_KEY, JSON.stringify(seen));
        } catch (_) { /* private mode: recents are a convenience, not a feature */ }
    },

    recent() {
        try {
            const raw = JSON.parse(localStorage.getItem(this.RECENT_KEY) || '[]');
            return Array.isArray(raw) ? raw.filter(id => this.ops.some(o => o.id === id)) : [];
        } catch (_) {
            return [];
        }
    },

    // Operations worth starting from. Matched by path so a renamed operationId
    // does not silently empty the list.
    QUICK_STARTS: [
        ['POST', '/api/v1/query', 'Run a BQL query'],
        ['POST', '/api/v1/ingest', 'Send logs'],
        ['GET', '/api/v1/alerts', 'List detections'],
        ['GET', '/api/v1/fractals', 'List fractals'],
    ],

    // The landing pane. The page exists to teach the API, so an unselected state
    // should answer "how do I call this at all", not wait for a click.
    renderEmpty() {
        const el = document.getElementById('apiExplorerDetail');
        if (!el || this.selected) return;

        const origin = window.location.origin;
        // Two examples, because the difference is the thing people trip on:
        // /health is public and takes no credential, so an auth header on it
        // would teach the wrong shape. Everything else needs both headers.
        const probe = `curl '${origin}/api/v1/health'`;
        const authed = [
            `curl '${origin}/api/v1/alerts'`,
            `  -H 'Authorization: Bearer bifract_<your-api-key>'`,
            `  -H 'X-Bifract-Scope: ${this.scope || 'fractal:<fractal-id>'}'`,
        ].join(' \\\n');

        const link = (o, label) => `
            <button class="api-quick" onclick="APIExplorer.select('${Utils.escapeHtml(o.id)}')">
                <span class="api-method api-method-${o.method.toLowerCase()}">${o.method}</span>
                <span class="api-quick-path">${Utils.escapeHtml(o.path.replace('/api/v1', ''))}</span>
                <span class="api-quick-label">${Utils.escapeHtml(label)}</span>
            </button>`;

        const quick = this.QUICK_STARTS
            .map(([m, p, label]) => {
                const o = this.ops.find(x => x.method === m && x.path === p);
                return o ? link(o, label) : '';
            }).join('');

        const recent = this.recent()
            .map(id => this.ops.find(o => o.id === id))
            .filter(Boolean)
            .map(o => link(o, o.op.summary || '')).join('');

        el.innerHTML = `
            <div class="api-landing">
                <div class="api-section">
                    <h4>Connect</h4>
                    <p class="api-detail-access">Every request needs a key and, for anything scoped to data, the scope header. This page uses your session instead of a key, so you can try an operation here and copy a curl that works from anywhere.</p>
                    ${this._copyRow('Base URL', `${origin}/api/v1`)}
                    ${this._copyRow('Authorization', 'Bearer bifract_<your-api-key>')}
                    ${this._copyRow('Scope', this.scope || 'fractal:<fractal-id>')}
                </div>

                <div class="api-section">
                    <h4>Check you can reach it</h4>
                    <p class="api-detail-access">Public, so it needs no credential.</p>
                    ${this._copyBlock(probe)}
                </div>

                <div class="api-section">
                    <h4>A real call</h4>
                    <p class="api-detail-access">Everything else needs a key, and anything scoped to data needs the scope header.</p>
                    ${this._copyBlock(authed)}
                </div>

                ${quick ? `<div class="api-section"><h4>Start here</h4><div class="api-quick-list">${quick}</div></div>` : ''}
                ${recent ? `<div class="api-section"><h4>Recently viewed</h4><div class="api-quick-list">${recent}</div></div>` : ''}

                <p class="api-landing-hint">Search with the box above, or pick an operation on the left.</p>
            </div>`;
    },

    // A multi-line example, copied whole rather than a line at a time.
    _copyBlock(text) {
        return `
            <div class="api-copy-block">
                <pre>${Utils.escapeHtml(text)}</pre>
                <button class="btn-secondary btn-sm" onclick="APIExplorer.copy(this)" data-copy="${Utils.escapeHtml(text)}">Copy</button>
            </div>`;
    },

    _copyRow(label, value) {
        return `
            <div class="api-copy-row">
                ${label ? `<span class="api-copy-label">${Utils.escapeHtml(label)}</span>` : ''}
                <code>${Utils.escapeHtml(value)}</code>
                <button class="btn-secondary btn-sm" onclick="APIExplorer.copy(this)" data-copy="${Utils.escapeHtml(value)}">Copy</button>
            </div>`;
    },

    copy(btn) {
        const text = btn?.dataset?.copy;
        if (!text) return;
        navigator.clipboard?.writeText(text)
            .then(() => window.Toast && Toast.success('Copied', 'Copied to the clipboard'))
            .catch(() => window.Toast && Toast.error('Copy failed', 'Select and copy manually'));
    },

    current() {
        return this.ops.find(o => o.id === this.selected);
    },

    renderDetail() {
        const el = document.getElementById('apiExplorerDetail');
        const entry = this.current();
        if (!el || !entry) return;

        const { method, path, op } = entry;
        const params = (op.parameters || []).filter(p => p.in === 'path');
        const query = (op.parameters || []).filter(p => p.in === 'query');
        const bodySchema = op.requestBody?.content?.['application/json']?.schema;
        const example = bodySchema ? JSON.stringify(this.example(bodySchema), null, 2) : '';

        el.innerHTML = `
            <div class="api-detail-head">
                <span class="api-method api-method-${method.toLowerCase()}">${method}</span>
                <code class="api-detail-path">${Utils.escapeHtml(path)}</code>
            </div>
            ${op.summary ? `<p class="api-detail-summary">${Utils.escapeHtml(op.summary)}</p>` : ''}
            ${op.description ? `<p class="api-detail-access">${Utils.escapeHtml(op.description)}</p>` : ''}

            ${params.length ? `
                <div class="api-section">
                    <h4>Path parameters</h4>
                    ${params.map(p => `
                        <label class="api-param">
                            <span>${Utils.escapeHtml(p.name)}</span>
                            <input type="text" class="admin-search" data-param="${Utils.escapeHtml(p.name)}"
                                   placeholder="${Utils.escapeHtml(p.name)}">
                        </label>`).join('')}
                </div>` : ''}

            ${query.length ? `
                <div class="api-section">
                    <h4>Query parameters</h4>
                    ${query.map(p => `
                        <label class="api-param">
                            <span>${Utils.escapeHtml(p.name)}${p.schema?.type && p.schema.type !== 'string' ? ` <em>${Utils.escapeHtml(p.schema.type)}</em>` : ''}</span>
                            <input type="text" class="admin-search" data-query="${Utils.escapeHtml(p.name)}"
                                   placeholder="${Utils.escapeHtml(p.description || 'optional')}">
                        </label>`).join('')}
                </div>` : ''}

            ${bodySchema ? `
                <div class="api-section">
                    <h4>Request body</h4>
                    <textarea id="apiExplorerBody" class="api-body" spellcheck="false" rows="10">${Utils.escapeHtml(example)}</textarea>
                </div>` : ''}

            <div class="api-actions">
                <button class="btn-primary btn-sm" onclick="APIExplorer.execute()">Execute</button>
                <button class="btn-secondary btn-sm" onclick="APIExplorer.copyCurl()">Copy as curl</button>
                ${this.isMutating(method) ? '<span class="api-warn">Runs against this instance’s real data.</span>' : ''}
            </div>

            <div class="api-section" id="apiExplorerResult" style="display:none;"></div>

            <div class="api-section">
                <h4>Responses</h4>
                ${Object.entries(op.responses || {}).map(([code, r]) => `
                    <div class="api-response">
                        <span class="api-code api-code-${code[0]}xx">${code}</span>
                        <span>${Utils.escapeHtml(r.description || '')}</span>
                        ${Object.keys(r.content || {}).map(m => `<code class="api-media">${Utils.escapeHtml(m)}</code>`).join('')}
                    </div>`).join('')}
            </div>`;
    },

    isMutating(method) {
        return method !== 'GET';
    },

    // Resolve a $ref against the document's components.
    resolve(schema, depth = 0) {
        if (!schema || depth > 8) return {};
        if (schema.$ref) {
            const name = schema.$ref.replace('#/components/schemas/', '');
            return this.resolve(this.doc.components?.schemas?.[name], depth + 1);
        }
        return schema;
    },

    // A body worth editing beats an empty box: build a skeleton from the schema.
    example(schema, depth = 0) {
        const s = this.resolve(schema, depth);
        if (depth > 6) return null;
        switch (s.type) {
            case 'object': {
                const out = {};
                for (const [k, v] of Object.entries(s.properties || {})) {
                    out[k] = this.example(v, depth + 1);
                }
                return out;
            }
            case 'array':
                return s.items ? [this.example(s.items, depth + 1)] : [];
            case 'string':
                return s.format === 'date-time' ? new Date().toISOString() : '';
            case 'integer':
            case 'number':
                return 0;
            case 'boolean':
                return false;
        }
        return null;
    },

    // Substitute the path parameters and append whatever query values were filled
    // in. Blank query inputs are left off rather than sent empty, which for most
    // handlers is not the same thing as absent.
    _url() {
        const entry = this.current();
        let url = entry.path;
        document.querySelectorAll('#apiExplorerDetail [data-param]').forEach(input => {
            url = url.replace(`{${input.dataset.param}}`, encodeURIComponent(input.value || ''));
        });

        const qs = new URLSearchParams();
        document.querySelectorAll('#apiExplorerDetail [data-query]').forEach(input => {
            const v = input.value.trim();
            if (v) qs.set(input.dataset.query, v);
        });
        const suffix = qs.toString();
        return suffix ? `${url}?${suffix}` : url;
    },

    _body() {
        const el = document.getElementById('apiExplorerBody');
        return el ? el.value.trim() : '';
    },

    _scopeHeader() {
        return this.scope ? { 'X-Bifract-Scope': this.scope } : {};
    },

    async execute() {
        const entry = this.current();
        if (!entry) return;

        const url = this._url();
        if (url.includes('{')) {
            if (window.Toast) Toast.error('Missing parameter', 'Fill in every path parameter first');
            return;
        }
        if (this.isMutating(entry.method) &&
            !confirm(`${entry.method} ${url}\n\nThis runs against this instance's real data. Continue?`)) {
            return;
        }

        const result = document.getElementById('apiExplorerResult');
        if (result) {
            result.style.display = '';
            result.innerHTML = '<h4>Result</h4><div class="loading">Running...</div>';
        }

        const started = performance.now();
        try {
            const body = this._body();
            const res = await fetch(url, {
                method: entry.method,
                credentials: 'include',
                headers: {
                    ...(body ? { 'Content-Type': 'application/json' } : {}),
                    ...this._scopeHeader(),
                },
                body: body || undefined,
            });
            const took = Math.round(performance.now() - started);
            const text = await res.text();
            let pretty = text;
            try {
                pretty = JSON.stringify(JSON.parse(text), null, 2);
            } catch (_) { /* not JSON: show it raw */ }

            if (result) {
                result.innerHTML = `
                    <h4>Result</h4>
                    <div class="api-result-head">
                        <span class="api-code api-code-${String(res.status)[0]}xx">${res.status}</span>
                        <span class="text-muted">${took} ms · ${(text.length / 1024).toFixed(1)} KB</span>
                    </div>
                    <pre class="api-result-body">${Utils.escapeHtml(pretty.slice(0, 20000))}</pre>
                    ${pretty.length > 20000 ? '<p class="text-muted">Truncated for display.</p>' : ''}`;
            }
        } catch (err) {
            if (result) {
                result.innerHTML = `<h4>Result</h4><div class="error">${Utils.escapeHtml(err.message)}</div>`;
            }
        }
    },

    curl() {
        const entry = this.current();
        if (!entry) return '';
        const parts = [`curl -X ${entry.method} '${window.location.origin}${this._url()}'`];
        parts.push(`  -H 'Authorization: Bearer <your-api-key>'`);
        if (this.scope) parts.push(`  -H 'X-Bifract-Scope: ${this.scope}'`);
        const body = this._body();
        if (body) {
            parts.push(`  -H 'Content-Type: application/json'`);
            parts.push(`  -d '${body.replace(/'/g, `'\\''`)}'`);
        }
        return parts.join(' \\\n');
    },

    copyCurl() {
        const text = this.curl();
        if (!text) return;
        navigator.clipboard?.writeText(text)
            .then(() => window.Toast && Toast.success('Copied', 'curl command copied'))
            .catch(() => window.Toast && Toast.error('Copy failed', 'Select and copy manually'));
    },
};

window.APIExplorer = APIExplorer;

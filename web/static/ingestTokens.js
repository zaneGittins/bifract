// IngestTokens module for Bifract
const IngestTokens = {
    tokens: [],
    filteredTokens: [],
    availableNormalizers: [],
    showCreateForm: false,
    revealedToken: null,
    editingToken: null,
    currentPage: 0,
    pageSize: 10,
    searchQuery: '',
    debounceTimer: null,

    init() {
        this.tokens = [];
        this.filteredTokens = [];
        this.showCreateForm = false;
        this.revealedToken = null;
        this.editingToken = null;
        this.currentPage = 0;
        this.searchQuery = '';
        this.bindEvents();
    },

    bindEvents() {
        const createBtn = document.getElementById('createIngestTokenBtn');
        if (createBtn) {
            createBtn.addEventListener('click', () => this.toggleCreateForm());
        }

        const refreshBtn = document.getElementById('ingestTokensRefreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.loadTokens());
        }

        const searchInput = document.getElementById('ingestTokenSearchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => this.handleSearch(e.target.value));
        }

        const prevBtn = document.getElementById('ingestTokensPrevBtn');
        if (prevBtn) {
            prevBtn.addEventListener('click', () => this.previousPage());
        }

        const nextBtn = document.getElementById('ingestTokensNextBtn');
        if (nextBtn) {
            nextBtn.addEventListener('click', () => this.nextPage());
        }
    },

    show() {
        this.loadNormalizersList();
        this.loadTokens();
        this.renderUsageExamples();
    },

    onFractalChange() {
        this.tokens = [];
        this.filteredTokens = [];
        this.revealedToken = null;
        this.editingToken = null;
        this.currentPage = 0;
        this.searchQuery = '';
        const searchInput = document.getElementById('ingestTokenSearchInput');
        if (searchInput) searchInput.value = '';
        const container = document.getElementById('fractalIngestTabContent');
        if (container && container.style.display !== 'none') {
            this.loadTokens();
        }
    },

    async loadTokens() {
        const fractal = window.FractalContext?.getCurrentFractal();
        if (!fractal) return;

        const tbody = document.getElementById('ingestTokensTableBody');
        if (tbody) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:40px;color:var(--text-muted);">Loading tokens...</td></tr>';
        }

        try {
            const resp = await fetch(`/api/v1/fractals/${fractal.id}/ingest-tokens`, {
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                this.tokens = data.data.ingest_tokens || [];
            } else {
                this.tokens = [];
            }
        } catch (err) {
            console.error('[IngestTokens] Failed to load tokens:', err);
            this.tokens = [];
        }
        this.applyFilter();
        this.renderTable();
        this.renderDynamicPanels();
    },

    async loadNormalizersList() {
        try {
            const resp = await fetch('/api/v1/normalizers', { credentials: 'include' });
            const data = await resp.json();
            if (data.success && data.data?.normalizers) {
                this.availableNormalizers = data.data.normalizers;
            }
        } catch (err) {
            console.error('[IngestTokens] Failed to load normalizers:', err);
        }
    },

    renderNormalizerSelect(id, selectedId) {
        const defaultNorm = this.availableNormalizers.find(n => n.is_default);
        const selected = selectedId || (defaultNorm ? defaultNorm.id : '');
        let options = '<option value="">None (no normalization)</option>';
        for (const n of this.availableNormalizers) {
            const sel = n.id === selected ? 'selected' : '';
            const badge = n.is_default ? ' (default)' : '';
            options += `<option value="${this.esc(n.id)}" ${sel}>${this.esc(n.name)}${badge}</option>`;
        }
        return `<select id="${id}">${options}</select>`;
    },

    handleSearch(query) {
        this.searchQuery = query.trim().toLowerCase();
        this.currentPage = 0;
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
            this.applyFilter();
            this.renderTable();
        }, 200);
    },

    applyFilter() {
        if (!this.searchQuery) {
            this.filteredTokens = this.tokens;
        } else {
            this.filteredTokens = this.tokens.filter(t =>
                t.name.toLowerCase().includes(this.searchQuery) ||
                (t.description && t.description.toLowerCase().includes(this.searchQuery)) ||
                t.parser_type.toLowerCase().includes(this.searchQuery) ||
                t.token_prefix.toLowerCase().includes(this.searchQuery)
            );
        }
    },

    getPagedTokens() {
        const start = this.currentPage * this.pageSize;
        return this.filteredTokens.slice(start, start + this.pageSize);
    },

    renderTable() {
        const tbody = document.getElementById('ingestTokensTableBody');
        if (!tbody) return;

        const paged = this.getPagedTokens();

        if (this.filteredTokens.length === 0) {
            const msg = this.searchQuery ? 'No tokens match your search' : 'No ingest tokens yet. Create one to start sending logs.';
            tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;padding:40px;color:var(--text-muted);">${msg}</td></tr>`;
            this.updatePagination();
            return;
        }

        tbody.innerHTML = paged.map(t => {
            const statusClass = t.is_active ? 'active' : 'inactive';
            const statusLabel = t.is_active ? 'Active' : 'Inactive';
            const defaultBadge = t.is_default ? '<span class="token-default-badge">DEFAULT</span>' : '';
            const lastUsed = t.last_used_at ? this.timeAgo(t.last_used_at) : 'Never';
            const editingClass = this.editingToken?.id === t.id ? 'editing' : '';

            let tokenCell = '';
            if (t.token_value) {
                const masked = t.token_prefix + '...';
                tokenCell = `
                    <div class="token-secret-cell">
                        <code class="token-secret-value" data-token-id="${t.id}" data-masked="${this.esc(masked)}">${this.esc(masked)}</code>
                        <button class="token-cell-btn" data-action="toggle-secret" data-token-id="${t.id}" title="Show/hide token">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
                                <circle cx="12" cy="12" r="3"/>
                            </svg>
                        </button>
                        <button class="token-cell-btn" data-action="copy-secret" data-token-id="${t.id}" title="Copy token">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                            </svg>
                        </button>
                    </div>`;
            } else {
                tokenCell = `<span style="color:var(--text-muted);font-size:0.75rem;">Unavailable</span>`;
            }

            const deleteBtn = t.is_default ? '' :
                `<button class="btn-action danger" data-action="delete" data-token-id="${t.id}" data-token-name="${this.esc(t.name)}">Delete</button>`;

            return `<tr class="ingest-token-row ${statusClass} ${editingClass}" data-token-id="${t.id}">
                <td>
                    <div class="token-name">${this.esc(t.name)}${defaultBadge}</div>
                    ${t.description ? `<div class="token-description">${this.esc(t.description)}</div>` : ''}
                </td>
                <td>${tokenCell}</td>
                <td><span class="token-parser-badge">${this.esc(t.parser_type)}</span></td>
                <td><span class="token-status status-${statusClass}">${statusLabel}</span></td>
                <td>
                    <div class="token-usage-info">
                        <span class="token-usage-count">${this.formatNumber(t.usage_count)} req / ${this.formatNumber(t.log_count)} logs</span>
                        <span class="token-last-used">Last: ${lastUsed}</span>
                    </div>
                </td>
                <td class="token-actions-cell">
                    <button class="btn-action" data-action="toggle" data-token-id="${t.id}">${t.is_active ? 'Disable' : 'Enable'}</button>
                    ${deleteBtn}
                </td>
            </tr>`;
        }).join('');

        this.updatePagination();
        this.bindTableEvents();
    },

    bindTableEvents() {
        const tbody = document.getElementById('ingestTokensTableBody');
        if (!tbody) return;

        // Row double-click to edit
        tbody.querySelectorAll('.ingest-token-row').forEach(row => {
            row.addEventListener('dblclick', (e) => {
                if (e.target.closest('.token-actions-cell') || e.target.closest('.token-secret-cell')) return;
                const tokenId = row.dataset.tokenId;
                const token = this.tokens.find(t => t.id === tokenId);
                if (token) this.openEdit(token);
            });
        });

        // Remove previous delegated listener before adding a new one
        if (this._tbodyClickHandler) {
            tbody.removeEventListener('click', this._tbodyClickHandler);
        }

        // Action buttons via delegation
        this._tbodyClickHandler = (e) => {
            const btn = e.target.closest('[data-action]');
            if (!btn) return;
            e.stopPropagation();

            const action = btn.dataset.action;
            const tokenId = btn.dataset.tokenId;

            switch (action) {
                case 'toggle-secret':
                    this.toggleSecretVisibility(tokenId);
                    break;
                case 'copy-secret':
                    this.copyTokenValue(tokenId);
                    break;
                case 'toggle':
                    this.toggleToken(tokenId);
                    break;
                case 'delete':
                    this.deleteToken(tokenId, btn.dataset.tokenName);
                    break;
            }
        };
        tbody.addEventListener('click', this._tbodyClickHandler);
    },

    updatePagination() {
        const total = this.filteredTokens.length;
        const totalPages = Math.max(1, Math.ceil(total / this.pageSize));
        const currentPage = this.currentPage + 1;

        const info = document.getElementById('ingestTokensPaginationInfo');
        if (info) {
            info.textContent = total > 0
                ? `Page ${currentPage} of ${totalPages} (${total} tokens)`
                : 'No tokens';
        }

        const prevBtn = document.getElementById('ingestTokensPrevBtn');
        if (prevBtn) prevBtn.disabled = this.currentPage === 0;

        const nextBtn = document.getElementById('ingestTokensNextBtn');
        if (nextBtn) nextBtn.disabled = this.currentPage >= totalPages - 1;
    },

    previousPage() {
        if (this.currentPage > 0) {
            this.currentPage--;
            this.renderTable();
        }
    },

    nextPage() {
        const totalPages = Math.ceil(this.filteredTokens.length / this.pageSize);
        if (this.currentPage < totalPages - 1) {
            this.currentPage++;
            this.renderTable();
        }
    },

    // -- Dynamic panels (create form, edit panel, reveal banner) --

    renderDynamicPanels() {
        const container = document.getElementById('ingestDynamicPanels');
        if (!container) return;

        let html = '';

        if (this.revealedToken) {
            html += this.renderRevealedToken();
        }

        if (this.showCreateForm) {
            html += this.renderCreateForm();
        }

        if (this.editingToken) {
            html += this.renderEditPanel();
        }

        container.innerHTML = html;
    },

    // -- Create form --

    toggleCreateForm() {
        this.showCreateForm = !this.showCreateForm;
        if (this.showCreateForm) this.editingToken = null;
        this.renderDynamicPanels();
        if (this.showCreateForm) {
            document.getElementById('ingestTokenName')?.focus();
        }
    },

    renderCreateForm() {
        return `<div class="ingest-create-form">
            <h3>Create Ingest Token</h3>
            <div class="ingest-form-grid">
                <div class="ingest-form-group">
                    <label for="ingestTokenName">Name</label>
                    <input type="text" id="ingestTokenName" placeholder="e.g. Velociraptor, Syslog">
                </div>
                <div class="ingest-form-group">
                    <label for="ingestTokenParser">Parser</label>
                    <select id="ingestTokenParser">
                        <option value="json" selected>JSON</option>
                        <option value="kv">Key=Value</option>
                        <option value="syslog">Syslog</option>
                    </select>
                    <span class="form-help">How incoming log data is parsed</span>
                </div>
                <div class="ingest-form-group full-width">
                    <label for="ingestTokenDesc">Description (optional)</label>
                    <input type="text" id="ingestTokenDesc" placeholder="What sends logs with this token?">
                </div>
                <div class="ingest-form-group">
                    <label for="ingestTokenNormalizer">Normalizer</label>
                    ${this.renderNormalizerSelect('ingestTokenNormalizer', null)}
                    <span class="form-help">Transform and standardize field names on ingest</span>
                </div>
            </div>
            <div class="ingest-form-actions">
                <button class="btn-secondary" onclick="IngestTokens.toggleCreateForm()">Cancel</button>
                <button class="btn-primary" onclick="IngestTokens.createToken()">Create Token</button>
            </div>
        </div>`;
    },

    async createToken() {
        const name = document.getElementById('ingestTokenName')?.value?.trim();
        const description = document.getElementById('ingestTokenDesc')?.value?.trim() || '';
        const parserType = document.getElementById('ingestTokenParser')?.value || 'json';
        const normalizerId = document.getElementById('ingestTokenNormalizer')?.value || null;

        if (!name) {
            if (window.Toast) Toast.error('Token name is required');
            return;
        }

        const fractal = window.FractalContext?.getCurrentFractal();
        if (!fractal) return;

        const payload = { name, description, parser_type: parserType };
        if (normalizerId) payload.normalizer_id = normalizerId;

        try {
            const resp = await fetch(`/api/v1/fractals/${fractal.id}/ingest-tokens`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(payload)
            });
            const data = await resp.json();
            if (data.success) {
                this.revealedToken = data.data;
                this.showCreateForm = false;
                await this.loadTokens();
                if (window.Toast) Toast.success('Ingest token created');
            } else {
                if (window.Toast) Toast.error(data.error || 'Failed to create token');
            }
        } catch (err) {
            console.error('[IngestTokens] Create failed:', err);
            if (window.Toast) Toast.error('Failed to create token');
        }
    },

    // -- Revealed token banner --

    renderRevealedToken() {
        const t = this.revealedToken;
        return `<div class="token-reveal">
            <h3>Token Created</h3>
            <div class="token-reveal-warning">
                <p>Save this token - you can also view it later from the table.</p>
            </div>
            <div class="token-copy-container">
                <input type="password" class="token-copy-input" value="${this.esc(t.token)}" readonly id="revealedTokenInput">
                <button class="token-eye-btn" onclick="IngestTokens.toggleRevealedVisibility()" title="Toggle visibility">
                    <svg class="eye-icon eye-closed" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/>
                        <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/>
                        <line x1="1" y1="1" x2="23" y2="23"/>
                    </svg>
                    <svg class="eye-icon eye-open" style="display:none" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
                        <circle cx="12" cy="12" r="3"/>
                    </svg>
                </button>
                <button class="token-copy-btn" onclick="IngestTokens.copyRevealedToken()">Copy</button>
            </div>
            <button class="btn-secondary" onclick="IngestTokens.dismissReveal()" style="margin-top:0.5rem;">Dismiss</button>
        </div>`;
    },

    toggleRevealedVisibility() {
        const input = document.getElementById('revealedTokenInput');
        const eyeBtn = input?.parentElement?.querySelector('.token-eye-btn');
        if (!input || !eyeBtn) return;

        const isHidden = input.type === 'password';
        input.type = isHidden ? 'text' : 'password';
        eyeBtn.querySelector('.eye-closed').style.display = isHidden ? 'none' : '';
        eyeBtn.querySelector('.eye-open').style.display = isHidden ? '' : 'none';
    },

    copyRevealedToken() {
        const input = document.getElementById('revealedTokenInput');
        if (!input) return;
        navigator.clipboard.writeText(input.value).then(() => {
            if (window.Toast) Toast.success('Token copied to clipboard');
        }).catch(() => {
            input.select();
            document.execCommand('copy');
            if (window.Toast) Toast.success('Token copied');
        });
    },

    dismissReveal() {
        this.revealedToken = null;
        this.renderDynamicPanels();
    },

    // -- Edit panel --

    openEdit(token) {
        this.editingToken = { ...token };
        this.showCreateForm = false;
        this.renderDynamicPanels();
        this.renderTable();
        document.getElementById('editTokenName')?.focus();
    },

    closeEdit() {
        this.editingToken = null;
        this.renderDynamicPanels();
        this.renderTable();
    },

    renderEditPanel() {
        const t = this.editingToken;
        const isJson = t.parser_type === 'json';
        return `<div class="ingest-edit-panel">
            <div class="ingest-edit-header">
                <h3>Edit Token</h3>
                <button class="ingest-edit-close" onclick="IngestTokens.closeEdit()" title="Close">&times;</button>
            </div>
            <div class="ingest-form-grid">
                <div class="ingest-form-group">
                    <label for="editTokenName">Name</label>
                    <input type="text" id="editTokenName" value="${this.esc(t.name)}">
                </div>
                <div class="ingest-form-group">
                    <label for="editTokenParser">Parser</label>
                    <select id="editTokenParser">
                        <option value="json" ${t.parser_type === 'json' ? 'selected' : ''}>JSON</option>
                        <option value="kv" ${t.parser_type === 'kv' ? 'selected' : ''}>Key=Value</option>
                        <option value="syslog" ${t.parser_type === 'syslog' ? 'selected' : ''}>Syslog</option>
                    </select>
                </div>
                <div class="ingest-form-group full-width">
                    <label for="editTokenDesc">Description</label>
                    <input type="text" id="editTokenDesc" value="${this.esc(t.description || '')}">
                </div>
                <div class="ingest-form-group">
                    <label for="editTokenNormalizer">Normalizer</label>
                    ${this.renderNormalizerSelect('editTokenNormalizer', t.normalizer_id || '')}
                    <span class="form-help">Transform and standardize field names on ingest</span>
                </div>
            </div>
            <div class="ingest-form-actions">
                <button class="btn-secondary" onclick="IngestTokens.closeEdit()">Cancel</button>
                <button class="btn-primary" onclick="IngestTokens.saveEdit()">Save Changes</button>
            </div>
        </div>`;
    },

    async saveEdit() {
        if (!this.editingToken) return;
        const fractal = window.FractalContext?.getCurrentFractal();
        if (!fractal) return;

        const name = document.getElementById('editTokenName')?.value?.trim();
        const description = document.getElementById('editTokenDesc')?.value?.trim() || '';
        const parserType = document.getElementById('editTokenParser')?.value || 'json';
        const normalizerId = document.getElementById('editTokenNormalizer')?.value || '';

        if (!name) {
            if (window.Toast) Toast.error('Token name is required');
            return;
        }

        const payload = { name, description, parser_type: parserType };
        if (normalizerId) {
            payload.normalizer_id = normalizerId;
        } else {
            payload.clear_normalizer = true;
        }

        try {
            const resp = await fetch(`/api/v1/fractals/${fractal.id}/ingest-tokens/${this.editingToken.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(payload)
            });
            const data = await resp.json();
            if (data.success) {
                this.editingToken = null;
                await this.loadTokens();
                if (window.Toast) Toast.success('Token updated');
            } else {
                if (window.Toast) Toast.error(data.error || 'Failed to update token');
            }
        } catch (err) {
            console.error('[IngestTokens] Update failed:', err);
            if (window.Toast) Toast.error('Failed to update token');
        }
    },

    // -- Token actions --

    toggleSecretVisibility(tokenId) {
        const el = document.querySelector(`.token-secret-value[data-token-id="${tokenId}"]`);
        if (!el) return;
        const token = this.tokens.find(t => t.id === tokenId);
        if (!token || !token.token_value) return;

        const masked = el.dataset.masked;
        if (el.textContent === token.token_value) {
            el.textContent = masked;
        } else {
            el.textContent = token.token_value;
        }
    },

    copyTokenValue(tokenId) {
        const token = this.tokens.find(t => t.id === tokenId);
        if (!token || !token.token_value) return;

        navigator.clipboard.writeText(token.token_value).then(() => {
            if (window.Toast) Toast.success('Token copied to clipboard');
        }).catch(() => {
            const tmp = document.createElement('textarea');
            tmp.value = token.token_value;
            document.body.appendChild(tmp);
            tmp.select();
            document.execCommand('copy');
            document.body.removeChild(tmp);
            if (window.Toast) Toast.success('Token copied');
        });
    },

    async toggleToken(tokenId) {
        const fractal = window.FractalContext?.getCurrentFractal();
        if (!fractal) return;

        try {
            const resp = await fetch(`/api/v1/fractals/${fractal.id}/ingest-tokens/${tokenId}/toggle`, {
                method: 'POST',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadTokens();
                if (window.Toast) Toast.success(data.message);
            } else {
                if (window.Toast) Toast.error(data.error || 'Failed to toggle token');
            }
        } catch (err) {
            console.error('[IngestTokens] Toggle failed:', err);
            if (window.Toast) Toast.error('Failed to toggle token');
        }
    },

    async deleteToken(tokenId, tokenName) {
        if (!confirm(`Delete ingest token "${tokenName}"? This cannot be undone.`)) return;

        const fractal = window.FractalContext?.getCurrentFractal();
        if (!fractal) return;

        try {
            const resp = await fetch(`/api/v1/fractals/${fractal.id}/ingest-tokens/${tokenId}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                await this.loadTokens();
                if (window.Toast) Toast.success('Ingest token deleted');
            } else {
                if (window.Toast) Toast.error(data.error || 'Failed to delete token');
            }
        } catch (err) {
            console.error('[IngestTokens] Delete failed:', err);
            if (window.Toast) Toast.error('Failed to delete token');
        }
    },

    // -- Usage examples --

    renderUsageExamples() {
        const container = document.getElementById('ingestUsageExamples');
        if (!container) return;

        const host = window.location.origin;
        container.innerHTML = `<div class="ingest-usage-examples">
            <h4>Sending Logs</h4>
            <div class="ingest-code-example">
                <strong>JSON (single object)</strong>
                <code>curl -X POST ${host}/api/v1/ingest \\
  -H "Authorization: Bearer bifract_ingest_YOUR_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{"message":"hello","level":"info"}'</code>
            </div>
            <div class="ingest-code-example">
                <strong>JSON (array)</strong>
                <code>curl -X POST ${host}/api/v1/ingest \\
  -H "Authorization: Bearer bifract_ingest_YOUR_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '[{"msg":"event1"},{"msg":"event2"}]'</code>
            </div>
            <div class="ingest-code-example">
                <strong>Elasticsearch Bulk API (Velociraptor)</strong>
                <code>curl -X POST ${host}/_bulk \\
  -H "Authorization: Bearer bifract_ingest_YOUR_TOKEN" \\
  -H "Content-Type: application/x-ndjson" \\
  --data-binary @bulk.ndjson</code>
            </div>
        </div>`;
    },

    // -- Helpers --

    esc(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    formatNumber(n) {
        if (n === undefined || n === null) return '0';
        if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
        if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
        return String(n);
    },

    timeAgo(dateStr) {
        if (!dateStr) return 'Never';
        const date = new Date(dateStr);
        const now = new Date();
        const seconds = Math.floor((now - date) / 1000);
        if (seconds < 60) return 'Just now';
        if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
        if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
        if (seconds < 604800) return Math.floor(seconds / 86400) + 'd ago';
        return date.toLocaleDateString();
    }
};

window.IngestTokens = IngestTokens;

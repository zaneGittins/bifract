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
            createBtn.addEventListener('click', () => this.showCreateModal());
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

        document.getElementById('cancelIngestTokenModalBtn')?.addEventListener('click', () => this.hideCreateModal());
        document.getElementById('submitIngestTokenModalBtn')?.addEventListener('click', () => this.createToken());
        document.getElementById('ingestTokenName')?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.createToken();
            if (e.key === 'Escape') this.hideCreateModal();
        });

        document.getElementById('cancelEditTokenModalBtn')?.addEventListener('click', () => this.closeEdit());
        document.getElementById('submitEditTokenModalBtn')?.addEventListener('click', () => this.saveEdit());
        document.getElementById('editTokenName')?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.saveEdit();
            if (e.key === 'Escape') this.closeEdit();
        });
    },

    show() {
        this.loadNormalizersList();
        this.loadTokens();
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
        if (container && container.offsetParent !== null) {
            this.loadTokens();
        }
    },

    async loadTokens() {
        const fractal = window.FractalContext?.getCurrentFractal();
        // Tokens belong to a fractal. In a prism the id here is the prism's, so
        // the request can only 404, and the same scope change is already hiding
        // this tab.
        if (!fractal || window.FractalContext?.isPrism?.()) return;

        const tbody = document.getElementById('ingestTokensTableBody');
        if (tbody) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:40px;color:var(--text-muted);">Loading tokens...</td></tr>';
        }

        try {
            const resp = await fetch(`/api/v1/fractals/${fractal.id}/ingest-tokens`, {
                credentials: 'include'
            });
            const data = await resp.json();
            if (data.success) {
                this.tokens = data.data || [];
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
            this.availableNormalizers = HttpUtils.list(await resp.json());
        } catch (err) {
            console.error('[IngestTokens] Failed to load normalizers:', err);
        }
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

        const tableContainer = tbody.closest('.ingest-tokens-table-container');
        const emptyEl = document.getElementById('ingestEmptyState');

        if (this.filteredTokens.length === 0) {
            if (tableContainer) tableContainer.style.display = 'none';
            if (emptyEl) {
                emptyEl.style.display = '';
                const msg = emptyEl.querySelector('p');
                if (msg) {
                    msg.textContent = this.searchQuery
                        ? 'No tokens match your search'
                        : 'No ingest tokens yet';
                }
            }
            this.updatePagination();
            return;
        }

        if (tableContainer) tableContainer.style.display = '';
        if (emptyEl) emptyEl.style.display = 'none';

        tbody.innerHTML = paged.map(t => {
            const statusClass = t.is_active ? 'active' : 'inactive';
            const defaultBadge = t.is_default ? '<span class="token-default-badge">DEFAULT</span>' : '';
            const lastUsed = t.last_used_at ? this.timeAgo(t.last_used_at) : 'Never';

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

            const toggleChecked = t.is_active ? 'checked' : '';
            const toggleTitle = t.is_active ? 'Disable token' : 'Enable token';

            const deleteBtn = t.is_default ? '' :
                `<button class="token-cell-btn token-delete-btn" data-action="delete" data-token-id="${t.id}" data-token-name="${this.esc(t.name)}" title="Delete token">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                    </svg>
                </button>`;

            return `<tr class="ingest-token-row ${statusClass}" data-token-id="${t.id}">
                <td>
                    <div class="token-name">${this.esc(t.name)}${defaultBadge}</div>
                    ${t.description ? `<div class="token-description">${this.esc(t.description)}</div>` : ''}
                </td>
                <td>${tokenCell}</td>
                <td><span class="token-parser-badge">${this.esc(t.parser_type)}</span></td>
                <td><span class="token-normalizer-name">${t.normalizer_name ? this.esc(t.normalizer_name) : 'None'}</span></td>
                <td>
                    <div class="token-usage-info">
                        <span class="token-usage-count">${this.formatNumber(t.usage_count)} req / ${this.formatNumber(t.log_count)} logs</span>
                        <span class="token-last-used">Last: ${lastUsed}</span>
                    </div>
                </td>
                <td class="token-actions-cell">
                    <label class="token-toggle" title="${toggleTitle}">
                        <input type="checkbox" ${toggleChecked} data-action="toggle" data-token-id="${t.id}">
                        <span class="token-toggle-slider"></span>
                    </label>
                    <div class="token-row-btns">
                        <button class="token-cell-btn" data-action="edit" data-token-id="${t.id}" title="Edit token">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                            </svg>
                        </button>
                        ${deleteBtn}
                    </div>
                </td>
            </tr>`;
        }).join('');

        this.updatePagination();
        this.bindTableEvents();
    },

    bindTableEvents() {
        const tbody = document.getElementById('ingestTokensTableBody');
        if (!tbody) return;

        if (this._tbodyClickHandler) {
            tbody.removeEventListener('click', this._tbodyClickHandler);
        }

        this._tbodyClickHandler = (e) => {
            const el = e.target.closest('[data-action]');
            if (!el) return;
            e.stopPropagation();

            const action = el.dataset.action;
            const tokenId = el.dataset.tokenId;

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
                case 'edit': {
                    const token = this.tokens.find(t => t.id === tokenId);
                    if (token) this.openEdit(token);
                    break;
                }
                case 'delete':
                    this.deleteToken(tokenId, el.dataset.tokenName);
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

        const paginationContainer = prevBtn?.parentElement;
        if (paginationContainer) {
            paginationContainer.style.display = totalPages <= 1 ? 'none' : '';
        }
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

    // -- Dynamic panels (reveal banner only; create form is now a modal) --

    renderDynamicPanels() {
        const container = document.getElementById('ingestDynamicPanels');
        if (!container) return;
        container.innerHTML = this.revealedToken ? this.renderRevealedToken() : '';
    },

    // -- Create modal --

    showCreateModal() {
        const modal = document.getElementById('createIngestTokenModal');
        if (!modal) return;
        const nameInput = document.getElementById('ingestTokenName');
        const descInput = document.getElementById('ingestTokenDesc');
        const parserSelect = document.getElementById('ingestTokenParser');
        if (nameInput) nameInput.value = '';
        if (descInput) descInput.value = '';
        if (parserSelect) parserSelect.value = 'json';
        this._populateNormalizerSelect('ingestTokenNormalizer', null);
        this.editingToken = null;
        this.showCreateForm = true;
        modal.style.display = 'flex';
        setTimeout(() => nameInput?.focus(), 100);
    },

    hideCreateModal() {
        const modal = document.getElementById('createIngestTokenModal');
        if (modal) modal.style.display = 'none';
        this.showCreateForm = false;
    },

    _populateNormalizerSelect(id, selectedId) {
        const select = document.getElementById(id);
        if (!select) return;
        const defaultNorm = this.availableNormalizers.find(n => n.is_default);
        const selected = selectedId || (defaultNorm ? defaultNorm.id : '');
        let options = '<option value="">None (no normalization)</option>';
        for (const n of this.availableNormalizers) {
            const sel = n.id === selected ? 'selected' : '';
            const badge = n.is_default ? ' (default)' : '';
            options += `<option value="${this.esc(n.id)}" ${sel}>${this.esc(n.name)}${badge}</option>`;
        }
        select.innerHTML = options;
    },

    async createToken() {
        const name = document.getElementById('ingestTokenName')?.value?.trim();
        const description = document.getElementById('ingestTokenDesc')?.value?.trim() || '';
        const parserType = document.getElementById('ingestTokenParser')?.value || 'json';
        const normalizerSelect = document.getElementById('ingestTokenNormalizer');
        const normalizerId = normalizerSelect?.value || null;

        if (!name) {
            if (window.Toast) Toast.error('Token name is required');
            return;
        }

        const fractal = window.FractalContext?.getCurrentFractal();
        if (!fractal) return;

        const payload = { name, description, parser_type: parserType };
        if (normalizerId) {
            payload.normalizer_id = normalizerId;
        } else if (normalizerSelect && normalizerSelect.value === '') {
            // User explicitly selected "None (no normalization)"
            payload.clear_normalizer = true;
        }
        // If neither, backend uses the default normalizer

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
                this.hideCreateModal();
                await this.loadTokens();
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
        navigator.clipboard.writeText(input.value).catch(() => {
            input.select();
            document.execCommand('copy');
        });
    },

    dismissReveal() {
        this.revealedToken = null;
        this.renderDynamicPanels();
    },

    // -- Edit panel --

    openEdit(token) {
        const modal = document.getElementById('editIngestTokenModal');
        if (!modal) return;
        this.hideCreateModal();
        this.editingToken = { ...token };

        const nameInput = document.getElementById('editTokenName');
        const descInput = document.getElementById('editTokenDesc');
        const parserSelect = document.getElementById('editTokenParser');
        if (nameInput) nameInput.value = token.name || '';
        if (descInput) descInput.value = token.description || '';
        if (parserSelect) parserSelect.value = token.parser_type || 'json';
        this._populateNormalizerSelect('editTokenNormalizer', token.normalizer_id || '');

        modal.style.display = 'flex';
        setTimeout(() => nameInput?.focus(), 100);
    },

    closeEdit() {
        const modal = document.getElementById('editIngestTokenModal');
        if (modal) modal.style.display = 'none';
        this.editingToken = null;
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
                this.closeEdit();
                await this.loadTokens();
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

        navigator.clipboard.writeText(token.token_value).catch(() => {
            const tmp = document.createElement('textarea');
            tmp.value = token.token_value;
            document.body.appendChild(tmp);
            tmp.select();
            document.execCommand('copy');
            document.body.removeChild(tmp);
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
            } else {
                if (window.Toast) Toast.error(data.error || 'Failed to delete token');
            }
        } catch (err) {
            console.error('[IngestTokens] Delete failed:', err);
            if (window.Toast) Toast.error('Failed to delete token');
        }
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
        return TZ.format(date, 'date');
    }
};

window.IngestTokens = IngestTokens;

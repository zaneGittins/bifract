// Saved Queries - server-side persistent queries per fractal
const SavedQueries = {
    isOpen: false,
    queries: [],
    searchTerm: '',
    debounceTimer: null,
    editingId: null,

    init() {
        const btn = document.getElementById('savedQueriesBtn');
        const dropdown = document.getElementById('savedQueriesDropdown');

        if (btn) {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggle();
            });
        }

        if (dropdown) {
            dropdown.addEventListener('click', (e) => e.stopPropagation());
        }

        document.addEventListener('click', (e) => {
            if (dropdown && this.isOpen && !dropdown.contains(e.target) && e.target.id !== 'savedQueriesBtn') {
                this.close();
            }
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
    },

    onFractalChange() {
        this.queries = [];
        this.editingId = null;
        if (this.isOpen) {
            this.loadAndRender();
        }
    },

    toggle() {
        if (this.isOpen) {
            this.close();
        } else {
            this.open();
        }
    },

    open() {
        const dropdown = document.getElementById('savedQueriesDropdown');
        if (!dropdown) return;

        // Close other dropdowns
        if (window.RecentQueries && RecentQueries.isOpen) {
            RecentQueries.close();
        }

        dropdown.style.display = 'block';
        this.isOpen = true;
        this.editingId = null;
        this.loadAndRender();

        const searchInput = document.getElementById('savedQueriesSearch');
        if (searchInput) {
            setTimeout(() => searchInput.focus(), 100);
        }
    },

    close() {
        const dropdown = document.getElementById('savedQueriesDropdown');
        if (dropdown) {
            dropdown.style.display = 'none';
        }
        this.isOpen = false;
        this.editingId = null;
        this.searchTerm = '';
        const searchInput = document.getElementById('savedQueriesSearch');
        if (searchInput) searchInput.value = '';
    },

    async loadAndRender(search) {
        let url = '/api/v1/saved-queries';
        const params = new URLSearchParams();
        if (search) {
            params.set('search', search);
        }
        // Include fractal_id to avoid session race conditions
        const fractalId = window.FractalContext?.currentFractal?.id;
        if (fractalId) {
            params.set('fractal_id', fractalId);
        }
        const qs = params.toString();
        if (qs) url += '?' + qs;

        try {
            const resp = await fetch(url, { credentials: 'include' });
            const data = await resp.json();
            if (data.success && data.data) {
                this.queries = data.data.saved_queries || [];
            } else {
                this.queries = [];
            }
        } catch (err) {
            console.error('[SavedQueries] Failed to load:', err);
            this.queries = [];
        }

        this.render();
    },

    render() {
        const list = document.getElementById('savedQueriesList');
        if (!list) return;

        const form = document.getElementById('savedQueriesSaveForm');

        if (this.queries.length === 0 && !this.editingId) {
            list.innerHTML = '<div class="empty-message">No saved queries</div>';
            return;
        }

        let html = '';
        this.queries.forEach(sq => {
            if (this.editingId === sq.id) {
                html += this.renderEditForm(sq);
                return;
            }

            const tagsHtml = (sq.tags || []).map(t =>
                `<span class="sq-tag">${this.escapeHtml(t)}</span>`
            ).join('');

            const preview = this.escapeHtml(sq.query_text.length > 80
                ? sq.query_text.substring(0, 80) + '...'
                : sq.query_text);

            html += `
                <div class="saved-query-item" data-id="${sq.id}">
                    <div class="sq-main" onclick="SavedQueries.loadQuery('${this.escapeJs(sq.id)}')">
                        <div class="sq-name">${this.escapeHtml(sq.name)}</div>
                        <div class="sq-preview">${preview}</div>
                        ${tagsHtml ? `<div class="sq-tags">${tagsHtml}</div>` : ''}
                    </div>
                    <div class="sq-actions">
                        <button class="sq-action-btn" onclick="SavedQueries.startEdit('${this.escapeJs(sq.id)}')" title="Edit">
                            <svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M11.5 1.5l3 3L5 14H2v-3L11.5 1.5z" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/></svg>
                        </button>
                        <button class="sq-action-btn delete" onclick="SavedQueries.deleteQuery('${this.escapeJs(sq.id)}')" title="Delete">
                            <svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M3 3l10 10M13 3L3 13" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
                        </button>
                    </div>
                </div>`;
        });

        list.innerHTML = html;
    },

    renderSaveForm() {
        return `
            <div class="sq-form">
                <input type="text" id="sqFormName" class="sq-form-input" placeholder="Query name" maxlength="255" />
                <input type="text" id="sqFormTags" class="sq-form-input" placeholder="Tags (comma-separated)" />
                <div class="sq-form-actions">
                    <button class="btn-secondary btn-sm" onclick="SavedQueries.cancelSave()">Cancel</button>
                    <button class="btn-primary btn-sm" onclick="SavedQueries.submitSave()">Save</button>
                </div>
            </div>`;
    },

    renderEditForm(sq) {
        const tags = (sq.tags || []).join(', ');
        return `
            <div class="saved-query-item editing">
                <div class="sq-form">
                    <input type="text" id="sqEditName" class="sq-form-input" placeholder="Query name" value="${this.escapeHtml(sq.name)}" maxlength="255" />
                    <textarea id="sqEditQuery" class="sq-form-input sq-form-textarea" placeholder="Query">${this.escapeHtml(sq.query_text)}</textarea>
                    <input type="text" id="sqEditTags" class="sq-form-input" placeholder="Tags (comma-separated)" value="${this.escapeHtml(tags)}" />
                    <div class="sq-form-actions">
                        <button class="btn-secondary btn-sm" onclick="SavedQueries.cancelEdit()">Cancel</button>
                        <button class="btn-primary btn-sm" onclick="SavedQueries.submitEdit('${this.escapeJs(sq.id)}')">Update</button>
                    </div>
                </div>
            </div>`;
    },

    showSaveForm() {
        const queryInput = document.getElementById('queryInput');
        if (!queryInput || !queryInput.value.trim()) {
            if (window.Toast) Toast.show('No query to save', 'warning');
            return;
        }

        const form = document.getElementById('savedQueriesSaveForm');
        if (form) {
            form.style.display = 'block';
            form.innerHTML = this.renderSaveForm();
            const nameInput = document.getElementById('sqFormName');
            if (nameInput) setTimeout(() => nameInput.focus(), 50);
        }
    },

    cancelSave() {
        const form = document.getElementById('savedQueriesSaveForm');
        if (form) {
            form.style.display = 'none';
            form.innerHTML = '';
        }
    },

    async submitSave() {
        const name = document.getElementById('sqFormName')?.value.trim();
        const tagsStr = document.getElementById('sqFormTags')?.value || '';
        const queryText = document.getElementById('queryInput')?.value.trim();

        if (!name) {
            if (window.Toast) Toast.show('Name is required', 'warning');
            return;
        }
        if (!queryText) {
            if (window.Toast) Toast.show('No query to save', 'warning');
            return;
        }

        const tags = tagsStr.split(',').map(t => t.trim()).filter(t => t);

        try {
            const body = { name, query_text: queryText, tags };
            const fractalId = window.FractalContext?.currentFractal?.id;
            if (fractalId) body.fractal_id = fractalId;

            const resp = await fetch('/api/v1/saved-queries', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.show(data.error || 'Failed to save', 'error');
                return;
            }
            if (window.Toast) Toast.show('Query saved', 'success');
            this.cancelSave();
            this.loadAndRender();
        } catch (err) {
            console.error('[SavedQueries] Save failed:', err);
            if (window.Toast) Toast.show('Failed to save query', 'error');
        }
    },

    loadQuery(id) {
        const sq = this.queries.find(q => q.id === id);
        if (!sq) return;

        const queryInput = document.getElementById('queryInput');
        if (queryInput) {
            queryInput.value = sq.query_text;
            setTimeout(() => {
                queryInput.dispatchEvent(new Event('input', { bubbles: true }));
                queryInput.focus();
            }, 0);
        }
        this.close();
    },

    startEdit(id) {
        this.editingId = id;
        this.render();
        const nameInput = document.getElementById('sqEditName');
        if (nameInput) setTimeout(() => nameInput.focus(), 50);
    },

    cancelEdit() {
        this.editingId = null;
        this.render();
    },

    async submitEdit(id) {
        const name = document.getElementById('sqEditName')?.value.trim();
        const queryText = document.getElementById('sqEditQuery')?.value.trim();
        const tagsStr = document.getElementById('sqEditTags')?.value || '';

        if (!name) {
            if (window.Toast) Toast.show('Name is required', 'warning');
            return;
        }
        if (!queryText) {
            if (window.Toast) Toast.show('Query text is required', 'warning');
            return;
        }

        const tags = tagsStr.split(',').map(t => t.trim()).filter(t => t);

        try {
            const resp = await fetch(`/api/v1/saved-queries/${id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, query_text: queryText, tags })
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.show(data.error || 'Failed to update', 'error');
                return;
            }
            if (window.Toast) Toast.show('Query updated', 'success');
            this.editingId = null;
            this.loadAndRender();
        } catch (err) {
            console.error('[SavedQueries] Update failed:', err);
            if (window.Toast) Toast.show('Failed to update query', 'error');
        }
    },

    async deleteQuery(id) {
        if (!confirm('Delete this saved query?')) return;

        try {
            const resp = await fetch(`/api/v1/saved-queries/${id}`, {
                method: 'DELETE',
                credentials: 'include'
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.show(data.error || 'Failed to delete', 'error');
                return;
            }
            if (window.Toast) Toast.show('Query deleted', 'success');
            this.loadAndRender();
        } catch (err) {
            console.error('[SavedQueries] Delete failed:', err);
            if (window.Toast) Toast.show('Failed to delete query', 'error');
        }
    },

    handleSearch(value) {
        this.searchTerm = value;
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
            this.loadAndRender(this.searchTerm);
        }, 300);
    },

    escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    },

    escapeJs(str) {
        if (!str) return '';
        return str.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    }
};

window.SavedQueries = SavedQueries;

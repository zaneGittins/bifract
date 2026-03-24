const InstructionLibraries = {
    libraries: [],
    currentLibrary: null,
    currentPages: [],
    view: 'list', // 'list' or 'detail' or 'editLibrary' or 'editPage'
    editingPage: null,
    pageSearch: '',
    pageOffset: 0,
    pageLimit: 20,

    init() {
        // No-op until shown
    },

    async show() {
        this.view = 'list';
        this.currentLibrary = null;
        await this.loadLibraries();
        this.render();
    },

    async loadLibraries() {
        try {
            const resp = await fetch('/api/v1/instruction-libraries');
            const data = await resp.json();
            this.libraries = data.success && data.data ? data.data : [];
        } catch (e) {
            console.error('Failed to load instruction libraries:', e);
            this.libraries = [];
        }
    },

    async loadLibraryDetail(id) {
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${id}`);
            const data = await resp.json();
            if (data.success && data.data) {
                this.currentLibrary = data.data.library;
                this.currentPages = data.data.pages || [];
            }
        } catch (e) {
            console.error('Failed to load library detail:', e);
        }
    },

    render() {
        const container = document.getElementById('instructionLibrariesView');
        if (!container) return;

        switch (this.view) {
            case 'list':
                container.innerHTML = this.renderListView();
                break;
            case 'detail':
                container.innerHTML = this.renderDetailView();
                break;
            case 'editLibrary':
                container.innerHTML = this.renderLibraryForm();
                break;
            case 'editPage':
                container.innerHTML = this.renderPageForm();
                break;
        }
    },

    renderListView() {
        const libs = this.libraries;
        let html = `
            <div class="il-header">
                <h3>Instruction Libraries</h3>
                <button class="btn-primary btn-sm" onclick="InstructionLibraries.showCreateLibrary()">+ New Library</button>
            </div>
            <p class="il-description">Instruction libraries provide context and guidance to the AI assistant. Pages marked as "always include" are injected into every conversation; others are loaded on demand.</p>
        `;

        if (libs.length === 0) {
            html += '<div class="il-empty">No instruction libraries yet. Create one to get started.</div>';
            return html;
        }

        html += '<div class="il-cards">';
        for (const lib of libs) {
            const sourceBadge = lib.source === 'repo'
                ? `<span class="il-badge il-badge-repo">Repo</span>`
                : `<span class="il-badge il-badge-manual">Manual</span>`;
            const defaultBadge = lib.is_default
                ? `<span class="il-badge il-badge-default">Default</span>`
                : '';
            const syncInfo = lib.source === 'repo' && lib.last_synced_at
                ? `<span class="il-meta">Last sync: ${this.timeAgo(lib.last_synced_at)}</span>`
                : '';

            html += `
                <div class="il-card" onclick="InstructionLibraries.openLibrary('${lib.id}')">
                    <div class="il-card-header">
                        <div class="il-card-title">${this.esc(lib.name)}</div>
                        <div class="il-card-badges">${sourceBadge}${defaultBadge}</div>
                    </div>
                    <div class="il-card-body">
                        <div class="il-card-desc">${this.esc(lib.description) || 'No description'}</div>
                        <div class="il-card-meta">
                            <span class="il-meta">${lib.page_count} page${lib.page_count !== 1 ? 's' : ''}</span>
                            ${syncInfo}
                        </div>
                    </div>
                    <div class="il-card-actions" onclick="event.stopPropagation()">
                        <button class="btn-secondary btn-xs" onclick="InstructionLibraries.showEditLibrary('${lib.id}')">Edit</button>
                        ${lib.source === 'repo' ? `<button class="btn-secondary btn-xs" onclick="InstructionLibraries.syncLibrary('${lib.id}')">Sync Now</button>` : ''}
                        <button class="btn-secondary btn-xs btn-danger-text" onclick="InstructionLibraries.deleteLibrary('${lib.id}', '${this.esc(lib.name)}')">Delete</button>
                    </div>
                </div>
            `;
        }
        html += '</div>';
        return html;
    },

    filterPagesInPlace() {
        const q = this.pageSearch.toLowerCase();
        const rows = document.querySelectorAll('.il-page-row');
        let visible = 0;
        rows.forEach(row => {
            const name = (row.dataset.name || '').toLowerCase();
            const desc = (row.dataset.desc || '').toLowerCase();
            const match = !q || name.includes(q) || desc.includes(q);
            row.style.display = match ? '' : 'none';
            if (match) visible++;
        });
        const count = document.querySelector('.il-page-count');
        if (count) {
            count.textContent = visible + ' page' + (visible !== 1 ? 's' : '') + (q ? ' matched' : '');
        }
        const pageList = document.querySelector('.il-page-list');
        const emptyMsg = document.querySelector('.il-page-empty');
        if (visible === 0 && q) {
            if (!emptyMsg) {
                const el = document.createElement('div');
                el.className = 'il-empty il-page-empty';
                el.textContent = 'No pages match your search.';
                pageList.parentNode.insertBefore(el, pageList.nextSibling);
            }
            if (pageList) pageList.style.display = 'none';
        } else {
            if (emptyMsg) emptyMsg.remove();
            if (pageList) pageList.style.display = '';
        }
    },

    setPageSearch(val) {
        this.pageSearch = val;
        this.filterPagesInPlace();
    },

    setPageOffset(offset) {
        this.pageOffset = offset;
        this.render();
    },

    renderDetailView() {
        const lib = this.currentLibrary;
        if (!lib) return '<div class="il-empty">Library not found.</div>';

        const allPages = this.currentPages;
        let html = `
            <div class="il-header">
                <div class="il-header-left">
                    <button class="btn-secondary btn-xs" onclick="InstructionLibraries.show()">Back</button>
                    <h3>${this.esc(lib.name)}</h3>
                    ${lib.is_default ? '<span class="il-badge il-badge-default">Default</span>' : ''}
                </div>
                <button class="btn-primary btn-sm" onclick="InstructionLibraries.showCreatePage()">+ New Page</button>
            </div>
        `;

        if (lib.description) {
            html += `<p class="il-description">${this.esc(lib.description)}</p>`;
        }

        if (allPages.length === 0) {
            html += '<div class="il-empty">No pages yet. Add a page to provide instructions to the AI.</div>';
            return html;
        }

        const total = allPages.length;

        html += `
            <div class="il-page-toolbar">
                <input type="text" class="il-page-search" placeholder="Search pages..." value="${this.esc(this.pageSearch)}" oninput="InstructionLibraries.setPageSearch(this.value)" />
                <span class="il-page-count">${total} page${total !== 1 ? 's' : ''}</span>
            </div>
        `;

        html += '<div class="il-page-list">';
        for (const page of allPages) {
            const pinIcon = page.always_include
                ? '<span class="il-pin il-pin-active" title="Always included in context">pinned</span>'
                : '<span class="il-pin" title="Available on demand">on-demand</span>';
            const desc = page.description ? `<span class="il-page-desc">${this.esc(page.description)}</span>` : '';

            html += `
                <div class="il-page-row" data-name="${this.esc(page.name)}" data-desc="${this.esc(page.description || '')}">
                    <div class="il-page-info">
                        <div class="il-page-name">${pinIcon} ${this.esc(page.name)}</div>
                        ${desc}
                    </div>
                    <div class="il-page-actions">
                        <button class="btn-secondary btn-xs" onclick="InstructionLibraries.togglePin('${page.id}', ${!page.always_include})">${page.always_include ? 'Unpin' : 'Pin'}</button>
                        <button class="btn-secondary btn-xs" onclick="InstructionLibraries.showEditPage('${page.id}')">Edit</button>
                        <button class="btn-secondary btn-xs btn-danger-text" onclick="InstructionLibraries.deletePage('${page.id}', '${this.esc(page.name)}')">Delete</button>
                    </div>
                </div>
            `;
        }
        html += '</div>';

        return html;
    },

    renderLibraryForm() {
        const lib = this.currentLibrary;
        const isEdit = !!lib;
        const title = isEdit ? 'Edit Library' : 'New Library';
        const name = isEdit ? this.esc(lib.name) : '';
        const desc = isEdit ? this.esc(lib.description) : '';
        const isDefault = isEdit ? lib.is_default : false;
        const source = isEdit ? lib.source : 'manual';
        const repoUrl = isEdit ? this.esc(lib.repo_url) : '';
        const branch = isEdit ? this.esc(lib.branch) : 'main';
        const path = isEdit ? this.esc(lib.path) : '';
        const schedule = isEdit ? lib.sync_schedule : 'daily';

        const repoDisplay = source === 'repo' ? '' : 'display:none;';

        return `
            <div class="il-header">
                <div class="il-header-left">
                    <button class="btn-secondary btn-xs" onclick="InstructionLibraries.${isEdit ? "openLibrary('" + lib.id + "')" : 'show()'}"">Back</button>
                    <h3>${title}</h3>
                </div>
            </div>
            <div class="il-form">
                <div class="il-form-group">
                    <label>Name</label>
                    <input type="text" id="ilFormName" value="${name}" placeholder="e.g. SOC Playbooks" />
                </div>
                <div class="il-form-group">
                    <label>Description</label>
                    <input type="text" id="ilFormDesc" value="${desc}" placeholder="Brief description of this library" />
                </div>
                <div class="il-form-row">
                    <div class="il-form-group">
                        <label>Source</label>
                        <select id="ilFormSource" onchange="InstructionLibraries.toggleRepoFields()">
                            <option value="manual" ${source === 'manual' ? 'selected' : ''}>Manual</option>
                            <option value="repo" ${source === 'repo' ? 'selected' : ''}>Git Repository</option>
                        </select>
                    </div>
                    <div class="il-form-group">
                        <label class="il-checkbox-label"><input type="checkbox" id="ilFormDefault" ${isDefault ? 'checked' : ''} /> Set as default</label>
                    </div>
                </div>
                <div id="ilRepoFields" style="${repoDisplay}">
                    <div class="il-form-group">
                        <label>Repository URL</label>
                        <input type="text" id="ilFormRepoUrl" value="${repoUrl}" placeholder="https://github.com/org/repo" />
                    </div>
                    <div class="il-form-row">
                        <div class="il-form-group">
                            <label>Branch</label>
                            <input type="text" id="ilFormBranch" value="${branch}" placeholder="main" />
                        </div>
                        <div class="il-form-group">
                            <label>Path</label>
                            <input type="text" id="ilFormPath" value="${path}" placeholder="instructions/" />
                        </div>
                    </div>
                    <div class="il-form-row">
                        <div class="il-form-group">
                            <label>Auth Token ${isEdit && lib.has_auth_token ? '(set)' : ''}</label>
                            <input type="password" id="ilFormAuthToken" placeholder="${isEdit && lib.has_auth_token ? 'Leave blank to keep current' : 'Optional'}" />
                        </div>
                        <div class="il-form-group">
                            <label>Sync Schedule</label>
                            <select id="ilFormSchedule">
                                <option value="never" ${schedule === 'never' ? 'selected' : ''}>Never</option>
                                <option value="hourly" ${schedule === 'hourly' ? 'selected' : ''}>Hourly</option>
                                <option value="daily" ${schedule === 'daily' ? 'selected' : ''}>Daily</option>
                                <option value="weekly" ${schedule === 'weekly' ? 'selected' : ''}>Weekly</option>
                                <option value="monthly" ${schedule === 'monthly' ? 'selected' : ''}>Monthly</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="il-form-actions">
                    <button class="btn-secondary" onclick="InstructionLibraries.${isEdit ? "openLibrary('" + lib.id + "')" : 'show()'}">Cancel</button>
                    <button class="btn-primary" onclick="InstructionLibraries.saveLibrary()">${isEdit ? 'Save' : 'Create'}</button>
                </div>
            </div>
        `;
    },

    renderPageForm() {
        const page = this.editingPage;
        const isEdit = !!page;
        const title = isEdit ? 'Edit Page' : 'New Page';
        const name = isEdit ? this.esc(page.name) : '';
        const desc = isEdit ? this.esc(page.description) : '';
        const content = isEdit ? this.esc(page.content) : '';
        const alwaysInclude = isEdit ? page.always_include : false;
        const sortOrder = isEdit ? page.sort_order : 0;

        return `
            <div class="il-header">
                <div class="il-header-left">
                    <button class="btn-secondary btn-xs" onclick="InstructionLibraries.openLibrary('${this.currentLibrary.id}')">Back</button>
                    <h3>${title}</h3>
                </div>
                <div class="il-header-actions">
                    <button class="btn-secondary" onclick="InstructionLibraries.openLibrary('${this.currentLibrary.id}')">Cancel</button>
                    <button class="btn-primary" onclick="InstructionLibraries.savePage()">${isEdit ? 'Save' : 'Create'}</button>
                </div>
            </div>
            <div class="il-page-editor">
                <div class="il-page-editor-main">
                    <div class="il-form-group il-form-group-grow">
                        <label>Content</label>
                        <textarea id="ilPageContent" placeholder="Instructions for the AI...">${content}</textarea>
                    </div>
                </div>
                <div class="il-page-editor-sidebar">
                    <div class="il-form-group">
                        <label>Name</label>
                        <input type="text" id="ilPageName" value="${name}" placeholder="e.g. Incident Response" />
                    </div>
                    <div class="il-form-group">
                        <label>Description <span class="il-hint">(shown in the AI's page index)</span></label>
                        <input type="text" id="ilPageDesc" value="${desc}" placeholder="Brief description for AI context" />
                    </div>
                    <div class="il-form-group">
                        <label>Order</label>
                        <input type="number" id="ilPageOrder" value="${sortOrder}" min="0" />
                    </div>
                    <div class="il-form-group">
                        <label class="il-checkbox-label"><input type="checkbox" id="ilPagePin" ${alwaysInclude ? 'checked' : ''} /> Always include (pinned)</label>
                        <span class="il-hint">Pinned pages are always in the AI's system prompt. Non-pinned pages appear in an index and are loaded on demand.</span>
                    </div>
                </div>
            </div>
        `;
    },

    toggleRepoFields() {
        const source = document.getElementById('ilFormSource').value;
        const fields = document.getElementById('ilRepoFields');
        if (fields) fields.style.display = source === 'repo' ? '' : 'none';
    },

    // --- Actions ---

    showCreateLibrary() {
        this.currentLibrary = null;
        this.view = 'editLibrary';
        this.render();
    },

    async showEditLibrary(id) {
        await this.loadLibraryDetail(id);
        this.view = 'editLibrary';
        this.render();
    },

    async openLibrary(id) {
        await this.loadLibraryDetail(id);
        this.pageSearch = '';
        this.pageOffset = 0;
        this.view = 'detail';
        this.render();
    },

    showCreatePage() {
        this.editingPage = null;
        this.view = 'editPage';
        this.render();
    },

    showEditPage(pageId) {
        this.editingPage = this.currentPages.find(p => p.id === pageId) || null;
        this.view = 'editPage';
        this.render();
    },

    async saveLibrary() {
        const name = document.getElementById('ilFormName').value.trim();
        if (!name) {
            if (window.Toast) Toast.error('Error', 'Name is required');
            return;
        }

        const body = {
            name: name,
            description: document.getElementById('ilFormDesc').value.trim(),
            source: document.getElementById('ilFormSource').value,
            is_default: document.getElementById('ilFormDefault').checked,
            repo_url: document.getElementById('ilFormRepoUrl')?.value.trim() || '',
            branch: document.getElementById('ilFormBranch')?.value.trim() || 'main',
            path: document.getElementById('ilFormPath')?.value.trim() || '',
            auth_token: document.getElementById('ilFormAuthToken')?.value || '',
            sync_schedule: document.getElementById('ilFormSchedule')?.value || 'never',
        };

        const isEdit = !!this.currentLibrary;
        const url = isEdit
            ? `/api/v1/instruction-libraries/${this.currentLibrary.id}`
            : '/api/v1/instruction-libraries';
        const method = isEdit ? 'PUT' : 'POST';

        try {
            const resp = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to save library');
                return;
            }
            if (window.Toast) Toast.success('Success', isEdit ? 'Library updated' : 'Library created');
            if (isEdit) {
                await this.openLibrary(this.currentLibrary.id);
            } else {
                await this.show();
            }
        } catch (e) {
            console.error('Failed to save library:', e);
            if (window.Toast) Toast.error('Error', 'Failed to save library');
        }
    },

    async deleteLibrary(id, name) {
        if (!confirm(`Delete library "${name}" and all its pages?`)) return;

        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${id}`, { method: 'DELETE' });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to delete');
                return;
            }
            if (window.Toast) Toast.success('Deleted', `Library "${name}" deleted`);
            await this.show();
        } catch (e) {
            console.error('Failed to delete library:', e);
        }
    },

    async syncLibrary(id) {
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${id}/sync`, { method: 'POST' });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Sync Failed', data.error || 'Sync failed');
                return;
            }
            if (window.Toast) Toast.success('Synced', `Added: ${data.data.added}, Updated: ${data.data.updated}, Deleted: ${data.data.deleted}`);
            await this.loadLibraries();
            this.render();
        } catch (e) {
            console.error('Failed to sync library:', e);
        }
    },

    async savePage() {
        const name = document.getElementById('ilPageName').value.trim();
        if (!name) {
            if (window.Toast) Toast.error('Error', 'Name is required');
            return;
        }

        const body = {
            name: name,
            description: document.getElementById('ilPageDesc').value.trim(),
            content: document.getElementById('ilPageContent').value,
            always_include: document.getElementById('ilPagePin').checked,
            sort_order: parseInt(document.getElementById('ilPageOrder').value) || 0,
        };

        const isEdit = !!this.editingPage;
        const libId = this.currentLibrary.id;
        const url = isEdit
            ? `/api/v1/instruction-libraries/${libId}/pages/${this.editingPage.id}`
            : `/api/v1/instruction-libraries/${libId}/pages`;
        const method = isEdit ? 'PUT' : 'POST';

        try {
            const resp = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to save page');
                return;
            }
            if (window.Toast) Toast.success('Success', isEdit ? 'Page updated' : 'Page created');
            await this.openLibrary(libId);
        } catch (e) {
            console.error('Failed to save page:', e);
            if (window.Toast) Toast.error('Error', 'Failed to save page');
        }
    },

    async deletePage(pageId, name) {
        if (!confirm(`Delete page "${name}"?`)) return;
        const libId = this.currentLibrary.id;

        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${libId}/pages/${pageId}`, { method: 'DELETE' });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to delete page');
                return;
            }
            if (window.Toast) Toast.success('Deleted', `Page "${name}" deleted`);
            await this.openLibrary(libId);
        } catch (e) {
            console.error('Failed to delete page:', e);
        }
    },

    async togglePin(pageId, pinned) {
        const page = this.currentPages.find(p => p.id === pageId);
        if (!page) return;
        const libId = this.currentLibrary.id;

        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${libId}/pages/${pageId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: page.name,
                    description: page.description,
                    content: page.content,
                    always_include: pinned,
                    sort_order: page.sort_order,
                }),
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to update');
                return;
            }
            await this.openLibrary(libId);
        } catch (e) {
            console.error('Failed to toggle pin:', e);
        }
    },

    // --- Helpers ---

    esc(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    timeAgo(dateStr) {
        if (!dateStr) return '';
        const now = new Date();
        const then = new Date(dateStr);
        const seconds = Math.floor((now - then) / 1000);
        if (seconds < 60) return 'just now';
        const minutes = Math.floor(seconds / 60);
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;
        const days = Math.floor(hours / 24);
        return `${days}d ago`;
    },
};

window.InstructionLibraries = InstructionLibraries;

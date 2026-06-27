// Library - one markdown library per fractal/prism (Obsidian-like), three panes.
//
// Left:   the library's pages (flat list; new page + settings).
// Center: the page editor (inline title + CodeMirror 6 live-preview) or library settings.
// Right:  outline (headings), backlinks ([[wikilinks]]), properties.
//
// Exactly one library exists per fractal/prism, created by default on first access
// (GET /instruction-libraries/ensure-default). Page names are unique within it, so
// [[Page Name]] links resolve deterministically and backlinks are computed server-side.
const InstructionLibraries = {
    library: null,           // the single library for this scope
    pages: [],
    folders: [],
    expandedFolders: {},     // folderId -> bool (persisted per library)
    _newPageFolder: null,    // folder a new page is being created in
    editingPage: null,       // page open in editor (null = new draft)
    selectedPageId: null,
    centerMode: 'page',      // 'page' | 'libraryForm'
    viewMode: 'edit',        // 'edit' | 'view' (Obsidian-style reading mode)
    backlinks: [],
    _pageEditor: null,
    _saveState: 'idle',      // 'idle' | 'unsaved' | 'saving' | 'saved'
    _autosaveTimer: null,
    _savedFadeTimer: null,
    _dragId: null,
    _dragKind: null,
    _prefillPageName: null,

    init() {
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('InstructionLibraries', () => this.onFractalChange());
        }
    },

    onFractalChange() {
        this._destroyEditor();
        this.library = null;
        this.pages = [];
        this.editingPage = null;
        this.selectedPageId = null;
        this.centerMode = 'page';
        this.backlinks = [];
        this._setSaveState('idle');
        const container = document.getElementById('instructionLibrariesView');
        if (container) container.innerHTML = '';
        if (container && container.offsetParent !== null) this.show();
    },

    get libraryId() { return this.library ? this.library.id : null; },

    async show(subPath = '') {
        await this.ensureLibrary();
        if (this.library) await this.loadPages();
        this.openDefault(subPath);
    },

    // --- Data ---

    async ensureLibrary() {
        const token = window.FractalContext?.scopeToken?.();
        try {
            const resp = await fetch('/api/v1/instruction-libraries/ensure-default');
            const data = await resp.json();
            if (window.FractalContext?.isScopeStale?.(token)) return;
            this.library = data.success && data.data ? data.data : null;
        } catch (e) {
            if (window.FractalContext?.isScopeStale?.(token)) return;
            console.error('Failed to resolve library:', e);
            this.library = null;
        }
    },

    async loadPages() {
        if (!this.library) { this.pages = []; this.folders = []; return; }
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${this.library.id}`);
            const data = await resp.json();
            if (data.success && data.data) {
                this.pages = data.data.pages || [];
                this.folders = data.data.folders || [];
                if (data.data.library) this.library = data.data.library;
                this._loadExpanded();
            }
        } catch (e) {
            console.error('Failed to load pages:', e);
        }
    },

    openDefault(subPath = '') {
        if (subPath) {
            const p = this.pages.find(pg => pg.id === subPath);
            if (p) { this.openPage(p.id); return; }
        }
        if (this.pages.length) { this.openPage(this.pages[0].id); return; }
        this.newPage();
    },

    // --- Page selection ---

    async openPage(pageId, doRender = true) {
        await this._maybeAutoSave();
        this._newPageFolder = null;
        this.editingPage = this.pages.find(p => p.id === pageId) || null;
        this.selectedPageId = pageId;
        this.centerMode = 'page';
        this._setSaveState('idle');
        this.backlinks = [];
        if (doRender) this.render();
        this.loadBacklinks(pageId);
    },

    async newPage(folderId) {
        await this._maybeAutoSave();
        this._newPageFolder = folderId || null;
        this.editingPage = null;
        this.selectedPageId = null;
        this.centerMode = 'page';
        this._setSaveState('idle');
        this.backlinks = [];
        this.render();
    },

    async loadBacklinks(pageId) {
        if (!pageId || !this.library) return;
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${this.library.id}/pages/${pageId}/backlinks`);
            const data = await resp.json();
            this.backlinks = data.success && data.data ? data.data : [];
        } catch (e) {
            this.backlinks = [];
        }
        // Update only the backlinks list so editing the description/properties isn't disrupted.
        if (this.selectedPageId === pageId) {
            const el = document.getElementById('ilBacklinksList');
            if (el) el.innerHTML = this._backlinksHtml();
        }
    },

    // --- Render ---

    render() {
        const container = document.getElementById('instructionLibrariesView');
        if (!container) return;
        this._destroyEditor();
        container.classList.add('il-shell-host');
        container.innerHTML = `
            <div class="il-shell">
                <aside class="il-tree" id="ilTree">${this.renderSidebar()}</aside>
                <main class="il-center" id="ilCenter">${this.renderCenter()}</main>
                <aside class="il-context" id="ilContext">${this.renderContext()}</aside>
            </div>
        `;
        this._wireDnD();
        if (this.centerMode === 'page') {
            if (this.viewMode === 'view' && this.editingPage) requestAnimationFrame(() => this._renderViewBody());
            else requestAnimationFrame(() => this._initPageEditor());
        }
    },

    // --- Read-only "View" rendering (no editor, so no cursor risk) ---

    _renderViewBody() {
        const el = document.getElementById('ilViewBody');
        if (!el) return;
        el.innerHTML = this._markdownToHtml(this.editingPage ? (this.editingPage.content || '') : '');
        this._enhanceView(el);
    },

    _markdownToHtml(md) {
        if (!md || !md.trim()) return '<p class="il-view-empty">This page is empty.</p>';
        // Turn [[Page]] into anchors before markdown parsing.
        const pre = md.replace(/\[\[([^\[\]\n]+)\]\]/g, (mm, nm) => {
            const n = nm.trim();
            return `<a class="il-wikilink" data-page="${this.escAttr(n)}">${this.esc(n)}</a>`;
        });
        let html;
        try {
            if (window.marked) {
                marked.setOptions({ breaks: true, gfm: true, headerIds: false, mangle: false });
                html = marked.parse(pre);
            } else {
                html = this.esc(pre);
            }
        } catch (e) {
            html = this.esc(pre);
        }
        return window.DOMPurify ? DOMPurify.sanitize(html, { ADD_ATTR: ['data-page'] }) : html;
    },

    _enhanceView(root) {
        // BQL-highlight code blocks (default / bql / sql) + add a copy button.
        root.querySelectorAll('pre > code').forEach(code => {
            const lang = ((code.className || '').match(/language-([\w-]+)/) || [, ''])[1].toLowerCase();
            const raw = code.textContent;
            if ((!lang || lang === 'bql' || lang === 'sql') && window.SyntaxHighlight && SyntaxHighlight.highlightLine) {
                code.innerHTML = raw.split('\n').map(line => {
                    const segs = SyntaxHighlight.highlightLine(line) || [];
                    return segs.map(s => `<span class="${s.c}">${this.esc(s.t)}</span>`).join('');
                }).join('\n');
            }
            const pre = code.parentElement;
            pre.classList.add('il-codeblock');
            const btn = document.createElement('button');
            btn.className = 'il-copy-btn';
            btn.title = 'Copy';
            btn.innerHTML = this._copyIcon();
            btn.addEventListener('click', () => {
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(raw).then(() => {
                        btn.classList.add('copied');
                        setTimeout(() => btn.classList.remove('copied'), 1200);
                    }).catch(() => {});
                }
            });
            pre.appendChild(btn);
        });
        // Wire wikilink clicks.
        root.querySelectorAll('.il-wikilink').forEach(a => {
            a.addEventListener('click', (e) => { e.preventDefault(); this.openWikilink(a.dataset.page); });
        });
    },

    _copyIcon() { return '<svg width="13" height="13" viewBox="0 0 16 16" fill="none"><rect x="5" y="5" width="8" height="9" rx="1.5" stroke="currentColor" stroke-width="1.4"/><path d="M3 11V3.5A1.5 1.5 0 0 1 4.5 2H10" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>'; },
    _eyeIcon() { return '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><path d="M1.5 8S4 3.5 8 3.5 14.5 8 14.5 8 12 12.5 8 12.5 1.5 8 1.5 8z" stroke-linejoin="round"/><circle cx="8" cy="8" r="2"/></svg>'; },
    _pencilIcon() { return '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><path d="M10.5 2.5l3 3-8 8H2.5v-3l8-8z"/><path d="M9 4l3 3"/></svg>'; },

    renderSidebar() {
        if (!this.library) {
            return `
                <div class="il-tree-header"><span class="il-tree-title">Library</span></div>
                <div class="il-tree-empty">No library configured for this fractal yet.</div>
            `;
        }
        const repoBadge = this.library.source === 'repo' ? `<span class="il-badge il-badge-repo" title="Synced from a git repo">repo</span>` : '';
        let html = `
            <div class="il-tree-header">
                <span class="il-tree-title">${this.esc(this.library.name)} ${repoBadge}</span>
                <span class="il-tree-head-actions">
                    <button class="il-icon-btn" title="New folder" onclick="InstructionLibraries.newFolder()">${this._folderPlusIcon()}</button>
                    <button class="il-icon-btn" title="New page" onclick="InstructionLibraries.newPage()">${this._plusIcon()}</button>
                    <button class="il-icon-btn" title="Library settings" onclick="InstructionLibraries.showLibrarySettings()">${this._gearIcon()}</button>
                </span>
            </div>
            <div class="il-tree-scroll" id="ilTreeScroll">
        `;
        const folders = this._sortedFolders();
        const rootPages = this._pagesIn(null);
        if (!folders.length && !this.pages.length) {
            html += '<div class="il-tree-page-empty">No pages yet</div>';
        } else {
            for (const f of folders) {
                html += this._folderRowHtml(f);
                if (this.expandedFolders[f.id]) {
                    const fpages = this._pagesIn(f.id);
                    html += '<div class="il-folder-pages">';
                    html += fpages.length ? fpages.map(p => this._pageRowHtml(p)).join('')
                                          : '<div class="il-tree-page-empty il-folder-empty">Empty - drop pages here</div>';
                    html += '</div>';
                }
            }
            html += rootPages.map(p => this._pageRowHtml(p)).join('');
        }
        html += '</div>';
        return html;
    },

    _sortedFolders() {
        return this.folders.slice().sort((a, b) => (a.sort_order - b.sort_order) || a.name.localeCompare(b.name));
    },

    _pagesIn(folderId) {
        const fid = folderId || null;
        return this.pages.filter(p => (p.folder_id || null) === fid)
            .sort((a, b) => (a.sort_order - b.sort_order) || a.name.localeCompare(b.name));
    },

    _pageRowHtml(page) {
        const active = page.id === this.selectedPageId ? 'is-active' : '';
        const pin = page.always_include ? `<span class="il-pin-dot" title="Always included in AI context"></span>` : '';
        return `
            <div class="il-tree-page ${active}" data-page-id="${page.id}" data-folder="${page.folder_id || ''}" onclick="InstructionLibraries.openPage('${page.id}')">
                ${this._pageIcon()}
                <span class="il-tree-page-name">${this.esc(page.name)}</span>
                ${pin}
                <span class="kebab-wrapper" onclick="event.stopPropagation()">
                    <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">&#8942;</button>
                    <div class="kebab-menu">
                        <button class="kebab-item danger" onclick="InstructionLibraries.deletePage('${page.id}', '${this.escAttr(page.name)}')">Delete</button>
                    </div>
                </span>
            </div>
        `;
    },

    _folderRowHtml(folder) {
        const open = !!this.expandedFolders[folder.id];
        const count = this._pagesIn(folder.id).length;
        return `
            <div class="il-tree-folder ${open ? 'is-open' : ''}" data-folder-id="${folder.id}" onclick="InstructionLibraries.toggleFolder('${folder.id}')">
                <span class="il-chevron">${this._chevronIcon()}</span>
                ${this._folderIcon()}
                <span class="il-tree-folder-name">${this.esc(folder.name)}</span>
                <span class="il-folder-count">${count}</span>
                <span class="il-tree-folder-actions" onclick="event.stopPropagation()">
                    <button class="il-icon-btn il-xs" title="New page in folder" onclick="InstructionLibraries.newPage('${folder.id}')">${this._plusIcon()}</button>
                    <span class="kebab-wrapper">
                        <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">&#8942;</button>
                        <div class="kebab-menu">
                            <button class="kebab-item" onclick="InstructionLibraries.renameFolder('${folder.id}', '${this.escAttr(folder.name)}')">Rename</button>
                            <button class="kebab-item danger" onclick="InstructionLibraries.deleteFolder('${folder.id}', '${this.escAttr(folder.name)}')">Delete folder</button>
                        </div>
                    </span>
                </span>
            </div>
        `;
    },

    // --- Folder actions ---

    _expandedKey() { return 'il-expanded-' + (this.library ? this.library.id : 'none'); },
    _loadExpanded() {
        try { this.expandedFolders = JSON.parse(localStorage.getItem(this._expandedKey()) || '{}') || {}; }
        catch (e) { this.expandedFolders = {}; }
    },
    _saveExpanded() {
        try { localStorage.setItem(this._expandedKey(), JSON.stringify(this.expandedFolders)); } catch (e) {}
    },

    toggleFolder(id) {
        this.expandedFolders[id] = !this.expandedFolders[id];
        this._saveExpanded();
        this._renderSidebar();
    },

    async newFolder() {
        if (!this.library) return;
        const name = (prompt('Folder name') || '').trim();
        if (!name) return;
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${this.library.id}/folders`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name }),
            });
            const data = await resp.json();
            if (!data.success) { if (window.Toast) Toast.error('Error', data.error || 'Failed to create folder'); return; }
            if (data.data && data.data.id) { this.expandedFolders[data.data.id] = true; this._saveExpanded(); }
            await this.loadPages();
            this._renderSidebar();
        } catch (e) { console.error('Failed to create folder:', e); }
    },

    async renameFolder(id, current) {
        const name = (prompt('Rename folder', current) || '').trim();
        if (!name || name === current) return;
        const folder = this.folders.find(f => f.id === id);
        try {
            await fetch(`/api/v1/instruction-libraries/${this.library.id}/folders/${id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, sort_order: folder ? folder.sort_order : 0 }),
            });
            await this.loadPages();
            this._renderSidebar();
        } catch (e) { console.error('Failed to rename folder:', e); }
    },

    async deleteFolder(id, name) {
        if (!confirm(`Delete folder "${name}"? Its pages move to the root (they are not deleted).`)) return;
        try {
            await fetch(`/api/v1/instruction-libraries/${this.library.id}/folders/${id}`, { method: 'DELETE' });
            delete this.expandedFolders[id];
            this._saveExpanded();
            await this.loadPages();
            this._renderSidebar();
        } catch (e) { console.error('Failed to delete folder:', e); }
    },

    renderCenter() {
        if (this.centerMode === 'libraryForm') return this.renderLibraryForm();
        return this.renderPageEditor();
    },

    renderPageEditor() {
        if (!this.library) return '<div class="il-center-empty"><div class="il-center-empty-inner"><p>No library available.</p></div></div>';
        const isEdit = !!this.editingPage;
        const isView = isEdit && this.viewMode === 'view';
        const name = isEdit ? this.escAttr(this.editingPage.name) : this.escAttr(this._prefillPageName || '');
        const toggle = isEdit
            ? `<button class="il-mode-toggle" title="${isView ? 'Edit' : 'View (read-only)'}" onclick="InstructionLibraries.setViewMode('${isView ? 'edit' : 'view'}')">${isView ? this._pencilIcon() : this._eyeIcon()}</button>`
            : '';
        const kebab = isEdit ? `<span class="kebab-wrapper">
                            <button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">&#8942;</button>
                            <div class="kebab-menu">
                                <button class="kebab-item danger" onclick="InstructionLibraries.deletePage('${this.editingPage.id}', '${this.escAttr(this.editingPage.name)}')">Delete page</button>
                            </div>
                        </span>` : '';
        return `
            <div class="il-editor">
                <div class="il-editor-topbar">
                    ${!isView ? `<span id="ilSaveState" class="il-save-state il-save-${this._saveState}" title=""></span>` : ''}
                    <span class="il-mode-pill ${isView ? 'is-view' : 'is-edit'}">${isView ? 'Reading' : 'Editing'}</span>
                    <input type="text" id="ilPageNameInput" class="il-title-input" placeholder="Untitled" value="${name}" ${isView ? 'readonly' : ''}
                        oninput="InstructionLibraries.markDirty()" />
                    <div class="il-editor-actions">${toggle}${kebab}</div>
                </div>
                ${isView
                    ? '<div id="ilViewBody" class="il-view"></div>'
                    : '<div id="ilPageEditorHost" class="il-cm-host"></div>'}
            </div>
        `;
    },

    async setViewMode(mode) {
        if (mode === 'view') await this._maybeAutoSave(); // flush edits so the render is current
        this.viewMode = mode;
        this.render();
    },

    renderContext() {
        return `<div class="il-context-inner">${this._contextBody()}</div>`;
    },

    _contextBody() {
        if (this.centerMode !== 'page' || !this.library) {
            return '<div class="il-context-empty">No page open.</div>';
        }
        const content = this._pageEditor ? this._pageEditor.getValue() : (this.editingPage ? this.editingPage.content : '');
        const page = this.editingPage;
        const pin = page ? page.always_include : false;
        const order = page ? page.sort_order : 0;
        const desc = page ? this.escAttr(page.description || '') : '';
        return `
            <div class="il-context-section">
                <div class="il-context-head">Outline</div>
                <div id="ilOutlineList" class="il-outline">${this._outlineHtml(content)}</div>
            </div>
            <div class="il-context-section">
                <div class="il-context-head">Backlinks</div>
                <div id="ilBacklinksList" class="il-backlinks">${this._backlinksHtml()}</div>
            </div>
            <div class="il-context-section">
                <div class="il-context-head">Properties</div>
                <label class="il-prop-row il-checkbox-label">
                    <input type="checkbox" id="ilPropPin" ${pin ? 'checked' : ''} onchange="InstructionLibraries.markDirty()" />
                    <span>Always include (pinned)</span>
                </label>
                <div class="il-prop-row">
                    <label>Description</label>
                    <input type="text" id="ilPropDesc" value="${desc}" placeholder="Shown in the AI's page index" oninput="InstructionLibraries.markDirty()" />
                </div>
                <div class="il-prop-row">
                    <label>Order</label>
                    <input type="number" id="ilPropOrder" value="${order}" min="0" oninput="InstructionLibraries.markDirty()" />
                </div>
            </div>
        `;
    },

    _renderContext() {
        const el = document.getElementById('ilContext');
        if (el) el.innerHTML = this.renderContext();
    },

    _outlineHtml(content) {
        const heads = this._headings(content);
        if (!heads.length) return '<div class="il-context-empty">No headings.</div>';
        return heads.map(h =>
            `<div class="il-outline-item il-outline-l${h.level}" onclick="InstructionLibraries.scrollToPos(${h.pos})">${this.esc(h.text)}</div>`
        ).join('');
    },

    _backlinksHtml() {
        if (!this.selectedPageId) return '<div class="il-context-empty">Save the page to see backlinks.</div>';
        if (!this.backlinks.length) return '<div class="il-context-empty">No pages link here.</div>';
        return this.backlinks.map(b =>
            `<div class="il-backlink" onclick="InstructionLibraries.openPage('${b.id}')">${this._linkIcon()}<span>${this.esc(b.name)}</span></div>`
        ).join('');
    },

    _headings(content) {
        const out = [];
        const re = /^(#{1,6})\s+(.+?)\s*$/gm;
        let m;
        while ((m = re.exec(content)) !== null) out.push({ level: m[1].length, text: m[2], pos: m.index });
        return out;
    },

    scrollToPos(pos) {
        if (this._pageEditor && this._pageEditor.scrollTo) this._pageEditor.scrollTo(pos);
    },

    // --- Editor lifecycle ---

    _initPageEditor() {
        const host = document.getElementById('ilPageEditorHost');
        if (!host || !window.MarkdownEditor || !MarkdownEditor.ready()) return;
        const value = this.editingPage ? (this.editingPage.content || '') : '';
        this._pageEditor = MarkdownEditor.create(host, {
            value,
            livePreview: true,
            onChange: () => { this.markDirty(); this._updateOutline(); },
            onSave: () => this.savePage(true),
            wikilinkTargets: () => this.pages.map(p => p.name),
            onWikilink: (name) => this.openWikilink(name),
        });
        const nameInput = document.getElementById('ilPageNameInput');
        if (nameInput && !this.editingPage) nameInput.focus();
        else if (this._pageEditor) this._pageEditor.focus();
        this._prefillPageName = null;
    },

    _destroyEditor() {
        if (this._pageEditor) { try { this._pageEditor.destroy(); } catch (e) {} this._pageEditor = null; }
    },

    _updateOutline() {
        const el = document.getElementById('ilOutlineList');
        if (el && this._pageEditor) el.innerHTML = this._outlineHtml(this._pageEditor.getValue());
    },

    markDirty() {
        this._setSaveState('unsaved');
        this.scheduleAutosave();
    },

    scheduleAutosave() {
        clearTimeout(this._autosaveTimer);
        this._autosaveTimer = setTimeout(() => this.autosave(), 900);
    },

    async autosave() {
        const nameEl = document.getElementById('ilPageNameInput');
        if (this.centerMode !== 'page' || !nameEl || !nameEl.value.trim()) return; // need a name to save
        await this.savePage(true);
    },

    _setSaveState(state) {
        this._saveState = state;
        const el = document.getElementById('ilSaveState');
        if (el) {
            el.className = 'il-save-state il-save-' + state;
            el.title = state === 'saving' ? 'Saving' : state === 'saved' ? 'Saved' : state === 'unsaved' ? 'Unsaved changes' : '';
        }
        if (state === 'saved') {
            clearTimeout(this._savedFadeTimer);
            this._savedFadeTimer = setTimeout(() => { if (this._saveState === 'saved') this._setSaveState('idle'); }, 1600);
        }
    },

    async _maybeAutoSave() {
        // Flush a pending autosave before navigating away (existing or named-new pages).
        clearTimeout(this._autosaveTimer);
        const nameEl = document.getElementById('ilPageNameInput');
        if (this.centerMode === 'page' && nameEl && nameEl.value.trim() &&
            (this._saveState === 'unsaved' || this._saveState === 'saving')) {
            await this.savePage(true);
        }
    },

    // Re-render only the sidebar tree (keeps the editor + its focus intact).
    _renderSidebar() {
        const el = document.getElementById('ilTree');
        if (el) { el.innerHTML = this.renderSidebar(); this._wireDnD(); }
    },

    // Drag-and-drop: pages into folders / root / reorder; folders reorder.
    _wireDnD() {
        const tree = document.getElementById('ilTree');
        if (!tree) return;
        const clearDrop = () => tree.querySelectorAll('.il-drop-before,.il-drop-after,.il-drop-into')
            .forEach(el => el.classList.remove('il-drop-before', 'il-drop-after', 'il-drop-into'));

        tree.querySelectorAll('.il-tree-page').forEach(row => {
            row.setAttribute('draggable', 'true');
            row.addEventListener('dragstart', (e) => { this._dragKind = 'page'; this._dragId = row.dataset.pageId; e.dataTransfer.effectAllowed = 'move'; e.stopPropagation(); row.classList.add('il-dragging'); });
            row.addEventListener('dragend', () => { row.classList.remove('il-dragging'); clearDrop(); });
            row.addEventListener('dragover', (e) => {
                if (this._dragKind !== 'page') return;
                e.preventDefault(); clearDrop();
                const rect = row.getBoundingClientRect();
                row.classList.add((e.clientY - rect.top) < rect.height / 2 ? 'il-drop-before' : 'il-drop-after');
            });
            row.addEventListener('dragleave', () => row.classList.remove('il-drop-before', 'il-drop-after'));
            row.addEventListener('drop', (e) => {
                e.preventDefault(); e.stopPropagation(); clearDrop();
                if (this._dragKind !== 'page') return;
                const rect = row.getBoundingClientRect();
                const before = (e.clientY - rect.top) < rect.height / 2;
                this._applyPageMove(this._dragId, row.dataset.folder || null, row.dataset.pageId, !before);
            });
        });

        tree.querySelectorAll('.il-tree-folder').forEach(row => {
            row.setAttribute('draggable', 'true');
            row.addEventListener('dragstart', (e) => { this._dragKind = 'folder'; this._dragId = row.dataset.folderId; e.dataTransfer.effectAllowed = 'move'; e.stopPropagation(); row.classList.add('il-dragging'); });
            row.addEventListener('dragend', () => { row.classList.remove('il-dragging'); clearDrop(); });
            row.addEventListener('dragover', (e) => {
                e.preventDefault(); clearDrop();
                if (this._dragKind === 'page') { row.classList.add('il-drop-into'); }
                else if (this._dragKind === 'folder') {
                    const rect = row.getBoundingClientRect();
                    row.classList.add((e.clientY - rect.top) < rect.height / 2 ? 'il-drop-before' : 'il-drop-after');
                }
            });
            row.addEventListener('dragleave', () => row.classList.remove('il-drop-before', 'il-drop-after', 'il-drop-into'));
            row.addEventListener('drop', (e) => {
                e.preventDefault(); e.stopPropagation(); clearDrop();
                const fid = row.dataset.folderId;
                if (this._dragKind === 'page') {
                    this.expandedFolders[fid] = true; this._saveExpanded();
                    this._applyPageMove(this._dragId, fid, null, false);
                } else if (this._dragKind === 'folder') {
                    const rect = row.getBoundingClientRect();
                    const before = (e.clientY - rect.top) < rect.height / 2;
                    this._applyFolderMove(this._dragId, fid, !before);
                }
            });
        });

        // Background of the tree = drop to root (only fires if a row didn't handle it).
        const scroll = tree.querySelector('#ilTreeScroll');
        if (scroll) {
            scroll.addEventListener('dragover', (e) => { e.preventDefault(); });
            scroll.addEventListener('drop', (e) => {
                e.preventDefault(); clearDrop();
                if (this._dragKind === 'page') this._applyPageMove(this._dragId, null, null, false);
                else if (this._dragKind === 'folder') this._applyFolderMove(this._dragId, null, false);
            });
        }
    },

    async _applyPageMove(pageId, targetFolder, refPageId, placeAfter) {
        if (!pageId || !this.library) return;
        targetFolder = targetFolder || null;
        const moved = this.pages.find(p => p.id === pageId);
        if (!moved) return;
        const oldFolder = moved.folder_id || null;
        const dest = this._pagesIn(targetFolder).filter(p => p.id !== pageId);
        let idx = dest.length;
        if (refPageId) {
            const i = dest.findIndex(p => p.id === refPageId);
            if (i >= 0) idx = placeAfter ? i + 1 : i;
        }
        dest.splice(idx, 0, moved);
        moved.folder_id = targetFolder;

        const changed = [];
        dest.forEach((p, i) => { if (p.sort_order !== i || p === moved) { p.sort_order = i; changed.push(p); } });
        if (oldFolder !== targetFolder) {
            this._pagesIn(oldFolder).forEach((p, i) => { if (p.sort_order !== i) { p.sort_order = i; changed.push(p); } });
        }
        this._renderSidebar();

        const seen = new Set();
        for (const p of changed) {
            if (seen.has(p.id)) continue;
            seen.add(p.id);
            await this._patchMovePage(p.id, p.folder_id || null, p.sort_order);
        }
    },

    async _patchMovePage(pageId, folderId, sortOrder) {
        try {
            await fetch(`/api/v1/instruction-libraries/${this.library.id}/pages/${pageId}/move`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ folder_id: folderId, sort_order: sortOrder }),
            });
        } catch (e) { console.error('Failed to move page:', e); }
    },

    async _applyFolderMove(folderId, refFolderId, placeAfter) {
        if (!folderId || folderId === refFolderId || !this.library) return;
        const list = this._sortedFolders();
        const moved = list.find(f => f.id === folderId);
        if (!moved) return;
        const rest = list.filter(f => f.id !== folderId);
        let idx = rest.length;
        if (refFolderId) {
            const i = rest.findIndex(f => f.id === refFolderId);
            if (i >= 0) idx = placeAfter ? i + 1 : i;
        }
        rest.splice(idx, 0, moved);

        const changed = [];
        rest.forEach((f, i) => { if (f.sort_order !== i) { f.sort_order = i; changed.push(f); } });
        this.folders = rest;
        this._renderSidebar();

        for (const f of changed) {
            try {
                await fetch(`/api/v1/instruction-libraries/${this.library.id}/folders/${f.id}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: f.name, sort_order: f.sort_order }),
                });
            } catch (e) { console.error('Failed to persist folder order:', e); }
        }
    },

    openWikilink(name) {
        const target = this.pages.find(p => p.name.toLowerCase() === String(name).trim().toLowerCase());
        if (target) {
            this.openPage(target.id);
        } else if (confirm(`Page "${name}" doesn't exist yet. Create it?`)) {
            this._prefillPageName = String(name).trim();
            this.newPage();
        }
    },

    // --- Page actions ---

    async savePage(silent = false) {
        const nameEl = document.getElementById('ilPageNameInput');
        if (!nameEl || !this.library) return;
        const name = nameEl.value.trim();
        if (!name) {
            if (!silent && window.Toast) Toast.error('Error', 'Page name is required');
            return;
        }
        const body = {
            name,
            description: (document.getElementById('ilPropDesc')?.value || '').trim(),
            content: this._pageEditor ? this._pageEditor.getValue() : (this.editingPage ? this.editingPage.content : ''),
            always_include: document.getElementById('ilPropPin')?.checked || false,
            sort_order: parseInt(document.getElementById('ilPropOrder')?.value) || 0,
        };
        const libId = this.library.id;
        const isEdit = !!this.editingPage;
        if (!isEdit) body.folder_id = this._newPageFolder || null; // create page inside its folder
        const url = isEdit
            ? `/api/v1/instruction-libraries/${libId}/pages/${this.editingPage.id}`
            : `/api/v1/instruction-libraries/${libId}/pages`;
        const method = isEdit ? 'PUT' : 'POST';
        this._setSaveState('saving');
        try {
            const resp = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!data.success) {
                this._setSaveState('unsaved');
                if (window.Toast) Toast.error('Error', data.error || 'Failed to save page');
                return;
            }
            const saved = data.data;
            await this.loadPages();
            if (saved && saved.id) {
                this.editingPage = this.pages.find(p => p.id === saved.id) || saved;
                this.selectedPageId = saved.id;
            }
            this._setSaveState('saved');
            if (silent) {
                this._renderSidebar();   // reflect new/renamed page without disturbing the editor
                this.loadBacklinks(this.selectedPageId);
            } else {
                this.render();
                this.loadBacklinks(this.selectedPageId);
            }
        } catch (e) {
            this._setSaveState('unsaved');
            console.error('Failed to save page:', e);
            if (window.Toast) Toast.error('Error', 'Failed to save page');
        }
    },

    async deletePage(pageId, name) {
        if (!confirm(`Delete page "${name}"?`)) return;
        if (!this.library) return;
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${this.library.id}/pages/${pageId}`, { method: 'DELETE' });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to delete page');
                return;
            }
            await this.loadPages();
            this._setSaveState('idle');
            this.openDefault();
        } catch (e) {
            console.error('Failed to delete page:', e);
        }
    },

    // --- Library settings ---

    showLibrarySettings() {
        this.centerMode = 'libraryForm';
        this.render();
    },

    renderLibraryForm() {
        const lib = this.library;
        if (!lib) return '';
        const name = this.escAttr(lib.name);
        const desc = this.escAttr(lib.description);
        const source = lib.source;
        const repoUrl = this.escAttr(lib.repo_url);
        const branch = this.escAttr(lib.branch) || 'main';
        const path = this.escAttr(lib.path);
        const schedule = lib.sync_schedule || 'never';
        const repoDisplay = source === 'repo' ? '' : 'display:none;';
        return `
            <div class="il-editor il-library-form">
                <div class="il-editor-topbar">
                    <span class="il-title-static">Library settings</span>
                    <div class="il-editor-actions">
                        <button class="btn-secondary btn-sm" onclick="InstructionLibraries.closeLibraryForm()">Cancel</button>
                        <button class="btn-primary btn-sm" onclick="InstructionLibraries.saveLibrary()">Save</button>
                    </div>
                </div>
                <div class="il-form il-form-scroll">
                    <div class="il-form-group">
                        <label>Name</label>
                        <input type="text" id="ilFormName" value="${name}" placeholder="Library" />
                    </div>
                    <div class="il-form-group">
                        <label>Description</label>
                        <input type="text" id="ilFormDesc" value="${desc}" placeholder="Brief description" />
                    </div>
                    <div class="il-form-group">
                        <label>Source</label>
                        <select id="ilFormSource" onchange="InstructionLibraries.toggleRepoFields()">
                            <option value="manual" ${source === 'manual' ? 'selected' : ''}>Manual</option>
                            <option value="repo" ${source === 'repo' ? 'selected' : ''}>Git Repository</option>
                        </select>
                        <span class="il-hint">A git-backed library syncs its pages from a repository.</span>
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
                                <label>Auth Token ${lib.has_auth_token ? '(set)' : ''}</label>
                                <input type="password" id="ilFormAuthToken" placeholder="${lib.has_auth_token ? 'Leave blank to keep current' : 'Optional'}" />
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
                        <div class="il-form-actions">
                            <button class="btn-secondary btn-sm" onclick="InstructionLibraries.syncLibrary()">Sync now</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
    },

    closeLibraryForm() {
        this.centerMode = 'page';
        this.render();
    },

    toggleRepoFields() {
        const source = document.getElementById('ilFormSource').value;
        const fields = document.getElementById('ilRepoFields');
        if (fields) fields.style.display = source === 'repo' ? '' : 'none';
    },

    async saveLibrary() {
        if (!this.library) return;
        const name = document.getElementById('ilFormName').value.trim();
        if (!name) {
            if (window.Toast) Toast.error('Error', 'Name is required');
            return;
        }
        const body = {
            name,
            description: document.getElementById('ilFormDesc').value.trim(),
            source: document.getElementById('ilFormSource').value,
            is_default: true,
            repo_url: document.getElementById('ilFormRepoUrl')?.value.trim() || '',
            branch: document.getElementById('ilFormBranch')?.value.trim() || 'main',
            path: document.getElementById('ilFormPath')?.value.trim() || '',
            auth_token: document.getElementById('ilFormAuthToken')?.value || '',
            sync_schedule: document.getElementById('ilFormSchedule')?.value || 'never',
        };
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${this.library.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Error', data.error || 'Failed to save library');
                return;
            }
            if (data.data) this.library = data.data;
            await this.loadPages();
            this.centerMode = 'page';
            this.openDefault();
        } catch (e) {
            console.error('Failed to save library:', e);
            if (window.Toast) Toast.error('Error', 'Failed to save library');
        }
    },

    async syncLibrary() {
        if (!this.library) return;
        try {
            const resp = await fetch(`/api/v1/instruction-libraries/${this.library.id}/sync`, { method: 'POST' });
            const data = await resp.json();
            if (!data.success) {
                if (window.Toast) Toast.error('Sync Failed', data.error || 'Sync failed');
                return;
            }
            if (window.Toast) Toast.success('Synced', `Added: ${data.data.added}, Updated: ${data.data.updated}, Deleted: ${data.data.deleted}`);
            await this.loadPages();
            this.centerMode = 'page';
            this.openDefault();
        } catch (e) {
            console.error('Failed to sync library:', e);
        }
    },

    // --- Icons / helpers ---

    _plusIcon() { return '<svg width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M8 3v10M3 8h10" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>'; },
    _chevronIcon() { return '<svg width="11" height="11" viewBox="0 0 16 16" fill="none"><path d="M6 4l4 4-4 4" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>'; },
    _folderIcon() { return '<svg class="il-folder-glyph" width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M1.5 4.5a1 1 0 0 1 1-1h3l1.5 1.5h6a1 1 0 0 1 1 1v6a1 1 0 0 1-1 1h-11a1 1 0 0 1-1-1v-7.5z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/></svg>'; },
    _folderPlusIcon() { return '<svg width="15" height="15" viewBox="0 0 16 16" fill="none"><path d="M1.5 4.5a1 1 0 0 1 1-1h3l1.5 1.5h6a1 1 0 0 1 1 1v6a1 1 0 0 1-1 1h-11a1 1 0 0 1-1-1v-7.5z" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"/><path d="M8 8v3M6.5 9.5h3" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>'; },
    _gearIcon() { return '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>'; },
    _pageIcon() { return '<svg class="il-page-glyph" width="13" height="13" viewBox="0 0 16 16" fill="none"><path d="M4 2h5l3 3v9H4V2z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/><path d="M9 2v3h3" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/></svg>'; },
    _linkIcon() { return '<svg width="12" height="12" viewBox="0 0 16 16" fill="none"><path d="M6.5 9.5l3-3M7 4l1-1a2.5 2.5 0 013.5 3.5l-1 1M9 12l-1 1A2.5 2.5 0 014.5 9.5l1-1" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>'; },

    esc(str) {
        if (str === null || str === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(str);
        return div.innerHTML;
    },

    escAttr(str) { return this.esc(str).replace(/"/g, '&quot;'); },

    timeAgo(dateStr) {
        if (!dateStr) return '';
        const seconds = Math.floor((new Date() - new Date(dateStr)) / 1000);
        if (seconds < 60) return 'just now';
        const minutes = Math.floor(seconds / 60);
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;
        return `${Math.floor(hours / 24)}d ago`;
    },
};

window.InstructionLibraries = InstructionLibraries;

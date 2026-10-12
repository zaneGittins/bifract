// Lookups module for Bifract
const Dictionaries = {
    currentDictionary: null,
    allDictionaries: [],
    filteredDictionaries: [],
    currentPage: 0,
    pageSize: 20,
    rowPage: 0,
    rowPageSize: 50,
    rowSearch: '',
    totalRows: 0,
    _csvFile: null,
    _metaSaveTimer: null,

    init() {
        this.currentDictionary = null;
        this.allDictionaries = [];
        this.filteredDictionaries = [];
        this.currentPage = 0;
        this.rowPage = 0;
        this.rowSearch = '';
        this.totalRows = 0;
        this._csvFile = null;
        if (window.FractalContext && typeof FractalContext.subscribe === 'function') {
            FractalContext.subscribe('Dictionaries', () => this.onFractalChange());
        }
    },

    onFractalChange() {
        this.currentDictionary = null;
        this.allDictionaries = [];
        this.filteredDictionaries = [];
        this.currentPage = 0;
        const tbody = document.getElementById('dictsTableBody');
        if (tbody) tbody.innerHTML = '';

        if (FractalContext.shouldReload('dictionariesView')) this.showListing();
    },

    show(subPath = '') {
        this.bindEvents();
        if (subPath) {
            this.openDictionary(subPath);
        } else {
            this.showListing();
        }
    },

    bindEvents() {
        document.getElementById('dictCreateBtn')?.addEventListener('click', () => this.showCreateForm());
        document.getElementById('dictCreateSaveBtn')?.addEventListener('click', () => this.saveCreateDictionary());
        document.getElementById('dictCreateCancelBtn')?.addEventListener('click', () => this.hideCreateForm());
        document.getElementById('dictNewKind')?.addEventListener('change', () => { this.updateKindHint(); this.updateNewSourceFields(); });
        document.getElementById('dictNewSourceMode')?.addEventListener('change', () => this.updateNewSourceFields());
        document.getElementById('dictNewName')?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.saveCreateDictionary();
            if (e.key === 'Escape') this.hideCreateForm();
        });
        document.getElementById('dictSearchInput')?.addEventListener('input', (e) => {
            this.currentPage = 0;
            this.filterDictionaries(e.target.value);
        });
        document.getElementById('dictsPrevBtn')?.addEventListener('click', () => {
            if (this.currentPage > 0) { this.currentPage--; this.renderPage(); }
        });
        document.getElementById('dictsNextBtn')?.addEventListener('click', () => {
            const max = Math.ceil(this.filteredDictionaries.length / this.pageSize) - 1;
            if (this.currentPage < max) { this.currentPage++; this.renderPage(); }
        });
    },

    // ---- Listing ----

    showListing() {
        const listing = document.getElementById('dictListing');
        const detail = document.getElementById('dictDetailView');
        if (listing) listing.style.display = 'block';
        if (detail) { detail.style.display = 'none'; detail.innerHTML = ''; }
        this.loadDictionaries();
    },

    async loadDictionaries() {
        const tableContainer = document.getElementById('dictsTableBody')?.closest('.dashboards-table-container');
        const emptyEl = document.getElementById('dictsEmptyState');
        const paginationEl = document.getElementById('dictsPrevBtn')?.parentElement;
        if (tableContainer) tableContainer.style.display = 'none';
        if (emptyEl) emptyEl.style.display = 'none';
        if (paginationEl) paginationEl.style.display = 'none';
        const token = window.FractalContext?.scopeToken?.();
        try {
            const resp = await fetch('/api/v1/dictionaries', { credentials: 'include' });
            const data = await resp.json();
            if (window.FractalContext?.isScopeStale?.(token)) return;
            if (data.success) {
                this.allDictionaries = data.data || [];
                this.filterDictionaries(document.getElementById('dictSearchInput')?.value || '');
            }
        } catch (e) {
            if (window.FractalContext?.isScopeStale?.(token)) return;
            console.error('Failed to load lookups:', e);
        }
    },

    filterDictionaries(query) {
        const q = (query || '').toLowerCase();
        this.filteredDictionaries = q
            ? this.allDictionaries.filter(d =>
                d.name.toLowerCase().includes(q) || (d.description || '').toLowerCase().includes(q))
            : this.allDictionaries.slice();
        this.renderPage();
    },

    renderPage() {
        const start = this.currentPage * this.pageSize;
        this.renderDictionaryTable(this.filteredDictionaries.slice(start, start + this.pageSize));
        this.updatePagination();
    },

    renderDictionaryTable(dicts) {
        const tbody = document.getElementById('dictsTableBody');
        if (!tbody) return;
        const tableContainer = tbody.closest('.dashboards-table-container');
        const emptyEl = document.getElementById('dictsEmptyState');

        if (dicts.length === 0) {
            if (tableContainer) tableContainer.style.display = 'none';
            if (emptyEl) {
                emptyEl.style.display = '';
                const msg = emptyEl.querySelector('p');
                if (msg) {
                    msg.textContent = this.filteredDictionaries.length === 0
                        ? 'No lookups yet'
                        : 'No results match your search';
                }
            }
            return;
        }

        if (tableContainer) tableContainer.style.display = '';
        if (emptyEl) emptyEl.style.display = 'none';

        tbody.innerHTML = dicts.map(d => `
<tr class="dict-list-row">
    <td><a href="#" class="dict-link" data-id="${d.id}">${this.esc(d.name)}</a>${d.source ? '<span class="dict-src-tag" title="Fetched from a URL">URL</span>' : ''}</td>
    <td class="dict-col-desc" title="${this.esc(d.description || '')}">${this.esc(d.description || '')}</td>
    <td class="dict-col-meta" title="${this.esc(this.KINDS[this.kindOf(d)].hint)}">${this.esc(this.KINDS[this.kindOf(d)].label)}</td>
    <td class="dict-col-meta${d.is_global ? ' dict-col-scope-global' : ''}">${this.esc(this.scopeLabel(d))}</td>
    <td class="dict-col-meta">${d.columns ? d.columns.length : 0}</td>
    <td class="dict-col-meta">${(d.row_count || 0).toLocaleString()}</td>
    <td class="kebab-cell"><div class="kebab-wrapper"><button class="kebab-btn" onclick="KebabMenu.toggle(event,this)">⋮</button><div class="kebab-menu"><button class="kebab-item danger" data-id="${d.id}" data-action="delete">Delete</button></div></div></td>
</tr>`).join('');

        tbody.querySelectorAll('.dict-link').forEach(a => {
            a.addEventListener('click', (e) => { e.preventDefault(); this.openDictionary(a.dataset.id); });
        });
        tbody.querySelectorAll('[data-action="delete"]').forEach(btn => {
            btn.addEventListener('click', () => this.deleteDictionaryById(btn.dataset.id));
        });
    },

    updatePagination() {
        const total = this.filteredDictionaries.length;
        const totalPages = Math.max(1, Math.ceil(total / this.pageSize));
        const info = document.getElementById('dictsPaginationInfo');
        if (info) info.textContent = `Page ${this.currentPage + 1} of ${totalPages}`;
        const prevBtn = document.getElementById('dictsPrevBtn');
        const nextBtn = document.getElementById('dictsNextBtn');
        if (prevBtn) prevBtn.disabled = this.currentPage === 0;
        if (nextBtn) nextBtn.disabled = this.currentPage >= totalPages - 1;

        const paginationContainer = prevBtn?.parentElement;
        if (paginationContainer) {
            paginationContainer.style.display = totalPages <= 1 ? 'none' : '';
        }
    },

    showCreateForm() {
        const modal = document.getElementById('createDictModal');
        if (!modal) return;
        const nameInput = document.getElementById('dictNewName');
        const descInput = document.getElementById('dictNewDesc');
        const globalCb = document.getElementById('dictNewGlobal');
        const err = document.getElementById('dictCreateError');
        if (nameInput) nameInput.value = '';
        if (descInput) descInput.value = '';
        if (globalCb) globalCb.checked = false;
        const kind = document.getElementById('dictNewKind');
        if (kind) { kind.value = 'value'; this.updateKindHint(); }
        // Only an administrator may point the server at a URL.
        const sourceGroup = document.getElementById('dictNewSourceGroup');
        if (sourceGroup) sourceGroup.style.display = this.isAdmin() ? '' : 'none';
        const mode = document.getElementById('dictNewSourceMode');
        if (mode) mode.value = 'manual';
        const fields = document.getElementById('dictNewSourceFields');
        if (fields) fields.innerHTML = this.sourceFieldsHTML(null, 'dictNewSrc');
        this.updateNewSourceFields();
        if (err) err.style.display = 'none';
        modal.style.display = 'flex';
        setTimeout(() => nameInput?.focus(), 100);
    },

    // KINDS is the one place a list's kind is described. The label is what the
    // listing and the editor show; the hint is what the create form explains.
    KINDS: {
        value: { label: 'Values', hint: 'Keys are matched byte for byte. Use this for names, hashes and indicators.' },
        network: { label: 'Networks', hint: 'Keys are CIDR ranges. A lookup matches an address to the narrowest range holding it.' },
        pattern: { label: 'Patterns', hint: 'Keys are regular expressions. A lookup returns the first one that matches, so row order decides which wins.' },
    },

    // Order is part of what a Patterns list means: the first expression that
    // matches is the one that answers. Say so where the rows are edited.
    kindNote(d) {
        // A list filled from a URL keeps the file's order, which cannot be edited.
        return this.kindOf(d) === 'pattern' && !d.source
            ? 'Patterns are tried from the top. The first one that matches is the one that answers, so put the specific ones above the general ones.'
            : '';
    },

    kindOf(d) {
        const k = (d && d.kind) || 'value';
        return this.KINDS[k] ? k : 'value';
    },

    // The key column is the only one a Networks or Patterns list matches with its
    // own semantics: every other key column is a plain exact-match dictionary, so
    // the header says which column the ranges or expressions live in.
    KEY_BADGE: {
        value: { label: 'key', title: 'The lookup key. match(column=...) probes this column.' },
        network: { label: 'CIDR key', title: 'The only column matched as a CIDR range. Another column made a key matches exact values, not ranges.' },
        pattern: { label: 'regex key', title: 'The only column matched as a regular expression. Another column made a key matches exact values, not expressions.' },
    },

    keyBadge(d) {
        return this.KEY_BADGE[this.kindOf(d)] || this.KEY_BADGE.value;
    },

    // What promoting another column buys on this list. On a Networks or Patterns
    // list it is an exact-match key, which is not what the list's kind implies.
    secondaryKeyHint(d) {
        return this.kindOf(d) === 'value'
            ? 'Use as lookup key'
            : 'Use as an exact-match lookup key';
    },

    // Who can see a list. A global owned by another fractal shows up in this
    // fractal's listing, so reach is worth a column of its own.
    scopeLabel(d) {
        if (d && d.is_global) return 'Global';
        return d && d.prism_id ? 'Prism' : 'Fractal';
    },

    isAdmin() {
        return !!window.Auth?.currentUser?.is_admin;
    },

    REFRESH_CHOICES: [[15, '15 minutes'], [60, 'hour'], [360, '6 hours'], [1440, 'day'], [10080, 'week']],

    refreshLabel(minutes) {
        const hit = this.REFRESH_CHOICES.find(([m]) => m === minutes);
        return hit ? hit[1] : `${minutes} minutes`;
    },

    // The fields describing a source, shared by the create form and the source
    // editor. prefix keeps their ids unique; read back with readSourceFields.
    sourceFieldsHTML(src, prefix) {
        src = src || {};
        const refresh = src.refresh_minutes || 60;
        const choices = this.REFRESH_CHOICES.some(([m]) => m === refresh)
            ? this.REFRESH_CHOICES : [...this.REFRESH_CHOICES, [refresh, `${refresh} minutes`]];
        const formats = [['', 'Detect'], ['csv', 'CSV'], ['tsv', 'TSV'], ['json', 'JSON array'], ['ndjson', 'NDJSON'], ['lines', 'One value per line']];
        const id = (k) => `${prefix}-${k}`;
        const text = (k, label, value, placeholder, type = 'text') => `
    <div class="form-group">
        <label for="${id(k)}">${label}</label>
        <input type="${type}" id="${id(k)}" class="form-input" placeholder="${placeholder}" value="${this.esc(value)}" spellcheck="false">
    </div>`;
        return `${text('url', 'URL', src.url, 'https://example.com/list.csv', 'url')}
<div class="dict-source-grid">
    ${text('key_field', 'Key column', src.key_field, 'Column holding the key')}
    <div class="form-group">
        <label for="${id('format')}">Format</label>
        <select id="${id('format')}" class="form-input">${formats.map(([v, l]) => `<option value="${v}"${(src.format || '') === v ? ' selected' : ''}>${l}</option>`).join('')}</select>
    </div>
    ${text('columns', 'Other columns to keep', (src.columns || []).join(', '), 'All of them')}
    ${text('split_key', 'Split key cells on', src.split_key, "Don't split")}
</div>
<div class="dict-source-grid">
    <div class="form-group">
        <label for="${id('refresh')}">Refresh every</label>
        <select id="${id('refresh')}" class="form-input">${choices.map(([m, l]) => `<option value="${m}"${m === refresh ? ' selected' : ''}>${l}</option>`).join('')}</select>
    </div>
    <div class="form-group dict-source-glob">
        <label class="dict-global-label"><input type="checkbox" id="${id('glob')}"${src.key_format === 'glob' ? ' checked' : ''}> Keys are globs
            <span class="form-hint">* and ? wildcards, matched ignoring case</span></label>
    </div>
</div>
<p class="form-hint dict-source-hint">The server fetches the file on this schedule and replaces every row with what it holds. Rows can't be edited by hand.</p>`;
    },

    readSourceFields(prefix, kind) {
        const val = (k) => document.getElementById(`${prefix}-${k}`);
        return {
            url: (val('url')?.value || '').trim(),
            key_field: (val('key_field')?.value || '').trim(),
            format: val('format')?.value || '',
            columns: (val('columns')?.value || '').split(',').map(c => c.trim()).filter(Boolean),
            split_key: val('split_key')?.value || '',
            refresh_minutes: parseInt(val('refresh')?.value || '60', 10),
            key_format: kind === 'pattern' && val('glob')?.checked ? 'glob' : '',
        };
    },

    // Glob keys only mean something on a Patterns list.
    showGlobFor(root, kind) {
        const glob = root?.querySelector('.dict-source-glob');
        if (glob) glob.style.visibility = kind === 'pattern' ? '' : 'hidden';
    },

    // A load that outlives the request answers 202 and goes on in the background.
    sourceToast(resp, data, done) {
        if (resp.status === 202) this.showToast(data.message || 'Still loading', 'info');
        else this.showToast(done, 'success');
    },

    updateNewSourceFields() {
        const fields = document.getElementById('dictNewSourceFields');
        if (!fields) return;
        const url = this.isAdmin() && document.getElementById('dictNewSourceMode')?.value === 'url';
        fields.style.display = url ? '' : 'none';
        this.showGlobFor(fields, document.getElementById('dictNewKind')?.value || 'value');
        const save = document.getElementById('dictCreateSaveBtn');
        if (save) save.textContent = url ? 'Create and fetch' : 'Create';
    },

    updateKindHint() {
        const sel = document.getElementById('dictNewKind');
        const hint = document.getElementById('dictNewKindHint');
        if (!sel || !hint) return;
        hint.textContent = (this.KINDS[sel.value] || this.KINDS.value).hint;
    },

    hideCreateForm() {
        const modal = document.getElementById('createDictModal');
        if (modal) modal.style.display = 'none';
    },

    async saveCreateDictionary() {
        const name = (document.getElementById('dictNewName')?.value || '').trim();
        const desc = (document.getElementById('dictNewDesc')?.value || '').trim();
        const isGlobal = document.getElementById('dictNewGlobal')?.checked || false;
        const kind = document.getElementById('dictNewKind')?.value || 'value';
        const err = document.getElementById('dictCreateError');

        if (!name) {
            if (err) { err.textContent = 'Name is required.'; err.style.display = 'block'; }
            document.getElementById('dictNewName')?.focus();
            return;
        }
        if (err) err.style.display = 'none';

        const body = { name, description: desc, columns: [], is_global: isGlobal, kind };
        const fields = document.getElementById('dictNewSourceFields');
        if (fields && fields.style.display !== 'none') body.source = this.readSourceFields('dictNewSrc', kind);
        const save = document.getElementById('dictCreateSaveBtn');
        const label = save?.textContent;
        if (save && body.source) { save.disabled = true; save.textContent = 'Fetching...'; }

        try {
            const resp = await fetch('/api/v1/dictionaries', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.hideCreateForm();
            this.sourceToast(resp, data, 'Lookup created');
            this.allDictionaries.unshift(data.data);
            this.filterDictionaries(document.getElementById('dictSearchInput')?.value || '');
            this.openDictionary(data.data.id);
        } catch (e) {
            if (err) { err.textContent = e.message; err.style.display = 'block'; }
        } finally {
            if (save) { save.disabled = false; save.textContent = label; }
        }
    },

    // ---- Detail view ----

    async openDictionary(id) {
        try {
            window.App?.pushSubPath(id);
            const resp = await fetch(`/api/v1/dictionaries/${id}`, { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.rowPage = 0;
            this.rowSearch = '';
            this._csvFile = null;
            this.showDetailView();
        } catch (e) {
            this.showToast('Failed to load lookup: ' + e.message, 'error');
        }
    },

    showDetailView() {
        const d = this.currentDictionary;
        if (!d) return;

        document.getElementById('dictListing').style.display = 'none';
        const detail = document.getElementById('dictDetailView');
        if (!detail) return;
        detail.style.display = 'block';

        detail.innerHTML = `
<div class="dict-detail-header">
    <div class="dict-detail-header-left">
        <div class="dict-editable-block">
            <div id="dictDetailName" class="dict-editable-title" contenteditable="true" spellcheck="false"></div>
            <div id="dictDetailDesc" class="dict-editable-desc" contenteditable="true" spellcheck="false" data-placeholder="Add description..."></div>
        </div>
    </div>
    <div class="dict-detail-header-right">
        ${d.source
            ? '<button id="dictSyncBtn" class="btn-secondary" title="Fetch the source now">Refresh now</button>'
            : `<button id="dictImportBtn" class="btn-secondary">Import CSV</button>${this.isAdmin() ? '<button id="dictSourceAttachBtn" class="btn-secondary" title="Replace the rows with a file fetched from a URL, kept up to date">Fetch from URL</button>' : ''}`}
        <button id="dictExportBtn" class="btn-secondary">Export CSV</button>
        ${d.source ? '' : '<button id="dictReloadBtn" class="btn-secondary" title="Reload ClickHouse dictionary">Reload</button>'}
        <button id="dictDeleteBtn" class="btn-secondary" style="color:var(--error);">Delete</button>
    </div>
</div>

<div class="dict-meta-bar">
    <span class="dict-meta-pill" id="dictMetaRows"></span>
    <label class="dict-meta-global"><input type="checkbox" id="dictGlobalToggle"${d.is_global ? ' checked' : ''}> Global</label>
    ${this.kindOf(d) === 'value'
        ? `<label class="dict-meta-global" title="Lookups match the key whatever its casing"><input type="checkbox" id="dictCaseToggle"${d.case_insensitive_keys ? ' checked' : ''}> Ignore case</label>`
        : `<span class="dict-meta-pill" title="${this.esc(this.KINDS[this.kindOf(d)].hint)}">${this.esc(this.KINDS[this.kindOf(d)].label)}</span>`}
    <span class="dict-meta-syntax" id="dictMetaSyntax"></span>
</div>
${this.kindNote(d) ? `<p class="form-hint dict-kind-note">${this.esc(this.kindNote(d))}</p>` : ''}
${d.source ? `<div class="dict-source-bar">${this.sourceBarHTML(d)}</div>` : ''}

<div id="dictSourcePanel" class="dict-import-panel" style="display:none;">
    <div class="dict-import-inner">
        <div id="dictSourcePanelFields"></div>
        <div id="dictSourceError" class="form-error" style="display:none;"></div>
        <div class="dict-import-actions">
            <button id="dictSourceCancelBtn" class="btn-secondary btn-sm">Cancel</button>
            <button id="dictSourceSaveBtn" class="btn-primary btn-sm">Save and fetch</button>
        </div>
    </div>
</div>

<div id="dictImportPanel" class="dict-import-panel" style="display:none;">
    <div class="dict-import-inner">
        <p class="form-hint" style="margin:0 0 10px;">First row must be column headers. New columns are auto-created. Rows with an empty key column are skipped.</p>
        <div class="dict-drop-zone" id="csvDropZone">
            <span>Drag &amp; drop CSV here, or</span>
            <label class="btn-secondary btn-sm dict-file-label">Browse<input type="file" id="csvFileInput" accept=".csv,text/csv" style="display:none;"></label>
        </div>
        <div id="csvFileInfo" class="dict-file-info" style="display:none;"></div>
        <div id="importError" class="form-error" style="display:none;"></div>
        <div class="dict-import-actions">
            <button id="cancelImportBtn" class="btn-secondary btn-sm">Cancel</button>
            <button id="doImportBtn" class="btn-primary btn-sm">Import</button>
        </div>
    </div>
</div>

<div class="dict-data-controls">
    <input type="text" id="dictRowSearch" class="dict-search" placeholder="Search rows...">
</div>
<div id="dictDataTable" class="dict-data-table-wrap"></div>
<div class="dict-pagination">
    <button id="dictRowPrevBtn" class="btn-secondary">Previous</button>
    <span id="dictRowPageInfo" class="page-info"></span>
    <button id="dictRowNextBtn" class="btn-secondary">Next</button>
</div>`;

        document.getElementById('dictDetailName').textContent = d.name;
        document.getElementById('dictDetailDesc').textContent = d.description || '';
        document.getElementById('dictMetaRows').textContent = (d.row_count || 0).toLocaleString() + ' rows';
        const probeField = { network: 'src_ip', pattern: 'commandline' }[this.kindOf(d)] || '…';
        document.getElementById('dictMetaSyntax').textContent = `match(dict="${d.name}", field=${probeField}, column=${d.key_column}, include=[…])`;

        this._bindDetailEvents();
        this.loadRows();
    },

    _bindDetailEvents() {
        document.getElementById('dictRowSearch')?.addEventListener('input', (e) => {
            this.rowSearch = e.target.value;
            this.rowPage = 0;
            clearTimeout(this._rowSearchTimer);
            this._rowSearchTimer = setTimeout(() => this.loadRows(), 200);
        });
        document.getElementById('dictRowPrevBtn')?.addEventListener('click', () => {
            if (this.rowPage > 0) { this.rowPage--; this.loadRows(); }
        });
        document.getElementById('dictRowNextBtn')?.addEventListener('click', () => {
            const max = Math.max(0, Math.ceil(this.totalRows / this.rowPageSize) - 1);
            if (this.rowPage < max) { this.rowPage++; this.loadRows(); }
        });
        document.getElementById('dictImportBtn')?.addEventListener('click', () => this.toggleImportPanel());
        document.getElementById('dictSyncBtn')?.addEventListener('click', () => this.syncSource());
        document.getElementById('dictSourceAttachBtn')?.addEventListener('click', () => this.toggleSourcePanel());
        document.getElementById('dictSourceEditBtn')?.addEventListener('click', () => this.toggleSourcePanel());
        document.getElementById('dictSourceDetachBtn')?.addEventListener('click', () => this.detachSource());
        document.getElementById('dictSourceCancelBtn')?.addEventListener('click', () => this.hideSourcePanel());
        document.getElementById('dictSourceSaveBtn')?.addEventListener('click', () => this.saveSource());
        document.getElementById('dictExportBtn')?.addEventListener('click', () => this.doExport());
        document.getElementById('cancelImportBtn')?.addEventListener('click', () => this.hideImportPanel());
        document.getElementById('doImportBtn')?.addEventListener('click', () => this.doImport());
        document.getElementById('dictReloadBtn')?.addEventListener('click', () => this.reloadDictionary());
        document.getElementById('dictDeleteBtn')?.addEventListener('click', () => this.deleteDictionary());
        document.getElementById('dictGlobalToggle')?.addEventListener('change', () => this._scheduleSaveMeta());
        document.getElementById('dictCaseToggle')?.addEventListener('change', (e) => this._setCaseInsensitive(e.target.checked));

        const dropZone = document.getElementById('csvDropZone');
        const fileInput = document.getElementById('csvFileInput');
        if (dropZone && fileInput) {
            dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('drag-over'); });
            dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
            dropZone.addEventListener('drop', (e) => {
                e.preventDefault();
                dropZone.classList.remove('drag-over');
                if (e.dataTransfer.files[0]) this.setCSVFile(e.dataTransfer.files[0]);
            });
            fileInput.addEventListener('change', () => { if (fileInput.files[0]) this.setCSVFile(fileInput.files[0]); });
        }

        const nameEl = document.getElementById('dictDetailName');
        if (nameEl) {
            nameEl.addEventListener('blur', () => this._scheduleSaveMeta());
            nameEl.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') { e.preventDefault(); nameEl.blur(); }
                if (e.key === 'Escape') { if (this.currentDictionary) nameEl.textContent = this.currentDictionary.name; nameEl.blur(); }
            });
        }
        const descEl = document.getElementById('dictDetailDesc');
        if (descEl) {
            descEl.addEventListener('blur', () => this._scheduleSaveMeta());
            descEl.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') { e.preventDefault(); descEl.blur(); }
                if (e.key === 'Escape') { if (this.currentDictionary) descEl.textContent = this.currentDictionary.description || ''; descEl.blur(); }
            });
        }
    },

    // ---- Row management ----

    async loadRows() {
        const d = this.currentDictionary;
        if (!d) return;

        // Responses can land out of order (typing in the row search, an edit saved
        // while a page load is in flight). Only the newest request may paint.
        const req = (this._rowsReq = (this._rowsReq || 0) + 1);

        const params = new URLSearchParams({ limit: this.rowPageSize, offset: this.rowPage * this.rowPageSize });
        if (this.rowSearch) params.set('search', this.rowSearch);

        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/data?${params}`, { credentials: 'include' });
            const result = await resp.json();
            if (req !== this._rowsReq) return;
            if (!result.success) throw new Error(result.error);
            this.totalRows = result.page?.total || 0;

            // Deleting the last row of the last page leaves the view past the end.
            const lastPage = Math.max(0, Math.ceil(this.totalRows / this.rowPageSize) - 1);
            if (this.rowPage > lastPage) {
                this.rowPage = lastPage;
                return this.loadRows();
            }

            this.renderRowTable(d, result.data || []);
            this.updateRowPagination();
        } catch (e) {
            if (req !== this._rowsReq) return;
            const wrap = document.getElementById('dictDataTable');
            if (wrap) wrap.innerHTML = `<div class="dict-error">Failed to load rows: ${this.esc(e.message)}</div>`;
        }
    },

    renderRowTable(dict, rows) {
        const wrap = document.getElementById('dictDataTable');
        if (!wrap) return;
        const cols = dict.columns || [];
        // A list filled from a URL is replaced on every refresh, so hand edits
        // are not offered. Promoting a column to a lookup key still is.
        const readOnly = !!dict.source;

        if (readOnly && cols.length === 0) {
            wrap.innerHTML = '<table class="dict-table dict-sheet"><tbody><tr><td class="dict-empty-cell">No rows yet.</td></tr></tbody></table>';
            return;
        }

        // Empty sheet - no columns yet
        if (cols.length === 0) {
            wrap.innerHTML = `<table class="dict-table dict-sheet dict-sheet-empty">
                <thead><tr>
                    <th class="dict-th dict-th-num"></th>
                    <th class="dict-th dict-th-add-col"><button class="dict-add-col-btn" title="Add column">+</button></th>
                </tr></thead>
                <tbody>
                    <tr class="dict-add-row-tr">
                        <td class="dict-td dict-td-num"><button class="dict-add-row-btn" title="Add row">+</button></td>
                        <td class="dict-td dict-td-add-col-spacer"></td>
                    </tr>
                </tbody>
            </table>`;
            wrap.querySelector('.dict-add-col-btn')?.addEventListener('click', (e) => {
                this._startAddColumnInHeader(e.currentTarget.closest('th'));
            });
            wrap.querySelector('.dict-add-row-btn')?.addEventListener('click', () => {
                this.showToast('Add columns first', 'error');
            });
            return;
        }

        const colHeaders = cols.map(c => {
            const isPrimary = c.name === dict.key_column;
            const isSecondary = c.is_key && !isPrimary;
            let thClass = 'dict-th dict-th-col';
            if (isPrimary) thClass += ' dict-th-col-key';
            else if (isSecondary) thClass += ' dict-th-col-key2';

            const keyBadge = this.keyBadge(dict);
            let inner = `<span class="dict-col-name">${this.esc(c.name)}</span>`;
            if (isPrimary) {
                inner += `<span class="dict-key-badge dict-key-badge-hdr" title="${this.esc(keyBadge.title)}">${this.esc(keyBadge.label)}</span>`;
            } else if (isSecondary) {
                inner += `<button class="dict-col-key-active-btn" data-col="${this.esc(c.name)}" title="Remove as lookup key">key</button>`;
            } else {
                inner += `<button class="dict-col-key-btn" data-col="${this.esc(c.name)}" title="${this.esc(this.secondaryKeyHint(dict))}">key</button>`;
            }
            if (!isPrimary && !readOnly) {
                inner += `<button class="dict-col-del-btn" data-col="${this.esc(c.name)}" title="Remove column">&times;</button>`;
            }
            return `<th class="${thClass}" data-col="${this.esc(c.name)}">${inner}</th>`;
        }).join('');

        const bodyRows = rows.map((row, i) => {
            const num = this.rowPage * this.rowPageSize + i + 1;
            const cells = cols.map(c => {
                const val = row.fields ? (row.fields[c.name] || '') : '';
                return `<td class="dict-td${readOnly ? '' : ' dict-td-editable'}" data-col="${this.esc(c.name)}" data-key="${this.esc(row.key)}">${this.esc(val)}</td>`;
            }).join('');
            return `<tr class="dict-row" data-key="${this.esc(row.key)}">
                <td class="dict-td dict-td-num">
                    <span class="dict-row-num">${num}</span>
                    ${readOnly ? '' : `<button class="dict-delete-row" data-key="${this.esc(row.key)}" title="Delete row">&times;</button>`}
                </td>
                ${cells}
                <td class="dict-td dict-td-add-col-spacer"></td>
            </tr>`;
        }).join('');

        const emptyRow = rows.length === 0
            ? `<tr><td colspan="${cols.length + 2}" class="dict-empty-cell">No rows found.</td></tr>`
            : '';

        wrap.innerHTML = `<table class="dict-table dict-sheet">
            <thead><tr>
                <th class="dict-th dict-th-num"></th>
                ${colHeaders}
                <th class="dict-th dict-th-add-col">${readOnly ? '' : '<button class="dict-add-col-btn" title="Add column">+</button>'}</th>
            </tr></thead>
            <tbody id="dictRowsBody">
                ${bodyRows}${emptyRow}
                ${readOnly ? '' : `<tr class="dict-add-row-tr">
                    <td class="dict-td dict-td-num"><button class="dict-add-row-btn" title="Add row">+</button></td>
                    <td colspan="${cols.length + 1}" class="dict-td dict-td-add-row-spacer"></td>
                </tr>`}
            </tbody>
        </table>`;

        wrap.querySelector('.dict-add-col-btn')?.addEventListener('click', (e) => {
            this._startAddColumnInHeader(e.currentTarget.closest('th'));
        });
        wrap.querySelector('.dict-add-row-btn')?.addEventListener('click', () => this.addNewRowInline());
        wrap.querySelectorAll('.dict-col-del-btn').forEach(btn => {
            btn.addEventListener('click', (e) => { e.stopPropagation(); this.removeColumn(btn.dataset.col); });
        });
        wrap.querySelectorAll('.dict-col-key-btn').forEach(btn => {
            btn.addEventListener('click', (e) => { e.stopPropagation(); this.setColumnKey(btn.dataset.col); });
        });
        wrap.querySelectorAll('.dict-col-key-active-btn').forEach(btn => {
            btn.addEventListener('click', (e) => { e.stopPropagation(); this.unsetColumnKey(btn.dataset.col); });
        });
        wrap.querySelectorAll('.dict-td-editable').forEach(cell => {
            cell.addEventListener('dblclick', () => this.startCellEdit(cell, dict));
        });
        wrap.querySelectorAll('.dict-delete-row').forEach(btn => {
            btn.addEventListener('click', () => this.deleteRow(btn.dataset.key));
        });
    },

    // Inline add column - replaces the + button in the th with an input
    _startAddColumnInHeader(th) {
        th.innerHTML = '';
        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'dict-col-header-input';
        input.placeholder = 'Column name';
        th.appendChild(input);
        input.focus();

        const cancel = () => {
            th.innerHTML = '<button class="dict-add-col-btn" title="Add column">+</button>';
            th.querySelector('.dict-add-col-btn').addEventListener('click', () => this._startAddColumnInHeader(th));
        };

        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); input.blur(); }
            if (e.key === 'Escape') { cancel(); }
        });
        input.addEventListener('blur', async () => {
            const name = input.value.trim();
            if (!name) { cancel(); return; }
            await this.saveAddColumn(name);
        });
    },

    addNewRowInline() {
        const d = this.currentDictionary;
        if (!d) return;
        const cols = d.columns || [];
        if (cols.length === 0) { this.showToast('Add columns first', 'error'); return; }

        const existing = document.getElementById('dictNewRow');
        if (existing) existing.remove();

        const addRowTr = document.querySelector('.dict-add-row-tr');
        if (!addRowTr) return;

        const tr = document.createElement('tr');
        tr.id = 'dictNewRow';
        tr.className = 'dict-row dict-new-row';

        // Row # cell
        const numTd = document.createElement('td');
        numTd.className = 'dict-td dict-td-num';
        numTd.innerHTML = '<span class="dict-row-num" style="color:var(--accent-primary);">•</span>';
        tr.appendChild(numTd);

        const inputEls = [];
        cols.forEach(c => {
            const td = document.createElement('td');
            td.className = 'dict-td';
            const input = document.createElement('input');
            input.type = 'text';
            input.className = 'dict-cell-input';
            input.dataset.col = c.name;
            input.placeholder = c.name === d.key_column ? 'key (required)' : '';
            td.appendChild(input);
            inputEls.push(input);
            tr.appendChild(td);
        });

        // Spacer
        const spacerTd = document.createElement('td');
        spacerTd.className = 'dict-td dict-td-add-col-spacer';
        tr.appendChild(spacerTd);

        addRowTr.parentNode.insertBefore(tr, addRowTr);
        if (inputEls[0]) inputEls[0].focus();

        inputEls.forEach((input, i) => {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Tab') {
                    e.preventDefault();
                    if (inputEls[i + 1]) inputEls[i + 1].focus();
                    else this._saveNewRow(tr, d, inputEls); // Tab past last field saves
                }
                if (e.key === 'Enter') this._saveNewRow(tr, d, inputEls);
                if (e.key === 'Escape') tr.remove();
            });
        });

        // Clicking away commits what was typed, or discards an untouched draft.
        // Without this the draft row stays on screen looking like a saved row.
        tr.addEventListener('focusout', () => {
            setTimeout(() => {
                if (!tr.isConnected || tr.contains(document.activeElement)) return;
                if (inputEls.every(i => !i.value.trim())) { tr.remove(); return; }
                this._saveNewRow(tr, d, inputEls);
            }, 0);
        });
    },

    async _saveNewRow(tr, dict, inputEls) {
        const fields = {};
        let key = '';
        inputEls.forEach(input => {
            fields[input.dataset.col] = input.value;
            if (input.dataset.col === dict.key_column) key = input.value;
        });

        if (!key) {
            const keyInput = inputEls.find(i => i.dataset.col === dict.key_column);
            if (keyInput) keyInput.focus();
            this.showToast('Key column is required', 'error');
            return;
        }

        try {
            const resp = await fetch(`/api/v1/dictionaries/${dict.id}/data`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ rows: [{ key, fields }] }),
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            tr.remove();
            // New rows are appended, so follow it to the page it landed on.
            this.rowPage = Math.floor(this.totalRows / this.rowPageSize);
            this.loadRows();
        } catch (e) {
            this.showToast('Failed to save row: ' + e.message, 'error');
        }
    },

    startCellEdit(cell, dict) {
        if (cell.querySelector('input')) return;
        const col = cell.dataset.col;
        const key = cell.dataset.key;
        const current = cell.textContent;

        cell.innerHTML = '';
        const input = document.createElement('input');
        input.type = 'text';
        input.value = current;
        input.className = 'dict-cell-input';
        cell.appendChild(input);
        input.focus();
        input.select();

        const commit = async () => {
            const newVal = input.value;
            cell.textContent = newVal;
            if (newVal === current) return;

            const renamedKey = col === dict.key_column;
            if (renamedKey && !newVal.trim()) {
                cell.textContent = current;
                this.showToast('Key column cannot be empty', 'error');
                return;
            }

            // key is the row's identity before this edit: the server treats a changed
            // key column as a rename rather than leaving a second copy behind.
            const row = { key, fields: {} };
            cell.closest('tr').querySelectorAll('.dict-td-editable').forEach(c => {
                row.fields[c.dataset.col] = c.textContent;
            });
            row.fields[col] = newVal;

            try {
                const resp = await fetch(`/api/v1/dictionaries/${dict.id}/data`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ rows: [row] }),
                });
                const data = await resp.json();
                if (!data.success) throw new Error(data.error || 'save failed');
                // A rename changes the row's identity, so the table has to be re-read.
                // Any other edit is already on screen; reloading would only fight the
                // cursor as it tabs along the row.
                if (renamedKey) this.loadRows();
            } catch (e) {
                cell.textContent = current;
                this.showToast('Failed to save cell: ' + e.message, 'error');
            }
        };

        input.addEventListener('blur', commit);
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); input.blur(); }
            if (e.key === 'Escape') { cell.textContent = current; }
            if (e.key === 'Tab') {
                e.preventDefault();
                input.blur();
                const cells = Array.from(cell.closest('tr').querySelectorAll('.dict-td-editable'));
                const next = cells[cells.indexOf(cell) + 1];
                if (next) setTimeout(() => next.dispatchEvent(new MouseEvent('dblclick')), 0);
            }
        });
    },

    async deleteRow(key) {
        const d = this.currentDictionary;
        if (!d || !key) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/data?key=${encodeURIComponent(key)}`, {
                method: 'DELETE', credentials: 'include',
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            document.querySelector(`tr.dict-row[data-key="${CSS.escape(key)}"]`)?.remove();
            this.loadRows();
        } catch (e) {
            this.showToast('Failed to delete row: ' + e.message, 'error');
        }
    },

    updateRowPagination() {
        const info = document.getElementById('dictRowPageInfo');
        const prev = document.getElementById('dictRowPrevBtn');
        const next = document.getElementById('dictRowNextBtn');
        if (!info) return;
        const totalPages = Math.max(1, Math.ceil(this.totalRows / this.rowPageSize));
        info.textContent = `Page ${this.rowPage + 1} of ${totalPages} (${this.totalRows.toLocaleString()} rows)`;
        const meta = document.getElementById('dictMetaRows');
        if (meta && !this.rowSearch) {
            meta.textContent = this.totalRows.toLocaleString() + ' rows';
            if (this.currentDictionary) this.currentDictionary.row_count = this.totalRows;
        }
        if (prev) prev.disabled = this.rowPage === 0;
        if (next) next.disabled = this.rowPage >= totalPages - 1;
    },

    // ---- Column management ----

    async saveAddColumn(name) {
        const d = this.currentDictionary;
        if (!d || !name) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/columns`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name }),
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.loadRows();
        } catch (e) {
            this.showToast('Failed to add column: ' + e.message, 'error');
        }
    },

    async removeColumn(colName) {
        const d = this.currentDictionary;
        if (!d) return;
        if (!confirm(`Remove column "${colName}"? All data in this column will be lost.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/columns/${encodeURIComponent(colName)}`, {
                method: 'DELETE', credentials: 'include',
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.showToast('Column removed', 'success');
            this.loadRows();
        } catch (e) {
            this.showToast('Failed to remove column: ' + e.message, 'error');
        }
    },

    async setColumnKey(colName) {
        const d = this.currentDictionary;
        if (!d) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/columns/${encodeURIComponent(colName)}/key`, {
                method: 'POST', credentials: 'include',
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.showToast(`"${colName}" enabled as lookup key`, 'success');
            this.loadRows();
        } catch (e) {
            this.showToast('Failed to set key: ' + e.message, 'error');
        }
    },

    async unsetColumnKey(colName) {
        const d = this.currentDictionary;
        if (!d) return;
        if (!confirm(`Remove "${colName}" as a lookup key? The column data is kept, but match(column=${colName}) will no longer work.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/columns/${encodeURIComponent(colName)}/key`, {
                method: 'DELETE', credentials: 'include',
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.showToast(`"${colName}" removed as lookup key`, 'success');
            this.loadRows();
        } catch (e) {
            this.showToast('Failed to unset key: ' + e.message, 'error');
        }
    },

    // ---- Inline meta editing ----

    _scheduleSaveMeta() {
        clearTimeout(this._metaSaveTimer);
        this._metaSaveTimer = setTimeout(() => this._saveMeta(), 200);
    },

    async _saveMeta() {
        const d = this.currentDictionary;
        if (!d) return;
        const name = document.getElementById('dictDetailName')?.textContent.trim() || d.name;
        const desc = document.getElementById('dictDetailDesc')?.textContent.trim() || (d.description || '');
        const isGlobal = document.getElementById('dictGlobalToggle')?.checked || false;
        if (!name) { if (document.getElementById('dictDetailName')) document.getElementById('dictDetailName').textContent = d.name; return; }
        if (name === d.name && desc === (d.description || '') && isGlobal === !!d.is_global) return;

        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, description: desc, is_global: isGlobal }),
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            const metaSyntax = document.getElementById('dictMetaSyntax');
            if (metaSyntax) metaSyntax.textContent = `match(dict="${data.data.name}", field=…, column=${data.data.key_column}, include=[…])`;
            this.showToast('Saved', 'success');
        } catch (e) {
            this.showToast('Failed to save: ' + e.message, 'error');
        }
    },

    // Case-insensitivity rebuilds the ClickHouse dictionary objects over lower(key),
    // so it is its own endpoint rather than part of the debounced metadata save.
    // Turning it on where keys collide is refused once and confirmed explicitly:
    // only one row per collapsed key stays reachable.
    async _setCaseInsensitive(enabled, confirmCollisions = false) {
        const d = this.currentDictionary;
        const toggle = document.getElementById('dictCaseToggle');
        if (!d) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/case-insensitive`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ enabled, confirm_collisions: confirmCollisions }),
            });
            const data = await resp.json();
            if (!data.success) {
                // Only the collision refusal is retryable, and only with the user's
                // consent. Any other 400 (no key column, say) is a real error: keying
                // on the status alone offered to confirm "0 keys" and then failed again.
                if (enabled && !confirmCollisions && resp.status === 400) {
                    const report = await this._keyCollisions(d);
                    if (report && report.collisions > 0) {
                        if (await this._confirmKeyCollisions(report)) return this._setCaseInsensitive(true, true);
                        if (toggle) toggle.checked = false;
                        return;
                    }
                }
                throw new Error(data.error);
            }
            this.currentDictionary = data.data;
            this.showToast(enabled ? 'Lookups now ignore case' : 'Lookups now match case exactly', 'success');
        } catch (e) {
            if (toggle) toggle.checked = !enabled;
            this.showToast('Failed to change case sensitivity: ' + e.message, 'error');
        }
    },

    async _keyCollisions(d) {
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/key-collisions`, { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            return data.data;
        } catch (e) {
            this.showToast('Failed to check key collisions: ' + e.message, 'error');
            return null;
        }
    },

    async _confirmKeyCollisions(report) {
        const examples = (report.samples || []).slice(0, 5)
            .map(c => `  ${c.stored.join(' / ')}`).join('\n');
        return confirm(
            `${report.collisions} key${report.collisions === 1 ? '' : 's'} stop being distinct when case is ignored.\n\n` +
            `Only one row per key stays reachable, and which one is not defined.\n\n` +
            (examples ? `${examples}\n\n` : '') +
            `Ignore case anyway?`);
    },

    // ---- Source ----

    sourceBarHTML(d) {
        const src = d.source;
        const st = d.source_status || {};
        let shown = src.url;
        try { const u = new URL(src.url); shown = u.host + u.pathname; } catch (_) { /* shown as stored */ }
        const ago = (t) => t ? `<span title="${this.esc(Utils.formatTimestamp(t))}">${this.esc(Utils.timeAgo(t))}</span>` : 'never';
        const admin = this.isAdmin()
            ? '<button id="dictSourceEditBtn" class="dict-source-action">Edit source</button><button id="dictSourceDetachBtn" class="dict-source-action">Detach</button>'
            : '';
        let html = `<div class="dict-source-line">
            <span class="dict-source-label">From</span>
            <a class="dict-source-url" href="${this.esc(src.url)}" target="_blank" rel="noopener noreferrer" title="${this.esc(src.url)}">${this.esc(shown)}</a>
            <span class="dict-source-sep">·</span><span>every ${this.esc(this.refreshLabel(src.refresh_minutes))}</span>
            <span class="dict-source-sep">·</span><span>checked ${ago(st.checked_at)}</span>
            <span class="dict-source-sep">·</span><span>changed ${ago(st.changed_at)}</span>
            ${admin}
        </div>`;
        if (st.error) html += `<div class="dict-source-error">Last refresh failed: ${this.esc(st.error)}</div>`;
        if (st.skipped) html += `<div class="dict-source-note">${st.skipped.toLocaleString()} record${st.skipped === 1 ? '' : 's'} skipped, e.g. ${this.esc(st.skip_sample || '')}</div>`;
        return html;
    },

    toggleSourcePanel() {
        const panel = document.getElementById('dictSourcePanel');
        if (!panel) return;
        if (panel.style.display !== 'none') { this.hideSourcePanel(); return; }
        const d = this.currentDictionary;
        const fields = document.getElementById('dictSourcePanelFields');
        if (fields) {
            fields.innerHTML = this.sourceFieldsHTML(d.source, 'dictSrc');
            this.showGlobFor(fields, this.kindOf(d));
        }
        const err = document.getElementById('dictSourceError');
        if (err) err.style.display = 'none';
        this.hideImportPanel();
        panel.style.display = 'block';
        document.getElementById('dictSrc-url')?.focus();
    },

    hideSourcePanel() {
        const panel = document.getElementById('dictSourcePanel');
        if (panel) panel.style.display = 'none';
    },

    async saveSource() {
        const d = this.currentDictionary;
        if (!d) return;
        const source = this.readSourceFields('dictSrc', this.kindOf(d));
        // Attaching a source to a hand-built list replaces its rows for good.
        if (!d.source && (d.row_count || 0) > 0) {
            DangerConfirm.open({
                title: 'Replace rows from a URL',
                body: [`The ${(d.row_count || 0).toLocaleString()} rows in "${d.name}" are replaced by the file at this URL, and can't be edited by hand afterwards.`],
                phrase: d.name,
                confirmLabel: 'Replace rows',
                busyLabel: 'Fetching...',
                onConfirm: () => this._putSource(d, source),
            });
            return;
        }
        const err = document.getElementById('dictSourceError');
        const btn = document.getElementById('dictSourceSaveBtn');
        if (err) err.style.display = 'none';
        if (btn) { btn.disabled = true; btn.textContent = 'Fetching...'; }
        try {
            await this._putSource(d, source);
        } catch (e) {
            if (err) { err.textContent = e.message; err.style.display = 'block'; }
            if (btn) { btn.disabled = false; btn.textContent = 'Save and fetch'; }
        }
    },

    async _putSource(d, source) {
        const resp = await fetch(`/api/v1/dictionaries/${d.id}/source`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify(source),
        });
        const data = await resp.json();
        if (!data.success) throw new Error(data.error);
        this.currentDictionary = data.data;
        this.sourceToast(resp, data, 'Source saved');
        this.showDetailView();
    },

    async syncSource() {
        const d = this.currentDictionary;
        const btn = document.getElementById('dictSyncBtn');
        if (!d) return;
        if (btn) { btn.disabled = true; btn.textContent = 'Refreshing...'; }
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/sync`, { method: 'POST', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.sourceToast(resp, data, 'Refreshed');
            this.showDetailView();
        } catch (e) {
            this.showToast(e.message, 'error');
            // The failure is recorded on the list; show it where it lives.
            this.openDictionary(d.id);
        }
    },

    async detachSource() {
        const d = this.currentDictionary;
        if (!d || !confirm(`Stop refreshing "${d.name}" from its URL? The rows it holds stay and become editable.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/source`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.showToast('Source detached', 'success');
            this.showDetailView();
        } catch (e) {
            this.showToast('Failed to detach: ' + e.message, 'error');
        }
    },

    // ---- Delete / Reload ----

    async deleteDictionary() {
        const d = this.currentDictionary;
        if (!d) return;
        if (!confirm(`Delete "${d.name}"? This drops all data and cannot be undone.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.showToast('Lookup deleted', 'success');
            this.allDictionaries = this.allDictionaries.filter(x => x.id !== d.id);
            window.App?.pushSubPath('');
            this.showListing();
        } catch (e) {
            this.showToast('Failed to delete: ' + e.message, 'error');
        }
    },

    async deleteDictionaryById(id) {
        const dict = this.allDictionaries.find(d => d.id === id);
        if (!confirm(`Delete "${dict ? dict.name : 'this lookup'}"? This drops all data and cannot be undone.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${id}`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.showToast('Lookup deleted', 'success');
            this.allDictionaries = this.allDictionaries.filter(d => d.id !== id);
            this.filterDictionaries(document.getElementById('dictSearchInput')?.value || '');
        } catch (e) {
            this.showToast('Failed to delete: ' + e.message, 'error');
        }
    },

    async reloadDictionary() {
        const d = this.currentDictionary;
        if (!d) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/reload`, { method: 'POST', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.showToast('Lookup reloaded', 'success');
        } catch (e) {
            this.showToast('Reload failed: ' + e.message, 'error');
        }
    },

    // ---- CSV Import ----

    toggleImportPanel() {
        const panel = document.getElementById('dictImportPanel');
        if (!panel) return;
        if (panel.style.display === 'none') this.showImportPanel();
        else this.hideImportPanel();
    },

    showImportPanel() {
        this._csvFile = null;
        const info = document.getElementById('csvFileInfo');
        if (info) info.style.display = 'none';
        const fileInput = document.getElementById('csvFileInput');
        if (fileInput) fileInput.value = '';
        const err = document.getElementById('importError');
        if (err) err.style.display = 'none';
        const panel = document.getElementById('dictImportPanel');
        if (panel) panel.style.display = 'block';
    },

    hideImportPanel() {
        const panel = document.getElementById('dictImportPanel');
        if (panel) panel.style.display = 'none';
    },

    setCSVFile(file) {
        this._csvFile = file;
        const info = document.getElementById('csvFileInfo');
        if (info) { info.style.display = 'block'; info.textContent = `${file.name} (${(file.size / 1024).toFixed(1)} KB)`; }
    },

    async doImport() {
        const d = this.currentDictionary;
        if (!d) return;
        const err = document.getElementById('importError');
        if (!this._csvFile) {
            if (err) { err.textContent = 'Please select a CSV file.'; err.style.display = 'block'; }
            return;
        }
        if (err) err.style.display = 'none';

        const btn = document.getElementById('doImportBtn');
        const cancelBtn = document.getElementById('cancelImportBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Importing...'; }
        if (cancelBtn) cancelBtn.disabled = true;

        const formData = new FormData();
        formData.append('file', this._csvFile);

        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/import`, {
                method: 'POST', credentials: 'include', body: formData,
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            const count = data.data.imported || 0;
            this.hideImportPanel();
            this.showToast(`Imported ${count.toLocaleString()} rows`, 'success');

            const refreshResp = await fetch(`/api/v1/dictionaries/${d.id}`, { credentials: 'include' });
            const refreshData = await refreshResp.json();
            if (refreshData.success) {
                this.currentDictionary = refreshData.data;
                const metaRows = document.getElementById('dictMetaRows');
                if (metaRows) metaRows.textContent = (refreshData.data.row_count || 0).toLocaleString() + ' rows';
            }
            this.rowPage = 0;
            this.loadRows();
        } catch (e) {
            if (err) { err.textContent = e.message; err.style.display = 'block'; }
            if (btn) { btn.disabled = false; btn.textContent = 'Import'; }
            if (cancelBtn) cancelBtn.disabled = false;
        }
    },

    doExport() {
        const d = this.currentDictionary;
        if (!d) return;
        const a = document.createElement('a');
        a.href = `/api/v1/dictionaries/${d.id}/export`;
        a.download = `${d.name}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    },

    // ---- Utilities ----

    showToast(msg, type) {
        if (window.Toast && typeof Toast.show === 'function') Toast.show(msg, type);
    },

    esc(str) {
        return String(str || '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    },
};

window.Dictionaries = Dictionaries;

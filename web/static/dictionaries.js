// Dictionaries module for Bifract
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
    },

    show() {
        this.bindEvents();
        this.showListing();
    },

    bindEvents() {
        document.getElementById('dictCreateBtn')?.addEventListener('click', () => this.showCreateForm());
        document.getElementById('dictCreateSaveBtn')?.addEventListener('click', () => this.saveCreateDictionary());
        document.getElementById('dictCreateCancelBtn')?.addEventListener('click', () => this.hideCreateForm());
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
        try {
            const resp = await fetch('/api/v1/dictionaries', { credentials: 'include' });
            const data = await resp.json();
            if (data.success) {
                this.allDictionaries = data.data.dictionaries || [];
                this.filterDictionaries(document.getElementById('dictSearchInput')?.value || '');
            }
        } catch (e) {
            console.error('Failed to load dictionaries:', e);
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

        if (dicts.length === 0 && this.filteredDictionaries.length === 0) {
            tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:40px;color:var(--text-muted);">No dicts yet. Create one to enrich your logs with <code style="font-family:var(--font-mono);color:var(--accent-secondary);">match()</code>.</td></tr>`;
            return;
        }
        if (dicts.length === 0) {
            tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:40px;color:var(--text-muted);">No results.</td></tr>`;
            return;
        }

        tbody.innerHTML = dicts.map(d => `
<tr>
    <td><a href="#" class="dict-link" data-id="${d.id}">${this.esc(d.name)}${d.is_global ? ' <span class="dict-global-badge">Global</span>' : ''}</a></td>
    <td style="color:var(--text-secondary);font-size:0.88rem;">${this.esc(d.description || '')}</td>
    <td style="color:var(--text-secondary);">${d.columns ? d.columns.length : 0}</td>
    <td style="color:var(--text-secondary);">${(d.row_count || 0).toLocaleString()}</td>
    <td><button class="btn-action danger" data-id="${d.id}" data-action="delete">Delete</button></td>
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
    },

    showCreateForm() {
        const form = document.getElementById('dictCreateForm');
        if (!form) return;
        form.style.display = 'block';
        document.getElementById('dictNewName').value = '';
        document.getElementById('dictNewDesc').value = '';
        const globalCb = document.getElementById('dictNewGlobal');
        if (globalCb) globalCb.checked = false;
        const err = document.getElementById('dictCreateError');
        if (err) err.style.display = 'none';
        document.getElementById('dictNewName').focus();
    },

    hideCreateForm() {
        const form = document.getElementById('dictCreateForm');
        if (form) form.style.display = 'none';
    },

    async saveCreateDictionary() {
        const name = (document.getElementById('dictNewName')?.value || '').trim();
        const desc = (document.getElementById('dictNewDesc')?.value || '').trim();
        const isGlobal = document.getElementById('dictNewGlobal')?.checked || false;
        const err = document.getElementById('dictCreateError');

        if (!name) {
            if (err) { err.textContent = 'Name is required.'; err.style.display = 'block'; }
            document.getElementById('dictNewName')?.focus();
            return;
        }
        if (err) err.style.display = 'none';

        try {
            const resp = await fetch('/api/v1/dictionaries', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ name, description: desc, columns: [], is_global: isGlobal }),
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.hideCreateForm();
            this.showToast('Dictionary created', 'success');
            this.allDictionaries.unshift(data.data);
            this.filterDictionaries(document.getElementById('dictSearchInput')?.value || '');
            this.openDictionary(data.data.id);
        } catch (e) {
            if (err) { err.textContent = e.message; err.style.display = 'block'; }
        }
    },

    // ---- Detail view ----

    async openDictionary(id) {
        try {
            const resp = await fetch(`/api/v1/dictionaries/${id}`, { credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.currentDictionary = data.data;
            this.rowPage = 0;
            this.rowSearch = '';
            this._csvFile = null;
            this.showDetailView();
        } catch (e) {
            this.showToast('Failed to load dictionary: ' + e.message, 'error');
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
        <button id="dictBackBtn" class="btn-secondary">← Back to Dicts</button>
        <div class="dict-editable-block">
            <div id="dictDetailName" class="dict-editable-title" contenteditable="true" spellcheck="false"></div>
            <div id="dictDetailDesc" class="dict-editable-desc" contenteditable="true" spellcheck="false" data-placeholder="Add description..."></div>
        </div>
    </div>
    <div class="dict-detail-header-right">
        <button id="dictImportBtn" class="btn-secondary">Import CSV</button>
        <button id="dictReloadBtn" class="btn-secondary" title="Reload ClickHouse dictionary">Reload</button>
        <button id="dictDeleteBtn" class="btn-secondary" style="color:var(--error);">Delete</button>
    </div>
</div>

<div class="dict-meta-bar">
    <span class="dict-meta-pill" id="dictMetaRows"></span>
    <label class="dict-meta-global"><input type="checkbox" id="dictGlobalToggle"${d.is_global ? ' checked' : ''}> Global</label>
    <span class="dict-meta-syntax" id="dictMetaSyntax"></span>
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
        document.getElementById('dictMetaSyntax').textContent = `match(dict="${d.name}", field=…, column=${d.key_column}, include=[…])`;

        this._bindDetailEvents();
        this.loadRows();
    },

    _bindDetailEvents() {
        document.getElementById('dictBackBtn')?.addEventListener('click', () => this.showListing());
        document.getElementById('dictRowSearch')?.addEventListener('input', (e) => {
            this.rowSearch = e.target.value;
            this.rowPage = 0;
            this.loadRows();
        });
        document.getElementById('dictRowPrevBtn')?.addEventListener('click', () => {
            if (this.rowPage > 0) { this.rowPage--; this.loadRows(); }
        });
        document.getElementById('dictRowNextBtn')?.addEventListener('click', () => {
            const max = Math.max(0, Math.ceil(this.totalRows / this.rowPageSize) - 1);
            if (this.rowPage < max) { this.rowPage++; this.loadRows(); }
        });
        document.getElementById('dictImportBtn')?.addEventListener('click', () => this.toggleImportPanel());
        document.getElementById('cancelImportBtn')?.addEventListener('click', () => this.hideImportPanel());
        document.getElementById('doImportBtn')?.addEventListener('click', () => this.doImport());
        document.getElementById('dictReloadBtn')?.addEventListener('click', () => this.reloadDictionary());
        document.getElementById('dictDeleteBtn')?.addEventListener('click', () => this.deleteDictionary());
        document.getElementById('dictGlobalToggle')?.addEventListener('change', () => this._scheduleSaveMeta());

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

        const params = new URLSearchParams({ limit: this.rowPageSize, offset: this.rowPage * this.rowPageSize });
        if (this.rowSearch) params.set('search', this.rowSearch);

        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/data?${params}`, { credentials: 'include' });
            const result = await resp.json();
            if (!result.success) throw new Error(result.error);
            this.totalRows = result.data.total || 0;
            this.renderRowTable(d, result.data.rows || []);
            this.updateRowPagination();
        } catch (e) {
            const wrap = document.getElementById('dictDataTable');
            if (wrap) wrap.innerHTML = `<div class="dict-error">Failed to load rows: ${this.esc(e.message)}</div>`;
        }
    },

    renderRowTable(dict, rows) {
        const wrap = document.getElementById('dictDataTable');
        if (!wrap) return;
        const cols = dict.columns || [];

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

            let inner = `<span class="dict-col-name">${this.esc(c.name)}</span>`;
            if (isPrimary) {
                inner += '<span class="dict-key-badge dict-key-badge-hdr">key</span>';
            } else if (isSecondary) {
                inner += `<button class="dict-col-key-active-btn" data-col="${this.esc(c.name)}" title="Remove as lookup key">key</button>`;
                inner += `<button class="dict-col-del-btn" data-col="${this.esc(c.name)}" title="Remove column">&times;</button>`;
            } else {
                inner += `<button class="dict-col-key-btn" data-col="${this.esc(c.name)}" title="Use as lookup key">key</button>`;
                inner += `<button class="dict-col-del-btn" data-col="${this.esc(c.name)}" title="Remove column">&times;</button>`;
            }
            return `<th class="${thClass}" data-col="${this.esc(c.name)}">${inner}</th>`;
        }).join('');

        const bodyRows = rows.map((row, i) => {
            const num = this.rowPage * this.rowPageSize + i + 1;
            const cells = cols.map(c => {
                const val = row.fields ? (row.fields[c.name] || '') : '';
                return `<td class="dict-td dict-td-editable" data-col="${this.esc(c.name)}" data-key="${this.esc(row.key)}">${this.esc(val)}</td>`;
            }).join('');
            return `<tr class="dict-row" data-key="${this.esc(row.key)}">
                <td class="dict-td dict-td-num">
                    <span class="dict-row-num">${num}</span>
                    <button class="dict-delete-row" data-key="${this.esc(row.key)}" title="Delete row">&times;</button>
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
                <th class="dict-th dict-th-add-col"><button class="dict-add-col-btn" title="Add column">+</button></th>
            </tr></thead>
            <tbody id="dictRowsBody">
                ${bodyRows}${emptyRow}
                <tr class="dict-add-row-tr">
                    <td class="dict-td dict-td-num"><button class="dict-add-row-btn" title="Add row">+</button></td>
                    <td colspan="${cols.length + 1}" class="dict-td dict-td-add-row-spacer"></td>
                </tr>
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
            this.showToast('Row saved', 'success');
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
            try {
                const row = { key, fields: {} };
                cell.closest('tr').querySelectorAll('.dict-td-editable').forEach(c => {
                    row.fields[c.dataset.col] = c.textContent;
                });
                row.fields[col] = newVal;
                await fetch(`/api/v1/dictionaries/${dict.id}/data`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ rows: [row] }),
                });
            } catch (e) {
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
            const resp = await fetch(`/api/v1/dictionaries/${d.id}/data/${encodeURIComponent(key)}`, {
                method: 'DELETE', credentials: 'include',
            });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.showToast('Row deleted', 'success');
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
            this.showToast('Column added', 'success');
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

    // ---- Delete / Reload ----

    async deleteDictionary() {
        const d = this.currentDictionary;
        if (!d) return;
        if (!confirm(`Delete "${d.name}"? This drops all data and cannot be undone.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${d.id}`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.showToast('Dictionary deleted', 'success');
            this.allDictionaries = this.allDictionaries.filter(x => x.id !== d.id);
            this.showListing();
        } catch (e) {
            this.showToast('Failed to delete: ' + e.message, 'error');
        }
    },

    async deleteDictionaryById(id) {
        const dict = this.allDictionaries.find(d => d.id === id);
        if (!confirm(`Delete "${dict ? dict.name : 'this dictionary'}"? This drops all data and cannot be undone.`)) return;
        try {
            const resp = await fetch(`/api/v1/dictionaries/${id}`, { method: 'DELETE', credentials: 'include' });
            const data = await resp.json();
            if (!data.success) throw new Error(data.error);
            this.showToast('Dictionary deleted', 'success');
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
            this.showToast('Dictionary reloaded', 'success');
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
        }
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

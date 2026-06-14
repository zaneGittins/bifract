const SchemaFields = {
    defaults: [],
    custom: [],
    _pollTimer: null,

    init() {
        document.getElementById('schemaFieldAddBtn')?.addEventListener('click', () => this.showAddForm());
        document.getElementById('schemaFieldCancelBtn')?.addEventListener('click', () => this.hideAddForm());
        document.getElementById('schemaFieldSaveBtn')?.addEventListener('click', () => this.saveField());
        document.getElementById('schemaFieldResetBtn')?.addEventListener('click', () => this.openResetModal());

        // Import / export
        document.getElementById('schemaExportBtn')?.addEventListener('click', () => this.exportYaml());
        document.getElementById('schemaImportBtn')?.addEventListener('click', () => document.getElementById('schemaImportInput')?.click());
        document.getElementById('schemaImportInput')?.addEventListener('change', e => this.importYaml(e));

        // Submit on Enter in the field name input
        document.getElementById('schemaFieldName')?.addEventListener('keydown', e => {
            if (e.key === 'Enter') this.saveField();
        });

        // Reset modal wiring
        document.getElementById('schemaResetCancelBtn')?.addEventListener('click', () => this.closeResetModal());
        document.getElementById('schemaResetConfirmInput')?.addEventListener('input', e => this._onResetPhraseInput(e));
        document.getElementById('schemaResetDoBtn')?.addEventListener('click', () => this.executeReset());
        document.getElementById('schemaResetModal')?.addEventListener('click', e => {
            if (e.target === document.getElementById('schemaResetModal')) this.closeResetModal();
        });
    },

    show() {
        this.load();
    },

    async load() {
        const container = document.getElementById('schemaFieldsListContainer');
        // Only show the full-container spinner on the first paint; background
        // refreshes (status polling) should not flash the list.
        if (container && !this.custom.length && !this.defaults.length) {
            container.innerHTML = '<div class="loading">Loading schema fields...</div>';
        }
        try {
            const data = await HttpUtils.safeFetch('/api/v1/admin/schema-fields');
            this.defaults = data.data.defaults || [];
            this.custom = data.data.custom || [];
            this.render();
            this._scheduleStatusPoll();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to load schema fields', err.message);
        }
    },

    // Re-fetch while any custom field is still applying its ClickHouse schema so
    // the "Indexing" badge flips to "Active" without a manual refresh.
    _scheduleStatusPoll() {
        if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
        const pending = this.custom.some(f => f.sync_status === 'pending');
        // offsetParent is null when the schema tab is hidden; stop polling then
        // so we don't hit the API forever in the background. show() resumes it.
        const visible = document.getElementById('mainSchemaTabContent')?.offsetParent !== null;
        if (pending && visible) {
            this._pollTimer = setTimeout(() => this.load(), 2500);
        }
    },

    render() {
        const container = document.getElementById('schemaFieldsListContainer');
        if (!container) return;

        let html = '';

        // Project Defaults
        html += `<div class="schema-section-heading">Project Defaults <span class="schema-section-count">${this.defaults.length}</span></div>`;
        if (this.defaults.length > 0) {
            html += '<table class="schema-fields-table"><thead><tr><th>Field Name</th><th>Index Type</th><th></th></tr></thead><tbody>';
            for (const f of this.defaults) {
                html += `<tr>
                    <td>${this.escHtml(f.field_name)}</td>
                    <td>${this._indexBadge(f.index_type)}</td>
                    <td><span class="badge-default">built-in</span></td>
                </tr>`;
            }
            html += '</tbody></table>';
        }

        html += '<div class="schema-section-divider"></div>';

        // Custom Fields
        html += `<div class="schema-section-heading">Custom Fields <span class="schema-section-count">${this.custom.length}</span></div>`;
        if (this.custom.length === 0) {
            html += `<div class="schema-empty-state">
                <strong>No custom fields yet</strong>
                Add a field to enable index acceleration for log attributes specific to your environment.
            </div>`;
        } else {
            html += '<table class="schema-fields-table"><thead><tr><th>Field Name</th><th>Index Type</th><th>Status</th><th>Added By</th><th></th></tr></thead><tbody>';
            for (const f of this.custom) {
                html += `<tr>
                    <td>${this.escHtml(f.field_name)}</td>
                    <td>${this._indexBadge(f.index_type)}</td>
                    <td>${this._statusBadge(f)}</td>
                    <td style="color:var(--text-muted);font-size:0.8125rem">${this.escHtml(f.created_by || '')}</td>
                    <td style="text-align:right"><button class="btn-danger btn-sm" onclick="SchemaFields.deleteField('${this.escHtml(f.field_name)}')">Remove</button></td>
                </tr>`;
            }
            html += '</tbody></table>';
        }

        container.innerHTML = html;
    },

    _indexBadge(type) {
        if (type === 'none' || !type) {
            return '<span class="index-badge index-badge-none">None</span>';
        }
        if (type === 'set') {
            return '<span class="index-badge index-badge-set">Set</span>';
        }
        return '<span class="index-badge index-badge-bloom">Bloom Filter</span>';
    },

    _statusBadge(f) {
        switch (f.sync_status) {
            case 'pending':
                return '<span class="schema-status schema-status-pending"><span class="schema-status-dot"></span>Indexing</span>';
            case 'error':
                return `<span class="schema-status schema-status-error" title="${this.escHtml(f.sync_error || 'Schema update failed')}">Error</span>`;
            default:
                return '<span class="schema-status schema-status-active">Active</span>';
        }
    },

    showAddForm() {
        const form = document.getElementById('schemaFieldAddForm');
        const input = document.getElementById('schemaFieldName');
        if (form) form.style.display = 'block';
        if (input) { input.value = ''; input.focus(); }
        const sel = document.getElementById('schemaFieldIndexType');
        if (sel) sel.value = 'none';
    },

    hideAddForm() {
        const form = document.getElementById('schemaFieldAddForm');
        if (form) form.style.display = 'none';
    },

    async saveField() {
        const name = document.getElementById('schemaFieldName')?.value.trim();
        const indexType = document.getElementById('schemaFieldIndexType')?.value;
        if (!name) {
            if (window.Toast) Toast.error('Validation Error', 'Field name is required');
            document.getElementById('schemaFieldName')?.focus();
            return;
        }
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ field_name: name, index_type: indexType }),
            });
            this.hideAddForm();
            if (window.Toast) Toast.success('Field added', `"${name}" is indexing. Active for newly ingested logs; existing logs are not retroactively indexed.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to add field', err.message);
        }
    },

    async deleteField(name) {
        if (!confirm(`Remove custom field "${name}"?\n\nThe ClickHouse type hint and index remain until the next schema reset.`)) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/${encodeURIComponent(name)}`, { method: 'DELETE' });
            if (window.Toast) Toast.success('Field removed', `"${name}" removed. Queries will no longer use its index.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to remove field', err.message);
        }
    },

    openResetModal() {
        const modal = document.getElementById('schemaResetModal');
        const input = document.getElementById('schemaResetConfirmInput');
        const btn = document.getElementById('schemaResetDoBtn');
        if (input) { input.value = ''; input.classList.remove('phrase-match'); }
        if (btn) btn.disabled = true;
        if (modal) modal.style.display = 'flex';
        setTimeout(() => input?.focus(), 50);
    },

    closeResetModal() {
        const modal = document.getElementById('schemaResetModal');
        if (modal) modal.style.display = 'none';
    },

    _onResetPhraseInput(e) {
        const phrase = 'DELETE ALL LOG DATA';
        const match = e.target.value === phrase;
        const btn = document.getElementById('schemaResetDoBtn');
        e.target.classList.toggle('phrase-match', match);
        if (btn) btn.disabled = !match;
    },

    async executeReset() {
        const btn = document.getElementById('schemaResetDoBtn');
        const cancelBtn = document.getElementById('schemaResetCancelBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Resetting...'; }
        if (cancelBtn) cancelBtn.disabled = true;
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ confirm: 'DELETE ALL LOG DATA' }),
            });
            this.closeResetModal();
            if (window.Toast) Toast.success('Schema reset complete', 'All log data has been deleted and the schema rebuilt');
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Reset failed', err.message);
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = 'Reset and Delete All Logs'; }
            if (cancelBtn) cancelBtn.disabled = false;
        }
    },

    async exportYaml() {
        try {
            const res = await fetch('/api/v1/admin/schema-fields/export', { credentials: 'include' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'schema-fields.yaml';
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);
        } catch (err) {
            if (window.Toast) Toast.error('Export failed', err.message);
        }
    },

    async importYaml(e) {
        const input = e.target;
        const file = input.files && input.files[0];
        if (!file) return;
        const count = this.custom.length;
        if (!confirm(`Import "${file.name}"?\n\nThis replaces all ${count} current custom field(s) with the file's contents. Fields not in the file are removed. Log data is not affected.`)) {
            input.value = '';
            return;
        }
        try {
            const text = await file.text();
            const data = await HttpUtils.safeFetch('/api/v1/admin/schema-fields/import', {
                method: 'POST',
                headers: { 'Content-Type': 'text/yaml' },
                body: text,
            });
            if (window.Toast) Toast.success('Schema imported', data.data?.message || 'Custom fields replaced.');
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Import failed', err.message);
        } finally {
            input.value = '';
        }
    },

    escHtml(s) {
        return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    },
};

window.SchemaFields = SchemaFields;

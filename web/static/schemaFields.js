const SchemaFields = {
    defaults: [],
    custom: [],

    init() {
        document.getElementById('schemaFieldAddBtn')?.addEventListener('click', () => this.showAddForm());
        document.getElementById('schemaFieldCancelBtn')?.addEventListener('click', () => this.hideAddForm());
        document.getElementById('schemaFieldSaveBtn')?.addEventListener('click', () => this.saveField());
        document.getElementById('schemaFieldResetBtn')?.addEventListener('click', () => this.confirmReset());
    },

    show() {
        this.load();
    },

    async load() {
        const container = document.getElementById('schemaFieldsListContainer');
        if (container) container.innerHTML = '<div class="loading">Loading schema fields...</div>';
        try {
            const data = await HttpUtils.safeFetch('/api/v1/admin/schema-fields');
            this.defaults = data.data.defaults || [];
            this.custom = data.data.custom || [];
            this.render();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to load schema fields', err.message);
        }
    },

    render() {
        const container = document.getElementById('schemaFieldsListContainer');
        if (!container) return;

        const indexLabel = t => t === 'set' ? 'set(256)' : 'bloom_filter(0.001)';

        let html = '<h4 style="margin:0 0 8px 0;color:var(--text-muted)">Project Defaults</h4>';
        html += '<table class="schema-fields-table"><thead><tr><th>Field Name</th><th>Index Type</th><th></th></tr></thead><tbody>';
        for (const f of this.defaults) {
            html += `<tr>
                <td><code>${this.escHtml(f.field_name)}</code></td>
                <td><span class="index-type-badge">${indexLabel(f.index_type)}</span></td>
                <td><span class="badge-readonly">default</span></td>
            </tr>`;
        }
        html += '</tbody></table>';

        html += '<h4 style="margin:16px 0 8px 0;color:var(--text-muted)">Custom Fields</h4>';
        if (this.custom.length === 0) {
            html += '<p class="help-text">No custom fields defined.</p>';
        } else {
            html += '<table class="schema-fields-table"><thead><tr><th>Field Name</th><th>Index Type</th><th>Added By</th><th></th></tr></thead><tbody>';
            for (const f of this.custom) {
                html += `<tr>
                    <td><code>${this.escHtml(f.field_name)}</code></td>
                    <td><span class="index-type-badge">${indexLabel(f.index_type)}</span></td>
                    <td>${this.escHtml(f.created_by)}</td>
                    <td><button class="btn-danger btn-sm" onclick="SchemaFields.deleteField('${this.escHtml(f.field_name)}')">Remove</button></td>
                </tr>`;
            }
            html += '</tbody></table>';
        }

        container.innerHTML = html;
    },

    showAddForm() {
        const form = document.getElementById('schemaFieldAddForm');
        const input = document.getElementById('schemaFieldName');
        if (form) form.style.display = 'block';
        if (input) { input.value = ''; input.focus(); }
        const sel = document.getElementById('schemaFieldIndexType');
        if (sel) sel.value = 'bloom_filter';
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
            return;
        }
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ field_name: name, index_type: indexType }),
            });
            this.hideAddForm();
            if (window.Toast) Toast.success('Field added', `"${name}" added and schema updated`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to add field', err.message);
        }
    },

    async deleteField(name) {
        if (!confirm(`Remove custom field "${name}"?\n\nThe type hint and index remain in ClickHouse until the next schema reset.`)) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/admin/schema-fields/${encodeURIComponent(name)}`, { method: 'DELETE' });
            if (window.Toast) Toast.success('Field removed', `"${name}" removed. Queries on this field will no longer use its index.`);
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Failed to remove field', err.message);
        }
    },

    confirmReset() {
        const phrase = 'DELETE ALL LOG DATA';
        const input = prompt(`This will permanently delete ALL ingested log data and rebuild the ClickHouse schema.\n\nFractal configs, normalizers, and all other settings are not affected.\n\nType "${phrase}" to confirm:`);
        if (input === null) return;
        if (input !== phrase) {
            if (window.Toast) Toast.error('Reset cancelled', 'Confirmation phrase did not match');
            return;
        }
        this.executeReset();
    },

    async executeReset() {
        const btn = document.getElementById('schemaFieldResetBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Resetting...'; }
        try {
            await HttpUtils.safeFetch('/api/v1/admin/schema-fields/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ confirm: 'DELETE ALL LOG DATA' }),
            });
            if (window.Toast) Toast.success('Schema reset complete', 'All log data has been deleted and schema rebuilt');
            this.load();
        } catch (err) {
            if (window.Toast) Toast.error('Reset failed', err.message);
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = 'Reset Schema & Delete All Logs'; }
        }
    },

    escHtml(s) {
        return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    },
};

window.SchemaFields = SchemaFields;

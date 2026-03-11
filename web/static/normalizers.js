const Normalizers = {
    normalizers: [],
    editingId: null,
    currentTransforms: [],

    TRANSFORMS: {
        flatten_leaf: { label: 'flatten_leaf', desc: 'Flatten nested keys to leaf name only (user.profile.name -> name)', conflicts: ['dedot'] },
        snake_case:   { label: 'snake_case',   desc: 'Convert field names to snake_case',              conflicts: ['camelCase', 'PascalCase'] },
        camelCase:    { label: 'camelCase',     desc: 'Convert field names to camelCase',               conflicts: ['snake_case', 'PascalCase'] },
        PascalCase:   { label: 'PascalCase',    desc: 'Convert field names to PascalCase',              conflicts: ['snake_case', 'camelCase'] },
        dedot:        { label: 'dedot',         desc: 'Replace dots with underscores (a.b.c -> a_b_c)', conflicts: ['flatten_leaf'] },
        lowercase:  { label: 'lowercase',  desc: 'Lowercase all field names',                      conflicts: ['uppercase'] },
        uppercase:  { label: 'uppercase',  desc: 'Uppercase all field names',                      conflicts: ['lowercase'] }
    },

    init() {
        const createBtn = document.getElementById('normalizerCreateBtn');
        if (createBtn) {
            createBtn.addEventListener('click', () => this.openCreateForm());
        }
    },

    show() {
        const editor = document.getElementById('normalizerEditorView');
        if (editor) editor.style.display = 'none';
        this.loadNormalizers();
    },

    hide() {
        const editor = document.getElementById('normalizerEditorView');
        if (editor) editor.style.display = 'none';
    },

    async loadNormalizers() {
        try {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers');
            this.normalizers = data.data.normalizers || [];
            this.renderNormalizers();
        } catch (err) {
            console.error('[Normalizers] Failed to load:', err);
            Utils.showNotification('Failed to load normalizers', 'error');
        }
    },

    renderNormalizers() {
        const container = document.getElementById('normalizersListContainer');
        if (!container) return;

        if (this.normalizers.length === 0) {
            container.innerHTML = '<p class="empty-state">No normalizers configured.</p>';
            return;
        }

        let html = `<table class="context-links-table">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Transforms</th>
                    <th>Field Mappings</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>`;

        this.normalizers.forEach(n => {
            const transforms = (n.transforms || []).join(', ') || 'None';
            const mappingCount = (n.field_mappings || []).length;
            const defaultBadge = n.is_default ? ' <span class="context-link-badge">default</span>' : '';
            html += `<tr>
                <td>${Utils.escapeHtml(n.name)}${defaultBadge}</td>
                <td class="context-link-fields">${Utils.escapeHtml(transforms)}</td>
                <td>${mappingCount} mapping${mappingCount !== 1 ? 's' : ''}</td>
                <td class="context-link-actions">
                    <button class="btn-sm btn-secondary" onclick="Normalizers.openEditForm('${n.id}')" title="Edit">Edit</button>
                    ${!n.is_default ? `<button class="btn-sm btn-secondary" onclick="Normalizers.setDefault('${n.id}')" title="Set as default">Set Default</button>` : ''}
                    ${!n.is_default ? `<button class="btn-sm btn-danger" onclick="Normalizers.deleteNormalizer('${n.id}')" title="Delete">Delete</button>` : ''}
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    },

    showEditor(title) {
        const listView = document.getElementById('normalizersView');
        const editorView = document.getElementById('normalizerEditorView');
        if (listView) listView.style.display = 'none';
        if (editorView) editorView.style.display = 'block';

        const titleEl = document.getElementById('normalizerEditorTitle');
        if (titleEl) titleEl.textContent = title;
    },

    backToList() {
        const listView = document.getElementById('normalizersView');
        const editorView = document.getElementById('normalizerEditorView');
        if (editorView) editorView.style.display = 'none';
        if (listView) listView.style.display = 'block';
        this.editingId = null;
        this.loadNormalizers();
    },

    // --- Transform list management ---

    _renderTransformsList() {
        const container = document.getElementById('normalizerTransformsList');
        if (!container) return;

        container.innerHTML = '';
        this.currentTransforms.forEach((key, i) => {
            const t = this.TRANSFORMS[key];
            if (!t) return;
            const row = document.createElement('div');
            row.className = 'transform-item';
            row.innerHTML = `
                <div class="transform-item-order">
                    <button onclick="Normalizers.moveTransform(${i}, -1)" title="Move up" ${i === 0 ? 'disabled' : ''}>&#9650;</button>
                    <button onclick="Normalizers.moveTransform(${i}, 1)" title="Move down" ${i === this.currentTransforms.length - 1 ? 'disabled' : ''}>&#9660;</button>
                </div>
                <span class="transform-item-label"><code>${Utils.escapeHtml(t.label)}</code> <span class="transform-item-desc">- ${Utils.escapeHtml(t.desc)}</span></span>
                <button class="btn-sm btn-danger" onclick="Normalizers.removeTransform(${i})" title="Remove">&times;</button>
            `;
            container.appendChild(row);
        });

        this._updateTransformSelect();
    },

    _updateTransformSelect() {
        const select = document.getElementById('normalizerTransformSelect');
        if (!select) return;

        const available = this._getAvailableTransforms();
        select.innerHTML = '<option value="">Add a transform...</option>';
        available.forEach(key => {
            const t = this.TRANSFORMS[key];
            const opt = document.createElement('option');
            opt.value = key;
            opt.textContent = `${t.label} - ${t.desc}`;
            select.appendChild(opt);
        });
    },

    _getAvailableTransforms() {
        const blocked = new Set(this.currentTransforms);
        this.currentTransforms.forEach(key => {
            const t = this.TRANSFORMS[key];
            if (t) t.conflicts.forEach(c => blocked.add(c));
        });
        return Object.keys(this.TRANSFORMS).filter(k => !blocked.has(k));
    },

    addTransformFromSelect() {
        const select = document.getElementById('normalizerTransformSelect');
        if (!select || !select.value) return;
        this.currentTransforms.push(select.value);
        this._renderTransformsList();
    },

    removeTransform(index) {
        this.currentTransforms.splice(index, 1);
        this._renderTransformsList();
    },

    moveTransform(index, direction) {
        const newIndex = index + direction;
        if (newIndex < 0 || newIndex >= this.currentTransforms.length) return;
        const item = this.currentTransforms.splice(index, 1)[0];
        this.currentTransforms.splice(newIndex, 0, item);
        this._renderTransformsList();
    },

    // --- Form open/close ---

    openCreateForm() {
        this.editingId = null;

        document.getElementById('normalizerName').value = '';
        document.getElementById('normalizerDescription').value = '';

        this.currentTransforms = [];
        this._renderTransformsList();

        document.getElementById('normalizerMappings').innerHTML = '';
        this.addMappingRow();

        document.getElementById('normalizerTimestampFields').innerHTML = '';

        this.showEditor('Create Normalizer');
        document.getElementById('normalizerName')?.focus();
    },

    async openEditForm(id) {
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${id}`);
            const n = data.data;
            this.editingId = id;

            document.getElementById('normalizerName').value = n.name;
            document.getElementById('normalizerDescription').value = n.description || '';

            this.currentTransforms = (n.transforms || []).filter(t => t in this.TRANSFORMS);
            this._renderTransformsList();

            const container = document.getElementById('normalizerMappings');
            container.innerHTML = '';
            const mappings = n.field_mappings || [];
            if (mappings.length === 0) {
                this.addMappingRow();
            } else {
                mappings.forEach(m => {
                    this.addMappingRow((m.sources || []).join(', '), m.target);
                });
            }

            const tsContainer = document.getElementById('normalizerTimestampFields');
            tsContainer.innerHTML = '';
            const tsFields = n.timestamp_fields || [];
            tsFields.forEach(tf => {
                this.addTimestampFieldRow(tf.field, tf.format);
            });

            this.showEditor('Edit Normalizer');
        } catch (err) {
            Utils.showNotification('Failed to load normalizer', 'error');
        }
    },

    _editingSourcesInput: null,

    addMappingRow(sources, target) {
        const container = document.getElementById('normalizerMappings');
        if (!container) return;

        const row = document.createElement('div');
        row.className = 'normalizer-mapping-row';
        row.innerHTML = `
            <input type="text" class="mapping-sources" placeholder="source1, source2, ..." value="${Utils.escapeHtml(sources || '')}" readonly>
            <span class="mapping-arrow">-></span>
            <input type="text" class="mapping-target" placeholder="target_field" value="${Utils.escapeHtml(target || '')}">
            <button class="btn-sm btn-danger mapping-remove" onclick="this.parentElement.remove()" title="Remove">&times;</button>
        `;
        const sourcesInput = row.querySelector('.mapping-sources');
        sourcesInput.addEventListener('dblclick', () => this.openMappingSourcesModal(sourcesInput));
        sourcesInput.addEventListener('click', () => this.openMappingSourcesModal(sourcesInput));
        container.appendChild(row);
    },

    addTimestampFieldRow(field, format) {
        const container = document.getElementById('normalizerTimestampFields');
        if (!container) return;

        const row = document.createElement('div');
        row.className = 'normalizer-ts-field-row';
        row.innerHTML = `
            <input type="text" class="ts-field-name" placeholder="Field name (e.g. system_time)" value="${Utils.escapeHtml(field || '')}">
            <input type="text" class="ts-field-format" placeholder="Go time format" value="${Utils.escapeHtml(format || '')}">
            <button class="btn-sm btn-danger mapping-remove" onclick="this.parentElement.remove()" title="Remove">&times;</button>
        `;
        container.appendChild(row);
    },

    openMappingSourcesModal(inputEl) {
        this._editingSourcesInput = inputEl;
        const csv = inputEl.value;
        const lines = csv ? csv.split(',').map(s => s.trim()).filter(s => s).join('\n') : '';
        const textarea = document.getElementById('mappingSourcesTextarea');
        textarea.value = lines;
        document.getElementById('mappingSourcesModal').style.display = 'flex';
        textarea.focus();
    },

    saveMappingSourcesModal() {
        const textarea = document.getElementById('mappingSourcesTextarea');
        const sources = textarea.value.split('\n').map(s => s.trim()).filter(s => s);
        if (this._editingSourcesInput) {
            this._editingSourcesInput.value = sources.join(', ');
        }
        this.closeMappingSourcesModal();
    },

    closeMappingSourcesModal() {
        document.getElementById('mappingSourcesModal').style.display = 'none';
        this._editingSourcesInput = null;
    },

    _getFormData() {
        const name = document.getElementById('normalizerName').value.trim();
        const description = document.getElementById('normalizerDescription').value.trim();

        const transforms = [...this.currentTransforms];

        const fieldMappings = [];
        const rows = document.querySelectorAll('#normalizerMappings .normalizer-mapping-row');
        rows.forEach(row => {
            const sourcesInput = row.querySelector('.mapping-sources').value.trim();
            const targetInput = row.querySelector('.mapping-target').value.trim();
            if (sourcesInput && targetInput) {
                const sources = sourcesInput.split(',').map(s => s.trim()).filter(s => s);
                if (sources.length > 0) {
                    fieldMappings.push({ sources, target: targetInput });
                }
            }
        });

        const timestampFields = [];
        const tsRows = document.querySelectorAll('#normalizerTimestampFields .normalizer-ts-field-row');
        tsRows.forEach(row => {
            const field = row.querySelector('.ts-field-name').value.trim();
            const format = row.querySelector('.ts-field-format').value.trim();
            if (field && format) {
                timestampFields.push({ field, format });
            }
        });

        return { name, description, transforms, field_mappings: fieldMappings, timestamp_fields: timestampFields };
    },

    async saveNormalizer() {
        const body = this._getFormData();

        if (!body.name) {
            Utils.showNotification('Name is required', 'error');
            return;
        }

        try {
            if (this.editingId) {
                await HttpUtils.safeFetch(`/api/v1/normalizers/${this.editingId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                Utils.showNotification('Normalizer updated', 'success');
            } else {
                await HttpUtils.safeFetch('/api/v1/normalizers', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                Utils.showNotification('Normalizer created', 'success');
            }
            this.backToList();
        } catch (err) {
            Utils.showNotification(`Failed to save: ${err.message}`, 'error');
        }
    },

    async deleteNormalizer(id) {
        if (!confirm('Delete this normalizer? Tokens using it will revert to no normalization.')) return;
        try {
            await HttpUtils.safeFetch(`/api/v1/normalizers/${id}`, { method: 'DELETE' });
            Utils.showNotification('Normalizer deleted', 'success');
            this.loadNormalizers();
        } catch (err) {
            Utils.showNotification(`Failed to delete: ${err.message}`, 'error');
        }
    },

    async setDefault(id) {
        try {
            await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/set-default`, { method: 'POST' });
            Utils.showNotification('Default normalizer updated', 'success');
            this.loadNormalizers();
        } catch (err) {
            Utils.showNotification(`Failed to set default: ${err.message}`, 'error');
        }
    }
};

window.Normalizers = Normalizers;

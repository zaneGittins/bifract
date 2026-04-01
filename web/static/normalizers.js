const Normalizers = {
    normalizers: [],
    editingId: null,
    currentTransforms: [],

    TRANSFORMS: {
        flatten_leaf: { label: 'flatten_leaf', desc: 'Flatten nested keys to leaf name only (user.profile.name -> name)', conflicts: ['flatten_full', 'dedot'] },
        flatten_full: { label: 'flatten_full', desc: 'Flatten nested keys with full path (user.profile.name -> user_profile_name)', conflicts: ['flatten_leaf', 'dedot'] },
        snake_case:   { label: 'snake_case',   desc: 'Convert field names to snake_case',              conflicts: ['camelCase', 'PascalCase'] },
        camelCase:    { label: 'camelCase',     desc: 'Convert field names to camelCase',               conflicts: ['snake_case', 'PascalCase'] },
        PascalCase:   { label: 'PascalCase',    desc: 'Convert field names to PascalCase',              conflicts: ['snake_case', 'camelCase'] },
        dedot:        { label: 'dedot',         desc: 'Replace dots with underscores (a.b.c -> a_b_c)', conflicts: ['flatten_leaf', 'flatten_full'] },
        lowercase:  { label: 'lowercase',  desc: 'Lowercase all field names',                      conflicts: ['uppercase'] },
        uppercase:  { label: 'uppercase',  desc: 'Uppercase all field names',                      conflicts: ['lowercase'] }
    },

    TIMESTAMP_PRESETS: [
        { label: 'ISO 8601 / RFC 3339',         value: '2006-01-02T15:04:05Z07:00' },
        { label: 'ISO 8601 (nanoseconds)',       value: '2006-01-02T15:04:05.999999999Z07:00' },
        { label: 'ISO 8601 (milliseconds)',      value: '2006-01-02T15:04:05.000Z07:00' },
        { label: 'Date + Time (space)',          value: '2006-01-02 15:04:05' },
        { label: 'Date + Time (millis)',         value: '2006-01-02 15:04:05.000' },
        { label: 'Unix (seconds)',               value: 'unix' },
        { label: 'Unix (milliseconds)',          value: 'unixmilli' },
        { label: 'Unix (microseconds)',          value: 'unixmicro' },
        { label: 'Unix (nanoseconds)',           value: 'unixnano' },
        { label: 'RFC 822',                      value: '02 Jan 06 15:04 MST' },
        { label: 'RFC 850',                      value: 'Monday, 02-Jan-06 15:04:05 MST' },
        { label: 'ANSIC',                        value: 'Mon Jan _2 15:04:05 2006' },
        { label: 'Syslog (BSD)',                 value: 'Jan _2 15:04:05' },
        { label: 'Syslog (ISO)',                 value: '2006-01-02T15:04:05.000000+00:00' },
        { label: 'US Date + Time (12h)',           value: '1/2/2006 3:04:05 PM' },
        { label: 'US Date + Time (24h)',           value: '1/2/2006 15:04:05' },
        { label: 'Apache Common Log',            value: '02/Jan/2006:15:04:05 -0700' },
        { label: 'Windows FileTime (ticks)',     value: 'unixnano' },
    ],

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
                    <th>Used By</th>
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
                <td><span class="normalizer-usage-cell" id="normalizer-usage-${n.id}">--</span></td>
                <td class="context-link-actions">
                    <button class="btn-sm btn-secondary" onclick="Normalizers.openEditForm('${n.id}')" title="Edit">Edit</button>
                    <button class="btn-sm btn-secondary" onclick="Normalizers.duplicateNormalizer('${n.id}')" title="Duplicate">Duplicate</button>
                    <button class="btn-sm btn-secondary" onclick="Normalizers.exportNormalizer('${n.id}', '${Utils.escapeHtml(n.name)}')" title="Export YAML">Export</button>
                    ${!n.is_default ? `<button class="btn-sm btn-secondary" onclick="Normalizers.setDefault('${n.id}')" title="Set as default">Set Default</button>` : ''}
                    ${!n.is_default ? `<button class="btn-sm btn-danger" onclick="Normalizers.deleteNormalizer('${n.id}')" title="Delete">Delete</button>` : ''}
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;

        // Load token usage for each normalizer
        this.normalizers.forEach(n => this._loadTokenUsage(n.id));
    },

    async _loadTokenUsage(normalizerId) {
        const cell = document.getElementById(`normalizer-usage-${normalizerId}`);
        if (!cell) return;
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${normalizerId}/tokens`);
            const tokens = data.data.tokens || [];
            if (tokens.length === 0) {
                cell.textContent = 'No tokens';
                cell.className = 'normalizer-usage-cell usage-none';
                return;
            }
            // Group by fractal
            const byFractal = {};
            tokens.forEach(t => {
                if (!byFractal[t.fractal_name]) byFractal[t.fractal_name] = [];
                byFractal[t.fractal_name].push(t.token_name);
            });
            const parts = Object.entries(byFractal).map(([fractal, tnames]) => {
                const tokenList = tnames.map(n => Utils.escapeHtml(n)).join(', ');
                return `<span class="usage-fractal" title="${tokenList}">${Utils.escapeHtml(fractal)} (${tnames.length})</span>`;
            });
            cell.innerHTML = parts.join(', ');
            cell.className = 'normalizer-usage-cell';
        } catch {
            cell.textContent = '--';
        }
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

        const previewContainer = document.getElementById('normalizerPreviewContainer');
        if (previewContainer) previewContainer.style.display = 'none';

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

            const previewContainer = document.getElementById('normalizerPreviewContainer');
            if (previewContainer) previewContainer.style.display = 'none';

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

        // Build preset options
        let presetOptions = '<option value="">Select preset...</option>';
        this.TIMESTAMP_PRESETS.forEach(p => {
            const selected = (format && format === p.value) ? ' selected' : '';
            presetOptions += `<option value="${Utils.escapeHtml(p.value)}"${selected}>${Utils.escapeHtml(p.label)}</option>`;
        });
        // Add custom option if format doesn't match any preset
        const isCustom = format && !this.TIMESTAMP_PRESETS.some(p => p.value === format);
        if (isCustom) {
            presetOptions += `<option value="${Utils.escapeHtml(format)}" selected>Custom: ${Utils.escapeHtml(format)}</option>`;
        }

        row.innerHTML = `
            <input type="text" class="ts-field-name" placeholder="Field name (e.g. system_time)" value="${Utils.escapeHtml(field || '')}">
            <select class="ts-field-preset" title="Select a preset or type a custom format">
                ${presetOptions}
            </select>
            <input type="text" class="ts-field-format" placeholder="Custom Go format (e.g. 2006-01-02T15:04:05Z07:00)" value="${Utils.escapeHtml(format || '')}">
            <button class="btn-sm btn-danger mapping-remove" onclick="this.parentElement.remove()" title="Remove">&times;</button>
        `;

        // Wire up preset -> format sync
        const presetSelect = row.querySelector('.ts-field-preset');
        const formatInput = row.querySelector('.ts-field-format');
        presetSelect.addEventListener('change', () => {
            if (presetSelect.value) {
                formatInput.value = presetSelect.value;
            }
        });
        formatInput.addEventListener('input', () => {
            // If user types a custom value, deselect preset
            const match = this.TIMESTAMP_PRESETS.find(p => p.value === formatInput.value);
            presetSelect.value = match ? match.value : '';
        });

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
        // Show token usage before confirming
        let usageMsg = '';
        try {
            const data = await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/tokens`);
            const tokens = data.data.tokens || [];
            if (tokens.length > 0) {
                const names = tokens.map(t => `${t.token_name} (${t.fractal_name})`).join(', ');
                usageMsg = `\n\nCurrently used by ${tokens.length} token(s): ${names}`;
            }
        } catch { /* ignore */ }

        if (!confirm(`Delete this normalizer? Tokens using it will revert to no normalization.${usageMsg}`)) return;
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
    },

    // --- Duplicate ---

    async duplicateNormalizer(id) {
        try {
            await HttpUtils.safeFetch(`/api/v1/normalizers/${id}/duplicate`, { method: 'POST' });
            Utils.showNotification('Normalizer duplicated', 'success');
            this.loadNormalizers();
        } catch (err) {
            Utils.showNotification(`Failed to duplicate: ${err.message}`, 'error');
        }
    },

    // --- Export / Import YAML ---

    async exportNormalizer(id, name) {
        try {
            const response = await fetch(`/api/v1/normalizers/${id}/export`, { credentials: 'include' });
            if (!response.ok) throw new Error('Export failed');
            const yamlText = await response.text();
            const blob = new Blob([yamlText], { type: 'text/yaml' });
            const link = document.createElement('a');
            const safeName = name.replace(/[^a-zA-Z0-9_-]/g, '_');
            link.href = URL.createObjectURL(blob);
            link.download = `${safeName}.yaml`;
            link.style.display = 'none';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(link.href);
            Utils.showNotification('Normalizer exported', 'success');
        } catch (err) {
            Utils.showNotification(`Export failed: ${err.message}`, 'error');
        }
    },

    importNormalizer() {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.yaml,.yml';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            try {
                const text = await file.text();
                const response = await fetch('/api/v1/normalizers/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'text/yaml' },
                    credentials: 'include',
                    body: text
                });
                const data = await response.json();
                if (!data.success) throw new Error(data.error || 'Import failed');
                Utils.showNotification('Normalizer imported', 'success');
                this.loadNormalizers();
            } catch (err) {
                Utils.showNotification(`Import failed: ${err.message}`, 'error');
            }
        };
        input.click();
    },

    // --- Dry-Run Preview ---

    async runPreview() {
        const textarea = document.getElementById('normalizerPreviewInput');
        const resultsContainer = document.getElementById('normalizerPreviewResults');
        if (!textarea || !resultsContainer) return;

        const sampleJSON = textarea.value.trim();
        if (!sampleJSON) {
            Utils.showNotification('Paste sample JSON to preview', 'error');
            return;
        }

        // Validate JSON client-side first
        try {
            JSON.parse(sampleJSON);
        } catch {
            Utils.showNotification('Invalid JSON', 'error');
            return;
        }

        const formData = this._getFormData();

        try {
            const data = await HttpUtils.safeFetch('/api/v1/normalizers/preview', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    transforms: formData.transforms,
                    field_mappings: formData.field_mappings,
                    sample_json: sampleJSON
                })
            });

            const fields = data.data.fields || [];
            const collisions = data.data.collisions || {};
            const hasCollisions = Object.keys(collisions).length > 0;

            let html = '';

            if (hasCollisions) {
                html += '<div class="preview-collision-warning">Field name collisions detected. Colliding fields will fall back to their full dot-notation path during ingestion.</div>';
            }

            html += `<table class="context-links-table preview-table">
                <thead>
                    <tr>
                        <th>Original Field</th>
                        <th>Normalized</th>
                        <th>Sample Value</th>
                    </tr>
                </thead>
                <tbody>`;

            fields.forEach(f => {
                const collisionClass = f.collision ? ' class="preview-collision-row"' : '';
                const collisionBadge = f.collision ? ' <span class="preview-collision-badge">collision</span>' : '';
                const value = f.value.length > 80 ? Utils.escapeHtml(f.value.substring(0, 80)) + '...' : Utils.escapeHtml(f.value);
                html += `<tr${collisionClass}>
                    <td><code>${Utils.escapeHtml(f.original)}</code></td>
                    <td><code>${Utils.escapeHtml(f.normalized)}</code>${collisionBadge}</td>
                    <td class="preview-value">${value}</td>
                </tr>`;
            });

            html += '</tbody></table>';
            resultsContainer.innerHTML = html;
        } catch (err) {
            resultsContainer.innerHTML = `<div class="preview-error">${Utils.escapeHtml(err.message)}</div>`;
        }
    },

    togglePreview() {
        const container = document.getElementById('normalizerPreviewContainer');
        if (!container) return;
        const isVisible = container.style.display !== 'none';
        container.style.display = isVisible ? 'none' : 'block';
        if (!isVisible) {
            document.getElementById('normalizerPreviewInput')?.focus();
        }
    }
};

window.Normalizers = Normalizers;
